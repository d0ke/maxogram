from __future__ import annotations

import asyncio
import logging
import uuid
from copy import deepcopy
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any

from maxogram.db.models import OutboxTask
from maxogram.db.repositories import Repository
from maxogram.db.session import Database
from maxogram.domain import LocalMediaFile, OutboxAction, Platform, TaskStatus
from maxogram.metrics import dlq_total, retry_total
from maxogram.platforms.base import PlatformClient, PlatformDeliveryError
from maxogram.services.dedup import stable_json_hash
from maxogram.runtime_resilience import (
    RuntimeBackoffState,
    is_retryable_worker_error,
    wait_or_stop,
)
from maxogram.services.media import (
    OUTBOUND_MEDIA_COUNT_LIMIT,
    OUTBOUND_MEDIA_PIECE_BUDGET_BYTES,
    destination_media_upload_limit_bytes,
    destination_upload_oversize_hint,
    resolve_media_identity,
)
from maxogram.services.relay import cleanup_local_media, materialize_media
from maxogram.services.retry import retry_decision

logger = logging.getLogger(__name__)


class EditMode(StrEnum):
    TEXT_ONLY = "text_only"
    CAPTION_ONLY_SAME_MEDIA = "caption_only_same_media"
    REPLACE_MEDIA = "replace_media"
    REPLACE_MEDIA_GROUP = "replace_media_group"


class DeliveryShape(StrEnum):
    TEXT = "text"
    SINGLE_MEDIA = "single_media"
    GROUP_SINGLE_PIECE = "group_single_piece"
    GROUP_MULTI_PIECE = "group_multi_piece"
    GROUP_FALLBACK_ONLY = "group_fallback_only"


@dataclass(frozen=True, slots=True)
class DeliveryContext:
    outbox_id: uuid.UUID
    bridge_id: uuid.UUID
    attempt_count: int
    action: OutboxAction
    dst_platform: Platform
    dst_chat_id: str
    payload: dict[str, Any]
    src_platform: Platform | None
    src_chat_id: str | None
    src_message_id: str | None
    src_event_id: uuid.UUID | None = None
    dedup_key: str | None = None
    partition_key: str | None = None
    edit_mode: EditMode = EditMode.TEXT_ONLY


@dataclass(frozen=True, slots=True)
class DeliveryResult:
    dst_message_id: str | None = None
    dst_message_ids: tuple[str, ...] = ()
    sent_with_media: bool = False
    delivery_state: DeliveryStateSnapshot | None = None


@dataclass(frozen=True, slots=True)
class DeliveryStateSnapshot:
    shape: DeliveryShape
    media_filtered: bool
    emitted_message_ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class MaterializedMediaItem:
    local_file: LocalMediaFile
    byte_size: int
    upload_limit_bytes: int
    oversize_hint: str


@dataclass(frozen=True, slots=True)
class MediaPiece:
    items: tuple[MaterializedMediaItem, ...]


@dataclass(frozen=True, slots=True)
class HintPiece:
    text_plain: str


@dataclass(frozen=True, slots=True)
class PlannedDeliveryPieces:
    pieces: tuple[MediaPiece | HintPiece, ...]
    source_is_group: bool
    media_filtered: bool


class DeliveryWorker:
    name = "delivery"

    def __init__(
        self,
        *,
        database: Database,
        clients: dict[Platform, PlatformClient],
        stop_event: asyncio.Event,
        lease_seconds: int,
        idle_seconds: float,
        root_dir: Path,
        batch_size: int = 25,
    ) -> None:
        self.database = database
        self.clients = clients
        self.stop_event = stop_event
        self.lease_seconds = lease_seconds
        self.idle_seconds = idle_seconds
        self.root_dir = root_dir
        self.batch_size = batch_size
        self._retry_backoff = RuntimeBackoffState()

    async def run(self) -> None:
        while not self.stop_event.is_set():
            try:
                processed = await self.run_once()
                self._log_recovery_if_needed()
                if processed == 0 and await wait_or_stop(
                    self.stop_event, self.idle_seconds
                ):
                    return
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if is_retryable_worker_error(exc):
                    if await self._wait_after_retryable_failure(exc):
                        return
                    continue
                self._retry_backoff.clear()
                logger.exception("Delivery failed")
                if await wait_or_stop(self.stop_event, self.idle_seconds):
                    return

    async def run_once(self) -> int:
        async with self.database.session() as session:
            repo = Repository(session)
            async with session.begin():
                tasks = await repo.claim_outbox(self.batch_size, self.lease_seconds)
        for task in tasks:
            await self._deliver_one(task)
        return len(tasks)

    async def _deliver_one(self, task: OutboxTask) -> None:
        try:
            context = await self._load_context(task.outbox_id)
        except PlatformDeliveryError as exc:
            await self._finalize_error(
                outbox_id=task.outbox_id,
                bridge_id=task.bridge_id,
                attempt_count=task.attempt_count,
                action=OutboxAction(task.action),
                payload=deepcopy(task.task),
                dst_platform=task.dst_platform,
                exc=exc,
            )
            return
        if context is None:
            return

        heartbeat_stop = asyncio.Event()
        heartbeat_task = asyncio.create_task(
            self._lease_heartbeat(context, heartbeat_stop),
            name=f"delivery-lease-{context.outbox_id}",
        )
        try:
            try:
                result = await self._call_platform(context)
            except PlatformDeliveryError as exc:
                await self._finalize_error(
                    outbox_id=context.outbox_id,
                    bridge_id=context.bridge_id,
                    attempt_count=context.attempt_count,
                    action=context.action,
                    payload=context.payload,
                    dst_platform=context.dst_platform,
                    exc=exc,
                )
            except Exception as exc:
                wrapped = PlatformDeliveryError(
                    str(exc), retryable=True, code=exc.__class__.__name__
                )
                await self._finalize_error(
                    outbox_id=context.outbox_id,
                    bridge_id=context.bridge_id,
                    attempt_count=context.attempt_count,
                    action=context.action,
                    payload=context.payload,
                    dst_platform=context.dst_platform,
                    exc=wrapped,
                )
            else:
                await self._finalize_success(context, result)
        finally:
            heartbeat_stop.set()
            await self._await_heartbeat(heartbeat_task)

    async def _load_context(self, outbox_id: uuid.UUID) -> DeliveryContext | None:
        async with self.database.session() as session:
            repo = Repository(session)
            async with session.begin():
                task = await repo.get_outbox_task(outbox_id)
                if task is None or task.status != TaskStatus.INFLIGHT:
                    return None
                payload = deepcopy(task.task)
                src = _expect_dict(payload.get("src"), "src")
                dst = _expect_dict(payload.get("dst"), "dst")
                src_platform = _parse_platform(src.get("platform"), "src.platform")
                dst_platform = _parse_platform(dst.get("platform"), "dst.platform")
                edit_mode = await self._classify_edit_mode(
                    repo=repo,
                    bridge_id=task.bridge_id,
                    payload=payload,
                    action=OutboxAction(task.action),
                )
                return DeliveryContext(
                    outbox_id=task.outbox_id,
                    bridge_id=task.bridge_id,
                    attempt_count=task.attempt_count,
                    action=OutboxAction(task.action),
                    dst_platform=dst_platform,
                    dst_chat_id=str(dst["chat_id"]),
                    payload=payload,
                    src_platform=src_platform,
                    src_chat_id=_optional_str(src.get("chat_id")),
                    src_message_id=_optional_str(src.get("message_id")),
                    src_event_id=task.src_event_id,
                    dedup_key=task.dedup_key,
                    partition_key=task.partition_key,
                    edit_mode=edit_mode,
                )

    async def _call_platform(self, context: DeliveryContext) -> DeliveryResult:
        payload = context.payload
        client = self.clients[context.dst_platform]

        if context.action == OutboxAction.SEND:
            return await self._send_message(
                client,
                payload,
                context.dst_chat_id,
                context.dst_platform,
            )
        if context.action == OutboxAction.EDIT:
            if context.edit_mode == EditMode.REPLACE_MEDIA_GROUP:
                return await self._edit_media_group(
                    client,
                    payload,
                    context.dst_chat_id,
                    context.dst_platform,
                )
            replacement_media = None
            try:
                if context.edit_mode == EditMode.REPLACE_MEDIA:
                    media = _optional_dict(payload.get("media"))
                    if media is None:
                        raise PlatformDeliveryError(
                            "Removing mirrored media during edit is unsupported",
                            retryable=False,
                            code="media_removal_unsupported",
                        )
                    replacement_media = await materialize_media(
                        clients=self.clients,
                        media=media,
                        root_dir=self.root_dir,
                    )
                    if replacement_media is None:
                        raise PlatformDeliveryError(
                            "Replacement media for edit is unavailable",
                            retryable=False,
                            code="replacement_media_unavailable",
                        )
                await client.edit_message(
                    context.dst_chat_id,
                    str(payload["dst_message_id"]),
                    _payload_text_plain(payload),
                    text_html=_payload_text_html(payload),
                    has_media=_payload_edit_has_media(payload, context.edit_mode),
                    replacement_media=replacement_media,
                )
                return DeliveryResult()
            finally:
                cleanup_local_media(replacement_media)
        if context.action == OutboxAction.DELETE:
            if _is_media_group_payload(payload):
                for message_id in _payload_dst_message_ids(payload):
                    await client.delete_message(
                        context.dst_chat_id,
                        message_id,
                    )
                return DeliveryResult()
            await client.delete_message(
                context.dst_chat_id,
                str(payload["dst_message_id"]),
            )
            return DeliveryResult()
        raise PlatformDeliveryError(
            f"Unsupported outbox action: {context.action}",
            retryable=False,
            code="unsupported_action",
        )

    async def _send_message(
        self,
        client: PlatformClient,
        payload: dict[str, Any],
        chat_id: str,
        dst_platform: Platform,
    ) -> DeliveryResult:
        media = _optional_dict(payload.get("media"))
        media_items = _payload_media_items(payload)
        local_media_items: list[LocalMediaFile] = []
        try:
            if media is None and not media_items:
                return await self._send_plain_text(
                    client,
                    chat_id,
                    text_plain=_payload_text_plain(payload),
                    text_html=_payload_text_html(payload),
                    reply_to_message_id=_optional_str(payload.get("reply_to_message_id")),
                    shape=DeliveryShape.TEXT,
                )
            if media_items:
                local_media_items = await self._materialize_media_items(media_items)
            elif media is not None:
                local_media = await materialize_media(
                    clients=self.clients,
                    media=media,
                    root_dir=self.root_dir,
                )
                if local_media is not None:
                    local_media_items = [local_media]
            if media_items and not local_media_items:
                return await self._send_plain_text(
                    client,
                    chat_id,
                    text_plain=_payload_fallback_text(payload),
                    reply_to_message_id=_optional_str(payload.get("reply_to_message_id")),
                    shape=DeliveryShape.GROUP_FALLBACK_ONLY,
                )
            if media is not None and not local_media_items:
                return await self._send_plain_text(
                    client,
                    chat_id,
                    text_plain=_payload_fallback_text(payload),
                    reply_to_message_id=_optional_str(payload.get("reply_to_message_id")),
                    shape=DeliveryShape.TEXT,
                )
            planned = _plan_delivery_pieces(
                payload=payload,
                media_items=[
                    _materialized_media_item(dst_platform, local_media)
                    for local_media in local_media_items
                ],
                source_is_group=bool(media_items),
            )
            return await self._send_planned_pieces(
                client=client,
                chat_id=chat_id,
                payload=payload,
                planned=planned,
            )
        finally:
            for item in local_media_items:
                cleanup_local_media(item)

    async def _classify_edit_mode(
        self,
        *,
        repo: Repository,
        bridge_id: uuid.UUID,
        payload: dict[str, Any],
        action: OutboxAction,
    ) -> EditMode:
        if action != OutboxAction.EDIT:
            return EditMode.TEXT_ONLY
        src = _expect_dict(payload.get("src"), "src")
        dst = _expect_dict(payload.get("dst"), "dst")
        src_platform = _parse_platform(src.get("platform"), "src.platform")
        dst_platform = _parse_platform(dst.get("platform"), "dst.platform")
        src_chat_id = _optional_str(src.get("chat_id"))
        src_message_id = _optional_str(src.get("message_id"))
        if _is_media_group_payload(payload):
            return await self._classify_media_group_edit_mode(
                repo=repo,
                bridge_id=bridge_id,
                payload=payload,
                src_platform=src_platform,
                src_chat_id=src_chat_id,
                src_message_id=src_message_id,
                dst_platform=dst_platform,
            )
        current_media = _optional_dict(payload.get("media"))
        if src_chat_id is None or src_message_id is None:
            return EditMode.TEXT_ONLY
        created_send_payload = await repo.get_created_send_payload(
            bridge_id,
            src_platform,
            src_chat_id,
            src_message_id,
            dst_platform,
        )
        if _payload_delivery_shape(created_send_payload) == DeliveryShape.TEXT:
            return self._log_edit_mode(
                payload,
                EditMode.TEXT_ONLY,
                current_identity=_payload_media_identity(payload, current_media),
                created_identity=_payload_media_identity(created_send_payload, None),
            )
        created_payload = created_send_payload
        if created_payload is None:
            created_payload = await repo.get_created_event_payload(
                bridge_id,
                src_platform,
                src_chat_id,
                src_message_id,
            )
        created_media = (
            _optional_dict(created_payload.get("media"))
            if isinstance(created_payload, dict)
            else None
        )
        if current_media is None and created_media is None:
            return EditMode.TEXT_ONLY

        current_identity = _payload_media_identity(payload, current_media)
        created_identity = _payload_media_identity(created_payload, created_media)
        if current_media is None or created_media is None:
            return self._log_edit_mode(
                payload,
                EditMode.REPLACE_MEDIA,
                current_identity=current_identity,
                created_identity=created_identity,
            )
        if current_identity is None or created_identity is None:
            return self._log_edit_mode(
                payload,
                EditMode.CAPTION_ONLY_SAME_MEDIA,
                current_identity=current_identity,
                created_identity=created_identity,
            )
        mode = (
            EditMode.CAPTION_ONLY_SAME_MEDIA
            if current_identity == created_identity
            else EditMode.REPLACE_MEDIA
        )
        return self._log_edit_mode(
            payload,
            mode,
            current_identity=current_identity,
            created_identity=created_identity,
        )

    async def _classify_media_group_edit_mode(
        self,
        *,
        repo: Repository,
        bridge_id: uuid.UUID,
        payload: dict[str, Any],
        src_platform: Platform,
        src_chat_id: str | None,
        src_message_id: str | None,
        dst_platform: Platform,
    ) -> EditMode:
        current_group_signature = _payload_media_group_signature(payload)
        if dst_platform != Platform.TELEGRAM:
            return self._log_edit_mode(
                payload,
                EditMode.REPLACE_MEDIA_GROUP,
                current_identity=current_group_signature,
                created_identity=None,
            )
        if src_chat_id is None or src_message_id is None:
            return self._log_edit_mode(
                payload,
                EditMode.REPLACE_MEDIA_GROUP,
                current_identity=current_group_signature,
                created_identity=None,
            )
        created_payload = await repo.get_created_send_payload(
            bridge_id,
            src_platform,
            src_chat_id,
            src_message_id,
            dst_platform,
        )
        created_group_signature = _payload_media_group_signature(created_payload)
        current_group_identities = _payload_media_group_identities(payload)
        created_group_identities = _payload_media_group_identities(created_payload)
        mode = (
            EditMode.CAPTION_ONLY_SAME_MEDIA
            if (
                _payload_delivery_shape(created_payload) == DeliveryShape.GROUP_SINGLE_PIECE
                and not _payload_delivery_media_filtered(created_payload)
                and current_group_identities is not None
                and created_group_identities is not None
                and current_group_identities == created_group_identities
            )
            else EditMode.REPLACE_MEDIA_GROUP
        )
        return self._log_edit_mode(
            payload,
            mode,
            current_identity=current_group_signature,
            created_identity=created_group_signature,
        )

    def _log_edit_mode(
        self,
        payload: dict[str, Any],
        mode: EditMode,
        *,
        current_identity: str | None,
        created_identity: str | None,
    ) -> EditMode:
        dst = _optional_dict(payload.get("dst")) or {}
        src = _optional_dict(payload.get("src")) or {}
        logger.info(
            "Classified media edit mode=%s src_platform=%s dst_platform=%s "
            "src_message_id=%s identity_changed=%s",
            mode.value,
            src.get("platform"),
            dst.get("platform"),
            src.get("message_id"),
            (
                current_identity != created_identity
                if current_identity is not None and created_identity is not None
                else "unknown"
            ),
        )
        return mode

    async def _edit_media_group(
        self,
        client: PlatformClient,
        payload: dict[str, Any],
        chat_id: str,
        dst_platform: Platform,
    ) -> DeliveryResult:
        for message_id in _payload_dst_message_ids(payload):
            await client.delete_message(chat_id, message_id)
        return await self._send_message(client, payload, chat_id, dst_platform)

    async def _materialize_media_items(
        self,
        media_items: list[dict[str, Any]],
    ) -> list[LocalMediaFile]:
        local_media_items: list[LocalMediaFile] = []
        for media in media_items:
            local_media = await materialize_media(
                clients=self.clients,
                media=media,
                root_dir=self.root_dir,
            )
            if local_media is None:
                for item in local_media_items:
                    cleanup_local_media(item)
                return []
            local_media_items.append(local_media)
        return local_media_items

    async def _send_plain_text(
        self,
        client: PlatformClient,
        chat_id: str,
        *,
        text_plain: str,
        text_html: str | None = None,
        reply_to_message_id: str | None = None,
        shape: DeliveryShape,
        media_filtered: bool = False,
    ) -> DeliveryResult:
        result = await client.send_text(
            chat_id,
            text_plain,
            text_html=text_html,
            reply_to_message_id=reply_to_message_id,
        )
        emitted_ids = (result.message_id,)
        return DeliveryResult(
            dst_message_id=result.message_id,
            dst_message_ids=emitted_ids,
            sent_with_media=False,
            delivery_state=DeliveryStateSnapshot(
                shape=shape,
                media_filtered=media_filtered,
                emitted_message_ids=emitted_ids,
            ),
        )

    async def _send_planned_pieces(
        self,
        *,
        client: PlatformClient,
        chat_id: str,
        payload: dict[str, Any],
        planned: PlannedDeliveryPieces,
    ) -> DeliveryResult:
        queue = list(planned.pieces)
        emitted_ids: list[str] = []
        media_piece_count = 0
        hint_piece_count = 0
        media_text_consumed = False
        media_filtered = planned.media_filtered
        reply_to_message_id = _optional_str(payload.get("reply_to_message_id"))
        base_text_plain = _payload_text_plain(payload)
        base_text_html = _payload_text_html(payload)

        while queue:
            piece = queue.pop(0)
            if isinstance(piece, HintPiece):
                result = await client.send_text(
                    chat_id,
                    piece.text_plain,
                    reply_to_message_id=(
                        reply_to_message_id if not emitted_ids else None
                    ),
                )
                emitted_ids.append(result.message_id)
                hint_piece_count += 1
                continue

            try:
                send_result = await client.send_message(
                    chat_id,
                    base_text_plain if not media_text_consumed else "",
                    text_html=base_text_html if not media_text_consumed else None,
                    reply_to_message_id=(
                        reply_to_message_id if not emitted_ids else None
                    ),
                    media=_piece_media_argument(piece),
                )
            except PlatformDeliveryError as exc:
                if _is_entity_too_large_error(exc):
                    media_filtered = True
                    if len(piece.items) > 1:
                        queue[0:0] = list(_split_media_piece(piece))
                        continue
                    queue.insert(0, HintPiece(piece.items[0].oversize_hint))
                    continue
                raise

            emitted_piece_ids = _send_result_message_ids(send_result)
            emitted_ids.extend(emitted_piece_ids)
            media_piece_count += 1
            media_text_consumed = True

        primary_id = emitted_ids[0] if emitted_ids else None
        delivery_state = DeliveryStateSnapshot(
            shape=_delivery_shape_for_result(
                source_is_group=planned.source_is_group,
                media_piece_count=media_piece_count,
                hint_piece_count=hint_piece_count,
            ),
            media_filtered=media_filtered,
            emitted_message_ids=tuple(emitted_ids),
        )
        return DeliveryResult(
            dst_message_id=primary_id,
            dst_message_ids=tuple(emitted_ids),
            sent_with_media=media_piece_count > 0,
            delivery_state=delivery_state,
        )

    async def _lease_heartbeat(
        self,
        context: DeliveryContext,
        stop_event: asyncio.Event,
    ) -> None:
        interval = self._heartbeat_interval_seconds()
        while not stop_event.is_set():
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=interval)
                return
            except TimeoutError:
                pass

            try:
                async with self.database.session() as session:
                    repo = Repository(session)
                    async with session.begin():
                        renewed = await repo.renew_outbox_lease(
                            context.outbox_id,
                            context.attempt_count,
                            self.lease_seconds,
                        )
                if not renewed:
                    logger.warning(
                        "Stopped lease heartbeat for stale outbox task "
                        "outbox_id=%s attempt=%s",
                        context.outbox_id,
                        context.attempt_count,
                    )
                    return
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    "Lease heartbeat failed outbox_id=%s attempt=%s",
                    context.outbox_id,
                    context.attempt_count,
                )

    async def _finalize_success(
        self,
        context: DeliveryContext,
        result: DeliveryResult,
    ) -> None:
        src_platform, src_chat_id, src_message_id = _mapping_source_fields(context)
        post_send_payload = _build_post_send_task_payload(
            context,
            result,
        )
        async with self.database.session() as session:
            repo = Repository(session)
            async with session.begin():
                finalized = await repo.finalize_outbox_success(
                    outbox_id=context.outbox_id,
                    attempt_count=context.attempt_count,
                    bridge_id=context.bridge_id,
                    dst_platform=context.dst_platform,
                    dst_chat_id=context.dst_chat_id,
                    dst_message_id=result.dst_message_id,
                    dst_message_ids=list(result.dst_message_ids),
                    src_platform=src_platform,
                    src_chat_id=src_chat_id,
                    src_message_id=src_message_id,
                    group_kind=_payload_group_kind(context.payload),
                    src_member_message_ids=_payload_source_member_message_ids(
                        context.payload
                    ),
                    delivery_state=_delivery_state_payload(result.delivery_state),
                )
                if finalized and post_send_payload is not None:
                    if context.src_event_id is None:
                        raise PlatformDeliveryError(
                            "Auxiliary post-send task requires src_event_id",
                            retryable=True,
                            code="missing_src_event_id",
                        )
                    if context.dedup_key is None or context.partition_key is None:
                        raise PlatformDeliveryError(
                            "Auxiliary post-send task requires queue metadata",
                            retryable=True,
                            code="missing_queue_metadata",
                        )
                    await repo.enqueue_outbox(
                        bridge_id=context.bridge_id,
                        dedup_key=f"{context.dedup_key}:post_send_text",
                        src_event_id=context.src_event_id,
                        dst_platform=context.dst_platform,
                        action=OutboxAction.SEND,
                        partition_key=context.partition_key,
                        task=post_send_payload,
                    )
        if not finalized:
            logger.warning(
                "Skipped stale success finalization outbox_id=%s attempt=%s",
                context.outbox_id,
                context.attempt_count,
            )

    async def _finalize_error(
        self,
        *,
        outbox_id: uuid.UUID,
        bridge_id: uuid.UUID,
        attempt_count: int,
        action: OutboxAction,
        payload: dict[str, Any],
        dst_platform: Platform,
        exc: PlatformDeliveryError,
    ) -> None:
        decision = retry_decision(
            action,
            attempt_count,
            retryable_error=exc.retryable,
        )
        finalized = False
        async with self.database.session() as session:
            repo = Repository(session)
            async with session.begin():
                if decision.retryable and decision.next_attempt_at is not None:
                    finalized = await repo.finalize_outbox_retry(
                        outbox_id=outbox_id,
                        attempt_count=attempt_count,
                        next_attempt_at=decision.next_attempt_at,
                        http_status=exc.http_status,
                        error_code=exc.code,
                        error_message=str(exc),
                    )
                else:
                    finalized = await repo.finalize_outbox_dead(
                        outbox_id=outbox_id,
                        attempt_count=attempt_count,
                        bridge_id=bridge_id,
                        reason=exc.code or "permanent_failure",
                        payload=payload,
                        http_status=exc.http_status,
                        error_code=exc.code,
                        error_message=str(exc),
                    )
        if not finalized:
            logger.warning(
                "Skipped stale error finalization outbox_id=%s attempt=%s code=%s",
                outbox_id,
                attempt_count,
                exc.code,
            )
            return
        if decision.retryable and decision.next_attempt_at is not None:
            retry_total.labels(dst_platform.value).inc()
            return
        dlq_total.labels(dst_platform.value).inc()

    def _heartbeat_interval_seconds(self) -> float:
        return float(max(5, self.lease_seconds // 3))

    async def _await_heartbeat(self, heartbeat_task: asyncio.Task[None]) -> None:
        try:
            await heartbeat_task
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Lease heartbeat join failed")

    async def _wait_after_retryable_failure(self, exc: BaseException) -> bool:
        delay_seconds = self._retry_backoff.next_delay_seconds()
        logger.warning(
            "%s temporary failure attempt=%s error=%s retry_in=%.0fs: %s",
            self.name,
            self._retry_backoff.attempts,
            exc.__class__.__name__,
            delay_seconds,
            exc,
        )
        return await wait_or_stop(self.stop_event, delay_seconds)

    def _log_recovery_if_needed(self) -> None:
        recovered_attempts = self._retry_backoff.clear()
        if recovered_attempts:
            logger.info(
                "%s recovered after %s temporary failure(s)",
                self.name,
                recovered_attempts,
            )


def _expect_dict(value: Any, name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise PlatformDeliveryError(
            f"Task field {name} must be an object",
            retryable=False,
        )
    return value


def _optional_str(value: object) -> str | None:
    return str(value) if value is not None else None


def _optional_dict(value: object) -> dict[str, Any] | None:
    return value if isinstance(value, dict) else None


def _payload_text_plain(payload: dict[str, Any]) -> str:
    value = payload.get("text_plain")
    if value is not None:
        return str(value)
    legacy = payload.get("text")
    if legacy is not None:
        return str(legacy)
    return _payload_fallback_text(payload)


def _payload_text_html(payload: dict[str, Any]) -> str | None:
    value = payload.get("text_html")
    if isinstance(value, str) and value:
        return value
    return None


def _payload_fallback_text(payload: dict[str, Any]) -> str:
    value = payload.get("fallback_text")
    if value is not None:
        return str(value)
    legacy = payload.get("text")
    return str(legacy) if legacy is not None else ""


def _payload_edit_has_media(payload: dict[str, Any], edit_mode: EditMode) -> bool:
    return bool(payload.get("has_media")) and edit_mode != EditMode.TEXT_ONLY


def _fallback_text_with_hints(
    payload: dict[str, Any],
    hints: list[str],
) -> str:
    lines: list[str] = []
    text_plain = _payload_text_plain(payload)
    if text_plain:
        lines.append(text_plain)
    lines.extend(hint for hint in hints if hint)
    if lines:
        return "\n".join(lines)
    return _payload_fallback_text(payload)


def _delivery_state_payload(
    delivery_state: DeliveryStateSnapshot | None,
) -> dict[str, Any] | None:
    if delivery_state is None:
        return None
    return {
        "shape": delivery_state.shape.value,
        "media_filtered": delivery_state.media_filtered,
        "emitted_message_ids": list(delivery_state.emitted_message_ids),
    }


def _payload_post_send_text_plain(payload: dict[str, Any]) -> str | None:
    value = payload.get("post_send_text_plain")
    if isinstance(value, str) and value:
        return value
    return None


def _payload_post_send_text_html(payload: dict[str, Any]) -> str | None:
    value = payload.get("post_send_text_html")
    if isinstance(value, str) and value:
        return value
    return None


def _payload_media_items(payload: dict[str, Any]) -> list[dict[str, Any]]:
    items = payload.get("media_items")
    if not isinstance(items, list):
        return []
    return [item for item in items if isinstance(item, dict)]


def _payload_group_kind(payload: dict[str, Any]) -> str | None:
    value = payload.get("group_kind")
    if isinstance(value, str) and value:
        return value
    return None


def _payload_source_member_message_ids(payload: dict[str, Any]) -> list[str]:
    values = payload.get("source_member_message_ids")
    if not isinstance(values, list):
        return []
    return [str(value) for value in values if value is not None]


def _payload_dst_message_ids(payload: dict[str, Any]) -> list[str]:
    values = payload.get("dst_message_ids")
    if isinstance(values, list):
        result = [str(value) for value in values if value is not None]
        if result:
            return result
    message_id = _optional_str(payload.get("dst_message_id"))
    return [message_id] if message_id is not None else []


def _is_media_group_payload(payload: dict[str, Any]) -> bool:
    return _payload_group_kind(payload) == "photo_video_chunk" and bool(
        _payload_media_items(payload)
    )


def _materialized_media_item(
    dst_platform: Platform,
    local_file: LocalMediaFile,
) -> MaterializedMediaItem:
    byte_size = local_file.path.stat().st_size
    return MaterializedMediaItem(
        local_file=local_file,
        byte_size=byte_size,
        upload_limit_bytes=destination_media_upload_limit_bytes(
            dst_platform,
            local_file,
        ),
        oversize_hint=destination_upload_oversize_hint(
            dst_platform,
            local_file.kind,
            presentation=local_file.presentation,
        ),
    )


def _plan_delivery_pieces(
    *,
    payload: dict[str, Any],
    media_items: list[MaterializedMediaItem],
    source_is_group: bool,
) -> PlannedDeliveryPieces:
    if not source_is_group:
        if not media_items:
            return PlannedDeliveryPieces((), False, False)
        item = media_items[0]
        if item.byte_size > item.upload_limit_bytes:
            return PlannedDeliveryPieces(
                (
                    HintPiece(
                        _fallback_text_with_hints(payload, [item.oversize_hint])
                    ),
                ),
                False,
                True,
            )
        return PlannedDeliveryPieces((MediaPiece((item,)),), False, False)

    pieces: list[MediaPiece | HintPiece] = []
    current_piece: list[MaterializedMediaItem] = []
    current_piece_bytes = 0
    filtered_hints: list[str] = []
    media_filtered = False
    sendable_found = False

    for item in media_items:
        if item.byte_size > item.upload_limit_bytes:
            if current_piece:
                pieces.append(MediaPiece(tuple(current_piece)))
                current_piece = []
                current_piece_bytes = 0
            pieces.append(HintPiece(item.oversize_hint))
            filtered_hints.append(item.oversize_hint)
            media_filtered = True
            continue

        sendable_found = True
        would_exceed_count = len(current_piece) >= OUTBOUND_MEDIA_COUNT_LIMIT
        would_exceed_budget = (
            bool(current_piece)
            and current_piece_bytes + item.byte_size > OUTBOUND_MEDIA_PIECE_BUDGET_BYTES
        )
        if would_exceed_count or would_exceed_budget:
            pieces.append(MediaPiece(tuple(current_piece)))
            current_piece = []
            current_piece_bytes = 0

        current_piece.append(item)
        current_piece_bytes += item.byte_size

    if current_piece:
        pieces.append(MediaPiece(tuple(current_piece)))

    if not sendable_found:
        return PlannedDeliveryPieces(
            (
                HintPiece(_fallback_text_with_hints(payload, filtered_hints)),
            ),
            True,
            media_filtered,
        )
    return PlannedDeliveryPieces(tuple(pieces), True, media_filtered)


def _piece_media_argument(
    piece: MediaPiece,
) -> LocalMediaFile | list[LocalMediaFile]:
    media_files = [item.local_file for item in piece.items]
    if len(media_files) == 1:
        return media_files[0]
    return media_files


def _split_media_piece(piece: MediaPiece) -> tuple[MediaPiece, ...]:
    if len(piece.items) <= 1:
        return (piece,)
    midpoint = len(piece.items) // 2
    return (
        MediaPiece(piece.items[:midpoint]),
        MediaPiece(piece.items[midpoint:]),
    )


def _send_result_message_ids(result: Any) -> tuple[str, ...]:
    member_message_ids = getattr(result, "member_message_ids", ())
    if member_message_ids:
        return tuple(str(message_id) for message_id in member_message_ids)
    message_id = _optional_str(getattr(result, "message_id", None))
    return (message_id,) if message_id is not None else ()


def _delivery_shape_for_result(
    *,
    source_is_group: bool,
    media_piece_count: int,
    hint_piece_count: int,
) -> DeliveryShape:
    if not source_is_group:
        return DeliveryShape.SINGLE_MEDIA if media_piece_count else DeliveryShape.TEXT
    if media_piece_count == 0:
        return DeliveryShape.GROUP_FALLBACK_ONLY
    if media_piece_count == 1 and hint_piece_count == 0:
        return DeliveryShape.GROUP_SINGLE_PIECE
    return DeliveryShape.GROUP_MULTI_PIECE


def _is_entity_too_large_error(exc: PlatformDeliveryError) -> bool:
    if exc.http_status == 413 or exc.code == "entity_too_large":
        return True
    code = (exc.code or "").casefold()
    message = str(exc).casefold()
    return "entity too large" in code or "entity too large" in message


def _payload_creates_mapping(payload: dict[str, Any]) -> bool:
    value = payload.get("creates_mapping")
    if isinstance(value, bool):
        return value
    return True


def _mapping_source_fields(
    context: DeliveryContext,
) -> tuple[Platform | None, str | None, str | None]:
    if _payload_creates_mapping(context.payload):
        return context.src_platform, context.src_chat_id, context.src_message_id
    return None, None, None


def _build_post_send_task_payload(
    context: DeliveryContext,
    result: DeliveryResult,
) -> dict[str, Any] | None:
    if (
        context.action != OutboxAction.SEND
        or context.src_platform != Platform.TELEGRAM
        or context.dst_platform != Platform.MAX
        or not result.sent_with_media
    ):
        return None
    post_send_text_plain = _payload_post_send_text_plain(context.payload)
    if post_send_text_plain is None:
        return None
    src = _optional_dict(context.payload.get("src"))
    dst = _optional_dict(context.payload.get("dst"))
    if src is None or dst is None:
        return None
    return {
        "src": src,
        "dst": dst,
        "text": post_send_text_plain,
        "text_plain": post_send_text_plain,
        "text_html": _payload_post_send_text_html(context.payload),
        "fallback_text": post_send_text_plain,
        "raw": context.payload.get("raw") or {},
        "has_media": False,
        "media_kind": None,
        "media": None,
        "version": context.payload.get("version"),
        "creates_mapping": False,
    }


def _payload_raw_message(payload: dict[str, Any] | None) -> dict[str, Any] | None:
    raw = _optional_dict(payload.get("raw")) if isinstance(payload, dict) else None
    return _optional_dict(raw.get("raw_message")) if raw is not None else None


def _payload_media_identity(
    payload: dict[str, Any] | None,
    media: dict[str, Any] | None,
) -> str | None:
    return resolve_media_identity(
        media,
        raw_message=_payload_raw_message(payload),
    )


def _payload_media_group_identities(
    payload: dict[str, Any] | None,
) -> list[str] | None:
    if _payload_media_group_kind(payload) != "photo_video_chunk":
        return None
    identities: list[str] = []
    for media_item in _payload_media_group_items(payload):
        identity = resolve_media_identity(media_item)
        if identity is None:
            return None
        identities.append(identity)
    return identities if identities else None


def _payload_media_group_signature(
    payload: dict[str, Any] | None,
) -> str | None:
    identities = _payload_media_group_identities(payload)
    if identities is None:
        return None
    return stable_json_hash(identities)


def _payload_delivery_shape(
    payload: dict[str, Any] | None,
) -> DeliveryShape | None:
    if not isinstance(payload, dict):
        return None
    delivery_state = _optional_dict(payload.get("delivery_state"))
    if delivery_state is None:
        return None
    shape = delivery_state.get("shape")
    if not isinstance(shape, str) or not shape:
        return None
    try:
        return DeliveryShape(shape)
    except ValueError:
        return None


def _payload_delivery_media_filtered(payload: dict[str, Any] | None) -> bool:
    if not isinstance(payload, dict):
        return False
    delivery_state = _optional_dict(payload.get("delivery_state"))
    if delivery_state is None:
        return False
    return bool(delivery_state.get("media_filtered"))


def _payload_media_group_kind(payload: dict[str, Any] | None) -> str | None:
    if not isinstance(payload, dict):
        return None
    group_kind = payload.get("group_kind")
    if isinstance(group_kind, str) and group_kind:
        return group_kind
    media_group = _optional_dict(payload.get("media_group"))
    if media_group is None:
        return None
    nested_group_kind = media_group.get("group_kind")
    if isinstance(nested_group_kind, str) and nested_group_kind:
        return nested_group_kind
    return None


def _payload_media_group_items(payload: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    direct_items = payload.get("media_items")
    if isinstance(direct_items, list):
        return [item for item in direct_items if isinstance(item, dict)]
    media_group = _optional_dict(payload.get("media_group"))
    if media_group is None:
        return []
    nested_items = media_group.get("items")
    if not isinstance(nested_items, list):
        return []
    return [item for item in nested_items if isinstance(item, dict)]


def _parse_platform(value: object, field_name: str) -> Platform:
    if value is None:
        raise PlatformDeliveryError(
            f"Task field {field_name} is required",
            retryable=False,
            code="invalid_task_payload",
        )
    try:
        return Platform(str(value))
    except ValueError as exc:
        raise PlatformDeliveryError(
            f"Unsupported platform value for {field_name}: {value}",
            retryable=False,
            code="invalid_task_payload",
        ) from exc
