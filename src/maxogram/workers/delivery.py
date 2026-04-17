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
from maxogram.domain import OutboxAction, Platform, TaskStatus
from maxogram.metrics import dlq_total, retry_total
from maxogram.platforms.base import PlatformClient, PlatformDeliveryError
from maxogram.runtime_resilience import (
    RuntimeBackoffState,
    is_retryable_worker_error,
    wait_or_stop,
)
from maxogram.services.media import resolve_media_identity
from maxogram.services.relay import cleanup_local_media, materialize_media
from maxogram.services.retry import retry_decision

logger = logging.getLogger(__name__)


class EditMode(StrEnum):
    TEXT_ONLY = "text_only"
    CAPTION_ONLY_SAME_MEDIA = "caption_only_same_media"
    REPLACE_MEDIA = "replace_media"


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
    sent_with_media: bool = False


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
            return await self._send_message(client, payload, context.dst_chat_id)
        if context.action == OutboxAction.EDIT:
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
                    has_media=bool(payload.get("has_media")),
                    replacement_media=replacement_media,
                )
                return DeliveryResult()
            finally:
                cleanup_local_media(replacement_media)
        if context.action == OutboxAction.DELETE:
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
    ) -> DeliveryResult:
        media = _optional_dict(payload.get("media"))
        local_media = None
        try:
            if media is not None:
                local_media = await materialize_media(
                    clients=self.clients,
                    media=media,
                    root_dir=self.root_dir,
                )
            if media is None:
                result = await client.send_text(
                    chat_id,
                    _payload_text_plain(payload),
                    text_html=_payload_text_html(payload),
                    reply_to_message_id=_optional_str(
                        payload.get("reply_to_message_id")
                    ),
                )
                return DeliveryResult(dst_message_id=result.message_id)
            if local_media is None:
                result = await client.send_text(
                    chat_id,
                    _payload_fallback_text(payload),
                    reply_to_message_id=_optional_str(
                        payload.get("reply_to_message_id")
                    ),
                )
                return DeliveryResult(dst_message_id=result.message_id)
            result = await client.send_message(
                chat_id,
                _payload_text_plain(payload),
                text_html=_payload_text_html(payload),
                reply_to_message_id=_optional_str(payload.get("reply_to_message_id")),
                media=local_media,
            )
            return DeliveryResult(
                dst_message_id=result.message_id,
                sent_with_media=True,
            )
        finally:
            cleanup_local_media(local_media)

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
        current_media = _optional_dict(payload.get("media"))
        src = _expect_dict(payload.get("src"), "src")
        src_platform = _parse_platform(src.get("platform"), "src.platform")
        src_chat_id = _optional_str(src.get("chat_id"))
        src_message_id = _optional_str(src.get("message_id"))
        if src_chat_id is None or src_message_id is None:
            return EditMode.TEXT_ONLY
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
            reply_to_message_id=result.dst_message_id,
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
                    src_platform=src_platform,
                    src_chat_id=src_chat_id,
                    src_message_id=src_message_id,
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
    *,
    reply_to_message_id: str | None,
) -> dict[str, Any] | None:
    if (
        context.action != OutboxAction.SEND
        or context.src_platform != Platform.TELEGRAM
        or context.dst_platform != Platform.MAX
        or not result.sent_with_media
        or reply_to_message_id is None
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
        "reply_to_message_id": reply_to_message_id,
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
