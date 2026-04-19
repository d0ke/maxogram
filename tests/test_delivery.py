from __future__ import annotations

import asyncio
import os
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

from maxogram.domain import (
    DeliveryOutcome,
    LocalMediaFile,
    MediaKind,
    MediaPresentation,
    OutboxAction,
    Platform,
    SendResult,
    TaskStatus,
)
from maxogram.platforms.base import PlatformDeliveryError
from maxogram.platforms.telegram import TelegramClient
from maxogram.services.relay import (
    animated_sticker_cache_dir,
    cleanup_local_media,
    materialize_media,
)
from maxogram.workers.delivery import DeliveryContext, DeliveryWorker, EditMode
from maxogram.workers.reconciliation import ReconciliationWorker


class FakeClient:
    def __init__(
        self,
        name: str,
        *,
        state: DeliveryState | None = None,
        download_kind: MediaKind | None = None,
        send_delay: float = 0.0,
        download_delay: float = 0.0,
        on_send_text: Any | None = None,
        on_send_message: Any | None = None,
        assert_no_active_transaction: bool = False,
        download_size_bytes: int = 5,
        download_size_by_identity: dict[str, int] | None = None,
    ) -> None:
        self.name = name
        self.state = state
        self.download_kind = download_kind
        self.send_delay = send_delay
        self.download_delay = download_delay
        self.on_send_text = on_send_text
        self.on_send_message = on_send_message
        self.assert_no_active_transaction = assert_no_active_transaction
        self.download_size_bytes = download_size_bytes
        self.download_size_by_identity = dict(download_size_by_identity or {})
        self.send_text_calls: list[dict[str, Any]] = []
        self.send_message_calls: list[dict[str, Any]] = []
        self.edit_calls: list[dict[str, Any]] = []
        self.delete_calls: list[dict[str, Any]] = []
        self.download_calls: list[dict[str, object]] = []

    async def poll_updates(self, cursor: int | None, *, limit: int, poll_timeout: int):
        _ = cursor, limit, poll_timeout
        raise AssertionError("poll_updates is not used in delivery tests")

    async def send_text(
        self,
        chat_id: str,
        text_plain: str,
        *,
        text_html: str | None = None,
        reply_to_message_id: str | None = None,
    ) -> SendResult:
        self._assert_no_active_transaction()
        if self.on_send_text is not None:
            callback = self.on_send_text
            if asyncio.iscoroutinefunction(callback):
                await callback()
            else:
                callback()
        self.send_text_calls.append(
            {
                "chat_id": chat_id,
                "text": text_plain,
                "text_html": text_html,
                "reply_to_message_id": reply_to_message_id,
            }
        )
        if self.send_delay:
            await asyncio.sleep(self.send_delay)
        return SendResult(
            message_id=f"{self.name}-text-{len(self.send_text_calls)}",
            raw={"mode": "text"},
        )

    async def send_message(
        self,
        chat_id: str,
        text_plain: str,
        *,
        text_html: str | None = None,
        reply_to_message_id: str | None = None,
        media: LocalMediaFile | list[LocalMediaFile] | None = None,
    ) -> SendResult:
        self._assert_no_active_transaction()
        if self.on_send_message is not None:
            callback = self.on_send_message
            if asyncio.iscoroutinefunction(callback):
                await callback()
            else:
                callback()
        self.send_message_calls.append(
            {
                "chat_id": chat_id,
                "text": text_plain,
                "text_html": text_html,
                "reply_to_message_id": reply_to_message_id,
                "media": media,
            }
        )
        if self.send_delay:
            await asyncio.sleep(self.send_delay)
        member_message_ids: tuple[str, ...] = ()
        if self.name == "telegram" and isinstance(media, list) and media:
            member_message_ids = tuple(
                f"{self.name}-media-{len(self.send_message_calls)}-{index}"
                for index in range(1, len(media) + 1)
            )
        message_id = (
            member_message_ids[0]
            if member_message_ids
            else f"{self.name}-media-{len(self.send_message_calls)}"
        )
        return SendResult(
            message_id=message_id,
            raw={"mode": "media"},
            member_message_ids=member_message_ids,
        )

    async def edit_message(
        self,
        chat_id: str,
        message_id: str,
        text_plain: str,
        *,
        text_html: str | None = None,
        has_media: bool = False,
        replacement_media: LocalMediaFile | list[LocalMediaFile] | None = None,
    ) -> None:
        self.edit_calls.append(
            {
                "chat_id": chat_id,
                "message_id": message_id,
                "text": text_plain,
                "text_html": text_html,
                "has_media": has_media,
                "replacement_media": replacement_media,
            }
        )

    async def delete_message(self, chat_id: str, message_id: str) -> None:
        self.delete_calls.append({"chat_id": chat_id, "message_id": message_id})

    async def download_media(
        self,
        media: dict[str, object],
        destination_dir: Path,
    ) -> LocalMediaFile | None:
        self._assert_no_active_transaction()
        self.download_calls.append(media)
        if self.download_delay:
            await asyncio.sleep(self.download_delay)
        if self.download_kind is None:
            return None
        filename = str(media.get("filename") or "relay.bin")
        destination = destination_dir / f"{uuid.uuid4()}-{filename}"
        identity = str(media.get("identity") or "")
        download_size = self.download_size_by_identity.get(
            identity,
            self.download_size_bytes,
        )
        await asyncio.to_thread(_write_file_of_size, destination, download_size)
        return LocalMediaFile(
            kind=self.download_kind,
            path=destination,
            filename=filename,
            mime_type=str(media.get("mime_type") or "application/octet-stream"),
            sticker_variant=(
                str(media["sticker_variant"])
                if media.get("sticker_variant") is not None
                else None
            ),
            presentation=(
                MediaPresentation(str(media["presentation"]))
                if media.get("presentation") is not None
                else None
            ),
        )

    async def is_admin(self, chat_id: str, user_id: str) -> bool:
        _ = chat_id, user_id
        return False

    async def close(self) -> None:
        return None

    def _assert_no_active_transaction(self) -> None:
        if (
            self.assert_no_active_transaction
            and self.state is not None
            and self.state.active_transactions > 0
        ):
            raise AssertionError("platform call happened inside an open transaction")


@dataclass
class DeliveryState:
    tasks: dict[uuid.UUID, Any]
    created_payloads: dict[
        tuple[uuid.UUID, Platform, str, str], dict[str, Any]
    ] = field(default_factory=dict)
    created_send_snapshots: dict[
        tuple[uuid.UUID, Platform, str, str, Platform], dict[str, Any]
    ] = field(default_factory=dict)
    pending_rows: list[Any] = field(default_factory=list)
    canonical_event_ids: dict[str, uuid.UUID] = field(default_factory=dict)
    bridge_chats: dict[tuple[uuid.UUID, Platform], Any] = field(default_factory=dict)
    mappings: list[dict[str, Any]] = field(default_factory=list)
    chunk_destination_ids: dict[
        tuple[uuid.UUID, Platform, str, str], list[str]
    ] = field(default_factory=dict)
    attempts: list[dict[str, Any]] = field(default_factory=list)
    dead_letters: list[dict[str, Any]] = field(default_factory=list)
    active_transactions: int = 0
    session_count: int = 0
    lease_renewals: int = 0


class FakeSession:
    def __init__(self, state: DeliveryState) -> None:
        self.state = state

    @asynccontextmanager
    async def begin(self):
        self.state.active_transactions += 1
        try:
            yield self
        finally:
            self.state.active_transactions -= 1


class FakeDatabase:
    def __init__(self, state: DeliveryState) -> None:
        self.state = state

    @asynccontextmanager
    async def session(self):
        self.state.session_count += 1
        yield FakeSession(self.state)


class FakeRepository:
    def __init__(self, session: FakeSession) -> None:
        self.state = session.state

    async def claim_outbox(self, limit: int, lease_seconds: int) -> list[Any]:
        now = datetime.now(UTC)
        ready = [
            task
            for task in self.state.tasks.values()
            if task.status in {TaskStatus.READY, TaskStatus.RETRY_WAIT}
            and task.next_attempt_at <= now
        ]
        ready.sort(key=lambda item: (item.partition_key, item.seq))
        claimed = ready[:limit]
        for task in claimed:
            task.status = TaskStatus.INFLIGHT
            task.attempt_count += 1
            task.inflight_until = now + timedelta(seconds=lease_seconds)
        return claimed

    async def get_outbox_task(self, outbox_id: uuid.UUID) -> Any | None:
        return self.state.tasks.get(outbox_id)

    async def get_created_event_payload(
        self,
        bridge_id: uuid.UUID,
        src_platform: Platform,
        src_chat_id: str,
        src_message_id: str,
    ) -> dict[str, Any] | None:
        return self.state.created_payloads.get(
            (bridge_id, src_platform, src_chat_id, src_message_id)
        )

    async def get_created_send_payload(
        self,
        bridge_id: uuid.UUID,
        src_platform: Platform,
        src_chat_id: str,
        src_message_id: str,
        dst_platform: Platform,
    ) -> dict[str, Any] | None:
        return self.state.created_send_snapshots.get(
            (bridge_id, src_platform, src_chat_id, src_message_id, dst_platform)
        )

    async def renew_outbox_lease(
        self,
        outbox_id: uuid.UUID,
        attempt_count: int,
        lease_seconds: int,
    ) -> bool:
        task = self.state.tasks.get(outbox_id)
        if task is None:
            return False
        if not _matches_attempt(task, attempt_count):
            return False
        task.inflight_until = datetime.now(UTC) + timedelta(seconds=lease_seconds)
        self.state.lease_renewals += 1
        return True

    async def finalize_outbox_success(
        self,
        *,
        outbox_id: uuid.UUID,
        attempt_count: int,
        bridge_id: uuid.UUID,
        dst_platform: Platform,
        dst_chat_id: str,
        dst_message_id: str | None,
        dst_message_ids: list[str] | None,
        src_platform: Platform | None,
        src_chat_id: str | None,
        src_message_id: str | None,
        group_kind: str | None = None,
        src_member_message_ids: list[str] | None = None,
        delivery_state: dict[str, Any] | None = None,
    ) -> bool:
        task = self.state.tasks.get(outbox_id)
        if task is None:
            return False
        if not _matches_attempt(task, attempt_count):
            return False
        effective_dst_message_ids = (
            list(dst_message_ids)
            if dst_message_ids
            else ([dst_message_id] if dst_message_id is not None else [])
        )
        updated_payload = dict(task.task)
        if effective_dst_message_ids:
            updated_payload["dst_message_id"] = effective_dst_message_ids[0]
            updated_payload["dst_message_ids"] = effective_dst_message_ids
        elif dst_message_id is not None:
            updated_payload["dst_message_id"] = dst_message_id
        if delivery_state is not None:
            updated_payload["delivery_state"] = delivery_state
        task.task = updated_payload
        if (
            dst_message_id is not None
            and src_platform is not None
            and src_chat_id is not None
            and src_message_id is not None
        ):
            self.state.mappings.append(
                {
                    "bridge_id": bridge_id,
                    "src_platform": src_platform,
                    "src_chat_id": src_chat_id,
                    "src_message_id": src_message_id,
                    "dst_platform": dst_platform,
                    "dst_chat_id": dst_chat_id,
                    "dst_message_id": dst_message_id,
                }
            )
            self.state.created_send_snapshots[
                (bridge_id, src_platform, src_chat_id, src_message_id, dst_platform)
            ] = dict(updated_payload)
            if group_kind is not None:
                self.state.chunk_destination_ids[
                    (bridge_id, src_platform, src_chat_id, src_message_id)
                ] = effective_dst_message_ids
        task.status = TaskStatus.DONE
        task.inflight_until = None
        self.state.attempts.append(
            {
                "outbox_id": outbox_id,
                "attempt_no": attempt_count,
                "outcome": DeliveryOutcome.SUCCESS,
            }
        )
        return True

    async def finalize_outbox_retry(
        self,
        *,
        outbox_id: uuid.UUID,
        attempt_count: int,
        next_attempt_at: datetime,
        http_status: int | None,
        error_code: str | None,
        error_message: str | None,
    ) -> bool:
        task = self.state.tasks.get(outbox_id)
        if task is None:
            return False
        if not _matches_attempt(task, attempt_count):
            return False
        task.status = TaskStatus.RETRY_WAIT
        task.next_attempt_at = next_attempt_at
        task.inflight_until = None
        self.state.attempts.append(
            {
                "outbox_id": outbox_id,
                "attempt_no": attempt_count,
                "outcome": DeliveryOutcome.RETRY,
                "http_status": http_status,
                "error_code": error_code,
                "error_message": error_message,
            }
        )
        return True

    async def finalize_outbox_dead(
        self,
        *,
        outbox_id: uuid.UUID,
        attempt_count: int,
        bridge_id: uuid.UUID,
        reason: str,
        payload: dict[str, Any],
        http_status: int | None,
        error_code: str | None,
        error_message: str | None,
    ) -> bool:
        task = self.state.tasks.get(outbox_id)
        if task is None:
            return False
        if not _matches_attempt(task, attempt_count):
            return False
        task.status = TaskStatus.DEAD
        task.inflight_until = None
        self.state.dead_letters.append(
            {
                "bridge_id": bridge_id,
                "outbox_id": outbox_id,
                "reason": reason,
                "payload": payload,
                "http_status": http_status,
                "error_code": error_code,
                "error_message": error_message,
            }
        )
        self.state.attempts.append(
            {
                "outbox_id": outbox_id,
                "attempt_no": attempt_count,
                "outcome": DeliveryOutcome.DEAD,
            }
        )
        return True

    async def find_mapping_by_source(
        self,
        bridge_id: uuid.UUID,
        src_platform: Platform,
        src_chat_id: str,
        src_message_id: str,
    ) -> Any | None:
        for mapping in self.state.mappings:
            if (
                mapping["bridge_id"] == bridge_id
                and mapping["src_platform"] == src_platform
                and mapping["src_chat_id"] == src_chat_id
                and mapping["src_message_id"] == src_message_id
            ):
                return SimpleNamespace(**mapping)
        return None

    async def list_destination_message_ids(
        self,
        bridge_id: uuid.UUID,
        src_platform: Platform,
        src_chat_id: str,
        src_message_id: str,
    ) -> list[str]:
        chunk_ids = self.state.chunk_destination_ids.get(
            (bridge_id, src_platform, src_chat_id, src_message_id)
        )
        if chunk_ids:
            return list(chunk_ids)
        for mapping in self.state.mappings:
            if (
                mapping["bridge_id"] == bridge_id
                and mapping["src_platform"] == src_platform
                and mapping["src_chat_id"] == src_chat_id
                and mapping["src_message_id"] == src_message_id
            ):
                return [mapping["dst_message_id"]]
        return []

    async def find_canonical_event_id_by_dedup_key(
        self,
        dedup_key: str,
    ) -> uuid.UUID | None:
        return self.state.canonical_event_ids.get(dedup_key)

    async def find_other_chat(
        self,
        bridge_id: uuid.UUID,
        source_platform: Platform,
    ) -> Any | None:
        return self.state.bridge_chats.get((bridge_id, source_platform))

    async def enqueue_outbox(self, **kwargs: Any) -> uuid.UUID:
        partition = str(kwargs["partition_key"])
        seq = (
            max(
                (
                    task.seq
                    for task in self.state.tasks.values()
                    if task.partition_key == partition
                ),
                default=0,
            )
            + 1
        )
        outbox_id = uuid.uuid4()
        self.state.tasks[outbox_id] = SimpleNamespace(
            outbox_id=outbox_id,
            bridge_id=kwargs["bridge_id"],
            dedup_key=kwargs["dedup_key"],
            src_event_id=kwargs["src_event_id"],
            action=kwargs["action"].value,
            task=kwargs["task"],
            dst_platform=kwargs["dst_platform"],
            partition_key=partition,
            seq=seq,
            status=TaskStatus.READY,
            attempt_count=0,
            next_attempt_at=datetime.now(UTC),
            inflight_until=None,
        )
        return outbox_id

    async def mark_pending_mutation_done(self, pending: Any) -> None:
        pending.status = TaskStatus.DONE

    async def reschedule_pending_mutation(
        self,
        pending: Any,
        *,
        next_attempt_at: datetime,
    ) -> None:
        pending.status = TaskStatus.RETRY_WAIT
        pending.next_attempt_at = next_attempt_at

    async def reset_expired_inflight(self) -> int:
        now = datetime.now(UTC)
        reset = 0
        for task in self.state.tasks.values():
            if task.status == TaskStatus.INFLIGHT and task.inflight_until <= now:
                task.status = TaskStatus.RETRY_WAIT
                task.inflight_until = None
                task.next_attempt_at = now
                reset += 1
        return reset

    async def claim_pending_mutations(self, limit: int) -> list[Any]:
        now = datetime.now(UTC)
        rows = [
            pending
            for pending in self.state.pending_rows
            if pending.status == TaskStatus.RETRY_WAIT
            and pending.next_attempt_at <= now
            and pending.expires_at > now
        ]
        return rows[:limit]

    async def expire_pending_mutations(self) -> int:
        now = datetime.now(UTC)
        expired = 0
        for pending in self.state.pending_rows:
            if pending.status == TaskStatus.RETRY_WAIT and pending.expires_at <= now:
                pending.status = TaskStatus.DEAD
                self.state.dead_letters.append(
                    {
                        "bridge_id": pending.bridge_id,
                        "outbox_id": None,
                        "reason": "missing_mapping_after_3m",
                        "payload": pending.payload,
                    }
                )
                expired += 1
        return expired


def make_worker(
    tmp_path: Path,
    clients: dict[Platform, FakeClient],
    *,
    database: Any = None,
    lease_seconds: int = 60,
) -> DeliveryWorker:
    return DeliveryWorker(
        database=database,  # type: ignore[arg-type]
        clients=cast(dict[Platform, Any], clients),
        stop_event=asyncio.Event(),
        lease_seconds=lease_seconds,
        idle_seconds=0,
        root_dir=tmp_path,
    )


def make_context(
    *,
    action: str,
    bridge_id: uuid.UUID,
    dst_platform: Platform,
    task_payload: dict[str, Any],
    attempt_count: int = 1,
    edit_mode: EditMode = EditMode.TEXT_ONLY,
) -> DeliveryContext:
    src = cast(dict[str, Any], task_payload["src"])
    dst = cast(dict[str, Any], task_payload["dst"])
    return DeliveryContext(
        outbox_id=uuid.uuid4(),
        bridge_id=bridge_id,
        attempt_count=attempt_count,
        action=OutboxAction(action),
        dst_platform=dst_platform,
        dst_chat_id=str(dst["chat_id"]),
        payload=task_payload,
        src_platform=Platform(str(src["platform"])),
        src_chat_id=str(src["chat_id"]),
        src_message_id=str(src["message_id"]),
        src_event_id=uuid.uuid4(),
        dedup_key=f"{bridge_id}:{src['platform']}:{src['chat_id']}:{src['message_id']}:{action}",
        partition_key=f"{bridge_id}:{src['platform']}_to_{dst_platform.value}",
        edit_mode=edit_mode,
    )


def make_task(
    *,
    action: str,
    bridge_id: uuid.UUID,
    dst_platform: Platform,
    task_payload: dict[str, Any],
    outbox_id: uuid.UUID | None = None,
) -> Any:
    src = cast(dict[str, Any], task_payload["src"])
    return SimpleNamespace(
        outbox_id=outbox_id or uuid.uuid4(),
        bridge_id=bridge_id,
        dedup_key=f"{bridge_id}:{src['platform']}:{src['chat_id']}:{src['message_id']}:{action}",
        src_event_id=uuid.uuid4(),
        action=action,
        task=task_payload,
        dst_platform=dst_platform,
        partition_key=f"{bridge_id}:{src['platform']}_to_{dst_platform.value}",
        seq=1,
        status=TaskStatus.READY,
        attempt_count=0,
        next_attempt_at=datetime.now(UTC),
        inflight_until=None,
    )


def make_pending_mutation(
    *,
    bridge_id: uuid.UUID,
    dedup_key: str,
    src_platform: Platform,
    src_chat_id: str,
    src_message_id: str,
    mutation_type: str,
    payload: dict[str, Any],
) -> Any:
    return SimpleNamespace(
        pending_id=uuid.uuid4(),
        bridge_id=bridge_id,
        dedup_key=dedup_key,
        src_platform=src_platform,
        src_chat_id=src_chat_id,
        src_message_id=src_message_id,
        mutation_type=mutation_type,
        payload=payload,
        status=TaskStatus.RETRY_WAIT,
        next_attempt_at=datetime.now(UTC),
        expires_at=datetime.now(UTC) + timedelta(minutes=3),
    )


def media_payload(
    *,
    source_platform: Platform,
    kind: str,
    identity: str,
    presentation: str | None = None,
    source: dict[str, object] | None = None,
    filename: str = "relay.bin",
    mime_type: str = "application/octet-stream",
    sticker_variant: str | None = None,
) -> dict[str, Any]:
    payload = {
        "source_platform": source_platform.value,
        "kind": kind,
        "placeholder": "[photo]" if kind == "image" else f"[{kind}]",
        "filename": filename,
        "mime_type": mime_type,
        "identity": identity,
        "source": source
        or (
            {"file_id": "file-id"}
            if source_platform == Platform.TELEGRAM
            else {"url": "https://example.test/file.bin"}
        ),
    }
    if presentation is not None:
        payload["presentation"] = presentation
    if sticker_variant is not None:
        payload["sticker_variant"] = sticker_variant
    return payload


def _write_file_of_size(path: Path, size: int) -> None:
    with path.open("wb") as handle:
        handle.truncate(size)


class FakeTelegramMessage:
    def __init__(self, message_id: int, kind: str) -> None:
        self.message_id = message_id
        self.kind = kind

    def model_dump(
        self,
        *,
        mode: str,
        by_alias: bool,
        exclude_none: bool,
    ) -> dict[str, object]:
        _ = mode, by_alias, exclude_none
        return {"kind": self.kind}


class DeliveryTelegramBot:
    def __init__(self) -> None:
        self.send_message_calls: list[dict[str, object]] = []
        self.send_animation_calls: list[dict[str, object]] = []
        self.send_photo_calls: list[dict[str, object]] = []
        self.edit_message_caption_calls: list[dict[str, object]] = []
        self.edit_message_text_calls: list[dict[str, object]] = []
        self.edit_message_media_calls: list[dict[str, object]] = []

    async def send_message(self, **kwargs: object) -> FakeTelegramMessage:
        self.send_message_calls.append(dict(kwargs))
        return FakeTelegramMessage(5000, "message")

    async def send_animation(self, **kwargs: object) -> FakeTelegramMessage:
        self.send_animation_calls.append(dict(kwargs))
        return FakeTelegramMessage(5001, "animation")

    async def send_photo(self, **kwargs: object) -> FakeTelegramMessage:
        self.send_photo_calls.append(dict(kwargs))
        return FakeTelegramMessage(5002, "photo")

    async def edit_message_caption(self, **kwargs: object) -> FakeTelegramMessage:
        self.edit_message_caption_calls.append(dict(kwargs))
        return FakeTelegramMessage(5003, "caption")

    async def edit_message_text(self, **kwargs: object) -> FakeTelegramMessage:
        self.edit_message_text_calls.append(dict(kwargs))
        return FakeTelegramMessage(5005, "text")

    async def edit_message_media(self, **kwargs: object) -> FakeTelegramMessage:
        self.edit_message_media_calls.append(dict(kwargs))
        return FakeTelegramMessage(5004, "media")


class OversizeMultiMediaClient(FakeClient):
    async def send_message(
        self,
        chat_id: str,
        text_plain: str,
        *,
        text_html: str | None = None,
        reply_to_message_id: str | None = None,
        media: LocalMediaFile | list[LocalMediaFile] | None = None,
    ) -> SendResult:
        media_items = (
            media
            if isinstance(media, list)
            else ([media] if media is not None else [])
        )
        if len(media_items) > 1:
            raise PlatformDeliveryError(
                "Request Entity Too Large",
                retryable=False,
                code="entity_too_large",
                http_status=413,
            )
        return await super().send_message(
            chat_id,
            text_plain,
            text_html=text_html,
            reply_to_message_id=reply_to_message_id,
            media=media,
        )


def _matches_attempt(task: Any | None, attempt_count: int) -> bool:
    return bool(
        task is not None
        and task.status == TaskStatus.INFLIGHT
        and task.attempt_count == attempt_count
    )


@pytest.mark.asyncio
async def test_delivery_sends_telegram_photo_to_max_as_media(tmp_path: Path):
    telegram = FakeClient("telegram", download_kind=MediaKind.IMAGE)
    max_client = FakeClient("max")
    bridge_id = uuid.uuid4()
    worker = make_worker(
        tmp_path,
        {
            Platform.TELEGRAM: telegram,
            Platform.MAX: max_client,
        },
    )
    context = make_context(
        action="send",
        bridge_id=bridge_id,
        dst_platform=Platform.MAX,
        task_payload={
            "src": {"platform": "telegram", "chat_id": "-100", "message_id": "10"},
            "dst": {"platform": "max", "chat_id": "200"},
            "text": "Alice:",
            "fallback_text": "Alice: [photo]",
            "reply_to_message_id": "999",
            "has_media": True,
            "media_kind": "image",
            "media": media_payload(
                source_platform=Platform.TELEGRAM,
                kind="image",
                identity="telegram:image:id:file-id",
            ),
        },
    )

    result = await worker._call_platform(context)

    assert len(telegram.download_calls) == 1
    assert len(max_client.send_message_calls) == 1
    send_call = max_client.send_message_calls[0]
    assert send_call["text"] == "Alice:"
    assert send_call["reply_to_message_id"] == "999"
    assert send_call["media"] is not None
    assert send_call["media"].kind == MediaKind.IMAGE
    assert not send_call["media"].path.exists()
    assert result.dst_message_id == "max-media-1"


@pytest.mark.asyncio
async def test_delivery_sends_max_photo_video_chunk_to_telegram_as_media_group(
    tmp_path: Path,
):
    telegram = FakeClient("telegram")
    max_client = FakeClient("max", download_kind=MediaKind.IMAGE)
    worker = make_worker(
        tmp_path,
        {
            Platform.TELEGRAM: telegram,
            Platform.MAX: max_client,
        },
    )
    context = make_context(
        action="send",
        bridge_id=uuid.uuid4(),
        dst_platform=Platform.TELEGRAM,
        task_payload={
            "src": {"platform": "max", "chat_id": "300", "message_id": "mid-group"},
            "dst": {"platform": "telegram", "chat_id": "-100"},
            "text": "Alice: album",
            "fallback_text": "Alice: [photo/video group]",
            "has_media": True,
            "media_kind": "photo_video_chunk",
            "group_kind": "photo_video_chunk",
            "group_key": "max:300:mid-group",
            "media": None,
            "media_items": [
                media_payload(
                    source_platform=Platform.MAX,
                    kind="image",
                    identity="max:image:id:photo-1",
                    filename="one.jpg",
                    mime_type="image/jpeg",
                ),
                media_payload(
                    source_platform=Platform.MAX,
                    kind="image",
                    identity="max:image:id:photo-2",
                    filename="two.jpg",
                    mime_type="image/jpeg",
                ),
            ],
        },
    )

    result = await worker._call_platform(context)

    assert len(max_client.download_calls) == 2
    assert len(telegram.send_message_calls) == 1
    send_call = telegram.send_message_calls[0]
    assert isinstance(send_call["media"], list)
    assert len(send_call["media"]) == 2
    assert result.dst_message_id == "telegram-media-1-1"
    assert result.dst_message_ids == ("telegram-media-1-1", "telegram-media-1-2")


@pytest.mark.asyncio
async def test_delivery_splits_max_group_to_telegram_by_total_bytes(tmp_path: Path):
    telegram = FakeClient("telegram")
    media_items = [
        media_payload(
            source_platform=Platform.MAX,
            kind="image",
            identity=f"max:image:id:photo-{index}",
            filename=f"{index}.jpg",
            mime_type="image/jpeg",
        )
        for index in range(1, 11)
    ]
    max_client = FakeClient(
        "max",
        download_kind=MediaKind.IMAGE,
        download_size_by_identity={
            f"max:image:id:photo-{index}": 6 * 1024 * 1024 for index in range(1, 11)
        },
    )
    worker = make_worker(
        tmp_path,
        {
            Platform.TELEGRAM: telegram,
            Platform.MAX: max_client,
        },
    )
    context = make_context(
        action="send",
        bridge_id=uuid.uuid4(),
        dst_platform=Platform.TELEGRAM,
        task_payload={
            "src": {"platform": "max", "chat_id": "300", "message_id": "mid-budget"},
            "dst": {"platform": "telegram", "chat_id": "-100"},
            "text": "Alice: album",
            "fallback_text": "Alice: [photo group]",
            "has_media": True,
            "media_kind": "photo_video_chunk",
            "group_kind": "photo_video_chunk",
            "group_key": "max:300:mid-budget",
            "media": None,
            "media_items": media_items,
        },
    )

    result = await worker._call_platform(context)

    assert len(telegram.send_message_calls) == 2
    first_piece = cast(list[Any], telegram.send_message_calls[0]["media"])
    second_piece = cast(list[Any], telegram.send_message_calls[1]["media"])
    assert len(first_piece) == 8
    assert len(second_piece) == 2
    assert result.dst_message_id == "telegram-media-1-1"
    assert len(result.dst_message_ids) == 10


@pytest.mark.asyncio
async def test_delivery_filters_oversize_max_group_item_for_telegram(tmp_path: Path):
    telegram = FakeClient("telegram")
    max_client = FakeClient(
        "max",
        download_kind=MediaKind.IMAGE,
        download_size_by_identity={
            "max:image:id:photo-1": 8 * 1024 * 1024,
            "max:image:id:photo-2": 12 * 1024 * 1024,
            "max:image:id:photo-3": 8 * 1024 * 1024,
        },
    )
    worker = make_worker(
        tmp_path,
        {
            Platform.TELEGRAM: telegram,
            Platform.MAX: max_client,
        },
    )
    context = make_context(
        action="send",
        bridge_id=uuid.uuid4(),
        dst_platform=Platform.TELEGRAM,
        task_payload={
            "src": {"platform": "max", "chat_id": "300", "message_id": "mid-oversize"},
            "dst": {"platform": "telegram", "chat_id": "-100"},
            "text": "Alice: album",
            "fallback_text": "Alice: [photo group]",
            "has_media": True,
            "media_kind": "photo_video_chunk",
            "group_kind": "photo_video_chunk",
            "group_key": "max:300:mid-oversize",
            "media": None,
            "media_items": [
                media_payload(
                    source_platform=Platform.MAX,
                    kind="image",
                    identity="max:image:id:photo-1",
                    filename="one.jpg",
                    mime_type="image/jpeg",
                ),
                media_payload(
                    source_platform=Platform.MAX,
                    kind="image",
                    identity="max:image:id:photo-2",
                    filename="two.jpg",
                    mime_type="image/jpeg",
                ),
                media_payload(
                    source_platform=Platform.MAX,
                    kind="image",
                    identity="max:image:id:photo-3",
                    filename="three.jpg",
                    mime_type="image/jpeg",
                ),
            ],
        },
    )

    result = await worker._call_platform(context)

    assert len(telegram.send_message_calls) == 2
    assert len(telegram.send_text_calls) == 1
    assert (
        telegram.send_text_calls[0]["text"]
        == "[image unavailable: exceeds Telegram 10 MB upload limit]"
    )
    assert result.dst_message_ids == (
        "telegram-media-1",
        "telegram-text-1",
        "telegram-media-2",
    )


@pytest.mark.asyncio
async def test_delivery_splits_telegram_group_to_max_by_total_bytes(tmp_path: Path):
    telegram = FakeClient(
        "telegram",
        download_kind=MediaKind.VIDEO,
        download_size_by_identity={
            "telegram:video:id:file-1": 20 * 1024 * 1024,
            "telegram:video:id:file-2": 20 * 1024 * 1024,
            "telegram:video:id:file-3": 20 * 1024 * 1024,
        },
    )
    max_client = FakeClient("max")
    worker = make_worker(
        tmp_path,
        {
            Platform.TELEGRAM: telegram,
            Platform.MAX: max_client,
        },
    )
    context = make_context(
        action="send",
        bridge_id=uuid.uuid4(),
        dst_platform=Platform.MAX,
        task_payload={
            "src": {"platform": "telegram", "chat_id": "-100", "message_id": "grp-1"},
            "dst": {"platform": "max", "chat_id": "200"},
            "text": "Alice: album",
            "fallback_text": "Alice: [video group]",
            "has_media": True,
            "media_kind": "photo_video_chunk",
            "group_kind": "photo_video_chunk",
            "group_key": "telegram:-100:grp-1",
            "media": None,
            "media_items": [
                media_payload(
                    source_platform=Platform.TELEGRAM,
                    kind="video",
                    identity="telegram:video:id:file-1",
                    filename="one.mp4",
                    mime_type="video/mp4",
                ),
                media_payload(
                    source_platform=Platform.TELEGRAM,
                    kind="video",
                    identity="telegram:video:id:file-2",
                    filename="two.mp4",
                    mime_type="video/mp4",
                ),
                media_payload(
                    source_platform=Platform.TELEGRAM,
                    kind="video",
                    identity="telegram:video:id:file-3",
                    filename="three.mp4",
                    mime_type="video/mp4",
                ),
            ],
        },
    )

    result = await worker._call_platform(context)

    assert len(max_client.send_message_calls) == 2
    first_piece = cast(list[Any], max_client.send_message_calls[0]["media"])
    second_piece = max_client.send_message_calls[1]["media"]
    assert len(first_piece) == 2
    assert cast(Any, second_piece).kind == MediaKind.VIDEO
    assert result.dst_message_ids == ("max-media-1", "max-media-2")


@pytest.mark.asyncio
async def test_delivery_filters_oversize_telegram_group_item_for_max(tmp_path: Path):
    telegram = FakeClient(
        "telegram",
        download_kind=MediaKind.VIDEO,
        download_size_by_identity={
            "telegram:video:id:file-1": 45 * 1024 * 1024,
            "telegram:video:id:file-2": 51 * 1024 * 1024,
            "telegram:video:id:file-3": 45 * 1024 * 1024,
        },
    )
    max_client = FakeClient("max")
    worker = make_worker(
        tmp_path,
        {
            Platform.TELEGRAM: telegram,
            Platform.MAX: max_client,
        },
    )
    context = make_context(
        action="send",
        bridge_id=uuid.uuid4(),
        dst_platform=Platform.MAX,
        task_payload={
            "src": {"platform": "telegram", "chat_id": "-100", "message_id": "grp-max"},
            "dst": {"platform": "max", "chat_id": "200"},
            "text": "Alice: album",
            "fallback_text": "Alice: [video group]",
            "has_media": True,
            "media_kind": "photo_video_chunk",
            "group_kind": "photo_video_chunk",
            "group_key": "telegram:-100:grp-max",
            "media": None,
            "media_items": [
                media_payload(
                    source_platform=Platform.TELEGRAM,
                    kind="video",
                    identity="telegram:video:id:file-1",
                    filename="one.mp4",
                    mime_type="video/mp4",
                ),
                media_payload(
                    source_platform=Platform.TELEGRAM,
                    kind="video",
                    identity="telegram:video:id:file-2",
                    filename="two.mp4",
                    mime_type="video/mp4",
                ),
                media_payload(
                    source_platform=Platform.TELEGRAM,
                    kind="video",
                    identity="telegram:video:id:file-3",
                    filename="three.mp4",
                    mime_type="video/mp4",
                ),
            ],
        },
    )

    result = await worker._call_platform(context)

    assert len(max_client.send_message_calls) == 2
    assert len(max_client.send_text_calls) == 1
    assert (
        max_client.send_text_calls[0]["text"]
        == "[video unavailable: exceeds MAX 50 MB upload limit]"
    )
    assert result.dst_message_ids == (
        "max-media-1",
        "max-text-1",
        "max-media-2",
    )


@pytest.mark.asyncio
async def test_delivery_sends_max_document_to_telegram_as_media(tmp_path: Path):
    telegram = FakeClient("telegram")
    max_client = FakeClient("max", download_kind=MediaKind.DOCUMENT)
    worker = make_worker(
        tmp_path,
        {
            Platform.TELEGRAM: telegram,
            Platform.MAX: max_client,
        },
    )
    context = make_context(
        action="send",
        bridge_id=uuid.uuid4(),
        dst_platform=Platform.TELEGRAM,
        task_payload={
            "src": {"platform": "max", "chat_id": "300", "message_id": "mid-1"},
            "dst": {"platform": "telegram", "chat_id": "-100"},
            "text": "Alice: report",
            "fallback_text": "Alice: report",
            "has_media": True,
            "media_kind": "document",
            "media": media_payload(
                source_platform=Platform.MAX,
                kind="document",
                identity="max:document:path:source",
            ),
        },
    )

    await worker._call_platform(context)

    assert len(max_client.download_calls) == 1
    assert len(telegram.send_message_calls) == 1
    assert telegram.send_message_calls[0]["media"].kind == MediaKind.DOCUMENT


@pytest.mark.asyncio
async def test_delivery_falls_back_for_oversize_max_photo_to_telegram(tmp_path: Path):
    telegram = FakeClient("telegram")
    max_client = FakeClient(
        "max",
        download_kind=MediaKind.IMAGE,
        download_size_by_identity={"max:image:id:photo_id:999": 11 * 1024 * 1024},
    )
    worker = make_worker(
        tmp_path,
        {
            Platform.TELEGRAM: telegram,
            Platform.MAX: max_client,
        },
    )
    context = make_context(
        action="send",
        bridge_id=uuid.uuid4(),
        dst_platform=Platform.TELEGRAM,
        task_payload={
            "src": {"platform": "max", "chat_id": "300", "message_id": "mid-big-photo"},
            "dst": {"platform": "telegram", "chat_id": "-100"},
            "text": "Alice: caption",
            "fallback_text": "Alice: [photo]",
            "has_media": True,
            "media_kind": "image",
            "media": media_payload(
                source_platform=Platform.MAX,
                kind="image",
                identity="max:image:id:photo_id:999",
                filename="big.jpg",
                mime_type="image/jpeg",
            ),
        },
    )

    result = await worker._call_platform(context)

    assert telegram.send_message_calls == []
    assert len(telegram.send_text_calls) == 1
    assert (
        telegram.send_text_calls[0]["text"]
        == "Alice: caption\n[image unavailable: exceeds Telegram 10 MB upload limit]"
    )
    assert result.dst_message_id == "telegram-text-1"


@pytest.mark.asyncio
async def test_delivery_falls_back_for_oversize_telegram_video_to_max_without_retry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    task = make_task(
        action="send",
        bridge_id=uuid.uuid4(),
        dst_platform=Platform.MAX,
        task_payload={
            "src": {"platform": "telegram", "chat_id": "-100", "message_id": "big-video"},
            "dst": {"platform": "max", "chat_id": "200"},
            "text": "Alice: caption",
            "fallback_text": "Alice: [video]",
            "has_media": True,
            "media_kind": "video",
            "media": media_payload(
                source_platform=Platform.TELEGRAM,
                kind="video",
                identity="telegram:video:id:big-video",
                filename="big.mp4",
                mime_type="video/mp4",
            ),
        },
    )
    state = DeliveryState(tasks={task.outbox_id: task})
    database = FakeDatabase(state)
    telegram = FakeClient(
        "telegram",
        state=state,
        download_kind=MediaKind.VIDEO,
        download_size_by_identity={"telegram:video:id:big-video": 51 * 1024 * 1024},
    )
    max_client = FakeClient("max", state=state)
    worker = make_worker(
        tmp_path,
        {
            Platform.TELEGRAM: telegram,
            Platform.MAX: max_client,
        },
        database=database,
    )
    monkeypatch.setattr("maxogram.workers.delivery.Repository", FakeRepository)

    processed = await worker.run_once()

    assert processed == 1
    assert max_client.send_message_calls == []
    assert len(max_client.send_text_calls) == 1
    assert state.tasks[task.outbox_id].status == TaskStatus.DONE
    assert state.attempts == [
        {
            "outbox_id": task.outbox_id,
            "attempt_no": 1,
            "outcome": DeliveryOutcome.SUCCESS,
        }
    ]


@pytest.mark.asyncio
async def test_delivery_sends_telegram_gif_animation_to_max_as_image_media(
    tmp_path: Path,
):
    telegram = FakeClient("telegram", download_kind=MediaKind.IMAGE)
    max_client = FakeClient("max")
    worker = make_worker(
        tmp_path,
        {
            Platform.TELEGRAM: telegram,
            Platform.MAX: max_client,
        },
    )
    context = make_context(
        action="send",
        bridge_id=uuid.uuid4(),
        dst_platform=Platform.MAX,
        task_payload={
            "src": {"platform": "telegram", "chat_id": "-100", "message_id": "gif-1"},
            "dst": {"platform": "max", "chat_id": "200"},
            "text": "Alice:",
            "fallback_text": "Alice: [gif]",
            "has_media": True,
            "media_kind": "image",
            "media": media_payload(
                source_platform=Platform.TELEGRAM,
                kind="image",
                identity="telegram:image:id:gif-id",
                presentation="animation",
            ),
        },
    )

    await worker._call_platform(context)

    assert len(max_client.send_message_calls) == 1
    assert max_client.send_message_calls[0]["media"].kind == MediaKind.IMAGE
    assert (
        max_client.send_message_calls[0]["media"].presentation
        == MediaPresentation.ANIMATION
    )


@pytest.mark.asyncio
async def test_delivery_sends_telegram_mp4_animation_to_max_as_video_media(
    tmp_path: Path,
):
    telegram = FakeClient("telegram", download_kind=MediaKind.VIDEO)
    max_client = FakeClient("max")
    worker = make_worker(
        tmp_path,
        {
            Platform.TELEGRAM: telegram,
            Platform.MAX: max_client,
        },
    )
    context = make_context(
        action="send",
        bridge_id=uuid.uuid4(),
        dst_platform=Platform.MAX,
        task_payload={
            "src": {"platform": "telegram", "chat_id": "-100", "message_id": "anim-1"},
            "dst": {"platform": "max", "chat_id": "200"},
            "text": "Alice:",
            "fallback_text": "Alice: [animation]",
            "has_media": True,
            "media_kind": "video",
            "media": media_payload(
                source_platform=Platform.TELEGRAM,
                kind="video",
                identity="telegram:video:id:anim-id",
                presentation="animation",
            ),
        },
    )

    await worker._call_platform(context)

    assert len(max_client.send_message_calls) == 1
    assert max_client.send_message_calls[0]["media"].kind == MediaKind.VIDEO
    assert (
        max_client.send_message_calls[0]["media"].presentation
        == MediaPresentation.ANIMATION
    )


@pytest.mark.asyncio
async def test_delivery_sends_telegram_video_note_to_max_as_video_media(
    tmp_path: Path,
):
    telegram = FakeClient("telegram", download_kind=MediaKind.VIDEO)
    max_client = FakeClient("max")
    worker = make_worker(
        tmp_path,
        {
            Platform.TELEGRAM: telegram,
            Platform.MAX: max_client,
        },
    )
    context = make_context(
        action="send",
        bridge_id=uuid.uuid4(),
        dst_platform=Platform.MAX,
        task_payload={
            "src": {
                "platform": "telegram",
                "chat_id": "-100",
                "message_id": "video-note-1",
            },
            "dst": {"platform": "max", "chat_id": "200"},
            "text": "Alice:",
            "fallback_text": "Alice: [video note]",
            "has_media": True,
            "media_kind": "video",
            "media": media_payload(
                source_platform=Platform.TELEGRAM,
                kind="video",
                identity="telegram:video:id:video-note-id",
                source={"file_id": "video-note-id"},
            ),
        },
    )

    await worker._call_platform(context)

    assert len(telegram.download_calls) == 1
    assert len(max_client.send_message_calls) == 1
    assert max_client.send_message_calls[0]["text"] == "Alice:"
    assert max_client.send_message_calls[0]["media"] is not None
    assert max_client.send_message_calls[0]["media"].kind == MediaKind.VIDEO


@pytest.mark.asyncio
async def test_delivery_sends_max_opaque_gif_to_telegram_via_animation_path(
    tmp_path: Path,
):
    class LateGifMaxClient(FakeClient):
        async def download_media(
            self,
            media: dict[str, object],
            destination_dir: Path,
        ) -> LocalMediaFile | None:
            self.download_calls.append(media)
            destination = destination_dir / f"{uuid.uuid4()}-opaque"
            await asyncio.to_thread(destination.write_bytes, b"GIF89a-test")
            return LocalMediaFile(
                kind=MediaKind.IMAGE,
                path=destination,
                filename="opaque.gif",
                mime_type="image/gif",
                presentation=MediaPresentation.ANIMATION,
            )

    telegram_bot = DeliveryTelegramBot()
    telegram = object.__new__(TelegramClient)
    telegram.bot = cast(Any, telegram_bot)
    max_client = LateGifMaxClient("max")
    worker = make_worker(
        tmp_path,
        {
            Platform.TELEGRAM: cast(Any, telegram),
            Platform.MAX: max_client,
        },
    )
    context = make_context(
        action="send",
        bridge_id=uuid.uuid4(),
        dst_platform=Platform.TELEGRAM,
        task_payload={
            "src": {"platform": "max", "chat_id": "300", "message_id": "mid-gif"},
            "dst": {"platform": "telegram", "chat_id": "-100"},
            "text": "Alice:",
            "fallback_text": "Alice: [photo]",
            "has_media": True,
            "media_kind": "image",
            "media": media_payload(
                source_platform=Platform.MAX,
                kind="image",
                identity="max:image:path:opaque",
            ),
        },
    )

    result = await worker._call_platform(context)

    assert len(max_client.download_calls) == 1
    assert len(telegram_bot.send_animation_calls) == 1
    assert telegram_bot.send_photo_calls == []
    assert telegram_bot.send_animation_calls[0]["caption"] == "Alice:"
    assert result.dst_message_id == "5001"


@pytest.mark.asyncio
async def test_delivery_falls_back_to_text_when_media_is_not_materialized(
    tmp_path: Path,
):
    telegram = FakeClient("telegram", download_kind=None)
    max_client = FakeClient("max")
    worker = make_worker(
        tmp_path,
        {
            Platform.TELEGRAM: telegram,
            Platform.MAX: max_client,
        },
    )
    context = make_context(
        action="send",
        bridge_id=uuid.uuid4(),
        dst_platform=Platform.MAX,
        task_payload={
            "src": {"platform": "telegram", "chat_id": "-100", "message_id": "10"},
            "dst": {"platform": "max", "chat_id": "200"},
            "text": "Alice:",
            "fallback_text": "Alice: [photo]",
            "has_media": True,
            "media_kind": "image",
            "media": media_payload(
                source_platform=Platform.TELEGRAM,
                kind="image",
                identity="telegram:image:id:file-id",
            ),
        },
    )

    await worker._call_platform(context)

    assert max_client.send_message_calls == []
    assert len(max_client.send_text_calls) == 1
    assert max_client.send_text_calls[0]["text"] == "Alice: [photo]"


@pytest.mark.asyncio
async def test_materialize_telegram_animated_sticker_converts_and_caches_gif(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    telegram = FakeClient("telegram", download_kind=MediaKind.IMAGE)
    media = media_payload(
        source_platform=Platform.TELEGRAM,
        kind="image",
        identity="telegram:image:id:animated-sticker",
        presentation="animation",
        filename="sticker.tgs",
        mime_type="application/x-tgsticker",
        sticker_variant="animated_tgs",
    )

    async def fake_convert(source_path: Path, cache_path: Path) -> None:
        assert source_path.suffix == ".tgs"
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        await asyncio.to_thread(cache_path.write_bytes, b"GIF89a-test")

    monkeypatch.setattr(
        "maxogram.services.relay._convert_tgs_to_cached_gif",
        fake_convert,
    )

    local_media = await materialize_media(
        clients={Platform.TELEGRAM: cast(Any, telegram)},
        media=media,
        root_dir=tmp_path,
    )

    assert local_media is not None
    assert len(telegram.download_calls) == 1
    assert local_media.path.parent == animated_sticker_cache_dir(tmp_path)
    assert local_media.path.exists()
    assert local_media.filename == "sticker.gif"
    assert local_media.mime_type == "image/gif"
    assert local_media.sticker_variant == "animated_tgs"
    assert local_media.presentation == MediaPresentation.ANIMATION
    assert local_media.cleanup_after_use is False
    cleanup_local_media(local_media)
    assert local_media.path.exists()


@pytest.mark.asyncio
async def test_materialize_telegram_animated_sticker_uses_cache_without_download(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    telegram = FakeClient("telegram", download_kind=MediaKind.IMAGE)
    media = media_payload(
        source_platform=Platform.TELEGRAM,
        kind="image",
        identity="telegram:image:id:animated-sticker",
        presentation="animation",
        filename="sticker.tgs",
        mime_type="application/x-tgsticker",
        sticker_variant="animated_tgs",
    )

    async def fake_convert(source_path: Path, cache_path: Path) -> None:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        await asyncio.to_thread(cache_path.write_bytes, b"GIF89a-test")

    monkeypatch.setattr(
        "maxogram.services.relay._convert_tgs_to_cached_gif",
        fake_convert,
    )

    first_media = await materialize_media(
        clients={Platform.TELEGRAM: cast(Any, telegram)},
        media=media,
        root_dir=tmp_path,
    )
    assert first_media is not None
    assert len(telegram.download_calls) == 1

    stale_timestamp = datetime.now(UTC) - timedelta(days=10)
    await asyncio.to_thread(first_media.path.touch)
    await asyncio.to_thread(
        os.utime,
        first_media.path,
        (stale_timestamp.timestamp(), stale_timestamp.timestamp()),
    )

    async def fail_convert(source_path: Path, cache_path: Path) -> None:
        raise AssertionError("cache hit should skip conversion")

    monkeypatch.setattr(
        "maxogram.services.relay._convert_tgs_to_cached_gif",
        fail_convert,
    )
    telegram.download_calls.clear()

    second_media = await materialize_media(
        clients={Platform.TELEGRAM: cast(Any, telegram)},
        media=media,
        root_dir=tmp_path,
    )

    assert second_media is not None
    assert telegram.download_calls == []
    assert second_media.path == first_media.path
    assert second_media.path.stat().st_mtime > stale_timestamp.timestamp()


@pytest.mark.asyncio
async def test_delivery_falls_back_to_text_when_animated_sticker_conversion_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    telegram = FakeClient("telegram", download_kind=MediaKind.IMAGE)
    max_client = FakeClient("max")
    worker = make_worker(
        tmp_path,
        {
            Platform.TELEGRAM: telegram,
            Platform.MAX: max_client,
        },
    )
    context = make_context(
        action="send",
        bridge_id=uuid.uuid4(),
        dst_platform=Platform.MAX,
        task_payload={
            "src": {"platform": "telegram", "chat_id": "-100", "message_id": "11"},
            "dst": {"platform": "max", "chat_id": "200"},
            "text": "Alice:",
            "fallback_text": "Alice: [animated sticker]",
            "has_media": True,
            "media_kind": "image",
            "media": media_payload(
                source_platform=Platform.TELEGRAM,
                kind="image",
                identity="telegram:image:id:animated-sticker",
                presentation="animation",
                filename="sticker.tgs",
                mime_type="application/x-tgsticker",
                sticker_variant="animated_tgs",
            ),
        },
    )

    async def fail_convert(source_path: Path, cache_path: Path) -> None:
        raise RuntimeError("conversion boom")

    monkeypatch.setattr(
        "maxogram.services.relay._convert_tgs_to_cached_gif",
        fail_convert,
    )

    await worker._call_platform(context)

    assert len(telegram.download_calls) == 1
    assert max_client.send_message_calls == []
    assert len(max_client.send_text_calls) == 1
    assert max_client.send_text_calls[0]["text"] == "Alice: [animated sticker]"


@pytest.mark.asyncio
async def test_delivery_enqueues_follow_up_text_after_successful_telegram_audio_send(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    bridge_id = uuid.uuid4()
    task = make_task(
        action="send",
        bridge_id=bridge_id,
        dst_platform=Platform.MAX,
        task_payload={
            "src": {"platform": "telegram", "chat_id": "-100", "message_id": "audio-1"},
            "dst": {"platform": "max", "chat_id": "200"},
            "text_plain": "",
            "text_html": None,
            "fallback_text": "🔊 Alice\ncaption",
            "post_send_text_plain": "🔊 Alice\ncaption",
            "post_send_text_html": "🔊 Alice\n<b>caption</b>",
            "has_media": True,
            "media_kind": "audio",
            "media": media_payload(
                source_platform=Platform.TELEGRAM,
                kind="audio",
                identity="telegram:audio:id:file-id",
                source={"file_id": "file-id"},
            ),
        },
    )
    state = DeliveryState(tasks={task.outbox_id: task})
    database = FakeDatabase(state)
    telegram = FakeClient("telegram", download_kind=MediaKind.AUDIO)
    max_client = FakeClient("max")
    worker = make_worker(
        tmp_path,
        {
            Platform.TELEGRAM: telegram,
            Platform.MAX: max_client,
        },
        database=database,
    )
    monkeypatch.setattr("maxogram.workers.delivery.Repository", FakeRepository)

    processed_primary = await worker.run_once()

    assert processed_primary == 1
    assert len(max_client.send_message_calls) == 1
    assert max_client.send_message_calls[0]["text"] == ""
    assert len(state.mappings) == 1
    assert state.mappings[0]["dst_message_id"] == "max-media-1"
    assert len(state.tasks) == 2

    follow_up_tasks = [
        queued_task
        for queued_task in state.tasks.values()
        if queued_task.outbox_id != task.outbox_id
    ]
    assert len(follow_up_tasks) == 1
    follow_up_task = follow_up_tasks[0]
    assert follow_up_task.task["text_plain"] == "🔊 Alice\ncaption"
    assert follow_up_task.task["text_html"] == "🔊 Alice\n<b>caption</b>"
    assert "reply_to_message_id" not in follow_up_task.task
    assert follow_up_task.task["creates_mapping"] is False
    assert follow_up_task.status == TaskStatus.READY

    processed_follow_up = await worker.run_once()

    assert processed_follow_up == 1
    assert len(max_client.send_text_calls) == 1
    assert max_client.send_text_calls[0]["text"] == "🔊 Alice\ncaption"
    assert max_client.send_text_calls[0]["reply_to_message_id"] is None
    assert len(state.mappings) == 1
    assert [attempt["outcome"] for attempt in state.attempts] == [
        DeliveryOutcome.SUCCESS,
        DeliveryOutcome.SUCCESS,
    ]


@pytest.mark.asyncio
async def test_delivery_does_not_enqueue_follow_up_text_when_audio_falls_back_to_text(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    bridge_id = uuid.uuid4()
    task = make_task(
        action="send",
        bridge_id=bridge_id,
        dst_platform=Platform.MAX,
        task_payload={
            "src": {"platform": "telegram", "chat_id": "-100", "message_id": "audio-2"},
            "dst": {"platform": "max", "chat_id": "200"},
            "text_plain": "",
            "text_html": None,
            "fallback_text": "🔊 Alice",
            "post_send_text_plain": "🔊 Alice",
            "has_media": True,
            "media_kind": "voice",
            "media": media_payload(
                source_platform=Platform.TELEGRAM,
                kind="voice",
                identity="telegram:voice:id:file-id",
                source={"file_id": "file-id"},
            ),
        },
    )
    state = DeliveryState(tasks={task.outbox_id: task})
    database = FakeDatabase(state)
    telegram = FakeClient("telegram", download_kind=None)
    max_client = FakeClient("max")
    worker = make_worker(
        tmp_path,
        {
            Platform.TELEGRAM: telegram,
            Platform.MAX: max_client,
        },
        database=database,
    )
    monkeypatch.setattr("maxogram.workers.delivery.Repository", FakeRepository)

    processed = await worker.run_once()

    assert processed == 1
    assert max_client.send_message_calls == []
    assert len(max_client.send_text_calls) == 1
    assert max_client.send_text_calls[0]["text"] == "🔊 Alice"
    assert len(state.tasks) == 1
    assert len(state.mappings) == 1


@pytest.mark.asyncio
async def test_delivery_follow_up_text_retries_without_creating_extra_mapping(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    bridge_id = uuid.uuid4()
    task = make_task(
        action="send",
        bridge_id=bridge_id,
        dst_platform=Platform.MAX,
        task_payload={
            "src": {"platform": "telegram", "chat_id": "-100", "message_id": "audio-3"},
            "dst": {"platform": "max", "chat_id": "200"},
            "text_plain": "",
            "text_html": None,
            "fallback_text": "🔊 Alice\ncaption",
            "post_send_text_plain": "🔊 Alice\ncaption",
            "has_media": True,
            "media_kind": "audio",
            "media": media_payload(
                source_platform=Platform.TELEGRAM,
                kind="audio",
                identity="telegram:audio:id:file-id",
                source={"file_id": "file-id"},
            ),
        },
    )
    state = DeliveryState(tasks={task.outbox_id: task})
    database = FakeDatabase(state)
    telegram = FakeClient("telegram", download_kind=MediaKind.AUDIO)

    def fail_follow_up() -> None:
        raise PlatformDeliveryError(
            "temporary follow-up failure",
            retryable=True,
            code="temporary_follow_up_failure",
        )

    max_client = FakeClient("max", on_send_text=fail_follow_up)
    worker = make_worker(
        tmp_path,
        {
            Platform.TELEGRAM: telegram,
            Platform.MAX: max_client,
        },
        database=database,
    )
    monkeypatch.setattr("maxogram.workers.delivery.Repository", FakeRepository)

    processed_primary = await worker.run_once()

    assert processed_primary == 1
    assert len(state.mappings) == 1

    follow_up_task = next(
        queued_task
        for queued_task in state.tasks.values()
        if queued_task.outbox_id != task.outbox_id
    )

    processed_follow_up = await worker.run_once()

    assert processed_follow_up == 1
    assert follow_up_task.status == TaskStatus.RETRY_WAIT
    assert len(state.mappings) == 1
    assert [attempt["outcome"] for attempt in state.attempts] == [
        DeliveryOutcome.SUCCESS,
        DeliveryOutcome.RETRY,
    ]


@pytest.mark.asyncio
async def test_delivery_uses_plain_text_path_for_text_only_messages(tmp_path: Path):
    telegram = FakeClient("telegram")
    max_client = FakeClient("max")
    worker = make_worker(
        tmp_path,
        {
            Platform.TELEGRAM: telegram,
            Platform.MAX: max_client,
        },
    )
    context = make_context(
        action="send",
        bridge_id=uuid.uuid4(),
        dst_platform=Platform.TELEGRAM,
        task_payload={
            "src": {"platform": "max", "chat_id": "300", "message_id": "mid-2"},
            "dst": {"platform": "telegram", "chat_id": "-100"},
            "text": "Alice: hello",
            "fallback_text": "Alice: hello",
            "has_media": False,
            "media_kind": None,
            "media": None,
        },
    )

    await worker._call_platform(context)

    assert len(telegram.send_text_calls) == 1
    assert telegram.send_message_calls == []


@pytest.mark.asyncio
async def test_delivery_passes_html_for_text_only_messages(tmp_path: Path):
    telegram = FakeClient("telegram")
    max_client = FakeClient("max")
    worker = make_worker(
        tmp_path,
        {
            Platform.TELEGRAM: telegram,
            Platform.MAX: max_client,
        },
    )
    context = make_context(
        action="send",
        bridge_id=uuid.uuid4(),
        dst_platform=Platform.TELEGRAM,
        task_payload={
            "src": {"platform": "max", "chat_id": "300", "message_id": "mid-2"},
            "dst": {"platform": "telegram", "chat_id": "-100"},
            "text_plain": "Alice: hello",
            "text_html": "Alice: <i>hello</i>",
            "fallback_text": "Alice: hello",
            "has_media": False,
            "media_kind": None,
            "media": None,
        },
    )

    await worker._call_platform(context)

    assert len(telegram.send_text_calls) == 1
    assert telegram.send_text_calls[0]["text"] == "Alice: hello"
    assert telegram.send_text_calls[0]["text_html"] == "Alice: <i>hello</i>"


@pytest.mark.asyncio
async def test_delivery_edits_media_caption_without_replacing_attachment(
    tmp_path: Path,
):
    telegram = FakeClient("telegram")
    max_client = FakeClient("max")
    bridge_id = uuid.uuid4()
    worker = make_worker(
        tmp_path,
        {
            Platform.TELEGRAM: telegram,
            Platform.MAX: max_client,
        },
    )
    context = make_context(
        action="edit",
        bridge_id=bridge_id,
        dst_platform=Platform.MAX,
        task_payload={
            "src": {"platform": "telegram", "chat_id": "-100", "message_id": "10"},
            "dst": {"platform": "max", "chat_id": "200"},
            "dst_message_id": "max-msg-1",
            "text": "Alice: updated",
            "has_media": True,
            "media": media_payload(
                source_platform=Platform.TELEGRAM,
                kind="image",
                identity="telegram:image:id:file-id",
            ),
        },
        edit_mode=EditMode.CAPTION_ONLY_SAME_MEDIA,
    )

    await worker._call_platform(context)

    assert len(max_client.edit_calls) == 1
    assert max_client.edit_calls[0]["has_media"] is True
    assert max_client.edit_calls[0]["text"] == "Alice: updated"
    assert max_client.edit_calls[0]["replacement_media"] is None


@pytest.mark.asyncio
async def test_delivery_passes_html_for_caption_only_media_edits(tmp_path: Path):
    telegram = FakeClient("telegram")
    max_client = FakeClient("max")
    worker = make_worker(
        tmp_path,
        {
            Platform.TELEGRAM: telegram,
            Platform.MAX: max_client,
        },
    )
    context = make_context(
        action="edit",
        bridge_id=uuid.uuid4(),
        dst_platform=Platform.MAX,
        task_payload={
            "src": {"platform": "telegram", "chat_id": "-100", "message_id": "10"},
            "dst": {"platform": "max", "chat_id": "200"},
            "dst_message_id": "max-msg-1",
            "text_plain": "Alice: updated",
            "text_html": "Alice: <b>updated</b>",
            "has_media": True,
            "media": media_payload(
                source_platform=Platform.TELEGRAM,
                kind="image",
                identity="telegram:image:id:file-id",
            ),
        },
        edit_mode=EditMode.CAPTION_ONLY_SAME_MEDIA,
    )

    await worker._call_platform(context)

    assert len(max_client.edit_calls) == 1
    assert max_client.edit_calls[0]["text"] == "Alice: updated"
    assert max_client.edit_calls[0]["text_html"] == "Alice: <b>updated</b>"


@pytest.mark.asyncio
async def test_delivery_edits_max_photo_caption_on_telegram_without_replacing_media(
    tmp_path: Path,
):
    telegram_bot = DeliveryTelegramBot()
    telegram = object.__new__(TelegramClient)
    telegram.bot = cast(Any, telegram_bot)
    max_client = FakeClient("max")
    worker = make_worker(
        tmp_path,
        {
            Platform.TELEGRAM: cast(Any, telegram),
            Platform.MAX: max_client,
        },
    )
    context = make_context(
        action="edit",
        bridge_id=uuid.uuid4(),
        dst_platform=Platform.TELEGRAM,
        task_payload={
            "src": {"platform": "max", "chat_id": "300", "message_id": "mid-1"},
            "dst": {"platform": "telegram", "chat_id": "-100"},
            "dst_message_id": "55",
            "text": "Alice: updated",
            "has_media": True,
            "media": media_payload(
                source_platform=Platform.MAX,
                kind="image",
                identity="max:image:id:photo_id:123",
            ),
        },
        edit_mode=EditMode.CAPTION_ONLY_SAME_MEDIA,
    )

    await worker._call_platform(context)

    assert len(telegram_bot.edit_message_caption_calls) == 1
    assert telegram_bot.edit_message_caption_calls[0]["caption"] == "Alice: updated"
    assert telegram_bot.edit_message_media_calls == []


@pytest.mark.asyncio
async def test_delivery_replaces_media_on_max_edit(tmp_path: Path):
    telegram = FakeClient("telegram", download_kind=MediaKind.IMAGE)
    max_client = FakeClient("max")
    bridge_id = uuid.uuid4()
    worker = make_worker(
        tmp_path,
        {
            Platform.TELEGRAM: telegram,
            Platform.MAX: max_client,
        },
    )
    context = make_context(
        action="edit",
        bridge_id=bridge_id,
        dst_platform=Platform.MAX,
        task_payload={
            "src": {"platform": "telegram", "chat_id": "-100", "message_id": "10"},
            "dst": {"platform": "max", "chat_id": "200"},
            "dst_message_id": "max-msg-1",
            "text": "Alice: updated",
            "has_media": True,
            "media": media_payload(
                source_platform=Platform.TELEGRAM,
                kind="image",
                identity="telegram:image:id:new-file",
            ),
        },
        edit_mode=EditMode.REPLACE_MEDIA,
    )

    await worker._call_platform(context)

    assert len(telegram.download_calls) == 1
    assert len(max_client.edit_calls) == 1
    assert max_client.edit_calls[0]["replacement_media"] is not None
    assert max_client.edit_calls[0]["replacement_media"].kind == MediaKind.IMAGE
    assert not max_client.edit_calls[0]["replacement_media"].path.exists()


@pytest.mark.asyncio
async def test_delivery_replaces_media_on_telegram_edit(tmp_path: Path):
    telegram_bot = DeliveryTelegramBot()
    telegram = object.__new__(TelegramClient)
    telegram.bot = cast(Any, telegram_bot)
    max_client = FakeClient("max", download_kind=MediaKind.IMAGE)
    worker = make_worker(
        tmp_path,
        {
            Platform.TELEGRAM: cast(Any, telegram),
            Platform.MAX: max_client,
        },
    )
    context = make_context(
        action="edit",
        bridge_id=uuid.uuid4(),
        dst_platform=Platform.TELEGRAM,
        task_payload={
            "src": {"platform": "max", "chat_id": "300", "message_id": "mid-1"},
            "dst": {"platform": "telegram", "chat_id": "-100"},
            "dst_message_id": "55",
            "text": "Alice: updated",
            "has_media": True,
            "media": media_payload(
                source_platform=Platform.MAX,
                kind="image",
                identity="max:image:id:photo_id:456",
            ),
        },
        edit_mode=EditMode.REPLACE_MEDIA,
    )

    await worker._call_platform(context)

    assert len(max_client.download_calls) == 1
    assert len(telegram_bot.edit_message_media_calls) == 1
    input_media = cast(Any, telegram_bot.edit_message_media_calls[0]["media"])
    assert input_media.__class__.__name__ == "InputMediaPhoto"
    assert input_media.caption == "Alice: updated"


@pytest.mark.asyncio
async def test_delivery_recreates_telegram_media_group_on_group_edit(
    tmp_path: Path,
):
    telegram = FakeClient("telegram")
    max_client = FakeClient("max", download_kind=MediaKind.IMAGE)
    worker = make_worker(
        tmp_path,
        {
            Platform.TELEGRAM: telegram,
            Platform.MAX: max_client,
        },
    )
    context = make_context(
        action="edit",
        bridge_id=uuid.uuid4(),
        dst_platform=Platform.TELEGRAM,
        task_payload={
            "src": {"platform": "max", "chat_id": "300", "message_id": "mid-group"},
            "dst": {"platform": "telegram", "chat_id": "-100"},
            "dst_message_id": "old-1",
            "dst_message_ids": ["old-1", "old-2"],
            "text": "Alice: updated album",
            "has_media": True,
            "media": None,
            "group_kind": "photo_video_chunk",
            "group_key": "max:300:mid-group",
            "media_items": [
                media_payload(
                    source_platform=Platform.MAX,
                    kind="image",
                    identity="max:image:id:photo-1",
                    filename="one.jpg",
                    mime_type="image/jpeg",
                ),
                media_payload(
                    source_platform=Platform.MAX,
                    kind="image",
                    identity="max:image:id:photo-2",
                    filename="two.jpg",
                    mime_type="image/jpeg",
                ),
            ],
        },
        edit_mode=EditMode.REPLACE_MEDIA_GROUP,
    )

    result = await worker._call_platform(context)

    assert telegram.delete_calls == [
        {"chat_id": "-100", "message_id": "old-1"},
        {"chat_id": "-100", "message_id": "old-2"},
    ]
    assert len(telegram.send_message_calls) == 1
    send_call = telegram.send_message_calls[0]
    assert isinstance(send_call["media"], list)
    assert result.dst_message_id == "telegram-media-1-1"
    assert result.dst_message_ids == ("telegram-media-1-1", "telegram-media-1-2")


@pytest.mark.asyncio
async def test_delivery_edits_max_group_caption_on_telegram_without_recreating_album(
    tmp_path: Path,
):
    telegram_bot = DeliveryTelegramBot()
    telegram = object.__new__(TelegramClient)
    telegram.bot = cast(Any, telegram_bot)
    max_client = FakeClient("max")
    worker = make_worker(
        tmp_path,
        {
            Platform.TELEGRAM: cast(Any, telegram),
            Platform.MAX: max_client,
        },
    )
    context = make_context(
        action="edit",
        bridge_id=uuid.uuid4(),
        dst_platform=Platform.TELEGRAM,
        task_payload={
            "src": {"platform": "max", "chat_id": "300", "message_id": "mid-group"},
            "dst": {"platform": "telegram", "chat_id": "-100"},
            "dst_message_id": "55",
            "dst_message_ids": ["55", "56"],
            "text": "Alice: updated album",
            "has_media": True,
            "media": None,
            "group_kind": "photo_video_chunk",
            "group_key": "max:300:mid-group",
            "media_items": [
                media_payload(
                    source_platform=Platform.MAX,
                    kind="image",
                    identity="max:image:id:photo-1",
                    filename="one.jpg",
                    mime_type="image/jpeg",
                ),
                media_payload(
                    source_platform=Platform.MAX,
                    kind="image",
                    identity="max:image:id:photo-2",
                    filename="two.jpg",
                    mime_type="image/jpeg",
                ),
            ],
        },
        edit_mode=EditMode.CAPTION_ONLY_SAME_MEDIA,
    )

    await worker._call_platform(context)

    assert len(telegram_bot.edit_message_caption_calls) == 1
    assert telegram_bot.edit_message_caption_calls[0]["message_id"] == 55
    assert telegram_bot.edit_message_caption_calls[0]["caption"] == "Alice: updated album"
    assert telegram_bot.edit_message_media_calls == []
    assert telegram_bot.send_photo_calls == []
    assert telegram_bot.send_animation_calls == []
    assert telegram_bot.send_message_calls == []


@pytest.mark.asyncio
async def test_delivery_recreates_max_media_group_with_full_attachment_array(
    tmp_path: Path,
):
    telegram = FakeClient("telegram", download_kind=MediaKind.IMAGE)
    max_client = FakeClient("max")
    worker = make_worker(
        tmp_path,
        {
            Platform.TELEGRAM: telegram,
            Platform.MAX: max_client,
        },
    )
    context = make_context(
        action="edit",
        bridge_id=uuid.uuid4(),
        dst_platform=Platform.MAX,
        task_payload={
            "src": {
                "platform": "telegram",
                "chat_id": "-100",
                "message_id": "telegram:-100:grp-1",
            },
            "dst": {"platform": "max", "chat_id": "200"},
            "dst_message_id": "max-msg-1",
            "dst_message_ids": ["max-msg-1", "max-msg-2"],
            "text": "Alice: updated album",
            "has_media": True,
            "media": None,
            "group_kind": "photo_video_chunk",
            "group_key": "telegram:-100:grp-1",
            "media_items": [
                media_payload(
                    source_platform=Platform.TELEGRAM,
                    kind="image",
                    identity="telegram:image:id:file-1",
                    filename="one.jpg",
                    mime_type="image/jpeg",
                ),
                media_payload(
                    source_platform=Platform.TELEGRAM,
                    kind="image",
                    identity="telegram:image:id:file-2",
                    filename="two.jpg",
                    mime_type="image/jpeg",
                ),
            ],
        },
        edit_mode=EditMode.REPLACE_MEDIA_GROUP,
    )

    await worker._call_platform(context)

    assert len(telegram.download_calls) == 2
    assert max_client.delete_calls == [
        {"chat_id": "200", "message_id": "max-msg-1"},
        {"chat_id": "200", "message_id": "max-msg-2"},
    ]
    assert len(max_client.send_message_calls) == 1
    replacement_media = cast(list[Any], max_client.send_message_calls[0]["media"])
    assert len(replacement_media) == 2
    assert max_client.edit_calls == []


@pytest.mark.asyncio
async def test_delivery_rejects_telegram_voice_replacement_edit(tmp_path: Path):
    telegram_bot = DeliveryTelegramBot()
    telegram = object.__new__(TelegramClient)
    telegram.bot = cast(Any, telegram_bot)
    max_client = FakeClient("max", download_kind=MediaKind.VOICE)
    worker = make_worker(
        tmp_path,
        {
            Platform.TELEGRAM: cast(Any, telegram),
            Platform.MAX: max_client,
        },
    )
    context = make_context(
        action="edit",
        bridge_id=uuid.uuid4(),
        dst_platform=Platform.TELEGRAM,
        task_payload={
            "src": {"platform": "max", "chat_id": "300", "message_id": "mid-1"},
            "dst": {"platform": "telegram", "chat_id": "-100"},
            "dst_message_id": "55",
            "text": "Alice: updated",
            "has_media": True,
            "media": media_payload(
                source_platform=Platform.MAX,
                kind="voice",
                identity="max:voice:path:source",
                source={"url": "https://example.test/file.ogg"},
            ),
        },
        edit_mode=EditMode.REPLACE_MEDIA,
    )

    with pytest.raises(PlatformDeliveryError) as exc_info:
        await worker._call_platform(context)

    assert exc_info.value.code == "unsupported_voice_media_edit"
    assert telegram_bot.edit_message_media_calls == []


@pytest.mark.asyncio
async def test_classify_edit_mode_keeps_same_max_photo_as_caption_only(
    tmp_path: Path,
):
    bridge_id = uuid.uuid4()
    state = DeliveryState(tasks={})
    repo = FakeRepository(FakeSession(state))
    telegram = FakeClient("telegram")
    max_client = FakeClient("max")
    worker = make_worker(
        tmp_path,
        {
            Platform.TELEGRAM: telegram,
            Platform.MAX: max_client,
        },
    )
    state.created_payloads[(bridge_id, Platform.MAX, "300", "mid-1")] = {
        "media": {
            "source_platform": "max",
            "kind": "image",
            "source": {
                "url": "https://i.oneme.ru/i?old",
                "token": "old-token",
            },
        },
        "raw": {
            "raw_message": {
                "body": {
                    "attachments": [
                        {
                            "type": "image",
                            "payload": {
                                "photo_id": 123,
                                "token": "old-token",
                                "url": "https://i.oneme.ru/i?old",
                            },
                        }
                    ]
                }
            }
        },
    }

    mode = await worker._classify_edit_mode(
        repo=cast(Any, repo),
        bridge_id=bridge_id,
        payload={
            "src": {"platform": "max", "chat_id": "300", "message_id": "mid-1"},
            "dst": {"platform": "telegram", "chat_id": "-100"},
            "media": media_payload(
                source_platform=Platform.MAX,
                kind="image",
                identity="max:image:id:photo_id:123",
                source={
                    "url": "https://i.oneme.ru/i?new",
                    "token": "new-token",
                    "photo_id": 123,
                },
            ),
            "raw": {
                "raw_message": {
                    "body": {
                        "attachments": [
                            {
                                "type": "image",
                                "payload": {
                                    "photo_id": 123,
                                    "token": "new-token",
                                    "url": "https://i.oneme.ru/i?new",
                                },
                            }
                        ]
                    }
                }
            },
        },
        action=OutboxAction.EDIT,
    )

    assert mode == EditMode.CAPTION_ONLY_SAME_MEDIA


@pytest.mark.asyncio
async def test_classify_edit_mode_detects_max_photo_replacement(tmp_path: Path):
    bridge_id = uuid.uuid4()
    state = DeliveryState(tasks={})
    repo = FakeRepository(FakeSession(state))
    telegram = FakeClient("telegram")
    max_client = FakeClient("max")
    worker = make_worker(
        tmp_path,
        {
            Platform.TELEGRAM: telegram,
            Platform.MAX: max_client,
        },
    )
    state.created_payloads[(bridge_id, Platform.MAX, "300", "mid-1")] = {
        "media": {"identity": "max:image:id:photo_id:123"},
    }

    mode = await worker._classify_edit_mode(
        repo=cast(Any, repo),
        bridge_id=bridge_id,
        payload={
            "src": {"platform": "max", "chat_id": "300", "message_id": "mid-1"},
            "dst": {"platform": "telegram", "chat_id": "-100"},
            "media": media_payload(
                source_platform=Platform.MAX,
                kind="image",
                identity="max:image:id:photo_id:456",
            ),
        },
        action=OutboxAction.EDIT,
    )

    assert mode == EditMode.REPLACE_MEDIA


@pytest.mark.asyncio
async def test_classify_edit_mode_treats_text_fallback_media_as_text_only(
    tmp_path: Path,
):
    bridge_id = uuid.uuid4()
    state = DeliveryState(tasks={})
    repo = FakeRepository(FakeSession(state))
    telegram = FakeClient("telegram")
    max_client = FakeClient("max")
    worker = make_worker(
        tmp_path,
        {
            Platform.TELEGRAM: telegram,
            Platform.MAX: max_client,
        },
    )
    state.created_send_snapshots[
        (bridge_id, Platform.MAX, "300", "mid-fallback", Platform.TELEGRAM)
    ] = {
        "delivery_state": {
            "shape": "text",
            "media_filtered": True,
            "emitted_message_ids": ["55"],
        },
        "media": {"identity": "max:image:id:photo_id:123"},
    }

    mode = await worker._classify_edit_mode(
        repo=cast(Any, repo),
        bridge_id=bridge_id,
        payload={
            "src": {"platform": "max", "chat_id": "300", "message_id": "mid-fallback"},
            "dst": {"platform": "telegram", "chat_id": "-100"},
            "media": media_payload(
                source_platform=Platform.MAX,
                kind="image",
                identity="max:image:id:photo_id:123",
            ),
            "has_media": True,
        },
        action=OutboxAction.EDIT,
    )

    assert mode == EditMode.TEXT_ONLY


@pytest.mark.asyncio
async def test_classify_edit_mode_keeps_same_max_group_as_caption_only(tmp_path: Path):
    bridge_id = uuid.uuid4()
    state = DeliveryState(tasks={})
    repo = FakeRepository(FakeSession(state))
    telegram = FakeClient("telegram")
    max_client = FakeClient("max")
    worker = make_worker(
        tmp_path,
        {
            Platform.TELEGRAM: telegram,
            Platform.MAX: max_client,
        },
    )
    state.created_send_snapshots[
        (bridge_id, Platform.MAX, "300", "mid-group", Platform.TELEGRAM)
    ] = {
        "delivery_state": {
            "shape": "group_single_piece",
            "media_filtered": False,
            "emitted_message_ids": ["55", "56"],
        },
        "media_group": {
            "group_kind": "photo_video_chunk",
            "items": [
                media_payload(
                    source_platform=Platform.MAX,
                    kind="image",
                    identity="max:image:id:photo-1",
                ),
                media_payload(
                    source_platform=Platform.MAX,
                    kind="video",
                    identity="max:video:id:video-2",
                ),
            ],
        },
    }

    mode = await worker._classify_edit_mode(
        repo=cast(Any, repo),
        bridge_id=bridge_id,
        payload={
            "src": {"platform": "max", "chat_id": "300", "message_id": "mid-group"},
            "dst": {"platform": "telegram", "chat_id": "-100"},
            "group_kind": "photo_video_chunk",
            "media_items": [
                media_payload(
                    source_platform=Platform.MAX,
                    kind="image",
                    identity="max:image:id:photo-1",
                ),
                media_payload(
                    source_platform=Platform.MAX,
                    kind="video",
                    identity="max:video:id:video-2",
                ),
            ],
        },
        action=OutboxAction.EDIT,
    )

    assert mode == EditMode.CAPTION_ONLY_SAME_MEDIA


@pytest.mark.asyncio
async def test_classify_edit_mode_recreates_max_group_when_any_item_changes(
    tmp_path: Path,
):
    bridge_id = uuid.uuid4()
    state = DeliveryState(tasks={})
    repo = FakeRepository(FakeSession(state))
    telegram = FakeClient("telegram")
    max_client = FakeClient("max")
    worker = make_worker(
        tmp_path,
        {
            Platform.TELEGRAM: telegram,
            Platform.MAX: max_client,
        },
    )
    state.created_send_snapshots[
        (bridge_id, Platform.MAX, "300", "mid-group", Platform.TELEGRAM)
    ] = {
        "delivery_state": {
            "shape": "group_single_piece",
            "media_filtered": False,
            "emitted_message_ids": ["55", "56"],
        },
        "media_group": {
            "group_kind": "photo_video_chunk",
            "items": [
                media_payload(
                    source_platform=Platform.MAX,
                    kind="image",
                    identity="max:image:id:photo-1",
                ),
                media_payload(
                    source_platform=Platform.MAX,
                    kind="video",
                    identity="max:video:id:video-2",
                ),
            ],
        },
    }

    mode = await worker._classify_edit_mode(
        repo=cast(Any, repo),
        bridge_id=bridge_id,
        payload={
            "src": {"platform": "max", "chat_id": "300", "message_id": "mid-group"},
            "dst": {"platform": "telegram", "chat_id": "-100"},
            "group_kind": "photo_video_chunk",
            "media_items": [
                media_payload(
                    source_platform=Platform.MAX,
                    kind="image",
                    identity="max:image:id:photo-1",
                ),
                media_payload(
                    source_platform=Platform.MAX,
                    kind="video",
                    identity="max:video:id:video-3",
                ),
            ],
        },
        action=OutboxAction.EDIT,
    )

    assert mode == EditMode.REPLACE_MEDIA_GROUP


@pytest.mark.asyncio
async def test_classify_edit_mode_recreates_max_group_when_created_payload_is_missing(
    tmp_path: Path,
):
    bridge_id = uuid.uuid4()
    state = DeliveryState(tasks={})
    repo = FakeRepository(FakeSession(state))
    telegram = FakeClient("telegram")
    max_client = FakeClient("max")
    worker = make_worker(
        tmp_path,
        {
            Platform.TELEGRAM: telegram,
            Platform.MAX: max_client,
        },
    )

    mode = await worker._classify_edit_mode(
        repo=cast(Any, repo),
        bridge_id=bridge_id,
        payload={
            "src": {"platform": "max", "chat_id": "300", "message_id": "mid-group"},
            "dst": {"platform": "telegram", "chat_id": "-100"},
            "group_kind": "photo_video_chunk",
            "media_items": [
                media_payload(
                    source_platform=Platform.MAX,
                    kind="image",
                    identity="max:image:id:photo-1",
                ),
                media_payload(
                    source_platform=Platform.MAX,
                    kind="video",
                    identity="max:video:id:video-2",
                ),
            ],
        },
        action=OutboxAction.EDIT,
    )

    assert mode == EditMode.REPLACE_MEDIA_GROUP


@pytest.mark.asyncio
async def test_delivery_deletes_mirrored_message(tmp_path: Path):
    telegram = FakeClient("telegram")
    max_client = FakeClient("max")
    worker = make_worker(
        tmp_path,
        {
            Platform.TELEGRAM: telegram,
            Platform.MAX: max_client,
        },
    )
    context = make_context(
        action="delete",
        bridge_id=uuid.uuid4(),
        dst_platform=Platform.TELEGRAM,
        task_payload={
            "src": {"platform": "max", "chat_id": "300", "message_id": "mid-3"},
            "dst": {"platform": "telegram", "chat_id": "-100"},
            "dst_message_id": "55",
        },
    )

    await worker._call_platform(context)

    assert telegram.delete_calls == [{"chat_id": "-100", "message_id": "55"}]


@pytest.mark.asyncio
async def test_delivery_refines_oversize_group_piece_in_same_attempt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    task = make_task(
        action="send",
        bridge_id=uuid.uuid4(),
        dst_platform=Platform.TELEGRAM,
        task_payload={
            "src": {"platform": "max", "chat_id": "300", "message_id": "mid-refine"},
            "dst": {"platform": "telegram", "chat_id": "-100"},
            "text": "Alice: album",
            "fallback_text": "Alice: [photo group]",
            "has_media": True,
            "media_kind": "photo_video_chunk",
            "group_kind": "photo_video_chunk",
            "group_key": "max:300:mid-refine",
            "media": None,
            "media_items": [
                media_payload(
                    source_platform=Platform.MAX,
                    kind="image",
                    identity="max:image:id:photo-a",
                    filename="a.jpg",
                    mime_type="image/jpeg",
                ),
                media_payload(
                    source_platform=Platform.MAX,
                    kind="image",
                    identity="max:image:id:photo-b",
                    filename="b.jpg",
                    mime_type="image/jpeg",
                ),
            ],
        },
    )
    state = DeliveryState(tasks={task.outbox_id: task})
    database = FakeDatabase(state)
    telegram = OversizeMultiMediaClient("telegram", state=state)
    max_client = FakeClient("max", state=state, download_kind=MediaKind.IMAGE)
    worker = make_worker(
        tmp_path,
        {
            Platform.TELEGRAM: telegram,
            Platform.MAX: max_client,
        },
        database=database,
    )
    monkeypatch.setattr("maxogram.workers.delivery.Repository", FakeRepository)

    processed = await worker.run_once()

    assert processed == 1
    assert len(telegram.send_message_calls) == 2
    assert state.tasks[task.outbox_id].status == TaskStatus.DONE
    assert state.attempts == [
        {
            "outbox_id": task.outbox_id,
            "attempt_no": 1,
            "outcome": DeliveryOutcome.SUCCESS,
        }
    ]


@pytest.mark.asyncio
async def test_delivery_persists_all_group_destination_ids_for_delete_and_snapshot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    bridge_id = uuid.uuid4()
    task = make_task(
        action="send",
        bridge_id=bridge_id,
        dst_platform=Platform.TELEGRAM,
        task_payload={
            "src": {"platform": "max", "chat_id": "300", "message_id": "mid-delete"},
            "dst": {"platform": "telegram", "chat_id": "-100"},
            "text": "Alice: album",
            "fallback_text": "Alice: [photo group]",
            "has_media": True,
            "media_kind": "photo_video_chunk",
            "group_kind": "photo_video_chunk",
            "group_key": "max:300:mid-delete",
            "media": None,
            "media_items": [
                media_payload(
                    source_platform=Platform.MAX,
                    kind="image",
                    identity="max:image:id:photo-1",
                    filename="one.jpg",
                    mime_type="image/jpeg",
                ),
                media_payload(
                    source_platform=Platform.MAX,
                    kind="image",
                    identity="max:image:id:photo-2",
                    filename="two.jpg",
                    mime_type="image/jpeg",
                ),
                media_payload(
                    source_platform=Platform.MAX,
                    kind="image",
                    identity="max:image:id:photo-3",
                    filename="three.jpg",
                    mime_type="image/jpeg",
                ),
            ],
        },
    )
    state = DeliveryState(tasks={task.outbox_id: task})
    database = FakeDatabase(state)
    telegram = FakeClient("telegram", state=state)
    max_client = FakeClient(
        "max",
        state=state,
        download_kind=MediaKind.IMAGE,
        download_size_by_identity={
            "max:image:id:photo-1": 8 * 1024 * 1024,
            "max:image:id:photo-2": 12 * 1024 * 1024,
            "max:image:id:photo-3": 8 * 1024 * 1024,
        },
    )
    worker = make_worker(
        tmp_path,
        {
            Platform.TELEGRAM: telegram,
            Platform.MAX: max_client,
        },
        database=database,
    )
    monkeypatch.setattr("maxogram.workers.delivery.Repository", FakeRepository)

    processed = await worker.run_once()

    assert processed == 1
    repo = FakeRepository(FakeSession(state))
    dst_ids = await repo.list_destination_message_ids(
        bridge_id,
        Platform.MAX,
        "300",
        "mid-delete",
    )
    assert dst_ids == ["telegram-media-1", "telegram-text-1", "telegram-media-2"]
    snapshot = state.created_send_snapshots[
        (bridge_id, Platform.MAX, "300", "mid-delete", Platform.TELEGRAM)
    ]
    assert snapshot["delivery_state"]["shape"] == "group_multi_piece"
    assert snapshot["delivery_state"]["media_filtered"] is True
    assert snapshot["delivery_state"]["emitted_message_ids"] == dst_ids

    delete_context = make_context(
        action="delete",
        bridge_id=bridge_id,
        dst_platform=Platform.TELEGRAM,
        task_payload={
            "src": {"platform": "max", "chat_id": "300", "message_id": "mid-delete"},
            "dst": {"platform": "telegram", "chat_id": "-100"},
            "dst_message_id": dst_ids[0],
            "dst_message_ids": dst_ids,
            "group_kind": "photo_video_chunk",
            "media_items": [media_payload(source_platform=Platform.MAX, kind="image", identity="max:image:id:photo-1")],
        },
    )

    await worker._call_platform(delete_context)

    assert telegram.delete_calls == [
        {"chat_id": "-100", "message_id": "telegram-media-1"},
        {"chat_id": "-100", "message_id": "telegram-text-1"},
        {"chat_id": "-100", "message_id": "telegram-media-2"},
    ]


@pytest.mark.asyncio
async def test_delivery_media_send_runs_outside_transactions_and_uses_fresh_sessions(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    task = make_task(
        action="send",
        bridge_id=uuid.uuid4(),
        dst_platform=Platform.TELEGRAM,
        task_payload={
            "src": {"platform": "max", "chat_id": "300", "message_id": "mid-9"},
            "dst": {"platform": "telegram", "chat_id": "-100"},
            "text": "Alice: report",
            "fallback_text": "Alice: report",
            "has_media": True,
            "media_kind": "document",
            "media": media_payload(
                source_platform=Platform.MAX,
                kind="document",
                identity="max:document:path:source",
            ),
        },
    )
    state = DeliveryState(tasks={task.outbox_id: task})
    database = FakeDatabase(state)
    max_client = FakeClient(
        "max",
        state=state,
        download_kind=MediaKind.DOCUMENT,
        assert_no_active_transaction=True,
    )
    telegram = FakeClient(
        "telegram",
        state=state,
        assert_no_active_transaction=True,
    )
    worker = make_worker(
        tmp_path,
        {
            Platform.TELEGRAM: telegram,
            Platform.MAX: max_client,
        },
        database=database,
        lease_seconds=60,
    )
    monkeypatch.setattr("maxogram.workers.delivery.Repository", FakeRepository)

    processed = await worker.run_once()

    assert processed == 1
    assert state.session_count >= 3
    assert state.active_transactions == 0
    assert state.tasks[task.outbox_id].status == TaskStatus.DONE
    assert len(state.mappings) == 1
    assert state.attempts == [
        {
            "outbox_id": task.outbox_id,
            "attempt_no": 1,
            "outcome": DeliveryOutcome.SUCCESS,
        }
    ]


@pytest.mark.asyncio
async def test_delivery_heartbeat_prevents_duplicate_requeue_for_slow_send(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    task = make_task(
        action="send",
        bridge_id=uuid.uuid4(),
        dst_platform=Platform.TELEGRAM,
        task_payload={
            "src": {"platform": "max", "chat_id": "300", "message_id": "mid-video"},
            "dst": {"platform": "telegram", "chat_id": "-100"},
            "text": "Alice:",
            "fallback_text": "Alice: [video]",
            "has_media": True,
            "media_kind": "video",
            "media": media_payload(
                source_platform=Platform.MAX,
                kind="video",
                identity="max:video:path:source",
            ),
        },
    )
    state = DeliveryState(tasks={task.outbox_id: task})
    database = FakeDatabase(state)
    max_client = FakeClient(
        "max",
        state=state,
        download_kind=MediaKind.VIDEO,
        assert_no_active_transaction=True,
    )
    telegram = FakeClient(
        "telegram",
        state=state,
        send_delay=0.25,
        assert_no_active_transaction=True,
    )
    worker = make_worker(
        tmp_path,
        {
            Platform.TELEGRAM: telegram,
            Platform.MAX: max_client,
        },
        database=database,
        lease_seconds=0.1,  # type: ignore[arg-type]
    )
    reconciliation = ReconciliationWorker(
        database=database,  # type: ignore[arg-type]
        stop_event=asyncio.Event(),
        idle_seconds=0,
        root_dir=tmp_path,
    )
    monkeypatch.setattr("maxogram.workers.delivery.Repository", FakeRepository)
    monkeypatch.setattr("maxogram.workers.reconciliation.Repository", FakeRepository)
    monkeypatch.setattr(
        DeliveryWorker,
        "_heartbeat_interval_seconds",
        lambda self: 0.05,
    )

    delivery_task = asyncio.create_task(worker.run_once())
    await asyncio.sleep(0.16)
    reset, replayed, expired = await reconciliation.run_once()
    processed = await delivery_task
    processed_again = await worker.run_once()

    assert processed == 1
    assert processed_again == 0
    assert reset == 0
    assert replayed == 0
    assert expired == 0
    assert state.lease_renewals >= 1
    assert len(telegram.send_message_calls) == 1
    assert state.tasks[task.outbox_id].status == TaskStatus.DONE


@pytest.mark.asyncio
async def test_delivery_skips_stale_success_finalization(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    task = make_task(
        action="send",
        bridge_id=uuid.uuid4(),
        dst_platform=Platform.TELEGRAM,
        task_payload={
            "src": {"platform": "max", "chat_id": "300", "message_id": "mid-stale"},
            "dst": {"platform": "telegram", "chat_id": "-100"},
            "text": "Alice: hello",
            "fallback_text": "Alice: hello",
            "has_media": False,
            "media_kind": None,
            "media": None,
        },
    )
    state = DeliveryState(tasks={task.outbox_id: task})
    database = FakeDatabase(state)

    def mark_task_stale() -> None:
        current = state.tasks[task.outbox_id]
        current.status = TaskStatus.RETRY_WAIT
        current.attempt_count = 99
        current.inflight_until = None

    telegram = FakeClient("telegram", state=state, on_send_text=mark_task_stale)
    max_client = FakeClient("max", state=state)
    worker = make_worker(
        tmp_path,
        {
            Platform.TELEGRAM: telegram,
            Platform.MAX: max_client,
        },
        database=database,
        lease_seconds=60,
    )
    monkeypatch.setattr("maxogram.workers.delivery.Repository", FakeRepository)

    processed = await worker.run_once()

    assert processed == 1
    assert len(telegram.send_text_calls) == 1
    assert state.tasks[task.outbox_id].status == TaskStatus.RETRY_WAIT
    assert state.tasks[task.outbox_id].attempt_count == 99
    assert state.attempts == []
    assert state.mappings == []


@pytest.mark.asyncio
async def test_delivery_finalizes_success_when_telegram_result_serialization_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    task = make_task(
        action="send",
        bridge_id=uuid.uuid4(),
        dst_platform=Platform.TELEGRAM,
        task_payload={
            "src": {"platform": "max", "chat_id": "300", "message_id": "mid-link"},
            "dst": {"platform": "telegram", "chat_id": "-100"},
            "text_plain": "Alice: https://example.test",
            "text_html": (
                'Alice: <a href="https://example.test">https://example.test</a>'
            ),
            "fallback_text": "Alice: https://example.test",
            "has_media": False,
            "media_kind": None,
            "media": None,
        },
    )
    state = DeliveryState(tasks={task.outbox_id: task})
    database = FakeDatabase(state)
    telegram_bot = DeliveryTelegramBot()
    telegram = object.__new__(TelegramClient)
    telegram.bot = cast(Any, telegram_bot)
    max_client = FakeClient("max", state=state)
    worker = make_worker(
        tmp_path,
        {
            Platform.TELEGRAM: cast(Any, telegram),
            Platform.MAX: max_client,
        },
        database=database,
    )

    def fail_serialize(message: object) -> object:
        _ = message
        raise TypeError("boom")

    monkeypatch.setattr("maxogram.workers.delivery.Repository", FakeRepository)
    monkeypatch.setattr(
        "maxogram.platforms.telegram.deserialize_telegram_object_to_python",
        fail_serialize,
    )

    processed = await worker.run_once()

    assert processed == 1
    assert len(telegram_bot.send_message_calls) == 1
    assert state.tasks[task.outbox_id].status == TaskStatus.DONE
    assert state.dead_letters == []
    assert state.attempts == [
        {
            "outbox_id": task.outbox_id,
            "attempt_no": 1,
            "outcome": DeliveryOutcome.SUCCESS,
        }
    ]
    assert len(state.mappings) == 1
    assert state.mappings[0]["dst_message_id"] == "5000"


@pytest.mark.asyncio
async def test_max_link_send_then_edit_replays_once_without_duplicate_send(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    bridge_id = uuid.uuid4()
    send_task = make_task(
        action="send",
        bridge_id=bridge_id,
        dst_platform=Platform.TELEGRAM,
        task_payload={
            "src": {"platform": "max", "chat_id": "300", "message_id": "mid-link"},
            "dst": {"platform": "telegram", "chat_id": "-100"},
            "text_plain": "Alice: https://example.test",
            "text_html": (
                'Alice: <a href="https://example.test">https://example.test</a>'
            ),
            "fallback_text": "Alice: https://example.test",
            "has_media": False,
            "media_kind": None,
            "media": None,
        },
    )
    edit_dedup_key = "max:300:mid-link:message.edited:edit-1"
    pending = make_pending_mutation(
        bridge_id=bridge_id,
        dedup_key=edit_dedup_key,
        src_platform=Platform.MAX,
        src_chat_id="300",
        src_message_id="mid-link",
        mutation_type="edit",
        payload={
            "src": {"platform": "max", "chat_id": "300", "message_id": "mid-link"},
            "dst": {"platform": "telegram", "chat_id": "-100"},
            "text_plain": "Alice: updated https://example.test",
            "text_html": (
                'Alice: <b>updated</b> '
                '<a href="https://example.test">https://example.test</a>'
            ),
            "fallback_text": "Alice: updated https://example.test",
            "has_media": False,
            "media_kind": None,
            "media": None,
            "version": "edit-1",
        },
    )
    state = DeliveryState(
        tasks={send_task.outbox_id: send_task},
        pending_rows=[pending],
        canonical_event_ids={edit_dedup_key: uuid.uuid4()},
    )
    database = FakeDatabase(state)
    telegram_bot = DeliveryTelegramBot()
    telegram = object.__new__(TelegramClient)
    telegram.bot = cast(Any, telegram_bot)
    max_client = FakeClient("max", state=state)
    delivery = make_worker(
        tmp_path,
        {
            Platform.TELEGRAM: cast(Any, telegram),
            Platform.MAX: max_client,
        },
        database=database,
    )
    reconciliation = ReconciliationWorker(
        database=database,  # type: ignore[arg-type]
        stop_event=asyncio.Event(),
        idle_seconds=0,
        root_dir=tmp_path,
    )

    def fail_serialize(message: object) -> object:
        _ = message
        raise TypeError("boom")

    monkeypatch.setattr("maxogram.workers.delivery.Repository", FakeRepository)
    monkeypatch.setattr("maxogram.workers.reconciliation.Repository", FakeRepository)
    monkeypatch.setattr(
        "maxogram.platforms.telegram.deserialize_telegram_object_to_python",
        fail_serialize,
    )

    processed_send = await delivery.run_once()
    reset, replayed, expired = await reconciliation.run_once()
    processed_edit = await delivery.run_once()

    assert processed_send == 1
    assert processed_edit == 1
    assert reset == 0
    assert replayed == 1
    assert expired == 0
    assert pending.status == TaskStatus.DONE
    assert len(telegram_bot.send_message_calls) == 1
    assert len(telegram_bot.edit_message_text_calls) == 1
    assert state.dead_letters == []
    assert [attempt["outcome"] for attempt in state.attempts] == [
        DeliveryOutcome.SUCCESS,
        DeliveryOutcome.SUCCESS,
    ]
