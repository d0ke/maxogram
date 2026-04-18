from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from maxogram.db.repositories import Repository
from maxogram.db.session import Database
from maxogram.domain import (
    CommandContext,
    CommandReply,
    EventType,
    OutboxAction,
    Platform,
    RowStatus,
    UserIdentity,
)
from maxogram.metrics import mutation_event_total
from maxogram.platforms.base import PlatformClient, PlatformDeliveryError
from maxogram.runtime_resilience import (
    RuntimeBackoffState,
    is_transient_db_error,
    wait_or_stop,
)
from maxogram.services.commands import CommandProcessor
from maxogram.services.dedup import outbox_dedup_key, partition_key
from maxogram.services.normalization import NormalizedUpdate, normalize_update
from maxogram.services.normalization import normalize_telegram_media_group
from maxogram.services.rendering import (
    default_alias,
    render_audio_caption,
    render_audio_caption_html,
    render_media_caption,
    render_media_caption_html,
    render_mirror_html,
    render_mirror_text,
)

logger = logging.getLogger(__name__)

TELEGRAM_MEDIA_GROUP_QUIET_WINDOW = timedelta(seconds=1)


class NormalizerWorker:
    name = "normalizer"

    def __init__(
        self,
        *,
        database: Database,
        clients: dict[Platform, PlatformClient],
        command_processor: CommandProcessor,
        stop_event: asyncio.Event,
        idle_seconds: float,
        batch_size: int = 50,
    ) -> None:
        self.database = database
        self.clients = clients
        self.command_processor = command_processor
        self.stop_event = stop_event
        self.idle_seconds = idle_seconds
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
                if is_transient_db_error(exc):
                    if await self._wait_after_retryable_failure(exc):
                        return
                    continue
                self._retry_backoff.clear()
                logger.exception("Normalizer failed")
                if await wait_or_stop(self.stop_event, self.idle_seconds):
                    return

    async def run_once(self) -> int:
        replies: list[CommandReply] = []
        processed_rows = 0
        flushed_groups = 0
        async with self.database.session() as session:
            repo = Repository(session)
            async with session.begin():
                rows = await repo.claim_inbox(self.batch_size)
                for row in rows:
                    reply = await self._process_row(
                        repo,
                        row.platform,
                        row.raw,
                        row.inbox_id,
                        row.received_at,
                    )
                    if reply:
                        replies.append(reply)
                    await repo.mark_inbox(row, RowStatus.PROCESSED)
                processed_rows = len(rows)
                flushed_groups = await self._flush_ready_telegram_media_groups(repo)
        for reply in replies:
            await self._send_command_reply(reply)
        return processed_rows + flushed_groups

    async def _process_row(
        self,
        repo: Repository,
        platform: Platform,
        raw: dict[str, Any],
        inbox_id: uuid.UUID,
        received_at: datetime | None = None,
    ) -> CommandReply | None:
        if received_at is None:
            received_at = datetime.now(UTC)
        normalized = normalize_update(platform, raw)
        if normalized is None or normalized.is_bot_message:
            return None
        max_mutation_events = {
            EventType.MESSAGE_EDITED,
            EventType.MESSAGE_DELETED,
        }
        if platform == Platform.MAX and normalized.event_type in max_mutation_events:
            mutation_event_total.labels(
                platform.value,
                normalized.event_type.value,
            ).inc()
            logger.info(
                "Normalized MAX mutation event type=%s chat_id=%s message_id=%s",
                normalized.event_type.value,
                normalized.chat_id,
                normalized.message_id,
            )
        if normalized.identity:
            await repo.upsert_identity(
                normalized.identity.platform,
                normalized.identity.user_id,
                username=normalized.identity.username,
                first_name=normalized.identity.first_name,
                last_name=normalized.identity.last_name,
                is_bot=normalized.identity.is_bot,
            )
        if normalized.is_command:
            return await self._process_command(repo, normalized)

        grouped_telegram_message = _grouped_telegram_message(raw, normalized)
        if grouped_telegram_message is not None:
            bridge = await repo.find_bridge_by_chat(platform, normalized.chat_id)
            if bridge is None:
                return None
            await repo.buffer_telegram_media_group_update(
                chat_id=normalized.chat_id,
                media_group_id=grouped_telegram_message["media_group_id"],
                group_key=grouped_telegram_message["group_key"],
                message_id=grouped_telegram_message["message_id"],
                raw_message=grouped_telegram_message["raw_message"],
                flush_after=received_at + TELEGRAM_MEDIA_GROUP_QUIET_WINDOW,
            )
            return None

        bridge = await repo.find_bridge_by_chat(platform, normalized.chat_id)
        if bridge is None:
            return None
        dst_chat = await repo.find_other_chat(bridge.bridge_id, platform)
        if dst_chat is None or normalized.message_id is None:
            return None

        await self._enqueue_relay_event(
            repo,
            normalized,
            bridge_id=bridge.bridge_id,
            dst_chat=dst_chat,
            raw_inbox_id=inbox_id,
        )
        return None

    async def _build_payload(
        self,
        repo: Repository,
        normalized: NormalizedUpdate,
        bridge_id: uuid.UUID,
        *,
        dst_platform: Platform | None = None,
    ) -> dict[str, object]:
        if dst_platform is None:
            dst_platform = (
                Platform.MAX
                if normalized.platform == Platform.TELEGRAM
                else Platform.TELEGRAM
            )
        alias = await repo.get_alias(
            bridge_id,
            normalized.platform,
            normalized.user_id or "",
        )
        identity = normalized.identity or _unknown_identity(normalized)
        alias = alias or default_alias(identity, normalized.user_id)
        reply_to_dst_id, reply_hint = await self._resolve_reply_target(
            repo,
            bridge_id,
            normalized,
        )
        media = _supported_media_payload(normalized.payload)
        media_group = _supported_media_group_payload(normalized.payload)
        media_items = (
            _media_group_items(media_group) if media_group is not None else None
        )
        group_kind = (
            str(media_group.get("group_kind")) if media_group is not None else None
        )
        group_key = (
            str(media_group.get("group_key")) if media_group is not None else None
        )
        source_member_message_ids = (
            _media_group_source_member_ids(media_group)
            if media_group is not None
            else None
        )
        has_media_payload = media is not None or media_group is not None
        media_kind = (
            group_kind
            if group_kind is not None
            else (
                str(media.get("kind"))
                if media is not None and media.get("kind") is not None
                else None
            )
        )
        placeholder = _media_text_hint(normalized.payload)
        if placeholder is None and media_group is not None:
            placeholder = (
                str(media_group.get("text_hint"))
                if media_group.get("text_hint") is not None
                else None
            )
        post_send_text_plain: str | None = None
        post_send_text_html: str | None = None
        if media is not None and media_kind in {"audio", "voice"}:
            audio_text_plain = render_audio_caption(
                alias,
                normalized.text,
                forwarded=normalized.forwarded,
                reply_hint=reply_hint,
            )
            audio_text_html = render_audio_caption_html(
                alias,
                normalized.text,
                normalized.formatted_html,
                forwarded=normalized.forwarded,
                reply_hint=reply_hint,
            )
            if normalized.platform == Platform.TELEGRAM and dst_platform == Platform.MAX:
                rendered_plain = ""
                rendered_html = None
                fallback_text = audio_text_plain
                post_send_text_plain = audio_text_plain
                post_send_text_html = audio_text_html
            else:
                rendered_plain = audio_text_plain
                rendered_html = audio_text_html
                fallback_text = audio_text_plain
        elif has_media_payload:
            rendered_plain = render_media_caption(
                alias,
                normalized.text,
                forwarded=normalized.forwarded,
                reply_hint=reply_hint,
            )
            rendered_html = render_media_caption_html(
                alias,
                normalized.text,
                normalized.formatted_html,
                forwarded=normalized.forwarded,
                reply_hint=reply_hint,
            )
            fallback_text = render_mirror_text(
                alias,
                normalized.text,
                forwarded=normalized.forwarded,
                reply_hint=reply_hint,
                media_hint=placeholder,
            )
        else:
            rendered_plain = render_mirror_text(
                alias,
                normalized.text,
                forwarded=normalized.forwarded,
                reply_hint=reply_hint,
                media_hint=placeholder,
            )
            rendered_html = render_mirror_html(
                alias,
                normalized.text,
                normalized.formatted_html,
                forwarded=normalized.forwarded,
                reply_hint=reply_hint,
                media_hint=placeholder,
            )
            fallback_text = render_mirror_text(
                alias,
                normalized.text,
                forwarded=normalized.forwarded,
                reply_hint=reply_hint,
                media_hint=placeholder,
            )
        payload = {
            "src": {
                "platform": identity.platform.value,
                "chat_id": normalized.chat_id,
                "message_id": normalized.message_id,
                "user_id": normalized.user_id,
            },
            "text": rendered_plain,
            "text_plain": rendered_plain,
            "text_html": rendered_html,
            "fallback_text": fallback_text,
            "reply_to_message_id": reply_to_dst_id,
            "raw": normalized.payload or {},
            "has_media": has_media_payload,
            "media_kind": media_kind,
            "media": media,
            "media_items": media_items,
            "group_kind": group_kind,
            "group_key": group_key,
            "source_member_message_ids": source_member_message_ids,
            "version": normalized.event_version,
        }
        if post_send_text_plain is not None:
            payload["post_send_text_plain"] = post_send_text_plain
        if post_send_text_html is not None:
            payload["post_send_text_html"] = post_send_text_html
        return payload

    async def _flush_ready_telegram_media_groups(self, repo: Repository) -> int:
        flushed = 0
        buffers = await repo.claim_flushable_telegram_media_groups(self.batch_size)
        for buffer in buffers:
            members = await repo.list_telegram_media_group_members(buffer.buffer_id)
            raw_members = [member.raw_message for member in members]
            normalized = normalize_telegram_media_group(
                group_key=buffer.group_key,
                members=raw_members,
                has_flushed=buffer.has_flushed,
            )
            await repo.mark_telegram_media_group_flushed(buffer)
            if normalized is None or normalized.message_id is None:
                continue

            bridge = await repo.find_bridge_by_chat(Platform.TELEGRAM, buffer.chat_id)
            if bridge is None:
                continue
            dst_chat = await repo.find_other_chat(bridge.bridge_id, Platform.TELEGRAM)
            if dst_chat is None:
                continue
            await self._enqueue_relay_event(
                repo,
                normalized,
                bridge_id=bridge.bridge_id,
                dst_chat=dst_chat,
                raw_inbox_id=None,
            )
            flushed += 1
        return flushed

    async def _enqueue_relay_event(
        self,
        repo: Repository,
        normalized: NormalizedUpdate,
        *,
        bridge_id: uuid.UUID,
        dst_chat: Any,
        raw_inbox_id: uuid.UUID | None,
    ) -> None:
        dst_platform = dst_chat.platform
        payload = await self._build_payload(
            repo,
            normalized,
            bridge_id,
            dst_platform=dst_platform,
        )
        payload["dst"] = {"platform": dst_platform.value, "chat_id": dst_chat.chat_id}
        event_id = await repo.insert_canonical_event(
            bridge_id=bridge_id,
            dedup_key=normalized.dedup_key,
            src_platform=normalized.platform,
            src_chat_id=normalized.chat_id,
            src_user_id=normalized.user_id,
            src_message_id=normalized.message_id,
            event_type=normalized.event_type.value,
            happened_at=normalized.happened_at,
            payload=payload,
            raw_inbox_id=raw_inbox_id,
        )
        if event_id is None or normalized.message_id is None:
            return

        action = _action_for_event(normalized.event_type)
        if action in {OutboxAction.EDIT, OutboxAction.DELETE}:
            mapping = await repo.find_mapping_by_source(
                bridge_id,
                normalized.platform,
                normalized.chat_id,
                normalized.message_id,
            )
            if mapping is None:
                await repo.insert_pending_mutation(
                    bridge_id=bridge_id,
                    dedup_key=normalized.dedup_key,
                    src_platform=normalized.platform,
                    src_chat_id=normalized.chat_id,
                    src_message_id=normalized.message_id,
                    mutation_type=action.value,
                    payload=payload,
                )
                return
            payload["dst_message_id"] = mapping.dst_message_id
            dst_message_ids = await repo.list_destination_message_ids(
                bridge_id,
                normalized.platform,
                normalized.chat_id,
                normalized.message_id,
            )
            if dst_message_ids:
                payload["dst_message_ids"] = dst_message_ids

        version_key = _payload_version(payload)
        await repo.enqueue_outbox(
            bridge_id=bridge_id,
            dedup_key=outbox_dedup_key(
                bridge_id,
                normalized.platform,
                normalized.chat_id,
                normalized.message_id,
                dst_platform,
                action,
                version_key,
            ),
            src_event_id=event_id,
            dst_platform=dst_platform,
            action=action,
            partition_key=partition_key(bridge_id, normalized.platform, dst_platform),
            task=payload,
        )

    async def _resolve_reply_target(
        self,
        repo: Repository,
        bridge_id: uuid.UUID,
        normalized: NormalizedUpdate,
    ) -> tuple[str | None, str | None]:
        if normalized.reply_to_message_id is None:
            return None, None

        mapping = await repo.find_mapping_by_source(
            bridge_id,
            normalized.platform,
            normalized.chat_id,
            normalized.reply_to_message_id,
        )
        if mapping is not None:
            return mapping.dst_message_id, None

        mapping = await repo.find_mapping_by_destination(
            bridge_id,
            normalized.platform,
            normalized.chat_id,
            normalized.reply_to_message_id,
        )
        if mapping is not None:
            return mapping.src_message_id, None

        return None, normalized.reply_to_message_id

    async def _process_command(
        self,
        repo: Repository,
        normalized: NormalizedUpdate,
    ) -> CommandReply | None:
        if normalized.user_id is None:
            return None
        context = CommandContext(
            platform=normalized.platform,
            chat_id=normalized.chat_id,
            user_id=normalized.user_id,
            message_id=normalized.message_id,
            text=normalized.text or "",
            reply_to_user_id=normalized.reply_to_user_id,
            reply_to_message_id=normalized.reply_to_message_id,
        )
        return await self.command_processor.process(
            repo.session,
            context,
            is_admin=self._is_admin,
        )

    async def _is_admin(self, context: CommandContext) -> bool:
        client = self.clients[context.platform]
        return await client.is_admin(context.chat_id, context.user_id)

    async def _send_command_reply(self, reply: CommandReply) -> None:
        try:
            await self.clients[reply.platform].send_text(reply.chat_id, reply.text)
        except PlatformDeliveryError as exc:
            logger.warning("Command reply failed: %s", exc)

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


def _action_for_event(event_type: EventType) -> OutboxAction:
    if event_type == EventType.MESSAGE_EDITED:
        return OutboxAction.EDIT
    if event_type == EventType.MESSAGE_DELETED:
        return OutboxAction.DELETE
    return OutboxAction.SEND


def _unknown_identity(normalized: NormalizedUpdate) -> UserIdentity:
    return UserIdentity(
        platform=normalized.platform,
        user_id=normalized.user_id or "unknown",
    )


def _supported_media_payload(payload: dict[str, Any] | None) -> dict[str, Any] | None:
    media = payload.get("media") if isinstance(payload, dict) else None
    if not isinstance(media, dict) or not media.get("supported"):
        return None
    media_payload = media.get("payload")
    return media_payload if isinstance(media_payload, dict) else None


def _supported_media_group_payload(
    payload: dict[str, Any] | None,
) -> dict[str, Any] | None:
    media_group = payload.get("media_group") if isinstance(payload, dict) else None
    if not isinstance(media_group, dict) or not media_group.get("supported"):
        return None
    if media_group.get("group_kind") != "photo_video_chunk":
        return None
    items = media_group.get("items")
    if not isinstance(items, list) or not items:
        return None
    return media_group


def _media_group_items(media_group: dict[str, Any]) -> list[dict[str, Any]]:
    items = media_group.get("items")
    return [item for item in items if isinstance(item, dict)] if isinstance(items, list) else []


def _media_group_source_member_ids(media_group: dict[str, Any]) -> list[str]:
    values = media_group.get("source_member_message_ids")
    if not isinstance(values, list):
        return []
    return [str(value) for value in values if value is not None]


def _media_text_hint(payload: dict[str, Any] | None) -> str | None:
    media = payload.get("media") if isinstance(payload, dict) else None
    if not isinstance(media, dict):
        return None
    text_hint = media.get("text_hint")
    return str(text_hint) if text_hint is not None else None


def _grouped_telegram_message(
    raw: dict[str, Any],
    normalized: NormalizedUpdate,
) -> dict[str, str | dict[str, Any]] | None:
    if normalized.platform != Platform.TELEGRAM:
        return None
    raw_message = raw.get("message")
    if raw_message is None and raw.get("edited_message") is not None:
        raw_message = raw["edited_message"]
    if not isinstance(raw_message, dict):
        return None
    media_group_id = raw_message.get("media_group_id")
    if media_group_id is None:
        return None
    media = _supported_media_payload(normalized.payload)
    if media is None or str(media.get("kind")) not in {"image", "video"}:
        return None
    if normalized.message_id is None:
        return None
    return {
        "group_key": f"{Platform.TELEGRAM.value}:{normalized.chat_id}:{media_group_id}",
        "media_group_id": str(media_group_id),
        "message_id": normalized.message_id,
        "raw_message": raw_message,
    }


def _payload_version(payload: dict[str, object]) -> str | int | None:
    version = payload.get("version")
    return version if isinstance(version, str | int) else None
