from __future__ import annotations

import asyncio
import logging
import uuid
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
from maxogram.services.rendering import (
    default_alias,
    render_media_caption,
    render_media_caption_html,
    render_mirror_html,
    render_mirror_text,
)

logger = logging.getLogger(__name__)


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
        async with self.database.session() as session:
            repo = Repository(session)
            async with session.begin():
                rows = await repo.claim_inbox(self.batch_size)
                for row in rows:
                    reply = await self._process_row(
                        repo, row.platform, row.raw, row.inbox_id
                    )
                    if reply:
                        replies.append(reply)
                    await repo.mark_inbox(row, RowStatus.PROCESSED)
        for reply in replies:
            await self._send_command_reply(reply)
        return len(replies) if replies else len(rows)

    async def _process_row(
        self,
        repo: Repository,
        platform: Platform,
        raw: dict[str, Any],
        inbox_id: uuid.UUID,
    ) -> CommandReply | None:
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

        bridge = await repo.find_bridge_by_chat(platform, normalized.chat_id)
        if bridge is None:
            return None
        dst_chat = await repo.find_other_chat(bridge.bridge_id, platform)
        if dst_chat is None or normalized.message_id is None:
            return None

        dst_platform = dst_chat.platform
        payload = await self._build_payload(repo, normalized, bridge.bridge_id)
        payload["dst"] = {"platform": dst_platform.value, "chat_id": dst_chat.chat_id}
        event_id = await repo.insert_canonical_event(
            bridge_id=bridge.bridge_id,
            dedup_key=normalized.dedup_key,
            src_platform=platform,
            src_chat_id=normalized.chat_id,
            src_user_id=normalized.user_id,
            src_message_id=normalized.message_id,
            event_type=normalized.event_type.value,
            happened_at=normalized.happened_at,
            payload=payload,
            raw_inbox_id=inbox_id,
        )
        if event_id is None:
            return None

        action = _action_for_event(normalized.event_type)
        if action in {OutboxAction.EDIT, OutboxAction.DELETE}:
            mapping = await repo.find_mapping_by_source(
                bridge.bridge_id,
                platform,
                normalized.chat_id,
                normalized.message_id,
            )
            if mapping is None:
                await repo.insert_pending_mutation(
                    bridge_id=bridge.bridge_id,
                    dedup_key=normalized.dedup_key,
                    src_platform=platform,
                    src_chat_id=normalized.chat_id,
                    src_message_id=normalized.message_id,
                    mutation_type=action.value,
                    payload=payload,
                )
                return None
            payload["dst_message_id"] = mapping.dst_message_id

        version_key = _payload_version(payload)
        await repo.enqueue_outbox(
            bridge_id=bridge.bridge_id,
            dedup_key=outbox_dedup_key(
                bridge.bridge_id,
                platform,
                normalized.chat_id,
                normalized.message_id,
                dst_platform,
                action,
                version_key,
            ),
            src_event_id=event_id,
            dst_platform=dst_platform,
            action=action,
            partition_key=partition_key(bridge.bridge_id, platform, dst_platform),
            task=payload,
        )
        return None

    async def _build_payload(
        self,
        repo: Repository,
        normalized: NormalizedUpdate,
        bridge_id: uuid.UUID,
    ) -> dict[str, object]:
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
        placeholder = _media_text_hint(normalized.payload)
        rendered_plain = (
            render_media_caption(
                alias,
                normalized.text,
                forwarded=normalized.forwarded,
                reply_hint=reply_hint,
            )
            if media is not None
            else render_mirror_text(
                alias,
                normalized.text,
                forwarded=normalized.forwarded,
                reply_hint=reply_hint,
                media_hint=placeholder,
            )
        )
        rendered_html = (
            render_media_caption_html(
                alias,
                normalized.text,
                normalized.formatted_html,
                forwarded=normalized.forwarded,
                reply_hint=reply_hint,
            )
            if media is not None
            else render_mirror_html(
                alias,
                normalized.text,
                normalized.formatted_html,
                forwarded=normalized.forwarded,
                reply_hint=reply_hint,
                media_hint=placeholder,
            )
        )
        fallback_text = render_mirror_text(
            alias,
            normalized.text,
            forwarded=normalized.forwarded,
            reply_hint=reply_hint,
            media_hint=placeholder,
        )
        return {
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
            "has_media": media is not None,
            "media_kind": media.get("kind") if media is not None else None,
            "media": media,
            "version": normalized.event_version,
        }

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


def _media_text_hint(payload: dict[str, Any] | None) -> str | None:
    media = payload.get("media") if isinstance(payload, dict) else None
    if not isinstance(media, dict):
        return None
    text_hint = media.get("text_hint")
    return str(text_hint) if text_hint is not None else None


def _payload_version(payload: dict[str, object]) -> str | int | None:
    version = payload.get("version")
    return version if isinstance(version, str | int) else None
