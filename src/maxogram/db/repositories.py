from __future__ import annotations

import secrets
import uuid
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

from sqlalchemy import Select, func, select, text, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from maxogram.domain import (
    BridgeStatus,
    DeliveryOutcome,
    OutboxAction,
    Platform,
    RowStatus,
    TaskStatus,
)

from .models import (
    Alias,
    AliasAudit,
    BotCredential,
    Bridge,
    BridgeAdmin,
    BridgeChat,
    CanonicalEvent,
    DeadLetter,
    DeliveryAttempt,
    InboxUpdate,
    LinkCode,
    MessageChunk,
    MessageChunkMember,
    MessageMapping,
    OutboxTask,
    PendingMutation,
    PlatformCursor,
    PlatformIdentity,
    ProxyProfile,
    Tenant,
    TelegramMediaGroupBuffer,
    TelegramMediaGroupBufferMember,
)


class Repository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def ensure_bot_credential(self, platform: Platform) -> uuid.UUID:
        existing = await self.session.scalar(
            select(BotCredential.bot_id).where(BotCredential.platform == platform)
        )
        if existing:
            return existing
        bot_id = uuid.uuid4()
        stmt = (
            insert(BotCredential)
            .values(
                bot_id=bot_id,
                platform=platform,
                token_ciphertext=b"",
                token_kid="local-file",
                is_active=True,
            )
            .on_conflict_do_nothing(index_elements=["platform"])
        )
        await self.session.execute(stmt)
        saved = await self.session.scalar(
            select(BotCredential.bot_id).where(BotCredential.platform == platform)
        )
        return saved or bot_id

    async def ensure_proxy_profile(self, platform: Platform) -> None:
        await self.session.execute(
            insert(ProxyProfile)
            .values(platform=platform)
            .on_conflict_do_nothing(index_elements=["platform"])
        )

    async def get_cursor(self, platform: Platform, bot_id: uuid.UUID) -> int | None:
        return await self.session.scalar(
            select(PlatformCursor.cursor_value).where(
                PlatformCursor.platform == platform,
                PlatformCursor.bot_id == bot_id,
            )
        )

    async def upsert_cursor(
        self,
        platform: Platform,
        bot_id: uuid.UUID,
        cursor_value: int,
    ) -> None:
        stmt = (
            insert(PlatformCursor)
            .values(platform=platform, bot_id=bot_id, cursor_value=cursor_value)
            .on_conflict_do_update(
                index_elements=["platform", "bot_id"],
                set_={"cursor_value": cursor_value, "updated_at": func.now()},
            )
        )
        await self.session.execute(stmt)

    async def insert_inbox_update(
        self,
        platform: Platform,
        bot_id: uuid.UUID,
        update_key: str,
        raw: dict[str, Any],
    ) -> bool:
        stmt = (
            insert(InboxUpdate)
            .values(
                inbox_id=uuid.uuid4(),
                platform=platform,
                bot_id=bot_id,
                update_key=update_key,
                raw=raw,
            )
            .on_conflict_do_nothing(
                index_elements=["platform", "bot_id", "update_key"]
            )
        )
        result = await self.session.execute(stmt)
        return _rowcount(result) == 1

    async def claim_inbox(self, limit: int) -> list[InboxUpdate]:
        stmt: Select[tuple[InboxUpdate]] = (
            select(InboxUpdate)
            .where(InboxUpdate.status == RowStatus.NEW)
            .order_by(InboxUpdate.received_at, InboxUpdate.inbox_id)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        return list((await self.session.scalars(stmt)).all())

    async def mark_inbox(self, inbox: InboxUpdate, status: RowStatus) -> None:
        inbox.status = status

    async def buffer_telegram_media_group_update(
        self,
        *,
        chat_id: str,
        media_group_id: str,
        group_key: str,
        message_id: str,
        raw_message: dict[str, Any],
        flush_after: datetime,
    ) -> None:
        buffer = await self.session.scalar(
            select(TelegramMediaGroupBuffer)
            .where(TelegramMediaGroupBuffer.group_key == group_key)
            .with_for_update()
        )
        if buffer is None:
            buffer = TelegramMediaGroupBuffer(
                buffer_id=uuid.uuid4(),
                group_key=group_key,
                chat_id=chat_id,
                media_group_id=media_group_id,
                anchor_message_id=message_id,
                pending_flush=True,
                has_flushed=False,
                flush_after=flush_after,
                updated_at=datetime.now(UTC),
            )
            self.session.add(buffer)
            await self.session.flush()
        else:
            buffer.chat_id = chat_id
            buffer.media_group_id = media_group_id
            buffer.pending_flush = True
            buffer.flush_after = max(buffer.flush_after, flush_after)
            if buffer.anchor_message_id is None:
                buffer.anchor_message_id = message_id
            buffer.updated_at = datetime.now(UTC)

        member = await self.session.scalar(
            select(TelegramMediaGroupBufferMember)
            .where(
                TelegramMediaGroupBufferMember.buffer_id == buffer.buffer_id,
                TelegramMediaGroupBufferMember.message_id == message_id,
            )
            .with_for_update()
        )
        if member is None:
            max_position = await self.session.scalar(
                select(func.max(TelegramMediaGroupBufferMember.position)).where(
                    TelegramMediaGroupBufferMember.buffer_id == buffer.buffer_id
                )
            )
            self.session.add(
                TelegramMediaGroupBufferMember(
                    buffer_member_id=uuid.uuid4(),
                    buffer_id=buffer.buffer_id,
                    message_id=message_id,
                    position=int(max_position or 0) + 1,
                    raw_message=raw_message,
                    updated_at=datetime.now(UTC),
                )
            )
        else:
            member.raw_message = raw_message
            member.updated_at = datetime.now(UTC)

    async def claim_flushable_telegram_media_groups(
        self,
        limit: int,
    ) -> list[TelegramMediaGroupBuffer]:
        now = datetime.now(UTC)
        stmt = (
            select(TelegramMediaGroupBuffer)
            .where(
                TelegramMediaGroupBuffer.pending_flush.is_(True),
                TelegramMediaGroupBuffer.flush_after <= now,
            )
            .order_by(
                TelegramMediaGroupBuffer.flush_after,
                TelegramMediaGroupBuffer.created_at,
            )
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        return list((await self.session.scalars(stmt)).all())

    async def list_telegram_media_group_members(
        self,
        buffer_id: uuid.UUID,
    ) -> list[TelegramMediaGroupBufferMember]:
        stmt = (
            select(TelegramMediaGroupBufferMember)
            .where(TelegramMediaGroupBufferMember.buffer_id == buffer_id)
            .order_by(TelegramMediaGroupBufferMember.position)
        )
        return list((await self.session.scalars(stmt)).all())

    async def mark_telegram_media_group_flushed(
        self,
        buffer: TelegramMediaGroupBuffer,
    ) -> None:
        buffer.pending_flush = False
        buffer.has_flushed = True
        buffer.updated_at = datetime.now(UTC)

    async def find_bridge_by_chat(
        self,
        platform: Platform,
        chat_id: str,
        *,
        include_paused: bool = False,
    ) -> Bridge | None:
        stmt = (
            select(Bridge)
            .join(BridgeChat, BridgeChat.bridge_id == Bridge.bridge_id)
            .where(BridgeChat.platform == platform, BridgeChat.chat_id == chat_id)
        )
        if not include_paused:
            stmt = stmt.where(Bridge.status == BridgeStatus.ACTIVE)
        return await self.session.scalar(stmt)

    async def find_bridge_chat(
        self, bridge_id: uuid.UUID, platform: Platform
    ) -> BridgeChat | None:
        return await self.session.get(
            BridgeChat, {"bridge_id": bridge_id, "platform": platform}
        )

    async def find_other_chat(
        self, bridge_id: uuid.UUID, source_platform: Platform
    ) -> BridgeChat | None:
        destination = (
            Platform.MAX if source_platform == Platform.TELEGRAM else Platform.TELEGRAM
        )
        return await self.find_bridge_chat(bridge_id, destination)

    async def upsert_identity(
        self,
        platform: Platform,
        user_id: str,
        *,
        username: str | None,
        first_name: str | None,
        last_name: str | None,
        is_bot: bool | None,
    ) -> None:
        stmt = (
            insert(PlatformIdentity)
            .values(
                platform=platform,
                user_id=user_id,
                username=username,
                first_name=first_name,
                last_name=last_name,
                is_bot=is_bot,
                updated_at=func.now(),
            )
            .on_conflict_do_update(
                index_elements=["platform", "user_id"],
                set_={
                    "username": username,
                    "first_name": first_name,
                    "last_name": last_name,
                    "is_bot": is_bot,
                    "updated_at": func.now(),
                },
            )
        )
        await self.session.execute(stmt)

    async def get_alias(
        self,
        bridge_id: uuid.UUID,
        platform: Platform,
        user_id: str,
    ) -> str | None:
        return await self.session.scalar(
            select(Alias.alias).where(
                Alias.bridge_id == bridge_id,
                Alias.platform == platform,
                Alias.user_id == user_id,
            )
        )

    async def set_alias(
        self,
        bridge_id: uuid.UUID,
        platform: Platform,
        user_id: str,
        alias: str,
        set_by_user_id: str,
        *,
        is_admin_override: bool,
    ) -> None:
        old_alias = await self.get_alias(bridge_id, platform, user_id)
        await self.session.execute(
            insert(Alias)
            .values(
                bridge_id=bridge_id,
                platform=platform,
                user_id=user_id,
                alias=alias,
                set_by_user_id=set_by_user_id,
                is_admin_override=is_admin_override,
            )
            .on_conflict_do_update(
                index_elements=["bridge_id", "platform", "user_id"],
                set_={
                    "alias": alias,
                    "set_by_user_id": set_by_user_id,
                    "is_admin_override": is_admin_override,
                    "updated_at": func.now(),
                },
            )
        )
        self.session.add(
            AliasAudit(
                audit_id=uuid.uuid4(),
                bridge_id=bridge_id,
                platform=platform,
                user_id=user_id,
                old_alias=old_alias,
                new_alias=alias,
                set_by_user_id=set_by_user_id,
            )
        )

    async def remove_alias(
        self,
        bridge_id: uuid.UUID,
        platform: Platform,
        user_id: str,
        set_by_user_id: str,
    ) -> None:
        old_alias = await self.get_alias(bridge_id, platform, user_id)
        await self.session.execute(
            text(
                "DELETE FROM aliases WHERE bridge_id = :bridge_id "
                "AND platform = :platform AND user_id = :user_id"
            ),
            {
                "bridge_id": bridge_id,
                "platform": platform.value,
                "user_id": user_id,
            },
        )
        self.session.add(
            AliasAudit(
                audit_id=uuid.uuid4(),
                bridge_id=bridge_id,
                platform=platform,
                user_id=user_id,
                old_alias=old_alias,
                new_alias=None,
                set_by_user_id=set_by_user_id,
            )
        )

    async def list_aliases(self, bridge_id: uuid.UUID) -> list[Alias]:
        stmt = (
            select(Alias)
            .where(Alias.bridge_id == bridge_id)
            .order_by(Alias.platform, Alias.alias)
        )
        return list((await self.session.scalars(stmt)).all())

    async def insert_canonical_event(
        self,
        *,
        bridge_id: uuid.UUID,
        dedup_key: str,
        src_platform: Platform,
        src_chat_id: str,
        src_user_id: str | None,
        src_message_id: str | None,
        event_type: str,
        happened_at: datetime,
        payload: dict[str, Any],
        raw_inbox_id: uuid.UUID | None,
    ) -> uuid.UUID | None:
        event_id = uuid.uuid4()
        stmt = (
            insert(CanonicalEvent)
            .values(
                event_id=event_id,
                bridge_id=bridge_id,
                dedup_key=dedup_key,
                src_platform=src_platform,
                src_chat_id=src_chat_id,
                src_user_id=src_user_id,
                src_message_id=src_message_id,
                type=event_type,
                happened_at=happened_at,
                payload=payload,
                raw_inbox_id=raw_inbox_id,
            )
            .on_conflict_do_nothing(index_elements=["dedup_key"])
        )
        result = await self.session.execute(stmt)
        return event_id if _rowcount(result) == 1 else None

    async def enqueue_outbox(
        self,
        *,
        bridge_id: uuid.UUID,
        dedup_key: str,
        src_event_id: uuid.UUID,
        dst_platform: Platform,
        action: OutboxAction,
        partition_key: str,
        task: dict[str, Any],
    ) -> uuid.UUID | None:
        await self.session.execute(
            text("SELECT pg_advisory_xact_lock(hashtext(:partition_key))"),
            {"partition_key": partition_key},
        )
        max_seq = await self.session.scalar(
            select(func.max(OutboxTask.seq)).where(
                OutboxTask.partition_key == partition_key
            )
        )
        outbox_id = uuid.uuid4()
        stmt = (
            insert(OutboxTask)
            .values(
                outbox_id=outbox_id,
                bridge_id=bridge_id,
                dedup_key=dedup_key,
                src_event_id=src_event_id,
                dst_platform=dst_platform,
                action=action.value,
                partition_key=partition_key,
                seq=(max_seq or 0) + 1,
                task=task,
            )
            .on_conflict_do_nothing(index_elements=["dedup_key"])
        )
        result = await self.session.execute(stmt)
        return outbox_id if _rowcount(result) == 1 else None

    async def find_mapping_by_source(
        self,
        bridge_id: uuid.UUID,
        src_platform: Platform,
        src_chat_id: str,
        src_message_id: str,
    ) -> MessageMapping | None:
        mapping = await self.session.scalar(
            select(MessageMapping).where(
                MessageMapping.bridge_id == bridge_id,
                MessageMapping.src_platform == src_platform,
                MessageMapping.src_chat_id == src_chat_id,
                MessageMapping.src_message_id == src_message_id,
            )
        )
        if mapping is not None:
            return mapping
        return await self._find_chunk_mapping_by_member(
            bridge_id=bridge_id,
            platform=src_platform,
            chat_id=src_chat_id,
            message_id=src_message_id,
            member_role="src",
        )

    async def find_mapping_by_destination(
        self,
        bridge_id: uuid.UUID,
        dst_platform: Platform,
        dst_chat_id: str,
        dst_message_id: str,
    ) -> MessageMapping | None:
        mapping = await self.session.scalar(
            select(MessageMapping).where(
                MessageMapping.bridge_id == bridge_id,
                MessageMapping.dst_platform == dst_platform,
                MessageMapping.dst_chat_id == dst_chat_id,
                MessageMapping.dst_message_id == dst_message_id,
            )
        )
        if mapping is not None:
            return mapping
        return await self._find_chunk_mapping_by_member(
            bridge_id=bridge_id,
            platform=dst_platform,
            chat_id=dst_chat_id,
            message_id=dst_message_id,
            member_role="dst",
        )

    async def list_destination_message_ids(
        self,
        bridge_id: uuid.UUID,
        src_platform: Platform,
        src_chat_id: str,
        src_message_id: str,
    ) -> list[str]:
        chunk = await self.session.scalar(
            select(MessageChunk).where(
                MessageChunk.bridge_id == bridge_id,
                MessageChunk.src_platform == src_platform,
                MessageChunk.src_chat_id == src_chat_id,
                MessageChunk.src_message_id == src_message_id,
            )
        )
        if chunk is not None:
            members = await self.session.scalars(
                select(MessageChunkMember.message_id)
                .where(
                    MessageChunkMember.chunk_id == chunk.chunk_id,
                    MessageChunkMember.member_role == "dst",
                )
                .order_by(MessageChunkMember.position)
            )
            member_ids = list(members.all())
            return member_ids or [chunk.dst_message_id]

        mapping = await self.find_mapping_by_source(
            bridge_id,
            src_platform,
            src_chat_id,
            src_message_id,
        )
        if mapping is None:
            return []
        return [mapping.dst_message_id]

    async def get_created_event_payload(
        self,
        bridge_id: uuid.UUID,
        src_platform: Platform,
        src_chat_id: str,
        src_message_id: str,
    ) -> dict[str, Any] | None:
        return await self.session.scalar(
            select(CanonicalEvent.payload).where(
                CanonicalEvent.bridge_id == bridge_id,
                CanonicalEvent.src_platform == src_platform,
                CanonicalEvent.src_chat_id == src_chat_id,
                CanonicalEvent.src_message_id == src_message_id,
                CanonicalEvent.type == "message.created",
            )
        )

    async def upsert_message_chunk(
        self,
        *,
        bridge_id: uuid.UUID,
        group_kind: str,
        src_platform: Platform,
        src_chat_id: str,
        src_message_id: str,
        dst_platform: Platform,
        dst_chat_id: str,
        dst_message_id: str,
    ) -> uuid.UUID:
        chunk_id = uuid.uuid4()
        stmt = (
            insert(MessageChunk)
            .values(
                chunk_id=chunk_id,
                bridge_id=bridge_id,
                group_kind=group_kind,
                src_platform=src_platform,
                src_chat_id=src_chat_id,
                src_message_id=src_message_id,
                dst_platform=dst_platform,
                dst_chat_id=dst_chat_id,
                dst_message_id=dst_message_id,
                updated_at=func.now(),
            )
            .on_conflict_do_update(
                index_elements=[
                    "bridge_id",
                    "src_platform",
                    "src_chat_id",
                    "src_message_id",
                ],
                set_={
                    "group_kind": group_kind,
                    "dst_platform": dst_platform,
                    "dst_chat_id": dst_chat_id,
                    "dst_message_id": dst_message_id,
                    "updated_at": func.now(),
                },
            )
        )
        await self.session.execute(stmt)
        saved = await self.session.scalar(
            select(MessageChunk.chunk_id).where(
                MessageChunk.bridge_id == bridge_id,
                MessageChunk.src_platform == src_platform,
                MessageChunk.src_chat_id == src_chat_id,
                MessageChunk.src_message_id == src_message_id,
            )
        )
        return saved or chunk_id

    async def replace_message_chunk_members(
        self,
        *,
        chunk_id: uuid.UUID,
        bridge_id: uuid.UUID,
        member_role: str,
        platform: Platform,
        chat_id: str,
        message_ids: list[str],
    ) -> None:
        await self.session.execute(
            text(
                "DELETE FROM message_chunk_members "
                "WHERE chunk_id = :chunk_id AND member_role = :member_role"
            ),
            {"chunk_id": chunk_id, "member_role": member_role},
        )
        for position, message_id in enumerate(message_ids, start=1):
            self.session.add(
                MessageChunkMember(
                    chunk_member_id=uuid.uuid4(),
                    chunk_id=chunk_id,
                    bridge_id=bridge_id,
                    member_role=member_role,
                    platform=platform,
                    chat_id=chat_id,
                    message_id=message_id,
                    position=position,
                    updated_at=datetime.now(UTC),
                )
            )

    async def find_canonical_event_id_by_dedup_key(
        self, dedup_key: str
    ) -> uuid.UUID | None:
        return await self.session.scalar(
            select(CanonicalEvent.event_id).where(CanonicalEvent.dedup_key == dedup_key)
        )

    async def insert_message_mapping(
        self,
        *,
        bridge_id: uuid.UUID,
        src_platform: Platform,
        src_chat_id: str,
        src_message_id: str,
        dst_platform: Platform,
        dst_chat_id: str,
        dst_message_id: str,
    ) -> None:
        await self.session.execute(
            insert(MessageMapping)
            .values(
                mapping_id=uuid.uuid4(),
                bridge_id=bridge_id,
                src_platform=src_platform,
                src_chat_id=src_chat_id,
                src_message_id=src_message_id,
                dst_platform=dst_platform,
                dst_chat_id=dst_chat_id,
                dst_message_id=dst_message_id,
            )
            .on_conflict_do_update(
                index_elements=[
                    "bridge_id",
                    "src_platform",
                    "src_chat_id",
                    "src_message_id",
                ],
                set_={
                    "dst_platform": dst_platform,
                    "dst_chat_id": dst_chat_id,
                    "dst_message_id": dst_message_id,
                },
            )
        )

    async def insert_pending_mutation(
        self,
        *,
        bridge_id: uuid.UUID,
        dedup_key: str,
        src_platform: Platform,
        src_chat_id: str,
        src_message_id: str,
        mutation_type: str,
        payload: dict[str, Any],
    ) -> None:
        now = datetime.now(UTC)
        await self.session.execute(
            insert(PendingMutation)
            .values(
                pending_id=uuid.uuid4(),
                bridge_id=bridge_id,
                dedup_key=dedup_key,
                src_platform=src_platform,
                src_chat_id=src_chat_id,
                src_message_id=src_message_id,
                mutation_type=mutation_type,
                payload=payload,
                next_attempt_at=now + timedelta(seconds=5),
                expires_at=now + timedelta(minutes=3),
            )
            .on_conflict_do_nothing(index_elements=["dedup_key"])
        )

    async def claim_pending_mutations(self, limit: int) -> list[PendingMutation]:
        now = datetime.now(UTC)
        stmt = (
            select(PendingMutation)
            .where(
                PendingMutation.status == TaskStatus.RETRY_WAIT,
                PendingMutation.next_attempt_at <= now,
                PendingMutation.expires_at > now,
            )
            .order_by(PendingMutation.next_attempt_at, PendingMutation.created_at)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        return list((await self.session.scalars(stmt)).all())

    async def mark_pending_mutation_done(self, pending: PendingMutation) -> None:
        pending.status = TaskStatus.DONE

    async def reschedule_pending_mutation(
        self,
        pending: PendingMutation,
        *,
        next_attempt_at: datetime,
    ) -> None:
        pending.status = TaskStatus.RETRY_WAIT
        pending.next_attempt_at = next_attempt_at

    async def claim_outbox(self, limit: int, lease_seconds: int) -> list[OutboxTask]:
        now = datetime.now(UTC)
        stmt = (
            select(OutboxTask)
            .where(
                OutboxTask.status.in_([TaskStatus.READY, TaskStatus.RETRY_WAIT]),
                OutboxTask.next_attempt_at <= now,
            )
            .order_by(OutboxTask.partition_key, OutboxTask.seq)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        tasks = list((await self.session.scalars(stmt)).all())
        for task in tasks:
            task.status = TaskStatus.INFLIGHT
            task.attempt_count += 1
            task.inflight_until = now + timedelta(seconds=lease_seconds)
        return tasks

    async def get_outbox_task(self, outbox_id: uuid.UUID) -> OutboxTask | None:
        return await self.session.get(OutboxTask, outbox_id)

    async def renew_outbox_lease(
        self,
        outbox_id: uuid.UUID,
        attempt_count: int,
        lease_seconds: int,
    ) -> bool:
        result = await self.session.execute(
            update(OutboxTask)
            .where(
                OutboxTask.outbox_id == outbox_id,
                OutboxTask.status == TaskStatus.INFLIGHT,
                OutboxTask.attempt_count == attempt_count,
            )
            .values(
                inflight_until=datetime.now(UTC) + timedelta(seconds=lease_seconds)
            )
        )
        return _rowcount(result) == 1

    async def mark_outbox_done(self, task: OutboxTask) -> None:
        task.status = TaskStatus.DONE
        task.inflight_until = None

    async def schedule_outbox_retry(
        self,
        task: OutboxTask,
        *,
        next_attempt_at: datetime,
    ) -> None:
        task.status = TaskStatus.RETRY_WAIT
        task.next_attempt_at = next_attempt_at
        task.inflight_until = None

    async def mark_outbox_dead(self, task: OutboxTask, reason: str) -> None:
        task.status = TaskStatus.DEAD
        task.inflight_until = None
        self.session.add(
            DeadLetter(
                dlq_id=uuid.uuid4(),
                bridge_id=task.bridge_id,
                outbox_id=task.outbox_id,
                reason=reason,
                payload=task.task,
            )
        )

    async def record_attempt(
        self,
        task: OutboxTask,
        *,
        outcome: DeliveryOutcome,
        http_status: int | None = None,
        error_code: str | None = None,
        error_message: str | None = None,
    ) -> None:
        self.session.add(
            DeliveryAttempt(
                attempt_id=uuid.uuid4(),
                outbox_id=task.outbox_id,
                attempt_no=task.attempt_count,
                finished_at=datetime.now(UTC),
                outcome=outcome.value,
                http_status=http_status,
                error_code=error_code,
                error_message=error_message,
            )
        )

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
    ) -> bool:
        task = await self._get_inflight_outbox(outbox_id, attempt_count)
        if task is None:
            return False
        if (
            dst_message_id is not None
            and src_platform is not None
            and src_chat_id is not None
            and src_message_id is not None
        ):
            await self.insert_message_mapping(
                bridge_id=bridge_id,
                src_platform=src_platform,
                src_chat_id=src_chat_id,
                src_message_id=src_message_id,
                dst_platform=dst_platform,
                dst_chat_id=dst_chat_id,
                dst_message_id=dst_message_id,
            )
        if (
            group_kind is not None
            and dst_message_id is not None
            and src_platform is not None
            and src_chat_id is not None
            and src_message_id is not None
        ):
            chunk_id = await self.upsert_message_chunk(
                bridge_id=bridge_id,
                group_kind=group_kind,
                src_platform=src_platform,
                src_chat_id=src_chat_id,
                src_message_id=src_message_id,
                dst_platform=dst_platform,
                dst_chat_id=dst_chat_id,
                dst_message_id=dst_message_id,
            )
            if src_member_message_ids:
                await self.replace_message_chunk_members(
                    chunk_id=chunk_id,
                    bridge_id=bridge_id,
                    member_role="src",
                    platform=src_platform,
                    chat_id=src_chat_id,
                    message_ids=src_member_message_ids,
                )
            effective_dst_message_ids = dst_message_ids or [dst_message_id]
            if effective_dst_message_ids:
                await self.replace_message_chunk_members(
                    chunk_id=chunk_id,
                    bridge_id=bridge_id,
                    member_role="dst",
                    platform=dst_platform,
                    chat_id=dst_chat_id,
                    message_ids=effective_dst_message_ids,
                )
        task.status = TaskStatus.DONE
        task.inflight_until = None
        self._add_delivery_attempt(
            outbox_id=outbox_id,
            attempt_no=attempt_count,
            outcome=DeliveryOutcome.SUCCESS,
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
        task = await self._get_inflight_outbox(outbox_id, attempt_count)
        if task is None:
            return False
        task.status = TaskStatus.RETRY_WAIT
        task.next_attempt_at = next_attempt_at
        task.inflight_until = None
        self._add_delivery_attempt(
            outbox_id=outbox_id,
            attempt_no=attempt_count,
            outcome=DeliveryOutcome.RETRY,
            http_status=http_status,
            error_code=error_code,
            error_message=error_message,
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
        task = await self._get_inflight_outbox(outbox_id, attempt_count)
        if task is None:
            return False
        task.status = TaskStatus.DEAD
        task.inflight_until = None
        self.session.add(
            DeadLetter(
                dlq_id=uuid.uuid4(),
                bridge_id=bridge_id,
                outbox_id=outbox_id,
                reason=reason,
                payload=payload,
            )
        )
        self._add_delivery_attempt(
            outbox_id=outbox_id,
            attempt_no=attempt_count,
            outcome=DeliveryOutcome.DEAD,
            http_status=http_status,
            error_code=error_code,
            error_message=error_message,
        )
        return True

    async def reset_expired_inflight(self) -> int:
        result = await self.session.execute(
            update(OutboxTask)
            .where(
                OutboxTask.status == TaskStatus.INFLIGHT,
                OutboxTask.inflight_until <= datetime.now(UTC),
            )
            .values(
                status=TaskStatus.RETRY_WAIT,
                inflight_until=None,
                next_attempt_at=func.now(),
            )
        )
        return _rowcount(result)

    async def expire_pending_mutations(self) -> int:
        rows = list(
            (
                await self.session.scalars(
                    select(PendingMutation)
                    .where(
                        PendingMutation.status == TaskStatus.RETRY_WAIT,
                        PendingMutation.expires_at <= datetime.now(UTC),
                    )
                    .with_for_update(skip_locked=True)
                )
            ).all()
        )
        for pending in rows:
            pending.status = TaskStatus.DEAD
            self.session.add(
                DeadLetter(
                    dlq_id=uuid.uuid4(),
                    bridge_id=pending.bridge_id,
                    outbox_id=None,
                    reason="missing_mapping_after_3m",
                    payload=pending.payload,
                )
            )
        return len(rows)

    async def create_link_code(
        self,
        platform: Platform,
        chat_id: str,
        user_id: str,
        *,
        ttl_seconds: int = 180,
    ) -> str:
        code = f"{secrets.randbelow(1_000_000):06d}"
        self.session.add(
            LinkCode(
                link_code_id=uuid.uuid4(),
                code=code,
                src_platform=platform,
                src_chat_id=chat_id,
                src_user_id=user_id,
                expires_at=datetime.now(UTC) + timedelta(seconds=ttl_seconds),
            )
        )
        return code

    async def consume_link_code(
        self,
        code: str,
        dst_platform: Platform,
        dst_chat_id: str,
        dst_user_id: str,
    ) -> uuid.UUID | None:
        link = await self.session.scalar(
            select(LinkCode)
            .where(
                LinkCode.code == code,
                LinkCode.consumed_at.is_(None),
                LinkCode.expires_at > datetime.now(UTC),
            )
            .with_for_update()
        )
        if link is None:
            return None
        if link.src_platform == dst_platform and link.src_chat_id == dst_chat_id:
            return None

        tenant_id = uuid.uuid4()
        bridge_id = uuid.uuid4()
        tenant = Tenant(tenant_id=tenant_id, display_name="Family")
        self.session.add(tenant)
        await self.session.flush()

        bridge = Bridge(bridge_id=bridge_id, tenant_id=tenant_id)
        self.session.add(bridge)
        await self.session.flush()

        self.session.add_all(
            [
                BridgeChat(
                    bridge_id=bridge_id,
                    platform=link.src_platform,
                    chat_id=link.src_chat_id,
                ),
                BridgeChat(
                    bridge_id=bridge_id,
                    platform=dst_platform,
                    chat_id=dst_chat_id,
                ),
                BridgeAdmin(
                    bridge_id=bridge_id,
                    platform=link.src_platform,
                    platform_user_id=link.src_user_id,
                    role="owner",
                ),
                BridgeAdmin(
                    bridge_id=bridge_id,
                    platform=dst_platform,
                    platform_user_id=dst_user_id,
                    role="owner",
                ),
            ]
        )
        link.consumed_at = datetime.now(UTC)
        return bridge_id

    async def set_bridge_status(
        self, bridge_id: uuid.UUID, status: BridgeStatus
    ) -> None:
        bridge = await self.session.get(Bridge, bridge_id)
        if bridge is not None:
            bridge.status = status
            bridge.updated_at = datetime.now(UTC)

    async def log_command(
        self,
        *,
        platform: Platform,
        chat_id: str,
        message_id: str | None,
        user_id: str,
        bridge_id: uuid.UUID | None,
        command: str,
        args: str | None,
    ) -> None:
        query = text(
            """
            INSERT INTO command_log (
                cmd_id,
                platform,
                chat_id,
                message_id,
                user_id,
                bridge_id,
                command,
                args
            )
            SELECT
                CAST(:cmd_id AS uuid),
                CAST(:platform AS platform),
                :chat_id,
                :message_id,
                :user_id,
                CAST(:bridge_id AS uuid),
                :command,
                :args
            WHERE NOT EXISTS (
                SELECT 1
                FROM command_log
                WHERE platform = CAST(:platform AS platform)
                  AND chat_id = :chat_id
                  AND message_id IS NOT DISTINCT FROM :message_id
            )
            """
        )
        await self.session.execute(
            query,
            {
                "cmd_id": uuid.uuid4(),
                "platform": platform.value,
                "chat_id": chat_id,
                "message_id": message_id,
                "user_id": user_id,
                "bridge_id": bridge_id,
                "command": command,
                "args": args,
            },
        )

    async def _find_chunk_mapping_by_member(
        self,
        *,
        bridge_id: uuid.UUID,
        platform: Platform,
        chat_id: str,
        message_id: str,
        member_role: str,
    ) -> MessageMapping | None:
        row = await self.session.execute(
            select(
                MessageChunk.src_message_id,
                MessageChunk.dst_message_id,
            )
            .join(
                MessageChunkMember,
                MessageChunkMember.chunk_id == MessageChunk.chunk_id,
            )
            .where(
                MessageChunk.bridge_id == bridge_id,
                MessageChunkMember.member_role == member_role,
                MessageChunkMember.platform == platform,
                MessageChunkMember.chat_id == chat_id,
                MessageChunkMember.message_id == message_id,
            )
        )
        result = row.first()
        if result is None:
            return None
        return SimpleNamespace(
            src_message_id=result.src_message_id,
            dst_message_id=result.dst_message_id,
        )

    async def _get_inflight_outbox(
        self,
        outbox_id: uuid.UUID,
        attempt_count: int,
    ) -> OutboxTask | None:
        stmt = (
            select(OutboxTask)
            .where(
                OutboxTask.outbox_id == outbox_id,
                OutboxTask.status == TaskStatus.INFLIGHT,
                OutboxTask.attempt_count == attempt_count,
            )
            .with_for_update()
        )
        return await self.session.scalar(stmt)

    def _add_delivery_attempt(
        self,
        *,
        outbox_id: uuid.UUID,
        attempt_no: int,
        outcome: DeliveryOutcome,
        http_status: int | None = None,
        error_code: str | None = None,
        error_message: str | None = None,
    ) -> None:
        self.session.add(
            DeliveryAttempt(
                attempt_id=uuid.uuid4(),
                outbox_id=outbox_id,
                attempt_no=attempt_no,
                finished_at=datetime.now(UTC),
                outcome=outcome.value,
                http_status=http_status,
                error_code=error_code,
                error_message=error_message,
            )
        )


def _rowcount(result: object) -> int:
    value = getattr(result, "rowcount", 0)
    return int(value or 0)
