from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    LargeBinary,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from maxogram.domain import (
    BridgeStatus,
    Platform,
    RowStatus,
    TaskStatus,
)


def _enum_values(enum_cls: type[Any]) -> list[str]:
    return [item.value for item in enum_cls]


class Base(DeclarativeBase):
    pass


platform_enum = Enum(
    Platform,
    name="platform",
    native_enum=True,
    values_callable=_enum_values,
)
bridge_status_enum = Enum(
    BridgeStatus,
    name="bridge_status",
    native_enum=True,
    values_callable=_enum_values,
)
row_status_enum = Enum(
    RowStatus,
    name="row_status",
    native_enum=True,
    values_callable=_enum_values,
)
task_status_enum = Enum(
    TaskStatus,
    name="task_status",
    native_enum=True,
    values_callable=_enum_values,
)


class Tenant(Base):
    __tablename__ = "tenants"

    tenant_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    display_name: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class BotCredential(Base):
    __tablename__ = "bot_credentials"
    __table_args__ = (UniqueConstraint("platform"),)

    bot_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    platform: Mapped[Platform] = mapped_column(platform_enum, nullable=False)
    token_ciphertext: Mapped[bytes] = mapped_column(
        LargeBinary, nullable=False, default=b""
    )
    token_kid: Mapped[str] = mapped_column(Text, nullable=False, default="local-file")
    bot_user_id: Mapped[str | None] = mapped_column(Text)
    is_active: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default="true"
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class ProxyProfile(Base):
    __tablename__ = "proxy_profiles"

    platform: Mapped[Platform] = mapped_column(platform_enum, primary_key=True)
    proxy_url: Mapped[str | None] = mapped_column(Text)
    trust_env: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default="false"
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class Bridge(Base):
    __tablename__ = "bridges"

    bridge_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    tenant_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("tenants.tenant_id"), nullable=False
    )
    status: Mapped[BridgeStatus] = mapped_column(
        bridge_status_enum, nullable=False, server_default=BridgeStatus.ACTIVE.value
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class BridgeChat(Base):
    __tablename__ = "bridge_chats"
    __table_args__ = (
        UniqueConstraint("platform", "chat_id"),
        Index("bridge_chats_platform_chat_idx", "platform", "chat_id"),
    )

    bridge_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("bridges.bridge_id"), primary_key=True
    )
    platform: Mapped[Platform] = mapped_column(platform_enum, primary_key=True)
    chat_id: Mapped[str] = mapped_column(Text, nullable=False)


class BridgeAdmin(Base):
    __tablename__ = "bridge_admins"
    __table_args__ = (
        CheckConstraint("role IN ('admin','owner')", name="bridge_admins_role_ck"),
        Index("bridge_admins_bridge_platform_idx", "bridge_id", "platform"),
    )

    bridge_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("bridges.bridge_id"), primary_key=True
    )
    platform: Mapped[Platform] = mapped_column(platform_enum, primary_key=True)
    platform_user_id: Mapped[str] = mapped_column(Text, primary_key=True)
    role: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class PlatformIdentity(Base):
    __tablename__ = "platform_identities"

    platform: Mapped[Platform] = mapped_column(platform_enum, primary_key=True)
    user_id: Mapped[str] = mapped_column(Text, primary_key=True)
    username: Mapped[str | None] = mapped_column(Text)
    first_name: Mapped[str | None] = mapped_column(Text)
    last_name: Mapped[str | None] = mapped_column(Text)
    is_bot: Mapped[bool | None] = mapped_column(Boolean)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class Alias(Base):
    __tablename__ = "aliases"

    bridge_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("bridges.bridge_id"), primary_key=True
    )
    platform: Mapped[Platform] = mapped_column(platform_enum, primary_key=True)
    user_id: Mapped[str] = mapped_column(Text, primary_key=True)
    alias: Mapped[str] = mapped_column(Text, nullable=False)
    set_by_user_id: Mapped[str] = mapped_column(Text, nullable=False)
    is_admin_override: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default="false"
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class AliasAudit(Base):
    __tablename__ = "alias_audit"

    audit_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    bridge_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    platform: Mapped[Platform] = mapped_column(platform_enum, nullable=False)
    user_id: Mapped[str] = mapped_column(Text, nullable=False)
    old_alias: Mapped[str | None] = mapped_column(Text)
    new_alias: Mapped[str | None] = mapped_column(Text)
    set_by_user_id: Mapped[str] = mapped_column(Text, nullable=False)
    changed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class PlatformCursor(Base):
    __tablename__ = "platform_cursors"

    platform: Mapped[Platform] = mapped_column(platform_enum, primary_key=True)
    bot_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("bot_credentials.bot_id"), primary_key=True
    )
    cursor_value: Mapped[int] = mapped_column(BigInteger, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class InboxUpdate(Base):
    __tablename__ = "inbox_updates"
    __table_args__ = (
        UniqueConstraint("platform", "bot_id", "update_key"),
        Index("inbox_updates_work_idx", "status", "received_at"),
    )

    inbox_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    platform: Mapped[Platform] = mapped_column(platform_enum, nullable=False)
    bot_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("bot_credentials.bot_id"), nullable=False
    )
    update_key: Mapped[str] = mapped_column(Text, nullable=False)
    raw: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    status: Mapped[RowStatus] = mapped_column(
        row_status_enum, nullable=False, server_default=RowStatus.NEW.value
    )
    received_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class CanonicalEvent(Base):
    __tablename__ = "canonical_events"
    __table_args__ = (
        UniqueConstraint("dedup_key"),
        Index("canonical_events_bridge_created_idx", "bridge_id", "created_at"),
    )

    event_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    bridge_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("bridges.bridge_id"), nullable=False
    )
    dedup_key: Mapped[str] = mapped_column(Text, nullable=False)
    src_platform: Mapped[Platform] = mapped_column(platform_enum, nullable=False)
    src_chat_id: Mapped[str] = mapped_column(Text, nullable=False)
    src_user_id: Mapped[str | None] = mapped_column(Text)
    src_message_id: Mapped[str | None] = mapped_column(Text)
    type: Mapped[str] = mapped_column(Text, nullable=False)
    happened_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    raw_inbox_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("inbox_updates.inbox_id")
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class MessageMapping(Base):
    __tablename__ = "message_mappings"
    __table_args__ = (
        UniqueConstraint(
            "bridge_id",
            "src_platform",
            "src_chat_id",
            "src_message_id",
            name="message_mappings_src_uq",
        ),
        UniqueConstraint(
            "bridge_id",
            "dst_platform",
            "dst_chat_id",
            "dst_message_id",
            name="message_mappings_dst_uq",
        ),
        Index(
            "message_mappings_dst_idx",
            "bridge_id",
            "dst_platform",
            "dst_message_id",
        ),
    )

    mapping_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    bridge_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("bridges.bridge_id"), nullable=False
    )
    src_platform: Mapped[Platform] = mapped_column(platform_enum, nullable=False)
    src_chat_id: Mapped[str] = mapped_column(Text, nullable=False)
    src_message_id: Mapped[str] = mapped_column(Text, nullable=False)
    dst_platform: Mapped[Platform] = mapped_column(platform_enum, nullable=False)
    dst_chat_id: Mapped[str] = mapped_column(Text, nullable=False)
    dst_message_id: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class PendingMutation(Base):
    __tablename__ = "pending_mutations"
    __table_args__ = (
        UniqueConstraint("dedup_key"),
        CheckConstraint(
            "mutation_type IN ('edit','delete')", name="pending_mutations_type_ck"
        ),
        Index("pending_mutations_idx", "status", "next_attempt_at"),
    )

    pending_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    bridge_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("bridges.bridge_id"), nullable=False
    )
    dedup_key: Mapped[str] = mapped_column(Text, nullable=False)
    src_platform: Mapped[Platform] = mapped_column(platform_enum, nullable=False)
    src_chat_id: Mapped[str] = mapped_column(Text, nullable=False)
    src_message_id: Mapped[str] = mapped_column(Text, nullable=False)
    mutation_type: Mapped[str] = mapped_column(Text, nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    status: Mapped[TaskStatus] = mapped_column(
        task_status_enum, nullable=False, server_default=TaskStatus.RETRY_WAIT.value
    )
    next_attempt_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    expires_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class OutboxTask(Base):
    __tablename__ = "outbox_tasks"
    __table_args__ = (
        UniqueConstraint("dedup_key"),
        UniqueConstraint("partition_key", "seq", name="outbox_tasks_partition_seq_uq"),
        CheckConstraint("action IN ('send','edit','delete')", name="outbox_action_ck"),
        Index("outbox_ready_idx", "status", "next_attempt_at"),
        Index("outbox_order_idx", "partition_key", "seq"),
    )

    outbox_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    bridge_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("bridges.bridge_id"), nullable=False
    )
    dedup_key: Mapped[str] = mapped_column(Text, nullable=False)
    src_event_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("canonical_events.event_id"), nullable=False
    )
    dst_platform: Mapped[Platform] = mapped_column(platform_enum, nullable=False)
    action: Mapped[str] = mapped_column(Text, nullable=False)
    partition_key: Mapped[str] = mapped_column(Text, nullable=False)
    seq: Mapped[int] = mapped_column(BigInteger, nullable=False)
    task: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    status: Mapped[TaskStatus] = mapped_column(
        task_status_enum, nullable=False, server_default=TaskStatus.READY.value
    )
    attempt_count: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default="0"
    )
    next_attempt_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    inflight_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class DeliveryAttempt(Base):
    __tablename__ = "delivery_attempts"
    __table_args__ = (
        UniqueConstraint("outbox_id", "attempt_no"),
        Index("delivery_attempts_outbox_idx", "outbox_id"),
    )

    attempt_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    outbox_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("outbox_tasks.outbox_id"), nullable=False
    )
    attempt_no: Mapped[int] = mapped_column(Integer, nullable=False)
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    outcome: Mapped[str] = mapped_column(Text, nullable=False)
    http_status: Mapped[int | None] = mapped_column(Integer)
    error_code: Mapped[str | None] = mapped_column(Text)
    error_message: Mapped[str | None] = mapped_column(Text)


class DeadLetter(Base):
    __tablename__ = "dead_letters"
    __table_args__ = (
        Index("dead_letters_bridge_created_idx", "bridge_id", "created_at"),
    )

    dlq_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    bridge_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    outbox_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True))
    reason: Mapped[str] = mapped_column(Text, nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class CommandLog(Base):
    __tablename__ = "command_log"
    __table_args__ = (
        UniqueConstraint(
            "platform", "chat_id", "message_id", name="command_log_msg_uq"
        ),
        Index("command_log_bridge_created_idx", "bridge_id", "created_at"),
    )

    cmd_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    platform: Mapped[Platform] = mapped_column(platform_enum, nullable=False)
    chat_id: Mapped[str] = mapped_column(Text, nullable=False)
    message_id: Mapped[str | None] = mapped_column(Text)
    user_id: Mapped[str] = mapped_column(Text, nullable=False)
    bridge_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True))
    command: Mapped[str] = mapped_column(Text, nullable=False)
    args: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class MediaObject(Base):
    __tablename__ = "media_objects"
    __table_args__ = (Index("media_objects_exp_idx", "expires_at"),)

    media_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    bridge_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True))
    src_platform: Mapped[Platform | None] = mapped_column(platform_enum)
    src_message_id: Mapped[str | None] = mapped_column(Text)
    content_hash: Mapped[str | None] = mapped_column(Text)
    mime_type: Mapped[str | None] = mapped_column(Text)
    byte_size: Mapped[int | None] = mapped_column(BigInteger)
    local_path: Mapped[str | None] = mapped_column(Text)
    expires_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class LinkCode(Base):
    __tablename__ = "link_codes"
    __table_args__ = (
        UniqueConstraint("code"),
        Index("link_codes_code_idx", "code", "expires_at"),
    )

    link_code_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    code: Mapped[str] = mapped_column(Text, nullable=False)
    src_platform: Mapped[Platform] = mapped_column(platform_enum, nullable=False)
    src_chat_id: Mapped[str] = mapped_column(Text, nullable=False)
    src_user_id: Mapped[str] = mapped_column(Text, nullable=False)
    expires_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    consumed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
