from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from maxogram.db.models import platform_enum

revision = "20260419_0002"
down_revision = "20260410_0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "telegram_media_group_buffers",
        sa.Column(
            "buffer_id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
        ),
        sa.Column("group_key", sa.Text(), nullable=False),
        sa.Column("chat_id", sa.Text(), nullable=False),
        sa.Column("media_group_id", sa.Text(), nullable=False),
        sa.Column("anchor_message_id", sa.Text(), nullable=True),
        sa.Column(
            "pending_flush",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("true"),
        ),
        sa.Column(
            "has_flushed",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
        sa.Column("flush_after", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.UniqueConstraint("group_key"),
    )
    op.create_index(
        "telegram_media_group_buffers_flush_idx",
        "telegram_media_group_buffers",
        ["pending_flush", "flush_after"],
        unique=False,
    )

    op.create_table(
        "telegram_media_group_buffer_members",
        sa.Column(
            "buffer_member_id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
        ),
        sa.Column(
            "buffer_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("telegram_media_group_buffers.buffer_id"),
            nullable=False,
        ),
        sa.Column("message_id", sa.Text(), nullable=False),
        sa.Column("position", sa.Integer(), nullable=False),
        sa.Column("raw_message", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.UniqueConstraint(
            "buffer_id",
            "message_id",
            name="telegram_media_group_buffer_members_msg_uq",
        ),
        sa.UniqueConstraint(
            "buffer_id",
            "position",
            name="telegram_media_group_buffer_members_position_uq",
        ),
    )
    op.create_index(
        "telegram_media_group_buffer_members_buffer_idx",
        "telegram_media_group_buffer_members",
        ["buffer_id", "position"],
        unique=False,
    )

    op.create_table(
        "message_chunks",
        sa.Column(
            "chunk_id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
        ),
        sa.Column(
            "bridge_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("bridges.bridge_id"),
            nullable=False,
        ),
        sa.Column("group_kind", sa.Text(), nullable=False),
        sa.Column("src_platform", platform_enum, nullable=False),
        sa.Column("src_chat_id", sa.Text(), nullable=False),
        sa.Column("src_message_id", sa.Text(), nullable=False),
        sa.Column("dst_platform", platform_enum, nullable=False),
        sa.Column("dst_chat_id", sa.Text(), nullable=False),
        sa.Column("dst_message_id", sa.Text(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.UniqueConstraint(
            "bridge_id",
            "src_platform",
            "src_chat_id",
            "src_message_id",
            name="message_chunks_src_uq",
        ),
    )
    op.create_index(
        "message_chunks_src_idx",
        "message_chunks",
        ["bridge_id", "src_platform", "src_chat_id", "src_message_id"],
        unique=False,
    )
    op.create_index(
        "message_chunks_dst_idx",
        "message_chunks",
        ["bridge_id", "dst_platform", "dst_chat_id", "dst_message_id"],
        unique=False,
    )

    op.create_table(
        "message_chunk_members",
        sa.Column(
            "chunk_member_id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
        ),
        sa.Column(
            "chunk_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("message_chunks.chunk_id"),
            nullable=False,
        ),
        sa.Column(
            "bridge_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("bridges.bridge_id"),
            nullable=False,
        ),
        sa.Column("member_role", sa.Text(), nullable=False),
        sa.Column("platform", platform_enum, nullable=False),
        sa.Column("chat_id", sa.Text(), nullable=False),
        sa.Column("message_id", sa.Text(), nullable=False),
        sa.Column("position", sa.Integer(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.CheckConstraint(
            "member_role IN ('src','dst')",
            name="message_chunk_members_role_ck",
        ),
        sa.UniqueConstraint(
            "chunk_id",
            "member_role",
            "position",
            name="message_chunk_members_position_uq",
        ),
        sa.UniqueConstraint(
            "bridge_id",
            "platform",
            "chat_id",
            "message_id",
            name="message_chunk_members_message_uq",
        ),
    )
    op.create_index(
        "message_chunk_members_lookup_idx",
        "message_chunk_members",
        ["bridge_id", "platform", "chat_id", "message_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("message_chunk_members_lookup_idx", table_name="message_chunk_members")
    op.drop_table("message_chunk_members")
    op.drop_index("message_chunks_dst_idx", table_name="message_chunks")
    op.drop_index("message_chunks_src_idx", table_name="message_chunks")
    op.drop_table("message_chunks")
    op.drop_index(
        "telegram_media_group_buffer_members_buffer_idx",
        table_name="telegram_media_group_buffer_members",
    )
    op.drop_table("telegram_media_group_buffer_members")
    op.drop_index(
        "telegram_media_group_buffers_flush_idx",
        table_name="telegram_media_group_buffers",
    )
    op.drop_table("telegram_media_group_buffers")
