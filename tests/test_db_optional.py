from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from sqlalchemy import delete, select, text

from maxogram.config import load_settings
from maxogram.db.models import (
    TelegramMediaGroupBuffer,
    TelegramMediaGroupBufferMember,
)
from maxogram.db.repositories import Repository
from maxogram.db.session import Database


@pytest.mark.asyncio
async def test_optional_test_db_connectivity():
    settings = load_settings()
    if settings.test_db is None:
        pytest.skip("TEST_DB_CONFIG is not configured in local tokens.py")
    database = Database(settings.test_db.sqlalchemy_url())
    async with database, database.session() as session:
        value = await session.scalar(text("SELECT 1"))
    assert value == 1


@pytest.mark.asyncio
async def test_optional_telegram_media_group_buffer_resequences_without_unique_conflict():
    settings = load_settings()
    if settings.test_db is None:
        pytest.skip("TEST_DB_CONFIG is not configured in local tokens.py")

    group_key = f"telegram:-100:optional-{uuid4()}"
    database = Database(settings.test_db.sqlalchemy_url())
    try:
        async with database, database.session() as session:
            repo = Repository(session)
            async with session.begin():
                for message_id, raw_message in [
                    (
                        "411",
                        {
                            "message_id": 411,
                            "media_group_id": "optional-grp",
                            "photo": [
                                {
                                    "file_id": "photo-411",
                                    "file_unique_id": "photo-411",
                                }
                            ],
                        },
                    ),
                    (
                        "414",
                        {
                            "message_id": 414,
                            "media_group_id": "optional-grp",
                            "video": {
                                "file_id": "video-414",
                                "file_unique_id": "video-414",
                                "file_size": 2048,
                            },
                        },
                    ),
                    (
                        "413",
                        {
                            "message_id": 413,
                            "media_group_id": "optional-grp",
                            "photo": [
                                {
                                    "file_id": "photo-413",
                                    "file_unique_id": "photo-413",
                                }
                            ],
                        },
                    ),
                    (
                        "412",
                        {
                            "message_id": 412,
                            "media_group_id": "optional-grp",
                            "video": {
                                "file_id": "video-412",
                                "file_unique_id": "video-412",
                                "file_size": 2048,
                            },
                        },
                    ),
                ]:
                    await repo.buffer_telegram_media_group_update(
                        chat_id="-100",
                        media_group_id="optional-grp",
                        group_key=group_key,
                        message_id=message_id,
                        raw_message=raw_message,
                        flush_after=datetime.now(UTC),
                    )

                await repo.buffer_telegram_media_group_update(
                    chat_id="-100",
                    media_group_id="optional-grp",
                    group_key=group_key,
                    message_id="414",
                    raw_message={
                        "message_id": 414,
                        "media_group_id": "optional-grp",
                        "caption": "updated",
                        "video": {
                            "file_id": "video-414",
                            "file_unique_id": "video-414",
                            "file_size": 2048,
                        },
                    },
                    flush_after=datetime.now(UTC),
                )

            buffer_id = await session.scalar(
                select(TelegramMediaGroupBuffer.buffer_id).where(
                    TelegramMediaGroupBuffer.group_key == group_key
                )
            )
            assert buffer_id is not None
            members = list(
                (
                    await session.scalars(
                        select(TelegramMediaGroupBufferMember)
                        .where(
                            TelegramMediaGroupBufferMember.buffer_id == buffer_id
                        )
                        .order_by(TelegramMediaGroupBufferMember.position)
                    )
                ).all()
            )

            assert [member.message_id for member in members] == [
                "411",
                "412",
                "413",
                "414",
            ]
            assert [member.position for member in members] == [1, 2, 3, 4]
            assert len(members) == 4
            assert members[-1].raw_message["caption"] == "updated"
    finally:
        async with database, database.session() as cleanup_session:
            async with cleanup_session.begin():
                buffer_id = await cleanup_session.scalar(
                    select(TelegramMediaGroupBuffer.buffer_id).where(
                        TelegramMediaGroupBuffer.group_key == group_key
                    )
                )
                if buffer_id is not None:
                    await cleanup_session.execute(
                        delete(TelegramMediaGroupBufferMember).where(
                            TelegramMediaGroupBufferMember.buffer_id == buffer_id
                        )
                    )
                    await cleanup_session.execute(
                        delete(TelegramMediaGroupBuffer).where(
                            TelegramMediaGroupBuffer.buffer_id == buffer_id
                        )
                    )
