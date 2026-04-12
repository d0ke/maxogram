from __future__ import annotations

import pytest
from sqlalchemy import text

from maxogram.config import load_settings
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
