from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_media_group_migration_reuses_existing_platform_enum() -> None:
    migration = (
        ROOT / "alembic" / "versions" / "20260419_0002_media_groups.py"
    ).read_text(encoding="utf-8")

    assert 'name="platform"' in migration
    assert "create_type=False" in migration
