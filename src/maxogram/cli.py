from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path

from alembic.config import Config

from alembic import command

from .app import MaxogramApp
from .config import ConfigError, load_settings
from .logging import setup_logging


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="maxogram")
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--log-level", default="INFO")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("check-config")
    subparsers.add_parser("db-upgrade")
    subparsers.add_parser("run")
    args = parser.parse_args(argv)

    setup_logging(args.log_level)
    try:
        settings = load_settings(args.root)
    except ConfigError as exc:
        parser.error(str(exc))
        return 2

    if args.command == "check-config":
        print(json.dumps(settings.safe_summary(), ensure_ascii=False, indent=2))
        return 0
    if args.command == "db-upgrade":
        alembic_cfg = _alembic_config(settings.root_dir)
        alembic_cfg.set_main_option(
            "sqlalchemy.url",
            _escape_alembic_ini_value(settings.db.sqlalchemy_url()),
        )
        command.upgrade(alembic_cfg, "head")
        return 0
    if args.command == "run":
        asyncio.run(MaxogramApp(settings).run_forever())
        return 0
    raise AssertionError(f"Unhandled command: {args.command}")


def _alembic_config(root_dir: Path) -> Config:
    cfg = Config(str(root_dir / "alembic.ini"))
    cfg.set_main_option("script_location", str(root_dir / "alembic"))
    return cfg


def _escape_alembic_ini_value(value: str) -> str:
    return value.replace("%", "%%")
