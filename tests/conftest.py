from __future__ import annotations

from pathlib import Path


def write_tokens(path: Path) -> None:
    path.write_text(
        "\n".join(
            [
                "VPS_host = '127.0.0.1'",
                "VPS_SSH_port = 22",
                "TG_bot_token = 'telegram-token'",
                "Max_bot_token = 'max-token'",
                "DB_CONFIG = {",
                "    'database': 'maxogram',",
                "    'user': 'maxogram_app',",
                "    'password': 'secret',",
                "    'host': 'localhost',",
                "    'port': 5432,",
                "}",
            ]
        ),
        encoding="utf-8",
    )
