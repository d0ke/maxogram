from __future__ import annotations

import pytest
from conftest import write_tokens

from maxogram.cli import _escape_alembic_ini_value
from maxogram.config import ENV_FILE_VAR, ENV_KEYS, ConfigError, load_settings


@pytest.fixture(autouse=True)
def clear_maxogram_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(ENV_FILE_VAR, raising=False)
    for key in ENV_KEYS:
        monkeypatch.delenv(key, raising=False)


def test_load_settings_redacts_secrets_with_tokens_fallback(tmp_path):
    write_tokens(tmp_path / "tokens.py")

    settings = load_settings(tmp_path)

    assert settings.telegram_token == "telegram-token"
    assert settings.max_token == "max-token"
    summary = settings.safe_summary()
    assert summary["telegram_token"] == "set"
    assert summary["max_token"] == "set"
    assert "secret" not in str(summary)


def test_load_settings_requires_tokens_file(tmp_path):
    with pytest.raises(ConfigError):
        load_settings(tmp_path)


def test_load_settings_from_dotenv_file(tmp_path):
    (tmp_path / ".env").write_text(
        "\n".join(
            [
                "MAXOGRAM_TG_BOT_TOKEN=dotenv-telegram-token",
                "MAXOGRAM_MAX_BOT_TOKEN=dotenv-max-token",
                "MAXOGRAM_DB_DATABASE=maxogram_env",
                "MAXOGRAM_DB_USER=maxogram_env_user",
                "MAXOGRAM_DB_PASSWORD=dotenv-secret",
                "MAXOGRAM_DB_HOST=127.0.0.1",
                "MAXOGRAM_DB_PORT=55432",
                "MAXOGRAM_VPS_HOST=vps.example.test",
                "MAXOGRAM_VPS_SSH_PORT=41223",
            ]
        ),
        encoding="utf-8",
    )

    settings = load_settings(tmp_path)

    assert settings.telegram_token == "dotenv-telegram-token"
    assert settings.max_token == "dotenv-max-token"
    assert settings.db.database == "maxogram_env"
    assert settings.db.user == "maxogram_env_user"
    assert settings.db.password == "dotenv-secret"
    assert settings.db.host == "127.0.0.1"
    assert settings.db.port == 55432
    assert settings.vps_host == "vps.example.test"
    assert settings.vps_ssh_port == 41223


def test_load_settings_from_dotenv_file_with_utf8_bom(tmp_path):
    (tmp_path / ".env").write_text(
        "\n".join(
            [
                "MAXOGRAM_TG_BOT_TOKEN=bom-telegram-token",
                "MAXOGRAM_MAX_BOT_TOKEN=bom-max-token",
                "MAXOGRAM_DB_DATABASE=maxogram_bom",
                "MAXOGRAM_DB_USER=maxogram_bom_user",
                "MAXOGRAM_DB_PASSWORD=bom-secret",
                "MAXOGRAM_DB_HOST=127.0.0.1",
                "MAXOGRAM_DB_PORT=6543",
            ]
        ),
        encoding="utf-8-sig",
    )

    settings = load_settings(tmp_path)

    assert settings.telegram_token == "bom-telegram-token"
    assert settings.max_token == "bom-max-token"
    assert settings.db.database == "maxogram_bom"
    assert settings.db.user == "maxogram_bom_user"
    assert settings.db.password == "bom-secret"
    assert settings.db.host == "127.0.0.1"
    assert settings.db.port == 6543


def test_load_settings_env_overrides_tokens_fallback(tmp_path):
    write_tokens(tmp_path / "tokens.py")
    (tmp_path / ".env").write_text(
        "\n".join(
            [
                "MAXOGRAM_TG_BOT_TOKEN=env-telegram-token",
                "MAXOGRAM_MAX_BOT_TOKEN=env-max-token",
                "MAXOGRAM_DB_DATABASE=maxogram_override",
                "MAXOGRAM_DB_USER=maxogram_override_user",
                "MAXOGRAM_DB_PASSWORD=override-secret",
                "MAXOGRAM_DB_HOST=env.example.test",
                "MAXOGRAM_DB_PORT=6432",
            ]
        ),
        encoding="utf-8",
    )

    settings = load_settings(tmp_path)

    assert settings.telegram_token == "env-telegram-token"
    assert settings.max_token == "env-max-token"
    assert settings.db.database == "maxogram_override"
    assert settings.db.user == "maxogram_override_user"
    assert settings.db.password == "override-secret"
    assert settings.db.host == "env.example.test"
    assert settings.db.port == 6432


def test_load_settings_supports_process_environment(monkeypatch, tmp_path):
    monkeypatch.setenv("MAXOGRAM_TG_BOT_TOKEN", "envvar-telegram-token")
    monkeypatch.setenv("MAXOGRAM_MAX_BOT_TOKEN", "envvar-max-token")
    monkeypatch.setenv("MAXOGRAM_DB_DATABASE", "maxogram_envvar")
    monkeypatch.setenv("MAXOGRAM_DB_USER", "maxogram_envvar_user")
    monkeypatch.setenv("MAXOGRAM_DB_PASSWORD", "envvar-secret")
    monkeypatch.setenv("MAXOGRAM_DB_HOST", "db.example.test")
    monkeypatch.setenv("MAXOGRAM_DB_PORT", "7432")

    settings = load_settings(tmp_path)

    assert settings.telegram_token == "envvar-telegram-token"
    assert settings.max_token == "envvar-max-token"
    assert settings.db.database == "maxogram_envvar"
    assert settings.db.user == "maxogram_envvar_user"
    assert settings.db.password == "envvar-secret"
    assert settings.db.host == "db.example.test"
    assert settings.db.port == 7432


def test_load_settings_requires_complete_env_config(tmp_path):
    (tmp_path / ".env").write_text(
        "\n".join(
            [
                "MAXOGRAM_TG_BOT_TOKEN=partial-telegram-token",
                "MAXOGRAM_MAX_BOT_TOKEN=partial-max-token",
                "MAXOGRAM_DB_DATABASE=maxogram_partial",
            ]
        ),
        encoding="utf-8",
    )

    with pytest.raises(
        ConfigError, match="Environment config is missing MAXOGRAM_DB_USER"
    ):
        load_settings(tmp_path)


def test_escape_alembic_ini_value_allows_percent_encoded_password():
    value = "postgresql+asyncpg://user:pwd%21%21@example.test/db"

    assert _escape_alembic_ini_value(value) == (
        "postgresql+asyncpg://user:pwd%%21%%21@example.test/db"
    )
