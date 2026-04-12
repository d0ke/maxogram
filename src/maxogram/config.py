from __future__ import annotations

import importlib.util
import os
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import Any

from dotenv import dotenv_values
from sqlalchemy.engine import URL

ENV_FILE_VAR = "MAXOGRAM_ENV_FILE"
ENV_KEYS = {
    "MAXOGRAM_TG_BOT_TOKEN",
    "MAXOGRAM_MAX_BOT_TOKEN",
    "MAXOGRAM_DB_DATABASE",
    "MAXOGRAM_DB_USER",
    "MAXOGRAM_DB_PASSWORD",
    "MAXOGRAM_DB_HOST",
    "MAXOGRAM_DB_PORT",
    "MAXOGRAM_TEST_DB_DATABASE",
    "MAXOGRAM_TEST_DB_USER",
    "MAXOGRAM_TEST_DB_PASSWORD",
    "MAXOGRAM_TEST_DB_HOST",
    "MAXOGRAM_TEST_DB_PORT",
    "MAXOGRAM_VPS_HOST",
    "MAXOGRAM_VPS_SSH_PORT",
}


class ConfigError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class DatabaseConfig:
    database: str
    user: str
    password: str
    host: str
    port: int

    @classmethod
    def from_mapping(cls, value: object) -> DatabaseConfig:
        if not isinstance(value, dict):
            raise ConfigError("DB_CONFIG must be a dictionary")
        required = {"database", "user", "password", "host", "port"}
        missing = sorted(required.difference(value))
        if missing:
            raise ConfigError(f"DB_CONFIG is missing keys: {', '.join(missing)}")
        return cls(
            database=str(value["database"]),
            user=str(value["user"]),
            password=str(value["password"]),
            host=str(value["host"]),
            port=int(value["port"]),
        )

    def sqlalchemy_url(self, *, async_driver: bool = True) -> str:
        driver = "postgresql+asyncpg" if async_driver else "postgresql"
        return URL.create(
            drivername=driver,
            username=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
            database=self.database,
        ).render_as_string(hide_password=False)

    def safe_summary(self) -> dict[str, str]:
        return {
            "database": self.database,
            "user": self.user,
            "host": "set" if self.host else "missing",
            "port": "set" if self.port else "missing",
        }


@dataclass(frozen=True, slots=True)
class AppSettings:
    telegram_token: str
    max_token: str
    db: DatabaseConfig
    vps_host: str | None
    vps_ssh_port: int | None
    root_dir: Path
    test_db: DatabaseConfig | None = None
    log_level: str = "INFO"
    poll_limit: int = 100
    telegram_poll_timeout: int = 30
    max_poll_timeout: int = 30
    worker_idle_seconds: float = 1.0
    outbox_lease_seconds: int = 60

    def safe_summary(self) -> dict[str, object]:
        return {
            "telegram_token": "set" if self.telegram_token else "missing",
            "max_token": "set" if self.max_token else "missing",
            "db": self.db.safe_summary(),
            "test_db": self.test_db.safe_summary() if self.test_db else None,
            "vps_host": "set" if self.vps_host else "missing",
            "vps_ssh_port": "set" if self.vps_ssh_port else "missing",
            "root_dir": str(self.root_dir),
        }


def load_settings(root_dir: Path | None = None) -> AppSettings:
    root = (root_dir or Path.cwd()).resolve()
    env_mapping = _load_env_mapping(root)
    if env_mapping is not None:
        return _load_settings_from_env(root, env_mapping)

    module = _load_tokens_module(root / "tokens.py")

    telegram_token = _required_str(module, "TG_bot_token")
    max_token = _required_str(module, "Max_bot_token")
    db = DatabaseConfig.from_mapping(_required_attr(module, "DB_CONFIG"))
    test_db_value = getattr(module, "TEST_DB_CONFIG", None)
    test_db = None
    if test_db_value is not None:
        test_db = DatabaseConfig.from_mapping(test_db_value)

    vps_ssh_port = getattr(module, "VPS_SSH_port", None)
    return AppSettings(
        telegram_token=telegram_token,
        max_token=max_token,
        db=db,
        test_db=test_db,
        vps_host=getattr(module, "VPS_host", None),
        vps_ssh_port=int(vps_ssh_port) if vps_ssh_port is not None else None,
        root_dir=root,
    )


def _load_env_mapping(root_dir: Path) -> dict[str, str] | None:
    merged: dict[str, str] = {}
    for path in _env_file_candidates(root_dir):
        if not path.exists():
            continue
        for key, value in dotenv_values(path).items():
            normalized_key = key.lstrip("\ufeff")
            if normalized_key in ENV_KEYS and value is not None:
                merged[normalized_key] = value

    for key in ENV_KEYS:
        value = os.environ.get(key)
        if value is not None:
            merged[key] = value

    return merged or None


def _env_file_candidates(root_dir: Path) -> list[Path]:
    candidates: list[Path] = []
    explicit = os.environ.get(ENV_FILE_VAR)
    if explicit:
        candidates.append(Path(explicit).expanduser())
    candidates.append(root_dir / ".env")
    return candidates


def _load_settings_from_env(
    root_dir: Path,
    env_mapping: dict[str, str],
) -> AppSettings:
    # Installer-managed deployments normally provide these variables via
    # systemd EnvironmentFile=/etc/maxogram/maxogram.env. Local development can
    # keep using tokens.py, which remains the fallback when no MAXOGRAM_* values
    # are present.
    db = DatabaseConfig.from_mapping(
        {
            "database": _required_env_str(env_mapping, "MAXOGRAM_DB_DATABASE"),
            "user": _required_env_str(env_mapping, "MAXOGRAM_DB_USER"),
            "password": _required_env_str(env_mapping, "MAXOGRAM_DB_PASSWORD"),
            "host": _required_env_str(env_mapping, "MAXOGRAM_DB_HOST"),
            "port": _required_env_int(env_mapping, "MAXOGRAM_DB_PORT"),
        }
    )
    test_db = _optional_env_database(env_mapping, "MAXOGRAM_TEST_DB_")
    return AppSettings(
        telegram_token=_required_env_str(env_mapping, "MAXOGRAM_TG_BOT_TOKEN"),
        max_token=_required_env_str(env_mapping, "MAXOGRAM_MAX_BOT_TOKEN"),
        db=db,
        test_db=test_db,
        vps_host=env_mapping.get("MAXOGRAM_VPS_HOST"),
        vps_ssh_port=_optional_env_int(env_mapping, "MAXOGRAM_VPS_SSH_PORT"),
        root_dir=root_dir,
    )


def _optional_env_database(
    env_mapping: dict[str, str], prefix: str
) -> DatabaseConfig | None:
    keys = ("DATABASE", "USER", "PASSWORD", "HOST", "PORT")
    values = {
        key: env_mapping.get(f"{prefix}{key}")
        for key in keys
    }
    if not any(values.values()):
        return None

    missing = [f"{prefix}{key}" for key, value in values.items() if not value]
    if missing:
        raise ConfigError(
            "Incomplete test database config in environment: "
            + ", ".join(missing)
        )

    return DatabaseConfig.from_mapping(
        {
            "database": values["DATABASE"],
            "user": values["USER"],
            "password": values["PASSWORD"],
            "host": values["HOST"],
            "port": int(values["PORT"]),
        }
    )


def _load_tokens_module(path: Path) -> ModuleType:
    if not path.exists():
        raise ConfigError(f"Missing local config file: {path}")
    spec = importlib.util.spec_from_file_location("maxogram_local_tokens", path)
    if spec is None or spec.loader is None:
        raise ConfigError(f"Cannot load local config file: {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _required_attr(module: ModuleType, name: str) -> Any:
    if not hasattr(module, name):
        raise ConfigError(f"tokens.py is missing {name}")
    return getattr(module, name)


def _required_str(module: ModuleType, name: str) -> str:
    value = _required_attr(module, name)
    if not isinstance(value, str) or not value:
        raise ConfigError(f"{name} must be a non-empty string")
    return value


def _required_env_str(env_mapping: dict[str, str], name: str) -> str:
    value = env_mapping.get(name)
    if not value:
        raise ConfigError(f"Environment config is missing {name}")
    return value


def _required_env_int(env_mapping: dict[str, str], name: str) -> int:
    value = _required_env_str(env_mapping, name)
    try:
        return int(value)
    except ValueError as exc:
        raise ConfigError(f"{name} must be an integer") from exc


def _optional_env_int(env_mapping: dict[str, str], name: str) -> int | None:
    value = env_mapping.get(name)
    if value is None or value == "":
        return None
    try:
        return int(value)
    except ValueError as exc:
        raise ConfigError(f"{name} must be an integer") from exc
