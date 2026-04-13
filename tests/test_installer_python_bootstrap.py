from __future__ import annotations

import os
import shutil
import subprocess
import textwrap
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]


def find_bash() -> str | None:
    candidates = [
        shutil.which("bash"),
        r"C:\Program Files\Git\bin\bash.exe",
        r"C:\Program Files\Git\usr\bin\bash.exe",
        "/bin/bash",
    ]
    for candidate in candidates:
        if candidate and Path(candidate).exists():
            return candidate
    return None


def bash_path() -> str:
    resolved = find_bash()
    if not resolved:
        pytest.skip("bash is required for installer behavior tests")
    return resolved


def run_bash(
    script: str, *, extra_env: dict[str, str] | None = None
) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    if extra_env:
        env.update(extra_env)
    return subprocess.run(
        [bash_path(), "-lc", script],
        cwd=ROOT,
        env=env,
        text=True,
        capture_output=True,
    )


def source_install_and_run(script_body: str) -> subprocess.CompletedProcess[str]:
    install_path = (ROOT / "install.sh").resolve().as_posix()
    script = textwrap.dedent(
        f"""
        set -euo pipefail
        source "{install_path}"
        {script_body}
        """
    )
    return run_bash(script)


def test_installs_docker_runtime_and_starts_service_when_needed(tmp_path: Path) -> None:
    state_dir = tmp_path / "state"

    result = source_install_and_run(
        f"""
        mkdir -p "{state_dir.as_posix()}"
        PKG_MANAGER="apt"
        DOCKER_PACKAGE_GROUPS=("docker.io docker-compose-v2" "docker.io docker-compose-plugin")
        docker() {{
          if [[ "$1" == "compose" && "$2" == "version" ]]; then
            [[ -f "{(state_dir / 'compose-ready').as_posix()}" ]]
            return $?
          fi
          if [[ "$1" == "info" ]]; then
            [[ -f "{(state_dir / 'daemon-ready').as_posix()}" ]]
            return $?
          fi
          return 0
        }}
        install_from_candidate_groups() {{
          printf 'install=%s\\n' "$1"
          touch "{(state_dir / 'compose-ready').as_posix()}"
          return 0
        }}
        service_enable_now_candidates() {{
          touch "{(state_dir / 'daemon-ready').as_posix()}"
          printf 'docker.service\\n'
          return 0
        }}
        ensure_docker_installed
        printf 'service=%s\\n' "$DOCKER_SERVICE_UNIT"
        """
    )

    assert result.returncode == 0, result.stderr
    assert "install=Docker runtime" in result.stdout
    assert "service=docker.service" in result.stdout


def test_fails_when_docker_compose_stays_unavailable() -> None:
    result = source_install_and_run(
        """
        PKG_MANAGER="apt"
        DOCKER_PACKAGE_GROUPS=("docker.io docker-compose-v2")
        docker() {
          if [[ "$1" == "compose" && "$2" == "version" ]]; then
            return 1
          fi
          if [[ "$1" == "info" ]]; then
            return 0
          fi
          return 0
        }
        install_from_candidate_groups() {
          printf 'install=%s\n' "$1"
          return 0
        }
        ensure_docker_installed
        """
    )

    assert result.returncode != 0
    assert "Docker Compose is unavailable" in result.stderr


def test_load_existing_env_supports_old_quoted_values(tmp_path: Path) -> None:
    env_file = tmp_path / "maxogram.env"
    env_file.write_text(
        textwrap.dedent(
            """
            MAXOGRAM_TG_BOT_TOKEN="tg-token"
            MAXOGRAM_MAX_BOT_TOKEN=max-token
            MAXOGRAM_DB_HOST=10.0.0.15
            MAXOGRAM_DB_PORT=55432
            MAXOGRAM_DB_DATABASE=maxogram
            MAXOGRAM_DB_USER=maxogram_app
            MAXOGRAM_DB_PASSWORD="secret-value"
            MAXOGRAM_DB_SCHEMA=maxogram
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )

    result = source_install_and_run(
        f"""
        ENV_FILE="{env_file.as_posix()}"
        load_existing_env
        printf 'tg=%s\\nmax=%s\\nhost=%s\\nport=%s\\npassword=%s\\n' \
          "$TG_BOT_TOKEN" "$MAX_BOT_TOKEN" "$DB_HOST" "$DB_PORT" "$DB_PASSWORD"
        """
    )

    assert result.returncode == 0, result.stderr
    assert "tg=tg-token" in result.stdout
    assert "max=max-token" in result.stdout
    assert "host=10.0.0.15" in result.stdout
    assert "port=55432" in result.stdout
    assert "password=secret-value" in result.stdout


def test_prompt_value_keeps_existing_value_on_enter() -> None:
    result = source_install_and_run(
        """
        DB_HOST="db.example.com"
        prompt_value DB_HOST "Database host" "127.0.0.1" <<< $'\n'
        printf 'host=%s\n' "$DB_HOST"
        """
    )

    assert result.returncode == 0, result.stderr
    assert "host=db.example.com" in result.stdout


def test_prompt_secret_keeps_existing_value_on_enter() -> None:
    result = source_install_and_run(
        """
        TG_BOT_TOKEN="telegram-token"
        prompt_secret TG_BOT_TOKEN "Telegram bot token" <<< $'\n'
        printf 'token=%s\n' "$TG_BOT_TOKEN"
        """
    )

    assert result.returncode == 0, result.stderr
    assert "token=telegram-token" in result.stdout


def test_auto_mode_stays_simpler_than_manual_mode() -> None:
    result = source_install_and_run(
        """
        prompt_value_calls=0
        prompt_secret_calls=0
        prompt_value() {
          prompt_value_calls=$((prompt_value_calls + 1))
          local __var_name="$1"
          local default_value="${3:-}"
          local current_value="${!__var_name:-}"
          if [[ -n "${current_value}" ]]; then
            default_value="${current_value}"
          fi
          printf -v "${__var_name}" '%s' "${default_value}"
        }
        prompt_secret() {
          prompt_secret_calls=$((prompt_secret_calls + 1))
          local __var_name="$1"
          local current_value="${!__var_name:-}"
          if [[ -n "${current_value}" ]]; then
            printf -v "${__var_name}" '%s' "${current_value}"
          else
            printf -v "${__var_name}" '%s' "filled"
          fi
        }

        DB_HOST="db.example.com"
        TG_BOT_TOKEN="tg"
        MAX_BOT_TOKEN="mx"
        DB_PASSWORD="pw"
        collect_auto_inputs
        printf 'auto_values=%s\\nauto_secrets=%s\\n' "$prompt_value_calls" "$prompt_secret_calls"

        prompt_value_calls=0
        prompt_secret_calls=0
        DB_HOST="db.example.com"
        TG_BOT_TOKEN="tg"
        MAX_BOT_TOKEN="mx"
        DB_PASSWORD="pw"
        collect_manual_inputs
        printf 'manual_values=%s\\nmanual_secrets=%s\\n' "$prompt_value_calls" "$prompt_secret_calls"
        """
    )

    assert result.returncode == 0, result.stderr
    assert "auto_values=0" in result.stdout
    assert "auto_secrets=2" in result.stdout
    assert "manual_values=5" in result.stdout
    assert "manual_secrets=3" in result.stdout
