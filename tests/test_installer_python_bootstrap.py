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


def run_bash(script: str, *, extra_env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
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


def test_debian_repairs_existing_python_with_versioned_venv_package(tmp_path: Path) -> None:
    state_dir = tmp_path / "state"

    result = source_install_and_run(
        f"""
        mkdir -p "{state_dir.as_posix()}"
        PKG_MANAGER="apt"
        PYTHON_PACKAGE_GROUPS=("python3.13 python3.13-venv python3.13-dev")
        discover_python_exec() {{ printf '/usr/bin/python3.13\\n'; }}
        python_version_series() {{ printf '3.13\\n'; }}
        python_can_create_working_venv() {{ [[ -f "{(state_dir / 'ready').as_posix()}" ]]; }}
        try_install_packages() {{
          printf 'install=%s\\n' "$*"
          if [[ "$1" == "python3.13-venv" ]]; then
            touch "{(state_dir / 'ready').as_posix()}"
            return 0
          fi
          return 1
        }}
        build_python_from_source() {{
          echo "build-source"
          touch "{(state_dir / 'built').as_posix()}"
        }}
        ensure_python
        printf 'python=%s\\nvenv_ready=%s\\n' "$PYTHON_EXEC" "$PYTHON_VENV_READY"
        if [[ -f "{(state_dir / 'built').as_posix()}" ]]; then
          printf 'built=yes\\n'
        else
          printf 'built=no\\n'
        fi
        """
    )

    assert result.returncode == 0, result.stderr
    assert "install=python3.13-venv" in result.stdout
    assert "install=python3-venv" not in result.stdout
    assert "python=/usr/bin/python3.13" in result.stdout
    assert "venv_ready=true" in result.stdout
    assert "built=no" in result.stdout


def test_debian_falls_back_to_generic_python3_venv_package(tmp_path: Path) -> None:
    state_dir = tmp_path / "state"

    result = source_install_and_run(
        f"""
        mkdir -p "{state_dir.as_posix()}"
        PKG_MANAGER="apt"
        PYTHON_PACKAGE_GROUPS=("python3.13 python3.13-venv python3.13-dev")
        discover_python_exec() {{ printf '/usr/bin/python3.13\\n'; }}
        python_version_series() {{ printf '3.13\\n'; }}
        python_can_create_working_venv() {{ [[ -f "{(state_dir / 'ready').as_posix()}" ]]; }}
        try_install_packages() {{
          printf 'install=%s\\n' "$*"
          if [[ "$1" == "python3-venv" ]]; then
            touch "{(state_dir / 'ready').as_posix()}"
            return 0
          fi
          return 1
        }}
        build_python_from_source() {{
          echo "build-source"
          touch "{(state_dir / 'built').as_posix()}"
        }}
        ensure_python
        printf 'python=%s\\nvenv_ready=%s\\n' "$PYTHON_EXEC" "$PYTHON_VENV_READY"
        if [[ -f "{(state_dir / 'built').as_posix()}" ]]; then
          printf 'built=yes\\n'
        else
          printf 'built=no\\n'
        fi
        """
    )

    assert result.returncode == 0, result.stderr
    assert "install=python3.13-venv" in result.stdout
    assert "install=python3-venv" in result.stdout
    assert "python=/usr/bin/python3.13" in result.stdout
    assert "venv_ready=true" in result.stdout
    assert "built=no" in result.stdout


def test_existing_venv_capable_python_skips_repairs_and_fallback() -> None:
    result = source_install_and_run(
        """
        PKG_MANAGER="apt"
        PYTHON_PACKAGE_GROUPS=("python3.13 python3.13-venv python3.13-dev")
        discover_python_exec() { printf '/usr/bin/python3.13\n'; }
        python_version_series() { printf '3.13\n'; }
        python_can_create_working_venv() { return 0; }
        try_install_packages() { echo "unexpected-install"; exit 97; }
        build_python_from_source() { echo "unexpected-build"; exit 98; }
        ensure_python
        printf 'python=%s\nvenv_ready=%s\n' "$PYTHON_EXEC" "$PYTHON_VENV_READY"
        """
    )

    assert result.returncode == 0, result.stderr
    assert "unexpected-install" not in result.stdout
    assert "unexpected-build" not in result.stdout
    assert "python=/usr/bin/python3.13" in result.stdout
    assert "venv_ready=true" in result.stdout


def test_non_apt_uses_source_build_fallback_when_system_python_cannot_make_venv(tmp_path: Path) -> None:
    state_dir = tmp_path / "state"

    result = source_install_and_run(
        f"""
        mkdir -p "{state_dir.as_posix()}"
        PKG_MANAGER="dnf"
        PYTHON_PACKAGE_GROUPS=("python3.13 python3.13-devel")
        discover_python_exec() {{
          if [[ -f "{(state_dir / 'built').as_posix()}" ]]; then
            printf '/usr/local/bin/python3.13\\n'
          else
            printf '/usr/bin/python3.13\\n'
          fi
        }}
        python_version_series() {{ printf '3.13\\n'; }}
        python_can_create_working_venv() {{
          [[ "$1" == "/usr/local/bin/python3.13" ]]
        }}
        install_from_candidate_groups() {{
          printf 'packages=%s\\n' "$1"
          return 0
        }}
        build_python_from_source() {{
          echo "build-source"
          touch "{(state_dir / 'built').as_posix()}"
        }}
        ensure_python
        printf 'python=%s\\nvenv_ready=%s\\n' "$PYTHON_EXEC" "$PYTHON_VENV_READY"
        if [[ -f "{(state_dir / 'built').as_posix()}" ]]; then
          printf 'built=yes\\n'
        else
          printf 'built=no\\n'
        fi
        """
    )

    assert result.returncode == 0, result.stderr
    assert "packages=Python >= 3.13" in result.stdout
    assert "build-source" in result.stdout
    assert "python=/usr/local/bin/python3.13" in result.stdout
    assert "venv_ready=true" in result.stdout
    assert "built=yes" in result.stdout


def test_python_bootstrap_fails_early_when_no_working_venv_is_possible(tmp_path: Path) -> None:
    state_dir = tmp_path / "state"

    result = source_install_and_run(
        f"""
        mkdir -p "{state_dir.as_posix()}"
        PKG_MANAGER="generic"
        PYTHON_PACKAGE_GROUPS=()
        discover_python_exec() {{
          if [[ -f "{(state_dir / 'built').as_posix()}" ]]; then
            printf '/usr/local/bin/python3.13\\n'
          else
            printf '/usr/bin/python3.13\\n'
          fi
        }}
        python_version_series() {{ printf '3.13\\n'; }}
        python_can_create_working_venv() {{ return 1; }}
        build_python_from_source() {{
          echo "build-source"
          touch "{(state_dir / 'built').as_posix()}"
        }}
        ensure_python
        """
    )

    assert result.returncode != 0
    assert "cannot create a working virtual environment" in result.stderr
