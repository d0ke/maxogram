from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_readme_uses_public_installer_url_and_modes() -> None:
    readme = (ROOT / "README.md").read_text(encoding="utf-8")

    assert "https://raw.githubusercontent.com/d0ke/maxogram/main/install.sh" in readme
    assert "sudo bash -s -- auto" in readme
    assert "sudo bash -s -- manual" in readme
    assert "sudo bash -s -- update" in readme
    assert "openSUSE" in readme
    assert "Arch Linux" in readme
    assert "generic Linux best-effort fallback" in readme
    assert "systemd timer" in readme
    assert ">= 3.13" in readme
    assert "working virtual environment with `pip`" in readme
    assert "pythonX.Y-venv" in readme
    assert "python3-venv" in readme
    assert "database name: `maxogram`" in readme
    assert "maxogram_app" in readme
    assert "GitHub tarball" in readme
    assert "listen_addresses" in readme
    assert "pg_hba.conf" in readme
    assert "firewall" in readme
    assert "detected selected local PostgreSQL instance port" in readme
    assert "existing local Maxogram env port match" in readme
    assert "highest live major version" in readme
    assert "explicit ambiguity error" in readme


def test_install_script_uses_real_cli_and_tarball_update_flow() -> None:
    installer = (ROOT / "install.sh").read_text(encoding="utf-8")

    assert 'ENV_DIR="/etc/maxogram"' in installer
    assert 'ENV_FILE="${ENV_DIR}/maxogram.env"' in installer
    assert "python -m maxogram --root ${APP_DIR} run" in installer
    assert "run_maxogram_cli db-upgrade" in installer
    assert "run_maxogram_cli check-config" in installer
    assert "openSUSE" in installer
    assert "Arch Linux" in installer
    assert "Generic Linux best-effort fallback" in installer
    assert "apt-get" in installer
    assert "dnf install -y" in installer
    assert "zypper --non-interactive install" in installer
    assert "pacman -Sy --noconfirm --needed" in installer
    assert "write_restart_timer" in installer
    assert "maxogram-restart.timer" in installer
    assert "APP_SOURCE_URL=" in installer
    assert "codeload.github.com/d0ke/maxogram/tar.gz/refs/heads/main" in installer
    assert 'DEFAULT_DB_NAME="maxogram"' in installer
    assert "sudo bash install.sh update" in installer
    assert 'DEFAULT_DB_USER="maxogram_app"' in installer
    assert "Update mode requires an existing ${ENV_FILE}." in installer
    assert "Unsupported distribution" not in installer
    assert "git clone" not in installer
    assert "git pull" not in installer
    assert "git fetch" not in installer
    assert "pg_hba.conf" not in installer
    assert "listen_addresses" not in installer
    assert "resolve_local_postgres_target" in installer
    assert "pg_lsclusters" in installer
    assert "postmaster.pid" in installer
    assert "Could not discover a live local PostgreSQL target" in installer
    assert "python_can_create_working_venv" in installer
    assert "python3-venv" in installer
    assert "Trying Debian/Ubuntu venv package repair" in installer
    assert "Falling back to source-built Python" in installer
    assert '-m pip install --upgrade pip setuptools wheel' in installer
