from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_readme_documents_docker_first_install_flow() -> None:
    readme = (ROOT / "README.md").read_text(encoding="utf-8")

    assert "https://raw.githubusercontent.com/d0ke/maxogram/main/install.sh" in readme
    assert "curl -fsSLo /tmp/maxogram-install.sh" in readme
    assert "sudo /tmp/maxogram-install.sh auto" in readme
    assert "sudo /tmp/maxogram-install.sh manual" in readme
    assert "sudo /tmp/maxogram-install.sh update" in readme
    assert "sudo bash -s -- auto" not in readme
    assert "sudo bash -s -- manual" not in readme
    assert "sudo bash -s -- update" not in readme
    assert "docker.io/d0ke/maxogram:latest" in readme
    assert "/etc/maxogram/maxogram.env" in readme
    assert "/opt/maxogram/docker-compose.app.yml" in readme
    assert "Docker Hub and GitHub Actions Setup" in readme
    assert "DOCKERHUB_USERNAME" in readme
    assert "DOCKERHUB_TOKEN" in readme
    assert "docker compose -f /opt/maxogram/docker-compose.app.yml up -d" in readme
    assert "git` on the server" in readme
    assert "Python on the server" in readme
    assert ".venv" not in readme
    assert "maxogram.service" not in readme
    assert "GitHub tarball" not in readme
    assert "python3-venv" not in readme


def test_install_script_uses_docker_compose_flow() -> None:
    installer = (ROOT / "install.sh").read_text(encoding="utf-8")

    assert 'ENV_DIR="/etc/maxogram"' in installer
    assert 'ENV_FILE="${ENV_DIR}/maxogram.env"' in installer
    assert 'COMPOSE_FILE="${APP_DIR}/docker-compose.app.yml"' in installer
    assert 'DOCKER_IMAGE="${DOCKER_IMAGE:-docker.io/d0ke/maxogram:latest}"' in installer
    assert "ensure_docker_installed" in installer
    assert "write_compose_file" in installer
    assert 'docker_compose_cmd pull "${APP_NAME}"' in installer
    assert "docker run --rm" in installer
    assert "--entrypoint python" in installer
    assert "docker compose -f" in installer
    assert "remove_legacy_host_artifacts" in installer
    assert "Update mode requires an existing ${ENV_FILE}." in installer
    assert "APP_SOURCE_URL=" not in installer
    assert "ensure_python" not in installer
    assert ".venv" not in installer
    assert "write_systemd_unit" not in installer
    assert "write_restart_watchdog" not in installer


def test_docker_assets_are_aligned_to_the_official_image() -> None:
    compose = (ROOT / "docker-compose.app.yml").read_text(encoding="utf-8")
    env_example = (ROOT / ".env.example").read_text(encoding="utf-8")
    workflow = (ROOT / ".github" / "workflows" / "docker-publish.yml").read_text(
        encoding="utf-8"
    )

    compose.encode("ascii")

    assert "docker.io/d0ke/maxogram:latest" in compose
    assert "/etc/maxogram/maxogram.env" in compose
    assert "YOUR_DOCKERHUB_USERNAME" not in compose
    assert "MAXOGRAM_DB_SCHEMA=maxogram" in env_example
    assert "images: docker.io/d0ke/maxogram" in workflow
    assert "DOCKERHUB_USERNAME" in workflow
    assert "DOCKERHUB_TOKEN" in workflow
    assert "docker.io/${{ secrets.DOCKERHUB_USERNAME }}/maxogram" not in workflow
