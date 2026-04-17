# Maxogram

Maxogram is a self-hosted Telegram-to-MAX bridge bot built around one Python service and PostgreSQL.

Production deployment is Docker-first:

- the application runs from the public image `docker.io/d0ke/maxogram:latest`
- the published runtime image is based on Python `3.12`
- PostgreSQL stays on the VPS host or on a remote server
- `install.sh` manages PostgreSQL discovery/provisioning plus Docker Compose deployment

## What It Supports

- Bidirectional Telegram `<->` MAX chat bridging.
- Bridge management commands such as `/bridge link`, `/bridge confirm`, `/bridge pause`, and `/bridge resume`.
- Alias-based sender rendering with `/nick` commands.
- Reply mapping in both directions, including replies to already mirrored messages.
- Message edit and delete synchronization when the source platform exposes the event.
- Rich-text preservation for supported Telegram and MAX formatting, including underline.
- Real media relay for common attachment types such as photos, videos, GIF-style animations, documents, audio, voice messages, and stickers.
- Recovery flows backed by PostgreSQL queues, message mappings, and pending-mutation replay.

## Official Production Artifact

- Docker image: `docker.io/d0ke/maxogram:latest`
- Installer-managed env file: `/etc/maxogram/maxogram.env`
- Installer-managed compose file: `/opt/maxogram/docker-compose.app.yml`

## One-Line Install

These commands download the installer to `/tmp` first and then run it locally, so interactive prompts work reliably.

### Auto Mode

```bash
curl -fsSLo /tmp/maxogram-install.sh https://raw.githubusercontent.com/d0ke/maxogram/main/install.sh && chmod +x /tmp/maxogram-install.sh && sudo /tmp/maxogram-install.sh auto
```

Auto mode:

- defaults to local PostgreSQL on `127.0.0.1`
- asks Telegram and MAX bot tokens
- may ask for the existing `maxogram_app` password if the local PostgreSQL role already exists and no env file exists yet
- keeps current env values when `/etc/maxogram/maxogram.env` already exists and you press Enter

Auto defaults:

- deployment directory: `/opt/maxogram`
- env file: `/etc/maxogram/maxogram.env`
- database host: `127.0.0.1`
- database port: detected selected local PostgreSQL instance port; `5432` is only the pre-detection fallback for a fresh single-instance install
- database name: `maxogram`
- database user: `maxogram_app`
- schema name: `maxogram`

### Manual Mode

```bash
curl -fsSLo /tmp/maxogram-install.sh https://raw.githubusercontent.com/d0ke/maxogram/main/install.sh && chmod +x /tmp/maxogram-install.sh && sudo /tmp/maxogram-install.sh manual
```

Manual mode asks:

- Telegram bot token
- MAX bot token
- database host
- PostgreSQL port
- database name
- database user
- database password
- schema name

If `/etc/maxogram/maxogram.env` already exists, pressing Enter keeps the current value.

For a remote PostgreSQL host, the installer skips local PostgreSQL installation and provisioning and uses the supplied connection details as-is.

### Update Mode

```bash
curl -fsSLo /tmp/maxogram-install.sh https://raw.githubusercontent.com/d0ke/maxogram/main/install.sh && chmod +x /tmp/maxogram-install.sh && sudo /tmp/maxogram-install.sh update
```

Update mode:

- requires an existing `/etc/maxogram/maxogram.env`
- asks no questions
- pulls the latest `docker.io/d0ke/maxogram:latest`
- runs in-container `check-config`
- runs in-container `db-upgrade`
- recreates the Docker Compose service
- removes legacy host `systemd` and watchdog artifacts from the old native installer if they still exist

## What The Installer Does

- verifies that it is running as `root`
- detects Debian/Ubuntu, Fedora/RHEL-family, openSUSE, and Arch Linux explicitly
- falls back to a generic Linux best-effort path when the distro is unknown
- installs Docker and the Docker Compose plugin when they are missing
- enables and starts the Docker daemon when needed
- installs PostgreSQL if it is missing and a local database host is being used
- uses the detected package manager when one is available: `apt`, `dnf`, `zypper`, or `pacman`
- resolves one concrete local PostgreSQL target before any admin action and can reconfigure its port in `manual` mode
- selects a local PostgreSQL target in this order: existing local Maxogram env port match, cluster or instance already containing the Maxogram role or database, highest live major version, otherwise an explicit ambiguity error
- creates or reuses the local PostgreSQL role, database, and schema when PostgreSQL is local
- writes `/etc/maxogram/maxogram.env`
- writes `/opt/maxogram/docker-compose.app.yml`
- pulls the published image through `docker compose`
- validates config with `python -m maxogram check-config` inside the image
- applies database migrations with `python -m maxogram db-upgrade` inside the image
- starts the long-running bridge container with `restart: unless-stopped`

The installer does not:

- modify the firewall
- modify `listen_addresses`
- modify `pg_hba.conf`
- drop existing Maxogram PostgreSQL data
- require `git` on the server
- require Python on the server

## Manual Docker Deployment

Use this path if you want to deploy without the installer.

This path does not provision PostgreSQL for you. Create the database, role, password, and optional schema first.

1. Install Docker and the Docker Compose plugin on the VPS.
2. Create the config directory and env file:

```bash
sudo install -d -m 700 /etc/maxogram
sudo cp .env.example /etc/maxogram/maxogram.env
sudo chmod 600 /etc/maxogram/maxogram.env
```

3. Edit `/etc/maxogram/maxogram.env` and set:

- `MAXOGRAM_TG_BOT_TOKEN`
- `MAXOGRAM_MAX_BOT_TOKEN`
- `MAXOGRAM_DB_DATABASE`
- `MAXOGRAM_DB_USER`
- `MAXOGRAM_DB_PASSWORD`
- `MAXOGRAM_DB_HOST`
- `MAXOGRAM_DB_PORT`
- `MAXOGRAM_DB_SCHEMA` if you use installer-managed local schema provisioning

4. Copy `docker-compose.app.yml` to `/opt/maxogram/docker-compose.app.yml`.
5. Pull and start the service:

```bash
sudo install -d -m 755 /opt/maxogram
sudo cp docker-compose.app.yml /opt/maxogram/docker-compose.app.yml
sudo docker compose -f /opt/maxogram/docker-compose.app.yml pull
sudo docker compose -f /opt/maxogram/docker-compose.app.yml up -d
```

6. Check logs:

```bash
sudo docker compose -f /opt/maxogram/docker-compose.app.yml logs -f
```

## Docker Hub and GitHub Actions Setup

The repository already contains `.github/workflows/docker-publish.yml` and publishes `docker.io/d0ke/maxogram`.

### Docker Hub

1. Open Docker Hub.
2. Go to `Account settings -> Personal access tokens`.
3. Click `Generate new token`.
4. Create a token with read/write access and copy it immediately.

### GitHub

1. Open the repository on GitHub.
2. Go to `Settings -> Secrets and variables -> Actions`.
3. Click `New repository secret`.
4. Create:
   - `DOCKERHUB_USERNAME`
   - `DOCKERHUB_TOKEN`

### Publish Flow

1. Push to `main`, or push a tag like `v0.1.0`.
2. Open `Actions` in GitHub.
3. Run or inspect the `Build and push Docker image` workflow.
4. Confirm that Docker Hub shows the updated `d0ke/maxogram` tags.

The workflow publishes:

- `latest` on the default branch
- branch tags
- git tag tags
- commit `sha` tags

## Operations

```bash
sudo docker compose -f /opt/maxogram/docker-compose.app.yml ps
sudo docker compose -f /opt/maxogram/docker-compose.app.yml logs -f
sudo docker compose -f /opt/maxogram/docker-compose.app.yml pull
sudo docker compose -f /opt/maxogram/docker-compose.app.yml up -d
sudo docker compose -f /opt/maxogram/docker-compose.app.yml down
```

## Runtime Model

- production config comes from environment variables, usually via `/etc/maxogram/maxogram.env`
- local development can still use `tokens.py`
- the application container runs with Docker Compose and `restart: unless-stopped`
- PostgreSQL is the only durable store

## Supported Operating Systems

- Debian and Ubuntu
- Fedora and RHEL-family systems
- openSUSE
- Arch Linux
- generic Linux best-effort fallback

## Development Notes

- Local development for this repository must use Python `3.12`.
- Existing Python `3.13` virtual environments are unsupported and should be recreated with Python `3.12`.
- Runtime config loading is env-first, with `tokens.py` kept as a local-development fallback.
- Database schema changes should continue to go through Alembic via `python -m maxogram db-upgrade`.
- The full runtime and schema reference lives in `architecture.md`.
