# Maxogram

Maxogram is a self-hosted Telegram-to-MAX bridge bot built around a single Python service and PostgreSQL.

It mirrors linked chats in both directions and keeps the bridge state durable enough for retries, edits, deletes, reply mapping, and restart recovery.

## What It Supports

- Bidirectional Telegram `<->` MAX chat bridging.
- Bridge management commands such as `/bridge link`, `/bridge confirm`, `/bridge pause`, and `/bridge resume`.
- Alias-based sender rendering with `/nick` commands.
- Reply mapping in both directions, including replies to already mirrored messages.
- Message edit and delete synchronization when the source platform exposes the event.
- Rich-text preservation for supported Telegram and MAX formatting, including underline.
- Real media relay for common attachment types such as photos, videos, GIF-style animations, documents, audio, voice messages, and stickers.
- Recovery flows backed by PostgreSQL queues, message mappings, and pending-mutation replay.

## One-Line Install

### Auto Mode

```bash
curl -fsSL https://raw.githubusercontent.com/d0ke/maxogram/main/install.sh | sudo bash -s -- auto
```

Auto mode asks:

- Telegram bot token
- MAX bot token

It may also ask for the existing `maxogram_app` database password if:

- PostgreSQL is local
- `/etc/maxogram/maxogram.env` does not exist yet
- the local PostgreSQL role `maxogram_app` already exists

Auto defaults:

- OS user: `maxogram`
- app directory: `/opt/maxogram`
- env file: `/etc/maxogram/maxogram.env`
- database host: `127.0.0.1`
- database port: detected selected local PostgreSQL instance port; `5432` is only the pre-detection fallback for fresh single-instance installs
- database name: `maxogram`
- database user: `maxogram_app`
- schema name: `maxogram`

### Manual Mode

```bash
curl -fsSL https://raw.githubusercontent.com/d0ke/maxogram/main/install.sh | sudo bash -s -- manual
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
curl -fsSL https://raw.githubusercontent.com/d0ke/maxogram/main/install.sh | sudo bash -s -- update
```

Update mode:

- requires an existing `/etc/maxogram/maxogram.env`
- asks no questions
- refreshes the application source from GitHub
- upgrades `.venv` and dependencies
- runs `check-config` and `db-upgrade`
- restarts `maxogram.service`

## What The Installer Does

- verifies that it is running as `root`
- detects Debian/Ubuntu, Fedora/RHEL-family, openSUSE, and Arch Linux explicitly
- falls back to a generic Linux best-effort path when the distro is unknown
- downloads the public GitHub tarball for `main` instead of using `git`
- installs PostgreSQL if it is missing and a local database host is being used
- uses the detected package manager when one is available: `apt`, `dnf`, `zypper`, or `pacman`
- resolves one concrete local PostgreSQL target before any admin action and can reconfigure its port in `manual` mode
- selects a local PostgreSQL target in this order: existing local Maxogram env port match, cluster or instance already containing the Maxogram role or database, highest live major version, otherwise an explicit ambiguity error
- creates or reuses the local PostgreSQL role, database, and schema when PostgreSQL is local
- reuses existing Maxogram data and applies only `python -m maxogram db-upgrade`
- creates or reuses the dedicated `maxogram` system user
- uses any system Python `>= 3.13`, otherwise tries distro packages, otherwise builds Python `3.13` from source
- creates or upgrades `.venv`
- installs or upgrades Python dependencies
- writes `/etc/maxogram/maxogram.env`
- validates config with `python -m maxogram check-config`
- installs and enables `maxogram.service`
- installs a cron watchdog that restarts the service every four hours
- falls back to a `systemd timer` when cron cannot be installed or started reliably

The installer does not:

- modify the firewall
- modify `listen_addresses`
- modify `pg_hba.conf`
- drop existing Maxogram PostgreSQL data
- require `git` on the server

## Runtime Model

- production config comes from environment variables, usually via `EnvironmentFile=/etc/maxogram/maxogram.env`
- local development can still use `tokens.py`
- the service starts at boot through `systemd`
- PostgreSQL is the only durable store

## Service Management

```bash
sudo systemctl status maxogram
sudo journalctl -u maxogram -f
sudo systemctl restart maxogram
sudo systemctl stop maxogram
```

## Supported Operating Systems

- Debian and Ubuntu
- Fedora and RHEL-family systems
- openSUSE
- Arch Linux
- generic Linux best-effort fallback

## Development Notes

- Runtime config loading is env-first, with `tokens.py` kept as a local-development fallback.
- Database schema changes should continue to go through Alembic via `python -m maxogram db-upgrade`.
- The full runtime and table reference lives in [architecture.md](/C:/Users/LA/YandexDisk/Coding/Maxogram/architecture.md).
