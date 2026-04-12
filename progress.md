# Progress

## Current State

- Runtime: the project runs as a single asyncio process with five worker loops: Telegram poller, MAX poller, normalizer, delivery, and reconciliation. Production configuration is env-first with `tokens.py` kept as a fallback for local development, and PostgreSQL is the only durable store for state and queueing.
- Supported behavior: bidirectional text relay with alias prefixes, native replies where mappings exist, real media relay for common attachment types, GIF and animation handling, supported Telegram/MAX formatting preservation, repeated-forward unwrap for mirrored bot messages so alias wrappers and forwarded media survive re-forwarding cleanly, bridge commands, and mirrored edit/delete sync with pending-mutation replay.
- Known limitations: ordinary Telegram chat-history deletions are still not broadly visible to the bot, service/member events are not broadly mirrored yet, proxy DB settings and `media_objects` are not wired into runtime, metrics are collected but not exposed, and secrets still come from `tokens.py` rather than DB-backed credentials.
- Schema and migrations: one Alembic revision, `20260410_0001`, creates the current SQLAlchemy metadata; there are no later incremental migrations yet.
- Test coverage: automated tests cover config loading, deduplication, commands, normalization, rendering, pollers, delivery, reconciliation, platform clients, and optional database connectivity.

## 2026-04-10

- Built the initial bridge application skeleton: repository-root entrypoint, CLI commands, local configuration loading, async app lifecycle, SQLAlchemy models, Alembic setup, repositories, worker loops, platform adapters, and baseline tests.
- Established the durable PostgreSQL relay model around `inbox_updates`, `canonical_events`, `outbox_tasks`, `message_mappings`, `pending_mutations`, `dead_letters`, and `link_codes` for the 3-minute bridge confirmation flow.
- Fixed early database and migration integration issues so the code works with the current VPS schema and local upgrade flow:
  - command-log and message-mapping writes no longer depend on optional named constraints
  - percent-encoded database URLs work with `db-upgrade`
  - bridge confirmation flushes `tenants` before inserting dependent `bridges`
- Fixed bidirectional reply bridging so native replies now resolve correctly in both directions, including replies to already mirrored bridge messages.
- Replaced placeholder attachment mirroring with real media relay for photos, videos, documents/PDF, audio, voice, static stickers, and video stickers, using transient files under `temp/media_cache` and keeping fallback text for unsupported cases.

## 2026-04-11

- Hardened delivery for slow and large media transfers by separating claim/read/finalize phases, adding outbox lease-heartbeat renewal, and ignoring stale finalization attempts so long-running sends do not get duplicated.
- Fixed Telegram-origin media downloads so `Telegram -> MAX` attachment relay now uses Telegram `get_file()` plus `download_file(file_path, ...)` correctly.
- Implemented edit/delete synchronization in both directions with explicit event versions, pending-mutation replay, and durable re-enqueue when the mirrored destination mapping becomes available.
- Fixed MAX deduplication and edit-version handling so create/edit updates that share message ids no longer collapse before normalization, allowing repeated MAX edits to reach Telegram correctly.
- Added GIF and animation handling without transcoding, including download-time GIF detection for opaque MAX image URLs and Telegram animation delivery when mirrored media resolves to an animation.
- Implemented media-edit classification and real replacement edits in both directions, while preserving caption-only media edits and explicitly rejecting unsupported Telegram voice-media replacement edits.
- Added formatting preservation for supported Telegram entities and MAX markup, extended relay payloads with `text_plain` and `text_html`, and hardened Telegram polling so serialization-failing updates are skipped instead of crashing the poller.
- Fixed Telegram normalization after the safe serialization change by reading `from_user` and nested `reply_to_message.from_user`, restoring sender identity, alias resolution, and reply metadata.
- Refreshed `architecture.md` and `progress.md` so the project documentation now describes the current implementation instead of the original research-heavy design draft.

## 2026-04-12

- Fixed repeated forwarding of mirrored bot messages so the bridge now unwraps its own alias/reply/forward wrapper before re-rendering, preventing `Alias: [unsupported message]` and `Alias: Alias: ...` regressions.
- Recovered forwarded MAX content from nested `link.message` when the outer forwarded body is empty, including nested text, formatting markup, and media attachments.
- Added Telegram forwarded-bot unwrap logic with entity offset trimming so repeated forwards of mirrored text or captions preserve supported formatting after the bridge prefix is removed.
- Added regression tests for repeated forwarded text, repeated forwarded media, nested old forward/reply prefixes, and the safety case where an ordinary forwarded user message that looks like `Alias: text` must remain untouched.
- Added env-first runtime configuration loading so production installs can use `MAXOGRAM_*` variables while local development can still rely on `tokens.py` as a fallback.
- Added installer-oriented config coverage for repository-local `.env`, direct process environment variables, env-overrides-tokens precedence, and clean failures for incomplete env config.
- Added a production-focused `install.sh` for Debian, Ubuntu, and Fedora that installs or reuses PostgreSQL, manages a dedicated system user, prepares `.venv`, writes `/etc/maxogram/maxogram.env`, runs `python -m maxogram db-upgrade`, installs `maxogram.service`, and adds a four-hour cron restart watchdog.
- Added a real `README.md` with one-line install commands, installer behavior, prompt lists for `auto` and `manual`, and operational service commands.
- Rewrote `architecture.md` so it now documents the actual runtime schema table-by-table, including column intent, env-first config loading, and the installer's schema/search-path behavior.
- Expanded `install.sh` to support openSUSE and Arch Linux explicitly, use package-manager fallback detection for unknown distros, and continue with a generic Linux best-effort path instead of hard-stopping on unrecognized `os-release` values.
- Updated the installer watchdog logic so it still prefers cron where available but falls back to a `systemd` timer on generic or non-cron environments, avoiding total install failure when cron is unavailable.
- Reworked `install.sh` to remove the `git` dependency entirely and deploy the application from the public GitHub tarball, while preserving and upgrading an existing `.venv` in `/opt/maxogram`.
- Added installer `update` mode, switched the canonical PostgreSQL app role default to `maxogram_app`, and taught the installer to rebuild `/etc/maxogram/maxogram.env` around existing local Maxogram databases without dropping bridge or alias data.
- Split installer behavior between local and remote PostgreSQL hosts so remote `manual` installs skip local PostgreSQL provisioning, while the installer never edits `pg_hba.conf`, `listen_addresses`, or firewall state.
- Updated `README.md` and installer static tests to document tarball-based installs, `update`, the `maxogram` OS user plus `maxogram_app` DB user defaults, and the explicit no-firewall/no-network-policy-change guarantees.
