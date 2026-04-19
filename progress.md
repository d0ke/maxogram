# Progress

## Current State

- Runtime: the project runs as a single asyncio process with five worker loops: Telegram poller, MAX poller, normalizer, delivery, and reconciliation. Production configuration is env-first with `tokens.py` kept as a fallback for local development, the public deployment artifact is a Docker image based on Python `3.12`, local development is expected to use Python `3.12`, and PostgreSQL is the only durable store for state and queueing.
- Relay behavior: bidirectional text relay with alias prefixes, native replies where mappings exist, real media relay for common attachment types, grouped photo/video chunk relay between Telegram and MAX, GIF and animation handling, Telegram animated sticker relay to MAX through on-demand `.tgs -> GIF` conversion with a container-local cache, supported Telegram/MAX formatting preservation, repeated-forward unwrap for mirrored bot messages, bridge commands, and mirrored edit/delete sync with pending-mutation replay.
- Storage and schema: the live schema is managed by Alembic revisions `20260410_0001` and `20260419_0002`, covering the baseline relay tables plus persistent Telegram media-group buffering and logical chunk/member mapping tables for grouped photo/video sync.
- Delivery model: ordered outbox processing, lease heartbeats, retry/backoff, DLQ handling, grouped chunk/member mappings, and persisted `delivery_state` snapshots support slow media, grouped sends, and later edit/delete classification.
- Deployment and tests: production uses Docker Compose with `docker.io/d0ke/maxogram:latest`, local PostgreSQL installs resolve one explicit target before admin actions, and automated tests cover config loading, docs/installer invariants, deduplication, commands, normalization, rendering, pollers, delivery, reconciliation, platform clients, and optional database connectivity.
- Known limitations: ordinary Telegram chat-history deletions are still not broadly visible to the bot, service/member events are not broadly mirrored yet, proxy DB settings and `media_objects` are not wired into runtime, metrics are collected but not exposed, and secrets still come from env or `tokens.py` rather than DB-backed credentials.

## 2026-04-10 to 2026-04-11

- Built the initial bridge runtime: repository-root entrypoint, CLI commands, async app lifecycle, SQLAlchemy models, Alembic integration, repositories, worker loops, platform adapters, and baseline tests.
- Established the durable PostgreSQL relay model around `inbox_updates`, `canonical_events`, `outbox_tasks`, `message_mappings`, `pending_mutations`, `dead_letters`, and `link_codes`, then fixed early database and migration integration issues for local upgrade and VPS compatibility.
- Added bidirectional reply bridging, real media relay for common attachment types, GIF/animation handling, formatting preservation, edit/delete synchronization, pending-mutation replay, and safer Telegram/MAX deduplication and normalization behavior.
- Hardened delivery for slow and large transfers by separating claim/read/finalize phases, renewing outbox leases during long sends, and ignoring stale success/failure finalization attempts.

## 2026-04-12 to 2026-04-13

- Added repeated-forward unwrap for mirrored bot messages, recovered nested forwarded MAX content more reliably, and extended regression coverage around forwarded text, forwarded media, and wrapper parsing safety.
- Switched the runtime to env-first configuration while keeping `tokens.py` as a local fallback, and refreshed the main documentation so the implementation snapshot, deployment story, and storage model were documented accurately for that stage.
- Evolved the installer into the current Docker-first deployment flow, including safe local PostgreSQL target resolution, explicit preserve-data behavior, remote-host bypass for PostgreSQL provisioning, `/etc/maxogram/maxogram.env`, `/opt/maxogram/docker-compose.app.yml`, and published-image deployment through Docker Compose.
- Expanded installer and docs coverage around local PostgreSQL discovery, ambiguity handling, Python bootstrap capability checks, Docker deployment invariants, and the documented one-line install/update commands.

## 2026-04-16 to 2026-04-17

- Polished audio and voice relay behavior, improved MAX attachment send readiness, and fixed Telegram-success serialization edge cases that had caused duplicate resend storms and missing mappings.
- Restored `Telegram -> MAX` relay for `video_note` messages and added regression coverage for audio follow-up queueing, safe Telegram result fallback, mapping finalization, and later pending-edit replay.
- Added `Telegram -> MAX` animated sticker relay through on-demand `.tgs -> GIF` conversion, introduced a container-local animated sticker cache under `temp/animated_sticker_cache`, and wired cache pruning into reconciliation.
- Simplified the Telegram-audio follow-up text behavior to send as a standalone next message while preserving outbox order and avoiding extra `message_mappings`.

## 2026-04-18

- Moved the supported runtime baseline from Python `3.13` to Python `3.12` across the Docker image, package metadata, and static-analysis configuration, and refreshed `README.md` to match.
- Fixed the Docker production artifact for animated sticker conversion by forcing executable permissions on bundled `pyrlottie` helper binaries after install.
- Added a GitHub Actions Docker smoke check that verifies the animated sticker converter binaries are executable before publish, catching container permission regressions earlier.

## 2026-04-19

- Added native grouped `photo/video` chunk relay between Telegram and MAX, including MAX multi-attachment normalization, Telegram album buffering with a quiet-window flush, persistent chunk/member mapping tables, and grouped delete/edit support.
- Reworked grouped delivery around actual materialized file sizes: bidirectional media budgeting, ordered splitting, per-item oversize hint stubs, same-attempt refinement, non-retryable oversize classification, and persisted `delivery_state` snapshots in successful `outbox_tasks.task` payloads.
- Narrowed the safe caption-only grouped edit optimization for `MAX -> Telegram`, kept delete-and-recreate as the conservative grouped-edit fallback elsewhere, and expanded regression coverage for grouped normalization, delivery, deletes, edit classification, oversize handling, and repository edge cases.
- Refreshed `architecture.md` and compressed `progress.md` so the documentation now matches the live grouped-media schema, delivery model, and current high-level project state.
