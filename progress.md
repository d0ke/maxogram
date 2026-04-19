# Progress

## Current State

- Runtime: the project runs as a single asyncio process with five worker loops: Telegram poller, MAX poller, normalizer, delivery, and reconciliation. Production configuration is env-first with `tokens.py` kept as a fallback for local development, the public deployment artifact is a Docker image based on Python `3.12`, local development is expected to use Python `3.12`, and PostgreSQL is the only durable store for state and queueing.
- Supported behavior: bidirectional text relay with alias prefixes, native replies where mappings exist, real media relay for common attachment types, grouped photo/video chunk relay between Telegram and MAX, GIF and animation handling, Telegram animated sticker relay to MAX through on-demand `.tgs -> GIF` conversion with a container-local cache, supported Telegram/MAX formatting preservation, repeated-forward unwrap for mirrored bot messages so alias wrappers and forwarded media survive re-forwarding cleanly, bridge commands, and mirrored edit/delete sync with pending-mutation replay.
- Installer behavior: local PostgreSQL installs now resolve one explicit live cluster or instance before any admin action, prefer preserving existing Maxogram data over blindly picking the newest version, fail closed instead of silently falling back to port `5432` after discovery errors, and deploy the application through Docker Compose instead of host Python and `systemd`.
- Known limitations: ordinary Telegram chat-history deletions are still not broadly visible to the bot, service/member events are not broadly mirrored yet, proxy DB settings and `media_objects` are not wired into runtime, metrics are collected but not exposed, and secrets still come from `tokens.py` rather than DB-backed credentials.
- Schema and migrations: two Alembic revisions now exist. `20260410_0001` creates the baseline schema, and `20260419_0002` adds persistent Telegram media-group buffering plus logical chunk/member mapping tables for grouped photo/video sync.
- Test coverage: automated tests cover config loading, deduplication, commands, normalization, rendering, pollers, delivery, reconciliation, platform clients, and optional database connectivity.

## 2026-04-18

- Fixed the Docker production artifact for Telegram animated sticker conversion by forcing executable permissions on bundled `pyrlottie` Linux helper binaries after the final `pip install .` layer.
- Added a Docker publish smoke check in GitHub Actions that builds the image locally, verifies `pyrlottie/linux_x86_64/lottie2gif` and `gif2webp` are executable, and fails before push if the container still reports `permission denied`.
- Confirmed the animated sticker regression root cause was Docker runtime file permissions inside the published image rather than bridge normalization, database state, or delivery fallback logic.
- Moved the supported runtime baseline from Python `3.13` to Python `3.12` by changing the production `Dockerfile` base image and tightening package metadata in `pyproject.toml` to `>=3.12,<3.13`.
- Updated Ruff and mypy configuration targets to Python `3.12` so local static analysis matches the supported runtime.
- Updated `README.md` to state that both production and local development now target Python `3.12`, and that existing Python `3.13` virtual environments are unsupported and should be recreated.
- Reworked `README.md` copy to be more user-friendly by simplifying the installer description, renaming the production setup section, clarifying sticker and animation support, removing extra installer wording, and adding a direct `Storage and Privacy` section that explains what PostgreSQL stores, what stays temporary on disk, and which retention rules exist today.
- Kept the animated sticker `.tgs -> GIF` path unchanged and treated this as a runtime compatibility fix for `pyrlottie` and its `numpy<2` dependency chain rather than a converter refactor.

## 2026-04-19

- Fixed destination-side oversized media handling after grouped `photo/video` relay changes by moving real chunk planning into the delivery worker and basing decisions on actual materialized file sizes instead of source-payload guesses.
- Added bidirectional outbound media budgeting and ordered splitting for `MAX -> Telegram` and `Telegram -> MAX`, including a `10`-item cap, a `48 MB` internal multi-piece packing budget, Telegram upload hard limits (`10 MB` photo, `50 MB` other uploads), and the existing project `50 MB` MAX upload limit.
- Reworked grouped delivery so one logical source chunk can emit multiple ordered destination messages plus inline bracketed oversize hint stubs, while persisting the full emitted destination id list for delete/reply/edit flows without a schema migration.
- Added non-retryable oversized upload classification for Telegram and MAX destination errors, local in-attempt refinement of failing media pieces, and immediate text fallback for single media that is too large even alone, preventing retry storms for deterministic `413` / `entity too large` failures.
- Enriched successful `outbox_tasks.task` payloads with `delivery_state` snapshots that record the actual delivery shape, whether media was filtered out, and the ordered emitted destination ids, and added repository access to the created successful send snapshot so edit classification can distinguish real text fallbacks and split groups from source intent.
- Tightened grouped edit rules to keep the Telegram caption-only optimization only for actual single-piece deliveries with matching media identities, while any split or filtered group now uses delete-and-recreate symmetrically and single-media text fallbacks are edited as text rather than media.
- Preserved MAX attachment `size` metadata for `image` and `video` payloads when MAX already exposes it, while keeping correctness anchored to local file size after download.
- Added regression coverage for oversize single-media fallbacks, bidirectional byte-based group splitting, per-item oversize stubs, same-attempt oversize piece refinement, actual-delivery snapshot persistence, expanded grouped delete coverage, Telegram oversize error classification, and MAX `413` classification.
- Added native grouped `photo/video` chunk relay between Telegram and MAX, including mixed `photo + video` chunks, while keeping existing single-message media flows unchanged.
- Reworked normalization so one MAX message with multiple supported `image/video` attachments becomes one logical chunk event with ordered `media_items[]`, stable `group_key`, and chunk-scoped caption handling.
- Added durable Telegram media-group buffering with a quiet-window flush model keyed by `telegram:<chat_id>:<media_group_id>`, so Telegram album members are aggregated into one canonical chunk event instead of being mirrored as separate MAX messages.
- Added persistent logical chunk and ordered member mapping tables so replies, edits, deletes, and chunk recreation can resolve `many Telegram member ids <-> one mirrored chunk` without overloading ordinary single-message mappings.
- Updated delivery so:
  - `Telegram -> MAX` grouped photo/video sends and edits use one MAX message with the full ordered attachments array.
  - `MAX -> Telegram` grouped sends use Telegram media groups, defensively split oversized payloads into `<=10` items per outbound album call, and place the caption only on the first emitted item.
  - `MAX -> Telegram` grouped edits use delete-and-recreate of the mirrored Telegram chunk and replace stored destination member ids after success.
  - grouped deletes remove the whole mirrored chunk instead of only one member.
- Added targeted regression coverage for grouped normalization, Telegram album aggregation, Telegram native media-group sends, MAX multi-attachment sends/edits, and both grouped edit directions in delivery.
- Fixed Telegram media-group buffer resequencing so one transaction can ingest multiple album members safely with `autoflush=False`, avoiding the repeated `telegram_media_group_buffer_members_position_uq` crash that had blocked all later normalizer work.
- Changed Telegram album member ordering to canonical message-id order instead of first-arrival order, so out-of-order Telegram updates still flush into one stable ordered chunk.
- Added regression coverage for out-of-order Telegram album arrival in the normalizer tests and an optional Postgres-backed repository test that commits one whole media group in a single async session without hitting the former unique-constraint failure.
- Optimized the narrow `MAX -> Telegram` grouped edit case where only the caption changes and the ordered media identities still match the original `message.created` chunk, so the bridge now edits the first mirrored Telegram album caption in place instead of deleting and recreating the whole album.
- Kept the conservative grouped-edit fallback for any media change, missing created-state comparison data, identity mismatch, or non-Telegram destination, so all uncertain grouped edits still use the existing delete-and-recreate path.
- Added delivery regressions for grouped caption-only Telegram edits, grouped edit-mode classification against created-state media identities, and the conservative recreate fallback when grouped media differ or created-state payloads are unavailable.

## 2026-04-16

- Updated mirrored `audio` and `voice` rendering to use `🔊 {Alias}` consistently in both directions while preserving forwarded and reply prefix lines and keeping any original audio caption or text on the next line.
- Reworked `Telegram -> MAX` mirrored audio and voice delivery so the primary MAX message is attachment-only and a second mirrored text message is enqueued after success as a reply to that audio, while `MAX -> Telegram` still uses the composed audio string as the mirrored caption.
- Removed the forced `sleep_after_input_media=False` override from MAX upload sends and replacement edits so the MAX SDK can wait for uploaded attachments before `POST /messages`, fixing the recurring first-attempt `attachment.not.ready` failures for Telegram-origin audio.
- Added regression coverage for the new Telegram-audio follow-up text outbox flow, its retry behavior without extra `message_mappings`, and MAX attachment-only upload sends.
- Fixed `MAX -> Telegram` duplicate resend storms that happened after Telegram had already accepted a send but local result serialization failed, which previously caused delivery retries, duplicate Telegram messages, and missing `message_mappings`.
- Reworked Telegram send-result handling to capture the successful `message_id` first, serialize returned Telegram `Message` objects through aiogram's safe serializer, and fall back to a minimal raw payload with a warning log instead of converting post-success serialization issues into retryable delivery failures.
- Added regression coverage for:
  - Telegram text sends that succeed even when post-send result serialization fails
  - Telegram media sends that use the same safe fallback path
  - delivery-worker success finalization and mapping creation when Telegram result serialization falls back
  - `MAX -> Telegram` send-plus-later-edit flow so pending edits replay once after mapping creation without repeated send retries or expired pending mutations
- Restored `Telegram -> MAX` relay for Telegram `video_note` messages by classifying them as supported inbound video media instead of letting them degrade to `"[unsupported message]"`.
- Added regression coverage for Telegram `video_note` normalization and delivery so round-video messages now download from Telegram and upload to MAX through the existing video pipeline.

## 2026-04-17

- Changed `Telegram -> MAX` audio and voice follow-up text delivery to send as a normal standalone next message instead of a reply to the mirrored MAX audio, while preserving the existing outbox ordering and retry behavior.
- Updated delivery regression coverage so the auxiliary follow-up task no longer carries `reply_to_message_id`, still appears immediately after the audio in queue order, and still avoids creating an extra `message_mapping`.
- Added `Telegram -> MAX` animated sticker relay by treating Telegram `.tgs` stickers as relayable image media, converting them to cached GIF files on demand, uploading the resulting GIF to MAX as a normal image, and falling back to text-only delivery when conversion fails instead of retrying the whole message.
- Added a container-local animated sticker cache under `temp/animated_sticker_cache`, keyed by source media identity plus a conversion profile version, with reuse on cache hits and daily pruning of entries not touched for more than 90 days from the reconciliation worker.
- Added regression coverage for animated sticker normalization, cache hit and miss materialization, text fallback on conversion failure, cached-media cleanup behavior, and reconciliation cache pruning cadence.

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

## 2026-04-13

- Reworked `install.sh` so local PostgreSQL admin actions always run against one explicitly resolved target instead of bare `psql` or `createdb`, eliminating Debian `pg_wrapper` mis-selection and silent fallback to `5432`.
- Added Debian cluster discovery through `pg_lsclusters`, including cluster-specific binary selection, safe reuse of existing Maxogram env and database state, and explicit ambiguity errors when multiple top-priority live clusters remain.
- Added generic Linux local PostgreSQL discovery via running postmaster processes and `postmaster.pid`, including port, socket, config, and service-unit resolution without hardcoding version-specific service names.
- Updated local installer role, database, search-path, and manual port-change flows to use the selected PostgreSQL target metadata consistently, including re-discovery after a local port rewrite.
- Made `install.sh` sourceable for pytest-based helper coverage and added installer behavior tests for Debian and generic multi-instance selection, preserve-data priority, ambiguity failures, remote-host bypass, and local port rewrites.
- Updated `README.md` to document the new local PostgreSQL target selection order and the stricter handling of non-default local PostgreSQL ports.
- Hardened Python bootstrap in `install.sh` so interpreter selection now probes real `venv` + `pip` capability instead of trusting version checks alone, tries Debian/Ubuntu `pythonX.Y-venv` then `python3-venv` as a conservative auto-repair path, and falls back to source-built Python on other distros instead of failing later during `.venv` creation.
- Added installer bootstrap tests for Debian versioned and generic `venv` repair, the already-healthy interpreter fast path, non-`apt` source-build fallback, and explicit early failure when no discovered interpreter can create a working virtual environment.
- Updated `README.md` and installer static assertions to document the capability-based Python selection rule and the Debian/Ubuntu-only automatic `venv` package repair behavior.
- Reworked `install.sh` into a Docker-first deployment flow that keeps the PostgreSQL discovery, role, database, schema, and port-selection logic, but now installs Docker when needed, writes `/opt/maxogram/docker-compose.app.yml`, runs in-container `check-config` and `db-upgrade`, and deploys `docker.io/d0ke/maxogram:latest` with Docker Compose.
- Added Docker deployment assets for production use: a compose sample bound to `/etc/maxogram/maxogram.env`, an expanded `.env.example`, a Docker Hub publish workflow pinned to `docker.io/d0ke/maxogram`, and cleanup of legacy native-installer `systemd` and watchdog artifacts during updates.
- Rewrote `README.md`, refreshed `architecture.md`, and replaced installer/docs tests so the documented production path is now host-managed PostgreSQL plus a containerized Maxogram service instead of host-side Python and `.venv` management.
- Updated the documented one-line install commands to download `install.sh` into `/tmp` and run it as a local file for `auto`, `manual`, and `update`, avoiding the fragile stdin-pipe path for the interactive installer.
