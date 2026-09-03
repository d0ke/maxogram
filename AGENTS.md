# Agent Instructions

## Startup and Context Refresh

At the start of a new dialog in this project, or after context compaction, read these files in full and in this order:

1. `AGENTS.md`
2. `README.md`
3. `architecture.md`
4. `progress.md`

During an uninterrupted working dialog with no context compaction, do not reread these files merely as a routine startup step. Reread a particular file only when it has changed or the current task requires its latest contents.

## Architecture

Before planning or making code changes, ensure that `README.md`, `architecture.md`, and `progress.md` have been read during the current uninterrupted dialog.

If a change affects the architecture described in `architecture.md`, update `architecture.md` in the same change so that it continues to describe the implementation accurately.

Very small changes do not require an architecture update unless they alter behavior, structure, an invariant, or another fact already documented in `architecture.md`.

## Progress

After making changes, update the `progress.md` file with what changed and the current project state.

## Live Runtime Investigation

When code or behavior analysis requires real bot messages or live runtime evidence, use the access details stored in the local `tokens.py` file:

1. Connect to the VPS using the host, SSH port, and credentials documented in `tokens.py`.
2. Before inspecting runtime state, stop the application container using the Docker Compose location and commands documented in `README.md`.
3. Inspect the relevant temporary directories and files on the VPS, including the applicable paths under `temp`.
4. Connect to PostgreSQL using `DB_CONFIG` from `tokens.py` and inspect the relevant raw updates, normalized payloads, canonical events, outbox tasks, mappings, and other database state needed for the investigation.
5. When the exact update or message returned to a bot must be inspected, use the appropriate Telegram or MAX bot token from `tokens.py` to call the platform's official HTTPS API directly and examine the response.
6. At the end of the inspection, start the application container again using the Docker Compose location and commands documented in `README.md`, then verify that the service is running.

Treat every value in `tokens.py` as a secret: never print secret values, copy them into documentation or logs, or commit them to version control.

## Language

All comments and headings must be written in English.
