# Copilot Instructions for this Workspace

Follow `backend/AGENT4_BASELINE_RULES.md` as the primary engineering standard.

## Required Defaults

- Use `backend/scraper-agent-4.js` as implementation baseline for scraper refactors.
- Keep scraper behavior stable unless user requests a behavior change.
- Prefer shared helpers (e.g., `processPropertyWithCoordinates`) over duplicate inline logic.
- Treat `backend/combined-scraper.js` as the default orchestration contract.

## Before every code change or answer

1. Load project context from `backend/AGENT4_BASELINE_RULES.md`.
2. Read relevant files before editing.
3. Keep edits small, targeted, and style-consistent.
4. Validate edited files for errors.

## Scraper-specific rules

- Use `crawler.run(initialRequests)` for initial static queue.
- Use pre-navigation resource blocking and reuse the same helper on detail pages.
- Maintain remove-status flow: mark all removed at start, un-remove when seen.
- Keep page-progress and action-based logging via `backend/lib/logger-helpers.js`.
- Preserve per-agent storage isolation in orchestrator via `CRAWLEE_STORAGE_DIR=backend/storage/agent-{id}`.
- Reuse helper modules before adding new function variants:
  - `backend/lib/property-helpers.js`
  - `backend/lib/db-helpers.js`
  - `backend/lib/logger-helpers.js`

## Logging quality

- No duplicate meaning in logs.
- Show action (`CREATED`, `UPDATED`, `SEEN`, `ERROR`) on property lines.
- Keep DB per-property logs quiet unless `DB_VERBOSE_LOGS=1`.

## Combined-scraper compatibility

- Keep agent CLI invocation compatible with orchestrator (`node scraper-agent-{id}.js [startPage]`).
- Do not introduce agent-side changes that break sequential queue execution.
