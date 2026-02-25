# Agent 4 Baseline Rules (Project Standard)

This file defines the coding and response rules we follow for this scraping project.
Agent `4` is the baseline implementation style.

## Project Context

- Runtime: Node.js (CommonJS)
- Scraping stack: `crawlee` + `playwright`
- DB: MySQL (`mysql2`)
- Backend location: `backend/`
- Orchestrator: `backend/combined-scraper.js`
- Key helpers:
  - `backend/db.js`
  - `backend/lib/db-helpers.js`
  - `backend/lib/property-helpers.js`
  - `backend/lib/logger-helpers.js`
  - `backend/lib/scraper-utils.js`

## Mandatory Pre-Work Checklist (Before Coding or Answering)

1. Read the target file(s) fully before changing behavior.
2. Confirm whether existing helper(s) already solve the task.
3. Keep changes minimal and consistent with Agent 4 style.
4. Verify logging clarity (no duplicate or conflicting lines).
5. Run diagnostics for edited files and fix introduced issues.

## Scraper Architecture Rules (Use Agent 4 Pattern)

1. **Crawler entrypoint**
   - Prefer `await crawler.run(initialRequests)` over `addRequests()+run()` for static initial request batches.
2. **Resource blocking**
   - Use a shared `blockNonEssentialResources(page)` helper.
   - Apply in `preNavigationHooks` (listing pages).
   - Reuse in detail pages.
3. **Timeouts**
   - Keep explicit `navigationTimeoutSecs` and `requestHandlerTimeoutSecs` on crawler.
4. **Data flow**
   - For each listing:
     - Call `updatePriceByPropertyURLOptimized(...)` first.
     - If not existing, scrape detail and call `updatePriceByPropertyURL(...)`.
5. **Remove-status strategy**
   - At run start: `markAllPropertiesRemovedForAgent(agentId)`.
   - On seen listings: updates must set `remove_status = 0`.

## Combined Runner Rules (`backend/combined-scraper.js`)

1. Use combined scraper as the default multi-agent entrypoint.
2. Keep agents executed sequentially in orchestrator unless user explicitly requests parallel execution.
3. Always isolate Crawlee storage per agent process:
   - set `CRAWLEE_STORAGE_DIR` to `backend/storage/agent-{agentId}` in spawned process env.
4. Preserve CLI behavior:
   - `<agentId> <startPage>` for single agent start page,
   - `<id1> <id2> ...` for sequential queue.
5. Keep user-facing queue logs in orchestrator concise and stable.
6. Prefer `combined-scraper.js` over ad-hoc `spawn` wrappers in utility files for normal multi-agent runs.

## Shared Helper Usage Rules

1. Prefer `isSoldProperty`, `formatPriceUk`, and coordinate extraction helpers from `property-helpers` over inline reimplementation.
2. Prefer `updatePriceByPropertyURLOptimized` + fallback `updatePriceByPropertyURL` pattern for listing persistence.
3. Keep DB helper return shape stable (`isExisting`, `updated`, optional `error`) for compatibility across agents.
4. Keep expensive/verbose per-property DB logging behind `DB_VERBOSE_LOGS=1`.
5. Use `logger-helpers` for all scraper runtime logs (step/page/property/error).

## Logging Rules

1. Use only shared logger from `backend/lib/logger-helpers.js`.
2. Include page progress using `totalPages` in `request.userData`.
3. Property logs must include action state: `CREATED`, `UPDATED`, `SEEN`, `ERROR`.
4. Avoid duplicate DB-level per-property noise:
   - Keep DB verbose logs behind `DB_VERBOSE_LOGS=1`.

## Request Metadata Standard

Every request `userData` should include (when applicable):

- `pageNum`
- `totalPages`
- `isRental`
- `label`

## Safety / Consistency Rules

- Do not change schema or table names unless explicitly requested.
- Do not add new dependencies unless required.
- Prefer helper extraction over copy-paste across agents.
- Preserve current behavior unless the user asked for behavioral change.
- When refactoring, keep agent CLI signature and output semantics compatible with `combined-scraper.js`.

## When Answering (Non-code responses)

- Be direct and implementation-focused.
- Reference exact files/functions impacted.
- If unsure, state assumptions and suggest the safest default.
