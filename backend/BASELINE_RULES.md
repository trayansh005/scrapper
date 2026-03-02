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

1. **Crawler selection**
   - **Prefer `CheerioCrawler` (API/JSON Extraction)**: If the target site (e.g., Homeflow sites like `countrywidescotland.co.uk`) embeds property data in a JSON object within the HTML source (e.g., `var propertyData`), use `CheerioCrawler` to extract and parse the JSON directly. This is faster and uses fewer resources.
   - **Fall-back to `PlaywrightCrawler`**: Use for sites requiring JavaScript execution or complex DOM interactions that cannot be solved via JSON extraction.
2. **Crawler entrypoint**
   - Prefer `await crawler.run(initialRequests)` over `addRequests()+run()` for static initial request batches.
3. **Resource blocking**
   - Use a shared `blockNonEssentialResources(page)` helper.
   - Apply in `preNavigationHooks` (listing pages).
   - Reuse in detail pages.
4. **Timeouts**
   - Keep explicit `navigationTimeoutSecs` and `requestHandlerTimeoutSecs` on crawler.
5. **Conditional Loop Sleep (Performance Optimization)**:
   - When processing property lists (especially large API payloads), only apply `await sleep(ms)` if the property was actually `CREATED` or `UPDATED`.
   - Skip the sleep for `UNCHANGED` records to allow the scraper to rapidly skip through thousands of known properties while remaining polite during write operations.
6. **Data flow**
   - For each listing:
     - Call `updatePriceByPropertyURLOptimized(...)` first.
     - If not existing, scrape detail (if missing coordinates) and call `processPropertyWithCoordinates(...)`.
   - **JSON-based extraction skip**: If coordinates and bedrooms are available in the JSON payload, call `processPropertyWithCoordinates(...)` directly for new records and skip the detail page entirely.
7. **Enhanced Remove-Status Strategy**
   - **Full Scrape**: Capture `scrapeStartTime` at the start of the execution.
   - **Safety Window**: Pass `scrapeStartTime` to `updateRemoveStatus(agentId, scrapeStartTime)`. This ensures only records NOT updated during THIS specific run are flagged as removed.
   - **Partial Run Protection**: Detect if the run is partial (e.g., `startPage > 1`). If so, **bypassing** `updateRemoveStatus` is MANDATORY to prevent accidental deletion of properties on pages not scraped.
   - ~~`markAllPropertiesRemovedForAgent` is deprecated and removed. Do not use it.~~

## API Scraper Architecture Pattern (Preferred over HTML/Playwright)

1. **Always Search for JSON APIs First**: Use browser network tools to find `.json`, `.ljson` (Homeflow), or GraphQL endpoints that provide raw property data before relying on HTML page parsing.
2. **Check for Next.js `__NEXT_DATA__`**: For sites built with Next.js, always check `window.__NEXT_DATA__` via `page.evaluate()`. This often contains the full property list with coordinates, prices, and bedroom counts, potentially eliminating the need to scrape detail pages.
3. **Prioritize Native `fetch`**: If a JSON API is available and not protected by bot-management, completely remove `crawlee`/`playwright` dependencies. Use lightweight native Node.js `fetch` loops for massive speedups.
4. **Bypass Cloudflare via In-Browser Fetch**: If native `fetch` is blocked (e.g., 403 "Just a moment..."), use Crawlee to navigate to a standard HTML search page to clear the Cloudflare challenge. Then, execute `fetch` natively _inside_ the established browser context using `page.evaluate()` to seamlessly retrieve the JSON payload.
5. **Eliminate Detail Page Scraping**: Whenever possible, extract coordinates (`lat`/`lon`), numeric prices, and bedroom counts directly from the API payload (or `__NEXT_DATA__`) to completely eliminate the overhead of loading individual property detail pages.

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
2. Prefer `updatePriceByPropertyURLOptimized` + fallback `processPropertyWithCoordinates` pattern for listing persistence.
3. Keep DB helper return shape stable (`isExisting`, `updated`, optional `error`) for compatibility across agents.
4. Keep expensive/verbose per-property DB logging behind `DB_VERBOSE_LOGS=1`.
5. Use `logger-helpers` for all scraper runtime logs (step/page/property/error).

## Logging Rules

1. Use only shared logger from `backend/lib/logger-helpers.js`.
2. **Mandatory Logging Methods**: Use `logger.step()`, `logger.page()`, `logger.property()`, and `logger.error()`. Avoid using `console.log` directly or non-standard methods like `logger.info`.
3. Include page progress using `totalPages` in `request.userData`.
4. **Verbose Status Logging**:
   - Log skip reasons clearly (e.g., `Skipped: Sold`, `Skipped: Already Processed`).
   - Log detail page progress (`[Detail] Scraping coordinates...`, `[Detail] Found coordinates`).
5. **Final Summary & Maintenance**:
   - Every agent should output a final summary block of stats.
   - **Mandatory**: Call `await updateRemoveStatus(AGENT_ID)` at the very end of every successful `run()`.
6. Avoid duplicate DB-level per-property noise:
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
