# TODO - Agent 275 (Haybrook)


- [x] Inspect existing scraper-agent implementations to copy the approved Agent 4 baseline structure.
- [x] Create `backend/scraper-agent-275.js` using Agent 4 rules and minimal changes.

- [ ] Configure two property types:
  - [ ] SALES (Haybrook)
  - [ ] RENTALS (Haybrook)
- [ ] Listing pagination:
  - [ ] Implement fixed page count for both sales and rentals (50 total pages each) using `Page=1..50`.
- [ ] Listing extraction on listing pages:
  - [ ] Extract property detail URLs from the search results HTML.
  - [ ] Extract price, bedrooms when possible.
- [ ] Detail extraction:
  - [ ] Use provided HTML approach: extract coordinates/bedrooms from embedded JS payload (if available) or DOM fallback.
- [ ] DB persistence:
  - [ ] Update price via `updatePriceByPropertyURLOptimized`.
  - [ ] For new records, call `processPropertyWithCoordinates`.
- [ ] Baseline compliance:
  - [ ] Correct logging via `createAgentLogger`.
  - [ ] Call `updateRemoveStatus(AGENT_ID, scrapeStartTime)` only at end of full run.
- [ ] Run a quick lint/test command (or node syntax check) and fix any issues.

