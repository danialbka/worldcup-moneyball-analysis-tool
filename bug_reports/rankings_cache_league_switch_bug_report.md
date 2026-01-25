# Rankings Cache Missing After League Switch

## Summary
Switching from World Cup analysis to Premier League analysis sometimes left the Role Rankings panel empty, even though the Premier League cache was present at startup.

## Impact
- Role Rankings appeared blank or showed a warming message after switching leagues.
- Users assumed the cache was lost, even though squad/player data still existed.

## Root Cause
Rankings recompute was triggered before analysis data loaded for the newly selected league. The rankings computation depends on the analysis teams list; with an empty list, it produced no rows and cleared `rankings_dirty`, preventing a later recompute once analysis arrived.

## Fix Overview
- Defer rankings recompute until analysis data is present.
- Mark rankings dirty whenever analysis data is updated so a recompute happens after the fetch completes.

## Fix Details
### 1) Guard recompute when analysis is empty
- **File:** `src/main.rs`
- **Change:** Only call `recompute_rankings_from_cache` when `analysis` is non-empty.

### 2) Force recompute after analysis refresh
- **File:** `src/state.rs`
- **Change:** Set `rankings_dirty = true` in `Delta::SetAnalysis`.

## Why This Fix Works
The rankings compute now waits for the required team analysis data and is re-triggered after analysis fetch completes, restoring cached rankings after a league switch.

## How to Test
1. Start the app with cached rankings for both leagues.
2. Open World Cup analysis, then switch to Premier League.
3. Open Role Rankings.

Expected result: rankings populate after analysis loads, using cached squads/players.

## Files Touched
- `src/main.rs`
- `src/state.rs`
