# Placeholder Match Disappearing in Terminal View

## Summary
When the placeholder match was toggled on, it appeared briefly and then vanished or showed empty panels (pitch/stats/events) after entering the Terminal view. This made it hard to preview the UI reliably.

## Impact
- Placeholder match would blink out after a few seconds.
- In Terminal view, the Pitch/Stats/Event Tape panels reverted to "no lineups" / empty content.
- Required repeatedly pressing `p` to re-enable the placeholder.

## Root Cause
The app continuously refreshes live match data and match details from the network:
- **Live matches refresh** (`SetMatches`) replaces the whole match list every few seconds.
- **Match details refresh** (`FetchMatchDetails`) runs when you enter the Terminal view or on live refresh ticks.

The placeholder match is not real, so the network call to the match-details endpoint returns `null`. That `null` response overwrote the placeholder’s cached details with empty data, which made the Terminal UI appear to "blink out".

## Fix Overview
I made the placeholder behave like an internal, non-network match:
- **Prevent network fetching** for the placeholder match ID.
- **Keep placeholder details pinned** in state, even when live updates arrive.
- **Re-insert placeholder match and details** during live refresh so they persist.

## Fix Details
### 1) Keep placeholder in the live match list
- **File:** `src/state.rs`
- **Change:** When `SetMatches` applies live updates, re-add the placeholder match if it is enabled.

### 2) Keep placeholder details in state after refresh
- **File:** `src/state.rs`
- **Change:** When `SetMatches` runs, ensure placeholder details are present in `match_detail` so the Pitch/Stats/Event Tape panels still have data.

### 3) Skip network match-details fetch for the placeholder
- **File:** `src/main.rs`
- **Change:** In `request_match_details_for`, short-circuit if the selected match is the placeholder and re-use local placeholder details. This prevents the `null` response from wiping out the UI.

### 4) Skip refresh ticks for placeholder details
- **File:** `src/main.rs`
- **Change:** `maybe_refresh_match_details` ignores the placeholder ID to avoid unnecessary detail refresh cycles.

## Why This Fix Works
- The placeholder match is synthetic and should not be overwritten by live network data.
- By keeping it out of live detail fetching, its UI panels stay stable.
- Live refreshes still happen for real matches, but the placeholder remains pinned.

## How to Test
1. Run the app: `cargo run`.
2. Press `p` to toggle the placeholder on.
3. Enter the placeholder match (Terminal view).
4. Wait several refresh cycles.

Expected result: The placeholder match and its Pitch/Stats/Event Tape stay visible without disappearing.

## Files Touched
- `src/state.rs`
- `src/main.rs`
