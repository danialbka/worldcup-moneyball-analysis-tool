# Bug Report: Missing Match Commentary (FotMob Live Ticker)

## Symptom

- Match details fetch succeeds (`[INFO] Match details request sent`) but the TUI shows no commentary / "No commentary yet" for matches that do have a FotMob ticker.
- This was reproducible in La Liga (example match id `4837312`) and could also affect Premier League matches.

## Repro

1) Open a live match in the Terminal view.
2) Press `i` to fetch match details.
3) Navigate to the commentary/ticker panel.
4) Observe the panel is empty even though the FotMob website shows live ticker entries.

## Root Cause

FotMob `/api/data/ltc` responses sometimes contain negative values for `elapsed` and/or `elapsedPlus` (e.g. `-1`) on some ticker events (commonly substitutions or other non-timed entries).

Our ltc parser deserialized these fields as `u16`. When a negative integer appears, `serde_json` fails the entire decode, causing us to drop commentary entirely.

Example failing error (captured via `--dump-match-details`):

- `invalid ltc json: invalid value: integer -1, expected u16`

## Fix

1) Make `LtcEvent.elapsed` / `LtcEvent.elapsed_plus` signed (`i16`) and convert to `Option<u16>` when mapping into `CommentaryEntry`, ignoring negatives.
2) Surface failures instead of silently discarding:
   - Add `commentary_error: Option<String>` to `MatchDetail`.
   - Populate it when ltc fetch/parse fails.
   - Show the error in the commentary view and debug dumps.

## Files Changed

- `src/upcoming_fetch.rs`
  - `LtcEvent.elapsed` and `elapsed_plus` changed from `Option<u16>` to `Option<i16>`.
  - Map to `CommentaryEntry` using `u16::try_from(val).ok()` to drop negatives.
  - Improved parse error reporting with a short JSON head preview.
  - `fetch_match_details_from_fotmob` now records `commentary_error` on failure.

- `src/state.rs`
  - `MatchDetail` gained `commentary_error: Option<String>` (serde default).
  - Initialized defaults in placeholder and delta paths.

- `src/main.rs`
  - UI shows ticker errors when present.
  - Added a `--dump-match-details <matchId>` debug mode to validate commentary counts without running the TUI.

## Validation

- Before fix:
  - `cargo run -- --dump-match-details 4837312` showed `commentary=0` and `commentary_error=invalid ltc json ... expected u16`.
- After fix:
  - `cargo run -- --dump-match-details 4837312` shows non-zero commentary and no error.

