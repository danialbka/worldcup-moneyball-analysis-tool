# WC26 Terminal (Ratatui TUI)

Bloomberg-style football terminal in the terminal. Pulse shows live match probabilities; Terminal view drills into a single match.

## Requirements

- Rust toolchain (stable)
- Network access (FotMob endpoints)

## Quick start

```bash
cargo run
```

The app reads `.env.local` first, then `.env`.

## Configuration

Copy `.env.example` to `.env.local` and edit as needed.

Key variables:

- `APP_LEAGUE_PREMIER_IDS` / `APP_LEAGUE_WORLDCUP_IDS`: League filters (FotMob IDs: 47 = PL, 77 = World Cup).
- `PULSE_POLL_SECS`: Live match refresh interval (seconds).
- `PULSE_DATE`: Optional matchday override (YYYYMMDD). Empty uses FotMob default (today).
- `UPCOMING_SOURCE`: `fotmob` or `auto` (same behavior right now).
- `UPCOMING_POLL_SECS`: Minimum seconds between manual upcoming fetches.
- `UPCOMING_DATE`: Optional matchday override (YYYYMMDD) for upcoming list.
- `UPCOMING_WINDOW_DAYS`: Number of days to fetch (1-14). Use 7 for a full weekend slate.

Notes:

- FotMob expects `date=YYYYMMDD`. ISO `YYYY-MM-DD` returns `null`.
- Win% is locally computed (simple heuristic + jitter).

## Controls

Global:

- `1`: Pulse
- `Enter` / `d`: Terminal (for selected live match, triggers matchDetails)
- `b` / `Esc`: Back
- `l`: Toggle league mode (Premier League / World Cup)
- `u`: Toggle Upcoming view and fetch matchday list
- `i`: Fetch match details (lineups/events/stats)
- `?`: Help overlay
- `q`: Quit

Pulse:

- `j/k` or `↑/↓`: Move selection (Live) / scroll list (Upcoming)
- `s`: Cycle sort mode

## Data sources

Live + upcoming:

- `https://www.fotmob.com/api/data/matches`
- `https://www.fotmob.com/api/data/matches?date=YYYYMMDD`

Detailed (manual wiring if needed later):

- `https://www.fotmob.com/api/data/matchDetails?matchId=...`

## Troubleshooting

- If upcoming is empty, set `UPCOMING_DATE` to a known matchday (YYYYMMDD) and press `u`.
- If Pulse shows nothing, confirm `APP_LEAGUE_*` IDs match the FotMob league IDs and that `PULSE_DATE` is set to a matchday with fixtures.
# football
