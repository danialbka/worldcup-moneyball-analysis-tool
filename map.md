# Project Map (WC26 Terminal)

Quick orientation guide for navigating the repo.

## File system tree

```
.
├── .env.example
├── .gitignore
├── Cargo.lock
├── Cargo.toml
├── FEATURES.md
├── README.md
├── src/
│   ├── analysis_export.rs
│   ├── analysis_fetch.rs
│   ├── analysis_rankings.rs
│   ├── feed.rs
│   ├── main.rs
│   ├── persist.rs
│   ├── state.rs
│   └── upcoming_fetch.rs
└── worldcup_analysis_*.xlsx (generated exports)
```

## High-level architecture

```
+------------------+          +--------------------+
| UI + Input Loop  |  deltas  |  AppState + Delta  |
|   (main.rs)      +<---------+   (state.rs)       |
|                  |          +--------------------+
|  sends commands  |                      ^
+--------+---------+                      |
         |                                |
         v                                |
+------------------+          +--------------------+
| Background worker|  fetch   | FotMob endpoints   |
| (feed.rs)        +--------->| (matches/details)  |
|                  |          | + analysis data    |
+------------------+          +--------------------+
```

## Module map (src)

- `main.rs`  
  App entry, UI rendering (Ratatui), input handling (Crossterm), and
  command dispatch to background provider.

- `state.rs`  
  Core domain types and UI state (`AppState`), filters, selections,
  and `Delta` application.

- `feed.rs`  
  Background worker thread: polls FotMob, handles `ProviderCommand`
  requests, and emits `Delta` updates.

- `upcoming_fetch.rs`  
  FotMob API calls and JSON parsing for match lists and match details
  (events, lineups, stats).

- `analysis_fetch.rs`  
  Team, squad, and player data fetch/parsing (World Cup seeds and
  Premier League fallback list).

- `analysis_rankings.rs`  
  Computes role rankings from cached squads/player details using
  per-role z-score features.

- `analysis_export.rs`  
  XLSX export pipeline for analysis/squad/player stats.

- `persist.rs`  
  On-disk cache load/save for analysis and ranking caches (XDG cache or
  `~/.cache/wc26_terminal/cache.json`).

## Runtime flow (data)

```
UI event -> ProviderCommand -> background worker -> Delta -> AppState
```

Where:
- `ProviderCommand` requests fetches/exports.
- `Delta` contains state changes (matches, details, analysis, logs).

## API endpoints and how they interact

Primary endpoints:
- `https://www.fotmob.com/api/data/matches`
- `https://www.fotmob.com/api/data/matches?date=YYYYMMDD`
- `https://www.fotmob.com/api/data/matchDetails?matchId={match_id}`
- `https://www.fotmob.com/api/leagues?id={league_id}`
- `https://www.fotmob.com/api/teams?id={team_id}`
- `https://www.fotmob.com/api/playerData?id={player_id}`

How the app chains them:
```
matches (live list)
  └─> match_id ──> matchDetails (events/lineups/stats)

matches?date=YYYYMMDD (upcoming list)
  └─> same match_id format as live list

leagues?id=47 (Premier League)
  └─> team_id ──> teams?id={team_id} (team overview + squad ids)
               └─> player_id ──> playerData?id={player_id}
```

Notes:
- Live + upcoming use the same base `matches` response shape; upcoming
  just filters by `started/finished/cancelled`.
- Match list items carry `matchId` (used for details) and `leagueId`
  (used for filtering by league mode).
- Analysis fetch uses `leagues` to discover team IDs, then `teams` for
  FIFA info and squad, then `playerData` for detailed player stats.

## Config entry points

- `.env.example` -> copy to `.env.local`
- `README.md` lists env vars (league IDs, poll intervals, date overrides).

## Where to start when browsing

- UI behavior: `src/main.rs`
- State model + filtering: `src/state.rs`
- Network/data parsing: `src/upcoming_fetch.rs`, `src/analysis_fetch.rs`
- Background orchestration: `src/feed.rs`
