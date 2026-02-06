# WC26 Terminal - Features Overview

This document provides a comprehensive overview of how the features work in the WC26 Terminal application.

## Architecture Overview

The application follows a **multi-threaded architecture** with clear separation between UI, state management, and data fetching:

- **Main Thread**: Handles UI rendering (Ratatui) and user input (Crossterm)
- **Background Thread**: Fetches data from FotMob API and processes updates
- **Channel Communication**: Uses Rust `mpsc` channels for thread-safe communication

```
┌─────────────┐         ┌──────────────┐         ┌─────────────┐
│   UI/Input  │◄──deltas─┤   AppState   │◄──cmds──┤ Background  │
│  (main.rs)  │          │  (state.rs)  │          │   (feed)    │
└─────────────┘          └──────────────┘          └─────────────┘
                                                          │
                                                          ▼
                                                   ┌─────────────┐
                                                   │   FotMob    │
                                                   │     API     │
                                                   └─────────────┘
```

## Core Features

### 1. Pulse View (Live Matches)

**Purpose**: Display live match data with win probability calculations.

**How it works**:
- Background thread polls FotMob API every `PULSE_POLL_SECS` (default: 15s)
- Fetches matches from `https://www.fotmob.com/api/data/matches`
- Filters matches based on league mode (Premier League or World Cup)
- Calculates win probabilities locally using heuristics based on score and match state
- Updates match minutes every 60 seconds for live matches
- Applies jitter to probabilities every ~900ms for visual dynamism

**Display Columns**:
- **Time**: Current minute (`45'`) or `FT` for finished matches
- **Match**: Home-Away team abbreviations
- **Score**: Current score (home-away)
- **Win% Bar**: Visual bar chart showing home/draw/away probabilities
- **H/D/A**: Numeric probabilities (Home/Draw/Away)
- **Delta**: Change in home team win probability since last update
- **Q**: Model quality (BASIC, EVENT, TRACK)
- **Conf**: Confidence percentage

**Sort Modes**:
- **HOT**: Sorted by absolute delta (most volatile matches first)
- **TIME**: Live matches first, then by minute
- **CLOSE**: Sorted by draw probability (closest matches)
- **UPSET**: Sorted by underdog win probability

**Key Bindings**:
- `j/k` or `↑/↓`: Navigate selection
- `s`: Cycle sort mode
- `Enter` or `d`: Open Terminal view for selected match

### 2. Upcoming View

**Purpose**: Show scheduled matches for the next N days.

**How it works**:
- User presses `u` to toggle to Upcoming view
- Triggers fetch from FotMob API with date range (`UPCOMING_WINDOW_DAYS`, default: 1)
- Fetches matches for each day in the window from `https://www.fotmob.com/api/data/matches?date=YYYYMMDD`
- Filters out already started/finished/cancelled matches
- Respects throttling (`UPCOMING_POLL_SECS`, default: 60s) to prevent API abuse
- Displays matches in scrollable list

**Display Columns**:
- **Kickoff (SGT)**: Match time in Singapore Time (UTC+8)
- **Match**: Home vs Away teams
- **League**: League name
- **Round**: Tournament stage/round

**Key Bindings**:
- `j/k` or `↑/↓`: Scroll through list
- `u`: Toggle back to Live view

### 3. Terminal View (Match Details)

**Purpose**: Deep dive into a single match with detailed information.

**How it works**:
- Activated by pressing `Enter`/`d` on a live match or `1` from Terminal view
- Automatically triggers match details fetch when entering Terminal view
- Displays comprehensive match information in Bloomberg-style layout

**Layout Sections**:

1. **Match List** (Left, Top):
   - Shows all filtered matches
   - Highlights currently selected match with `>`
   - Format: `Home-Away Score-Score`

2. **Group Mini** (Left, Bottom):
   - Placeholder for group standings (future feature)

3. **Pitch** (Middle, Top):
   - Placeholder for pitch visualization (future feature)

4. **Event Tape** (Middle, Bottom):
   - Shows last 6 match events
   - Events include: Goals, Cards, Substitutions, Shots
   - Format: `Minute' EVENT_TYPE Team Description`
   - Automatically updates when new events are detected

5. **Stats** (Right, Top):
   - Match time and score
   - Live status indicator
   - Up to 6 match statistics (e.g., Possession, Shots, Corners)
   - Format: `StatName: HomeValue-AwayValue`

6. **Lineups** (Right, Middle):
   - Starting XI for both teams
   - Formation (e.g., "4-3-3")
   - Player numbers, names, and positions
   - Substitutes list
   - Sorted alphabetically by team abbreviation

7. **Prediction** (Right, Bottom):
   - Win probabilities (Home/Draw/Away)
   - Delta (change in home team probability)
   - Model quality indicator

8. **Console** (Bottom):
   - Shows last 3 log messages
   - Includes fetch status, errors, and alerts
   - Goal alerts are highlighted

**Key Bindings**:
- `b` or `Esc`: Return to Pulse view
- `i`: Manually trigger match details fetch
- `1`: Return to Pulse view

### 4. Match Details Fetching

**Purpose**: Retrieve detailed match information (lineups, events, stats) from FotMob.

**How it works**:
- Makes HTTP GET request to `https://www.fotmob.com/api/data/matchDetails?matchId={match_id}`
- Parses JSON response structure:
  - **Lineups**: `content.lineup.homeTeam` / `content.lineup.awayTeam`
  - **Events**: `content.matchFacts.events.events[]`
  - **Stats**: `content.stats.stats[]`
- Handles various JSON field name variations (e.g., `homeTeam` vs `home`, `playerName` vs `name`)
- Falls back to seeded lineups if API fetch fails (for demo purposes)
- Stores results in `state.match_detail` HashMap keyed by match ID

**Trigger Points**:
- Automatically when entering Terminal view (`Enter`/`d`)
- Manually via `i` key (works in both Pulse and Terminal views)

**Data Parsing**:
- **Events**: Parses goals, cards, substitutions, shots with minute, team, and player info
- **Lineups**: Extracts formation, starting XI, and substitutes with player numbers and positions
- **Stats**: Parses match statistics with home/away values

### 5. League Mode Toggle

**Purpose**: Switch between Premier League and World Cup filtering.

**How it works**:
- Press `l` to cycle between league modes
- Filters matches based on:
  1. League ID matching (`APP_LEAGUE_PREMIER_IDS` / `APP_LEAGUE_WORLDCUP_IDS`)
  2. League name keyword matching (fallback)
- Resets selection and scroll positions when switching
- Applies to both Live and Upcoming views

**Configuration**:
- Premier League: Default IDs include 47 (FotMob ID)
- World Cup: Default IDs include 77 (FotMob ID)
- Keywords: "premier league", "premier", "epl" for PL; "world cup", "worldcup" for WC

### 6. Real-time Updates

**Purpose**: Keep match data current without manual refresh.

**How it works**:
- **Live Matches**: Polled every `PULSE_POLL_SECS` (default: 15s)
- **Match Minutes**: Incremented every 60 seconds for live matches
- **Probability Jitter**: Applied every ~900ms to simulate live probability changes
- **Event Detection**: Monitors score changes and generates goal events
- **Delta Calculation**: Tracks probability changes between updates

**Update Flow**:
1. Background thread fetches latest match data
2. Compares with existing matches to detect changes
3. Calculates deltas for win probabilities
4. Sends `Delta::UpsertMatch` or `Delta::SetMatches` to UI thread
5. UI thread applies deltas and re-renders

### 7. Event System

**Purpose**: Track and display match events (goals, cards, substitutions, shots).

**How it works**:
- **Score Changes**: Detected during match refresh, generates `EventKind::Goal`
- **Random Events**: Generated every ~900ms with 12% probability for live matches
- **Event Types**: Goals, Yellow Cards, Substitutions, Shots on target
- **Storage**: Events stored in `MatchDetail.events` vector
- **Display**: Shown in Event Tape (last 6 events) and Console (goal alerts)

**Event Parsing** (from API):
- Extracts from `content.matchFacts.events.events[]`
- Parses event type, minute, team (home/away), and player name
- Maps FotMob event types to internal `EventKind` enum

### 8. Win Probability Calculation

**Purpose**: Compute and display match outcome probabilities.

**How it works**:
- **Initial Calculation**: Based on score difference and match state
  - Finished matches: 100% for winner, 0% for others
  - Draw: 42% home, 30% draw, 28% away
  - 1-goal lead: 58% home, 25% draw, 17% away (or reverse)
  - 2+ goal lead: 75% home, 15% draw, 10% away (or reverse)
- **Live Updates**: Jitter applied to simulate probability changes
- **Quality Levels**: 
  - `BASIC`: Pre-match or finished matches
  - `EVENT`: Live matches with event tracking
  - `TRACK`: Future enhancement for tracking-based models
- **Confidence**: 68% for live matches, 84% for finished matches

**Delta Tracking**:
- Calculated as difference between current and previous probability
- Used for sorting (HOT mode) and display
- Helps identify volatile matches

## Data Flow

### Fetching Live Matches

```
Background Thread Loop:
  1. Sleep 900ms
  2. Check if PULSE_POLL_SECS elapsed
  3. If yes:
     a. Fetch from FotMob API
     b. Parse matches
     c. Merge with existing matches
     d. Calculate deltas
     e. Send Delta::SetMatches to UI
  4. Check if 60s elapsed for minute updates
  5. Apply probability jitter to random match
  6. Process commands from UI
```

### Fetching Match Details

```
User presses 'i' or Enter/d:
  1. UI sends ProviderCommand::FetchMatchDetails
  2. Background thread receives command
  3. Calls fetch_match_details_from_fotmob()
  4. Parses lineups, events, stats
  5. Sends Delta::SetMatchDetails to UI
  6. UI stores in state.match_detail HashMap
  7. UI re-renders Terminal view with new data
```

### State Updates

```
Delta Types:
  - SetMatches: Replace all matches
  - UpsertMatch: Update single match
  - SetMatchDetails: Store match details
  - AddEvent: Add event to match
  - SetUpcoming: Replace upcoming matches
  - Log: Add log message

UI Thread:
  1. Polls channel for deltas
  2. Applies delta to AppState
  3. Re-renders affected UI sections
```

## Configuration

All configuration is done via environment variables (`.env` or `.env.local`):

- **League Filtering**: `APP_LEAGUE_PREMIER_IDS`, `APP_LEAGUE_WORLDCUP_IDS`
- **Polling Intervals**: `PULSE_POLL_SECS`, `UPCOMING_POLL_SECS`
- **Date Overrides**: `PULSE_DATE`, `UPCOMING_DATE` (YYYYMMDD format)
- **Upcoming Window**: `UPCOMING_WINDOW_DAYS` (1-14 days)
- **Data Source**: `UPCOMING_SOURCE` (fotmob/auto)

## Error Handling

- **API Failures**: Logged to console, fallback to seeded data when available
- **Network Timeouts**: 10-second timeout on HTTP requests
- **Invalid JSON**: Returns empty match details
- **Missing Data**: Gracefully handles missing fields with defaults
- **Throttling**: Upcoming fetches respect minimum interval

## Future Enhancements

Based on code comments and placeholders:

- **Pitch Visualization**: Visual representation of match on field
- **Group Standings**: Display league/group tables
- **Tracking-based Models**: Enhanced probability calculation with player tracking data
- **More Event Types**: Additional event parsing from API
- **Match History**: View past matches and trends

## Technical Details

**Technologies**:
- Ratatui: Terminal UI framework
- Crossterm: Terminal input/output
- Reqwest: HTTP client (blocking)
- Serde: JSON parsing
- Chrono: Date/time handling

**Threading Model**:
- Main thread: UI rendering and input handling
- Background thread: API fetching and data processing
- Communication: Unbounded mpsc channels

**Performance**:
- UI refresh: ~250ms tick rate
- Match updates: Configurable polling intervals
- Efficient delta-based updates (only changed data sent)
