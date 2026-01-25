# WC26 Terminal (Ratatui TUI)

Bloomberg-style football terminal in the terminal. Pulse shows live match probabilities; Terminal view drills into a single match.

## Installation

### Prerequisites

- **Rust toolchain** (stable version recommended)
  - Install Rust from [rustup.rs](https://rustup.rs/) if you don't have it installed
  - Verify installation: `rustc --version`
- **Network access** to FotMob API endpoints
- **Terminal** with support for ANSI colors and keyboard input

### Build and Install

1. **Clone the repository** (if you haven't already):
   ```bash
   git clone <repository-url>
   cd worldcup
   ```

2. **Build the project**:
   ```bash
   cargo build --release
   ```

3. **Run the application**:
   ```bash
   cargo run --release
   ```
   
   Or use the built binary directly:
   ```bash
   ./target/release/wc26_terminal
   ```

### Configuration Setup

Before running the application, set up your environment configuration:

1. **Copy the example environment file**:
   ```bash
   cp .env.example .env.local
   ```

2. **Edit `.env.local`** with your preferred settings (see Configuration section below)

   The app reads `.env.local` first, then falls back to `.env` if `.env.local` doesn't exist.

## How to Use

### Starting the Application

1. Ensure you have configured your `.env.local` file (see Installation above)
2. Run the application:
   ```bash
   cargo run --release
   ```

### Basic Navigation

The application provides a terminal-based interface with multiple views:

- **Pulse View**: Shows live match probabilities and current match status
- **Terminal View**: Detailed view of a selected match with lineups, events, and stats
- **Upcoming View**: List of upcoming matches
- **Analysis View**: League analysis and rankings

### Keyboard Controls

**Global Controls:**
- `1`: Switch to Pulse view
- `Enter` / `d`: Open Terminal view (for selected live match, triggers match details)
- `b` / `Esc`: Go back to previous view
- `l`: Toggle league mode (Premier League / World Cup)
- `u`: Toggle Upcoming view and fetch matchday list
- `i`: Fetch match details (lineups/events/stats)
- `e`: Export analysis XLSX (from Analysis screen, current league)
- `?`: Show help overlay
- `q`: Quit application

**Pulse View Controls:**
- `j/k` or `↑/↓`: Move selection (Live) / scroll list (Upcoming)
- `s`: Cycle sort mode

### Workflow Example

1. Launch the application with `cargo run --release`
2. Press `1` to view the Pulse screen with live matches
3. Use `j/k` or arrow keys to navigate through matches
4. Press `Enter` or `d` to view detailed match information
5. Press `l` to switch between Premier League, La Liga, and World Cup leagues
6. Press `u` to view upcoming matches
7. Press `?` anytime to see available keyboard shortcuts
8. Press `q` to quit

## Configuration

Copy `.env.example` to `.env.local` and edit as needed.

### Key Configuration Variables

- `APP_LEAGUE_PREMIER_IDS` / `APP_LEAGUE_LALIGA_IDS` / `APP_LEAGUE_WORLDCUP_IDS`: League filters (FotMob IDs: 47 = PL, 87 = La Liga, 77 = World Cup).
- `PULSE_POLL_SECS`: Live match refresh interval (seconds).
- `PULSE_DATE`: Optional matchday override (YYYYMMDD). Empty uses FotMob default (today).
- `UPCOMING_SOURCE`: `fotmob` or `auto` (same behavior right now).
- `UPCOMING_POLL_SECS`: Minimum seconds between manual upcoming fetches.
- `UPCOMING_DATE`: Optional matchday override (YYYYMMDD) for upcoming list.
- `UPCOMING_WINDOW_DAYS`: Number of days to fetch (1-14). Use 7 for a full weekend slate.
- `DETAILS_POLL_SECS`: Auto-refresh interval for match details (lineups/events/stats) when live.
- `AUTO_WARM_CACHE`: `off`, `missing`, or `full` to prefetch squads + player details in the background.

### Configuration Notes

- FotMob expects `date=YYYYMMDD`. ISO `YYYY-MM-DD` returns `null`.
- Win% is locally computed (simple heuristic + jitter).

## Data Sources

Live + upcoming:

- `https://www.fotmob.com/api/data/matches`
- `https://www.fotmob.com/api/data/matches?date=YYYYMMDD`

Detailed (manual wiring if needed later):

- `https://www.fotmob.com/api/data/matchDetails?matchId=...`

## Troubleshooting

- **If upcoming is empty**: Set `UPCOMING_DATE` to a known matchday (YYYYMMDD) and press `u`.
- **If Pulse shows nothing**: Confirm `APP_LEAGUE_*` IDs match the FotMob league IDs and that `PULSE_DATE` is set to a matchday with fixtures.
- **Build errors**: Ensure you have the latest stable Rust toolchain installed (`rustup update stable`)
- **Network issues**: Verify you have internet connectivity and can access FotMob API endpoints
