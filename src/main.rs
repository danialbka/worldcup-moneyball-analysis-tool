use std::collections::HashMap;
use std::io;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use chrono::{Duration as ChronoDuration, NaiveDateTime};
use crossterm::event::{
    self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyEvent, KeyEventKind,
};
use crossterm::terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen};
use crossterm::execute;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::prelude::*;
use ratatui::style::{Color, Modifier, Style};
use ratatui::widgets::{Bar, BarChart, BarGroup, Block, Borders, Clear, Paragraph};

mod fake_feed;
mod state;
mod upcoming_fetch;

use crate::state::{apply_delta, league_label, AppState, PulseView, Screen};

struct App {
    state: AppState,
    should_quit: bool,
    cmd_tx: Option<mpsc::Sender<state::ProviderCommand>>,
    upcoming_refresh: Duration,
    last_upcoming_refresh: Instant,
    detail_refresh: Duration,
    last_detail_refresh: HashMap<String, Instant>,
}

impl App {
    fn new(cmd_tx: Option<mpsc::Sender<state::ProviderCommand>>) -> Self {
        let upcoming_refresh = std::env::var("UPCOMING_POLL_SECS")
            .ok()
            .and_then(|val| val.parse::<u64>().ok())
            .unwrap_or(60)
            .max(10);
        let detail_refresh = std::env::var("DETAILS_POLL_SECS")
            .ok()
            .and_then(|val| val.parse::<u64>().ok())
            .unwrap_or(60)
            .max(30);
        Self {
            state: AppState::new(),
            should_quit: false,
            cmd_tx,
            upcoming_refresh: Duration::from_secs(upcoming_refresh),
            last_upcoming_refresh: Instant::now(),
            detail_refresh: Duration::from_secs(detail_refresh),
            last_detail_refresh: HashMap::new(),
        }
    }

    fn on_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Char('q') => self.should_quit = true,
            KeyCode::Char('1') => self.state.screen = Screen::Pulse,
            KeyCode::Char('d') | KeyCode::Enter => {
                let match_id = self.state.selected_match_id();
                if self.state.pulse_view == PulseView::Live {
                    self.state.screen = Screen::Terminal { match_id };
                    self.request_match_details(true);
                }
            }
            KeyCode::Char('b') | KeyCode::Esc => self.state.screen = Screen::Pulse,
            KeyCode::Char('j') | KeyCode::Down => self.state.select_next(),
            KeyCode::Char('k') | KeyCode::Up => self.state.select_prev(),
            KeyCode::Char('s') => self.state.cycle_sort(),
            KeyCode::Char('l') | KeyCode::Char('L') => {
                self.state.cycle_league_mode();
                if self.state.pulse_view == PulseView::Upcoming {
                    self.request_upcoming(true);
                }
            }
            KeyCode::Char('u') | KeyCode::Char('U') => {
                let to_upcoming = self.state.pulse_view == PulseView::Live;
                self.state.toggle_pulse_view();
                if to_upcoming {
                    self.request_upcoming(true);
                }
            }
            KeyCode::Char('i') | KeyCode::Char('I') => self.request_match_details(true),
            KeyCode::Char('?') => self.state.help_overlay = !self.state.help_overlay,
            _ => {}
        }
    }

    fn request_match_details(&mut self, announce: bool) {
        let Some(match_id) = self.state.selected_match_id() else {
            if announce {
                self.state.push_log("[INFO] No match selected for details");
            }
            return;
        };
        self.request_match_details_for(&match_id, announce);
    }

    fn request_match_details_for(&mut self, match_id: &str, announce: bool) {
        let Some(tx) = &self.cmd_tx else {
            if announce {
                self.state.push_log("[INFO] Match details fetch unavailable");
            }
            return;
        };
        if tx
            .send(state::ProviderCommand::FetchMatchDetails {
                fixture_id: match_id.to_string(),
            })
            .is_err()
        {
            if announce {
                self.state.push_log("[WARN] Match details request failed");
            }
        } else {
            if announce {
                self.state.push_log("[INFO] Match details request sent");
            }
            self.last_detail_refresh
                .insert(match_id.to_string(), Instant::now());
        }
    }

    fn request_upcoming(&mut self, announce: bool) {
        let Some(tx) = &self.cmd_tx else {
            if announce {
                self.state.push_log("[INFO] Upcoming fetch unavailable");
            }
            return;
        };
        if tx.send(state::ProviderCommand::FetchUpcoming).is_err() {
            if announce {
                self.state.push_log("[WARN] Upcoming request failed");
            }
        } else {
            if announce {
                self.state.push_log("[INFO] Upcoming request sent");
            }
            self.last_upcoming_refresh = Instant::now();
        }
    }

    fn maybe_refresh_upcoming(&mut self) {
        if !matches!(self.state.screen, Screen::Pulse) {
            return;
        }
        if self.state.pulse_view != PulseView::Upcoming {
            return;
        }
        if self.last_upcoming_refresh.elapsed() >= self.upcoming_refresh {
            self.request_upcoming(false);
        }
    }

    fn maybe_refresh_match_details(&mut self) {
        let live_matches: Vec<String> = self
            .state
            .filtered_matches()
            .into_iter()
            .filter(|m| m.is_live)
            .map(|m| m.id.clone())
            .collect();

        for match_id in live_matches {
            let last = self.last_detail_refresh.get(&match_id);
            let should_fetch = last
                .map(|t| t.elapsed() >= self.detail_refresh)
                .unwrap_or(true);
            if should_fetch {
                self.request_match_details_for(&match_id, false);
            }
        }
    }
}

fn main() -> io::Result<()> {
    let _ = dotenvy::from_filename(".env.local");
    let _ = dotenvy::from_filename(".env");

    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = ratatui::backend::CrosstermBackend::new(stdout);
    let mut terminal = ratatui::Terminal::new(backend)?;

    let (tx, rx) = mpsc::channel();
    let (cmd_tx, cmd_rx) = mpsc::channel();
    fake_feed::spawn_fake_provider(tx, cmd_rx);

    let mut app = App::new(Some(cmd_tx));
    let res = run_app(&mut terminal, &mut app, rx);

    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;

    if let Err(err) = res {
        eprintln!("error: {err}");
    }
    Ok(())
}

fn run_app<B: Backend>(
    terminal: &mut Terminal<B>,
    app: &mut App,
    rx: mpsc::Receiver<state::Delta>,
) -> io::Result<()> {
    let tick_rate = Duration::from_millis(250);
    let mut last_tick = Instant::now();

    loop {
        while let Ok(delta) = rx.try_recv() {
            apply_delta(&mut app.state, delta);
        }

        app.maybe_refresh_upcoming();
        app.maybe_refresh_match_details();

        terminal.draw(|f| ui(f, app))?;

        let timeout = tick_rate
            .checked_sub(last_tick.elapsed())
            .unwrap_or(Duration::ZERO);
        if event::poll(timeout)? {
            if let Event::Key(key) = event::read()? {
                if key.kind == KeyEventKind::Press {
                    app.on_key(key);
                }
            }
        }

        if last_tick.elapsed() >= tick_rate {
            last_tick = Instant::now();
        }

        if app.should_quit {
            return Ok(());
        }
    }
}

fn ui(frame: &mut Frame, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(1),
            Constraint::Length(1),
        ])
        .split(frame.size());

    let header = Paragraph::new(header_text(&app.state))
        .block(Block::default().borders(Borders::BOTTOM));
    frame.render_widget(header, chunks[0]);

    match app.state.screen {
        Screen::Pulse => render_pulse(frame, chunks[1], &app.state),
        Screen::Terminal { .. } => render_terminal(frame, chunks[1], &app.state),
    }

    let footer = Paragraph::new(footer_text(&app.state))
        .block(Block::default().borders(Borders::TOP));
    frame.render_widget(footer, chunks[2]);

    if app.state.help_overlay {
        render_help_overlay(frame, frame.size());
    }
}

fn header_text(state: &AppState) -> String {
    let title = match state.screen {
        Screen::Pulse => format!(
            "WC26 PULSE | {} | {} | Sort: {}",
            league_label(state.league_mode),
            pulse_view_label(state.pulse_view),
            sort_label(state.sort)
        ),
        Screen::Terminal { .. } => "WC26 TERMINAL".to_string(),
    };
    let line1 = format!("  .-.  {}", title);
    let line2 = " /___\\".to_string();
    let line3 = "  |_|".to_string();
    format!("{line1}\n{line2}\n{line3}")
}

fn footer_text(state: &AppState) -> String {
    match state.screen {
        Screen::Pulse => match state.pulse_view {
            PulseView::Live => {
                "1 Pulse | Enter/d Terminal | j/k/↑/↓ Move | s Sort | l League | u Upcoming | i Details | ? Help | q Quit".to_string()
            }
            PulseView::Upcoming => {
                "1 Pulse | u Live | j/k/↑/↓ Scroll | l League | ? Help | q Quit".to_string()
            }
        },
        Screen::Terminal { .. } => {
            "1 Pulse | b/Esc Back | i Details | l League | ? Help | q Quit".to_string()
        }
    }
}

fn render_pulse(frame: &mut Frame, area: Rect, state: &AppState) {
    match state.pulse_view {
        PulseView::Live => render_pulse_live(frame, area, state),
        PulseView::Upcoming => render_pulse_upcoming(frame, area, state),
    }
}

fn render_pulse_live(frame: &mut Frame, area: Rect, state: &AppState) {
    let sections = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(1), Constraint::Min(1)])
        .split(area);

    let widths = pulse_columns();
    render_pulse_header(frame, sections[0], &widths);

    let list_area = sections[1];
    let filtered = state.filtered_matches();
    if filtered.is_empty() {
        let empty = Paragraph::new("No matches for this league")
            .style(Style::default().fg(Color::DarkGray));
        frame.render_widget(empty, list_area);
        return;
    }

    const ROW_HEIGHT: u16 = 3;
    if list_area.height < ROW_HEIGHT {
        let empty = Paragraph::new("Pulse list needs more height")
            .style(Style::default().fg(Color::DarkGray));
        frame.render_widget(empty, list_area);
        return;
    }

    let visible = (list_area.height / ROW_HEIGHT) as usize;
    let (start, end) = visible_range(state.selected, filtered.len(), visible);

    for (i, idx) in (start..end).enumerate() {
        let row_area = Rect {
            x: list_area.x,
            y: list_area.y + (i as u16) * ROW_HEIGHT,
            width: list_area.width,
            height: ROW_HEIGHT,
        };

        let selected = idx == state.selected;
        let row_style = if selected {
            Style::default().fg(Color::White).bg(Color::DarkGray)
        } else {
            Style::default()
        };

        if selected {
            frame.render_widget(Block::default().style(row_style), row_area);
        }

        let cols = Layout::default()
            .direction(Direction::Horizontal)
            .constraints(widths)
            .split(row_area);

        let m = filtered[idx];
        let time = if m.is_live {
            format!("{}'", m.minute)
        } else {
            "FT".to_string()
        };
        let match_name = format!("{}-{}", m.home, m.away);
        let score = format!("{}-{}", m.score_home, m.score_away);
        let hda = format!("H{:.0} D{:.0} A{:.0}", m.win.p_home, m.win.p_draw, m.win.p_away);
        let delta = format!("{:+.1}", m.win.delta_home);
        let quality = quality_label(m.win.quality).to_string();
        let conf = format!("{}%", m.win.confidence);

        render_cell_text(frame, cols[0], &time, row_style);
        render_cell_text(frame, cols[1], &match_name, row_style);
        render_cell_text(frame, cols[2], &score, row_style);

        let bar = win_bar_chart(&m.win, selected);
        frame.render_widget(bar, cols[3]);

        render_cell_text(frame, cols[4], &hda, row_style);
        render_cell_text(frame, cols[5], &delta, row_style);
        render_cell_text(frame, cols[6], &quality, row_style);
        render_cell_text(frame, cols[7], &conf, row_style);
    }
}

fn render_pulse_upcoming(frame: &mut Frame, area: Rect, state: &AppState) {
    let sections = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(1), Constraint::Min(1)])
        .split(area);

    let widths = upcoming_columns();
    render_upcoming_header(frame, sections[0], &widths);

    let list_area = sections[1];
    let upcoming = state.filtered_upcoming();
    if upcoming.is_empty() {
        let empty = Paragraph::new("No upcoming matches for this league")
            .style(Style::default().fg(Color::DarkGray));
        frame.render_widget(empty, list_area);
        return;
    }

    if list_area.height == 0 {
        return;
    }

    let visible = list_area.height as usize;
    let total = upcoming.len();
    let max_start = total.saturating_sub(visible);
    let start = (state.upcoming_scroll as usize).min(max_start);
    let end = (start + visible).min(total);

    for (i, idx) in (start..end).enumerate() {
        let row_area = Rect {
            x: list_area.x,
            y: list_area.y + i as u16,
            width: list_area.width,
            height: 1,
        };

        let cols = Layout::default()
            .direction(Direction::Horizontal)
            .constraints(widths)
            .split(row_area);

        let m = upcoming[idx];
        let kickoff = format_kickoff(&m.kickoff);
        let match_name = format!("{} vs {}", m.home, m.away);
        let league = if m.league_name.is_empty() {
            "-".to_string()
        } else {
            m.league_name.clone()
        };
        let round = if m.round.is_empty() {
            "-".to_string()
        } else {
            m.round.clone()
        };

        let sep_style = Style::default().fg(Color::DarkGray);
        render_cell_text(frame, cols[0], &kickoff, Style::default());
        render_vseparator(frame, cols[1], sep_style);
        render_cell_text(frame, cols[2], &match_name, Style::default());
        render_vseparator(frame, cols[3], sep_style);
        render_cell_text(frame, cols[4], &league, Style::default());
        render_vseparator(frame, cols[5], sep_style);
        render_cell_text(frame, cols[6], &round, Style::default());
    }
}

fn pulse_columns() -> [Constraint; 8] {
    [
        Constraint::Length(6),
        Constraint::Length(11),
        Constraint::Length(7),
        Constraint::Min(20),
        Constraint::Length(13),
        Constraint::Length(7),
        Constraint::Length(7),
        Constraint::Length(6),
    ]
}

fn upcoming_columns() -> [Constraint; 7] {
    [
        Constraint::Length(16),
        Constraint::Length(1),
        Constraint::Min(20),
        Constraint::Length(1),
        Constraint::Length(16),
        Constraint::Length(1),
        Constraint::Min(10),
    ]
}

fn render_pulse_header(frame: &mut Frame, area: Rect, widths: &[Constraint]) {
    let cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints(widths)
        .split(area);
    let style = Style::default().add_modifier(Modifier::BOLD);

    render_cell_text(frame, cols[0], "Time", style);
    render_cell_text(frame, cols[1], "Match", style);
    render_cell_text(frame, cols[2], "Score", style);
    render_cell_text(frame, cols[3], "Win% Bar", style);
    render_cell_text(frame, cols[4], "H/D/A", style);
    render_cell_text(frame, cols[5], "Delta", style);
    render_cell_text(frame, cols[6], "Q", style);
    render_cell_text(frame, cols[7], "Conf", style);
}

fn render_upcoming_header(frame: &mut Frame, area: Rect, widths: &[Constraint]) {
    let cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints(widths)
        .split(area);
    let style = Style::default().add_modifier(Modifier::BOLD);
    let sep_style = Style::default().fg(Color::DarkGray);

    render_cell_text(frame, cols[0], "Kickoff (SGT)", style);
    render_vseparator(frame, cols[1], sep_style);
    render_cell_text(frame, cols[2], "Match", style);
    render_vseparator(frame, cols[3], sep_style);
    render_cell_text(frame, cols[4], "League", style);
    render_vseparator(frame, cols[5], sep_style);
    render_cell_text(frame, cols[6], "Round", style);
}
fn render_cell_text(frame: &mut Frame, area: Rect, text: &str, style: Style) {
    let text_area = Rect {
        x: area.x,
        y: area.y + (area.height / 2),
        width: area.width,
        height: 1,
    };
    let paragraph = Paragraph::new(text).style(style);
    frame.render_widget(paragraph, text_area);
}

fn render_vseparator(frame: &mut Frame, area: Rect, style: Style) {
    if area.width == 0 || area.height == 0 {
        return;
    }
    let mut text = String::new();
    for i in 0..area.height {
        if i > 0 {
            text.push('\n');
        }
        text.push('│');
    }
    let paragraph = Paragraph::new(text).style(style);
    frame.render_widget(paragraph, area);
}

fn win_bar_chart(win: &state::WinProbRow, selected: bool) -> BarChart<'static> {
    let base_bg = if selected { Some(Color::DarkGray) } else { None };

    let mut home_style = Style::default().fg(Color::Green);
    let mut draw_style = Style::default().fg(Color::Yellow);
    let mut away_style = Style::default().fg(Color::Red);
    if let Some(bg) = base_bg {
        home_style = home_style.bg(bg);
        draw_style = draw_style.bg(bg);
        away_style = away_style.bg(bg);
    }

    let home = Bar::default()
        .value(win.p_home.round() as u64)
        .text_value(String::new())
        .style(home_style);
    let draw = Bar::default()
        .value(win.p_draw.round() as u64)
        .text_value(String::new())
        .style(draw_style);
    let away = Bar::default()
        .value(win.p_away.round() as u64)
        .text_value(String::new())
        .style(away_style);

    BarChart::default()
        .data(BarGroup::default().bars(&[home, draw, away]))
        .direction(Direction::Horizontal)
        .bar_width(1)
        .bar_gap(0)
        .group_gap(0)
        .max(100)
}

fn visible_range(selected: usize, total: usize, visible: usize) -> (usize, usize) {
    if total == 0 {
        return (0, 0);
    }
    if total <= visible {
        return (0, total);
    }

    let mut start = selected.saturating_sub(visible / 2);
    if start + visible > total {
        start = total - visible;
    }
    (start, start + visible)
}

fn render_terminal(frame: &mut Frame, area: Rect, state: &AppState) {
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(1), Constraint::Length(3)])
        .split(area);

    let columns = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Length(22),
            Constraint::Min(30),
            Constraint::Length(28),
        ])
        .split(rows[0]);

    let left_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(9), Constraint::Min(1)])
        .split(columns[0]);

    let middle_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(10), Constraint::Min(1)])
        .split(columns[1]);

    let right_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(6),
            Constraint::Length(12),
            Constraint::Min(1),
        ])
        .split(columns[2]);

    let match_list = match_list_text(state);
    let left_match = Paragraph::new(match_list)
        .block(Block::default().title("Match List").borders(Borders::ALL));
    frame.render_widget(left_match, left_chunks[0]);

    let standings = Paragraph::new("Standings placeholder")
        .block(Block::default().title("Group Mini").borders(Borders::ALL));
    frame.render_widget(standings, left_chunks[1]);

    let pitch = Paragraph::new("Pitch canvas placeholder")
        .block(Block::default().title("Pitch").borders(Borders::ALL));
    frame.render_widget(pitch, middle_chunks[0]);

    let tape = Paragraph::new(event_tape_text(state))
        .block(Block::default().title("Event Tape").borders(Borders::ALL));
    frame.render_widget(tape, middle_chunks[1]);

    let stats_text = stats_text(state);
    let stats = Paragraph::new(stats_text)
        .block(Block::default().title("Stats").borders(Borders::ALL));
    frame.render_widget(stats, right_chunks[0]);

    render_lineups(frame, right_chunks[1], state);

    let preds_text = prediction_text(state);
    let preds = Paragraph::new(preds_text)
        .block(Block::default().title("Prediction").borders(Borders::ALL));
    frame.render_widget(preds, right_chunks[2]);

    let console = Paragraph::new(console_text(state))
        .block(Block::default().title("Console").borders(Borders::ALL));
    frame.render_widget(console, rows[1]);
}

fn match_list_text(state: &AppState) -> String {
    let filtered = state.filtered_matches();
    if filtered.is_empty() {
        return "No matches yet".to_string();
    }

    let mut lines = Vec::new();
    for (idx, m) in filtered.iter().enumerate() {
        let prefix = if idx == state.selected { "> " } else { "  " };
        let line = format!(
            "{prefix}{}-{} {}-{}",
            m.home, m.away, m.score_home, m.score_away
        );
        lines.push(line);
    }
    lines.join("\n")
}

fn stats_text(state: &AppState) -> String {
    match state.selected_match() {
        Some(m) => {
            let time = if m.is_live {
                format!("{}'", m.minute)
            } else {
                "FT".to_string()
            };
            let mut lines = vec![
                format!("Time: {time}"),
                format!("Score: {}-{}", m.score_home, m.score_away),
                format!("Live: {}", if m.is_live { "yes" } else { "no" }),
            ];
            if let Some(detail) = state.match_detail.get(&m.id) {
                for row in detail.stats.iter().take(6) {
                    lines.push(format!("{}: {}-{}", row.name, row.home, row.away));
                }
            }
            lines.join("\n")
        }
        None => "No match selected".to_string(),
    }
}

fn render_lineups(frame: &mut Frame, area: Rect, state: &AppState) {
    let block = Block::default().title("Lineups").borders(Borders::ALL);
    let inner = block.inner(area);
    frame.render_widget(block, area);

    if inner.height == 0 || inner.width == 0 {
        return;
    }

    let Some(match_id) = state.selected_match_id() else {
        let empty = Paragraph::new("No match selected");
        frame.render_widget(empty, inner);
        return;
    };

    let Some(detail) = state.match_detail.get(&match_id) else {
        let empty = Paragraph::new("No lineups yet");
        frame.render_widget(empty, inner);
        return;
    };

    let Some(lineups) = &detail.lineups else {
        let empty = Paragraph::new("No lineups yet");
        frame.render_widget(empty, inner);
        return;
    };

    let mut sides = lineups.sides.clone();
    sides.sort_by(|a, b| a.team_abbr.cmp(&b.team_abbr));
    let left = sides.get(0);
    let right = sides.get(1);

    let cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(inner);

    render_lineup_side(frame, cols[0], left);
    render_lineup_side(frame, cols[1], right);
}

fn render_lineup_side(frame: &mut Frame, area: Rect, side: Option<&state::LineupSide>) {
    let text = if let Some(side) = side {
        lineup_text(side)
    } else {
        "No lineup".to_string()
    };
    let paragraph = Paragraph::new(text);
    frame.render_widget(paragraph, area);
}

fn lineup_text(side: &state::LineupSide) -> String {
    let mut lines = Vec::new();
    let heading = if side.formation.is_empty() {
        format!("{} {}", side.team_abbr, side.team)
    } else {
        format!("{} {} ({})", side.team_abbr, side.team, side.formation)
    };
    lines.push(heading);
    lines.push("Starters:".to_string());
    for player in &side.starting {
        lines.push(format_player(player));
    }
    lines.push("Subs:".to_string());
    for player in &side.subs {
        lines.push(format_player(player));
    }
    lines.join("\n")
}

fn format_player(player: &state::PlayerSlot) -> String {
    let num = player
        .number
        .map(|n| n.to_string())
        .unwrap_or_else(|| "--".to_string());
    let pos = player.pos.clone().unwrap_or_else(|| "".to_string());
    if pos.is_empty() {
        format!("{num} {}", player.name)
    } else {
        format!("{num} {} {pos}", player.name)
    }
}

fn event_tape_text(state: &AppState) -> String {
    let Some(match_id) = state.selected_match_id() else {
        return "No match selected".to_string();
    };
    let Some(detail) = state.match_detail.get(&match_id) else {
        return "No events yet".to_string();
    };
    if detail.events.is_empty() {
        return "No events yet".to_string();
    }

    let start = detail.events.len().saturating_sub(6);
    detail.events[start..]
        .iter()
        .map(|event| {
            format!(
                "{}' {} {} {}",
                event.minute,
                event_kind_label(event.kind),
                event.team,
                event.description
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn prediction_text(state: &AppState) -> String {
    match state.selected_match() {
        Some(m) => format!(
            "W: {:>4.0}%\nD: {:>4.0}%\nA: {:>4.0}%\nDelta: {:+.1}\nModel: {}",
            m.win.p_home,
            m.win.p_draw,
            m.win.p_away,
            m.win.delta_home,
            quality_label(m.win.quality)
        ),
        None => "No prediction data".to_string(),
    }
}

fn console_text(state: &AppState) -> String {
    if state.logs.is_empty() {
        return "No alerts yet".to_string();
    }
    state
        .logs
        .iter()
        .rev()
        .take(3)
        .cloned()
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect::<Vec<_>>()
        .join("\n")
}

fn quality_label(quality: state::ModelQuality) -> &'static str {
    match quality {
        state::ModelQuality::Basic => "BASIC",
        state::ModelQuality::Event => "EVENT",
        state::ModelQuality::Track => "TRACK",
    }
}

fn event_kind_label(kind: state::EventKind) -> &'static str {
    match kind {
        state::EventKind::Shot => "SHOT",
        state::EventKind::Card => "CARD",
        state::EventKind::Sub => "SUB",
        state::EventKind::Goal => "GOAL",
    }
}

fn sort_label(sort: state::SortMode) -> &'static str {
    match sort {
        state::SortMode::Hot => "HOT",
        state::SortMode::Time => "TIME",
        state::SortMode::Close => "CLOSE",
        state::SortMode::Upset => "UPSET",
    }
}

fn pulse_view_label(view: PulseView) -> &'static str {
    match view {
        PulseView::Live => "LIVE",
        PulseView::Upcoming => "UPCOMING",
    }
}

fn format_kickoff(raw: &str) -> String {
    if raw.is_empty() {
        return "TBD".to_string();
    }
    let cleaned = raw.trim();
    let parsed = parse_kickoff(cleaned);
    if let Some(dt) = parsed {
        let sgt = dt + ChronoDuration::hours(8);
        return format!("{} SGT", sgt.format("%Y-%m-%d %H:%M"));
    }
    if cleaned.len() >= 16 {
        return cleaned[..16].replace('T', " ");
    }
    cleaned.replace('T', " ")
}

fn parse_kickoff(raw: &str) -> Option<NaiveDateTime> {
    const FORMATS: [&str; 6] = [
        "%Y-%m-%dT%H:%M",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%d.%m.%Y T%H:%M",
        "%d.%m.%Y %H:%M",
    ];

    for fmt in FORMATS {
        if let Ok(dt) = NaiveDateTime::parse_from_str(raw, fmt) {
            return Some(dt);
        }
    }
    None
}

fn render_help_overlay(frame: &mut Frame, area: Rect) {
    let popup_area = centered_rect(60, 60, area);
    frame.render_widget(Clear, popup_area);

    let text = [
        "WC26 Terminal - Help",
        "",
        "Global:",
        "  1            Pulse",
        "  Enter / d    Terminal",
        "  b / Esc      Back",
        "  l            League toggle",
        "  u            Upcoming view",
        "  i            Fetch match details",
        "  ?            Toggle help",
        "  q            Quit",
        "",
        "Pulse:",
        "  j/k or ↑/↓   Move/scroll",
        "  s            Cycle sort mode",
    ]
    .join("\n");

    let help = Paragraph::new(text)
        .block(Block::default().title("Help").borders(Borders::ALL))
        .style(Style::default());
    frame.render_widget(help, popup_area);
}

fn centered_rect(percent_x: u16, percent_y: u16, area: Rect) -> Rect {
    let vertical = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(area);

    let horizontal = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(vertical[1]);

    horizontal[1]
}
