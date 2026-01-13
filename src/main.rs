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
mod analysis_fetch;
mod state;
mod upcoming_fetch;

use crate::state::{
    apply_delta, confed_label, league_label, AppState, PlayerDetail, PulseView, Screen,
    PLAYER_DETAIL_SECTIONS,
};

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
            KeyCode::Char('2') | KeyCode::Char('a') | KeyCode::Char('A') => {
                self.state.screen = Screen::Analysis;
                if self.state.analysis.is_empty() && !self.state.analysis_loading {
                    self.request_analysis(true);
                }
            }
            KeyCode::Char('d') | KeyCode::Enter => {
                match self.state.screen {
                    Screen::Pulse => {
                        let match_id = self.state.selected_match_id();
                        if self.state.pulse_view == PulseView::Live {
                            self.state.screen = Screen::Terminal { match_id };
                            self.request_match_details(true);
                        }
                    }
                    Screen::Analysis => {
                        let team = self.state.selected_analysis().cloned();
                        if let Some(team) = team {
                            self.state.screen = Screen::Squad;
                            let needs_fetch = self.state.squad_team_id != Some(team.id)
                                || self.state.squad.is_empty();
                            if needs_fetch && !self.state.squad_loading {
                                self.request_squad(team.id, team.name.clone(), true);
                            }
                        }
                    }
                    Screen::Squad => {
                        let player = self.state.selected_squad_player().cloned();
                        if let Some(player) = player {
                            self.state.screen = Screen::PlayerDetail;
                            self.state.player_detail_scroll = 0;
                            self.state.player_detail_section = 0;
                            self.state.player_detail_section_scrolls = [0; PLAYER_DETAIL_SECTIONS];
                            let cached_detail = self.state.player_detail.as_ref();
                            let cached = cached_detail
                                .map(|detail| detail.id == player.id)
                                .unwrap_or(false);
                            let needs_stats_refresh = cached_detail
                                .map(|detail| !player_detail_has_stats(detail))
                                .unwrap_or(false);
                            if (!cached || needs_stats_refresh) && !self.state.player_loading {
                                self.request_player_detail(player.id, player.name.clone(), true);
                            }
                        }
                    }
                    _ => {}
                }
            }
            KeyCode::Char('b') | KeyCode::Esc => {
                self.state.screen = match self.state.screen {
                    Screen::Terminal { .. } => Screen::Pulse,
                    Screen::Analysis => Screen::Pulse,
                    Screen::Squad => Screen::Analysis,
                    Screen::PlayerDetail => Screen::Squad,
                    Screen::Pulse => Screen::Pulse,
                };
            }
            KeyCode::Char('j') | KeyCode::Down => {
                if matches!(self.state.screen, Screen::Analysis) {
                    self.state.select_analysis_next();
                } else if matches!(self.state.screen, Screen::Squad) {
                    self.state.select_squad_next();
                } else if matches!(self.state.screen, Screen::PlayerDetail) {
                    let max_scroll = self
                        .state
                        .player_detail
                        .as_ref()
                        .map(|detail| player_detail_section_max_scroll(detail, self.state.player_detail_section))
                        .unwrap_or(0);
                    self.state.scroll_player_detail_down(max_scroll);
                } else {
                    self.state.select_next();
                }
            }
            KeyCode::Char('k') | KeyCode::Up => {
                if matches!(self.state.screen, Screen::Analysis) {
                    self.state.select_analysis_prev();
                } else if matches!(self.state.screen, Screen::Squad) {
                    self.state.select_squad_prev();
                } else if matches!(self.state.screen, Screen::PlayerDetail) {
                    self.state.scroll_player_detail_up();
                } else {
                    self.state.select_prev();
                }
            }
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
            KeyCode::Tab => {
                if matches!(self.state.screen, Screen::PlayerDetail) {
                    self.state.cycle_player_detail_section_next();
                }
            }
            KeyCode::BackTab => {
                if matches!(self.state.screen, Screen::PlayerDetail) {
                    self.state.cycle_player_detail_section_prev();
                }
            }
            KeyCode::Char('r') | KeyCode::Char('R') => {
                if matches!(self.state.screen, Screen::Analysis) {
                    self.request_analysis(true);
                } else if matches!(self.state.screen, Screen::Squad) {
                    if let Some(team_id) = self.state.squad_team_id {
                        let team_name = self
                            .state
                            .squad_team
                            .clone()
                            .unwrap_or_else(|| "Team".to_string());
                        self.request_squad(team_id, team_name, true);
                    }
                } else if matches!(self.state.screen, Screen::PlayerDetail) {
                    if let (Some(player_id), Some(player_name)) = (
                        self.state.player_last_id,
                        self.state.player_last_name.clone(),
                    ) {
                        self.request_player_detail(player_id, player_name, true);
                    }
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

    fn request_analysis(&mut self, announce: bool) {
        let Some(tx) = &self.cmd_tx else {
            if announce {
                self.state.push_log("[INFO] Analysis fetch unavailable");
            }
            return;
        };
        if tx.send(state::ProviderCommand::FetchAnalysis).is_err() {
            if announce {
                self.state.push_log("[WARN] Analysis request failed");
            }
        } else {
            if announce {
                self.state.push_log("[INFO] Analysis request sent");
            }
            self.state.analysis_loading = true;
        }
    }

    fn request_squad(&mut self, team_id: u32, team_name: String, announce: bool) {
        let Some(tx) = &self.cmd_tx else {
            if announce {
                self.state.push_log("[INFO] Squad fetch unavailable");
            }
            return;
        };
        if tx
            .send(state::ProviderCommand::FetchSquad { team_id, team_name })
            .is_err()
        {
            if announce {
                self.state.push_log("[WARN] Squad request failed");
            }
        } else {
            if announce {
                self.state.push_log("[INFO] Squad request sent");
            }
            self.state.squad_loading = true;
            self.state.squad = Vec::new();
            self.state.squad_selected = 0;
        }
    }

    fn request_player_detail(&mut self, player_id: u32, player_name: String, announce: bool) {
        let Some(tx) = &self.cmd_tx else {
            if announce {
                self.state.push_log("[INFO] Player fetch unavailable");
            }
            return;
        };
        self.state.player_last_id = Some(player_id);
        self.state.player_last_name = Some(player_name.clone());
        if tx
            .send(state::ProviderCommand::FetchPlayer {
                player_id,
                player_name,
            })
            .is_err()
        {
            if announce {
                self.state.push_log("[WARN] Player request failed");
            }
        } else {
            if announce {
                self.state.push_log("[INFO] Player request sent");
            }
            self.state.player_loading = true;
            self.state.player_detail = None;
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
        Screen::Analysis => render_analysis(frame, chunks[1], &app.state),
        Screen::Squad => render_squad(frame, chunks[1], &app.state),
        Screen::PlayerDetail => render_player_detail(frame, chunks[1], &app.state),
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
        Screen::Analysis => {
            let updated = state
                .analysis_updated
                .as_deref()
                .unwrap_or("-");
            let status = if state.analysis_loading {
                "LOADING"
            } else {
                "READY"
            };
            format!(
                "WC26 ANALYSIS | Teams: {} | FIFA: {} | {}",
                state.analysis.len(),
                updated,
                status
            )
        }
        Screen::Squad => {
            let team = state.squad_team.as_deref().unwrap_or("-");
            let status = if state.squad_loading { "LOADING" } else { "READY" };
            format!(
                "WC26 SQUAD | Team: {} | Players: {} | {}",
                team,
                state.squad.len(),
                status
            )
        }
        Screen::PlayerDetail => "WC26 PLAYER".to_string(),
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
                "1 Pulse | 2 Analysis | Enter/d Terminal | j/k/↑/↓ Move | s Sort | l League | u Upcoming | i Details | ? Help | q Quit".to_string()
            }
            PulseView::Upcoming => {
                "1 Pulse | 2 Analysis | u Live | j/k/↑/↓ Scroll | l League | ? Help | q Quit"
                    .to_string()
            }
        },
        Screen::Terminal { .. } => {
            "1 Pulse | 2 Analysis | b/Esc Back | i Details | l League | ? Help | q Quit"
                .to_string()
        }
        Screen::Analysis => {
            "1 Pulse | b/Esc Back | j/k/↑/↓ Move | Enter Squad | r Refresh | ? Help | q Quit"
                .to_string()
        }
        Screen::Squad => {
            "1 Pulse | b/Esc Back | j/k/↑/↓ Move | Enter Player | r Refresh | ? Help | q Quit"
                .to_string()
        }
        Screen::PlayerDetail => {
            "1 Pulse | b/Esc Back | j/k/↑/↓ Scroll | r Refresh | ? Help | q Quit".to_string()
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

fn analysis_columns() -> [Constraint; 11] {
    [
        Constraint::Length(10),
        Constraint::Length(1),
        Constraint::Min(20),
        Constraint::Length(1),
        Constraint::Length(6),
        Constraint::Length(1),
        Constraint::Length(7),
        Constraint::Length(1),
        Constraint::Length(12),
        Constraint::Length(1),
        Constraint::Length(5),
    ]
}

fn squad_columns() -> [Constraint; 13] {
    [
        Constraint::Min(18),
        Constraint::Length(1),
        Constraint::Length(4),
        Constraint::Length(1),
        Constraint::Length(12),
        Constraint::Length(1),
        Constraint::Length(16),
        Constraint::Length(1),
        Constraint::Length(4),
        Constraint::Length(1),
        Constraint::Length(6),
        Constraint::Length(1),
        Constraint::Length(10),
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

fn render_analysis(frame: &mut Frame, area: Rect, state: &AppState) {
    let sections = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(1), Constraint::Min(1)])
        .split(area);

    let widths = analysis_columns();
    render_analysis_header(frame, sections[0], &widths);

    let list_area = sections[1];
    if state.analysis.is_empty() {
        let message = if state.analysis_loading {
            "Loading analysis..."
        } else {
            "No analysis data yet"
        };
        let empty = Paragraph::new(message).style(Style::default().fg(Color::DarkGray));
        frame.render_widget(empty, list_area);
        return;
    }

    if list_area.height == 0 {
        return;
    }

    let visible = list_area.height as usize;
    let total = state.analysis.len();
    let (start, end) = visible_range(state.analysis_selected, total, visible);

    for (i, idx) in (start..end).enumerate() {
        let row_area = Rect {
            x: list_area.x,
            y: list_area.y + i as u16,
            width: list_area.width,
            height: 1,
        };

        let selected = idx == state.analysis_selected;
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

        let row = &state.analysis[idx];
        let confed = confed_label(row.confed);
        let rank = row
            .fifa_rank
            .map(|v| v.to_string())
            .unwrap_or_else(|| "-".to_string());
        let points = row
            .fifa_points
            .map(|v| v.to_string())
            .unwrap_or_else(|| "-".to_string());
        let updated = row
            .fifa_updated
            .clone()
            .unwrap_or_else(|| "-".to_string());
        let host = if row.host { "yes" } else { "-" };

        let sep_style = Style::default().fg(Color::DarkGray);
        render_cell_text(frame, cols[0], confed, row_style);
        render_vseparator(frame, cols[1], sep_style);
        render_cell_text(frame, cols[2], &row.name, row_style);
        render_vseparator(frame, cols[3], sep_style);
        render_cell_text(frame, cols[4], &rank, row_style);
        render_vseparator(frame, cols[5], sep_style);
        render_cell_text(frame, cols[6], &points, row_style);
        render_vseparator(frame, cols[7], sep_style);
        render_cell_text(frame, cols[8], &updated, row_style);
        render_vseparator(frame, cols[9], sep_style);
        render_cell_text(frame, cols[10], host, row_style);
    }
}

fn render_analysis_header(frame: &mut Frame, area: Rect, widths: &[Constraint]) {
    let cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints(widths)
        .split(area);
    let style = Style::default().add_modifier(Modifier::BOLD);
    let sep_style = Style::default().fg(Color::DarkGray);

    render_cell_text(frame, cols[0], "Confed", style);
    render_vseparator(frame, cols[1], sep_style);
    render_cell_text(frame, cols[2], "Team", style);
    render_vseparator(frame, cols[3], sep_style);
    render_cell_text(frame, cols[4], "Rank", style);
    render_vseparator(frame, cols[5], sep_style);
    render_cell_text(frame, cols[6], "Points", style);
    render_vseparator(frame, cols[7], sep_style);
    render_cell_text(frame, cols[8], "Updated", style);
    render_vseparator(frame, cols[9], sep_style);
    render_cell_text(frame, cols[10], "Host", style);
}

fn render_squad(frame: &mut Frame, area: Rect, state: &AppState) {
    let sections = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(1), Constraint::Min(1)])
        .split(area);

    let widths = squad_columns();
    render_squad_header(frame, sections[0], &widths);

    let list_area = sections[1];
    if state.squad.is_empty() {
        let message = if state.squad_loading {
            "Loading squad..."
        } else {
            "No squad data yet"
        };
        let empty = Paragraph::new(message).style(Style::default().fg(Color::DarkGray));
        frame.render_widget(empty, list_area);
        return;
    }

    if list_area.height == 0 {
        return;
    }

    let visible = list_area.height as usize;
    let total = state.squad.len();
    let (start, end) = visible_range(state.squad_selected, total, visible);

    for (i, idx) in (start..end).enumerate() {
        let row_area = Rect {
            x: list_area.x,
            y: list_area.y + i as u16,
            width: list_area.width,
            height: 1,
        };

        let selected = idx == state.squad_selected;
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

        let player = &state.squad[idx];
        let age = player
            .age
            .map(|v| v.to_string())
            .unwrap_or_else(|| "-".to_string());
        let height = player
            .height
            .map(|v| v.to_string())
            .unwrap_or_else(|| "-".to_string());
        let number = player
            .shirt_number
            .map(|v| v.to_string())
            .unwrap_or_else(|| "-".to_string());
        let value = player
            .market_value
            .map(|v| format!("€{:.1}M", v as f64 / 1_000_000.0))
            .unwrap_or_else(|| "-".to_string());

        let sep_style = Style::default().fg(Color::DarkGray);
        render_cell_text(frame, cols[0], &player.name, row_style);
        render_vseparator(frame, cols[1], sep_style);
        render_cell_text(frame, cols[2], &number, row_style);
        render_vseparator(frame, cols[3], sep_style);
        render_cell_text(frame, cols[4], &player.role, row_style);
        render_vseparator(frame, cols[5], sep_style);
        render_cell_text(frame, cols[6], &player.club, row_style);
        render_vseparator(frame, cols[7], sep_style);
        render_cell_text(frame, cols[8], &age, row_style);
        render_vseparator(frame, cols[9], sep_style);
        render_cell_text(frame, cols[10], &height, row_style);
        render_vseparator(frame, cols[11], sep_style);
        render_cell_text(frame, cols[12], &value, row_style);
    }
}

fn render_squad_header(frame: &mut Frame, area: Rect, widths: &[Constraint]) {
    let cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints(widths)
        .split(area);
    let style = Style::default().add_modifier(Modifier::BOLD);
    let sep_style = Style::default().fg(Color::DarkGray);

    render_cell_text(frame, cols[0], "Player", style);
    render_vseparator(frame, cols[1], sep_style);
    render_cell_text(frame, cols[2], "No", style);
    render_vseparator(frame, cols[3], sep_style);
    render_cell_text(frame, cols[4], "Role", style);
    render_vseparator(frame, cols[5], sep_style);
    render_cell_text(frame, cols[6], "Club", style);
    render_vseparator(frame, cols[7], sep_style);
    render_cell_text(frame, cols[8], "Age", style);
    render_vseparator(frame, cols[9], sep_style);
    render_cell_text(frame, cols[10], "Ht", style);
    render_vseparator(frame, cols[11], sep_style);
    render_cell_text(frame, cols[12], "Value", style);
}

fn render_player_detail(frame: &mut Frame, area: Rect, state: &AppState) {
    let block = Block::default().title("Player Detail").borders(Borders::ALL);
    let inner = block.inner(area);
    frame.render_widget(block, area);

    if inner.height == 0 || inner.width == 0 {
        return;
    }

    if state.player_loading {
        let text = Paragraph::new("Loading player details...")
            .style(Style::default().fg(Color::DarkGray));
        frame.render_widget(text, inner);
        return;
    }

    let Some(detail) = state.player_detail.as_ref() else {
        let text = Paragraph::new("No player data yet")
            .style(Style::default().fg(Color::DarkGray));
        frame.render_widget(text, inner);
        return;
    };

    if inner.height < 8 {
        let text = player_detail_text(detail);
        let paragraph = Paragraph::new(text).scroll((state.player_detail_scroll, 0));
        frame.render_widget(paragraph, inner);
        return;
    }

    let info_text = player_info_text(detail);
    let league_text = player_league_stats_text(detail);
    let top_text = player_top_stats_text(detail);
    let traits_text = player_traits_text(detail);
    let other_text = player_other_stats_text(detail);
    let recent_text = player_recent_matches_text(detail);

    let info_lines = text_line_count(&info_text);
    let league_lines = text_line_count(&league_text);
    let top_lines = text_line_count(&top_text);
    let traits_lines = text_line_count(&traits_text);
    let other_lines = text_line_count(&other_text);
    let recent_lines = text_line_count(&recent_text);

    let info_height = text_block_height_from_lines(info_lines, 8);
    let league_height = text_block_height_from_lines(league_lines, 7);
    let top_height = text_block_height_from_lines(top_lines, 7);
    let traits_height = text_block_height_from_lines(traits_lines, 7);
    let other_height = text_block_height_from_lines(other_lines, 9);

    let sections = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(info_height),
            Constraint::Length(league_height),
            Constraint::Length(top_height),
            Constraint::Length(traits_height),
            Constraint::Length(other_height),
            Constraint::Min(3),
        ])
        .split(inner);

    render_detail_section(
        frame,
        sections[0],
        "Player Info",
        &info_text,
        state.player_detail_section_scrolls[0],
        state.player_detail_section == 0,
        info_lines,
    );
    render_detail_section(
        frame,
        sections[1],
        "All Competitions",
        &league_text,
        state.player_detail_section_scrolls[1],
        state.player_detail_section == 1,
        league_lines,
    );
    render_detail_section(
        frame,
        sections[2],
        "Top Stats (All Competitions)",
        &top_text,
        state.player_detail_section_scrolls[2],
        state.player_detail_section == 2,
        top_lines,
    );
    render_detail_section(
        frame,
        sections[3],
        "Player Traits",
        &traits_text,
        state.player_detail_section_scrolls[3],
        state.player_detail_section == 3,
        traits_lines,
    );
    render_detail_section(
        frame,
        sections[4],
        "Other Stats",
        &other_text,
        state.player_detail_section_scrolls[4],
        state.player_detail_section == 4,
        other_lines,
    );
    render_detail_section(
        frame,
        sections[5],
        "Match Stats (Recent)",
        &recent_text,
        state.player_detail_section_scrolls[5],
        state.player_detail_section == 5,
        recent_lines,
    );
}

fn player_detail_has_stats(detail: &PlayerDetail) -> bool {
    !detail.all_competitions.is_empty()
        || detail.main_league.is_some()
        || !detail.top_stats.is_empty()
        || !detail.season_groups.is_empty()
        || detail
            .traits
            .as_ref()
            .map(|traits| !traits.items.is_empty())
            .unwrap_or(false)
        || !detail.recent_matches.is_empty()
}

fn player_detail_text(detail: &PlayerDetail) -> String {
    let mut lines = Vec::new();
    lines.push(player_info_text(detail));
    lines.push(String::new());
    lines.push(player_league_stats_text(detail));
    lines.push(String::new());
    lines.push(player_top_stats_text(detail));
    lines.push(String::new());
    lines.push(player_traits_text(detail));
    lines.push(String::new());
    lines.push(player_other_stats_text(detail));
    lines.push(String::new());
    lines.push(player_recent_matches_text(detail));
    lines.join("\n")
}

fn player_info_text(detail: &PlayerDetail) -> String {
    let mut lines = Vec::new();
    lines.push(format!("Name: {}", detail.name));
    lines.push(format!("ID: {}", detail.id));
    if let Some(team) = &detail.team {
        lines.push(format!("Team: {team}"));
    }
    if let Some(position) = &detail.position {
        lines.push(format!("Position: {position}"));
    }
    if let Some(age) = &detail.age {
        lines.push(format!("Age: {age}"));
    }
    if let Some(country) = &detail.country {
        lines.push(format!("Country: {country}"));
    }
    if let Some(height) = &detail.height {
        lines.push(format!("Height: {height}"));
    }
    if let Some(foot) = &detail.preferred_foot {
        lines.push(format!("Preferred foot: {foot}"));
    }
    if let Some(shirt) = &detail.shirt {
        lines.push(format!("Shirt: {shirt}"));
    }
    if let Some(value) = &detail.market_value {
        lines.push(format!("Market value: {value}"));
    }
    if let Some(contract_end) = &detail.contract_end {
        lines.push(format!("Contract end: {contract_end}"));
    }
    lines.join("\n")
}

fn player_league_stats_text(detail: &PlayerDetail) -> String {
    if detail.all_competitions.is_empty() {
        return "No all-competitions stats".to_string();
    }
    let mut lines = Vec::new();
    let season_label = detail
        .all_competitions_season
        .as_deref()
        .unwrap_or("-");
    lines.push(format!("All competitions ({season_label})"));
    for stat in detail.all_competitions.iter().take(8) {
        lines.push(format!("{}: {}", stat.title, stat.value));
    }
    lines.join("\n")
}

fn player_top_stats_text(detail: &PlayerDetail) -> String {
    if detail.top_stats.is_empty() {
        return "No all-competitions top stats".to_string();
    }
    let mut lines = Vec::new();
    for stat in detail.top_stats.iter().take(8) {
        lines.push(format!("{}: {}", stat.title, stat.value));
    }
    lines.join("\n")
}

fn player_traits_text(detail: &PlayerDetail) -> String {
    let Some(traits) = &detail.traits else {
        return "No traits".to_string();
    };
    let mut lines = Vec::new();
    lines.push(traits.title.clone());
    for item in traits.items.iter().take(8) {
        lines.push(format!("{}: {:.0}%", item.title, item.value * 100.0));
    }
    lines.join("\n")
}

fn player_other_stats_text(detail: &PlayerDetail) -> String {
    if detail.season_groups.is_empty() {
        return "No additional stats".to_string();
    }
    let mut lines = Vec::new();
    for group in detail.season_groups.iter().take(3) {
        lines.push(format!("{}:", group.title));
        for item in group.items.iter().take(5) {
            lines.push(format!("  {}: {}", item.title, item.value));
        }
    }
    lines.join("\n")
}

fn player_recent_matches_text(detail: &PlayerDetail) -> String {
    if detail.recent_matches.is_empty() {
        return "No recent matches".to_string();
    }
    let mut lines = Vec::new();
    for m in detail.recent_matches.iter().take(10) {
        let date = shorten_date(&m.date);
        let rating = m.rating.as_deref().unwrap_or("-");
        lines.push(format!(
            "{date} vs {} | {} | G {} A {} | R {}",
            m.opponent, m.league, m.goals, m.assists, rating
        ));
    }
    lines.join("\n")
}

fn render_detail_section(
    frame: &mut Frame,
    area: Rect,
    title: &str,
    body: &str,
    scroll: u16,
    active: bool,
    total_lines: u16,
) {
    if area.height == 0 || area.width == 0 {
        return;
    }
    let border_style = if active {
        Style::default().fg(Color::Yellow)
    } else {
        Style::default()
    };
    let max_scroll = total_lines.saturating_sub(1);
    let current = scroll.min(max_scroll) + 1;
    let total = max_scroll + 1;
    let title = format!("{title}  {current}/{total}");
    let block = Block::default()
        .title(title)
        .borders(Borders::ALL)
        .border_style(border_style);
    let inner = block.inner(area);
    frame.render_widget(block, area);
    if inner.height == 0 || inner.width == 0 {
        return;
    }
    let paragraph = Paragraph::new(body).scroll((scroll, 0));
    frame.render_widget(paragraph, inner);
}

fn text_line_count(text: &str) -> u16 {
    text.lines().count().max(1) as u16
}

fn text_block_height_from_lines(lines: u16, max_height: u16) -> u16 {
    (lines + 2).min(max_height).max(3)
}

fn shorten_date(raw: &str) -> String {
    if raw.len() >= 10 {
        raw[..10].to_string()
    } else {
        raw.to_string()
    }
}

fn player_detail_section_max_scroll(detail: &PlayerDetail, section: usize) -> u16 {
    let lines = match section {
        0 => player_info_text(detail),
        1 => player_league_stats_text(detail),
        2 => player_top_stats_text(detail),
        3 => player_traits_text(detail),
        4 => player_other_stats_text(detail),
        _ => player_recent_matches_text(detail),
    };
    text_line_count(&lines).saturating_sub(1)
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
        "  2 / a        Analysis",
        "  Enter / d    Terminal",
        "  b / Esc      Back",
        "  l            League toggle",
        "  u            Upcoming view",
        "  i            Fetch match details",
        "  r            Refresh analysis/squad/player",
        "  ?            Toggle help",
        "  q            Quit",
        "",
        "Pulse:",
        "  j/k or ↑/↓   Move/scroll",
        "  s            Cycle sort mode",
        "",
        "Analysis/Squad:",
        "  Enter        Open squad / player detail",
        "",
        "Player detail:",
        "  j/k or ↑/↓   Scroll",
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
