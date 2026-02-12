use std::collections::{HashMap, HashSet};
use std::io;
use std::sync::{OnceLock, mpsc};
use std::thread;
use std::time::{Duration, Instant, SystemTime};

use chrono::{DateTime, Local, NaiveDateTime, TimeZone, Utc};
use crossterm::event::{
    self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyEvent, KeyEventKind,
    KeyModifiers,
};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::prelude::*;
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{
    Block, BorderType, Borders, Clear, Gauge, Padding, Paragraph, Sparkline, Wrap,
};

use wc26_terminal::{analysis_rankings, feed, http_cache, persist, upcoming_fetch};

use wc26_terminal::state::{
    self, AppState, LeagueMode, PLACEHOLDER_MATCH_ID, PLAYER_DETAIL_SECTIONS, PlayerDetail,
    PlayerStatItem, PulseView, RoleCategory, Screen, TerminalFocus, apply_delta, confed_label,
    league_label, metric_label, placeholder_match_detail, placeholder_match_summary, role_label,
};

#[derive(Debug, Clone)]
struct PredictionSnapshot {
    matches: Vec<state::MatchSummary>,
    upcoming: Vec<state::UpcomingMatch>,
    match_detail: HashMap<String, state::MatchDetail>,
    combined_player_cache: HashMap<u32, state::PlayerDetail>,
    rankings_cache_squads: HashMap<u32, Vec<state::SquadPlayer>>,
    analysis: Vec<state::TeamAnalysis>,
    league_params: HashMap<u32, wc26_terminal::league_params::LeagueParams>,
    elo_by_league: HashMap<u32, HashMap<u32, f64>>,
    prematch_locked: HashSet<String>,
}

#[derive(Debug, Clone)]
enum PredictionCommand {
    Compute {
        generation: u64,
        snapshot: PredictionSnapshot,
    },
}

fn spawn_prediction_worker(tx: mpsc::Sender<state::Delta>) -> mpsc::Sender<PredictionCommand> {
    let (cmd_tx, cmd_rx) = mpsc::channel::<PredictionCommand>();
    thread::spawn(move || {
        loop {
            let Ok(mut cmd) = cmd_rx.recv() else {
                return;
            };
            while let Ok(next) = cmd_rx.try_recv() {
                cmd = next;
            }
            let PredictionCommand::Compute {
                generation,
                snapshot,
            } = cmd;

            let mut wins: Vec<state::ComputedWin> = Vec::with_capacity(snapshot.matches.len());
            let mut prematch: Vec<state::ComputedPrematch> =
                Vec::with_capacity(snapshot.matches.len() + snapshot.upcoming.len());

            for m in &snapshot.matches {
                let detail = snapshot.match_detail.get(&m.id);
                let league_id = m.league_id.unwrap_or(0);
                let params = snapshot.league_params.get(&league_id);
                let elo = snapshot.elo_by_league.get(&league_id);
                let (win, extras) = wc26_terminal::win_prob::compute_win_prob_explainable(
                    m,
                    detail,
                    &snapshot.combined_player_cache,
                    &snapshot.rankings_cache_squads,
                    &snapshot.analysis,
                    params,
                    elo,
                );
                wins.push(state::ComputedWin {
                    id: m.id.clone(),
                    win: win.clone(),
                    extras: extras.clone(),
                });

                if snapshot.prematch_locked.contains(&m.id) {
                    continue;
                }
                if !m.is_live && m.minute == 0 {
                    prematch.push(state::ComputedPrematch {
                        id: m.id.clone(),
                        win,
                        extras,
                        lock: false,
                    });
                } else if m.is_live || m.minute > 0 {
                    // Synthesize a pre-match snapshot once the match is underway.
                    let mut pre = m.clone();
                    pre.is_live = false;
                    pre.minute = 0;
                    pre.score_home = 0;
                    pre.score_away = 0;
                    let detail = snapshot.match_detail.get(&pre.id);
                    let league_id = pre.league_id.unwrap_or(0);
                    let params = snapshot.league_params.get(&league_id);
                    let elo = snapshot.elo_by_league.get(&league_id);
                    let (prematch_win, prematch_extras) =
                        wc26_terminal::win_prob::compute_win_prob_explainable(
                            &pre,
                            detail,
                            &snapshot.combined_player_cache,
                            &snapshot.rankings_cache_squads,
                            &snapshot.analysis,
                            params,
                            elo,
                        );
                    prematch.push(state::ComputedPrematch {
                        id: pre.id,
                        win: prematch_win,
                        extras: prematch_extras,
                        lock: true,
                    });
                }
            }

            for u in &snapshot.upcoming {
                if snapshot.prematch_locked.contains(&u.id) {
                    continue;
                }
                let summary = state::MatchSummary {
                    id: u.id.clone(),
                    league_id: u.league_id,
                    league_name: u.league_name.clone(),
                    home_team_id: u.home_team_id,
                    away_team_id: u.away_team_id,
                    home: u.home.clone(),
                    away: u.away.clone(),
                    minute: 0,
                    score_home: 0,
                    score_away: 0,
                    win: state::WinProbRow {
                        p_home: 0.0,
                        p_draw: 0.0,
                        p_away: 0.0,
                        delta_home: 0.0,
                        quality: state::ModelQuality::Basic,
                        confidence: 0,
                    },
                    is_live: false,
                };
                let detail = snapshot.match_detail.get(&u.id);
                let league_id = summary.league_id.unwrap_or(0);
                let params = snapshot.league_params.get(&league_id);
                let elo = snapshot.elo_by_league.get(&league_id);
                let (prematch_win, extras) = wc26_terminal::win_prob::compute_win_prob_explainable(
                    &summary,
                    detail,
                    &snapshot.combined_player_cache,
                    &snapshot.rankings_cache_squads,
                    &snapshot.analysis,
                    params,
                    elo,
                );
                prematch.push(state::ComputedPrematch {
                    id: u.id.clone(),
                    win: prematch_win,
                    extras,
                    lock: false,
                });
            }

            let _ = tx.send(state::Delta::ComputedPredictions {
                generation,
                wins,
                prematch,
            });
        }
    });
    cmd_tx
}

struct App {
    state: AppState,
    should_quit: bool,
    ui_anim_frame: u64,
    ui_anim_started_at: Instant,
    ui_last_anim_tick: Instant,
    cmd_tx: Option<mpsc::Sender<state::ProviderCommand>>,
    pred_tx: Option<mpsc::Sender<PredictionCommand>>,
    pred_inflight: bool,
    pred_generation: u64,
    upcoming_refresh: Duration,
    last_upcoming_refresh: Instant,
    upcoming_cache_ttl: Duration,
    detail_refresh: Duration,
    commentary_refresh: Duration,
    last_detail_refresh: HashMap<String, Instant>,
    detail_request_throttle: Duration,
    hover_prefetch_delay: Duration,
    hover_selected_match_id: Option<String>,
    hover_selected_since: Instant,
    hover_prefetched_match_id: Option<String>,
    detail_cache_ttl: Duration,
    prefetch_players_limit: usize,
    auto_warm_mode: AutoWarmMode,
    auto_warm_pending: bool,
    prediction_model_auto_warm: bool,
    prediction_model_warm_pending: bool,
    prediction_model_warm_ttl: Duration,
    analysis_request_throttle: Duration,
    last_analysis_request: HashMap<LeagueMode, Instant>,
    detail_dist_cache: Option<DetailDistCache>,

    rankings_last_recompute: Instant,
    rankings_update_counter: u32,
    rankings_recompute_interval: Duration,
    rankings_recompute_min_updates: u32,

    predictions_last_recompute: Instant,
    predictions_recompute_interval: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AutoWarmMode {
    Off,
    Missing,
    Full,
}

impl App {
    fn new(
        cmd_tx: Option<mpsc::Sender<state::ProviderCommand>>,
        pred_tx: Option<mpsc::Sender<PredictionCommand>>,
    ) -> Self {
        let upcoming_refresh = std::env::var("UPCOMING_POLL_SECS")
            .ok()
            .and_then(|val| val.parse::<u64>().ok())
            .unwrap_or(60)
            .max(10);
        let upcoming_cache_ttl = std::env::var("UPCOMING_CACHE_SECS")
            .ok()
            .and_then(|val| val.parse::<u64>().ok())
            .unwrap_or(300)
            .max(10);
        let detail_refresh = std::env::var("DETAILS_POLL_SECS")
            .ok()
            .and_then(|val| val.parse::<u64>().ok())
            .unwrap_or(60)
            .max(30);
        let commentary_refresh = std::env::var("COMMENTARY_POLL_SECS")
            .ok()
            .and_then(|val| val.parse::<u64>().ok())
            .unwrap_or(15)
            .clamp(5, 120);
        let detail_request_throttle = std::env::var("DETAILS_THROTTLE_SECS")
            .ok()
            .and_then(|val| val.parse::<u64>().ok())
            .unwrap_or(5)
            .max(1);
        let detail_cache_ttl = std::env::var("DETAILS_CACHE_SECS")
            .ok()
            .and_then(|val| val.parse::<u64>().ok())
            .unwrap_or(3600)
            .max(30);
        let hover_prefetch_delay_ms = std::env::var("PREFETCH_MATCH_DETAILS_MS")
            .ok()
            .and_then(|val| val.parse::<u64>().ok())
            .unwrap_or(450)
            .max(0);
        let prefetch_players_limit = std::env::var("PREFETCH_PLAYERS")
            .ok()
            .and_then(|val| val.parse::<usize>().ok())
            .unwrap_or(10)
            .clamp(0, 40);
        let analysis_request_throttle = std::env::var("ANALYSIS_THROTTLE_SECS")
            .ok()
            .and_then(|val| val.parse::<u64>().ok())
            .unwrap_or(10)
            .max(1);
        let rankings_recompute_ms = std::env::var("RANKINGS_RECOMPUTE_MS")
            .ok()
            .and_then(|val| val.parse::<u64>().ok())
            .unwrap_or(250)
            .clamp(50, 5_000);
        let rankings_recompute_min_updates = std::env::var("RANKINGS_RECOMPUTE_MIN_UPDATES")
            .ok()
            .and_then(|val| val.parse::<u32>().ok())
            .unwrap_or(25)
            .clamp(1, 5_000);
        let predictions_recompute_ms = std::env::var("PREDICTIONS_RECOMPUTE_MS")
            .ok()
            .and_then(|val| val.parse::<u64>().ok())
            .unwrap_or(500)
            .clamp(100, 10_000);

        let rankings_recompute_interval = Duration::from_millis(rankings_recompute_ms);
        let predictions_recompute_interval = Duration::from_millis(predictions_recompute_ms);
        let auto_warm_mode = parse_auto_warm_mode();
        let prediction_model_auto_warm = std::env::var("AUTO_WARM_PREDICTION_MODEL")
            .ok()
            .map(|v| v != "0" && v.to_ascii_lowercase() != "false")
            .unwrap_or(true);
        let prediction_model_warm_ttl = Duration::from_secs(
            std::env::var("PRED_MODEL_WARM_TTL_SECS")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(24 * 3600)
                .max(60),
        );
        let now = Instant::now();
        Self {
            state: AppState::new(),
            should_quit: false,
            ui_anim_frame: 0,
            ui_anim_started_at: now,
            ui_last_anim_tick: now,
            cmd_tx,
            pred_tx,
            pred_inflight: false,
            pred_generation: 0,
            upcoming_refresh: Duration::from_secs(upcoming_refresh),
            last_upcoming_refresh: Instant::now(),
            upcoming_cache_ttl: Duration::from_secs(upcoming_cache_ttl),
            detail_refresh: Duration::from_secs(detail_refresh),
            commentary_refresh: Duration::from_secs(commentary_refresh),
            last_detail_refresh: HashMap::new(),
            detail_request_throttle: Duration::from_secs(detail_request_throttle),
            hover_prefetch_delay: Duration::from_millis(hover_prefetch_delay_ms),
            hover_selected_match_id: None,
            hover_selected_since: Instant::now(),
            hover_prefetched_match_id: None,
            detail_cache_ttl: Duration::from_secs(detail_cache_ttl),
            prefetch_players_limit,
            auto_warm_pending: auto_warm_mode != AutoWarmMode::Off,
            auto_warm_mode,
            prediction_model_auto_warm,
            prediction_model_warm_pending: prediction_model_auto_warm,
            prediction_model_warm_ttl,
            analysis_request_throttle: Duration::from_secs(analysis_request_throttle),
            last_analysis_request: HashMap::new(),
            detail_dist_cache: None,

            rankings_last_recompute: Instant::now() - rankings_recompute_interval,
            rankings_update_counter: 0,
            rankings_recompute_interval,
            rankings_recompute_min_updates,

            predictions_last_recompute: Instant::now() - predictions_recompute_interval,
            predictions_recompute_interval,
        }
    }

    fn maybe_hover_prefetch_match_details(&mut self) {
        if self.hover_prefetch_delay.is_zero() {
            return;
        }
        if !matches!(self.state.screen, Screen::Pulse) || self.state.pulse_view != PulseView::Live {
            self.hover_selected_match_id = None;
            self.hover_prefetched_match_id = None;
            return;
        }

        let selected = self.state.selected_match_id();
        if selected != self.hover_selected_match_id {
            self.hover_selected_match_id = selected.clone();
            self.hover_selected_since = Instant::now();
            if self.hover_prefetched_match_id != selected {
                self.hover_prefetched_match_id = None;
            }
        }
        let Some(match_id) = selected else {
            return;
        };
        if self.hover_prefetched_match_id.as_deref() == Some(match_id.as_str()) {
            return;
        }
        if self.hover_selected_since.elapsed() < self.hover_prefetch_delay {
            return;
        }

        // Quietly warm details while the user hovers. UI updates when the provider responds.
        self.request_match_details_basic_for(&match_id);
        self.hover_prefetched_match_id = Some(match_id);
    }

    fn on_key(&mut self, key: KeyEvent) {
        if self.state.export.active {
            if self.state.export.done {
                self.state.export = state::ExportState::new();
            }
            return;
        }
        if self.state.terminal_detail.is_some() {
            match key.code {
                KeyCode::Esc | KeyCode::Char('b') | KeyCode::Enter => {
                    self.state.terminal_detail = None;
                    self.state.terminal_detail_scroll = 0;
                }
                KeyCode::Up | KeyCode::Left => {
                    self.state.terminal_detail_scroll =
                        self.state.terminal_detail_scroll.saturating_sub(1);
                }
                KeyCode::Down | KeyCode::Right => {
                    self.state.terminal_detail_scroll =
                        self.state.terminal_detail_scroll.saturating_add(1);
                }
                _ => {}
            }
            return;
        }

        if self.state.screen == Screen::Analysis
            && self.state.analysis_tab == state::AnalysisTab::RoleRankings
            && self.state.rankings_search_active
        {
            match key.code {
                KeyCode::Esc => {
                    self.state.rankings_search_active = false;
                    self.state.rankings_search.clear();
                    self.state.rankings_selected = 0;
                }
                KeyCode::Enter => {
                    self.state.rankings_search_active = false;
                    self.state.clamp_rankings_selection();
                }
                KeyCode::Backspace => {
                    self.state.rankings_search.pop();
                    self.state.clamp_rankings_selection();
                }
                KeyCode::Char(c) => {
                    if !key.modifiers.contains(KeyModifiers::CONTROL) {
                        self.state.rankings_search.push(c);
                        self.state.clamp_rankings_selection();
                    }
                }
                _ => {}
            }
            return;
        }

        match key.code {
            KeyCode::Char('q') => self.should_quit = true,
            KeyCode::Char('1') => self.state.screen = Screen::Pulse,
            KeyCode::Char('2') | KeyCode::Char('a') | KeyCode::Char('A') => {
                self.state.screen = Screen::Analysis;
                if self.state.analysis.is_empty() && !self.state.analysis_loading {
                    self.request_analysis(true);
                }
            }
            KeyCode::Char('d') | KeyCode::Enter => match self.state.screen {
                Screen::Pulse => {
                    let match_id = self.state.selected_match_id();
                    if self.state.pulse_view == PulseView::Live {
                        self.state.screen = Screen::Terminal { match_id };
                        self.state.terminal_focus = TerminalFocus::MatchList;
                        self.state.terminal_detail = None;
                        self.state.terminal_detail_scroll = 0;
                        self.request_match_details(true);
                    }
                }
                Screen::Analysis => {
                    if self.state.analysis_tab == state::AnalysisTab::Teams {
                        let team = self.state.selected_analysis().cloned();
                        if let Some(team) = team {
                            self.state.screen = Screen::Squad;
                            let needs_fetch = self.state.squad_team_id != Some(team.id)
                                || self.state.squad.is_empty();
                            if needs_fetch && !self.state.squad_loading {
                                self.request_squad(team.id, team.name.clone(), true, false);
                            }
                        }
                    } else {
                        // Rankings: open player detail directly.
                        let entry = {
                            let mut rows = self.state.rankings_filtered();
                            match self.state.rankings_metric {
                                state::RankMetric::Attacking => {
                                    rows.sort_by(|a, b| b.attack_score.total_cmp(&a.attack_score))
                                }
                                state::RankMetric::Defending => {
                                    rows.sort_by(|a, b| b.defense_score.total_cmp(&a.defense_score))
                                }
                            }
                            rows.get(self.state.rankings_selected).copied().cloned()
                        };

                        if let Some(entry) = entry {
                            self.state.screen = Screen::PlayerDetail;
                            self.state.player_detail_back = Screen::Analysis;
                            self.state.player_detail_scroll = 0;
                            self.state.player_detail_section = 0;
                            self.state.player_detail_section_scrolls = [0; PLAYER_DETAIL_SECTIONS];
                            self.state.player_detail_expanded = false;
                            self.detail_dist_cache = None;
                            self.state.player_last_id = Some(entry.player_id);
                            self.state.player_last_name = Some(entry.player_name.clone());

                            if let Some(cached) = self
                                .state
                                .rankings_cache_players
                                .get(&entry.player_id)
                                .cloned()
                            {
                                self.state.player_detail = Some(cached);
                                self.state.player_loading = false;
                            } else if !self.state.player_loading {
                                self.request_player_detail(
                                    entry.player_id,
                                    entry.player_name.clone(),
                                    true,
                                    false,
                                );
                            }
                        }
                    }
                }
                Screen::Squad => {
                    let player = self.state.selected_squad_player().cloned();
                    if let Some(player) = player {
                        self.state.screen = Screen::PlayerDetail;
                        self.state.player_detail_back = Screen::Squad;
                        self.state.player_detail_scroll = 0;
                        self.state.player_detail_section = 0;
                        self.state.player_detail_section_scrolls = [0; PLAYER_DETAIL_SECTIONS];
                        self.state.player_detail_expanded = false;
                        self.detail_dist_cache = None;
                        let cached_detail = self.state.player_detail.as_ref();
                        let cached = cached_detail
                            .map(|detail| detail.id == player.id)
                            .unwrap_or(false);
                        let needs_stats_refresh = cached_detail
                            .map(|detail| !player_detail_has_stats(detail))
                            .unwrap_or(false);
                        if (!cached || needs_stats_refresh) && !self.state.player_loading {
                            self.request_player_detail(player.id, player.name.clone(), true, false);
                        }
                    }
                }
                Screen::Terminal { .. } => {
                    // Expanding Ticker/Commentary should pull fresh match details immediately so
                    // the overlay updates in-place as new commentary arrives.
                    if matches!(
                        self.state.terminal_focus,
                        TerminalFocus::EventTape | TerminalFocus::Commentary
                    ) {
                        self.request_match_details_with_opts(false, true, false);
                    }
                    self.state.terminal_detail = Some(self.state.terminal_focus);
                    self.state.terminal_detail_scroll = 0;
                }
                Screen::PlayerDetail => {
                    self.state.player_detail_expanded = !self.state.player_detail_expanded;
                    self.state.player_detail_scroll = 0;
                }
            },
            KeyCode::Char('m') | KeyCode::Char('M') => self.dump_match_state(),
            KeyCode::Char('b') | KeyCode::Esc => {
                self.state.screen = match self.state.screen {
                    Screen::Terminal { .. } => Screen::Pulse,
                    Screen::Analysis => Screen::Pulse,
                    Screen::Squad => Screen::Analysis,
                    Screen::PlayerDetail => self.state.player_detail_back.clone(),
                    Screen::Pulse => Screen::Pulse,
                };
            }
            KeyCode::Char('j') | KeyCode::Down => {
                if matches!(self.state.screen, Screen::Analysis) {
                    match self.state.analysis_tab {
                        state::AnalysisTab::Teams => self.state.select_analysis_next(),
                        state::AnalysisTab::RoleRankings => self.state.select_rankings_next(),
                    }
                } else if matches!(self.state.screen, Screen::Squad) {
                    self.state.select_squad_next();
                } else if matches!(self.state.screen, Screen::PlayerDetail) {
                    let max_scroll = self
                        .state
                        .player_detail
                        .as_ref()
                        .map(|detail| {
                            player_detail_section_max_scroll(
                                detail,
                                self.state.player_detail_section,
                            )
                        })
                        .unwrap_or(0);
                    self.state.scroll_player_detail_down(max_scroll);
                } else {
                    self.state.select_next();
                }
            }
            KeyCode::Char('k') | KeyCode::Up => {
                if matches!(self.state.screen, Screen::Analysis) {
                    match self.state.analysis_tab {
                        state::AnalysisTab::Teams => self.state.select_analysis_prev(),
                        state::AnalysisTab::RoleRankings => self.state.select_rankings_prev(),
                    }
                } else if matches!(self.state.screen, Screen::Squad) {
                    self.state.select_squad_prev();
                } else if matches!(self.state.screen, Screen::PlayerDetail) {
                    self.state.scroll_player_detail_up();
                } else {
                    self.state.select_prev();
                }
            }
            KeyCode::Char('s') => {
                if matches!(self.state.screen, Screen::Analysis)
                    && self.state.analysis_tab == state::AnalysisTab::RoleRankings
                {
                    self.state.cycle_rankings_metric();
                } else {
                    self.state.cycle_sort();
                }
            }
            KeyCode::Char('l') | KeyCode::Char('L') => {
                // Persist current league cache before switching away.
                persist::save_from_state(&self.state);
                self.state.cycle_league_mode();
                self.detail_dist_cache = None;
                if self.auto_warm_mode != AutoWarmMode::Off {
                    self.auto_warm_pending = true;
                }
                // Load cache for the newly selected league.
                persist::load_into_state(&mut self.state);
                self.request_upcoming(true);
                if matches!(self.state.screen, Screen::Analysis) {
                    self.request_analysis(true);
                }
            }
            KeyCode::Char('/') | KeyCode::Char('f') | KeyCode::Char('F')
                if self.state.screen == Screen::Analysis
                    && self.state.analysis_tab == state::AnalysisTab::RoleRankings =>
            {
                self.state.rankings_search_active = true;
            }
            KeyCode::Char('u') | KeyCode::Char('U') => {
                let to_upcoming = self.state.pulse_view == PulseView::Live;
                self.state.toggle_pulse_view();
                if to_upcoming {
                    self.request_upcoming(true);
                }
            }
            KeyCode::Tab => {
                if matches!(self.state.screen, Screen::Analysis) {
                    self.state.cycle_analysis_tab();
                    if self.state.analysis_tab == state::AnalysisTab::RoleRankings {
                        self.request_rankings_cache_warm_missing(true);
                        self.recompute_rankings_from_cache();
                    }
                } else if matches!(self.state.screen, Screen::Terminal { .. }) {
                    let prev = self.state.terminal_focus;
                    self.state.cycle_terminal_focus_next();
                    if prev != self.state.terminal_focus
                        && self.state.terminal_focus == TerminalFocus::Commentary
                    {
                        // Ensure commentary is populated when the user focuses it.
                        self.request_match_details(false);
                    }
                } else if matches!(self.state.screen, Screen::PlayerDetail) {
                    self.state.cycle_player_detail_section_next();
                }
            }
            KeyCode::BackTab => {
                if matches!(self.state.screen, Screen::Terminal { .. }) {
                    let prev = self.state.terminal_focus;
                    self.state.cycle_terminal_focus_prev();
                    if prev != self.state.terminal_focus
                        && self.state.terminal_focus == TerminalFocus::Commentary
                    {
                        self.request_match_details(false);
                    }
                } else if matches!(self.state.screen, Screen::PlayerDetail) {
                    self.state.cycle_player_detail_section_prev();
                }
            }
            KeyCode::Left => {
                if matches!(self.state.screen, Screen::Analysis)
                    && self.state.analysis_tab == state::AnalysisTab::RoleRankings
                {
                    self.state.cycle_rankings_role_prev();
                }
            }
            KeyCode::Right => {
                if matches!(self.state.screen, Screen::Analysis)
                    && self.state.analysis_tab == state::AnalysisTab::RoleRankings
                {
                    self.state.cycle_rankings_role_next();
                }
            }
            KeyCode::Char('r') => {
                if matches!(self.state.screen, Screen::Analysis) {
                    match self.state.analysis_tab {
                        state::AnalysisTab::Teams => self.request_analysis(true),
                        state::AnalysisTab::RoleRankings => {
                            // Incremental: fetch only missing squads/players.
                            self.request_rankings_cache_warm_missing(true);
                            self.recompute_rankings_from_cache();
                        }
                    }
                } else if matches!(self.state.screen, Screen::Squad) {
                    if let Some(team_id) = self.state.squad_team_id {
                        let team_name = self
                            .state
                            .squad_team
                            .clone()
                            .unwrap_or_else(|| "Team".to_string());
                        self.request_squad(team_id, team_name, true, false);
                    }
                } else if matches!(self.state.screen, Screen::PlayerDetail)
                    && let (Some(player_id), Some(player_name)) = (
                        self.state.player_last_id,
                        self.state.player_last_name.clone(),
                    )
                {
                    self.detail_dist_cache = None;
                    self.request_player_detail(player_id, player_name, true, false);
                }
            }
            KeyCode::Char('p') | KeyCode::Char('P') => self.toggle_placeholder_match(),
            KeyCode::Char('R') => {
                if matches!(self.state.screen, Screen::Analysis)
                    && self.state.analysis_tab == state::AnalysisTab::RoleRankings
                {
                    // Full refresh for latest data.
                    self.clear_rankings_cache();
                    self.request_rankings_cache_warm_full(true);
                    self.recompute_rankings_from_cache();
                } else if matches!(self.state.screen, Screen::Analysis) {
                    self.request_analysis(true);
                } else if matches!(self.state.screen, Screen::Squad) {
                    if let Some(team_id) = self.state.squad_team_id {
                        let team_name = self
                            .state
                            .squad_team
                            .clone()
                            .unwrap_or_else(|| "Team".to_string());
                        self.request_squad(team_id, team_name, true, true);
                    }
                } else if matches!(self.state.screen, Screen::PlayerDetail)
                    && let (Some(player_id), Some(player_name)) = (
                        self.state.player_last_id,
                        self.state.player_last_name.clone(),
                    )
                {
                    self.request_player_detail(player_id, player_name, true, true);
                }
            }
            KeyCode::Char('i') | KeyCode::Char('I') => self.request_match_details(true),
            KeyCode::Char('e') | KeyCode::Char('E') => {
                if matches!(self.state.screen, Screen::Analysis) {
                    self.request_analysis_export(true);
                }
            }
            KeyCode::Char('x') | KeyCode::Char('X') => {
                if matches!(self.state.screen, Screen::Terminal { .. })
                    && self.state.terminal_focus == TerminalFocus::Prediction
                {
                    self.state.prediction_show_why = !self.state.prediction_show_why;
                }
            }
            KeyCode::Char('?') => self.state.help_overlay = !self.state.help_overlay,
            _ => {}
        }
    }

    fn dump_match_state(&mut self) {
        let filtered = self.state.filtered_matches();
        let upcoming_filtered = self.state.filtered_upcoming();
        let pulse_rows = self.state.pulse_live_rows();
        let active_id: Option<String> = match &self.state.screen {
            Screen::Terminal { match_id: Some(id) } => Some(id.clone()),
            _ => self.state.selected_match_id(),
        };
        let mut lines = Vec::new();
        lines.push("[DUMP] Match state".to_string());
        lines.push(format!(
            "screen={:?} view={:?} league={:?} sort={:?} selected={}",
            self.state.screen,
            self.state.pulse_view,
            self.state.league_mode,
            self.state.sort,
            self.state.selected
        ));
        lines.push(format!(
            "matches_total={} matches_filtered={} upcoming_total={} upcoming_filtered={}",
            self.state.matches.len(),
            filtered.len(),
            self.state.upcoming.len(),
            upcoming_filtered.len()
        ));
        lines.push(format!("pulse_rows_total={}", pulse_rows.len()));
        lines.push(format!(
            "league_ids: pl={:?} ll={:?} wc={:?}",
            self.state.league_pl_ids, self.state.league_ll_ids, self.state.league_wc_ids
        ));
        lines.push(format!(
            "league_ids_extra: bl={:?} cl={:?}",
            self.state.league_bl_ids, self.state.league_cl_ids
        ));
        if let Some(id) = active_id.as_deref() {
            let info = self
                .state
                .match_detail
                .get(id)
                .map(|d| {
                    let err = d
                        .commentary_error
                        .as_deref()
                        .unwrap_or("-");
                    format!(
                        "match_detail: id={id} events={} commentary={} stats={} lineups={} ticker_err={err}",
                        d.events.len(),
                        d.commentary.len(),
                        d.stats.len(),
                        d.lineups.as_ref().map(|l| l.sides.len()).unwrap_or(0),
                    )
                })
                .unwrap_or_else(|| format!("match_detail: id={id} missing"));
            lines.push(info);
        }

        let max_dump = 8usize;
        for (idx, m) in self.state.matches.iter().take(max_dump).enumerate() {
            let matches_mode = self.state.matches_league_mode(m);
            lines.push(format!(
                "match[{idx}] id={} league_id={:?} league_name={} live={} min={} score={}-{} in_mode={}",
                m.id,
                m.league_id,
                m.league_name,
                m.is_live,
                m.minute,
                m.score_home,
                m.score_away,
                matches_mode
            ));
        }
        if self.state.matches.len() > max_dump {
            lines.push(format!(
                "match[...] ({} more not shown)",
                self.state.matches.len().saturating_sub(max_dump)
            ));
        }

        let max_rows = 8usize;
        for (idx, row) in pulse_rows.iter().take(max_rows).enumerate() {
            match row {
                state::PulseLiveRow::Match(match_idx) => {
                    if let Some(m) = self.state.matches.get(*match_idx) {
                        lines.push(format!(
                            "row[{idx}] match id={} league_id={:?} league_name={} live={}",
                            m.id, m.league_id, m.league_name, m.is_live
                        ));
                    } else {
                        lines.push(format!("row[{idx}] match idx={} missing", match_idx));
                    }
                }
                state::PulseLiveRow::Upcoming(up_idx) => {
                    if let Some(u) = self.state.upcoming.get(*up_idx) {
                        lines.push(format!(
                            "row[{idx}] upcoming id={} league_id={:?} league_name={}",
                            u.id, u.league_id, u.league_name
                        ));
                    } else {
                        lines.push(format!("row[{idx}] upcoming idx={} missing", up_idx));
                    }
                }
            }
        }
        if pulse_rows.len() > max_rows {
            lines.push(format!(
                "row[...] ({} more not shown)",
                pulse_rows.len().saturating_sub(max_rows)
            ));
        }

        let max_upcoming = 6usize;
        for (idx, m) in self.state.upcoming.iter().take(max_upcoming).enumerate() {
            let matches_mode = self.state.upcoming_matches_league_mode(m);
            lines.push(format!(
                "upcoming[{idx}] id={} league_id={:?} league_name={} kickoff={} in_mode={}",
                m.id, m.league_id, m.league_name, m.kickoff, matches_mode
            ));
        }
        if self.state.upcoming.len() > max_upcoming {
            lines.push(format!(
                "upcoming[...] ({} more not shown)",
                self.state.upcoming.len().saturating_sub(max_upcoming)
            ));
        }

        for line in lines {
            self.state.push_log(line);
        }
    }

    fn request_match_details(&mut self, announce: bool) {
        // Default: when requesting "details", prefer the full payload (includes commentary when
        // available). Background refreshes use the basic endpoint separately.
        self.request_match_details_with_opts(announce, true, true);
    }

    fn request_match_details_with_opts(
        &mut self,
        announce: bool,
        require_commentary: bool,
        respect_throttle: bool,
    ) {
        let Some(match_id) = self.state.selected_match_id() else {
            if announce {
                self.state.push_log("[INFO] No match selected for details");
            }
            return;
        };
        self.request_match_details_for(&match_id, announce, require_commentary, respect_throttle);
    }

    fn request_match_details_basic_for(&mut self, match_id: &str) {
        if match_id == PLACEHOLDER_MATCH_ID && self.state.placeholder_match_enabled {
            self.state
                .match_detail
                .insert(PLACEHOLDER_MATCH_ID.to_string(), placeholder_match_detail());
            self.state
                .match_detail_cached_at
                .insert(PLACEHOLDER_MATCH_ID.to_string(), SystemTime::now());
            return;
        }
        if let Some(last) = self.last_detail_refresh.get(match_id) {
            if last.elapsed() < self.detail_request_throttle {
                return;
            }
        }

        let is_live = self
            .state
            .matches
            .iter()
            .find(|m| m.id == match_id)
            .map(|m| m.is_live)
            .unwrap_or(false);
        let cached_at = self.state.match_detail_cached_at.get(match_id).copied();
        let has_cached = self.state.match_detail.contains_key(match_id);

        // For non-live matches, avoid re-fetching when cache is fresh.
        if !is_live && has_cached && cache_fresh(cached_at, self.detail_cache_ttl) {
            self.last_detail_refresh
                .insert(match_id.to_string(), Instant::now());
            return;
        }

        let Some(tx) = &self.cmd_tx else {
            return;
        };
        let _ = tx.send(state::ProviderCommand::FetchMatchDetailsBasic {
            fixture_id: match_id.to_string(),
        });
        self.last_detail_refresh
            .insert(match_id.to_string(), Instant::now());
    }

    fn request_match_details_for(
        &mut self,
        match_id: &str,
        announce: bool,
        require_commentary: bool,
        respect_throttle: bool,
    ) {
        if match_id == PLACEHOLDER_MATCH_ID && self.state.placeholder_match_enabled {
            self.state
                .match_detail
                .insert(PLACEHOLDER_MATCH_ID.to_string(), placeholder_match_detail());
            self.state
                .match_detail_cached_at
                .insert(PLACEHOLDER_MATCH_ID.to_string(), SystemTime::now());
            if announce {
                self.state
                    .push_log("[INFO] Placeholder details ready (skipping fetch)");
            }
            return;
        }
        if respect_throttle {
            if let Some(last) = self.last_detail_refresh.get(match_id) {
                if last.elapsed() < self.detail_request_throttle {
                    if announce {
                        self.state.push_log(format!(
                            "[INFO] Match details throttled ({}s)",
                            self.detail_request_throttle.as_secs()
                        ));
                    }
                    return;
                }
            }
        } else if let Some(last) = self.last_detail_refresh.get(match_id) {
            // User-triggered requests can bypass throttling, but avoid bursting duplicate requests
            // within a single render/poll cycle (and while a provider job is likely inflight).
            if last.elapsed() < Duration::from_millis(800) {
                return;
            }
        }
        let is_live = self
            .state
            .matches
            .iter()
            .find(|m| m.id == match_id)
            .map(|m| m.is_live)
            .unwrap_or(false);
        let cached_at = self.state.match_detail_cached_at.get(match_id).copied();
        let has_cached = self.state.match_detail.contains_key(match_id);
        let has_commentary = self
            .state
            .match_detail
            .get(match_id)
            .map(|detail| !detail.commentary.is_empty())
            .unwrap_or(false);
        if !is_live
            && has_cached
            && cache_fresh(cached_at, self.detail_cache_ttl)
            && (!require_commentary || has_commentary)
        {
            if announce {
                self.state
                    .push_log("[INFO] Match details cached (skipping fetch)");
            }
            self.last_detail_refresh
                .insert(match_id.to_string(), Instant::now());
            return;
        }
        let Some(tx) = &self.cmd_tx else {
            if announce {
                self.state
                    .push_log("[INFO] Match details fetch unavailable");
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
        if !self.state.upcoming.is_empty()
            && cache_fresh(self.state.upcoming_cached_at, self.upcoming_cache_ttl)
        {
            if announce {
                self.state
                    .push_log("[INFO] Upcoming cached (skipping fetch)");
            }
            self.last_upcoming_refresh = Instant::now();
            return;
        }
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
        if self.state.analysis_loading {
            if announce {
                self.state.push_log("[INFO] Analysis already loading");
            }
            return;
        }
        let mode = self.state.league_mode;
        if let Some(last) = self.last_analysis_request.get(&mode) {
            if last.elapsed() < self.analysis_request_throttle {
                if announce {
                    self.state.push_log(format!(
                        "[INFO] Analysis throttled ({}s)",
                        self.analysis_request_throttle.as_secs()
                    ));
                }
                return;
            }
        }
        if tx
            .send(state::ProviderCommand::FetchAnalysis { mode })
            .is_err()
        {
            if announce {
                self.state.push_log("[WARN] Analysis request failed");
            }
        } else {
            if announce {
                self.state.push_log("[INFO] Analysis request sent");
            }
            self.last_analysis_request.insert(mode, Instant::now());
            self.state.analysis_loading = true;
            if self.auto_warm_mode != AutoWarmMode::Off {
                self.auto_warm_pending = true;
            }
            if self.prediction_model_auto_warm {
                self.prediction_model_warm_pending = true;
            }
        }
    }

    fn request_prediction_model_warm(&mut self, announce: bool) {
        if !self.prediction_model_auto_warm {
            return;
        }
        if self.state.analysis.is_empty() {
            if announce {
                self.state
                    .push_log("[INFO] No teams loaded yet (fetch Analysis first)");
            }
            return;
        }
        let Some(tx) = &self.cmd_tx else {
            if announce {
                self.state
                    .push_log("[INFO] Prediction model warm unavailable");
            }
            return;
        };

        let league_ids: Vec<u32> = match self.state.league_mode {
            LeagueMode::PremierLeague => self.state.league_pl_ids.clone(),
            LeagueMode::LaLiga => self.state.league_ll_ids.clone(),
            LeagueMode::Bundesliga => self.state.league_bl_ids.clone(),
            LeagueMode::SerieA => self.state.league_sa_ids.clone(),
            LeagueMode::Ligue1 => self.state.league_l1_ids.clone(),
            LeagueMode::ChampionsLeague => self.state.league_cl_ids.clone(),
            LeagueMode::WorldCup => self.state.league_wc_ids.clone(),
        };
        if league_ids.is_empty() {
            return;
        }

        // Skip if all league ids were warmed recently.
        let mut stale = false;
        for league_id in &league_ids {
            let at = self
                .state
                .prediction_model_fetched_at
                .get(league_id)
                .copied();
            let fresh = at
                .and_then(|t| t.elapsed().ok())
                .map(|e| e < self.prediction_model_warm_ttl)
                .unwrap_or(false);
            if !fresh {
                stale = true;
                break;
            }
        }
        if !stale {
            if announce {
                self.state.push_log("[INFO] Prediction model warm (cached)");
            }
            return;
        }

        let mut team_ids: Vec<u32> = self.state.analysis.iter().map(|t| t.id).collect();
        team_ids.sort_unstable();
        team_ids.dedup();

        if tx
            .send(state::ProviderCommand::WarmPredictionModel {
                league_ids,
                team_ids,
            })
            .is_err()
        {
            if announce {
                self.state
                    .push_log("[WARN] Prediction model warm request failed");
            }
        } else if announce {
            self.state.push_log("[INFO] Prediction model warm started");
        }
    }

    fn request_rankings_cache_warm_full(&mut self, announce: bool) {
        let Some(tx) = &self.cmd_tx else {
            if announce {
                self.state
                    .push_log("[INFO] Rankings cache warm unavailable");
            }
            return;
        };
        let mode = self.state.league_mode;
        if tx
            .send(state::ProviderCommand::WarmRankCacheFull { mode })
            .is_err()
        {
            if announce {
                self.state
                    .push_log("[WARN] Rankings cache warm request failed");
            }
        } else {
            if announce {
                self.state.push_log("[INFO] Rankings cache warm started");
            }
            self.state.rankings_loading = true;
            self.state.rankings_progress_current = 0;
            self.state.rankings_progress_total = 0;
            self.state.rankings_progress_message = "Starting rankings".to_string();
        }
    }

    fn request_rankings_cache_warm_missing(&mut self, announce: bool) {
        let Some(tx) = &self.cmd_tx else {
            if announce {
                self.state
                    .push_log("[INFO] Rankings cache warm unavailable");
            }
            return;
        };
        if self.state.rankings_loading {
            if announce {
                self.state.push_log("[INFO] Rankings cache already warming");
            }
            return;
        }
        if self.state.analysis.is_empty() {
            if announce {
                self.state
                    .push_log("[INFO] No teams loaded yet (fetch Analysis first)");
            }
            return;
        }

        let mut team_ids: Vec<u32> = Vec::new();
        let mut player_ids: Vec<u32> = Vec::new();

        // Missing squads for teams (treat empty cached squads as missing).
        for team in &self.state.analysis {
            let cached = self.state.rankings_cache_squads.get(&team.id);
            let missing = cached.map(|players| players.is_empty()).unwrap_or(true);
            if missing {
                team_ids.push(team.id);
            }
        }

        // Missing player details for cached squads.
        for squad in self.state.rankings_cache_squads.values() {
            for p in squad {
                let missing = self
                    .state
                    .rankings_cache_players
                    .get(&p.id)
                    .map(state::player_detail_is_stub)
                    .unwrap_or(true);
                if missing {
                    player_ids.push(p.id);
                }
            }
        }

        team_ids.sort_unstable();
        team_ids.dedup();
        player_ids.sort_unstable();
        player_ids.dedup();

        if team_ids.is_empty() && player_ids.is_empty() {
            if announce {
                self.state.push_log("[INFO] Rankings cache already warm");
            }
            return;
        }
        if announce {
            self.state.push_log(format!(
                "[INFO] Rankings warm missing: {} squads, {} players",
                team_ids.len(),
                player_ids.len()
            ));
        }

        let mode = self.state.league_mode;
        if tx
            .send(state::ProviderCommand::WarmRankCacheMissing {
                mode,
                team_ids,
                player_ids,
            })
            .is_err()
        {
            if announce {
                self.state
                    .push_log("[WARN] Rankings missing-cache request failed");
            }
        } else {
            if announce {
                self.state
                    .push_log("[INFO] Rankings missing-cache request sent");
            }
            self.state.rankings_loading = true;
            self.state.rankings_progress_current = 0;
            self.state.rankings_progress_total = 0;
            self.state.rankings_progress_message = "Warming missing cache".to_string();
        }
    }

    fn clear_rankings_cache(&mut self) {
        self.state.rankings_cache_squads.clear();
        self.state.rankings_cache_players.clear();
        self.state.rankings_cache_squads_at.clear();
        self.state.rankings_cache_players_at.clear();
        self.state.combined_player_cache.clear();
        self.detail_dist_cache = None;
        self.state.rankings.clear();
        self.state.rankings_selected = 0;
        self.state.rankings_dirty = true;
        self.state.rankings_progress_current = 0;
        self.state.rankings_progress_total = 0;
        self.state.rankings_progress_message = "Cache cleared".to_string();
        self.state.rankings_fetched_at = None;
    }

    fn recompute_rankings_from_cache(&mut self) {
        // Preserve current selection by player ID before recomputing
        let prev_player_id = self
            .state
            .rankings_filtered()
            .get(self.state.rankings_selected)
            .map(|entry| entry.player_id);

        let rows = analysis_rankings::compute_role_rankings_from_cache(
            &self.state.analysis,
            &self.state.rankings_cache_squads,
            &self.state.rankings_cache_players,
        );
        if rows.is_empty() {
            self.state.rankings_progress_message =
                "No cached player data yet (warming cache...)".to_string();
        } else {
            self.state.rankings_progress_message =
                format!("Rankings ready (cached: {})", rows.len());
            self.state.rankings_fetched_at = Some(SystemTime::now());
        }
        self.state.rankings = rows;

        // Restore selection to same player if still present, otherwise clamp
        if let Some(player_id) = prev_player_id {
            let filtered = self.state.rankings_filtered();
            if let Some(new_pos) = filtered
                .iter()
                .position(|entry| entry.player_id == player_id)
            {
                self.state.rankings_selected = new_pos;
            } else {
                let total = filtered.len();
                self.state.rankings_selected = if total == 0 {
                    0
                } else {
                    total.saturating_sub(1)
                };
            }
        } else {
            self.state.rankings_selected = 0;
        }

        self.state.rankings_dirty = false;
    }

    fn request_squad(&mut self, team_id: u32, team_name: String, announce: bool, force: bool) {
        if let Some(players) = self.state.rankings_cache_squads.get(&team_id).cloned() {
            let has_players = !players.is_empty();
            self.state.squad = players;
            self.state.squad_selected = 0;
            self.state.squad_loading = false;
            self.state.squad_team = Some(team_name.clone());
            self.state.squad_team_id = Some(team_id);
            self.prefetch_players(self.state.squad.iter().map(|p| p.id).collect());
            if has_players {
                if !force {
                    if announce {
                        self.state.push_log("[INFO] Squad cached (skipping fetch)");
                    }
                    return;
                }
                if announce {
                    self.state
                        .push_log("[INFO] Squad cached (refreshing in background)");
                }
            }
        }
        let Some(tx) = &self.cmd_tx else {
            if announce {
                self.state.push_log("[INFO] Squad fetch unavailable");
            }
            return;
        };
        let cmd = if force {
            state::ProviderCommand::FetchSquadRevalidate { team_id, team_name }
        } else {
            state::ProviderCommand::FetchSquad { team_id, team_name }
        };
        if tx.send(cmd).is_err() {
            if announce {
                self.state.push_log("[WARN] Squad request failed");
            }
        } else {
            if announce {
                self.state.push_log("[INFO] Squad request sent");
            }
            if self.state.squad.is_empty() {
                self.state.squad_loading = true;
                self.state.squad = Vec::new();
                self.state.squad_selected = 0;
            }
        }
    }

    fn request_player_detail(
        &mut self,
        player_id: u32,
        player_name: String,
        announce: bool,
        force: bool,
    ) {
        let Some(tx) = &self.cmd_tx else {
            if announce {
                self.state.push_log("[INFO] Player fetch unavailable");
            }
            return;
        };
        self.state.player_last_id = Some(player_id);
        self.state.player_last_name = Some(player_name.clone());
        let mut cache_hit = false;
        if let Some(cached) = self.state.rankings_cache_players.get(&player_id).cloned() {
            let is_stub = state::player_detail_is_stub(&cached);
            self.state.player_detail = Some(cached);
            self.state.player_loading = false;
            cache_hit = true;
            if !is_stub && !force {
                if announce {
                    self.state.push_log("[INFO] Player cached (skipping fetch)");
                }
                return;
            }
            if announce {
                self.state
                    .push_log("[INFO] Player cached (refreshing in background)");
            }
        }
        if !cache_hit {
            self.state.player_detail = None;
            self.state.player_loading = true;
        }
        let cmd = if force {
            state::ProviderCommand::FetchPlayerRevalidate {
                player_id,
                player_name,
            }
        } else {
            state::ProviderCommand::FetchPlayer {
                player_id,
                player_name,
            }
        };
        if tx.send(cmd).is_err() {
            if announce {
                self.state.push_log("[WARN] Player request failed");
            }
        } else {
            if announce {
                self.state.push_log("[INFO] Player request sent");
            }
            if self.state.player_detail.is_none() {
                self.state.player_loading = true;
            }
        }
    }

    fn prefetch_players(&mut self, player_ids: Vec<u32>) {
        if self.prefetch_players_limit == 0 {
            return;
        }
        let Some(tx) = &self.cmd_tx else {
            return;
        };
        let mut ids: Vec<u32> = player_ids
            .into_iter()
            .filter(|id| {
                let cached = self.state.rankings_cache_players.get(id);
                let is_stub = cached.map(state::player_detail_is_stub).unwrap_or(true);
                cached.is_none() || is_stub
            })
            .collect();
        if ids.is_empty() {
            return;
        }
        ids.sort_unstable();
        ids.dedup();
        ids.truncate(self.prefetch_players_limit);
        let _ = tx.send(state::ProviderCommand::PrefetchPlayers { player_ids: ids });
    }

    fn request_analysis_export(&mut self, announce: bool) {
        let Some(tx) = &self.cmd_tx else {
            if announce {
                self.state.push_log("[INFO] Export unavailable");
            }
            return;
        };

        let stamp = Local::now().format("%Y%m%d_%H%M%S");
        let (mode, prefix) = match self.state.league_mode {
            LeagueMode::PremierLeague => (LeagueMode::PremierLeague, "premier_league"),
            LeagueMode::LaLiga => (LeagueMode::LaLiga, "laliga"),
            LeagueMode::Bundesliga => (LeagueMode::Bundesliga, "bundesliga"),
            LeagueMode::SerieA => (LeagueMode::SerieA, "serie_a"),
            LeagueMode::Ligue1 => (LeagueMode::Ligue1, "ligue1"),
            LeagueMode::ChampionsLeague => (LeagueMode::ChampionsLeague, "champions_league"),
            LeagueMode::WorldCup => (LeagueMode::WorldCup, "worldcup"),
        };
        let path = format!("{prefix}_analysis_{stamp}.xlsx");

        if tx
            .send(state::ProviderCommand::ExportAnalysis {
                path: path.clone(),
                mode,
            })
            .is_err()
        {
            if announce {
                self.state.push_log("[WARN] Export request failed");
            }
        } else if announce {
            self.state
                .push_log(format!("[INFO] Export started: {path}"));
        }
    }

    fn maybe_refresh_upcoming(&mut self) {
        if !matches!(self.state.screen, Screen::Pulse) {
            return;
        }
        if self.last_upcoming_refresh.elapsed() >= self.upcoming_refresh {
            self.request_upcoming(false);
        }
    }

    fn maybe_refresh_match_details(&mut self) {
        const PREFETCH_LIMIT: usize = 3;
        let mut sent = 0usize;

        // If the user has expanded either Commentary or Ticker, refresh full match details for the
        // selected live match (commentary lives behind the full endpoint). Otherwise, background
        // refreshes use the basic endpoint to reduce load.
        let wants_full_details = matches!(self.state.screen, Screen::Terminal { .. })
            && (self.state.terminal_focus == TerminalFocus::Commentary
                || self.state.terminal_detail == Some(TerminalFocus::Commentary)
                || self.state.terminal_detail == Some(TerminalFocus::EventTape));
        let selected_live_id = self
            .state
            .selected_match()
            .filter(|m| m.is_live && m.id != PLACEHOLDER_MATCH_ID)
            .map(|m| m.id.clone());
        if wants_full_details {
            if let Some(match_id) = selected_live_id.as_deref() {
                let last = self.last_detail_refresh.get(match_id);
                let should_fetch = last
                    .map(|t| t.elapsed() >= self.commentary_refresh)
                    .unwrap_or(true);
                if should_fetch {
                    self.request_match_details_for(match_id, false, true, true);
                    sent += 1;
                }
            }
        }

        // Refresh live match stats/lineups periodically.
        let live_ids: Vec<String> = self
            .state
            .matches
            .iter()
            .filter(|m| m.is_live)
            .filter(|m| m.id != PLACEHOLDER_MATCH_ID)
            .map(|m| m.id.clone())
            .collect();

        for match_id in live_ids {
            if sent >= PREFETCH_LIMIT {
                return;
            }
            if wants_full_details && selected_live_id.as_deref() == Some(match_id.as_str()) {
                continue;
            }
            let last = self.last_detail_refresh.get(&match_id);
            let should_fetch = last
                .map(|t| t.elapsed() >= self.detail_refresh)
                .unwrap_or(true);
            if should_fetch {
                self.request_match_details_basic_for(&match_id);
                sent += 1;
            }
        }

        // Warm stats for finished matches (fetch once when missing/stale).
        let finished_ids: Vec<String> = self
            .state
            .matches
            .iter()
            .filter(|m| !m.is_live && m.minute >= 90)
            .filter(|m| m.id != PLACEHOLDER_MATCH_ID)
            .map(|m| m.id.clone())
            .collect();

        for match_id in finished_ids {
            if sent >= PREFETCH_LIMIT {
                return;
            }
            let cached_at = self.state.match_detail_cached_at.get(&match_id).copied();
            let has_cached = self.state.match_detail.contains_key(&match_id);
            if has_cached && cache_fresh(cached_at, self.detail_cache_ttl) {
                continue;
            }
            self.request_match_details_basic_for(&match_id);
            sent += 1;
        }
    }

    fn maybe_auto_warm_rankings(&mut self) {
        if self.auto_warm_mode == AutoWarmMode::Off || !self.auto_warm_pending {
            return;
        }
        if self.state.rankings_loading {
            return;
        }
        if self.state.analysis.is_empty() {
            if !self.state.analysis_loading {
                self.request_analysis(false);
            }
            return;
        }
        match self.auto_warm_mode {
            AutoWarmMode::Missing => self.request_rankings_cache_warm_missing(false),
            AutoWarmMode::Full => self.request_rankings_cache_warm_full(false),
            AutoWarmMode::Off => {}
        }
        self.auto_warm_pending = false;
    }

    fn maybe_auto_warm_prediction_model(&mut self) {
        if !self.prediction_model_auto_warm || !self.prediction_model_warm_pending {
            return;
        }
        if self.state.analysis.is_empty() {
            if !self.state.analysis_loading {
                self.request_analysis(false);
            }
            return;
        }
        self.request_prediction_model_warm(false);
        self.prediction_model_warm_pending = false;
    }

    fn toggle_placeholder_match(&mut self) {
        if self.state.placeholder_match_enabled {
            self.disable_placeholder_match();
        } else {
            self.enable_placeholder_match();
        }
    }

    fn enable_placeholder_match(&mut self) {
        let summary = placeholder_match_summary(self.state.league_mode);
        self.state.matches.retain(|m| m.id != PLACEHOLDER_MATCH_ID);
        self.state.matches.push(summary);
        self.state
            .match_detail
            .insert(PLACEHOLDER_MATCH_ID.to_string(), placeholder_match_detail());
        self.state
            .match_detail_cached_at
            .insert(PLACEHOLDER_MATCH_ID.to_string(), SystemTime::now());
        self.state.win_prob_history.insert(
            PLACEHOLDER_MATCH_ID.to_string(),
            vec![42.0, 48.0, 53.0, 49.0, 57.0, 61.0, 58.0, 56.0],
        );
        self.state.placeholder_match_enabled = true;
        self.state.sort_matches();
        self.state.clamp_selection();
    }

    fn disable_placeholder_match(&mut self) {
        self.state.matches.retain(|m| m.id != PLACEHOLDER_MATCH_ID);
        self.state.match_detail.remove(PLACEHOLDER_MATCH_ID);
        self.state
            .match_detail_cached_at
            .remove(PLACEHOLDER_MATCH_ID);
        self.state.win_prob_history.remove(PLACEHOLDER_MATCH_ID);
        self.state.placeholder_match_enabled = false;
        self.state.sort_matches();
        self.state.clamp_selection();
    }
}

fn cache_fresh(at: Option<std::time::SystemTime>, ttl: Duration) -> bool {
    let Some(at) = at else {
        return false;
    };
    match at.elapsed() {
        Ok(elapsed) => elapsed < ttl,
        Err(_) => false,
    }
}

fn parse_auto_warm_mode() -> AutoWarmMode {
    let Ok(raw) = std::env::var("AUTO_WARM_CACHE") else {
        return AutoWarmMode::Off;
    };
    let normalized = raw.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "" | "0" | "off" | "false" | "no" => AutoWarmMode::Off,
        "full" | "all" => AutoWarmMode::Full,
        "missing" | "1" | "true" | "yes" => AutoWarmMode::Missing,
        _ => AutoWarmMode::Off,
    }
}

fn main() -> io::Result<()> {
    let _ = dotenvy::from_filename(".env.local");
    let _ = dotenvy::from_filename(".env");

    // Lightweight debug mode to inspect FotMob match details without launching the TUI.
    // Example: `cargo run -- --dump-match-details 4837312`
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.first().map(|s| s.as_str()) == Some("--render-screenshots") {
        return render_screenshots();
    }
    if args.first().map(|s| s.as_str()) == Some("--dump-match-details") {
        let match_id = args.get(1).cloned().unwrap_or_default();
        if match_id.trim().is_empty() {
            eprintln!("usage: --dump-match-details <matchId>");
            return Ok(());
        }
        match upcoming_fetch::fetch_match_details_from_fotmob(match_id.trim()) {
            Ok(detail) => {
                println!(
                    "matchId={match_id}\nevents={}\ncommentary={}\ncommentary_error={}\nstats={}\nlineups={}",
                    detail.events.len(),
                    detail.commentary.len(),
                    detail.commentary_error.as_deref().unwrap_or("-"),
                    detail.stats.len(),
                    detail.lineups.as_ref().map(|l| l.sides.len()).unwrap_or(0)
                );
                if !detail.commentary.is_empty() {
                    println!("\ncommentary_head:");
                    for line in detail.commentary.iter().take(5).map(format_commentary_line) {
                        println!("{line}");
                    }
                }
            }
            Err(err) => {
                eprintln!("error: {err}");
            }
        }
        return Ok(());
    }

    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = ratatui::backend::CrosstermBackend::new(stdout);
    let mut terminal = ratatui::Terminal::new(backend)?;

    let (tx, rx) = mpsc::channel();
    let (cmd_tx, cmd_rx) = mpsc::channel();
    feed::spawn_provider(tx.clone(), cmd_rx);
    let pred_tx = spawn_prediction_worker(tx.clone());

    let mut app = App::new(Some(cmd_tx), Some(pred_tx));
    // Restore last used league mode (if any), then load its cached data.
    persist::load_last_league_mode(&mut app.state);
    persist::load_into_state(&mut app.state);
    // Keep upcoming fixtures available even while browsing Live.
    app.request_upcoming(false);
    let res = run_app(&mut terminal, &mut app, rx);

    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;

    // Persist cache on exit.
    persist::save_from_state(&app.state);
    http_cache::flush_http_cache();

    if let Err(err) = res {
        eprintln!("error: {err}");
    }
    Ok(())
}

fn render_screenshots() -> io::Result<()> {
    use ratatui::backend::TestBackend;
    use ratatui::buffer::Buffer;

    fn html_escape(mut s: String) -> String {
        s = s.replace('&', "&amp;");
        s = s.replace('<', "&lt;");
        s = s.replace('>', "&gt;");
        s
    }

    fn xterm_16_rgb(idx: u8) -> (u8, u8, u8) {
        match idx {
            0 => (0x00, 0x00, 0x00),
            1 => (0x80, 0x00, 0x00),
            2 => (0x00, 0x80, 0x00),
            3 => (0x80, 0x80, 0x00),
            4 => (0x00, 0x00, 0x80),
            5 => (0x80, 0x00, 0x80),
            6 => (0x00, 0x80, 0x80),
            7 => (0xc0, 0xc0, 0xc0),
            8 => (0x80, 0x80, 0x80),
            9 => (0xff, 0x00, 0x00),
            10 => (0x00, 0xff, 0x00),
            11 => (0xff, 0xff, 0x00),
            12 => (0x00, 0x00, 0xff),
            13 => (0xff, 0x00, 0xff),
            14 => (0x00, 0xff, 0xff),
            _ => (0xff, 0xff, 0xff),
        }
    }

    fn xterm_256_rgb(idx: u8) -> (u8, u8, u8) {
        if idx < 16 {
            return xterm_16_rgb(idx);
        }
        if (16..=231).contains(&idx) {
            let i = idx - 16;
            let r = i / 36;
            let g = (i % 36) / 6;
            let b = i % 6;
            let map = |v: u8| -> u8 {
                match v {
                    0 => 0,
                    1 => 95,
                    2 => 135,
                    3 => 175,
                    4 => 215,
                    _ => 255,
                }
            };
            return (map(r), map(g), map(b));
        }
        let v = 8u8.saturating_add(10u8.saturating_mul(idx.saturating_sub(232)));
        (v, v, v)
    }

    fn color_to_css(color: Color) -> Option<String> {
        let (r, g, b) = match color {
            Color::Reset => return None,
            Color::Black => (0x00, 0x00, 0x00),
            Color::Red => (0xcd, 0x31, 0x31),
            Color::Green => (0x0d, 0xbc, 0x79),
            Color::Yellow => (0xe5, 0xe5, 0x10),
            Color::Blue => (0x24, 0x71, 0xdb),
            Color::Magenta => (0xbc, 0x3f, 0xbc),
            Color::Cyan => (0x11, 0xa8, 0xcd),
            Color::Gray => (0xe5, 0xe5, 0xe5),
            Color::DarkGray => (0x66, 0x66, 0x66),
            Color::LightRed => (0xf1, 0x4c, 0x4c),
            Color::LightGreen => (0x23, 0xd1, 0x8b),
            Color::LightYellow => (0xf5, 0xf5, 0x43),
            Color::LightBlue => (0x3b, 0x8e, 0xea),
            Color::LightMagenta => (0xd6, 0x70, 0xd6),
            Color::LightCyan => (0x29, 0xb8, 0xdb),
            Color::White => (0xff, 0xff, 0xff),
            Color::Indexed(idx) => xterm_256_rgb(idx),
            Color::Rgb(r, g, b) => (r, g, b),
        };
        Some(format!("rgb({r},{g},{b})"))
    }

    fn buffer_to_html(buf: &Buffer, title: &str) -> String {
        let area = buf.area;
        let mut out = String::with_capacity((area.width as usize) * (area.height as usize) * 32);
        out.push_str("<!doctype html><html><head><meta charset=\"utf-8\">");
        out.push_str("<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">");
        out.push_str("<style>");
        out.push_str(
            r#"
            :root { --bg: rgb(6,9,14); --fg: rgb(228,234,244); }
            html, body { margin: 0; padding: 0; background: var(--bg); color: var(--fg); }
            .screen {
              display: inline-block;
              background: var(--bg);
              font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", monospace;
              font-variant-ligatures: none;
              font-size: 16px;
              line-height: 16px;
              white-space: pre;
            }
            .row { height: 16px; }
            .cell {
              display: inline-block;
              width: 1ch;
              height: 16px;
              overflow: hidden;
              vertical-align: top;
            }
            "#,
        );
        out.push_str("</style>");
        out.push_str("<title>");
        out.push_str(&html_escape(title.to_string()));
        out.push_str("</title></head><body>");
        out.push_str("<div class=\"screen\" role=\"img\" aria-label=\"");
        out.push_str(&html_escape(title.to_string()));
        out.push_str("\">");

        for y in 0..area.height {
            out.push_str("<div class=\"row\">");
            for x in 0..area.width {
                let cell = buf.get(x, y);
                let symbol = cell.symbol();
                let symbol = if symbol.is_empty() { " " } else { symbol };

                let mut style = String::new();
                if let Some(fg) = color_to_css(cell.fg) {
                    style.push_str("color:");
                    style.push_str(&fg);
                    style.push(';');
                }
                if let Some(bg) = color_to_css(cell.bg) {
                    style.push_str("background:");
                    style.push_str(&bg);
                    style.push(';');
                }
                if cell.modifier.contains(Modifier::BOLD) {
                    style.push_str("font-weight:700;");
                }
                if cell.modifier.contains(Modifier::ITALIC) {
                    style.push_str("font-style:italic;");
                }
                if cell.modifier.contains(Modifier::UNDERLINED) {
                    style.push_str("text-decoration:underline;");
                }
                if cell.modifier.contains(Modifier::DIM) {
                    style.push_str("opacity:0.8;");
                }

                out.push_str("<span class=\"cell\"");
                if !style.is_empty() {
                    out.push_str(" style=\"");
                    out.push_str(&style);
                    out.push('"');
                }
                out.push('>');
                out.push_str(&html_escape(symbol.to_string()));
                out.push_str("</span>");
            }
            out.push_str("</div>");
        }

        out.push_str("</div></body></html>");
        out
    }

    fn seed_demo(app: &mut App) {
        app.enable_placeholder_match();

        app.state.upcoming = vec![
            state::UpcomingMatch {
                id: "up-1".to_string(),
                league_id: None,
                league_name: "Premier League".to_string(),
                round: "Matchday 24".to_string(),
                kickoff: "Sat 12:30".to_string(),
                home_team_id: None,
                away_team_id: None,
                home: "Northbridge".to_string(),
                away: "Southport".to_string(),
            },
            state::UpcomingMatch {
                id: "up-2".to_string(),
                league_id: None,
                league_name: "Premier League".to_string(),
                round: "Matchday 24".to_string(),
                kickoff: "Sat 15:00".to_string(),
                home_team_id: None,
                away_team_id: None,
                home: "Kings FC".to_string(),
                away: "Harbor City".to_string(),
            },
            state::UpcomingMatch {
                id: "up-3".to_string(),
                league_id: None,
                league_name: "Premier League".to_string(),
                round: "Matchday 24".to_string(),
                kickoff: "Sun 16:30".to_string(),
                home_team_id: None,
                away_team_id: None,
                home: "Rovers".to_string(),
                away: "United".to_string(),
            },
        ];

        app.state
            .push_log("boot: offline demo seed (placeholder match)".to_string());
        app.state
            .push_log("hint: press ? for keys, p to toggle placeholder".to_string());
        app.state
            .push_log("provider: disabled (no network)".to_string());

        // Populate additional screens so UI iteration doesn't require network access.
        app.state.analysis = vec![
            state::TeamAnalysis {
                id: 1,
                name: "Argentina".to_string(),
                confed: state::Confederation::CONMEBOL,
                host: false,
                fifa_rank: Some(1),
                fifa_points: Some(1860),
                fifa_updated: Some("2025-12-19".to_string()),
            },
            state::TeamAnalysis {
                id: 2,
                name: "France".to_string(),
                confed: state::Confederation::UEFA,
                host: false,
                fifa_rank: Some(2),
                fifa_points: Some(1840),
                fifa_updated: Some("2025-12-19".to_string()),
            },
            state::TeamAnalysis {
                id: 3,
                name: "USA".to_string(),
                confed: state::Confederation::CONCACAF,
                host: true,
                fifa_rank: Some(11),
                fifa_points: Some(1675),
                fifa_updated: Some("2025-12-19".to_string()),
            },
            state::TeamAnalysis {
                id: 4,
                name: "Japan".to_string(),
                confed: state::Confederation::AFC,
                host: false,
                fifa_rank: Some(19),
                fifa_points: Some(1612),
                fifa_updated: Some("2025-12-19".to_string()),
            },
            state::TeamAnalysis {
                id: 5,
                name: "Nigeria".to_string(),
                confed: state::Confederation::CAF,
                host: false,
                fifa_rank: Some(28),
                fifa_points: Some(1540),
                fifa_updated: Some("2025-12-19".to_string()),
            },
            state::TeamAnalysis {
                id: 6,
                name: "New Zealand".to_string(),
                confed: state::Confederation::OFC,
                host: false,
                fifa_rank: Some(101),
                fifa_points: Some(1202),
                fifa_updated: Some("2025-12-19".to_string()),
            },
        ];

        app.state.rankings = vec![
            state::RoleRankingEntry {
                role: RoleCategory::Attacker,
                player_id: 1001,
                player_name: "K. Rook".to_string(),
                team_id: 3,
                team_name: "USA".to_string(),
                club: "Northbridge".to_string(),
                attack_score: 2.43,
                defense_score: 0.12,
                rating: Some(7.42),
                attack_factors: vec![
                    state::RankFactor {
                        label: "xG".to_string(),
                        z: 1.40,
                        weight: 0.55,
                        raw: Some(0.62),
                        pct: Some(88.0),
                        source: "All comps".to_string(),
                    },
                    state::RankFactor {
                        label: "Shots".to_string(),
                        z: 1.05,
                        weight: 0.30,
                        raw: Some(3.1),
                        pct: Some(81.0),
                        source: "Per 90".to_string(),
                    },
                ],
                defense_factors: vec![],
            },
            state::RoleRankingEntry {
                role: RoleCategory::Midfielder,
                player_id: 1002,
                player_name: "T. Vale".to_string(),
                team_id: 2,
                team_name: "France".to_string(),
                club: "Harbor City".to_string(),
                attack_score: 1.02,
                defense_score: 1.88,
                rating: Some(7.11),
                attack_factors: vec![],
                defense_factors: vec![
                    state::RankFactor {
                        label: "Tackles".to_string(),
                        z: 1.22,
                        weight: 0.45,
                        raw: Some(2.6),
                        pct: Some(84.0),
                        source: "Per 90".to_string(),
                    },
                    state::RankFactor {
                        label: "Interceptions".to_string(),
                        z: 0.92,
                        weight: 0.35,
                        raw: Some(1.8),
                        pct: Some(76.0),
                        source: "Per 90".to_string(),
                    },
                ],
            },
            state::RoleRankingEntry {
                role: RoleCategory::Defender,
                player_id: 1003,
                player_name: "M. Holt".to_string(),
                team_id: 1,
                team_name: "Argentina".to_string(),
                club: "Rovers".to_string(),
                attack_score: 0.44,
                defense_score: 2.05,
                rating: Some(7.29),
                attack_factors: vec![],
                defense_factors: vec![state::RankFactor {
                    label: "Duels won".to_string(),
                    z: 1.10,
                    weight: 0.55,
                    raw: Some(7.2),
                    pct: Some(83.0),
                    source: "All comps".to_string(),
                }],
            },
            state::RoleRankingEntry {
                role: RoleCategory::Goalkeeper,
                player_id: 1004,
                player_name: "A. Stone".to_string(),
                team_id: 4,
                team_name: "Japan".to_string(),
                club: "United".to_string(),
                attack_score: 0.05,
                defense_score: 1.52,
                rating: Some(7.05),
                attack_factors: vec![],
                defense_factors: vec![state::RankFactor {
                    label: "Save %".to_string(),
                    z: 0.95,
                    weight: 0.60,
                    raw: Some(74.0),
                    pct: Some(79.0),
                    source: "All comps".to_string(),
                }],
            },
        ];

        app.state.squad_team = Some("USA".to_string());
        app.state.squad_team_id = Some(3);
        app.state.squad = vec![
            state::SquadPlayer {
                id: 1001,
                name: "K. Rook".to_string(),
                role: "FW".to_string(),
                club: "Northbridge".to_string(),
                age: Some(24),
                height: Some(182),
                shirt_number: Some(9),
                market_value: Some(38_000_000),
            },
            state::SquadPlayer {
                id: 1002,
                name: "T. Vale".to_string(),
                role: "MF".to_string(),
                club: "Harbor City".to_string(),
                age: Some(27),
                height: Some(176),
                shirt_number: Some(8),
                market_value: Some(24_000_000),
            },
            state::SquadPlayer {
                id: 1003,
                name: "M. Holt".to_string(),
                role: "DF".to_string(),
                club: "Rovers".to_string(),
                age: Some(29),
                height: Some(188),
                shirt_number: Some(4),
                market_value: Some(18_500_000),
            },
            state::SquadPlayer {
                id: 1004,
                name: "A. Stone".to_string(),
                role: "GK".to_string(),
                club: "United".to_string(),
                age: Some(31),
                height: Some(191),
                shirt_number: Some(1),
                market_value: Some(6_000_000),
            },
        ];

        // Player detail demo (enough for the screen layout to look realistic).
        let player = state::PlayerDetail {
            id: 1001,
            name: "K. Rook".to_string(),
            team: Some("USA".to_string()),
            position: Some("Forward".to_string()),
            age: Some("24".to_string()),
            country: Some("USA".to_string()),
            height: Some("182 cm".to_string()),
            preferred_foot: Some("Right".to_string()),
            shirt: Some("9".to_string()),
            market_value: Some("EUR 38.0M".to_string()),
            contract_end: Some("2028-06-30".to_string()),
            birth_date: Some("2001-03-04".to_string()),
            status: Some("Available".to_string()),
            injury_info: None,
            international_duty: Some("Not called up".to_string()),
            positions: vec!["FW".to_string(), "RW".to_string()],
            all_competitions: vec![
                state::PlayerStatItem {
                    title: "Minutes".to_string(),
                    value: "1450".to_string(),
                    percentile_rank: Some(62.0),
                    percentile_rank_per90: None,
                },
                state::PlayerStatItem {
                    title: "Goals".to_string(),
                    value: "12".to_string(),
                    percentile_rank: Some(90.0),
                    percentile_rank_per90: Some(92.0),
                },
                state::PlayerStatItem {
                    title: "Assists".to_string(),
                    value: "5".to_string(),
                    percentile_rank: Some(72.0),
                    percentile_rank_per90: Some(70.0),
                },
                state::PlayerStatItem {
                    title: "xG".to_string(),
                    value: "10.1".to_string(),
                    percentile_rank: Some(88.0),
                    percentile_rank_per90: Some(89.0),
                },
            ],
            all_competitions_season: Some("2025/26".to_string()),
            main_league: Some(state::PlayerLeagueStats {
                league_name: "Premier League".to_string(),
                season: "2025/26".to_string(),
                stats: vec![
                    state::PlayerStatItem {
                        title: "Minutes".to_string(),
                        value: "1450".to_string(),
                        percentile_rank: None,
                        percentile_rank_per90: None,
                    },
                    state::PlayerStatItem {
                        title: "Goals".to_string(),
                        value: "10".to_string(),
                        percentile_rank: None,
                        percentile_rank_per90: None,
                    },
                    state::PlayerStatItem {
                        title: "Shots".to_string(),
                        value: "68".to_string(),
                        percentile_rank: None,
                        percentile_rank_per90: None,
                    },
                ],
            }),
            top_stats: vec![
                state::PlayerStatItem {
                    title: "Shots on target %".to_string(),
                    value: "46.0".to_string(),
                    percentile_rank: Some(74.0),
                    percentile_rank_per90: None,
                },
                state::PlayerStatItem {
                    title: "Goals per 90".to_string(),
                    value: "0.74".to_string(),
                    percentile_rank: Some(91.0),
                    percentile_rank_per90: Some(91.0),
                },
            ],
            season_groups: vec![state::PlayerStatGroup {
                title: "Passing".to_string(),
                items: vec![state::PlayerStatItem {
                    title: "Accurate passes %".to_string(),
                    value: "79.0".to_string(),
                    percentile_rank: Some(58.0),
                    percentile_rank_per90: None,
                }],
            }],
            season_performance: vec![state::PlayerSeasonPerformanceGroup {
                title: "Shooting".to_string(),
                items: vec![
                    state::PlayerSeasonPerformanceItem {
                        title: "Shots".to_string(),
                        total: "68".to_string(),
                        per90: Some("3.1".to_string()),
                        percentile_rank: Some(81.0),
                        percentile_rank_per90: Some(77.0),
                    },
                    state::PlayerSeasonPerformanceItem {
                        title: "xG".to_string(),
                        total: "10.1".to_string(),
                        per90: Some("0.62".to_string()),
                        percentile_rank: Some(88.0),
                        percentile_rank_per90: Some(89.0),
                    },
                ],
            }],
            traits: Some(state::PlayerTraitGroup {
                title: "Traits".to_string(),
                items: vec![
                    state::PlayerTraitItem {
                        title: "Finishing".to_string(),
                        value: 0.86,
                    },
                    state::PlayerTraitItem {
                        title: "Positioning".to_string(),
                        value: 0.74,
                    },
                ],
            }),
            recent_matches: vec![
                state::PlayerMatchStat {
                    opponent: "OMEGA".to_string(),
                    league: "PL".to_string(),
                    date: "2026-02-01".to_string(),
                    goals: 1,
                    assists: 0,
                    rating: Some("7.8".to_string()),
                },
                state::PlayerMatchStat {
                    opponent: "Rovers".to_string(),
                    league: "PL".to_string(),
                    date: "2026-01-25".to_string(),
                    goals: 0,
                    assists: 1,
                    rating: Some("7.1".to_string()),
                },
            ],
            season_breakdown: vec![
                state::PlayerSeasonTournamentStat {
                    league: "Premier League".to_string(),
                    season: "2025/26".to_string(),
                    appearances: "21".to_string(),
                    goals: "10".to_string(),
                    assists: "5".to_string(),
                    rating: "7.42".to_string(),
                },
                state::PlayerSeasonTournamentStat {
                    league: "Cup".to_string(),
                    season: "2025/26".to_string(),
                    appearances: "4".to_string(),
                    goals: "2".to_string(),
                    assists: "0".to_string(),
                    rating: "7.11".to_string(),
                },
            ],
            career_sections: vec![state::PlayerCareerSection {
                title: "club career".to_string(),
                entries: vec![state::PlayerCareerEntry {
                    team: "Northbridge".to_string(),
                    start_date: Some("2022-07-01".to_string()),
                    end_date: None,
                    appearances: Some("84".to_string()),
                    goals: Some("37".to_string()),
                    assists: Some("18".to_string()),
                }],
            }],
            trophies: vec![state::PlayerTrophyEntry {
                team: "Northbridge".to_string(),
                league: "Cup".to_string(),
                seasons_won: vec!["2024/25".to_string()],
                seasons_runner_up: vec![],
            }],
        };
        app.state.player_detail = Some(player.clone());
        app.state.player_last_id = Some(player.id);
        app.state.player_last_name = Some(player.name.clone());
        app.state
            .combined_player_cache
            .insert(player.id, player.clone());
        for i in 0..8u32 {
            let mut other = player.clone();
            other.id = 2000 + i;
            other.name = format!("Demo Player {i}");
            if let Some(item) = other
                .all_competitions
                .iter_mut()
                .find(|s| s.title == "Goals")
            {
                item.value = format!("{}", 5 + (i % 6));
            }
            app.state.combined_player_cache.insert(other.id, other);
        }
    }

    fn render_shot(
        name: &str,
        width: u16,
        height: u16,
        prep: impl FnOnce(&mut App),
    ) -> io::Result<()> {
        let mut app = App::new(None, None);
        seed_demo(&mut app);
        prep(&mut app);

        let mut terminal = Terminal::new(TestBackend::new(width, height))
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        terminal
            .draw(|f| ui(f, &mut app))
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        let buf = terminal.backend().buffer().clone();
        let html = buffer_to_html(&buf, name);
        let dir = std::path::Path::new("target/screenshots");
        std::fs::create_dir_all(dir)?;
        let path = dir.join(format!("{name}.html"));
        std::fs::write(&path, html)?;
        eprintln!("wrote {}", path.display());
        Ok(())
    }

    let width = 140;
    let height = 44;

    render_shot("pulse_live", width, height, |app| {
        app.state.screen = Screen::Pulse;
        app.state.pulse_view = PulseView::Live;
        app.state.selected = 0;
    })?;

    render_shot("pulse_live_select_upcoming", width, height, |app| {
        app.state.screen = Screen::Pulse;
        app.state.pulse_view = PulseView::Live;
        app.state.selected = 1;
    })?;

    render_shot("pulse_upcoming", width, height, |app| {
        app.state.screen = Screen::Pulse;
        app.state.pulse_view = PulseView::Upcoming;
        app.state.upcoming_scroll = 0;
    })?;

    render_shot("pulse_help", width, height, |app| {
        app.state.screen = Screen::Pulse;
        app.state.pulse_view = PulseView::Live;
        app.state.selected = 0;
        app.state.help_overlay = true;
    })?;

    render_shot("terminal_matchlist", width, height, |app| {
        app.state.screen = Screen::Terminal {
            match_id: Some(PLACEHOLDER_MATCH_ID.to_string()),
        };
        app.state.terminal_focus = TerminalFocus::MatchList;
    })?;

    render_shot("terminal_pitch", width, height, |app| {
        app.state.screen = Screen::Terminal {
            match_id: Some(PLACEHOLDER_MATCH_ID.to_string()),
        };
        app.state.terminal_focus = TerminalFocus::Pitch;
    })?;

    render_shot("terminal_ticker", width, height, |app| {
        app.state.screen = Screen::Terminal {
            match_id: Some(PLACEHOLDER_MATCH_ID.to_string()),
        };
        app.state.terminal_focus = TerminalFocus::EventTape;
    })?;

    render_shot("terminal_commentary", width, height, |app| {
        app.state.screen = Screen::Terminal {
            match_id: Some(PLACEHOLDER_MATCH_ID.to_string()),
        };
        app.state.terminal_focus = TerminalFocus::Commentary;
    })?;

    render_shot("terminal_stats", width, height, |app| {
        app.state.screen = Screen::Terminal {
            match_id: Some(PLACEHOLDER_MATCH_ID.to_string()),
        };
        app.state.terminal_focus = TerminalFocus::Stats;
    })?;

    render_shot("terminal_lineups", width, height, |app| {
        app.state.screen = Screen::Terminal {
            match_id: Some(PLACEHOLDER_MATCH_ID.to_string()),
        };
        app.state.terminal_focus = TerminalFocus::Lineups;
    })?;

    render_shot("terminal_prediction", width, height, |app| {
        app.state.screen = Screen::Terminal {
            match_id: Some(PLACEHOLDER_MATCH_ID.to_string()),
        };
        app.state.terminal_focus = TerminalFocus::Prediction;
    })?;

    render_shot("terminal_console", width, height, |app| {
        app.state.screen = Screen::Terminal {
            match_id: Some(PLACEHOLDER_MATCH_ID.to_string()),
        };
        app.state.terminal_focus = TerminalFocus::Console;
    })?;

    render_shot("terminal_detail_overlay", width, height, |app| {
        app.state.screen = Screen::Terminal {
            match_id: Some(PLACEHOLDER_MATCH_ID.to_string()),
        };
        app.state.terminal_focus = TerminalFocus::Prediction;
        app.state.terminal_detail = Some(TerminalFocus::Prediction);
    })?;

    render_shot("analysis_teams", width, height, |app| {
        app.state.screen = Screen::Analysis;
        app.state.analysis_tab = state::AnalysisTab::Teams;
        app.state.analysis_selected = 0;
    })?;

    render_shot("analysis_rankings", width, height, |app| {
        app.state.screen = Screen::Analysis;
        app.state.analysis_tab = state::AnalysisTab::RoleRankings;
        app.state.rankings_role = RoleCategory::Attacker;
        app.state.rankings_metric = state::RankMetric::Attacking;
        app.state.rankings_selected = 0;
    })?;

    render_shot("analysis_rankings_search", width, height, |app| {
        app.state.screen = Screen::Analysis;
        app.state.analysis_tab = state::AnalysisTab::RoleRankings;
        app.state.rankings_role = RoleCategory::Attacker;
        app.state.rankings_metric = state::RankMetric::Attacking;
        app.state.rankings_selected = 0;
        app.state.rankings_search_active = true;
        app.state.rankings_search = "rook".to_string();
    })?;

    render_shot("squad_table", width, height, |app| {
        app.state.screen = Screen::Squad;
        app.state.squad_selected = 0;
    })?;

    render_shot("player_detail", width, height, |app| {
        app.state.screen = Screen::PlayerDetail;
        app.state.player_detail_section = 0;
        app.state.player_detail_expanded = false;
    })?;

    render_shot("player_detail_expanded", width, height, |app| {
        app.state.screen = Screen::PlayerDetail;
        app.state.player_detail_section = 1;
        app.state.player_detail_expanded = true;
    })?;

    render_shot("analysis_empty", width, height, |app| {
        app.state.screen = Screen::Analysis;
        app.state.analysis_tab = state::AnalysisTab::Teams;
        app.state.analysis.clear();
    })?;

    render_shot("squad_empty", width, height, |app| {
        app.state.screen = Screen::Squad;
        app.state.squad.clear();
    })?;

    Ok(())
}

fn run_app<B: Backend>(
    terminal: &mut Terminal<B>,
    app: &mut App,
    rx: mpsc::Receiver<state::Delta>,
) -> io::Result<()> {
    let poll_rate = Duration::from_millis(250);
    let heartbeat_rate = Duration::from_secs(1);
    let animation_rate = Duration::from_millis(
        std::env::var("UI_ANIMATION_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(120)
            .clamp(60, 400),
    );
    let mut last_draw = Instant::now() - heartbeat_rate;
    let mut needs_redraw = true;

    loop {
        let mut changed = false;
        // Avoid long stalls when a background warm/prefetch streams lots of deltas.
        // Bound per-tick work so navigation/input stays responsive.
        let max_deltas_per_tick = std::env::var("UI_MAX_DELTAS_PER_TICK")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(250)
            .clamp(25, 50_000);
        let delta_time_budget = Duration::from_millis(
            std::env::var("UI_DELTA_BUDGET_MS")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(12)
                .clamp(2, 200),
        );

        let drain_started = Instant::now();
        let mut drained = 0usize;
        while let Ok(delta) = rx.try_recv() {
            // Cache-warm and prefetch can stream lots of updates; track them so we can debounce
            // expensive recomputes while keeping the UI responsive.
            match &delta {
                state::Delta::CacheSquad { .. }
                | state::Delta::CachePlayerDetail(_)
                | state::Delta::SetAnalysis { .. } => {
                    app.rankings_update_counter = app.rankings_update_counter.saturating_add(1);
                }
                state::Delta::ComputedPredictions { generation, .. } => {
                    if *generation == app.state.prediction_compute_generation {
                        app.pred_inflight = false;
                    }
                }
                _ => {}
            }
            apply_delta(&mut app.state, delta);
            changed = true;

            drained = drained.saturating_add(1);
            if drained >= max_deltas_per_tick || drain_started.elapsed() >= delta_time_budget {
                // Still more work waiting in the channel; render and poll input instead of
                // freezing until the backlog is drained.
                needs_redraw = true;
                break;
            }
        }
        if let Some(ids) = app.state.squad_prefetch_pending.take() {
            app.prefetch_players(ids);
        }

        // Debounced rankings recompute: progressive updates during warm without freezing input.
        if matches!(app.state.screen, Screen::Analysis)
            && app.state.analysis_tab == state::AnalysisTab::RoleRankings
            && app.state.rankings_dirty
            && !app.state.analysis.is_empty()
        {
            let now = Instant::now();
            if !app.state.rankings_loading {
                app.recompute_rankings_from_cache();
                app.rankings_last_recompute = now;
                app.rankings_update_counter = 0;
                changed = true;
            } else {
                let due = now.duration_since(app.rankings_last_recompute)
                    >= app.rankings_recompute_interval;
                let enough_updates = app.rankings_update_counter
                    >= app.rankings_recompute_min_updates
                    || app.state.rankings.is_empty();
                if due && enough_updates {
                    app.recompute_rankings_from_cache();
                    app.rankings_last_recompute = now;
                    app.rankings_update_counter = 0;
                    changed = true;
                }
            }
        }

        // Debounced win-prob recompute: avoid per-player recompute during warm/prefetch.
        {
            let in_prediction_context = matches!(app.state.screen, Screen::Pulse)
                || matches!(app.state.screen, Screen::Terminal { .. });
            if in_prediction_context && app.state.predictions_dirty {
                let now = Instant::now();
                if now.duration_since(app.predictions_last_recompute)
                    >= app.predictions_recompute_interval
                {
                    if let Some(tx) = app.pred_tx.as_ref() {
                        if !app.pred_inflight {
                            app.pred_generation = app.pred_generation.wrapping_add(1).max(1);
                            let generation = app.pred_generation;
                            app.state.prediction_compute_generation = generation;
                            let snapshot = PredictionSnapshot {
                                matches: app.state.matches.clone(),
                                upcoming: app.state.upcoming.clone(),
                                match_detail: app.state.match_detail.clone(),
                                combined_player_cache: app.state.combined_player_cache.clone(),
                                rankings_cache_squads: app.state.rankings_cache_squads.clone(),
                                analysis: app.state.analysis.clone(),
                                league_params: app.state.league_params.clone(),
                                elo_by_league: app.state.elo_by_league.clone(),
                                prematch_locked: app.state.prematch_locked.clone(),
                            };
                            let _ = tx.send(PredictionCommand::Compute {
                                generation,
                                snapshot,
                            });
                            app.pred_inflight = true;
                            app.state.predictions_dirty = false;
                            app.predictions_last_recompute = now;
                        }
                    } else {
                        // No worker (e.g. screenshot mode): skip background compute.
                        app.state.predictions_dirty = false;
                        app.predictions_last_recompute = now;
                    }
                }
            }
        }
        let export_was_active = app.state.export.active;
        app.state.maybe_clear_export(Instant::now());
        if export_was_active != app.state.export.active {
            changed = true;
        }

        app.maybe_refresh_upcoming();
        app.maybe_refresh_match_details();
        app.maybe_auto_warm_rankings();
        app.maybe_auto_warm_prediction_model();
        app.maybe_hover_prefetch_match_details();

        if app.ui_last_anim_tick.elapsed() >= animation_rate {
            let elapsed_ms = app.ui_last_anim_tick.elapsed().as_millis();
            let step_ms = animation_rate.as_millis().max(1);
            let steps = (elapsed_ms / step_ms).max(1) as u64;
            app.ui_anim_frame = app.ui_anim_frame.wrapping_add(steps);
            app.ui_last_anim_tick = Instant::now();
            needs_redraw = true;
        }

        if needs_redraw || changed || last_draw.elapsed() >= heartbeat_rate {
            terminal.draw(|f| ui(f, app))?;
            last_draw = Instant::now();
            needs_redraw = false;
        }

        if event::poll(poll_rate)?
            && let Event::Key(key) = event::read()?
            && key.kind == KeyEventKind::Press
        {
            app.on_key(key);
            needs_redraw = true;
        }

        if app.should_quit {
            return Ok(());
        }
    }
}

fn ui(frame: &mut Frame, app: &mut App) {
    let anim = ui_anim_from_frame(app.ui_anim_frame);
    let _uptime = app.ui_anim_started_at.elapsed();
    // Force a consistent dark background across the entire frame.
    frame.render_widget(
        Block::default().style(Style::default().bg(theme_bg())),
        frame.size(),
    );

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Min(1),
            Constraint::Length(1),
        ])
        .split(frame.size());

    let header = Paragraph::new(header_styled(&app.state, anim))
        .style(Style::default().bg(theme_chrome_bg()));
    frame.render_widget(header, chunks[0]);

    match app.state.screen {
        Screen::Pulse => render_pulse(frame, chunks[1], &app.state, anim),
        Screen::Terminal { .. } => render_terminal(frame, chunks[1], &app.state, anim),
        Screen::Analysis => render_analysis(frame, chunks[1], &app.state, anim),
        Screen::Squad => render_squad(frame, chunks[1], &app.state, anim),
        Screen::PlayerDetail => render_player_detail(frame, chunks[1], app, anim),
    }

    let footer = Paragraph::new(footer_styled(&app.state, anim))
        .style(Style::default().bg(theme_chrome_bg()))
        .block(
            Block::default()
                .borders(Borders::TOP)
                .border_style(Style::default().fg(theme_border_dim()))
                .style(Style::default().bg(theme_chrome_bg())),
        );
    frame.render_widget(footer, chunks[2]);

    if app.state.export.active {
        render_export_overlay(frame, frame.size(), &app.state, anim);
    }
    if app.state.help_overlay {
        render_help_overlay(frame, frame.size(), anim);
    }
    if app.state.terminal_detail.is_some() {
        render_terminal_detail_overlay(frame, frame.size(), &app.state, anim);
    }
}

fn header_styled(state: &AppState, anim: UiAnim) -> Line<'static> {
    let sep = Span::styled(ui_theme().glyphs.divider, Style::default().fg(theme_border_dim()));

    match state.screen {
        Screen::Pulse => {
            let mut spans = vec![
                Span::styled(
                    "WC26 PULSE",
                    Style::default()
                        .fg(theme_accent())
                        .add_modifier(Modifier::BOLD),
                ),
                sep.clone(),
                Span::styled(
                    league_label(state.league_mode).to_string(),
                    Style::default().fg(theme_accent_2()),
                ),
                sep.clone(),
                Span::styled(
                    pulse_view_label(state.pulse_view).to_string(),
                    Style::default().fg(Color::LightMagenta),
                ),
                sep.clone(),
                Span::styled("Sort: ", Style::default().fg(theme_muted())),
                Span::styled(
                    sort_label(state.sort).to_string(),
                    Style::default().fg(theme_success()),
                ),
            ];
            if state.pulse_view == PulseView::Live {
                spans.push(sep.clone());
                spans.push(Span::styled(
                    format!("{} LIVE", ui_live_dot(anim)),
                    Style::default().fg(if anim.blink_on {
                        theme_success()
                    } else {
                        theme_muted()
                    }),
                ));
            }
            Line::from(spans)
        }
        Screen::Terminal { .. } => Line::from(Span::styled(
            "WC26 TERMINAL",
            Style::default()
                .fg(theme_accent())
                .add_modifier(Modifier::BOLD),
        )),
        Screen::Analysis => {
            let updated = state.analysis_updated.as_deref().unwrap_or("-");
            let status_label = if state.analysis_loading {
                format!("{} LOADING", ui_spinner(anim))
            } else {
                "READY".to_string()
            };
            let status_color = if state.analysis_loading {
                theme_warn()
            } else {
                theme_success()
            };
            let tab = match state.analysis_tab {
                state::AnalysisTab::Teams => "TEAMS",
                state::AnalysisTab::RoleRankings => "RANKINGS",
            };
            let fetched = match state.analysis_tab {
                state::AnalysisTab::Teams => format_fetched_at(state.analysis_fetched_at),
                state::AnalysisTab::RoleRankings => format_fetched_at(state.rankings_fetched_at),
            };
            Line::from(vec![
                Span::styled(
                    "WC26 ANALYSIS",
                    Style::default()
                        .fg(theme_accent())
                        .add_modifier(Modifier::BOLD),
                ),
                sep.clone(),
                Span::styled(
                    league_label(state.league_mode).to_string(),
                    Style::default().fg(theme_accent_2()),
                ),
                sep.clone(),
                Span::styled("Tab: ", Style::default().fg(theme_muted())),
                Span::styled(tab.to_string(), Style::default().fg(Color::LightMagenta)),
                sep.clone(),
                Span::styled(
                    format!("Teams: {}", state.analysis.len()),
                    Style::default().fg(theme_text()),
                ),
                sep.clone(),
                Span::styled(format!("FIFA: {updated}"), Style::default().fg(theme_text())),
                sep.clone(),
                Span::styled(
                    format!("Fetched: {fetched}"),
                    Style::default().fg(theme_muted()),
                ),
                sep.clone(),
                Span::styled(
                    status_label,
                    Style::default()
                        .fg(status_color)
                        .add_modifier(if state.analysis_loading {
                            Modifier::BOLD
                        } else {
                            Modifier::empty()
                        }),
                ),
            ])
        }
        Screen::Squad => {
            let team = state.squad_team.as_deref().unwrap_or("-");
            let status_label = if state.squad_loading {
                format!("{} LOADING", ui_spinner(anim))
            } else {
                "READY".to_string()
            };
            let status_color = if state.squad_loading {
                theme_warn()
            } else {
                theme_success()
            };
            Line::from(vec![
                Span::styled(
                    "WC26 SQUAD",
                    Style::default()
                        .fg(theme_accent())
                        .add_modifier(Modifier::BOLD),
                ),
                sep.clone(),
                Span::styled(format!("Team: {team}"), Style::default().fg(theme_accent_2())),
                sep.clone(),
                Span::styled(
                    format!("Players: {}", state.squad.len()),
                    Style::default().fg(theme_text()),
                ),
                sep.clone(),
                Span::styled(
                    status_label,
                    Style::default()
                        .fg(status_color)
                        .add_modifier(if state.squad_loading {
                            Modifier::BOLD
                        } else {
                            Modifier::empty()
                        }),
                ),
            ])
        }
        Screen::PlayerDetail => Line::from(Span::styled(
            "WC26 PLAYER",
            Style::default()
                .fg(theme_accent())
                .add_modifier(Modifier::BOLD),
        )),
    }
}

fn format_fetched_at(fetched_at: Option<SystemTime>) -> String {
    fetched_at
        .map(|stamp| {
            DateTime::<Local>::from(stamp)
                .format("%Y-%m-%d %H:%M")
                .to_string()
        })
        .unwrap_or_else(|| "-".to_string())
}

fn footer_styled(state: &AppState, anim: UiAnim) -> Line<'static> {
    let bindings: &[(&str, &str)] = match state.screen {
        Screen::Pulse => match state.pulse_view {
            PulseView::Live => &[
                ("1", "Pulse"),
                ("2", "Analysis"),
                ("Enter/d", "Terminal"),
                ("j/k/↑/↓", "Move"),
                ("s", "Sort"),
                ("l", "League"),
                ("u", "Upcoming"),
                ("i", "Details"),
                ("?", "Help"),
                ("q", "Quit"),
            ],
            PulseView::Upcoming => &[
                ("1", "Pulse"),
                ("2", "Analysis"),
                ("u", "Live"),
                ("j/k/↑/↓", "Scroll"),
                ("l", "League"),
                ("?", "Help"),
                ("q", "Quit"),
            ],
        },
        Screen::Terminal { .. } => &[
            ("1", "Pulse"),
            ("2", "Analysis"),
            ("Tab", "Focus"),
            ("Enter", "Detail"),
            ("b/Esc", "Back"),
            ("i", "Details"),
            ("l", "League"),
            ("?", "Help"),
            ("q", "Quit"),
        ],
        Screen::Analysis => match state.analysis_tab {
            state::AnalysisTab::Teams => &[
                ("1", "Pulse"),
                ("b/Esc", "Back"),
                ("j/k/↑/↓", "Move"),
                ("Enter", "Squad"),
                ("Tab", "Rankings"),
                ("r", "Refresh"),
                ("?", "Help"),
                ("q", "Quit"),
            ],
            state::AnalysisTab::RoleRankings => &[
                ("1", "Pulse"),
                ("b/Esc", "Back"),
                ("j/k/↑/↓", "Move"),
                ("←/→", "Role"),
                ("s", "Metric"),
                ("Tab", "Teams"),
                ("r", "Missing"),
                ("R", "Full"),
                ("?", "Help"),
                ("q", "Quit"),
            ],
        },
        Screen::Squad => &[
            ("1", "Pulse"),
            ("b/Esc", "Back"),
            ("j/k/↑/↓", "Move"),
            ("Enter", "Player"),
            ("r", "Reload (cached)"),
            ("R", "Refresh (network)"),
            ("?", "Help"),
            ("q", "Quit"),
        ],
        Screen::PlayerDetail => &[
            ("1", "Pulse"),
            ("b/Esc", "Back"),
            ("j/k/↑/↓", "Scroll"),
            ("r", "Reload (cached)"),
            ("R", "Refresh (network)"),
            ("?", "Help"),
            ("q", "Quit"),
        ],
    };
    let color_mode = match ui_theme().mode {
        UiColorMode::Truecolor => "TC",
        UiColorMode::Ansi16 => "16c",
    };
    let mut spans: Vec<Span> = Vec::new();
    for (i, (key, desc)) in bindings.iter().enumerate() {
        if i > 0 {
            spans.push(Span::styled(
                ui_theme().glyphs.divider,
                Style::default().fg(theme_border_dim()),
            ));
        }
        spans.push(Span::styled(
            key.to_string(),
            Style::default()
                .fg(theme_accent())
                .add_modifier(Modifier::BOLD),
        ));
        spans.push(Span::styled(
            format!(" {desc}"),
            Style::default().fg(theme_muted()),
        ));
    }
    spans.push(Span::styled(
        format!(
            "{}{} {}",
            ui_theme().glyphs.divider,
            color_mode,
            ui_spinner(anim)
        ),
        Style::default().fg(theme_border_dim()),
    ));
    Line::from(spans)
}

fn render_pulse(frame: &mut Frame, area: Rect, state: &AppState, anim: UiAnim) {
    match state.pulse_view {
        PulseView::Live => render_pulse_live(frame, area, state, anim),
        PulseView::Upcoming => render_pulse_upcoming(frame, area, state, anim),
    }
}

fn render_pulse_live(frame: &mut Frame, area: Rect, state: &AppState, anim: UiAnim) {
    let (main_area, sidebar_area) = if area.width >= 110 {
        let cols = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Min(72), Constraint::Length(36)])
            .split(area);
        (cols[0], cols[1])
    } else {
        (area, Rect::new(0, 0, 0, 0))
    };

    let sections = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(1), Constraint::Min(1)])
        .split(main_area);

    let widths = pulse_columns();
    render_pulse_header(frame, sections[0], &widths, anim);

    let list_area = sections[1];
    let rows = state.pulse_live_rows_ref();
    if rows.is_empty() {
        let empty_style = Style::default()
            .fg(theme_muted())
            .add_modifier(Modifier::ITALIC);
        let empty = Paragraph::new(Text::styled(
            "No matches for this league",
            on_black(empty_style),
        ))
        .style(Style::default().bg(theme_bg()));
        frame.render_widget(empty, list_area);
        return;
    }

    const ROW_HEIGHT: u16 = 3;
    if list_area.height < ROW_HEIGHT {
        let empty_style = Style::default()
            .fg(theme_muted())
            .add_modifier(Modifier::ITALIC);
        let empty = Paragraph::new(Text::styled(
            "Pulse list needs more height",
            on_black(empty_style),
        ))
        .style(Style::default().bg(theme_bg()));
        frame.render_widget(empty, list_area);
        return;
    }

    let visible = (list_area.height / ROW_HEIGHT) as usize;
    let (start, end) = visible_range(state.selected, rows.len(), visible);

    let now = Utc::now();
    let upcoming_by_id: std::collections::HashMap<&str, &state::UpcomingMatch> =
        state.upcoming.iter().map(|u| (u.id.as_str(), u)).collect();
    for (i, idx) in (start..end).enumerate() {
        let row_area = Rect {
            x: list_area.x,
            y: list_area.y + (i as u16) * ROW_HEIGHT,
            width: list_area.width,
            height: ROW_HEIGHT,
        };

        let selected = idx == state.selected;
        let base_bg = pulse_row_bg(selected, idx, anim);
        let base_style = Style::default().fg(theme_text()).bg(base_bg);
        let cols = Layout::default()
            .direction(Direction::Horizontal)
            .constraints(widths)
            .split(row_area);

        match rows[idx] {
            state::PulseLiveRow::Match(match_idx) => {
                let Some(m) = state.matches.get(match_idx) else {
                    continue;
                };
                let is_not_started = !m.is_live && m.minute == 0;
                let is_finished = !m.is_live && m.minute >= 90;

                let row_style = if selected {
                    base_style.add_modifier(Modifier::BOLD)
                } else if is_not_started || is_finished {
                    base_style.fg(theme_muted())
                } else {
                    base_style
                };
                frame.render_widget(Block::default().style(row_style), row_area);

                let time = if m.is_live {
                    format!("{}'", m.minute)
                } else if is_finished {
                    "FT".to_string()
                } else {
                    upcoming_by_id
                        .get(m.id.as_str())
                        .map(|u| format_countdown_short(&u.kickoff, now))
                        .unwrap_or_else(|| "KO".to_string())
                };
                let time = format!(
                    "{}{}",
                    if selected {
                        ui_theme().glyphs.row_selected
                    } else {
                        " "
                    },
                    time
                );
                let match_name = format!("{} vs {}", m.home, m.away);
                let score = if is_not_started {
                    "--".to_string()
                } else {
                    format!("{}-{}", m.score_home, m.score_away)
                };

                // Time cell: green for live, dim for finished
                let time_style = if m.is_live {
                    row_style.fg(theme_success())
                } else if is_finished {
                    row_style.fg(theme_muted())
                } else {
                    row_style
                };
                render_cell_text(frame, cols[0], &time, time_style);
                render_cell_text(frame, cols[1], &match_name, row_style);

                // Score cell: bold for live matches
                let score_style = if m.is_live {
                    row_style.add_modifier(Modifier::BOLD)
                } else {
                    row_style
                };
                render_cell_text(frame, cols[2], &score, score_style);

                if is_not_started {
                    let dim = row_style.fg(Color::DarkGray);
                    render_cell_text(frame, cols[3], "upcoming", dim);
                    render_cell_text(frame, cols[4], "-", dim);
                    render_cell_text(frame, cols[5], "-", dim);
                    render_cell_text(frame, cols[6], "-", dim);
                    render_cell_text(frame, cols[7], "-", dim);
                } else {
                    let hda = format!(
                        "H{:.0} D{:.0} A{:.0}",
                        m.win.p_home, m.win.p_draw, m.win.p_away
                    );
                    let delta_val = m.win.delta_home;
                    let delta = format!("{:+.1}", delta_val);
                    let quality = quality_label(m.win.quality).to_string();
                    let conf = format!("{}%", m.win.confidence);

                    let values = win_prob_values(state.win_prob_history.get(&m.id), m.win.p_home);
                    let chart = win_line_chart(&values, row_style, selected);
                    frame.render_widget(chart, cols[3]);

                    render_cell_text(frame, cols[4], &hda, row_style);

                    // Delta: green for positive (home gaining), red for negative
                    let delta_color = if delta_val > 1.0 {
                        theme_success()
                    } else if delta_val < -1.0 {
                        theme_danger()
                    } else {
                        theme_muted()
                    };
                    render_cell_text(frame, cols[5], &delta, row_style.fg(delta_color));

                    // Quality badge: colored by model tier
                    let quality_color = match m.win.quality {
                        state::ModelQuality::Track => theme_success(),
                        state::ModelQuality::Event => theme_warn(),
                        state::ModelQuality::Basic => theme_muted(),
                    };
                    render_cell_text(frame, cols[6], &quality, row_style.fg(quality_color));

                    // Confidence: dim when low
                    let conf_color = if m.win.confidence >= 70 {
                        theme_success()
                    } else if m.win.confidence >= 40 {
                        theme_warn()
                    } else {
                        theme_muted()
                    };
                    render_cell_text(frame, cols[7], &conf, row_style.fg(conf_color));
                }
            }
            state::PulseLiveRow::Upcoming(upcoming_idx) => {
                let Some(u) = state.upcoming.get(upcoming_idx) else {
                    continue;
                };

                let row_style = if selected {
                    base_style.add_modifier(Modifier::BOLD)
                } else {
                    base_style.fg(theme_muted())
                };
                frame.render_widget(Block::default().style(row_style), row_area);

                let time = format_countdown_short(&u.kickoff, now);
                let time = format!(
                    "{}{}",
                    if selected {
                        ui_theme().glyphs.row_selected
                    } else {
                        " "
                    },
                    time
                );
                let match_name = format!("{} vs {}", u.home, u.away);

                render_cell_text(frame, cols[0], &time, row_style);
                render_cell_text(frame, cols[1], &match_name, row_style);
                render_cell_text(frame, cols[2], "--", row_style);
                render_cell_text(frame, cols[3], "upcoming", row_style);
                render_cell_text(frame, cols[4], "-", row_style);
                render_cell_text(frame, cols[5], "-", row_style);
                render_cell_text(frame, cols[6], "-", row_style);
                render_cell_text(frame, cols[7], "-", row_style);
            }
        }
    }

    if sidebar_area.width > 0 && sidebar_area.height > 0 {
        render_pulse_live_sidebar(frame, sidebar_area, state, anim);
    }
}

fn render_pulse_live_sidebar(frame: &mut Frame, area: Rect, state: &AppState, anim: UiAnim) {
    let block = terminal_block("Selected", true, anim);
    let inner = block.inner(area);
    frame.render_widget(block, area);
    if inner.width == 0 || inner.height == 0 {
        return;
    }

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(6), Constraint::Length(6)])
        .split(inner);

    let base = Style::default().fg(theme_text()).bg(theme_panel_bg());

    let mut lines: Vec<String> = Vec::new();
    let selected_id = state.selected_match_id();
    if let Some(m) = state.selected_match() {
        let time = if m.is_live {
            format!("{}'", m.minute)
        } else if m.minute >= 90 {
            "FT".to_string()
        } else {
            "KO".to_string()
        };
        lines.push(format!("{} vs {}", m.home, m.away));
        lines.push(format!("Score: {}-{}", m.score_home, m.score_away));
        lines.push(format!("Time: {time}"));
        lines.push(String::new());
        lines.push(format!("Live: {}", ui_live_dot(anim)));
        lines.push(format!(
            "Win: H{:.0} D{:.0} A{:.0}",
            m.win.p_home, m.win.p_draw, m.win.p_away
        ));
        lines.push(format!("Δ Home: {:+.1}", m.win.delta_home));
        lines.push(format!(
            "Model: {}   Conf: {}%",
            quality_label(m.win.quality),
            m.win.confidence
        ));
        lines.push(String::new());
        lines.push("Enter: Terminal   i: Details".to_string());

        let values = win_prob_values(state.win_prob_history.get(&m.id), m.win.p_home);
        let chart_style = Style::default().fg(theme_success()).bg(theme_panel_bg());
        let chart = Sparkline::default()
            .data(&values)
            .max(100)
            .style(chart_style);
        frame.render_widget(chart, chunks[1]);
    } else if let Some(id) = selected_id.as_deref()
        && let Some(u) = state.upcoming.iter().find(|u| u.id == id)
    {
        lines.push(format!("{} vs {}", u.home, u.away));
        lines.push("Score: --".to_string());
        lines.push(format!("Kickoff: {}", u.kickoff));
        lines.push(format!(
            "League: {}",
            if u.league_name.is_empty() {
                "-"
            } else {
                u.league_name.as_str()
            }
        ));
        lines.push(format!(
            "Round: {}",
            if u.round.is_empty() {
                "-"
            } else {
                u.round.as_str()
            }
        ));
        lines.push(String::new());
        lines.push("Enter: Terminal (pins fixture)".to_string());
        let hint = Paragraph::new(lines.join("\n"))
            .style(base)
            .wrap(Wrap { trim: true });
        frame.render_widget(hint, chunks[0]);
        return;
    } else {
        lines.push("No selection".to_string());
        lines.push(String::new());
        lines.push("j/k or arrows to move".to_string());
        lines.push("u to toggle Upcoming".to_string());
        lines.push("l to change league".to_string());
        lines.push("? for help".to_string());
    }

    let hint = Paragraph::new(lines.join("\n"))
        .style(base)
        .wrap(Wrap { trim: true });
    frame.render_widget(hint, chunks[0]);
}

fn render_pulse_upcoming(frame: &mut Frame, area: Rect, state: &AppState, anim: UiAnim) {
    let sections = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(1), Constraint::Min(1)])
        .split(area);

    let widths = upcoming_columns();
    render_upcoming_header(frame, sections[0], &widths, anim);

    let list_area = sections[1];
    let upcoming = state.filtered_upcoming();
    if upcoming.is_empty() {
        let empty_style = Style::default()
            .fg(theme_muted())
            .add_modifier(Modifier::ITALIC);
        let empty = Paragraph::new(Text::styled(
            "No upcoming matches for this league",
            on_black(empty_style),
        ))
        .style(Style::default().bg(theme_bg()));
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

    let now = Utc::now();
    for (i, idx) in (start..end).enumerate() {
        let row_area = Rect {
            x: list_area.x,
            y: list_area.y + i as u16,
            width: list_area.width,
            height: 1,
        };
        let row_bg = if idx % 2 == 0 {
            theme_panel_bg()
        } else {
            theme_bg()
        };
        let row_style = Style::default().fg(theme_text()).bg(row_bg);
        frame.render_widget(Block::default().style(row_style), row_area);

        let cols = Layout::default()
            .direction(Direction::Horizontal)
            .constraints(widths)
            .split(row_area);

        let m = upcoming[idx];
        let kickoff = format_countdown(&m.kickoff, now);
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

        let sep_style = Style::default().fg(theme_border_dim()).bg(row_bg);
        render_cell_text(frame, cols[0], &kickoff, row_style.fg(theme_muted()));
        render_vseparator(frame, cols[1], sep_style);
        render_cell_text(frame, cols[2], &match_name, row_style);
        render_vseparator(frame, cols[3], sep_style);
        render_cell_text(frame, cols[4], &league, row_style.fg(theme_muted()));
        render_vseparator(frame, cols[5], sep_style);
        render_cell_text(frame, cols[6], &round, row_style.fg(theme_muted()));
    }
}

fn pulse_columns() -> [Constraint; 8] {
    [
        Constraint::Length(6),
        Constraint::Length(22),
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

fn render_pulse_header(frame: &mut Frame, area: Rect, widths: &[Constraint], anim: UiAnim) {
    let cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints(widths)
        .split(area);
    let style = Style::default()
        .fg(theme_accent())
        .bg(theme_chrome_bg())
        .add_modifier(Modifier::BOLD);

    render_cell_text(
        frame,
        cols[0],
        &format!("{} Time", ui_live_dot(anim)),
        style,
    );
    render_cell_text(frame, cols[1], "Match", style);
    render_cell_text(frame, cols[2], "Score", style);
    render_cell_text(frame, cols[3], "Win% Line", style);
    render_cell_text(frame, cols[4], "H/D/A", style);
    render_cell_text(frame, cols[5], "Delta", style);
    render_cell_text(frame, cols[6], "Q", style);
    render_cell_text(frame, cols[7], "Conf", style);
}

fn render_upcoming_header(frame: &mut Frame, area: Rect, widths: &[Constraint], anim: UiAnim) {
    let cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints(widths)
        .split(area);
    let style = Style::default()
        .fg(theme_accent())
        .bg(theme_chrome_bg())
        .add_modifier(Modifier::BOLD);
    let sep_style = Style::default().fg(theme_border_dim()).bg(theme_chrome_bg());

    render_cell_text(
        frame,
        cols[0],
        &format!("{} Starts In", ui_spinner(anim)),
        style,
    );
    render_vseparator(frame, cols[1], sep_style);
    render_cell_text(frame, cols[2], "Match", style);
    render_vseparator(frame, cols[3], sep_style);
    render_cell_text(frame, cols[4], "League", style);
    render_vseparator(frame, cols[5], sep_style);
    render_cell_text(frame, cols[6], "Round", style);
}

fn render_analysis(frame: &mut Frame, area: Rect, state: &AppState, anim: UiAnim) {
    match state.analysis_tab {
        state::AnalysisTab::Teams => render_analysis_teams(frame, area, state, anim),
        state::AnalysisTab::RoleRankings => render_analysis_rankings(frame, area, state, anim),
    }
}

fn render_analysis_teams(frame: &mut Frame, area: Rect, state: &AppState, anim: UiAnim) {
    let (main_area, sidebar_area) = if area.width >= 110 {
        let cols = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Min(78), Constraint::Length(32)])
            .split(area);
        (cols[0], cols[1])
    } else {
        (area, Rect::new(0, 0, 0, 0))
    };

    let sections = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(1), Constraint::Min(1)])
        .split(main_area);

    let widths = analysis_columns();
    render_analysis_header(frame, sections[0], &widths, anim);

    let list_area = sections[1];
    if state.analysis.is_empty() {
        let message = if state.analysis_loading {
            format!("{} Loading analysis...", ui_spinner(anim))
        } else {
            "No analysis data yet".to_string()
        };
        let empty_style = Style::default()
            .fg(theme_muted())
            .add_modifier(Modifier::ITALIC);
        let empty = Paragraph::new(Text::styled(message, on_black(empty_style)))
            .style(Style::default().bg(theme_bg()));
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
        let base_bg = pulse_row_bg(selected, idx, anim);
        let row_style = Style::default().fg(theme_text()).bg(base_bg);
        frame.render_widget(Block::default().style(row_style), row_area);

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
        let updated = row.fifa_updated.clone().unwrap_or_else(|| "-".to_string());
        let host = if row.host { "yes" } else { "-" };

        // Confederation colored by region
        let confed_color = confed_color_for(row.confed);
        let confed_style = row_style.fg(confed_color);
        let sep_style = Style::default().fg(theme_border_dim()).bg(base_bg);
        render_cell_text(frame, cols[0], confed, confed_style);
        render_vseparator(frame, cols[1], sep_style);
        render_cell_text(frame, cols[2], &row.name, row_style);
        render_vseparator(frame, cols[3], sep_style);
        // Rank: highlight top 10
        let rank_style = if row.fifa_rank.map(|r| r <= 10).unwrap_or(false) {
            row_style.fg(theme_accent_2()).add_modifier(Modifier::BOLD)
        } else {
            row_style
        };
        render_cell_text(frame, cols[4], &rank, rank_style);
        render_vseparator(frame, cols[5], sep_style);
        render_cell_text(frame, cols[6], &points, row_style);
        render_vseparator(frame, cols[7], sep_style);
        render_cell_text(frame, cols[8], &updated, row_style.fg(theme_muted()));
        render_vseparator(frame, cols[9], sep_style);
        // Host badge: green
        let host_style = if row.host {
            row_style.fg(theme_success()).add_modifier(Modifier::BOLD)
        } else {
            row_style.fg(theme_muted())
        };
        render_cell_text(frame, cols[10], host, host_style);
    }

    if sidebar_area.width > 0 && sidebar_area.height > 0 {
        render_analysis_team_sidebar(frame, sidebar_area, state, anim);
    }
}

fn render_analysis_team_sidebar(frame: &mut Frame, area: Rect, state: &AppState, anim: UiAnim) {
    let block = terminal_block("Team", true, anim);
    let inner = block.inner(area);
    frame.render_widget(block, area);
    if inner.width == 0 || inner.height == 0 {
        return;
    }

    let base = Style::default().fg(theme_text()).bg(theme_panel_bg());
    let mut lines: Vec<String> = Vec::new();

    let Some(team) = state.selected_analysis() else {
        lines.push("No team selected".to_string());
        let p = Paragraph::new(lines.join("\n")).style(base);
        frame.render_widget(p, inner);
        return;
    };

    lines.push(team.name.clone());
    lines.push(String::new());
    lines.push(format!("Confed: {}", confed_label(team.confed)));
    lines.push(format!("Host: {}", if team.host { "yes" } else { "no" }));
    lines.push(String::new());
    lines.push(format!(
        "FIFA rank: {}",
        team.fifa_rank
            .map(|v| v.to_string())
            .unwrap_or_else(|| "-".to_string())
    ));
    lines.push(format!(
        "Points: {}",
        team.fifa_points
            .map(|v| v.to_string())
            .unwrap_or_else(|| "-".to_string())
    ));
    lines.push(format!(
        "Updated: {}",
        team.fifa_updated.as_deref().unwrap_or("-")
    ));
    lines.push(String::new());
    lines.push("Enter: Squad".to_string());
    lines.push("Tab: Rankings".to_string());

    let p = Paragraph::new(lines.join("\n"))
        .style(base)
        .wrap(Wrap { trim: true });
    frame.render_widget(p, inner);
}

fn render_analysis_rankings(frame: &mut Frame, area: Rect, state: &AppState, anim: UiAnim) {
    let detail_h: u16 = 7;
    let show_detail = area.height >= 2 + 1 + detail_h + 1;
    let sections = if show_detail {
        Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(2),
                Constraint::Length(1),
                Constraint::Min(1),
                Constraint::Length(detail_h),
            ])
            .split(area)
    } else {
        Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(2),
                Constraint::Length(1),
                Constraint::Min(1),
            ])
            .split(area)
    };

    let role = role_label(state.rankings_role);
    let metric = metric_label(state.rankings_metric);
    let sep = Span::styled(ui_theme().glyphs.divider, Style::default().fg(theme_border_dim()));
    let mut header_spans = vec![
        Span::styled(
            "Role Rankings",
            Style::default()
                .fg(theme_accent())
                .add_modifier(Modifier::BOLD),
        ),
        sep.clone(),
        Span::styled("Role: ", Style::default().fg(theme_muted())),
        Span::styled(
            role.to_string(),
            Style::default()
                .fg(theme_accent_2())
                .add_modifier(Modifier::BOLD),
        ),
        sep.clone(),
        Span::styled("Metric: ", Style::default().fg(theme_muted())),
        Span::styled(
            metric.to_string(),
            Style::default()
                .fg(theme_accent())
                .add_modifier(Modifier::BOLD),
        ),
    ];
    if state.rankings_loading {
        header_spans.push(sep.clone());
        let progress_color = theme_accent_2();
        if state.rankings_progress_total > 0 {
            header_spans.push(Span::styled(
                format!(
                    "{} {} ({}/{})",
                    ui_spinner(anim),
                    state.rankings_progress_message,
                    state.rankings_progress_current,
                    state.rankings_progress_total
                ),
                Style::default()
                    .fg(progress_color)
                    .add_modifier(Modifier::BOLD),
            ));
        } else {
            header_spans.push(Span::styled(
                format!("{} {}", ui_spinner(anim), state.rankings_progress_message),
                Style::default()
                    .fg(progress_color)
                    .add_modifier(Modifier::BOLD),
            ));
        }
    }
    frame.render_widget(
        Block::default().style(Style::default().bg(theme_chrome_bg())),
        sections[0],
    );
    frame.render_widget(
        Paragraph::new(Line::from(header_spans)).style(Style::default().bg(theme_chrome_bg())),
        sections[0],
    );

    let search_line = if state.rankings_search_active {
        Line::from(vec![
            Span::styled(
                "Search [/]: ",
                Style::default()
                    .fg(theme_accent())
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                state.rankings_search.clone(),
                Style::default().fg(theme_accent_2()),
            ),
            Span::styled(ui_theme().glyphs.caret, Style::default().fg(theme_accent_2())),
        ])
    } else if state.rankings_search.is_empty() {
        Line::from(Span::styled("Search [/]", Style::default().fg(theme_muted())))
    } else {
        Line::from(vec![
            Span::styled("Search [/]: ", Style::default().fg(theme_muted())),
            Span::styled(
                state.rankings_search.clone(),
                Style::default().fg(theme_text()),
            ),
        ])
    };
    frame.render_widget(
        Block::default().style(Style::default().bg(theme_chrome_bg())),
        sections[1],
    );
    frame.render_widget(
        Paragraph::new(search_line).style(Style::default().bg(theme_chrome_bg())),
        sections[1],
    );

    let list_area = sections[2];
    if list_area.height == 0 {
        return;
    }

    if state.rankings.is_empty() {
        let message = if state.rankings_loading {
            format!("{} Loading role rankings...", ui_spinner(anim))
        } else {
            "No role ranking data yet (press r to warm cache)".to_string()
        };
        let empty_style = Style::default()
            .fg(theme_muted())
            .add_modifier(Modifier::ITALIC);
        let empty = Paragraph::new(Text::styled(message, on_black(empty_style)))
            .style(Style::default().bg(theme_bg()));
        frame.render_widget(empty, list_area);
        return;
    }

    let mut rows: Vec<&state::RoleRankingEntry> = state.rankings_filtered();

    match state.rankings_metric {
        state::RankMetric::Attacking => {
            rows.sort_by(|a, b| b.attack_score.total_cmp(&a.attack_score))
        }
        state::RankMetric::Defending => {
            rows.sort_by(|a, b| b.defense_score.total_cmp(&a.defense_score))
        }
    }

    let visible = list_area.height as usize;
    let total = rows.len();
    if total == 0 {
        let message = if state.rankings_search.trim().is_empty() {
            "No role ranking data yet (press r to warm cache)"
        } else {
            "No players match the current search"
        };
        let empty_style = Style::default()
            .fg(theme_muted())
            .add_modifier(Modifier::ITALIC);
        let empty = Paragraph::new(Text::styled(message, on_black(empty_style)))
            .style(Style::default().bg(theme_bg()));
        frame.render_widget(empty, list_area);
        return;
    }
    let (start, end) = visible_range(state.rankings_selected, total, visible);

    for (i, idx) in (start..end).enumerate() {
        let row_area = Rect {
            x: list_area.x,
            y: list_area.y + i as u16,
            width: list_area.width,
            height: 1,
        };

        let selected = idx == state.rankings_selected;
        let base_bg = pulse_row_bg(selected, idx, anim);
        let row_style = Style::default().fg(theme_text()).bg(base_bg);
        frame.render_widget(Block::default().style(row_style), row_area);

        let entry = rows[idx];
        let rank = idx + 1;
        let score = match state.rankings_metric {
            state::RankMetric::Attacking => entry.attack_score,
            state::RankMetric::Defending => entry.defense_score,
        };
        let score_text = if score.is_finite() {
            format!("{score:>7.2}")
        } else {
            "   -   ".to_string()
        };
        let rating = entry
            .rating
            .map(|r| format!("{r:.2}"))
            .unwrap_or_else(|| "-".to_string());
        let text = format!(
            "{rank:>3}. {:<24} {:<18} Score {}  R {rating}  Nation {}",
            truncate(&entry.player_name, 24),
            truncate(&entry.team_name, 18),
            score_text,
            truncate(&entry.club, 18)
        );
        render_cell_text(frame, row_area, &text, row_style);
    }

    if show_detail {
        let detail_area = sections[3];
        if detail_area.height == 0 {
            return;
        }

        let Some(selected) = rows.get(state.rankings_selected).copied() else {
            return;
        };

        let (score, factors) = match state.rankings_metric {
            state::RankMetric::Attacking => (selected.attack_score, &selected.attack_factors),
            state::RankMetric::Defending => (selected.defense_score, &selected.defense_factors),
        };

        let score_text = if score.is_finite() {
            format!("{score:.2}")
        } else {
            "-".to_string()
        };
        let rating_text = selected
            .rating
            .map(|r| format!("{r:.2}"))
            .unwrap_or_else(|| "-".to_string());

        let mut lines: Vec<Line> = Vec::new();
        lines.push(Line::from(vec![
            Span::styled("Selected: ", Style::default().fg(theme_muted())),
            Span::styled(
                truncate(&selected.player_name, 28),
                Style::default().fg(theme_text()).add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                format!(" ({})", truncate(&selected.team_name, 22)),
                Style::default().fg(theme_muted()),
            ),
            Span::styled("  Score ", Style::default().fg(theme_muted())),
            Span::styled(
                score_text,
                Style::default()
                    .fg(theme_accent_2())
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled("  R ", Style::default().fg(theme_muted())),
            Span::styled(rating_text, Style::default().fg(theme_accent())),
        ]));

        lines.push(Line::from(Span::styled(
            "Top contributors",
            Style::default()
                .fg(theme_accent())
                .add_modifier(Modifier::BOLD),
        )));

        if factors.is_empty() {
            lines.push(Line::from(Span::styled(
                "No breakdown available (warm cache / insufficient stat coverage)",
                Style::default()
                    .fg(theme_muted())
                    .add_modifier(Modifier::ITALIC),
            )));
        } else {
            for f in factors
                .iter()
                .take((detail_area.height as usize).saturating_sub(2))
            {
                let impact = f.weight * f.z;
                let impact_style = if impact >= 0.0 {
                    Style::default().fg(theme_success())
                } else {
                    Style::default().fg(theme_danger())
                };
                let mut tail = String::new();
                if let Some(pct) = f.pct {
                    tail.push_str(&format!(" pct={pct:.0}"));
                } else if let Some(raw) = f.raw {
                    tail.push_str(&format!(" raw={raw:.2}"));
                }
                tail.push_str(&format!(" ({}, w={:.2}, z={:.2})", f.source, f.weight, f.z));
                lines.push(Line::from(vec![
                    Span::styled(format!("{impact:+.2} "), impact_style),
                    Span::styled(truncate(&f.label, 20), Style::default().fg(theme_text())),
                    Span::styled(tail, Style::default().fg(theme_muted())),
                ]));
            }
        }

        let detail = Paragraph::new(lines)
            .style(Style::default().fg(theme_text()).bg(theme_panel_bg()))
            .wrap(Wrap { trim: true });
        frame.render_widget(detail, detail_area);
    }
}

fn truncate(raw: &str, max: usize) -> String {
    if raw.len() <= max {
        return raw.to_string();
    }
    raw.chars().take(max.saturating_sub(1)).collect::<String>() + "…"
}

fn render_analysis_header(frame: &mut Frame, area: Rect, widths: &[Constraint], anim: UiAnim) {
    let cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints(widths)
        .split(area);
    let style = Style::default()
        .fg(theme_accent())
        .bg(theme_chrome_bg())
        .add_modifier(Modifier::BOLD);
    let sep_style = Style::default().fg(theme_border_dim()).bg(theme_chrome_bg());

    render_cell_text(
        frame,
        cols[0],
        &format!("{} Confed", ui_spinner(anim)),
        style,
    );
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

fn render_squad(frame: &mut Frame, area: Rect, state: &AppState, anim: UiAnim) {
    let (main_area, sidebar_area) = if area.width >= 110 {
        let cols = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Min(80), Constraint::Length(30)])
            .split(area);
        (cols[0], cols[1])
    } else {
        (area, Rect::new(0, 0, 0, 0))
    };

    let sections = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(1), Constraint::Min(1)])
        .split(main_area);

    let widths = squad_columns();
    render_squad_header(frame, sections[0], &widths, anim);

    let list_area = sections[1];
    if state.squad.is_empty() {
        let message = if state.squad_loading {
            format!("{} Loading squad...", ui_spinner(anim))
        } else {
            "No squad data yet".to_string()
        };
        let empty_style = Style::default()
            .fg(theme_muted())
            .add_modifier(Modifier::ITALIC);
        let empty = Paragraph::new(Text::styled(message, on_black(empty_style)))
            .style(Style::default().bg(theme_bg()));
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
        let base_bg = pulse_row_bg(selected, idx, anim);
        let row_style = Style::default().fg(theme_text()).bg(base_bg);
        frame.render_widget(Block::default().style(row_style), row_area);

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

        let sep_style = Style::default().fg(theme_border_dim()).bg(base_bg);
        render_cell_text(frame, cols[0], &player.name, row_style);
        render_vseparator(frame, cols[1], sep_style);
        render_cell_text(frame, cols[2], &number, row_style);
        render_vseparator(frame, cols[3], sep_style);
        render_cell_text(frame, cols[4], &player.role, row_style.fg(theme_muted()));
        render_vseparator(frame, cols[5], sep_style);
        render_cell_text(frame, cols[6], &player.club, row_style);
        render_vseparator(frame, cols[7], sep_style);
        render_cell_text(frame, cols[8], &age, row_style.fg(theme_muted()));
        render_vseparator(frame, cols[9], sep_style);
        render_cell_text(frame, cols[10], &height, row_style.fg(theme_muted()));
        render_vseparator(frame, cols[11], sep_style);
        render_cell_text(frame, cols[12], &value, row_style.fg(theme_accent_2()));
    }

    if sidebar_area.width > 0 && sidebar_area.height > 0 {
        render_squad_sidebar(frame, sidebar_area, state, anim);
    }
}

fn render_squad_sidebar(frame: &mut Frame, area: Rect, state: &AppState, anim: UiAnim) {
    let block = terminal_block("Player", true, anim);
    let inner = block.inner(area);
    frame.render_widget(block, area);
    if inner.width == 0 || inner.height == 0 {
        return;
    }

    let base = Style::default().fg(theme_text()).bg(theme_panel_bg());
    let mut lines: Vec<String> = Vec::new();

    let Some(p) = state.selected_squad_player() else {
        lines.push("No player selected".to_string());
        let para = Paragraph::new(lines.join("\n")).style(base);
        frame.render_widget(para, inner);
        return;
    };

    lines.push(p.name.clone());
    lines.push(String::new());
    lines.push(format!(
        "Role: {}",
        if p.role.is_empty() {
            "-"
        } else {
            p.role.as_str()
        }
    ));
    lines.push(format!(
        "Nation: {}",
        if p.club.is_empty() {
            "-"
        } else {
            p.club.as_str()
        }
    ));
    lines.push(String::new());
    lines.push(format!(
        "Age: {}",
        p.age
            .map(|v| v.to_string())
            .unwrap_or_else(|| "-".to_string())
    ));
    lines.push(format!(
        "Height: {}",
        p.height
            .map(|v| format!("{v} cm"))
            .unwrap_or_else(|| "-".to_string())
    ));
    lines.push(format!(
        "Shirt: {}",
        p.shirt_number
            .map(|v| v.to_string())
            .unwrap_or_else(|| "-".to_string())
    ));
    lines.push(format!(
        "Value: {}",
        p.market_value
            .map(|v| format!("EUR {:.1}M", v as f64 / 1_000_000.0))
            .unwrap_or_else(|| "-".to_string())
    ));
    lines.push(String::new());
    lines.push("Enter: Player detail".to_string());

    let para = Paragraph::new(lines.join("\n"))
        .style(base)
        .wrap(Wrap { trim: true });
    frame.render_widget(para, inner);
}

fn render_squad_header(frame: &mut Frame, area: Rect, widths: &[Constraint], anim: UiAnim) {
    let cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints(widths)
        .split(area);
    let style = Style::default()
        .fg(theme_accent())
        .bg(theme_chrome_bg())
        .add_modifier(Modifier::BOLD);
    let sep_style = Style::default().fg(theme_border_dim()).bg(theme_chrome_bg());

    render_cell_text(
        frame,
        cols[0],
        &format!("{} Player", ui_spinner(anim)),
        style,
    );
    render_vseparator(frame, cols[1], sep_style);
    render_cell_text(frame, cols[2], "No", style);
    render_vseparator(frame, cols[3], sep_style);
    render_cell_text(frame, cols[4], "Role", style);
    render_vseparator(frame, cols[5], sep_style);
    render_cell_text(frame, cols[6], "Nation", style);
    render_vseparator(frame, cols[7], sep_style);
    render_cell_text(frame, cols[8], "Age", style);
    render_vseparator(frame, cols[9], sep_style);
    render_cell_text(frame, cols[10], "Ht", style);
    render_vseparator(frame, cols[11], sep_style);
    render_cell_text(frame, cols[12], "Value", style);
}

fn render_player_detail(frame: &mut Frame, area: Rect, app: &mut App, anim: UiAnim) {
    let state = &app.state;
    let block = Block::default()
        .title(Span::styled(
            " Player Detail ",
            Style::default()
                .fg(theme_accent())
                .add_modifier(Modifier::BOLD),
        ))
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(if anim.pulse_on {
            theme_accent_2()
        } else {
            theme_border()
        }))
        .style(Style::default().bg(theme_panel_bg()))
        .padding(Padding::new(1, 1, 0, 0));
    let inner = block.inner(area);
    frame.render_widget(block, area);

    if inner.height == 0 || inner.width == 0 {
        return;
    }

    if state.player_loading {
        let empty_style = Style::default()
            .fg(theme_muted())
            .add_modifier(Modifier::ITALIC);
        let text = Paragraph::new(Text::styled(
            format!("{} Loading player details...", ui_spinner(anim)),
            empty_style,
        ))
            .style(Style::default().fg(theme_text()).bg(theme_panel_bg()));
        frame.render_widget(text, inner);
        return;
    }

    let Some(detail) = state.player_detail.as_ref() else {
        let empty_style = Style::default()
            .fg(theme_muted())
            .add_modifier(Modifier::ITALIC);
        let text = Paragraph::new(Text::styled("No player data yet", empty_style))
            .style(Style::default().fg(theme_text()).bg(theme_panel_bg()));
        frame.render_widget(text, inner);
        return;
    };

    if inner.height < 8 {
        let text = player_detail_text(detail);
        let paragraph = Paragraph::new(text)
            .style(Style::default().fg(theme_text()).bg(theme_panel_bg()))
            .scroll((state.player_detail_scroll, 0));
        frame.render_widget(paragraph, inner);
        return;
    }

    let cache_key = build_detail_cache_key(state);
    let cache_needs_rebuild = app
        .detail_dist_cache
        .as_ref()
        .map(|cache| cache.key != cache_key)
        .unwrap_or(true);
    if cache_needs_rebuild {
        let dist = build_stat_distributions(state);
        let rank_index = build_league_stat_rank_index(state);
        app.detail_dist_cache = Some(DetailDistCache {
            key: cache_key,
            dist,
            rank_index,
        });
    }
    let (dist, rank_index) = match app.detail_dist_cache.as_ref() {
        Some(cache) => (&cache.dist, &cache.rank_index),
        None => {
            let dist = build_stat_distributions(state);
            let rank_index = build_league_stat_rank_index(state);
            app.detail_dist_cache = Some(DetailDistCache {
                key: cache_key,
                dist,
                rank_index,
            });
            let cache = app.detail_dist_cache.as_ref().expect("detail dist");
            (&cache.dist, &cache.rank_index)
        }
    };

    let info_text = player_info_text(detail);
    let league_text = player_league_stats_text(detail);
    let top_text = player_top_stats_text(detail);
    let traits_text = player_traits_text(detail);
    let other_text = player_season_performance_text(detail);
    let season_text = player_season_breakdown_text(detail);
    let career_text = player_career_text(detail);
    let trophies_text = player_trophies_text(detail);
    let recent_text = player_recent_matches_text(detail);

    let info_lines = text_line_count(&info_text);
    let league_lines = text_line_count(&league_text);
    let top_lines = text_line_count(&top_text);
    let traits_lines = text_line_count(&traits_text);
    let other_lines = text_line_count(&other_text);
    let season_lines = text_line_count(&season_text);
    let career_lines = text_line_count(&career_text);
    let trophies_lines = text_line_count(&trophies_text);
    let recent_lines = text_line_count(&recent_text);

    let info_text = Text::from(info_text);
    let league_text = player_league_stats_text_styled(detail, dist, Some(rank_index));
    let top_text = player_top_stats_text_styled(detail, dist, Some(rank_index));
    let traits_text = Text::from(traits_text);
    let other_text = player_season_performance_text_styled(detail, dist, Some(rank_index));
    let season_text = player_season_breakdown_text_styled(detail, dist);
    let career_text = Text::from(career_text);
    let trophies_text = Text::from(trophies_text);
    let recent_text = player_recent_matches_text_styled(detail, dist);

    if state.player_detail_expanded {
        let (title, body, lines, scroll) = match state.player_detail_section {
            0 => (
                "Player Info",
                info_text.clone(),
                info_lines,
                state.player_detail_section_scrolls[0],
            ),
            1 => (
                "All Competitions",
                league_text.clone(),
                league_lines,
                state.player_detail_section_scrolls[1],
            ),
            2 => (
                "Top Stats (All Competitions)",
                top_text.clone(),
                top_lines,
                state.player_detail_section_scrolls[2],
            ),
            3 => (
                "Player Traits",
                traits_text.clone(),
                traits_lines,
                state.player_detail_section_scrolls[3],
            ),
            4 => (
                "Season Performance",
                other_text.clone(),
                other_lines,
                state.player_detail_section_scrolls[4],
            ),
            5 => (
                "Season Breakdown",
                season_text.clone(),
                season_lines,
                state.player_detail_section_scrolls[5],
            ),
            6 => (
                "Career Summary",
                career_text.clone(),
                career_lines,
                state.player_detail_section_scrolls[6],
            ),
            7 => (
                "Trophies",
                trophies_text.clone(),
                trophies_lines,
                state.player_detail_section_scrolls[7],
            ),
            _ => (
                "Match Stats (Recent)",
                recent_text.clone(),
                recent_lines,
                state.player_detail_section_scrolls[8],
            ),
        };
        render_detail_section(frame, inner, title, body, scroll, true, lines);
        return;
    }

    let left = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(inner);

    let left_sections = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(text_block_height_from_lines(info_lines, 8)),
            Constraint::Length(text_block_height_from_lines(league_lines, 7)),
            Constraint::Length(text_block_height_from_lines(top_lines, 7)),
            Constraint::Length(text_block_height_from_lines(traits_lines, 7)),
            Constraint::Min(3),
        ])
        .split(left[0]);

    let right_sections = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(text_block_height_from_lines(season_lines, 9)),
            Constraint::Length(text_block_height_from_lines(career_lines, 9)),
            Constraint::Length(text_block_height_from_lines(trophies_lines, 7)),
            Constraint::Min(3),
        ])
        .split(left[1]);

    render_detail_section(
        frame,
        left_sections[0],
        "Player Info",
        info_text,
        state.player_detail_section_scrolls[0],
        state.player_detail_section == 0,
        info_lines,
    );
    render_detail_section(
        frame,
        left_sections[1],
        "All Competitions",
        league_text,
        state.player_detail_section_scrolls[1],
        state.player_detail_section == 1,
        league_lines,
    );
    render_detail_section(
        frame,
        left_sections[2],
        "Top Stats (All Competitions)",
        top_text,
        state.player_detail_section_scrolls[2],
        state.player_detail_section == 2,
        top_lines,
    );
    render_detail_section(
        frame,
        left_sections[3],
        "Player Traits",
        traits_text,
        state.player_detail_section_scrolls[3],
        state.player_detail_section == 3,
        traits_lines,
    );
    render_detail_section(
        frame,
        left_sections[4],
        "Season Performance",
        other_text,
        state.player_detail_section_scrolls[4],
        state.player_detail_section == 4,
        other_lines,
    );

    render_detail_section(
        frame,
        right_sections[0],
        "Season Breakdown",
        season_text,
        state.player_detail_section_scrolls[5],
        state.player_detail_section == 5,
        season_lines,
    );
    render_detail_section(
        frame,
        right_sections[1],
        "Career Summary",
        career_text,
        state.player_detail_section_scrolls[6],
        state.player_detail_section == 6,
        career_lines,
    );
    render_detail_section(
        frame,
        right_sections[2],
        "Trophies",
        trophies_text,
        state.player_detail_section_scrolls[7],
        state.player_detail_section == 7,
        trophies_lines,
    );
    render_detail_section(
        frame,
        right_sections[3],
        "Match Stats (Recent)",
        recent_text,
        state.player_detail_section_scrolls[8],
        state.player_detail_section == 8,
        recent_lines,
    );
}

fn player_detail_has_stats(detail: &PlayerDetail) -> bool {
    !detail.all_competitions.is_empty()
        || detail.main_league.is_some()
        || !detail.top_stats.is_empty()
        || !detail.season_groups.is_empty()
        || !detail.season_performance.is_empty()
        || detail
            .traits
            .as_ref()
            .map(|traits| !traits.items.is_empty())
            .unwrap_or(false)
        || !detail.recent_matches.is_empty()
        || !detail.season_breakdown.is_empty()
        || !detail.career_sections.is_empty()
        || !detail.trophies.is_empty()
}

fn player_detail_text(detail: &PlayerDetail) -> String {
    vec![
        player_info_text(detail),
        String::new(),
        player_league_stats_text(detail),
        String::new(),
        player_top_stats_text(detail),
        String::new(),
        player_traits_text(detail),
        String::new(),
        player_season_performance_text(detail),
        String::new(),
        player_season_breakdown_text(detail),
        String::new(),
        player_career_text(detail),
        String::new(),
        player_trophies_text(detail),
        String::new(),
        player_recent_matches_text(detail),
    ]
    .join("\n")
}

struct StatDistributions {
    by_title_role: HashMap<(RoleCategory, String), Vec<f64>>,
    by_title: HashMap<String, Vec<f64>>,
    ratings_role: HashMap<RoleCategory, Vec<f64>>,
    ratings: Vec<f64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DetailDistCacheKey {
    league_mode: LeagueMode,
    analysis_teams: usize,
    cache_players: usize,
    cache_players_fallback: usize,
    squads_loaded_for_league: usize,
    squad_players_for_league: usize,
}

struct DetailDistCache {
    key: DetailDistCacheKey,
    dist: StatDistributions,
    rank_index: LeagueStatRankIndex,
}

#[derive(Debug, Clone)]
struct LeagueStatRankIndex {
    total_by_title: HashMap<String, Vec<f64>>,
    per90_by_title: HashMap<String, Vec<f64>>,
    provisional_pool: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RankDirection {
    HigherBetter,
    LowerBetter,
}

#[derive(Debug, Clone)]
struct RankDisplay {
    text: String,
}

fn build_detail_cache_key(state: &AppState) -> DetailDistCacheKey {
    let cache = if state.combined_player_cache.is_empty() {
        &state.rankings_cache_players
    } else {
        &state.combined_player_cache
    };
    let team_ids: HashSet<u32> = state.analysis.iter().map(|t| t.id).collect();
    let mut squads_loaded_for_league = 0usize;
    let mut squad_players_for_league = 0usize;
    for team_id in team_ids {
        if let Some(squad) = state.rankings_cache_squads.get(&team_id) {
            squads_loaded_for_league += 1;
            squad_players_for_league += squad.len();
        }
    }

    DetailDistCacheKey {
        league_mode: state.league_mode,
        analysis_teams: state.analysis.len(),
        cache_players: cache.len(),
        cache_players_fallback: state.rankings_cache_players.len(),
        squads_loaded_for_league,
        squad_players_for_league,
    }
}

fn build_league_stat_rank_index(state: &AppState) -> LeagueStatRankIndex {
    const MIN_POOL_PLAYERS: usize = 60;
    const MIN_DETAIL_COVERAGE: f64 = 0.85;
    const MIN_PER90_MINUTES: f64 = 180.0;

    let cache = if state.combined_player_cache.is_empty() {
        &state.rankings_cache_players
    } else {
        &state.combined_player_cache
    };

    let team_ids: HashSet<u32> = state.analysis.iter().map(|t| t.id).collect();
    let mut expected_players = 0usize;
    let mut loaded_teams = 0usize;
    let mut league_player_ids: HashSet<u32> = HashSet::new();
    for team_id in &team_ids {
        if let Some(squad) = state.rankings_cache_squads.get(team_id) {
            loaded_teams += 1;
            expected_players += squad.len();
            for player in squad {
                league_player_ids.insert(player.id);
            }
        }
    }

    let mut candidate_details: Vec<&PlayerDetail> = Vec::new();
    let mut details_loaded = 0usize;

    if !league_player_ids.is_empty() {
        for player_id in league_player_ids {
            let Some(detail) = cache.get(&player_id) else {
                continue;
            };
            if state::player_detail_is_stub(detail) {
                continue;
            }
            details_loaded += 1;
            candidate_details.push(detail);
        }
    } else {
        // Fallback when squads are not loaded yet: use league-name matching.
        let wanted = league_label(state.league_mode).to_ascii_lowercase();
        for detail in cache.values() {
            if state::player_detail_is_stub(detail) {
                continue;
            }
            let matches_league = detail
                .main_league
                .as_ref()
                .is_some_and(|l| l.league_name.to_ascii_lowercase().contains(&wanted));
            if matches_league {
                details_loaded += 1;
                candidate_details.push(detail);
            }
        }
    }

    let mut total_by_title: HashMap<String, Vec<f64>> = HashMap::new();
    let mut per90_by_title: HashMap<String, Vec<f64>> = HashMap::new();

    for detail in candidate_details {
        let mut totals_for_player: HashMap<String, f64> = HashMap::new();
        let mut per90_for_player: HashMap<String, f64> = HashMap::new();

        collect_player_totals_if_missing(&mut totals_for_player, &detail.all_competitions);
        if let Some(league) = detail.main_league.as_ref() {
            collect_player_totals_if_missing(&mut totals_for_player, &league.stats);
        }
        collect_player_totals_if_missing(&mut totals_for_player, &detail.top_stats);

        let minutes = detail_minutes(detail).unwrap_or_default();
        for group in &detail.season_performance {
            for item in &group.items {
                let key = normalize_stat_title(&item.title);
                if let Some(total) = parse_stat_value(&item.total) {
                    totals_for_player.entry(key.clone()).or_insert(total);
                }
                if minutes >= MIN_PER90_MINUTES
                    && let Some(per90) = item.per90.as_deref().and_then(parse_stat_value)
                {
                    per90_for_player.entry(key).or_insert(per90);
                }
            }
        }

        for (key, value) in totals_for_player {
            total_by_title.entry(key).or_default().push(value);
        }
        for (key, value) in per90_for_player {
            per90_by_title.entry(key).or_default().push(value);
        }
    }

    for values in total_by_title.values_mut() {
        values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    }
    for values in per90_by_title.values_mut() {
        values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    }

    let squads_complete = !team_ids.is_empty() && loaded_teams == team_ids.len();
    let detail_coverage = if expected_players > 0 {
        details_loaded as f64 / expected_players as f64
    } else {
        0.0
    };
    let provisional_pool = !squads_complete
        || expected_players == 0
        || detail_coverage < MIN_DETAIL_COVERAGE
        || details_loaded < MIN_POOL_PLAYERS;

    LeagueStatRankIndex {
        total_by_title,
        per90_by_title,
        provisional_pool,
    }
}

fn collect_player_totals_if_missing(target: &mut HashMap<String, f64>, stats: &[PlayerStatItem]) {
    for stat in stats {
        let key = normalize_stat_title(&stat.title);
        if let Some(value) = parse_stat_value(&stat.value) {
            target.entry(key).or_insert(value);
        }
    }
}

fn build_stat_distributions(state: &AppState) -> StatDistributions {
    const MIN_MINUTES: f64 = 450.0;
    let mut by_title: HashMap<String, Vec<f64>> = HashMap::new();
    let mut by_title_role: HashMap<(RoleCategory, String), Vec<f64>> = HashMap::new();
    let mut ratings: Vec<f64> = Vec::new();
    let mut ratings_role: HashMap<RoleCategory, Vec<f64>> = HashMap::new();

    let cache = if state.combined_player_cache.is_empty() {
        &state.rankings_cache_players
    } else {
        &state.combined_player_cache
    };

    for detail in cache.values() {
        let role = role_from_detail(detail);
        let minutes = detail_minutes(detail);
        collect_stat_items(&mut by_title, &detail.all_competitions);
        collect_stat_items_role(&mut by_title_role, role, &detail.all_competitions);
        if let Some(league) = detail.main_league.as_ref() {
            collect_stat_items(&mut by_title, &league.stats);
            collect_stat_items_role(&mut by_title_role, role, &league.stats);
        }
        collect_stat_items(&mut by_title, &detail.top_stats);
        collect_stat_items_role(&mut by_title_role, role, &detail.top_stats);

        if minutes.map(|m| m >= MIN_MINUTES).unwrap_or(false) {
            for group in &detail.season_performance {
                for item in &group.items {
                    let value = item.per90.as_deref().and_then(parse_stat_value);
                    if let Some(v) = value {
                        by_title
                            .entry(normalize_stat_title(&item.title))
                            .or_default()
                            .push(v);
                        if let Some(role) = role {
                            by_title_role
                                .entry((role, normalize_stat_title(&item.title)))
                                .or_default()
                                .push(v);
                        }
                    }
                }
            }
        }

        for row in &detail.season_breakdown {
            if let Some(v) = parse_stat_value(&row.rating) {
                ratings.push(v);
                if let Some(role) = role {
                    ratings_role.entry(role).or_default().push(v);
                }
            }
        }
        for row in &detail.recent_matches {
            if let Some(v) = row.rating.as_deref().and_then(parse_stat_value) {
                ratings.push(v);
                if let Some(role) = role {
                    ratings_role.entry(role).or_default().push(v);
                }
            }
        }
    }

    for values in by_title.values_mut() {
        values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    }
    for values in by_title_role.values_mut() {
        values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    }
    ratings.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    for values in ratings_role.values_mut() {
        values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    }

    StatDistributions {
        by_title_role,
        by_title,
        ratings_role,
        ratings,
    }
}

fn collect_stat_items(target: &mut HashMap<String, Vec<f64>>, items: &[PlayerStatItem]) {
    for stat in items {
        if let Some(v) = parse_stat_value(&stat.value) {
            target
                .entry(normalize_stat_title(&stat.title))
                .or_default()
                .push(v);
        }
    }
}

fn collect_stat_items_role(
    target: &mut HashMap<(RoleCategory, String), Vec<f64>>,
    role: Option<RoleCategory>,
    items: &[PlayerStatItem],
) {
    let Some(role) = role else {
        return;
    };
    for stat in items {
        if let Some(v) = parse_stat_value(&stat.value) {
            target
                .entry((role, normalize_stat_title(&stat.title)))
                .or_default()
                .push(v);
        }
    }
}

fn role_from_detail(detail: &PlayerDetail) -> Option<RoleCategory> {
    let text = detail
        .position
        .as_ref()
        .or_else(|| detail.positions.first())
        .map(|s| s.as_str())?;
    role_from_text(text)
}

fn role_from_text(raw: &str) -> Option<RoleCategory> {
    let s = raw.to_lowercase();
    if s.contains("goalkeeper") || s.contains("keeper") || s == "gk" {
        return Some(RoleCategory::Goalkeeper);
    }
    if s.contains("defender")
        || s.contains("back")
        || s.contains("centre-back")
        || s.contains("center-back")
    {
        return Some(RoleCategory::Defender);
    }
    if s.contains("midfield") || s.contains("midfielder") {
        return Some(RoleCategory::Midfielder);
    }
    if s.contains("attacker")
        || s.contains("forward")
        || s.contains("striker")
        || s.contains("wing")
    {
        return Some(RoleCategory::Attacker);
    }
    None
}

fn detail_minutes(detail: &PlayerDetail) -> Option<f64> {
    let league = detail.main_league.as_ref()?;
    let stat = league
        .stats
        .iter()
        .find(|stat| stat.title.to_lowercase().contains("minutes"))?;
    parse_stat_value(&stat.value)
}

fn normalize_stat_title(title: &str) -> String {
    title.trim().to_lowercase()
}

fn parse_stat_value(raw: &str) -> Option<f64> {
    let trimmed = raw.trim();
    if trimmed.is_empty() || trimmed == "-" {
        return None;
    }
    let filtered: String = trimmed
        .chars()
        .filter(|c| c.is_ascii_digit() || *c == '.' || *c == '-')
        .collect();
    if filtered.is_empty() {
        return None;
    }
    filtered.parse::<f64>().ok()
}

fn percentile(values: &[f64], value: f64) -> Option<f64> {
    if values.is_empty() || !value.is_finite() {
        return None;
    }
    let idx = values.partition_point(|v| *v <= value);
    Some(idx as f64 / values.len() as f64 * 100.0)
}

/// FotMob-style percentile gradient.
///
/// Key stops (matching FotMob's stat bar colors):
///   0% → #E55541 (red)
///  25% → #F09D51 (orange)
///  50% → #EDC65E (gold)
///  75% → #69C05F (green)
/// 100% → #19BE62 (bright green)
fn color_for_percentile(percentile: f64) -> Color {
    let p = percentile.clamp(0.0, 100.0);

    // Color stops as (percentile, r, g, b).
    const STOPS: &[(f64, u8, u8, u8)] = &[
        (0.0, 229, 85, 65),   // #E55541
        (25.0, 240, 157, 81), // #F09D51
        (50.0, 237, 198, 94), // #EDC65E
        (75.0, 105, 192, 95), // #69C05F
        (100.0, 25, 190, 98), // #19BE62
    ];

    // Find the two stops to interpolate between.
    let mut lo = STOPS[0];
    let mut hi = STOPS[STOPS.len() - 1];
    for window in STOPS.windows(2) {
        if p >= window[0].0 && p <= window[1].0 {
            lo = window[0];
            hi = window[1];
            break;
        }
    }

    let range = hi.0 - lo.0;
    let t = if range > 0.0 { (p - lo.0) / range } else { 0.0 };

    let lerp = |a: u8, b: u8| -> u8 {
        let v = a as f64 + (b as f64 - a as f64) * t;
        v.round().clamp(0.0, 255.0) as u8
    };

    Color::Rgb(lerp(lo.1, hi.1), lerp(lo.2, hi.2), lerp(lo.3, hi.3))
}

fn style_from_percentile(percentile: Option<f64>) -> Option<Style> {
    percentile.map(|p| Style::default().fg(color_for_percentile(p)))
}

fn style_for_stat(
    dist: &StatDistributions,
    role: Option<RoleCategory>,
    title: &str,
    value: Option<f64>,
) -> Style {
    let Some(value) = value else {
        return Style::default();
    };
    let key = normalize_stat_title(title);
    let values = role
        .and_then(|r| dist.by_title_role.get(&(r, key.clone())))
        .or_else(|| dist.by_title.get(&key));
    let Some(values) = values else {
        return Style::default();
    };
    percentile(values, value)
        .map(|p| Style::default().fg(color_for_percentile(p)))
        .unwrap_or_default()
}

fn style_for_rating(
    dist: &StatDistributions,
    role: Option<RoleCategory>,
    value: Option<f64>,
) -> Style {
    let Some(value) = value else {
        return Style::default();
    };
    let values = role
        .and_then(|r| dist.ratings_role.get(&r))
        .unwrap_or(&dist.ratings);
    percentile(values, value)
        .map(|p| Style::default().fg(color_for_percentile(p)))
        .unwrap_or_default()
}

fn rank_direction_for_title(normalized_title: &str) -> RankDirection {
    if normalized_title.contains("goals conceded")
        || normalized_title.contains("xg against")
        || normalized_title.contains("against while on pitch")
        || normalized_title.contains("fouls committed")
        || normalized_title.contains("yellow card")
        || normalized_title.contains("red card")
        || normalized_title.contains("dribbled past")
        || normalized_title.contains("error led to goal")
        || normalized_title.contains("dispossessed")
    {
        RankDirection::LowerBetter
    } else {
        RankDirection::HigherBetter
    }
}

fn rank_for_value(values: &[f64], value: f64, direction: RankDirection) -> Option<(usize, usize)> {
    if values.is_empty() || !value.is_finite() {
        return None;
    }
    let n = values.len();
    let better = match direction {
        RankDirection::HigherBetter => n.saturating_sub(values.partition_point(|v| *v <= value)),
        RankDirection::LowerBetter => values.partition_point(|v| *v < value),
    };
    Some((better + 1, n))
}

fn stat_rank_suffix(
    rank_index: Option<&LeagueStatRankIndex>,
    title: &str,
    total_value: Option<f64>,
    per90_value: Option<f64>,
) -> Option<RankDisplay> {
    const MIN_STAT_SAMPLE: usize = 24;

    let Some(rank_index) = rank_index else {
        return None;
    };
    let key = normalize_stat_title(title);
    let direction = rank_direction_for_title(&key);

    let total_rank = total_value.and_then(|v| {
        rank_index
            .total_by_title
            .get(&key)
            .and_then(|vals| rank_for_value(vals, v, direction))
    });
    let per90_rank = per90_value.and_then(|v| {
        rank_index
            .per90_by_title
            .get(&key)
            .and_then(|vals| rank_for_value(vals, v, direction))
    });

    if total_rank.is_none() && per90_rank.is_none() {
        return None;
    }

    let mut parts: Vec<String> = Vec::new();
    let mut provisional = rank_index.provisional_pool;
    if let Some((rank, n)) = total_rank {
        if n < MIN_STAT_SAMPLE {
            provisional = true;
        }
        parts.push(format!("#{rank}/{n}"));
    }
    if let Some((rank, n)) = per90_rank {
        if n < MIN_STAT_SAMPLE {
            provisional = true;
        }
        parts.push(format!("p90 #{rank}/{n}"));
    }
    let mut text = format!("[{}]", parts.join(" | "));
    if provisional {
        text.push_str(" provisional");
    }
    Some(RankDisplay { text })
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
        lines.push(format!("Contract end: {}", shorten_date(contract_end)));
    }
    if let Some(birth_date) = &detail.birth_date {
        lines.push(format!("Birth date: {}", shorten_date(birth_date)));
    }
    if let Some(status) = &detail.status {
        lines.push(format!("Status: {status}"));
    }
    if let Some(injury) = &detail.injury_info {
        lines.push(format!("Injury: {injury}"));
    }
    if let Some(duty) = &detail.international_duty {
        lines.push(format!("International duty: {duty}"));
    }
    if !detail.positions.is_empty() {
        lines.push(format!("Positions: {}", detail.positions.join(", ")));
    }
    lines.join("\n")
}

fn player_league_stats_text(detail: &PlayerDetail) -> String {
    if !detail.all_competitions.is_empty() {
        let mut lines = Vec::new();
        let season_label = detail.all_competitions_season.as_deref().unwrap_or("-");
        lines.push(format!("All competitions ({season_label})"));
        for stat in detail.all_competitions.iter().take(8) {
            lines.push(format!("{}: {}", stat.title, stat.value));
        }
        return lines.join("\n");
    }

    if let Some(league) = detail.main_league.as_ref()
        && !league.stats.is_empty()
    {
        let mut lines = Vec::new();
        lines.push(format!("{} ({})", league.league_name, league.season));
        for stat in league.stats.iter().take(8) {
            lines.push(format!("{}: {}", stat.title, stat.value));
        }
        return lines.join("\n");
    }

    "No league stats available".to_string()
}

fn player_league_stats_text_styled(
    detail: &PlayerDetail,
    dist: &StatDistributions,
    rank_index: Option<&LeagueStatRankIndex>,
) -> Text<'static> {
    let role = role_from_detail(detail);
    let mut lines: Vec<Line> = Vec::new();
    if !detail.all_competitions.is_empty() {
        let season_label = detail.all_competitions_season.as_deref().unwrap_or("-");
        lines.push(Line::from(format!("All competitions ({season_label})")));
        for stat in detail.all_competitions.iter().take(8) {
            let value = stat.value.clone();
            let style = style_from_percentile(stat.percentile_rank_per90)
                .or_else(|| style_from_percentile(stat.percentile_rank))
                .unwrap_or_else(|| {
                    style_for_stat(dist, role, &stat.title, parse_stat_value(&value))
                });
            let mut spans = vec![
                Span::raw(format!("  {}: ", stat.title)),
                Span::styled(value, style),
            ];
            if let Some(rank) =
                stat_rank_suffix(rank_index, &stat.title, parse_stat_value(&stat.value), None)
            {
                spans.push(Span::raw(" "));
                spans.push(Span::styled(rank.text, Style::default().fg(theme_muted())));
            }
            lines.push(Line::from(spans));
        }
    }
    if let Some(league) = detail.main_league.as_ref()
        && !league.stats.is_empty()
    {
        if !lines.is_empty() {
            lines.push(Line::from(""));
        }
        lines.push(Line::from(format!(
            "{} ({})",
            league.league_name, league.season
        )));
        for stat in league.stats.iter().take(8) {
            let value = stat.value.clone();
            let style = style_from_percentile(stat.percentile_rank_per90)
                .or_else(|| style_from_percentile(stat.percentile_rank))
                .unwrap_or_else(|| {
                    style_for_stat(dist, role, &stat.title, parse_stat_value(&value))
                });
            let mut spans = vec![
                Span::raw(format!("  {}: ", stat.title)),
                Span::styled(value, style),
            ];
            if let Some(rank) =
                stat_rank_suffix(rank_index, &stat.title, parse_stat_value(&stat.value), None)
            {
                spans.push(Span::raw(" "));
                spans.push(Span::styled(rank.text, Style::default().fg(theme_muted())));
            }
            lines.push(Line::from(spans));
        }
    }
    if lines.is_empty() {
        Text::from("No league stats available".to_string())
    } else {
        Text::from(lines)
    }
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

fn player_top_stats_text_styled(
    detail: &PlayerDetail,
    dist: &StatDistributions,
    rank_index: Option<&LeagueStatRankIndex>,
) -> Text<'static> {
    if detail.top_stats.is_empty() {
        return Text::from("No all-competitions top stats".to_string());
    }
    let role = role_from_detail(detail);
    let mut lines = Vec::new();
    for stat in detail.top_stats.iter().take(8) {
        let value = stat.value.clone();
        let style = style_from_percentile(stat.percentile_rank_per90)
            .or_else(|| style_from_percentile(stat.percentile_rank))
            .unwrap_or_else(|| style_for_stat(dist, role, &stat.title, parse_stat_value(&value)));
        let mut spans = vec![
            Span::raw(format!("{}: ", stat.title)),
            Span::styled(value, style),
        ];
        if let Some(rank) =
            stat_rank_suffix(rank_index, &stat.title, parse_stat_value(&stat.value), None)
        {
            spans.push(Span::raw(" "));
            spans.push(Span::styled(rank.text, Style::default().fg(theme_muted())));
        }
        lines.push(Line::from(spans));
    }
    Text::from(lines)
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

fn player_minutes_played(detail: &PlayerDetail) -> Option<String> {
    let league = detail.main_league.as_ref()?;
    league
        .stats
        .iter()
        .find(|stat| stat.title.to_lowercase().contains("minutes"))
        .map(|stat| format_with_commas(&stat.value))
}

fn format_with_commas(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.is_empty() || !trimmed.chars().all(|c| c.is_ascii_digit()) {
        return raw.to_string();
    }
    let mut out = String::with_capacity(trimmed.len() + trimmed.len() / 3);
    let len = trimmed.len();
    for (idx, ch) in trimmed.chars().enumerate() {
        out.push(ch);
        let remaining = len - idx - 1;
        if remaining > 0 && remaining.is_multiple_of(3) {
            out.push(',');
        }
    }
    out
}

fn player_season_performance_text(detail: &PlayerDetail) -> String {
    if detail.season_performance.is_empty() {
        return "No season performance stats".to_string();
    }
    let mut lines = Vec::new();
    if let Some(minutes) = player_minutes_played(detail) {
        lines.push(format!("Minutes played: {minutes}"));
    }
    lines.push("Total | Per 90".to_string());
    for group in &detail.season_performance {
        lines.push(format!("{}:", group.title));
        for item in &group.items {
            let per90 = item.per90.as_deref().unwrap_or("-");
            lines.push(format!("  {}: {} | {}", item.title, item.total, per90));
        }
    }
    lines.join("\n")
}

fn player_season_performance_text_styled(
    detail: &PlayerDetail,
    dist: &StatDistributions,
    rank_index: Option<&LeagueStatRankIndex>,
) -> Text<'static> {
    if detail.season_performance.is_empty() {
        return Text::from("No season performance stats".to_string());
    }
    let role = role_from_detail(detail);
    let mut lines = Vec::new();
    if let Some(minutes) = player_minutes_played(detail) {
        lines.push(Line::from(format!("Minutes played: {minutes}")));
    }
    lines.push(Line::from("Total | Per 90"));
    for group in &detail.season_performance {
        lines.push(Line::from(format!("{}:", group.title)));
        for item in &group.items {
            let per90 = item.per90.as_deref().unwrap_or("-");

            // Total column: use percentile_rank (total-based).
            let total_style = style_from_percentile(item.percentile_rank).unwrap_or_else(|| {
                let color_value = parse_stat_value(&item.total);
                style_for_stat(dist, role, &item.title, color_value)
            });

            // Per 90 column: use percentile_rank_per90.
            let per90_style =
                style_from_percentile(item.percentile_rank_per90).unwrap_or_else(|| {
                    let color_value = item.per90.as_deref().and_then(parse_stat_value);
                    style_for_stat(dist, role, &item.title, color_value)
                });

            let mut spans = vec![
                Span::raw(format!("  {}: ", item.title)),
                Span::styled(item.total.clone(), total_style),
                Span::raw(" | "),
                Span::styled(per90.to_string(), per90_style),
            ];
            if let Some(rank) = stat_rank_suffix(
                rank_index,
                &item.title,
                parse_stat_value(&item.total),
                item.per90.as_deref().and_then(parse_stat_value),
            ) {
                spans.push(Span::raw(" "));
                spans.push(Span::styled(rank.text, Style::default().fg(theme_muted())));
            }
            lines.push(Line::from(spans));
        }
    }
    Text::from(lines)
}

fn player_season_breakdown_text(detail: &PlayerDetail) -> String {
    if detail.season_breakdown.is_empty() {
        return "No season breakdown".to_string();
    }
    let mut lines = Vec::new();
    for row in detail.season_breakdown.iter().take(10) {
        lines.push(format!(
            "{} {} | Apps {} G {} A {} | R {}",
            row.season, row.league, row.appearances, row.goals, row.assists, row.rating
        ));
    }
    lines.join("\n")
}

fn player_season_breakdown_text_styled(
    detail: &PlayerDetail,
    dist: &StatDistributions,
) -> Text<'static> {
    if detail.season_breakdown.is_empty() {
        return Text::from("No season breakdown".to_string());
    }
    let role = role_from_detail(detail);
    let mut lines = Vec::new();
    for row in detail.season_breakdown.iter().take(10) {
        let rating_style = style_for_rating(dist, role, parse_stat_value(&row.rating));
        lines.push(Line::from(vec![
            Span::raw(format!(
                "{} {} | Apps {} G {} A {} | R ",
                row.season, row.league, row.appearances, row.goals, row.assists
            )),
            Span::styled(row.rating.clone(), rating_style),
        ]));
    }
    Text::from(lines)
}

fn player_career_text(detail: &PlayerDetail) -> String {
    if detail.career_sections.is_empty() {
        return "No career history".to_string();
    }
    let mut lines = Vec::new();
    for section in detail.career_sections.iter().take(3) {
        lines.push(format!("{}:", title_case(&section.title)));
        for entry in section.entries.iter().take(6) {
            let start = entry
                .start_date
                .as_deref()
                .map(shorten_date)
                .unwrap_or_else(|| "-".to_string());
            let end = entry
                .end_date
                .as_deref()
                .map(shorten_date)
                .unwrap_or_else(|| "-".to_string());
            let apps = entry.appearances.as_deref().unwrap_or("-");
            let goals = entry.goals.as_deref().unwrap_or("-");
            let assists = entry.assists.as_deref().unwrap_or("-");
            lines.push(format!(
                "  {} {start}→{end} | Apps {apps} G {goals} A {assists}",
                entry.team
            ));
        }
    }
    lines.join("\n")
}

fn player_trophies_text(detail: &PlayerDetail) -> String {
    if detail.trophies.is_empty() {
        return "No trophies listed".to_string();
    }
    let mut lines = Vec::new();
    for trophy in detail.trophies.iter().take(10) {
        if !trophy.seasons_won.is_empty() {
            lines.push(format!(
                "{} - {}: {}",
                trophy.team,
                trophy.league,
                trophy.seasons_won.join(", ")
            ));
        }
        if !trophy.seasons_runner_up.is_empty() {
            lines.push(format!(
                "{} - {} (Runner-up): {}",
                trophy.team,
                trophy.league,
                trophy.seasons_runner_up.join(", ")
            ));
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

fn player_recent_matches_text_styled(
    detail: &PlayerDetail,
    dist: &StatDistributions,
) -> Text<'static> {
    if detail.recent_matches.is_empty() {
        return Text::from("No recent matches".to_string());
    }
    let role = role_from_detail(detail);
    let mut lines = Vec::new();
    for m in detail.recent_matches.iter().take(10) {
        let date = shorten_date(&m.date);
        let rating = m.rating.as_deref().unwrap_or("-");
        let rating_style =
            style_for_rating(dist, role, m.rating.as_deref().and_then(parse_stat_value));
        lines.push(Line::from(vec![
            Span::raw(format!(
                "{date} vs {} | {} | G {} A {} | R ",
                m.opponent, m.league, m.goals, m.assists
            )),
            Span::styled(rating.to_string(), rating_style),
        ]));
    }
    Text::from(lines)
}

fn render_detail_section(
    frame: &mut Frame,
    area: Rect,
    title: &str,
    body: Text,
    scroll: u16,
    active: bool,
    total_lines: u16,
) {
    if area.height == 0 || area.width == 0 {
        return;
    }
    let max_scroll = total_lines.saturating_sub(1);
    let current = scroll.min(max_scroll) + 1;
    let total = max_scroll + 1;
    let (border_style, title_style) = if active {
        (
            Style::default()
                .fg(theme_accent_2())
                .add_modifier(Modifier::BOLD),
            Style::default()
                .fg(theme_accent_2())
                .add_modifier(Modifier::BOLD),
        )
    } else {
        (
            Style::default().fg(theme_border_dim()),
            Style::default().fg(theme_muted()),
        )
    };
    let scroll_indicator = Span::styled(
        format!("  {current}/{total}"),
        Style::default().fg(theme_muted()),
    );
    let block = Block::default()
        .title(Line::from(vec![
            Span::styled(title.to_string(), title_style),
            scroll_indicator,
        ]))
        .borders(Borders::ALL)
        .border_type(if active {
            BorderType::Double
        } else {
            BorderType::Rounded
        })
        .border_style(border_style)
        .style(Style::default().bg(theme_panel_bg()))
        .padding(Padding::new(1, 1, 0, 0));
    let inner = block.inner(area);
    frame.render_widget(block, area);
    if inner.height == 0 || inner.width == 0 {
        return;
    }
    let paragraph = Paragraph::new(body)
        .style(Style::default().fg(theme_text()).bg(theme_panel_bg()))
        .scroll((scroll, 0));
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

fn title_case(raw: &str) -> String {
    raw.split_whitespace()
        .map(|word| {
            let mut chars = word.chars();
            let Some(first) = chars.next() else {
                return String::new();
            };
            let rest = chars.as_str().to_lowercase();
            format!("{}{}", first.to_uppercase(), rest)
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn player_detail_section_max_scroll(detail: &PlayerDetail, section: usize) -> u16 {
    let lines = match section {
        0 => player_info_text(detail),
        1 => player_league_stats_text(detail),
        2 => player_top_stats_text(detail),
        3 => player_traits_text(detail),
        4 => player_season_performance_text(detail),
        5 => player_season_breakdown_text(detail),
        6 => player_career_text(detail),
        7 => player_trophies_text(detail),
        _ => player_recent_matches_text(detail),
    };
    text_line_count(&lines).saturating_sub(1)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UiColorMode {
    Truecolor,
    Ansi16,
}

#[derive(Debug, Clone, Copy)]
struct UiPalette {
    bg: Color,
    panel_bg: Color,
    focus_bg: Color,
    chrome_bg: Color,
    border: Color,
    border_dim: Color,
    text: Color,
    muted: Color,
    accent: Color,
    accent_2: Color,
    success: Color,
    warn: Color,
    danger: Color,
}

#[derive(Debug, Clone, Copy)]
struct UiGlyphs {
    row_selected: &'static str,
    panel_focus: &'static str,
    divider: &'static str,
    vsep: char,
    caret: &'static str,
    live_on: &'static str,
    live_off: &'static str,
    spinner: [&'static str; 8],
}

#[derive(Debug, Clone, Copy)]
struct UiTheme {
    mode: UiColorMode,
    palette: UiPalette,
    glyphs: UiGlyphs,
}

#[derive(Debug, Clone, Copy)]
struct UiAnim {
    spinner_idx: usize,
    pulse_on: bool,
    blink_on: bool,
}

static UI_THEME: OnceLock<UiTheme> = OnceLock::new();

fn ui_theme() -> &'static UiTheme {
    UI_THEME.get_or_init(resolve_ui_theme)
}

fn resolve_ui_theme() -> UiTheme {
    let color_mode = detect_ui_color_mode();
    let unicode = !std::env::var("NO_UNICODE")
        .ok()
        .is_some_and(|v| v == "1" || v.eq_ignore_ascii_case("true"));
    let palette = match color_mode {
        UiColorMode::Truecolor => UiPalette {
            bg: Color::Rgb(6, 9, 14),
            panel_bg: Color::Rgb(10, 14, 22),
            focus_bg: Color::Rgb(92, 60, 88),
            chrome_bg: Color::Rgb(9, 12, 18),
            border: Color::Rgb(46, 58, 78),
            border_dim: Color::Rgb(30, 38, 52),
            text: Color::Rgb(228, 234, 244),
            muted: Color::Rgb(138, 148, 170),
            accent: Color::Rgb(0, 214, 255),
            accent_2: Color::Rgb(255, 196, 61),
            success: Color::LightGreen,
            warn: Color::Yellow,
            danger: Color::LightRed,
        },
        UiColorMode::Ansi16 => UiPalette {
            bg: Color::Black,
            panel_bg: Color::Black,
            focus_bg: Color::Magenta,
            chrome_bg: Color::Black,
            border: Color::Blue,
            border_dim: Color::DarkGray,
            text: Color::Gray,
            muted: Color::DarkGray,
            accent: Color::Cyan,
            accent_2: Color::Yellow,
            success: Color::Green,
            warn: Color::Yellow,
            danger: Color::Red,
        },
    };
    let glyphs = if unicode {
        UiGlyphs {
            row_selected: "▸",
            panel_focus: "◆ ",
            divider: " │ ",
            vsep: '│',
            caret: "▌",
            live_on: "●",
            live_off: "○",
            spinner: ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧"],
        }
    } else {
        UiGlyphs {
            row_selected: ">",
            panel_focus: "* ",
            divider: " | ",
            vsep: '|',
            caret: "|",
            live_on: "*",
            live_off: ".",
            spinner: ["-", "\\", "|", "/", "-", "\\", "|", "/"],
        }
    };

    UiTheme {
        mode: color_mode,
        palette,
        glyphs,
    }
}

fn detect_ui_color_mode() -> UiColorMode {
    let no_color = std::env::var("NO_COLOR")
        .ok()
        .is_some_and(|v| !v.trim().is_empty());
    if no_color {
        return UiColorMode::Ansi16;
    }
    let colorterm = std::env::var("COLORTERM")
        .unwrap_or_default()
        .to_ascii_lowercase();
    let term = std::env::var("TERM").unwrap_or_default().to_ascii_lowercase();
    detect_ui_color_mode_from_values(&term, &colorterm, no_color)
}

fn detect_ui_color_mode_from_values(term: &str, colorterm: &str, no_color: bool) -> UiColorMode {
    if no_color {
        return UiColorMode::Ansi16;
    }
    if colorterm.contains("truecolor")
        || colorterm.contains("24bit")
        || term.contains("truecolor")
        || term.contains("24bit")
        || term.contains("direct")
    {
        UiColorMode::Truecolor
    } else {
        UiColorMode::Ansi16
    }
}

fn ui_anim_from_frame(frame: u64) -> UiAnim {
    UiAnim {
        spinner_idx: (frame as usize) % 8,
        pulse_on: frame % 2 == 0,
        blink_on: frame % 2 == 0,
    }
}

fn theme_bg() -> Color {
    ui_theme().palette.bg
}
fn theme_panel_bg() -> Color {
    ui_theme().palette.panel_bg
}
fn theme_focus_bg() -> Color {
    ui_theme().palette.focus_bg
}
fn theme_chrome_bg() -> Color {
    ui_theme().palette.chrome_bg
}
fn theme_border() -> Color {
    ui_theme().palette.border
}
fn theme_border_dim() -> Color {
    ui_theme().palette.border_dim
}
fn theme_text() -> Color {
    ui_theme().palette.text
}
fn theme_muted() -> Color {
    ui_theme().palette.muted
}
fn theme_accent() -> Color {
    ui_theme().palette.accent
}
fn theme_accent_2() -> Color {
    ui_theme().palette.accent_2
}
fn theme_success() -> Color {
    ui_theme().palette.success
}
fn theme_warn() -> Color {
    ui_theme().palette.warn
}
fn theme_danger() -> Color {
    ui_theme().palette.danger
}

fn ui_spinner(anim: UiAnim) -> &'static str {
    ui_theme().glyphs.spinner[anim.spinner_idx]
}

fn ui_live_dot(anim: UiAnim) -> &'static str {
    if anim.blink_on {
        ui_theme().glyphs.live_on
    } else {
        ui_theme().glyphs.live_off
    }
}

fn pulse_row_bg(selected: bool, idx: usize, _anim: UiAnim) -> Color {
    if selected {
        theme_focus_bg()
    } else if idx.is_multiple_of(2) {
        theme_panel_bg()
    } else {
        theme_bg()
    }
}

fn on_black(mut style: Style) -> Style {
    // Ratatui widgets often overwrite the entire cell style.
    // If a widget style doesn't specify a bg, that cell's bg becomes "reset",
    // which can show up as white in light-themed terminals (especially on loading/empty screens).
    // Force a consistent background unless a caller explicitly chose another bg.
    match style.bg {
        None | Some(Color::Reset) => style.bg = Some(theme_bg()),
        _ => {}
    }
    style
}

fn render_cell_text(frame: &mut Frame, area: Rect, text: &str, style: Style) {
    if area.width == 0 || area.height == 0 {
        return;
    }
    let max_y = area.y.saturating_add(area.height.saturating_sub(1));
    let text_y = area.y.saturating_add(area.height / 2).min(max_y);
    let text_area = Rect {
        x: area.x,
        y: text_y,
        width: area.width,
        height: 1,
    };
    let paragraph = Paragraph::new(text).style(on_black(style));
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
        text.push(ui_theme().glyphs.vsep);
    }
    let paragraph = Paragraph::new(text).style(on_black(style));
    frame.render_widget(paragraph, area);
}

fn win_prob_values(history: Option<&Vec<f32>>, fallback: f32) -> Vec<u64> {
    let mut values = match history {
        Some(items) if !items.is_empty() => items
            .iter()
            .map(|v| v.round().clamp(0.0, 100.0) as u64)
            .collect(),
        _ => vec![fallback.round().clamp(0.0, 100.0) as u64],
    };
    if values.len() == 1 {
        values.push(values[0]);
    }
    values
}

fn win_line_chart(values: &[u64], row_style: Style, selected: bool) -> Sparkline<'_> {
    let mut style = row_style.fg(theme_success());
    if selected {
        style = style.add_modifier(Modifier::BOLD);
    }
    Sparkline::default().data(values).max(100).style(style)
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

fn terminal_block(title: &str, focused: bool, anim: UiAnim) -> Block<'_> {
    let (border_color, title_color, border_type) = if focused {
        (
            if anim.pulse_on {
                theme_accent_2()
            } else {
                theme_accent()
            },
            theme_accent_2(),
            BorderType::Double,
        )
    } else {
        (theme_border(), theme_muted(), BorderType::Rounded)
    };
    let marker = if focused {
        ui_theme().glyphs.panel_focus
    } else {
        "  "
    };
    Block::default()
        .title(Span::styled(
            format!("{marker}{title}"),
            Style::default().fg(title_color).add_modifier(if focused {
                Modifier::BOLD
            } else {
                Modifier::empty()
            }),
        ))
        .borders(Borders::ALL)
        .border_type(border_type)
        .border_style(Style::default().fg(border_color))
        .style(Style::default().bg(theme_panel_bg()))
        .padding(Padding::new(1, 1, 0, 0))
}

fn render_terminal(frame: &mut Frame, area: Rect, state: &AppState, anim: UiAnim) {
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

    let base_panel = Style::default().fg(theme_text()).bg(theme_panel_bg());

    let match_list = match_list_text(state);
    let left_match = Paragraph::new(match_list)
        .style(base_panel)
        .block(terminal_block(
            "Match List",
            state.terminal_focus == TerminalFocus::MatchList,
            anim,
        ));
    frame.render_widget(left_match, left_chunks[0]);

    let standings = Paragraph::new("Standings placeholder")
        .style(base_panel.fg(theme_muted()))
        .block(terminal_block("Group Mini", false, anim));
    frame.render_widget(standings, left_chunks[1]);

    render_pitch(frame, middle_chunks[0], state, anim);

    let (tape_title, tape_text, tape_focus) = match state.terminal_focus {
        TerminalFocus::Commentary => ("Commentary", commentary_tape_text(state), true),
        _ => (
            "Ticker",
            event_tape_text(state),
            state.terminal_focus == TerminalFocus::EventTape,
        ),
    };
    let tape = Paragraph::new(tape_text).block(terminal_block(tape_title, tape_focus, anim));
    let tape = tape.style(base_panel);
    frame.render_widget(tape, middle_chunks[1]);

    let stats_text = stats_text(state);
    let stats = Paragraph::new(stats_text)
        .style(base_panel)
        .block(terminal_block(
            "Stats",
            state.terminal_focus == TerminalFocus::Stats,
            anim,
        ));
    frame.render_widget(stats, right_chunks[0]);

    render_lineups(frame, right_chunks[1], state, anim);

    let preds_text = prediction_text(state);
    let preds = Paragraph::new(preds_text)
        .style(base_panel)
        .block(terminal_block(
            "Prediction",
            state.terminal_focus == TerminalFocus::Prediction,
            anim,
        ));
    frame.render_widget(preds, right_chunks[2]);

    let console = Paragraph::new(console_text(state))
        .style(base_panel)
        .block(terminal_block(
            "Console",
            state.terminal_focus == TerminalFocus::Console,
            anim,
        ));
    frame.render_widget(console, rows[1]);
}

fn match_list_text(state: &AppState) -> String {
    let filtered = state.filtered_matches();
    if filtered.is_empty() {
        return "No matches yet".to_string();
    }

    let selected_id = state.selected_match_id();
    let active_id = match &state.screen {
        Screen::Terminal { match_id: Some(id) } => Some(id.as_str()),
        _ => selected_id.as_deref(),
    };

    let mut lines = Vec::new();
    for m in filtered.iter() {
        let prefix = if active_id == Some(m.id.as_str()) {
            ui_theme().glyphs.row_selected
        } else {
            " "
        };
        let status = if m.is_live {
            format!("{}'", m.minute.min(99))
        } else if m.minute >= 90 {
            "FT".to_string()
        } else if m.minute == 0 {
            "KO".to_string()
        } else {
            format!("{}'", m.minute.min(99))
        };
        let home = truncate(&m.home, 5);
        let away = truncate(&m.away, 5);
        let score = if !m.is_live && m.minute == 0 {
            "  -  ".to_string()
        } else {
            format!("{}-{}", m.score_home, m.score_away)
        };
        let line = format!("{prefix}{status:>3} {home:<5} {score:^5} {away:<5}");
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
                lines.extend(stats_compact_lines(detail, 6));
            }
            lines.join("\n")
        }
        None => "No match selected".to_string(),
    }
}

fn stats_compact_lines(detail: &state::MatchDetail, limit: usize) -> Vec<String> {
    if detail.stats.is_empty() || limit == 0 {
        return Vec::new();
    }

    // Prefer Top stats, and pick a consistent subset.
    let preferred = [
        "Expected goals",
        "Total shots",
        "Shots on target",
        "Ball possession",
        "Accurate passes",
        "Big chances",
    ];
    let mut out = Vec::new();
    for needle in preferred {
        if out.len() >= limit {
            break;
        }
        if let Some(row) = detail.stats.iter().find(|row| {
            row.group
                .as_deref()
                .is_some_and(|g| g.eq_ignore_ascii_case("Top stats"))
                && row.name.to_lowercase().contains(&needle.to_lowercase())
        }) {
            out.push(format!("{}: {}-{}", row.name, row.home, row.away));
        }
    }
    if out.len() >= limit {
        return out;
    }

    // Fallback: first stats rows (any group).
    for row in detail.stats.iter() {
        if out.len() >= limit {
            break;
        }
        out.push(format!("{}: {}-{}", row.name, row.home, row.away));
    }
    out
}

fn grouped_stats_lines(detail: &state::MatchDetail) -> Vec<String> {
    if detail.stats.is_empty() {
        return Vec::new();
    }

    let mut groups: std::collections::HashMap<String, Vec<&state::StatRow>> =
        std::collections::HashMap::new();
    for row in &detail.stats {
        let g = row.group.clone().unwrap_or_else(|| "Other".to_string());
        groups.entry(g).or_default().push(row);
    }

    let preferred = [
        "Top stats",
        "Expected goals (xG)",
        "Shots",
        "Passes",
        "Defence",
        "Duels",
        "Discipline",
        "Other",
    ];
    let mut out = Vec::new();

    for g in preferred {
        let Some(rows) = groups.remove(g) else {
            continue;
        };
        out.push(format!("{g}:"));
        for row in rows {
            out.push(format!("  {}: {}-{}", row.name, row.home, row.away));
        }
        out.push(String::new());
    }

    let mut rest: Vec<(String, Vec<&state::StatRow>)> = groups.into_iter().collect();
    rest.sort_by(|a, b| a.0.cmp(&b.0));
    for (g, rows) in rest {
        out.push(format!("{g}:"));
        for row in rows {
            out.push(format!("  {}: {}-{}", row.name, row.home, row.away));
        }
        out.push(String::new());
    }

    while out.last().is_some_and(|s| s.is_empty()) {
        out.pop();
    }
    out
}

fn render_lineups(frame: &mut Frame, area: Rect, state: &AppState, anim: UiAnim) {
    let block = terminal_block(
        "Lineups",
        state.terminal_focus == TerminalFocus::Lineups,
        anim,
    );
    let inner = block.inner(area);
    frame.render_widget(block, area);

    if inner.height == 0 || inner.width == 0 {
        return;
    }

    let Some(match_id) = state.selected_match_id() else {
        let empty = Paragraph::new("No match selected")
            .style(Style::default().fg(theme_muted()).bg(theme_panel_bg()));
        frame.render_widget(empty, inner);
        return;
    };

    let Some(detail) = state.match_detail.get(&match_id) else {
        let empty = Paragraph::new("No lineups yet")
            .style(Style::default().fg(theme_muted()).bg(theme_panel_bg()));
        frame.render_widget(empty, inner);
        return;
    };

    let Some(lineups) = &detail.lineups else {
        let empty = Paragraph::new("No lineups yet")
            .style(Style::default().fg(theme_muted()).bg(theme_panel_bg()));
        frame.render_widget(empty, inner);
        return;
    };

    let mut sides = lineups.sides.clone();
    sides.sort_by(|a, b| a.team_abbr.cmp(&b.team_abbr));
    let left = sides.first();
    let right = sides.get(1);

    let cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(inner);

    render_lineup_side(frame, cols[0], left);
    render_lineup_side(frame, cols[1], right);
}

fn render_pitch(frame: &mut Frame, area: Rect, state: &AppState, anim: UiAnim) {
    let block = terminal_block("Pitch", state.terminal_focus == TerminalFocus::Pitch, anim);
    let inner = block.inner(area);
    frame.render_widget(block, area);

    if inner.width == 0 || inner.height == 0 {
        return;
    }

    let text = pitch_text(state, inner.width as usize, inner.height as usize);
    frame.render_widget(
        Paragraph::new(text).style(Style::default().fg(theme_text()).bg(theme_panel_bg())),
        inner,
    );
}

fn pitch_text(state: &AppState, width: usize, height: usize) -> String {
    let Some(match_id) = state.selected_match_id() else {
        return "No match selected".to_string();
    };
    let Some(detail) = state.match_detail.get(&match_id) else {
        return "No lineups yet".to_string();
    };
    let Some(lineups) = &detail.lineups else {
        return "No lineups yet".to_string();
    };
    if lineups.sides.len() < 2 {
        return "Lineups incomplete".to_string();
    }

    let home = &lineups.sides[0];
    let away = &lineups.sides[1];
    let sep = "-".repeat(width.clamp(4, 24));
    let mut lines = pitch_team_lines("AWAY", away, width);
    lines.push(center_line(&sep, width));
    lines.extend(pitch_team_lines("HOME", home, width));

    if lines.len() > height {
        lines.truncate(height);
    }
    lines.join("\n")
}

fn pitch_team_lines(label: &str, side: &state::LineupSide, width: usize) -> Vec<String> {
    let mut lines = Vec::new();
    lines.push(crop_line(
        &format!("{label} {} ({})", side.team_abbr, side.formation),
        width,
    ));
    lines.extend(pitch_pos_lines(&side.starting, width));
    lines
}

fn pitch_pos_lines(players: &[state::PlayerSlot], width: usize) -> Vec<String> {
    let mut gk = Vec::new();
    let mut df = Vec::new();
    let mut mf = Vec::new();
    let mut fw = Vec::new();
    let mut other = Vec::new();

    for player in players {
        let name = player.name.as_str();
        match player.pos.as_deref() {
            Some("GK") => gk.push(name),
            Some("DF") => df.push(name),
            Some("MF") => mf.push(name),
            Some("FW") => fw.push(name),
            _ => other.push(name),
        }
    }

    let mut lines = vec![
        crop_line(&format_pos_line("GK", &gk), width),
        crop_line(&format_pos_line("DF", &df), width),
        crop_line(&format_pos_line("MF", &mf), width),
        crop_line(&format_pos_line("FW", &fw), width),
    ];
    if !other.is_empty() {
        lines.push(crop_line(&format_pos_line("OT", &other), width));
    }
    lines
}

fn format_pos_line(label: &str, names: &[&str]) -> String {
    let body = if names.is_empty() {
        "-".to_string()
    } else {
        names.join(", ")
    };
    format!("{label}: {body}")
}

fn center_line(text: &str, width: usize) -> String {
    if text.len() >= width {
        return crop_line(text, width);
    }
    let pad = (width - text.len()) / 2;
    format!("{:pad$}{}", "", text, pad = pad)
}

fn crop_line(text: &str, width: usize) -> String {
    if text.len() <= width {
        return text.to_string();
    }
    text.chars()
        .take(width.saturating_sub(1))
        .collect::<String>()
        + "…"
}

fn render_lineup_side(frame: &mut Frame, area: Rect, side: Option<&state::LineupSide>) {
    let text = if let Some(side) = side {
        lineup_text(side)
    } else {
        "No lineup".to_string()
    };
    let paragraph = Paragraph::new(text).style(Style::default().fg(theme_text()).bg(theme_panel_bg()));
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
    lines.push(String::new());
    lines.push("Starters:".to_string());
    for player in &side.starting {
        lines.push(format!("  {}", format_player(player)));
    }
    lines.push(String::new());
    lines.push("Subs:".to_string());
    for player in &side.subs {
        lines.push(format!("  {}", format_player(player)));
    }
    lines.join("\n")
}

fn format_player(player: &state::PlayerSlot) -> String {
    let num = player
        .number
        .map(|n| n.to_string())
        .unwrap_or_else(|| "--".to_string());
    let pos = player.pos.clone().unwrap_or_default();
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
        return "No ticker yet".to_string();
    };
    if !detail.commentary.is_empty() {
        let start = detail.commentary.len().saturating_sub(6);
        return detail.commentary[start..]
            .iter()
            .map(format_commentary_line)
            .collect::<Vec<_>>()
            .join("\n");
    }
    if detail.events.is_empty() {
        return "No ticker yet".to_string();
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

fn ticker_full_text(state: &AppState) -> String {
    let Some(match_id) = state.selected_match_id() else {
        return "No match selected".to_string();
    };
    let Some(detail) = state.match_detail.get(&match_id) else {
        return "No ticker yet".to_string();
    };
    if !detail.commentary.is_empty() {
        return detail
            .commentary
            .iter()
            .map(format_commentary_line)
            .collect::<Vec<_>>()
            .join("\n");
    }
    if detail.events.is_empty() {
        return "No ticker yet".to_string();
    }
    detail
        .events
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

fn commentary_tape_text(state: &AppState) -> String {
    let Some(match_id) = state.selected_match_id() else {
        return "No match selected".to_string();
    };
    let Some(detail) = state.match_detail.get(&match_id) else {
        return "No commentary yet".to_string();
    };
    if detail.commentary.is_empty() {
        if let Some(err) = detail.commentary_error.as_deref() {
            return format!("Ticker error: {err}");
        }
        return "No commentary yet".to_string();
    }
    let start = detail.commentary.len().saturating_sub(6);
    detail.commentary[start..]
        .iter()
        .map(format_commentary_line)
        .collect::<Vec<_>>()
        .join("\n")
}

fn commentary_full_text(state: &AppState) -> String {
    let Some(match_id) = state.selected_match_id() else {
        return "No match selected".to_string();
    };
    let Some(detail) = state.match_detail.get(&match_id) else {
        return "No commentary yet".to_string();
    };
    if detail.commentary.is_empty() {
        if let Some(err) = detail.commentary_error.as_deref() {
            return format!("Ticker error: {err}");
        }
        return "No commentary yet".to_string();
    }
    detail
        .commentary
        .iter()
        .map(format_commentary_line)
        .collect::<Vec<_>>()
        .join("\n")
}

fn format_commentary_line(entry: &state::CommentaryEntry) -> String {
    let time = match (entry.minute, entry.minute_plus) {
        (Some(min), Some(plus)) if plus > 0 => format!("{min}+{plus}'"),
        (Some(min), _) => format!("{min}'"),
        _ => "--".to_string(),
    };
    if let Some(team) = entry.team.as_ref() {
        format!("{time} {team}: {}", entry.text)
    } else {
        format!("{time} {}", entry.text)
    }
}

fn stats_full_text(state: &AppState) -> String {
    let Some(match_id) = state.selected_match_id() else {
        return "No match selected".to_string();
    };
    let Some(detail) = state.match_detail.get(&match_id) else {
        return "No stats yet".to_string();
    };
    if detail.stats.is_empty() {
        return "No stats yet".to_string();
    }
    grouped_stats_lines(detail).join("\n")
}

fn lineups_full_text(state: &AppState) -> String {
    let Some(match_id) = state.selected_match_id() else {
        return "No match selected".to_string();
    };
    let Some(detail) = state.match_detail.get(&match_id) else {
        return "No lineups yet".to_string();
    };
    let Some(lineups) = &detail.lineups else {
        return "No lineups yet".to_string();
    };
    if lineups.sides.is_empty() {
        return "No lineups yet".to_string();
    }

    let mut sides = lineups.sides.clone();
    sides.sort_by(|a, b| a.team_abbr.cmp(&b.team_abbr));

    let mut lines = Vec::new();
    for (idx, side) in sides.iter().enumerate() {
        if idx > 0 {
            lines.push(String::new());
        }
        lines.extend(lineup_text(side).lines().map(|line| line.to_string()));
    }
    lines.join("\n")
}

fn prediction_detail_text(state: &AppState) -> String {
    let Some(m) = state.selected_match() else {
        return "No prediction data".to_string();
    };

    let extras = state.prediction_extras.get(&m.id);

    let mut lines = Vec::new();
    if m.is_live {
        lines.push("Now:".to_string());
        lines.push(format!("{}: {:.1}%", m.home, m.win.p_home));
        lines.push(format!("Draw: {:.1}%", m.win.p_draw));
        lines.push(format!("{}: {:.1}%", m.away, m.win.p_away));
        lines.push(format!("Delta home: {:+.1}", m.win.delta_home));
        lines.push(format!("Model: {}", quality_label(m.win.quality)));
        lines.push(format!("Confidence: {}", m.win.confidence));

        if let Some(pre) = state.prematch_win.get(&m.id) {
            lines.push(String::new());
            lines.push("Pre-match snapshot:".to_string());
            lines.push(format!("{}: {:.1}%", m.home, pre.p_home));
            lines.push(format!("Draw: {:.1}%", pre.p_draw));
            lines.push(format!("{}: {:.1}%", m.away, pre.p_away));
            lines.push(format!("Model: {}", quality_label(pre.quality)));
            lines.push(format!("Confidence: {}", pre.confidence));
        } else {
            lines.push(String::new());
            lines.push("Pre-match snapshot: (not captured)".to_string());
        }
    } else {
        let label = if state.prematch_locked.contains(&m.id) {
            "Pre-match snapshot:"
        } else {
            "Pre-match (preview, locks at kickoff):"
        };
        lines.push(label.to_string());
        lines.push(format!("{}: {:.1}%", m.home, m.win.p_home));
        lines.push(format!("Draw: {:.1}%", m.win.p_draw));
        lines.push(format!("{}: {:.1}%", m.away, m.win.p_away));
        lines.push(format!("Model: {}", quality_label(m.win.quality)));
        lines.push(format!("Confidence: {}", m.win.confidence));
    }

    if let Some(ex) = extras {
        lines.push(String::new());
        lines.push("Explain (pre-match):".to_string());
        lines.push(format!(
            "Contrib (home win pp): Lineup {:+.1}",
            ex.explain.pp_lineup
        ));
        lines.push(format!(
            "Baseline: H{:.1} D{:.1} A{:.1}",
            ex.explain.p_home_baseline, ex.explain.p_draw_baseline, ex.explain.p_away_baseline
        ));
        lines.push(format!(
            "Final:    H{:.1} D{:.1} A{:.1}",
            ex.explain.p_home_final, ex.explain.p_draw_final, ex.explain.p_away_final
        ));

        lines.push(String::new());
        lines.push(format!(
            "xG prior (pre): {:.2} - {:.2}",
            ex.lambda_home_pre, ex.lambda_away_pre
        ));
        if let Some(gt) = ex.goals_total_base {
            let rho = ex.dc_rho.unwrap_or(0.0);
            lines.push(format!("League params: goals={gt:.2} dcRho={rho:+.2}"));
        }

        let l_h = ex
            .s_home_lineup
            .map(|v| format!("{v:.2}"))
            .unwrap_or_else(|| "-".to_string());
        let l_a = ex
            .s_away_lineup
            .map(|v| format!("{v:.2}"))
            .unwrap_or_else(|| "-".to_string());
        lines.push(format!("Lineup strength:  home={l_h} away={l_a}"));
        if let (Some(ch), Some(ca)) = (ex.lineup_coverage_home, ex.lineup_coverage_away) {
            lines.push(format!(
                "Lineup: {:.0}/11 vs {:.0}/11, w={:.2}",
                (ch * 11.0).round().clamp(0.0, 11.0),
                (ca * 11.0).round().clamp(0.0, 11.0),
                ex.blend_w_lineup
            ));
        } else {
            lines.push(format!("Lineup: none, w={:.2}", ex.blend_w_lineup));
        }
        if !ex.explain.signals.is_empty() {
            lines.push(format!("Signals: {}", ex.explain.signals.join(", ")));
        }

        if let (Some(dh), Some(da)) = (ex.disc_home, ex.disc_away) {
            let cov_h = ex
                .disc_cov_home
                .map(|v| (v * 11.0).round().clamp(0.0, 11.0) as u8)
                .unwrap_or(0);
            let cov_a = ex
                .disc_cov_away
                .map(|v| (v * 11.0).round().clamp(0.0, 11.0) as u8)
                .unwrap_or(0);
            let mh = ex.disc_mult_home.unwrap_or(1.0);
            let ma = ex.disc_mult_away.unwrap_or(1.0);
            lines.push(format!(
                "Discipline (pct, higher=worse): home={dh:.0} away={da:.0} cov={cov_h}/11 {cov_a}/11 mult={mh:.2}/{ma:.2}"
            ));
        }
    }

    if let Some(history) = state.win_prob_history.get(&m.id)
        && !history.is_empty()
    {
        let start = history.len().saturating_sub(10);
        let slice = &history[start..];
        let points = slice
            .iter()
            .map(|val| format!("{:.0}", val))
            .collect::<Vec<_>>()
            .join(", ");
        lines.push(String::new());
        lines.push(format!(
            "Home win history (last {}): {}",
            slice.len(),
            points
        ));
    }

    lines.join("\n")
}

fn console_full_text(state: &AppState) -> String {
    if state.logs.is_empty() {
        return "No alerts yet".to_string();
    }
    state.logs.iter().cloned().collect::<Vec<_>>().join("\n")
}

fn match_detail_overview_text(state: &AppState) -> String {
    let Some(m) = state.selected_match() else {
        return "No match selected".to_string();
    };

    let status = if m.is_live {
        format!("Minute: {}'", m.minute)
    } else {
        "Status: FT".to_string()
    };

    let mut lines = vec![
        format!("{} vs {}", m.home, m.away),
        format!("Score: {}-{}", m.score_home, m.score_away),
        status,
        format!("League: {}", m.league_name),
        format!(
            "Model: {} (confidence {})",
            quality_label(m.win.quality),
            m.win.confidence
        ),
    ];

    let Some(detail) = state.match_detail.get(&m.id) else {
        lines.push(String::new());
        lines.push("Match details not loaded. Press i to fetch.".to_string());
        return lines.join("\n");
    };

    lines.push(String::new());
    lines.push("Ticker:".to_string());
    if detail.commentary.is_empty() {
        if let Some(err) = detail.commentary_error.as_deref() {
            lines.push(format!("Ticker error: {err}"));
        } else {
            lines.push("No ticker yet. Press i to refresh.".to_string());
        }
    } else {
        let start = detail.commentary.len().saturating_sub(12);
        lines.extend(
            detail.commentary[start..]
                .iter()
                .map(format_commentary_line),
        );
    }

    if !detail.stats.is_empty() {
        lines.push(String::new());
        lines.push("Stats:".to_string());
        lines.extend(grouped_stats_lines(detail));
    }

    if !detail.events.is_empty() {
        lines.push(String::new());
        lines.push("Events:".to_string());
        lines.extend(detail.events.iter().map(|event| {
            format!(
                "{}' {} {} {}",
                event.minute,
                event_kind_label(event.kind),
                event.team,
                event.description
            )
        }));
    }

    if let Some(lineups) = &detail.lineups
        && !lineups.sides.is_empty()
    {
        lines.push(String::new());
        lines.push("Lineups:".to_string());
        let mut sides = lineups.sides.clone();
        sides.sort_by(|a, b| a.team_abbr.cmp(&b.team_abbr));
        for (idx, side) in sides.iter().enumerate() {
            if idx > 0 {
                lines.push(String::new());
            }
            lines.extend(lineup_text(side).lines().map(|line| line.to_string()));
        }
    }

    lines.join("\n")
}

fn prediction_text(state: &AppState) -> String {
    match state.selected_match() {
        Some(m) => {
            if m.is_live {
                let pre = state.prematch_win.get(&m.id);
                let pre_line = pre
                    .map(|w| {
                        format!(
                            "Pre: H{:>3.0} D{:>3.0} A{:>3.0} ({}, {}%)",
                            w.p_home,
                            w.p_draw,
                            w.p_away,
                            quality_label(w.quality),
                            w.confidence
                        )
                    })
                    .unwrap_or_else(|| "Pre: (not captured)".to_string());
                format!(
                    "Now: H{:>3.0} D{:>3.0} A{:>3.0} ({}, {}%)\n{}\nΔH: {:+.1}",
                    m.win.p_home,
                    m.win.p_draw,
                    m.win.p_away,
                    quality_label(m.win.quality),
                    m.win.confidence,
                    pre_line,
                    m.win.delta_home
                )
            } else {
                let label = if state.prematch_locked.contains(&m.id) {
                    "Pre:"
                } else {
                    "Pre (locks at kickoff):"
                };
                let mut out = format!(
                    "{} H{:>3.0} D{:>3.0} A{:>3.0}\nModel: {} ({}%)",
                    label,
                    m.win.p_home,
                    m.win.p_draw,
                    m.win.p_away,
                    quality_label(m.win.quality),
                    m.win.confidence
                );
                if state.prediction_show_why {
                    if let Some(ex) = state.prediction_extras.get(&m.id) {
                        let disc = if ex.disc_home.is_some() && ex.disc_away.is_some() {
                            " DISC"
                        } else {
                            ""
                        };
                        out.push_str(&format!(
                            "\nWhy: ANA{:+.1} LU{:+.1}{}",
                            ex.explain.pp_analysis, ex.explain.pp_lineup, disc
                        ));
                    }
                }
                out
            }
        }
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

fn confed_color_for(confed: state::Confederation) -> Color {
    match confed {
        state::Confederation::UEFA => Color::Blue,
        state::Confederation::CONMEBOL => Color::Yellow,
        state::Confederation::CONCACAF => Color::Green,
        state::Confederation::AFC => Color::Red,
        state::Confederation::CAF => Color::Magenta,
        state::Confederation::OFC => Color::Cyan,
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

fn format_countdown(raw: &str, now: DateTime<Utc>) -> String {
    let cleaned = raw.trim();
    if cleaned.is_empty() {
        return "TBD".to_string();
    }
    let Some(dt) = parse_kickoff(cleaned) else {
        return "TBD".to_string();
    };
    let kickoff = Utc.from_utc_datetime(&dt);
    let delta = kickoff.signed_duration_since(now);
    let total_secs = delta.num_seconds();
    if total_secs <= 0 {
        return "LIVE".to_string();
    }
    let total_mins = (total_secs + 59) / 60;
    let days = total_mins / 1440;
    let hours = (total_mins % 1440) / 60;
    let mins = total_mins % 60;

    if days > 0 {
        format!("{days}d {hours:02}h {mins:02}m")
    } else if hours > 0 {
        format!("{hours}h {mins:02}m")
    } else if mins > 0 {
        format!("{mins}m")
    } else {
        "<1m".to_string()
    }
}

fn format_countdown_short(raw: &str, now: DateTime<Utc>) -> String {
    let cleaned = raw.trim();
    if cleaned.is_empty() {
        return "TBD".to_string();
    }
    let Some(dt) = parse_kickoff(cleaned) else {
        return "TBD".to_string();
    };
    let kickoff = Utc.from_utc_datetime(&dt);
    let delta = kickoff.signed_duration_since(now);
    let total_secs = delta.num_seconds();
    if total_secs <= 0 {
        return "LIVE".to_string();
    }
    let total_mins = (total_secs + 59) / 60;
    let days = total_mins / 1440;
    let hours = (total_mins % 1440) / 60;
    let mins = total_mins % 60;

    if days > 0 {
        format!("{days}d{hours:02}")
    } else if hours > 0 {
        format!("{hours}h{mins:02}")
    } else if mins > 0 {
        format!("{mins}m")
    } else {
        "<1m".to_string()
    }
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

fn render_export_overlay(frame: &mut Frame, area: Rect, state: &AppState, anim: UiAnim) {
    let popup_area = centered_rect(70, 22, area);
    frame.render_widget(Clear, popup_area);

    let (title, title_color) = if state.export.done {
        ("Export complete", theme_success())
    } else {
        (
            if anim.pulse_on {
                "Exporting..."
            } else {
                "Exporting"
            },
            theme_accent_2(),
        )
    };

    let block = Block::default()
        .title(Span::styled(
            format!(" {title} "),
            Style::default()
                .fg(title_color)
                .add_modifier(Modifier::BOLD),
        ))
        .borders(Borders::ALL)
        .border_type(BorderType::Double)
        .border_style(Style::default().fg(theme_border()))
        .style(Style::default().bg(theme_panel_bg()))
        .padding(Padding::new(1, 1, 0, 0));
    frame.render_widget(block.clone(), popup_area);

    let inner = block.inner(popup_area);
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(2),
            Constraint::Length(3),
            Constraint::Min(1),
        ])
        .margin(1)
        .split(inner);

    let path = state
        .export
        .path
        .clone()
        .unwrap_or_else(|| "analysis.xlsx".to_string());

    let status = if state.export.total == 0 {
        format!("{path}\n{}", state.export.message)
    } else {
        format!(
            "{path}\n{} ({}/{})",
            state.export.message, state.export.current, state.export.total
        )
    };

    frame.render_widget(
        Paragraph::new(status).style(Style::default().fg(theme_text()).bg(theme_panel_bg())),
        chunks[0],
    );

    let ratio = if state.export.total == 0 {
        0.0
    } else {
        (state.export.current as f64 / state.export.total as f64).clamp(0.0, 1.0)
    };

    let gauge = Gauge::default()
        .ratio(ratio)
        .label(format!("{} {:.0}%", ui_spinner(anim), ratio * 100.0))
        .gauge_style(Style::default().fg(theme_success()).bg(theme_panel_bg()))
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_type(BorderType::Rounded)
                .border_style(Style::default().fg(theme_border_dim()))
                .style(Style::default().bg(theme_panel_bg())),
        );

    frame.render_widget(gauge, chunks[1]);

    let footer = if state.export.done {
        "Press any key to close"
    } else {
        "Please wait..."
    };

    frame.render_widget(
        Paragraph::new(footer).style(Style::default().fg(theme_muted()).bg(theme_panel_bg())),
        chunks[2],
    );
}

fn render_terminal_detail_overlay(
    frame: &mut Frame,
    area: Rect,
    state: &AppState,
    anim: UiAnim,
) {
    let Some(focus) = state.terminal_detail else {
        return;
    };

    let popup_area = centered_rect(80, 80, area);
    frame.render_widget(Clear, popup_area);

    let title = match focus {
        TerminalFocus::MatchList => "Match Details",
        TerminalFocus::Pitch => "Pitch",
        TerminalFocus::EventTape => "Ticker",
        TerminalFocus::Commentary => "Commentary",
        TerminalFocus::Stats => "Stats",
        TerminalFocus::Lineups => "Lineups",
        TerminalFocus::Prediction => "Prediction",
        TerminalFocus::Console => "Console",
    };

    let block = Block::default()
        .title(Span::styled(
            format!(" {} {title} ", ui_spinner(anim)),
            Style::default()
                .fg(theme_accent())
                .add_modifier(Modifier::BOLD),
        ))
        .borders(Borders::ALL)
        .border_type(BorderType::Double)
        .border_style(Style::default().fg(theme_border()))
        .style(Style::default().bg(theme_panel_bg()))
        .padding(Padding::new(1, 1, 0, 0));
    frame.render_widget(block.clone(), popup_area);

    let inner = block.inner(popup_area);
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(1), Constraint::Length(1)])
        .margin(1)
        .split(inner);

    let text = match focus {
        TerminalFocus::MatchList => match_detail_overview_text(state),
        TerminalFocus::Pitch => {
            pitch_text(state, chunks[0].width as usize, chunks[0].height as usize)
        }
        TerminalFocus::EventTape => ticker_full_text(state),
        TerminalFocus::Commentary => commentary_full_text(state),
        TerminalFocus::Stats => stats_full_text(state),
        TerminalFocus::Lineups => lineups_full_text(state),
        TerminalFocus::Prediction => prediction_detail_text(state),
        TerminalFocus::Console => console_full_text(state),
    };

    let (content, line_count) = if matches!(focus, TerminalFocus::Pitch) {
        let count = text.lines().count().max(1);
        (
            Paragraph::new(text).style(Style::default().fg(theme_text()).bg(theme_panel_bg())),
            count,
        )
    } else {
        let count = wrapped_line_count(&text, chunks[0].width);
        (
            Paragraph::new(text)
                .style(Style::default().fg(theme_text()).bg(theme_panel_bg()))
                .wrap(Wrap { trim: false }),
            count.max(1),
        )
    };
    let max_scroll = line_count
        .saturating_sub(chunks[0].height as usize)
        .min(u16::MAX as usize) as u16;
    let scroll = state.terminal_detail_scroll.min(max_scroll);
    let content = content.scroll((scroll, 0));
    frame.render_widget(content, chunks[0]);

    let footer = Paragraph::new("Arrows scroll | Enter/Esc/b close")
        .style(Style::default().fg(theme_muted()).bg(theme_panel_bg()));
    frame.render_widget(footer, chunks[1]);
}

fn wrapped_line_count(text: &str, width: u16) -> usize {
    let width = width.max(1) as usize;
    text.lines()
        .map(|line| {
            let len = line.chars().count();
            let chunks = len.div_ceil(width);
            chunks.max(1)
        })
        .sum()
}

fn render_help_overlay(frame: &mut Frame, area: Rect, anim: UiAnim) {
    let popup_area = centered_rect(60, 60, area);
    frame.render_widget(Clear, popup_area);

    let section_style = Style::default()
        .fg(theme_accent_2())
        .add_modifier(Modifier::BOLD);
    let key_style = Style::default()
        .fg(theme_accent())
        .add_modifier(Modifier::BOLD);
    let desc_style = Style::default().fg(theme_text());
    let dim = Style::default().fg(theme_muted());

    let help_bindings: &[(&str, &[(&str, &str)])] = &[
        (
            "Global",
            &[
                ("1", "Pulse"),
                ("2 / a", "Analysis"),
                ("Enter / d", "Terminal"),
                ("b / Esc", "Back"),
                ("l", "League toggle"),
                ("u", "Upcoming view"),
                ("i", "Fetch match details"),
                ("e", "Export analysis to XLSX"),
                ("r", "Refresh (context)"),
                ("R", "Force refresh"),
                ("p", "Toggle placeholder match"),
                ("?", "Toggle help"),
                ("q", "Quit"),
            ],
        ),
        (
            "Pulse",
            &[("j/k or ↑/↓", "Move/scroll"), ("s", "Cycle sort mode")],
        ),
        (
            "Terminal",
            &[
                ("Tab", "Cycle focus"),
                ("Enter", "Open focused detail"),
                ("Arrows", "Scroll detail view"),
                ("x", "Toggle prediction explain"),
            ],
        ),
        (
            "Analysis / Squad",
            &[
                ("Enter", "Open squad / player detail"),
                ("/ or f", "Search rankings"),
            ],
        ),
        (
            "Player Detail",
            &[
                ("j/k or ↑/↓", "Scroll"),
                ("Enter", "Expand/collapse section"),
            ],
        ),
    ];

    let mut lines: Vec<Line> = Vec::new();
    lines.push(Line::from(Span::styled(
        format!("WC26 Terminal {} Help", ui_spinner(anim)),
        Style::default()
            .fg(theme_accent())
            .add_modifier(Modifier::BOLD),
    )));
    lines.push(Line::from(""));

    for (i, (section, binds)) in help_bindings.iter().enumerate() {
        if i > 0 {
            lines.push(Line::from(""));
        }
        lines.push(Line::from(Span::styled(
            format!("{section}:"),
            section_style,
        )));
        for (key, desc) in *binds {
            lines.push(Line::from(vec![
                Span::styled("  ", dim),
                Span::styled(format!("{key:<14}"), key_style),
                Span::styled(format!(" {desc}"), desc_style),
            ]));
        }
    }

    let help = Paragraph::new(Text::from(lines))
        .block(
            Block::default()
                .title(Span::styled(
                    " Help ",
                    Style::default()
                        .fg(theme_accent())
                        .add_modifier(Modifier::BOLD),
                ))
                .borders(Borders::ALL)
                .border_type(BorderType::Double)
                .border_style(Style::default().fg(theme_border()))
                .style(Style::default().bg(theme_panel_bg()))
                .padding(Padding::new(1, 1, 0, 0)),
        )
        .style(Style::default().fg(theme_text()).bg(theme_panel_bg()));
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

#[cfg(test)]
mod ui_tests {
    use super::{UiColorMode, detect_ui_color_mode_from_values};

    #[test]
    fn color_mode_truecolor_when_colorterm_has_truecolor() {
        let mode = detect_ui_color_mode_from_values("xterm-256color", "truecolor", false);
        assert_eq!(mode, UiColorMode::Truecolor);
    }

    #[test]
    fn color_mode_ansi16_when_no_color_is_set() {
        let mode = detect_ui_color_mode_from_values("xterm-256color", "truecolor", true);
        assert_eq!(mode, UiColorMode::Ansi16);
    }

    #[test]
    fn color_mode_ansi16_without_truecolor_hints() {
        let mode = detect_ui_color_mode_from_values("xterm-256color", "", false);
        assert_eq!(mode, UiColorMode::Ansi16);
    }
}
