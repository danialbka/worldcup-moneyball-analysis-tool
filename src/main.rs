use std::collections::HashMap;
use std::io;
use std::sync::mpsc;
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
use ratatui::widgets::{Block, Borders, Clear, Gauge, Paragraph, Sparkline, Wrap};

use wc26_terminal::{analysis_rankings, feed, http_cache, persist, upcoming_fetch};

use wc26_terminal::state::{
    self, AppState, LeagueMode, PLACEHOLDER_MATCH_ID, PLAYER_DETAIL_SECTIONS, PlayerDetail,
    PlayerStatItem, PulseView, RoleCategory, Screen, TerminalFocus, apply_delta, confed_label,
    league_label, metric_label, placeholder_match_detail, placeholder_match_summary, role_label,
};

struct App {
    state: AppState,
    should_quit: bool,
    cmd_tx: Option<mpsc::Sender<state::ProviderCommand>>,
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
    squad_cache_ttl: Duration,
    player_cache_ttl: Duration,
    prefetch_players_limit: usize,
    auto_warm_mode: AutoWarmMode,
    auto_warm_pending: bool,
    analysis_request_throttle: Duration,
    last_analysis_request: HashMap<LeagueMode, Instant>,
    detail_dist_cache: Option<DetailDistCache>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AutoWarmMode {
    Off,
    Missing,
    Full,
}

impl App {
    fn new(cmd_tx: Option<mpsc::Sender<state::ProviderCommand>>) -> Self {
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
        let squad_cache_ttl = std::env::var("SQUAD_CACHE_SECS")
            .ok()
            .and_then(|val| val.parse::<u64>().ok())
            .unwrap_or(21600)
            .max(60);
        let player_cache_ttl = std::env::var("PLAYER_CACHE_SECS")
            .ok()
            .and_then(|val| val.parse::<u64>().ok())
            .unwrap_or(21600)
            .max(60);
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
        let auto_warm_mode = parse_auto_warm_mode();
        Self {
            state: AppState::new(),
            should_quit: false,
            cmd_tx,
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
            squad_cache_ttl: Duration::from_secs(squad_cache_ttl),
            player_cache_ttl: Duration::from_secs(player_cache_ttl),
            prefetch_players_limit,
            auto_warm_pending: auto_warm_mode != AutoWarmMode::Off,
            auto_warm_mode,
            analysis_request_throttle: Duration::from_secs(analysis_request_throttle),
            last_analysis_request: HashMap::new(),
            detail_dist_cache: None,
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
                                self.request_squad(team.id, team.name.clone(), true);
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
                            self.request_player_detail(player.id, player.name.clone(), true);
                        }
                    }
                }
                Screen::Terminal { .. } => {
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
                        self.request_squad(team_id, team_name, true);
                    }
                } else if matches!(self.state.screen, Screen::PlayerDetail)
                    && let (Some(player_id), Some(player_name)) = (
                        self.state.player_last_id,
                        self.state.player_last_name.clone(),
                    )
                {
                    self.detail_dist_cache = None;
                    self.request_player_detail(player_id, player_name, true);
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
                        self.request_squad(team_id, team_name, true);
                    }
                } else if matches!(self.state.screen, Screen::PlayerDetail)
                    && let (Some(player_id), Some(player_name)) = (
                        self.state.player_last_id,
                        self.state.player_last_name.clone(),
                    )
                {
                    self.request_player_detail(player_id, player_name, true);
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
        let Some(match_id) = self.state.selected_match_id() else {
            if announce {
                self.state.push_log("[INFO] No match selected for details");
            }
            return;
        };
        self.request_match_details_for(&match_id, announce);
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

    fn request_match_details_for(&mut self, match_id: &str, announce: bool) {
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
            && (!announce || has_commentary)
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

    fn request_squad(&mut self, team_id: u32, team_name: String, announce: bool) {
        if let Some(players) = self.state.rankings_cache_squads.get(&team_id).cloned() {
            let has_players = !players.is_empty();
            self.state.squad = players;
            self.state.squad_selected = 0;
            self.state.squad_loading = false;
            self.state.squad_team = Some(team_name.clone());
            self.state.squad_team_id = Some(team_id);
            self.prefetch_players(self.state.squad.iter().map(|p| p.id).collect());
            if has_players {
                let cached_at = self.state.rankings_cache_squads_at.get(&team_id).copied();
                if cache_fresh(cached_at, self.squad_cache_ttl) {
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
            if self.state.squad.is_empty() {
                self.state.squad_loading = true;
                self.state.squad = Vec::new();
                self.state.squad_selected = 0;
            }
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
        let mut cache_hit = false;
        if let Some(cached) = self.state.rankings_cache_players.get(&player_id).cloned() {
            let cached_at = self
                .state
                .rankings_cache_players_at
                .get(&player_id)
                .copied();
            let is_stub = state::player_detail_is_stub(&cached);
            self.state.player_detail = Some(cached);
            self.state.player_loading = false;
            cache_hit = true;
            if cache_fresh(cached_at, self.player_cache_ttl) && !is_stub {
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
                let cached_at = self.state.rankings_cache_players_at.get(id).copied();
                let is_stub = cached.map(state::player_detail_is_stub).unwrap_or(true);
                !cache_fresh(cached_at, self.player_cache_ttl) || is_stub
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

        // If the user is actively looking at commentary, refresh full match details for the
        // selected live match. Background refreshes otherwise use the basic endpoint to reduce
        // load.
        let wants_commentary = matches!(self.state.screen, Screen::Terminal { .. })
            && (self.state.terminal_focus == TerminalFocus::Commentary
                || self.state.terminal_detail == Some(TerminalFocus::Commentary));
        let selected_live_id = self
            .state
            .selected_match()
            .filter(|m| m.is_live && m.id != PLACEHOLDER_MATCH_ID)
            .map(|m| m.id.clone());
        if wants_commentary {
            if let Some(match_id) = selected_live_id.as_deref() {
                let last = self.last_detail_refresh.get(match_id);
                let should_fetch = last
                    .map(|t| t.elapsed() >= self.commentary_refresh)
                    .unwrap_or(true);
                if should_fetch {
                    self.request_match_details_for(match_id, false);
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
            if wants_commentary && selected_live_id.as_deref() == Some(match_id.as_str()) {
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
    feed::spawn_provider(tx, cmd_rx);

    let mut app = App::new(Some(cmd_tx));
    // Load cached rankings/analysis (if any) for current league.
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

fn run_app<B: Backend>(
    terminal: &mut Terminal<B>,
    app: &mut App,
    rx: mpsc::Receiver<state::Delta>,
) -> io::Result<()> {
    let poll_rate = Duration::from_millis(250);
    let heartbeat_rate = Duration::from_secs(1);
    let mut last_draw = Instant::now() - heartbeat_rate;
    let mut needs_redraw = true;

    loop {
        let mut changed = false;
        while let Ok(delta) = rx.try_recv() {
            apply_delta(&mut app.state, delta);
            changed = true;
        }
        if let Some(ids) = app.state.squad_prefetch_pending.take() {
            app.prefetch_players(ids);
        }
        if matches!(app.state.screen, Screen::Analysis)
            && app.state.analysis_tab == state::AnalysisTab::RoleRankings
            && app.state.rankings_dirty
        {
            app.recompute_rankings_from_cache();
            changed = true;
        }
        let export_was_active = app.state.export.active;
        app.state.maybe_clear_export(Instant::now());
        if export_was_active != app.state.export.active {
            changed = true;
        }

        app.maybe_refresh_upcoming();
        app.maybe_refresh_match_details();
        app.maybe_auto_warm_rankings();
        app.maybe_hover_prefetch_match_details();

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
    // Force black background across the entire frame.
    frame.render_widget(
        Block::default().style(Style::default().bg(Color::Black)),
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

    let header = Paragraph::new(header_styled(&app.state)).style(Style::default().bg(Color::Black));
    frame.render_widget(header, chunks[0]);

    match app.state.screen {
        Screen::Pulse => render_pulse(frame, chunks[1], &app.state),
        Screen::Terminal { .. } => render_terminal(frame, chunks[1], &app.state),
        Screen::Analysis => render_analysis(frame, chunks[1], &app.state),
        Screen::Squad => render_squad(frame, chunks[1], &app.state),
        Screen::PlayerDetail => render_player_detail(frame, chunks[1], app),
    }

    let footer = Paragraph::new(footer_styled(&app.state))
        .style(Style::default().bg(Color::Black))
        .block(
            Block::default()
                .borders(Borders::TOP)
                .style(Style::default().bg(Color::Black)),
        );
    frame.render_widget(footer, chunks[2]);

    if app.state.export.active {
        render_export_overlay(frame, frame.size(), &app.state);
    }
    if app.state.help_overlay {
        render_help_overlay(frame, frame.size());
    }
    if app.state.terminal_detail.is_some() {
        render_terminal_detail_overlay(frame, frame.size(), &app.state);
    }
}

fn header_styled(state: &AppState) -> Line<'static> {
    let sep = Span::styled(" | ", on_black(Style::default().fg(Color::DarkGray)));

    match state.screen {
        Screen::Pulse => Line::from(vec![
            Span::styled(
                "WC26 PULSE",
                on_black(
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD),
                ),
            ),
            sep.clone(),
            Span::styled(
                league_label(state.league_mode).to_string(),
                on_black(Style::default().fg(Color::Yellow)),
            ),
            sep.clone(),
            Span::styled(
                pulse_view_label(state.pulse_view).to_string(),
                on_black(Style::default().fg(Color::Magenta)),
            ),
            sep.clone(),
            Span::styled("Sort: ", on_black(Style::default().fg(Color::DarkGray))),
            Span::styled(
                sort_label(state.sort).to_string(),
                on_black(Style::default().fg(Color::Green)),
            ),
        ]),
        Screen::Terminal { .. } => Line::from(Span::styled(
            "WC26 TERMINAL",
            on_black(
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            ),
        )),
        Screen::Analysis => {
            let updated = state.analysis_updated.as_deref().unwrap_or("-");
            let status_label = if state.analysis_loading {
                "LOADING"
            } else {
                "READY"
            };
            let status_color = if state.analysis_loading {
                Color::Yellow
            } else {
                Color::Green
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
                    on_black(
                        Style::default()
                            .fg(Color::Cyan)
                            .add_modifier(Modifier::BOLD),
                    ),
                ),
                sep.clone(),
                Span::styled(
                    league_label(state.league_mode).to_string(),
                    on_black(Style::default().fg(Color::Yellow)),
                ),
                sep.clone(),
                Span::styled("Tab: ", on_black(Style::default().fg(Color::DarkGray))),
                Span::styled(
                    tab.to_string(),
                    on_black(Style::default().fg(Color::Magenta)),
                ),
                sep.clone(),
                Span::styled(
                    format!("Teams: {}", state.analysis.len()),
                    on_black(Style::default().fg(Color::White)),
                ),
                sep.clone(),
                Span::styled(
                    format!("FIFA: {updated}"),
                    on_black(Style::default().fg(Color::White)),
                ),
                sep.clone(),
                Span::styled(
                    format!("Fetched: {fetched}"),
                    on_black(Style::default().fg(Color::DarkGray)),
                ),
                sep.clone(),
                Span::styled(
                    status_label.to_string(),
                    on_black(Style::default().fg(status_color)),
                ),
            ])
        }
        Screen::Squad => {
            let team = state.squad_team.as_deref().unwrap_or("-");
            let status_label = if state.squad_loading {
                "LOADING"
            } else {
                "READY"
            };
            let status_color = if state.squad_loading {
                Color::Yellow
            } else {
                Color::Green
            };
            Line::from(vec![
                Span::styled(
                    "WC26 SQUAD",
                    on_black(
                        Style::default()
                            .fg(Color::Cyan)
                            .add_modifier(Modifier::BOLD),
                    ),
                ),
                sep.clone(),
                Span::styled(
                    format!("Team: {team}"),
                    on_black(Style::default().fg(Color::Yellow)),
                ),
                sep.clone(),
                Span::styled(
                    format!("Players: {}", state.squad.len()),
                    on_black(Style::default().fg(Color::White)),
                ),
                sep.clone(),
                Span::styled(
                    status_label.to_string(),
                    on_black(Style::default().fg(status_color)),
                ),
            ])
        }
        Screen::PlayerDetail => Line::from(Span::styled(
            "WC26 PLAYER",
            on_black(
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            ),
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

fn footer_styled(state: &AppState) -> Line<'static> {
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
            ("r", "Refresh"),
            ("?", "Help"),
            ("q", "Quit"),
        ],
        Screen::PlayerDetail => &[
            ("1", "Pulse"),
            ("b/Esc", "Back"),
            ("j/k/↑/↓", "Scroll"),
            ("r", "Refresh"),
            ("?", "Help"),
            ("q", "Quit"),
        ],
    };
    let mut spans: Vec<Span> = Vec::new();
    for (i, (key, desc)) in bindings.iter().enumerate() {
        if i > 0 {
            spans.push(Span::styled(
                " │ ",
                on_black(Style::default().fg(Color::DarkGray)),
            ));
        }
        spans.push(Span::styled(
            key.to_string(),
            on_black(
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            ),
        ));
        spans.push(Span::styled(
            format!(" {desc}"),
            on_black(Style::default().fg(Color::Gray)),
        ));
    }
    Line::from(spans)
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
    let rows = state.pulse_live_rows();
    if rows.is_empty() {
        let empty_style = Style::default()
            .fg(Color::DarkGray)
            .add_modifier(Modifier::ITALIC);
        let empty = Paragraph::new(Text::styled(
            "No matches for this league",
            on_black(empty_style),
        ))
        .style(Style::default().bg(Color::Black));
        frame.render_widget(empty, list_area);
        return;
    }

    const ROW_HEIGHT: u16 = 3;
    if list_area.height < ROW_HEIGHT {
        let empty_style = Style::default()
            .fg(Color::DarkGray)
            .add_modifier(Modifier::ITALIC);
        let empty = Paragraph::new(Text::styled(
            "Pulse list needs more height",
            on_black(empty_style),
        ))
        .style(Style::default().bg(Color::Black));
        frame.render_widget(empty, list_area);
        return;
    }

    let visible = (list_area.height / ROW_HEIGHT) as usize;
    let (start, end) = visible_range(state.selected, rows.len(), visible);

    let now = Utc::now();
    for (i, idx) in (start..end).enumerate() {
        let row_area = Rect {
            x: list_area.x,
            y: list_area.y + (i as u16) * ROW_HEIGHT,
            width: list_area.width,
            height: ROW_HEIGHT,
        };

        let selected = idx == state.selected;
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

                let row_style = if is_not_started {
                    if selected {
                        Style::default().fg(Color::Gray).bg(Color::Rgb(30, 30, 46))
                    } else {
                        Style::default().fg(Color::DarkGray)
                    }
                } else if selected {
                    Style::default().fg(Color::White).bg(Color::Rgb(30, 30, 46))
                } else {
                    Style::default()
                };

                if selected {
                    frame.render_widget(Block::default().style(row_style), row_area);
                }

                let time = if m.is_live {
                    format!("{}'", m.minute)
                } else if is_finished {
                    "FT".to_string()
                } else {
                    state
                        .upcoming
                        .iter()
                        .find(|u| u.id == m.id)
                        .map(|u| format_countdown_short(&u.kickoff, now))
                        .unwrap_or_else(|| "KO".to_string())
                };
                let match_name = format!("{}-{}", m.home, m.away);
                let score = if is_not_started {
                    "--".to_string()
                } else {
                    format!("{}-{}", m.score_home, m.score_away)
                };

                // Time cell: green for live, dim for finished
                let time_style = if m.is_live {
                    row_style.fg(Color::Green)
                } else if is_finished {
                    row_style.fg(Color::DarkGray)
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
                    let chart = win_line_chart(&values, selected);
                    frame.render_widget(chart, cols[3]);

                    render_cell_text(frame, cols[4], &hda, row_style);

                    // Delta: green for positive (home gaining), red for negative
                    let delta_color = if delta_val > 1.0 {
                        Color::Green
                    } else if delta_val < -1.0 {
                        Color::Red
                    } else {
                        Color::Gray
                    };
                    let bg = if selected {
                        Some(Color::DarkGray)
                    } else {
                        None
                    };
                    let mut delta_style = Style::default().fg(delta_color);
                    if let Some(bg_color) = bg {
                        delta_style = delta_style.bg(bg_color);
                    }
                    render_cell_text(frame, cols[5], &delta, delta_style);

                    // Quality badge: colored by model tier
                    let quality_color = match m.win.quality {
                        state::ModelQuality::Track => Color::Green,
                        state::ModelQuality::Event => Color::Yellow,
                        state::ModelQuality::Basic => Color::DarkGray,
                    };
                    let mut quality_style = Style::default().fg(quality_color);
                    if let Some(bg_color) = bg {
                        quality_style = quality_style.bg(bg_color);
                    }
                    render_cell_text(frame, cols[6], &quality, quality_style);

                    // Confidence: dim when low
                    let conf_color = if m.win.confidence >= 70 {
                        Color::Green
                    } else if m.win.confidence >= 40 {
                        Color::Yellow
                    } else {
                        Color::DarkGray
                    };
                    let mut conf_style = Style::default().fg(conf_color);
                    if let Some(bg_color) = bg {
                        conf_style = conf_style.bg(bg_color);
                    }
                    render_cell_text(frame, cols[7], &conf, conf_style);
                }
            }
            state::PulseLiveRow::Upcoming(upcoming_idx) => {
                let Some(u) = state.upcoming.get(upcoming_idx) else {
                    continue;
                };

                let row_style = if selected {
                    Style::default().fg(Color::Gray).bg(Color::Rgb(30, 30, 46))
                } else {
                    Style::default().fg(Color::DarkGray)
                };

                if selected {
                    frame.render_widget(Block::default().style(row_style), row_area);
                }

                let time = format_countdown_short(&u.kickoff, now);
                let match_name = format!("{}-{}", u.home, u.away);

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
        let empty_style = Style::default()
            .fg(Color::DarkGray)
            .add_modifier(Modifier::ITALIC);
        let empty = Paragraph::new(Text::styled(
            "No upcoming matches for this league",
            on_black(empty_style),
        ))
        .style(Style::default().bg(Color::Black));
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
    let style = Style::default()
        .fg(Color::Cyan)
        .add_modifier(Modifier::BOLD | Modifier::UNDERLINED);

    render_cell_text(frame, cols[0], "Time", style);
    render_cell_text(frame, cols[1], "Match", style);
    render_cell_text(frame, cols[2], "Score", style);
    render_cell_text(frame, cols[3], "Win% Line", style);
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
    let style = Style::default()
        .fg(Color::Cyan)
        .add_modifier(Modifier::BOLD | Modifier::UNDERLINED);
    let sep_style = Style::default().fg(Color::DarkGray);

    render_cell_text(frame, cols[0], "Starts In", style);
    render_vseparator(frame, cols[1], sep_style);
    render_cell_text(frame, cols[2], "Match", style);
    render_vseparator(frame, cols[3], sep_style);
    render_cell_text(frame, cols[4], "League", style);
    render_vseparator(frame, cols[5], sep_style);
    render_cell_text(frame, cols[6], "Round", style);
}

fn render_analysis(frame: &mut Frame, area: Rect, state: &AppState) {
    match state.analysis_tab {
        state::AnalysisTab::Teams => render_analysis_teams(frame, area, state),
        state::AnalysisTab::RoleRankings => render_analysis_rankings(frame, area, state),
    }
}

fn render_analysis_teams(frame: &mut Frame, area: Rect, state: &AppState) {
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
        let empty_style = Style::default()
            .fg(Color::DarkGray)
            .add_modifier(Modifier::ITALIC);
        let empty = Paragraph::new(Text::styled(message, on_black(empty_style)))
            .style(Style::default().bg(Color::Black));
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
            Style::default().fg(Color::White).bg(Color::Rgb(30, 30, 46))
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
        let updated = row.fifa_updated.clone().unwrap_or_else(|| "-".to_string());
        let host = if row.host { "yes" } else { "-" };

        // Confederation colored by region
        let confed_color = confed_color_for(row.confed);
        let confed_style = if selected {
            Style::default().fg(confed_color).bg(Color::Rgb(30, 30, 46))
        } else {
            Style::default().fg(confed_color)
        };
        let sep_style = Style::default().fg(Color::DarkGray);
        render_cell_text(frame, cols[0], confed, confed_style);
        render_vseparator(frame, cols[1], sep_style);
        render_cell_text(frame, cols[2], &row.name, row_style);
        render_vseparator(frame, cols[3], sep_style);
        // Rank: highlight top 10
        let rank_style = if row.fifa_rank.map(|r| r <= 10).unwrap_or(false) {
            row_style.fg(Color::Yellow)
        } else {
            row_style
        };
        render_cell_text(frame, cols[4], &rank, rank_style);
        render_vseparator(frame, cols[5], sep_style);
        render_cell_text(frame, cols[6], &points, row_style);
        render_vseparator(frame, cols[7], sep_style);
        render_cell_text(frame, cols[8], &updated, row_style.fg(Color::DarkGray));
        render_vseparator(frame, cols[9], sep_style);
        // Host badge: green
        let host_style = if row.host {
            row_style.fg(Color::Green)
        } else {
            row_style.fg(Color::DarkGray)
        };
        render_cell_text(frame, cols[10], host, host_style);
    }
}

fn render_analysis_rankings(frame: &mut Frame, area: Rect, state: &AppState) {
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
    let sep = Span::styled(" | ", on_black(Style::default().fg(Color::DarkGray)));
    let mut header_spans = vec![
        Span::styled(
            "Role Rankings",
            on_black(
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            ),
        ),
        sep.clone(),
        Span::styled("Role: ", on_black(Style::default().fg(Color::DarkGray))),
        Span::styled(
            role.to_string(),
            on_black(
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            ),
        ),
        sep.clone(),
        Span::styled("Metric: ", on_black(Style::default().fg(Color::DarkGray))),
        Span::styled(
            metric.to_string(),
            on_black(
                Style::default()
                    .fg(Color::Magenta)
                    .add_modifier(Modifier::BOLD),
            ),
        ),
    ];
    if state.rankings_loading {
        header_spans.push(sep.clone());
        let progress_color = Color::Yellow;
        if state.rankings_progress_total > 0 {
            header_spans.push(Span::styled(
                format!(
                    "{} ({}/{})",
                    state.rankings_progress_message,
                    state.rankings_progress_current,
                    state.rankings_progress_total
                ),
                Style::default().fg(progress_color),
            ));
        } else {
            header_spans.push(Span::styled(
                state.rankings_progress_message.clone(),
                on_black(Style::default().fg(progress_color)),
            ));
        }
    }
    frame.render_widget(Paragraph::new(Line::from(header_spans)), sections[0]);

    let search_line = if state.rankings_search_active {
        Line::from(vec![
            Span::styled(
                "Search [/]: ",
                on_black(
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD),
                ),
            ),
            Span::styled(
                state.rankings_search.clone(),
                on_black(Style::default().fg(Color::Yellow)),
            ),
            Span::styled("▌", on_black(Style::default().fg(Color::Yellow))),
        ])
    } else if state.rankings_search.is_empty() {
        Line::from(Span::styled(
            "Search [/]",
            on_black(Style::default().fg(Color::DarkGray)),
        ))
    } else {
        Line::from(vec![
            Span::styled(
                "Search [/]: ",
                on_black(Style::default().fg(Color::DarkGray)),
            ),
            Span::styled(
                state.rankings_search.clone(),
                on_black(Style::default().fg(Color::Gray)),
            ),
        ])
    };
    frame.render_widget(Paragraph::new(search_line), sections[1]);

    let list_area = sections[2];
    if list_area.height == 0 {
        return;
    }

    if state.rankings.is_empty() {
        let message = if state.rankings_loading {
            "Loading role rankings..."
        } else {
            "No role ranking data yet (press r to warm cache)"
        };
        let empty_style = Style::default()
            .fg(Color::DarkGray)
            .add_modifier(Modifier::ITALIC);
        let empty = Paragraph::new(Text::styled(message, on_black(empty_style)))
            .style(Style::default().bg(Color::Black));
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
            .fg(Color::DarkGray)
            .add_modifier(Modifier::ITALIC);
        let empty = Paragraph::new(Text::styled(message, on_black(empty_style)))
            .style(Style::default().bg(Color::Black));
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
        let row_style = if selected {
            Style::default().fg(Color::White).bg(Color::Rgb(30, 30, 46))
        } else {
            Style::default()
        };
        if selected {
            frame.render_widget(Block::default().style(row_style), row_area);
        }

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
            "{rank:>3}. {:<24} {:<18} Score {}  R {rating}  Club {}",
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
            Span::styled("Selected: ", on_black(Style::default().fg(Color::DarkGray))),
            Span::styled(
                truncate(&selected.player_name, 28),
                on_black(
                    Style::default()
                        .fg(Color::White)
                        .add_modifier(Modifier::BOLD),
                ),
            ),
            Span::styled(
                format!(" ({})", truncate(&selected.team_name, 22)),
                on_black(Style::default().fg(Color::Gray)),
            ),
            Span::styled("  Score ", on_black(Style::default().fg(Color::DarkGray))),
            Span::styled(
                score_text,
                on_black(
                    Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD),
                ),
            ),
            Span::styled("  R ", on_black(Style::default().fg(Color::DarkGray))),
            Span::styled(rating_text, on_black(Style::default().fg(Color::Magenta))),
        ]));

        lines.push(Line::from(Span::styled(
            "Top contributors",
            on_black(
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            ),
        )));

        if factors.is_empty() {
            lines.push(Line::from(Span::styled(
                "No breakdown available (warm cache / insufficient stat coverage)",
                on_black(
                    Style::default()
                        .fg(Color::DarkGray)
                        .add_modifier(Modifier::ITALIC),
                ),
            )));
        } else {
            for f in factors
                .iter()
                .take((detail_area.height as usize).saturating_sub(2))
            {
                let impact = f.weight * f.z;
                let impact_style = if impact >= 0.0 {
                    on_black(Style::default().fg(Color::Green))
                } else {
                    on_black(Style::default().fg(Color::Red))
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
                    Span::styled(
                        truncate(&f.label, 20),
                        on_black(Style::default().fg(Color::White)),
                    ),
                    Span::styled(tail, on_black(Style::default().fg(Color::DarkGray))),
                ]));
            }
        }

        let detail = Paragraph::new(lines)
            .style(Style::default().bg(Color::Black))
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

fn render_analysis_header(frame: &mut Frame, area: Rect, widths: &[Constraint]) {
    let cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints(widths)
        .split(area);
    let style = Style::default()
        .fg(Color::Cyan)
        .add_modifier(Modifier::BOLD | Modifier::UNDERLINED);
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
        let empty_style = Style::default()
            .fg(Color::DarkGray)
            .add_modifier(Modifier::ITALIC);
        let empty = Paragraph::new(Text::styled(message, on_black(empty_style)))
            .style(Style::default().bg(Color::Black));
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
            Style::default().fg(Color::White).bg(Color::Rgb(30, 30, 46))
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
    let style = Style::default()
        .fg(Color::Cyan)
        .add_modifier(Modifier::BOLD | Modifier::UNDERLINED);
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

fn render_player_detail(frame: &mut Frame, area: Rect, app: &mut App) {
    let state = &app.state;
    let block = Block::default()
        .title(Span::styled(
            " Player Detail ",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ))
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Rgb(60, 60, 80)));
    let inner = block.inner(area);
    frame.render_widget(block, area);

    if inner.height == 0 || inner.width == 0 {
        return;
    }

    if state.player_loading {
        let empty_style = Style::default()
            .fg(Color::DarkGray)
            .add_modifier(Modifier::ITALIC);
        let text = Paragraph::new(Text::styled(
            "Loading player details...",
            on_black(empty_style),
        ))
        .style(Style::default().bg(Color::Black));
        frame.render_widget(text, inner);
        return;
    }

    let Some(detail) = state.player_detail.as_ref() else {
        let empty_style = Style::default()
            .fg(Color::DarkGray)
            .add_modifier(Modifier::ITALIC);
        let text = Paragraph::new(Text::styled("No player data yet", on_black(empty_style)))
            .style(Style::default().bg(Color::Black));
        frame.render_widget(text, inner);
        return;
    };

    if inner.height < 8 {
        let text = player_detail_text(detail);
        let paragraph = Paragraph::new(text).scroll((state.player_detail_scroll, 0));
        frame.render_widget(paragraph, inner);
        return;
    }

    let player_id = state.player_last_id;
    let cache_needs_rebuild = app
        .detail_dist_cache
        .as_ref()
        .map(|cache| cache.player_id != player_id)
        .unwrap_or(true);
    if cache_needs_rebuild {
        let dist = build_stat_distributions(state);
        app.detail_dist_cache = Some(DetailDistCache { player_id, dist });
    }
    let dist = match app.detail_dist_cache.as_ref() {
        Some(cache) => &cache.dist,
        None => {
            let dist = build_stat_distributions(state);
            app.detail_dist_cache = Some(DetailDistCache { player_id, dist });
            &app.detail_dist_cache.as_ref().expect("detail dist").dist
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
    let league_text = player_league_stats_text_styled(detail, dist);
    let top_text = player_top_stats_text_styled(detail, dist);
    let traits_text = Text::from(traits_text);
    let other_text = player_season_performance_text_styled(detail, dist);
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

struct DetailDistCache {
    player_id: Option<u32>,
    dist: StatDistributions,
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
            lines.push(Line::from(vec![
                Span::raw(format!("  {}: ", stat.title)),
                Span::styled(value, style),
            ]));
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
            lines.push(Line::from(vec![
                Span::raw(format!("  {}: ", stat.title)),
                Span::styled(value, style),
            ]));
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

fn player_top_stats_text_styled(detail: &PlayerDetail, dist: &StatDistributions) -> Text<'static> {
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
        lines.push(Line::from(vec![
            Span::raw(format!("{}: ", stat.title)),
            Span::styled(value, style),
        ]));
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

            lines.push(Line::from(vec![
                Span::raw(format!("  {}: ", item.title)),
                Span::styled(item.total.clone(), total_style),
                Span::raw(" | "),
                Span::styled(per90.to_string(), per90_style),
            ]));
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
            Style::default().fg(Color::Yellow),
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        )
    } else {
        (
            Style::default().fg(Color::Rgb(60, 60, 80)),
            Style::default().fg(Color::DarkGray),
        )
    };
    let scroll_indicator = Span::styled(
        format!("  {current}/{total}"),
        Style::default().fg(Color::DarkGray),
    );
    let block = Block::default()
        .title(Line::from(vec![
            Span::styled(title.to_string(), title_style),
            scroll_indicator,
        ]))
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

fn on_black(mut style: Style) -> Style {
    // Ratatui widgets often overwrite the entire cell style.
    // If a widget style doesn't specify a bg, that cell's bg becomes "reset",
    // which can show up as white in light-themed terminals (especially on loading/empty screens).
    // Force a black background unless a caller explicitly chose another bg.
    match style.bg {
        None | Some(Color::Reset) => style.bg = Some(Color::Black),
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
        text.push('│');
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

fn win_line_chart(values: &[u64], selected: bool) -> Sparkline<'_> {
    let mut style = Style::default().fg(Color::Green);
    if selected {
        style = style.bg(Color::DarkGray);
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

fn terminal_block(title: &str, focused: bool) -> Block<'_> {
    if focused {
        Block::default()
            .title(Span::styled(
                title,
                on_black(
                    Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD),
                ),
            ))
            .borders(Borders::ALL)
            .border_style(on_black(
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            ))
    } else {
        Block::default()
            .title(Span::styled(
                title,
                on_black(Style::default().fg(Color::DarkGray)),
            ))
            .borders(Borders::ALL)
            .border_style(on_black(Style::default().fg(Color::Rgb(60, 60, 80))))
    }
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
    let left_match = Paragraph::new(match_list).block(terminal_block(
        "Match List",
        state.terminal_focus == TerminalFocus::MatchList,
    ));
    frame.render_widget(left_match, left_chunks[0]);

    let standings =
        Paragraph::new("Standings placeholder").block(terminal_block("Group Mini", false));
    frame.render_widget(standings, left_chunks[1]);

    render_pitch(frame, middle_chunks[0], state);

    let (tape_title, tape_text, tape_focus) = match state.terminal_focus {
        TerminalFocus::Commentary => ("Commentary", commentary_tape_text(state), true),
        _ => (
            "Ticker",
            event_tape_text(state),
            state.terminal_focus == TerminalFocus::EventTape,
        ),
    };
    let tape = Paragraph::new(tape_text).block(terminal_block(tape_title, tape_focus));
    frame.render_widget(tape, middle_chunks[1]);

    let stats_text = stats_text(state);
    let stats = Paragraph::new(stats_text).block(terminal_block(
        "Stats",
        state.terminal_focus == TerminalFocus::Stats,
    ));
    frame.render_widget(stats, right_chunks[0]);

    render_lineups(frame, right_chunks[1], state);

    let preds_text = prediction_text(state);
    let preds = Paragraph::new(preds_text).block(terminal_block(
        "Prediction",
        state.terminal_focus == TerminalFocus::Prediction,
    ));
    frame.render_widget(preds, right_chunks[2]);

    let console = Paragraph::new(console_text(state)).block(terminal_block(
        "Console",
        state.terminal_focus == TerminalFocus::Console,
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
            "> "
        } else {
            "  "
        };
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

fn render_lineups(frame: &mut Frame, area: Rect, state: &AppState) {
    let block = terminal_block("Lineups", state.terminal_focus == TerminalFocus::Lineups);
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
    let left = sides.first();
    let right = sides.get(1);

    let cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(inner);

    render_lineup_side(frame, cols[0], left);
    render_lineup_side(frame, cols[1], right);
}

fn render_pitch(frame: &mut Frame, area: Rect, state: &AppState) {
    let block = terminal_block("Pitch", state.terminal_focus == TerminalFocus::Pitch);
    let inner = block.inner(area);
    frame.render_widget(block, area);

    if inner.width == 0 || inner.height == 0 {
        return;
    }

    let text = pitch_text(state, inner.width as usize, inner.height as usize);
    frame.render_widget(Paragraph::new(text), inner);
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
            "Contrib (home win pp): HA {:+.1}, FIFA {:+.1}, Lineup {:+.1}",
            ex.explain.pp_home_adv, ex.explain.pp_analysis, ex.explain.pp_lineup
        ));
        lines.push(format!(
            "Baseline: H{:.1} D{:.1} A{:.1}",
            ex.explain.p_home_baseline, ex.explain.p_draw_baseline, ex.explain.p_away_baseline
        ));
        lines.push(format!(
            "HA only:  H{:.1} D{:.1} A{:.1}",
            ex.explain.p_home_ha, ex.explain.p_draw_ha, ex.explain.p_away_ha
        ));
        lines.push(format!(
            "Analysis: H{:.1} D{:.1} A{:.1}",
            ex.explain.p_home_analysis, ex.explain.p_draw_analysis, ex.explain.p_away_analysis
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
        let a_h = ex
            .s_home_analysis
            .map(|v| format!("{v:.2}"))
            .unwrap_or_else(|| "-".to_string());
        let a_a = ex
            .s_away_analysis
            .map(|v| format!("{v:.2}"))
            .unwrap_or_else(|| "-".to_string());
        lines.push(format!("Analysis strength: home={a_h} away={a_a}"));

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
                        out.push_str(&format!(
                            "\nWhy: HA{:+.1} ANA{:+.1} LU{:+.1}",
                            ex.explain.pp_home_adv, ex.explain.pp_analysis, ex.explain.pp_lineup
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

fn render_export_overlay(frame: &mut Frame, area: Rect, state: &AppState) {
    let popup_area = centered_rect(70, 22, area);
    frame.render_widget(Clear, popup_area);

    let (title, title_color) = if state.export.done {
        ("Export complete", Color::Green)
    } else {
        ("Exporting...", Color::Yellow)
    };

    let block = Block::default()
        .title(Span::styled(
            format!(" {title} "),
            Style::default()
                .fg(title_color)
                .add_modifier(Modifier::BOLD),
        ))
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::DarkGray));
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

    frame.render_widget(Paragraph::new(status), chunks[0]);

    let ratio = if state.export.total == 0 {
        0.0
    } else {
        (state.export.current as f64 / state.export.total as f64).clamp(0.0, 1.0)
    };

    let gauge = Gauge::default()
        .ratio(ratio)
        .label(format!("{:.0}%", ratio * 100.0))
        .gauge_style(Style::default().fg(Color::LightGreen))
        .block(Block::default().borders(Borders::ALL));

    frame.render_widget(gauge, chunks[1]);

    let footer = if state.export.done {
        "Press any key to close"
    } else {
        "Please wait"
    };

    frame.render_widget(Paragraph::new(footer), chunks[2]);
}

fn render_terminal_detail_overlay(frame: &mut Frame, area: Rect, state: &AppState) {
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
            format!(" {title} "),
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ))
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::DarkGray));
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
        (Paragraph::new(text), count)
    } else {
        let count = wrapped_line_count(&text, chunks[0].width);
        (
            Paragraph::new(text).wrap(Wrap { trim: false }),
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
        .style(Style::default().fg(Color::DarkGray));
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

fn render_help_overlay(frame: &mut Frame, area: Rect) {
    let popup_area = centered_rect(60, 60, area);
    frame.render_widget(Clear, popup_area);

    let section_style = Style::default()
        .fg(Color::Yellow)
        .add_modifier(Modifier::BOLD);
    let key_style = Style::default()
        .fg(Color::Cyan)
        .add_modifier(Modifier::BOLD);
    let desc_style = Style::default().fg(Color::Gray);
    let dim = Style::default().fg(Color::DarkGray);

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
                ("r", "Refresh analysis/squad/player"),
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
        "WC26 Terminal — Help",
        Style::default()
            .fg(Color::Cyan)
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
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD),
                ))
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::DarkGray)),
        )
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
