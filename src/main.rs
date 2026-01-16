use std::collections::HashMap;
use std::io;
use std::sync::mpsc;
use std::time::{Duration, Instant, SystemTime};

use chrono::{DateTime, Local, NaiveDateTime, TimeZone, Utc};
use crossterm::event::{
    self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyEvent, KeyEventKind,
};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::prelude::*;
use ratatui::style::{Color, Modifier, Style};
use ratatui::widgets::{Block, Borders, Clear, Gauge, Paragraph, Sparkline};

mod analysis_export;
mod analysis_fetch;
mod analysis_rankings;
mod fake_feed;
mod http_cache;
mod http_client;
mod persist;
mod state;
mod upcoming_fetch;

use crate::state::{
    AppState, LeagueMode, PLACEHOLDER_MATCH_ID, PLAYER_DETAIL_SECTIONS, PlayerDetail, PulseView,
    Screen, apply_delta, confed_label, league_label, metric_label, placeholder_match_detail,
    placeholder_match_summary, role_label,
};

struct App {
    state: AppState,
    should_quit: bool,
    cmd_tx: Option<mpsc::Sender<state::ProviderCommand>>,
    upcoming_refresh: Duration,
    last_upcoming_refresh: Instant,
    upcoming_cache_ttl: Duration,
    detail_refresh: Duration,
    last_detail_refresh: HashMap<String, Instant>,
    detail_cache_ttl: Duration,
    squad_cache_ttl: Duration,
    player_cache_ttl: Duration,
    prefetch_players_limit: usize,
    auto_warm_mode: AutoWarmMode,
    auto_warm_pending: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AutoWarmMode {
    Off,
    Missing,
    Full,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PlayerPositionGroup {
    Goalkeeper,
    Defender,
    Midfielder,
    Forward,
    Unknown,
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
        let detail_cache_ttl = std::env::var("DETAILS_CACHE_SECS")
            .ok()
            .and_then(|val| val.parse::<u64>().ok())
            .unwrap_or(3600)
            .max(30);
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
        let auto_warm_mode = parse_auto_warm_mode();
        Self {
            state: AppState::new(),
            should_quit: false,
            cmd_tx,
            upcoming_refresh: Duration::from_secs(upcoming_refresh),
            last_upcoming_refresh: Instant::now(),
            upcoming_cache_ttl: Duration::from_secs(upcoming_cache_ttl),
            detail_refresh: Duration::from_secs(detail_refresh),
            last_detail_refresh: HashMap::new(),
            detail_cache_ttl: Duration::from_secs(detail_cache_ttl),
            squad_cache_ttl: Duration::from_secs(squad_cache_ttl),
            player_cache_ttl: Duration::from_secs(player_cache_ttl),
            prefetch_players_limit,
            auto_warm_pending: auto_warm_mode != AutoWarmMode::Off,
            auto_warm_mode,
        }
    }

    fn on_key(&mut self, key: KeyEvent) {
        if self.state.export.active {
            if self.state.export.done {
                self.state.export = crate::state::ExportState::new();
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
                        self.request_match_details(true);
                    }
                }
                Screen::Analysis => {
                    if self.state.analysis_tab == crate::state::AnalysisTab::Teams {
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
                            let mut rows: Vec<&crate::state::RoleRankingEntry> = self
                                .state
                                .rankings
                                .iter()
                                .filter(|r| r.role == self.state.rankings_role)
                                .collect();
                            match self.state.rankings_metric {
                                crate::state::RankMetric::Attacking => {
                                    rows.sort_by(|a, b| b.attack_score.total_cmp(&a.attack_score))
                                }
                                crate::state::RankMetric::Defending => {
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
            },
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
                        crate::state::AnalysisTab::Teams => self.state.select_analysis_next(),
                        crate::state::AnalysisTab::RoleRankings => {
                            self.state.select_rankings_next()
                        }
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
                        crate::state::AnalysisTab::Teams => self.state.select_analysis_prev(),
                        crate::state::AnalysisTab::RoleRankings => {
                            self.state.select_rankings_prev()
                        }
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
                    && self.state.analysis_tab == crate::state::AnalysisTab::RoleRankings
                {
                    self.state.cycle_rankings_metric();
                } else {
                    self.state.cycle_sort();
                }
            }
            KeyCode::Char('l') | KeyCode::Char('L') => {
                // Persist current league cache before switching away.
                crate::persist::save_from_state(&self.state);
                self.state.cycle_league_mode();
                if self.auto_warm_mode != AutoWarmMode::Off {
                    self.auto_warm_pending = true;
                }
                // Load cache for the newly selected league.
                crate::persist::load_into_state(&mut self.state);
                self.request_upcoming(true);
                if matches!(self.state.screen, Screen::Analysis) {
                    self.request_analysis(true);
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
                if matches!(self.state.screen, Screen::Analysis) {
                    self.state.cycle_analysis_tab();
                    if self.state.analysis_tab == crate::state::AnalysisTab::RoleRankings {
                        self.request_rankings_cache_warm_missing(true);
                        self.recompute_rankings_from_cache();
                    }
                } else if matches!(self.state.screen, Screen::PlayerDetail) {
                    self.state.cycle_player_detail_section_next();
                }
            }
            KeyCode::BackTab => {
                if matches!(self.state.screen, Screen::PlayerDetail) {
                    self.state.cycle_player_detail_section_prev();
                }
            }
            KeyCode::Left => {
                if matches!(self.state.screen, Screen::Analysis)
                    && self.state.analysis_tab == crate::state::AnalysisTab::RoleRankings
                {
                    self.state.cycle_rankings_role_prev();
                }
            }
            KeyCode::Right => {
                if matches!(self.state.screen, Screen::Analysis)
                    && self.state.analysis_tab == crate::state::AnalysisTab::RoleRankings
                {
                    self.state.cycle_rankings_role_next();
                }
            }
            KeyCode::Char('r') => {
                if matches!(self.state.screen, Screen::Analysis) {
                    match self.state.analysis_tab {
                        crate::state::AnalysisTab::Teams => self.request_analysis(true),
                        crate::state::AnalysisTab::RoleRankings => {
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
                } else if matches!(self.state.screen, Screen::PlayerDetail) {
                    if let (Some(player_id), Some(player_name)) = (
                        self.state.player_last_id,
                        self.state.player_last_name.clone(),
                    ) {
                        self.request_player_detail(player_id, player_name, true);
                    }
                }
            }
            KeyCode::Char('p') | KeyCode::Char('P') => self.toggle_placeholder_match(),
            KeyCode::Char('R') => {
                if matches!(self.state.screen, Screen::Analysis)
                    && self.state.analysis_tab == crate::state::AnalysisTab::RoleRankings
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
            KeyCode::Char('e') | KeyCode::Char('E') => {
                if matches!(self.state.screen, Screen::Analysis) {
                    self.request_analysis_export(true);
                }
            }
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
        let is_live = self
            .state
            .matches
            .iter()
            .find(|m| m.id == match_id)
            .map(|m| m.is_live)
            .unwrap_or(false);
        let cached_at = self.state.match_detail_cached_at.get(match_id).copied();
        let has_cached = self.state.match_detail.contains_key(match_id);
        if !is_live && has_cached && cache_fresh(cached_at, self.detail_cache_ttl) {
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
        if cache_fresh(self.state.upcoming_cached_at, self.upcoming_cache_ttl) {
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
        let mode = self.state.league_mode;
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
                    .map(|d| crate::state::player_detail_is_stub(d))
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
        self.state.rankings.clear();
        self.state.rankings_selected = 0;
        self.state.rankings_dirty = true;
        self.state.rankings_progress_current = 0;
        self.state.rankings_progress_total = 0;
        self.state.rankings_progress_message = "Cache cleared".to_string();
        self.state.rankings_fetched_at = None;
    }

    fn recompute_rankings_from_cache(&mut self) {
        let rows = crate::analysis_rankings::compute_role_rankings_from_cache(
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
        self.state.rankings_selected = 0;
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
            let is_stub = crate::state::player_detail_is_stub(&cached);
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
                let is_stub = cached
                    .map(|detail| crate::state::player_detail_is_stub(detail))
                    .unwrap_or(true);
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
        let live_matches: Vec<String> = self
            .state
            .filtered_matches()
            .into_iter()
            .filter(|m| m.is_live)
            .filter(|m| m.id != PLACEHOLDER_MATCH_ID)
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

    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = ratatui::backend::CrosstermBackend::new(stdout);
    let mut terminal = ratatui::Terminal::new(backend)?;

    let (tx, rx) = mpsc::channel();
    let (cmd_tx, cmd_rx) = mpsc::channel();
    fake_feed::spawn_fake_provider(tx, cmd_rx);

    let mut app = App::new(Some(cmd_tx));
    // Load cached rankings/analysis (if any) for current league.
    crate::persist::load_into_state(&mut app.state);
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
    crate::persist::save_from_state(&app.state);
    crate::http_cache::flush_http_cache();

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
        if let Some(ids) = app.state.squad_prefetch_pending.take() {
            app.prefetch_players(ids);
        }
        if matches!(app.state.screen, Screen::Analysis)
            && app.state.analysis_tab == crate::state::AnalysisTab::RoleRankings
            && app.state.rankings_dirty
        {
            app.recompute_rankings_from_cache();
        }
        app.state.maybe_clear_export(Instant::now());

        app.maybe_refresh_upcoming();
        app.maybe_refresh_match_details();
        app.maybe_auto_warm_rankings();

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

    let header =
        Paragraph::new(header_text(&app.state)).block(Block::default().borders(Borders::BOTTOM));
    frame.render_widget(header, chunks[0]);

    match app.state.screen {
        Screen::Pulse => render_pulse(frame, chunks[1], &app.state),
        Screen::Terminal { .. } => render_terminal(frame, chunks[1], &app.state),
        Screen::Analysis => render_analysis(frame, chunks[1], &app.state),
        Screen::Squad => render_squad(frame, chunks[1], &app.state),
        Screen::PlayerDetail => render_player_detail(frame, chunks[1], &app.state),
    }

    let footer =
        Paragraph::new(footer_text(&app.state)).block(Block::default().borders(Borders::TOP));
    frame.render_widget(footer, chunks[2]);

    if app.state.export.active {
        render_export_overlay(frame, frame.size(), &app.state);
    }
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
            let updated = state.analysis_updated.as_deref().unwrap_or("-");
            let status = if state.analysis_loading {
                "LOADING"
            } else {
                "READY"
            };
            let tab = match state.analysis_tab {
                crate::state::AnalysisTab::Teams => "TEAMS",
                crate::state::AnalysisTab::RoleRankings => "RANKINGS",
            };
            let fetched = match state.analysis_tab {
                crate::state::AnalysisTab::Teams => format_fetched_at(state.analysis_fetched_at),
                crate::state::AnalysisTab::RoleRankings => {
                    format_fetched_at(state.rankings_fetched_at)
                }
            };
            format!(
                "WC26 ANALYSIS | {} | Tab: {} | Teams: {} | FIFA: {} | Fetched: {} | {}",
                league_label(state.league_mode),
                tab,
                state.analysis.len(),
                updated,
                fetched,
                status
            )
        }
        Screen::Squad => {
            let team = state.squad_team.as_deref().unwrap_or("-");
            let status = if state.squad_loading {
                "LOADING"
            } else {
                "READY"
            };
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

fn format_fetched_at(fetched_at: Option<SystemTime>) -> String {
    fetched_at
        .map(|stamp| {
            DateTime::<Local>::from(stamp)
                .format("%Y-%m-%d %H:%M")
                .to_string()
        })
        .unwrap_or_else(|| "-".to_string())
}

fn footer_text(state: &AppState) -> String {
    match state.screen {
        Screen::Pulse => match state.pulse_view {
            PulseView::Live => {
                "1 Pulse | 2 Analysis | Enter/d Terminal | j/k/↑/↓ Move | s Sort | l League | u Upcoming | i Details | p Placeholder | ? Help | q Quit".to_string()
            }
            PulseView::Upcoming => {
                "1 Pulse | 2 Analysis | u Live | j/k/↑/↓ Scroll | l League | p Placeholder | ? Help | q Quit"
                    .to_string()
            }
        },
        Screen::Terminal { .. } => {
            "1 Pulse | 2 Analysis | b/Esc Back | i Details | l League | p Placeholder | ? Help | q Quit"
                .to_string()
        }
        Screen::Analysis => {
            match state.analysis_tab {
                crate::state::AnalysisTab::Teams => {
                    "1 Pulse | b/Esc Back | j/k/↑/↓ Move | Enter Squad | Tab Rankings | r Refresh | ? Help | q Quit"
                        .to_string()
                }
                crate::state::AnalysisTab::RoleRankings => {
                    "1 Pulse | b/Esc Back | j/k/↑/↓ Move | ←/→ Role | s Metric | Tab Teams | r Missing | R Full | ? Help | q Quit"
                        .to_string()
                }
            }
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
    let rows = state.pulse_live_rows();
    if rows.is_empty() {
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
            crate::state::PulseLiveRow::Match(match_idx) => {
                let Some(m) = state.matches.get(match_idx) else {
                    continue;
                };
                let is_not_started = !m.is_live && m.minute == 0;
                let is_finished = !m.is_live && m.minute >= 90;

                let row_style = if is_not_started {
                    if selected {
                        Style::default().fg(Color::Gray).bg(Color::DarkGray)
                    } else {
                        Style::default().fg(Color::DarkGray)
                    }
                } else if selected {
                    Style::default().fg(Color::White).bg(Color::DarkGray)
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

                render_cell_text(frame, cols[0], &time, row_style);
                render_cell_text(frame, cols[1], &match_name, row_style);
                render_cell_text(frame, cols[2], &score, row_style);

                if is_not_started {
                    render_cell_text(frame, cols[3], "upcoming", row_style);
                    render_cell_text(frame, cols[4], "-", row_style);
                    render_cell_text(frame, cols[5], "-", row_style);
                    render_cell_text(frame, cols[6], "-", row_style);
                    render_cell_text(frame, cols[7], "-", row_style);
                } else {
                    let hda = format!(
                        "H{:.0} D{:.0} A{:.0}",
                        m.win.p_home, m.win.p_draw, m.win.p_away
                    );
                    let delta = format!("{:+.1}", m.win.delta_home);
                    let quality = quality_label(m.win.quality).to_string();
                    let conf = format!("{}%", m.win.confidence);

                    let values = win_prob_values(state.win_prob_history.get(&m.id), m.win.p_home);
                    let chart = win_line_chart(&values, selected);
                    frame.render_widget(chart, cols[3]);

                    render_cell_text(frame, cols[4], &hda, row_style);
                    render_cell_text(frame, cols[5], &delta, row_style);
                    render_cell_text(frame, cols[6], &quality, row_style);
                    render_cell_text(frame, cols[7], &conf, row_style);
                }
            }
            crate::state::PulseLiveRow::Upcoming(upcoming_idx) => {
                let Some(u) = state.upcoming.get(upcoming_idx) else {
                    continue;
                };

                let row_style = if selected {
                    Style::default().fg(Color::Gray).bg(Color::DarkGray)
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
    let style = Style::default().add_modifier(Modifier::BOLD);

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
    let style = Style::default().add_modifier(Modifier::BOLD);
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
        crate::state::AnalysisTab::Teams => render_analysis_teams(frame, area, state),
        crate::state::AnalysisTab::RoleRankings => render_analysis_rankings(frame, area, state),
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
        let updated = row.fifa_updated.clone().unwrap_or_else(|| "-".to_string());
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

fn render_analysis_rankings(frame: &mut Frame, area: Rect, state: &AppState) {
    let sections = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(2), Constraint::Min(1)])
        .split(area);

    let role = role_label(state.rankings_role);
    let metric = metric_label(state.rankings_metric);
    let header = if state.rankings_loading {
        if state.rankings_progress_total > 0 {
            format!(
                "Role Rankings | Role: {role} | Metric: {metric} | {} ({}/{})",
                state.rankings_progress_message,
                state.rankings_progress_current,
                state.rankings_progress_total
            )
        } else {
            format!(
                "Role Rankings | Role: {role} | Metric: {metric} | {}",
                state.rankings_progress_message
            )
        }
    } else {
        format!("Role Rankings | Role: {role} | Metric: {metric}")
    };
    let header_style = Style::default().add_modifier(Modifier::BOLD);
    frame.render_widget(Paragraph::new(header).style(header_style), sections[0]);

    let list_area = sections[1];
    if list_area.height == 0 {
        return;
    }

    if state.rankings.is_empty() {
        let message = if state.rankings_loading {
            "Loading role rankings..."
        } else {
            "No role ranking data yet (press r to warm cache)"
        };
        let empty = Paragraph::new(message).style(Style::default().fg(Color::DarkGray));
        frame.render_widget(empty, list_area);
        return;
    }

    let mut rows: Vec<&crate::state::RoleRankingEntry> = state
        .rankings
        .iter()
        .filter(|r| r.role == state.rankings_role)
        .collect();

    match state.rankings_metric {
        crate::state::RankMetric::Attacking => {
            rows.sort_by(|a, b| b.attack_score.total_cmp(&a.attack_score))
        }
        crate::state::RankMetric::Defending => {
            rows.sort_by(|a, b| b.defense_score.total_cmp(&a.defense_score))
        }
    }

    let visible = list_area.height as usize;
    let total = rows.len();
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
            Style::default().fg(Color::White).bg(Color::DarkGray)
        } else {
            Style::default()
        };
        if selected {
            frame.render_widget(Block::default().style(row_style), row_area);
        }

        let entry = rows[idx];
        let rank = idx + 1;
        let score = match state.rankings_metric {
            crate::state::RankMetric::Attacking => entry.attack_score,
            crate::state::RankMetric::Defending => entry.defense_score,
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
    let block = Block::default()
        .title("Player Detail")
        .borders(Borders::ALL);
    let inner = block.inner(area);
    frame.render_widget(block, area);

    if inner.height == 0 || inner.width == 0 {
        return;
    }

    if state.player_loading {
        let text =
            Paragraph::new("Loading player details...").style(Style::default().fg(Color::DarkGray));
        frame.render_widget(text, inner);
        return;
    }

    let Some(detail) = state.player_detail.as_ref() else {
        let text = Paragraph::new("No player data yet").style(Style::default().fg(Color::DarkGray));
        frame.render_widget(text, inner);
        return;
    };

    if inner.height < 8 {
        let text = player_detail_text(detail);
        let paragraph = Paragraph::new(styled_detail_text(&text, player_position_group(detail)))
            .scroll((state.player_detail_scroll, 0));
        frame.render_widget(paragraph, inner);
        return;
    }

    let position_group = player_position_group(detail);
    let info_text = player_info_text(detail);
    let league_text = player_league_stats_text(detail);
    let top_text = player_top_stats_text(detail);
    let traits_text = player_traits_text(detail);
    let other_text_styled = player_season_performance_text_styled(detail, position_group);
    let season_text = player_season_breakdown_text(detail);
    let career_text = player_career_text(detail);
    let trophies_text = player_trophies_text(detail);
    let recent_text = player_recent_matches_text(detail);

    let info_lines = text_line_count(&info_text);
    let league_lines = text_line_count(&league_text);
    let top_lines = text_line_count(&top_text);
    let traits_lines = text_line_count(&traits_text);
    let other_styled_lines = other_text_styled.len() as u16;
    let season_lines = text_line_count(&season_text);
    let career_lines = text_line_count(&career_text);
    let trophies_lines = text_line_count(&trophies_text);
    let recent_lines = text_line_count(&recent_text);

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
        &info_text,
        state.player_detail_section_scrolls[0],
        state.player_detail_section == 0,
        info_lines,
        position_group,
    );
    render_detail_section(
        frame,
        left_sections[1],
        "All Competitions",
        &league_text,
        state.player_detail_section_scrolls[1],
        state.player_detail_section == 1,
        league_lines,
        position_group,
    );
    render_detail_section(
        frame,
        left_sections[2],
        "Top Stats (All Competitions)",
        &top_text,
        state.player_detail_section_scrolls[2],
        state.player_detail_section == 2,
        top_lines,
        position_group,
    );
    render_detail_section(
        frame,
        left_sections[3],
        "Player Traits",
        &traits_text,
        state.player_detail_section_scrolls[3],
        state.player_detail_section == 3,
        traits_lines,
        position_group,
    );
    render_detail_section_lines(
        frame,
        left_sections[4],
        "Season Performance",
        other_text_styled,
        state.player_detail_section_scrolls[4],
        state.player_detail_section == 4,
        other_styled_lines,
    );

    render_detail_section(
        frame,
        right_sections[0],
        "Season Breakdown",
        &season_text,
        state.player_detail_section_scrolls[5],
        state.player_detail_section == 5,
        season_lines,
        position_group,
    );
    render_detail_section(
        frame,
        right_sections[1],
        "Career Summary",
        &career_text,
        state.player_detail_section_scrolls[6],
        state.player_detail_section == 6,
        career_lines,
        position_group,
    );
    render_detail_section(
        frame,
        right_sections[2],
        "Trophies",
        &trophies_text,
        state.player_detail_section_scrolls[7],
        state.player_detail_section == 7,
        trophies_lines,
        position_group,
    );
    render_detail_section(
        frame,
        right_sections[3],
        "Match Stats (Recent)",
        &recent_text,
        state.player_detail_section_scrolls[8],
        state.player_detail_section == 8,
        recent_lines,
        position_group,
    );
}

fn player_detail_has_stats(detail: &PlayerDetail) -> bool {
    !detail.all_competitions.is_empty()
        || detail.main_league.is_some()
        || !detail.top_stats.is_empty()
        || !detail.season_groups.is_empty()
        || !detail.season_performance.is_empty()
        || detail.traits.is_some()
        || !detail.recent_matches.is_empty()
        || !detail.season_breakdown.is_empty()
        || !detail.career_sections.is_empty()
        || !detail.trophies.is_empty()
}

fn player_position_group(detail: &PlayerDetail) -> PlayerPositionGroup {
    let position = detail
        .position
        .as_deref()
        .or_else(|| detail.positions.first().map(|p| p.as_str()))
        .unwrap_or("-")
        .to_lowercase();

    if position.contains("gk") || position.contains("goalkeeper") {
        PlayerPositionGroup::Goalkeeper
    } else if position.contains("cb")
        || position.contains("lb")
        || position.contains("rb")
        || position.contains("def")
    {
        PlayerPositionGroup::Defender
    } else if position.contains("mid") || position.contains("cm") || position.contains("am") {
        PlayerPositionGroup::Midfielder
    } else if position.contains("fw")
        || position.contains("st")
        || position.contains("wing")
        || position.contains("att")
    {
        PlayerPositionGroup::Forward
    } else {
        PlayerPositionGroup::Unknown
    }
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
    lines.push(player_season_performance_text(detail));
    lines.push(String::new());
    lines.push(player_season_breakdown_text(detail));
    lines.push(String::new());
    lines.push(player_career_text(detail));
    lines.push(String::new());
    lines.push(player_trophies_text(detail));
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

    if let Some(league) = detail.main_league.as_ref() {
        if !league.stats.is_empty() {
            let mut lines = Vec::new();
            lines.push(format!("{} ({})", league.league_name, league.season));
            for stat in league.stats.iter().take(8) {
                lines.push(format!("{}: {}", stat.title, stat.value));
            }
            return lines.join("\n");
        }
    }

    "No league stats available".to_string()
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
        if remaining > 0 && remaining % 3 == 0 {
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
    position_group: PlayerPositionGroup,
) -> Vec<Line<'static>> {
    if detail.season_performance.is_empty() {
        return vec![Line::from("No season performance stats")];
    }
    let mut lines = Vec::new();
    if let Some(minutes) = player_minutes_played(detail) {
        lines.push(Line::from(style_detail_line_value(
            "Minutes played",
            &minutes,
            None,
            PlayerPositionGroup::Unknown,
        )));
    }
    lines.push(Line::from("Total | Per 90"));
    for group in &detail.season_performance {
        lines.push(Line::from(format!("{}:", group.title)));
        for item in &group.items {
            let per90 = item.per90.as_deref().unwrap_or("-");
            lines.push(Line::from(style_stat_with_rank(
                &item.title,
                &item.total,
                per90,
                item.percentile_rank,
                item.percentile_rank_per90,
                position_group,
            )));
        }
    }
    lines
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

fn render_detail_section(
    frame: &mut Frame,
    area: Rect,
    title: &str,
    body: &str,
    scroll: u16,
    active: bool,
    total_lines: u16,
    position_group: PlayerPositionGroup,
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
    let paragraph = Paragraph::new(styled_detail_text(body, position_group)).scroll((scroll, 0));
    frame.render_widget(paragraph, inner);
}

fn render_detail_section_lines(
    frame: &mut Frame,
    area: Rect,
    title: &str,
    body: Vec<Line<'static>>,
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

fn style_percentile_line(line: &str) -> Vec<Span<'_>> {
    const PUNCH: Color = Color::Rgb(221, 54, 54);
    const PRINCETON_ORANGE: Color = Color::Rgb(240, 128, 34);
    const UFO_GREEN: Color = Color::Rgb(51, 199, 113);

    let mut spans = Vec::new();
    let mut start = 0;
    for (idx, ch) in line.char_indices() {
        if ch == '%' {
            let percent_start = line[..idx]
                .char_indices()
                .rev()
                .take_while(|(_, c)| c.is_ascii_digit() || *c == '.')
                .last()
                .map(|(pos, _)| pos)
                .unwrap_or(idx);
            if percent_start == idx {
                continue;
            }
            let raw = &line[percent_start..idx];
            if let Ok(value) = raw.parse::<f64>() {
                if percent_start > start {
                    spans.push(Span::raw(line[start..percent_start].to_string()));
                }
                let color = if value < 30.0 {
                    PUNCH
                } else if value < 70.0 {
                    PRINCETON_ORANGE
                } else {
                    UFO_GREEN
                };
                spans.push(Span::styled(
                    format!("{}%", raw),
                    Style::default().fg(color),
                ));
                start = idx + 1;
            }
        }
    }

    if spans.is_empty() {
        spans.push(Span::raw(line.to_string()));
    } else if start < line.len() {
        spans.push(Span::raw(line[start..].to_string()));
    }

    spans
}

fn styled_stat_line(line: &str, position_group: PlayerPositionGroup) -> Vec<Span<'_>> {
    if line.trim().is_empty() || line.ends_with(':') || line.contains('|') {
        return vec![Span::raw(line.to_string())];
    }

    let (prefix, value) = match line.rsplit_once(':') {
        Some((lhs, rhs)) => (lhs, rhs.trim()),
        None => return vec![Span::raw(line.to_string())],
    };

    style_detail_line_value(prefix.trim(), value, None, position_group)
}

fn style_detail_line_value(
    label: &str,
    raw_value: &str,
    percentile: Option<f64>,
    position_group: PlayerPositionGroup,
) -> Vec<Span<'static>> {
    let value_spans = style_value_only(label, raw_value, percentile, position_group);
    if label.is_empty() {
        return value_spans;
    }

    let mut spans = Vec::new();
    spans.push(Span::raw(format!("{label}: ")));
    spans.extend(value_spans);
    spans
}

fn style_value_only(
    label: &str,
    raw_value: &str,
    percentile: Option<f64>,
    position_group: PlayerPositionGroup,
) -> Vec<Span<'static>> {
    let Some((value, suffix)) = parse_stat_value(raw_value) else {
        return vec![Span::raw(raw_value.to_string())];
    };

    if should_skip_stat_color(label) {
        return vec![Span::raw(format!("{value}{suffix}"))];
    }

    let score = percentile
        .map(|v| (v / 100.0).clamp(0.0, 1.0))
        .unwrap_or_else(|| normalize_stat_value(label, value, position_group));
    let color = stat_color(score);

    vec![Span::styled(
        format!("{value}{suffix}"),
        Style::default().fg(color),
    )]
}

fn should_skip_stat_color(label: &str) -> bool {
    let lowered = label.to_lowercase();
    lowered == "id" || lowered == "age" || lowered.contains("shirt") || lowered.contains("troph")
}

fn style_stat_with_rank(
    label: &str,
    total: &str,
    per90: &str,
    total_rank: Option<f64>,
    per90_rank: Option<f64>,
    position_group: PlayerPositionGroup,
) -> Vec<Span<'static>> {
    let total_text = style_detail_line_value(label, total, total_rank, position_group);
    let per90_text = if per90 == "-" {
        vec![Span::raw("-".to_string())]
    } else {
        style_value_only(label, per90, per90_rank, position_group)
    };

    let mut out = Vec::new();
    out.extend(total_text);
    out.push(Span::raw(" | ".to_string()));
    out.extend(per90_text);

    out
}

fn styled_detail_text(text: &str, position_group: PlayerPositionGroup) -> Text<'_> {
    Text::from(
        text.lines()
            .map(|line| {
                let spans = if line.contains('%') {
                    style_percentile_line(line)
                } else {
                    styled_stat_line(line, position_group)
                };
                Line::from(spans)
            })
            .collect::<Vec<_>>(),
    )
}

fn parse_stat_value(raw: &str) -> Option<(f64, String)> {
    let trimmed = raw.trim();
    if trimmed.is_empty() || trimmed == "-" {
        return None;
    }
    let value_str = trimmed
        .trim_end_matches('%')
        .trim_end_matches(|c: char| c == 'm' || c == 'M' || c == 'k' || c == 'K')
        .trim_start_matches('+');
    let value = value_str.parse::<f64>().ok()?;
    let suffix = if trimmed.ends_with('%') {
        "%".to_string()
    } else if trimmed.ends_with('m') || trimmed.ends_with('M') {
        "M".to_string()
    } else if trimmed.ends_with('k') || trimmed.ends_with('K') {
        "K".to_string()
    } else {
        "".to_string()
    };
    Some((value, suffix))
}

fn normalize_stat_value(label: &str, value: f64, position_group: PlayerPositionGroup) -> f64 {
    let lowered = label.to_lowercase();
    let is_per90 = lowered.contains("per 90") || lowered.contains("per90");
    let Some((bounds, higher_better)) = stat_bounds(&lowered, position_group, is_per90) else {
        return (value / 100.0).clamp(0.0, 1.0);
    };
    let raw_score = ((value - bounds.0) / (bounds.1 - bounds.0)).clamp(0.0, 1.0);
    if higher_better {
        raw_score
    } else {
        1.0 - raw_score
    }
}

fn stat_bounds(
    label: &str,
    position_group: PlayerPositionGroup,
    per90: bool,
) -> Option<((f64, f64), bool)> {
    for (needle, bounds, higher_better) in global_bounds() {
        if label.contains(needle) {
            return Some((bounds, higher_better));
        }
    }

    let base = if per90 {
        per90_bounds(position_group)
    } else {
        total_bounds(position_group)
    };

    for (needle, bounds, higher_better) in base {
        if label.contains(needle) {
            return Some((bounds, higher_better));
        }
    }

    None
}

fn global_bounds() -> Vec<(&'static str, (f64, f64), bool)> {
    vec![
        ("rating", (0.0, 10.0), true),
        ("minutes", (0.0, 4000.0), true),
        ("matches started", (0.0, 60.0), true),
        ("started", (0.0, 60.0), true),
        ("appearances", (0.0, 60.0), true),
        ("matches", (0.0, 60.0), true),
    ]
}

fn total_bounds(position_group: PlayerPositionGroup) -> Vec<(&'static str, (f64, f64), bool)> {
    match position_group {
        PlayerPositionGroup::Goalkeeper => vec![
            ("save", (0.0, 180.0), true),
            ("clean sheet", (0.0, 20.0), true),
            ("conceded", (0.0, 80.0), false),
            ("penalty", (0.0, 10.0), true),
            ("distribution", (0.0, 2000.0), true),
            ("pass", (0.0, 2500.0), true),
            ("touch", (0.0, 2000.0), true),
            ("error", (0.0, 10.0), false),
        ],
        PlayerPositionGroup::Defender => vec![
            ("tackle", (0.0, 200.0), true),
            ("interception", (0.0, 200.0), true),
            ("clearance", (0.0, 500.0), true),
            ("block", (0.0, 150.0), true),
            ("aerial duel", (0.0, 300.0), true),
            ("duel", (0.0, 400.0), true),
            ("recover", (0.0, 400.0), true),
            ("touch", (0.0, 3500.0), true),
            ("pass", (0.0, 4000.0), true),
            ("cross", (0.0, 120.0), true),
            ("conceded", (0.0, 80.0), false),
            ("foul", (0.0, 80.0), false),
            ("yellow", (0.0, 15.0), false),
            ("red", (0.0, 5.0), false),
            ("goal", (0.0, 10.0), true),
            ("assist", (0.0, 10.0), true),
        ],
        PlayerPositionGroup::Midfielder => vec![
            ("goal", (0.0, 20.0), true),
            ("assist", (0.0, 20.0), true),
            ("expected goals", (0.0, 12.0), true),
            ("xg", (0.0, 12.0), true),
            ("expected assists", (0.0, 12.0), true),
            ("xa", (0.0, 12.0), true),
            ("shot", (0.0, 120.0), true),
            ("shots on target", (0.0, 80.0), true),
            ("chances created", (0.0, 150.0), true),
            ("key pass", (0.0, 150.0), true),
            ("pass", (0.0, 5000.0), true),
            ("accurate long balls", (0.0, 300.0), true),
            ("cross", (0.0, 120.0), true),
            ("dribble", (0.0, 300.0), true),
            ("touch", (0.0, 5000.0), true),
            ("tackle", (0.0, 200.0), true),
            ("interception", (0.0, 200.0), true),
            ("duel", (0.0, 400.0), true),
            ("recover", (0.0, 400.0), true),
            ("foul", (0.0, 80.0), false),
            ("yellow", (0.0, 15.0), false),
            ("red", (0.0, 5.0), false),
        ],
        PlayerPositionGroup::Forward => vec![
            ("goal", (0.0, 30.0), true),
            ("assist", (0.0, 15.0), true),
            ("expected goals", (0.0, 15.0), true),
            ("xg", (0.0, 15.0), true),
            ("expected assists", (0.0, 8.0), true),
            ("xa", (0.0, 8.0), true),
            ("xgot", (0.0, 18.0), true),
            ("shot", (0.0, 150.0), true),
            ("shots on target", (0.0, 100.0), true),
            ("chances created", (0.0, 120.0), true),
            ("key pass", (0.0, 120.0), true),
            ("dribble", (0.0, 350.0), true),
            ("touch", (0.0, 4000.0), true),
            ("touches in opposition box", (0.0, 300.0), true),
            ("aerial duel", (0.0, 250.0), true),
            ("duel", (0.0, 350.0), true),
            ("foul", (0.0, 80.0), false),
            ("yellow", (0.0, 15.0), false),
            ("red", (0.0, 5.0), false),
        ],
        PlayerPositionGroup::Unknown => vec![
            ("goal", (0.0, 25.0), true),
            ("assist", (0.0, 20.0), true),
            ("xg", (0.0, 12.0), true),
            ("xa", (0.0, 10.0), true),
            ("shot", (0.0, 120.0), true),
            ("chances created", (0.0, 120.0), true),
            ("pass", (0.0, 4000.0), true),
            ("touch", (0.0, 5000.0), true),
            ("tackle", (0.0, 200.0), true),
            ("interception", (0.0, 200.0), true),
            ("duel", (0.0, 400.0), true),
            ("foul", (0.0, 80.0), false),
            ("yellow", (0.0, 15.0), false),
            ("red", (0.0, 5.0), false),
            ("minutes", (0.0, 4000.0), true),
            ("rating", (0.0, 10.0), true),
        ],
    }
}

fn per90_bounds(position_group: PlayerPositionGroup) -> Vec<(&'static str, (f64, f64), bool)> {
    match position_group {
        PlayerPositionGroup::Goalkeeper => vec![
            ("save", (0.0, 6.0), true),
            ("conceded", (0.0, 2.5), false),
            ("pass", (0.0, 40.0), true),
            ("touch", (0.0, 50.0), true),
            ("error", (0.0, 0.5), false),
        ],
        PlayerPositionGroup::Defender => vec![
            ("tackle", (0.0, 4.0), true),
            ("interception", (0.0, 3.5), true),
            ("clearance", (0.0, 8.0), true),
            ("block", (0.0, 3.0), true),
            ("aerial duel", (0.0, 6.0), true),
            ("duel", (0.0, 10.0), true),
            ("recover", (0.0, 10.0), true),
            ("pass", (0.0, 80.0), true),
            ("cross", (0.0, 2.0), true),
            ("goal", (0.0, 0.5), true),
            ("assist", (0.0, 0.4), true),
            ("foul", (0.0, 3.0), false),
        ],
        PlayerPositionGroup::Midfielder => vec![
            ("goal", (0.0, 0.8), true),
            ("assist", (0.0, 0.8), true),
            ("xg", (0.0, 0.6), true),
            ("xa", (0.0, 0.6), true),
            ("shot", (0.0, 4.0), true),
            ("shots on target", (0.0, 2.0), true),
            ("chances created", (0.0, 3.5), true),
            ("key pass", (0.0, 3.5), true),
            ("pass", (0.0, 90.0), true),
            ("accurate long balls", (0.0, 6.0), true),
            ("cross", (0.0, 2.0), true),
            ("dribble", (0.0, 5.0), true),
            ("touch", (0.0, 100.0), true),
            ("tackle", (0.0, 3.5), true),
            ("interception", (0.0, 2.5), true),
            ("duel", (0.0, 10.0), true),
            ("recover", (0.0, 10.0), true),
            ("foul", (0.0, 3.0), false),
        ],
        PlayerPositionGroup::Forward => vec![
            ("goal", (0.0, 1.2), true),
            ("assist", (0.0, 0.6), true),
            ("xg", (0.0, 0.9), true),
            ("xa", (0.0, 0.4), true),
            ("xgot", (0.0, 1.1), true),
            ("shot", (0.0, 5.5), true),
            ("shots on target", (0.0, 2.8), true),
            ("chances created", (0.0, 3.0), true),
            ("key pass", (0.0, 2.5), true),
            ("dribble", (0.0, 6.0), true),
            ("touch", (0.0, 80.0), true),
            ("touches in opposition box", (0.0, 8.0), true),
            ("aerial duel", (0.0, 6.0), true),
            ("duel", (0.0, 9.0), true),
            ("foul", (0.0, 3.0), false),
        ],
        PlayerPositionGroup::Unknown => vec![
            ("goal", (0.0, 1.0), true),
            ("assist", (0.0, 0.6), true),
            ("xg", (0.0, 0.8), true),
            ("xa", (0.0, 0.5), true),
            ("shot", (0.0, 5.0), true),
            ("chances created", (0.0, 3.0), true),
            ("pass", (0.0, 80.0), true),
            ("touch", (0.0, 90.0), true),
            ("tackle", (0.0, 3.0), true),
            ("interception", (0.0, 2.5), true),
            ("duel", (0.0, 9.0), true),
            ("foul", (0.0, 3.0), false),
        ],
    }
}

fn stat_color(score: f64) -> Color {
    const PUNCH: Color = Color::Rgb(221, 54, 54);
    const PRINCETON_ORANGE: Color = Color::Rgb(240, 128, 34);
    const UFO_GREEN: Color = Color::Rgb(51, 199, 113);

    if score < 0.30 {
        PUNCH
    } else if score < 0.70 {
        PRINCETON_ORANGE
    } else {
        UFO_GREEN
    }
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

    render_pitch(frame, middle_chunks[0], state);

    let tape = Paragraph::new(event_tape_text(state))
        .block(Block::default().title("Event Tape").borders(Borders::ALL));
    frame.render_widget(tape, middle_chunks[1]);

    let stats_text = stats_text(state);
    let stats =
        Paragraph::new(stats_text).block(Block::default().title("Stats").borders(Borders::ALL));
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

fn render_pitch(frame: &mut Frame, area: Rect, state: &AppState) {
    let block = Block::default().title("Pitch").borders(Borders::ALL);
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
    let mut lines = Vec::new();
    lines.extend(pitch_team_lines("AWAY", away, width));
    let sep = "-".repeat(width.min(24).max(4));
    lines.push(center_line(&sep, width));
    lines.extend(pitch_team_lines("HOME", home, width));

    if lines.len() > height {
        lines.truncate(height);
    }
    lines.join("\n")
}

fn pitch_team_lines(label: &str, side: &crate::state::LineupSide, width: usize) -> Vec<String> {
    let mut lines = Vec::new();
    lines.push(crop_line(
        &format!("{label} {} ({})", side.team_abbr, side.formation),
        width,
    ));
    lines.extend(pitch_pos_lines(&side.starting, width));
    lines
}

fn pitch_pos_lines(players: &[crate::state::PlayerSlot], width: usize) -> Vec<String> {
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

    let mut lines = Vec::new();
    lines.push(crop_line(&format_pos_line("GK", &gk), width));
    lines.push(crop_line(&format_pos_line("DF", &df), width));
    lines.push(crop_line(&format_pos_line("MF", &mf), width));
    lines.push(crop_line(&format_pos_line("FW", &fw), width));
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

    let title = if state.export.done {
        "Export complete"
    } else {
        "Exporting..."
    };

    let block = Block::default().title(title).borders(Borders::ALL);
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
        "  e            Export analysis to XLSX (current league)",
        "  r            Refresh analysis/squad/player",
        "  p            Toggle placeholder match",
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
