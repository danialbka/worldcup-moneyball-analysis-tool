use std::collections::{HashMap, HashSet, VecDeque};
use std::env;
use std::time::SystemTime;

use serde::{Deserialize, Serialize};

use crate::win_prob;

#[derive(Debug, Clone)]
pub struct PredictionExplain {
    // Probability snapshots (H/D/A, in percent) used to explain pre-match priors.
    pub p_home_baseline: f32,
    pub p_draw_baseline: f32,
    pub p_away_baseline: f32,
    pub p_home_ha: f32,
    pub p_draw_ha: f32,
    pub p_away_ha: f32,
    pub p_home_analysis: f32,
    pub p_draw_analysis: f32,
    pub p_away_analysis: f32,
    pub p_home_final: f32,
    pub p_draw_final: f32,
    pub p_away_final: f32,

    // Percentage-point contributions to home-win probability.
    pub pp_home_adv: f32,
    pub pp_analysis: f32,
    pub pp_lineup: f32,

    // Short tags describing what signals were available (best-effort).
    pub signals: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct PredictionExtras {
    pub prematch_only: bool,
    pub lambda_home_pre: f64,
    pub lambda_away_pre: f64,

    pub s_home_analysis: Option<f64>,
    pub s_away_analysis: Option<f64>,
    pub s_home_lineup: Option<f64>,
    pub s_away_lineup: Option<f64>,
    pub lineup_coverage_home: Option<f32>,
    pub lineup_coverage_away: Option<f32>,
    pub blend_w_lineup: f32,

    pub explain: PredictionExplain,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Screen {
    Pulse,
    Terminal { match_id: Option<String> },
    Analysis,
    Squad,
    PlayerDetail,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminalFocus {
    MatchList,
    Pitch,
    EventTape,
    Commentary,
    Stats,
    Lineups,
    Prediction,
    Console,
}

pub const PLACEHOLDER_MATCH_ID: &str = "placeholder-demo";
pub const PLACEHOLDER_HOME: &str = "ALPHA";
pub const PLACEHOLDER_AWAY: &str = "OMEGA";

pub fn placeholder_match_summary(mode: LeagueMode) -> MatchSummary {
    let league_name = match mode {
        LeagueMode::PremierLeague => "Premier League",
        LeagueMode::LaLiga => "La Liga",
        LeagueMode::Bundesliga => "Bundesliga",
        LeagueMode::SerieA => "Serie A",
        LeagueMode::Ligue1 => "Ligue 1",
        LeagueMode::ChampionsLeague => "Champions League",
        LeagueMode::WorldCup => "World Cup",
    };
    MatchSummary {
        id: PLACEHOLDER_MATCH_ID.to_string(),
        league_id: None,
        league_name: league_name.to_string(),
        home: PLACEHOLDER_HOME.to_string(),
        away: PLACEHOLDER_AWAY.to_string(),
        minute: 54,
        score_home: 2,
        score_away: 1,
        win: WinProbRow {
            p_home: 56.0,
            p_draw: 22.0,
            p_away: 22.0,
            delta_home: 0.0,
            quality: ModelQuality::Event,
            confidence: 74,
        },
        is_live: true,
    }
}

pub fn placeholder_match_detail() -> MatchDetail {
    let stats = vec![
        StatRow {
            group: None,
            name: "Possession".to_string(),
            home: "58%".to_string(),
            away: "42%".to_string(),
        },
        StatRow {
            group: None,
            name: "Shots".to_string(),
            home: "14".to_string(),
            away: "9".to_string(),
        },
        StatRow {
            group: None,
            name: "Shots on target".to_string(),
            home: "6".to_string(),
            away: "3".to_string(),
        },
        StatRow {
            group: None,
            name: "xG".to_string(),
            home: "1.72".to_string(),
            away: "0.86".to_string(),
        },
        StatRow {
            group: None,
            name: "Passes".to_string(),
            home: "412".to_string(),
            away: "298".to_string(),
        },
        StatRow {
            group: None,
            name: "Corners".to_string(),
            home: "5".to_string(),
            away: "2".to_string(),
        },
    ];

    let events = vec![
        Event {
            minute: 6,
            kind: EventKind::Goal,
            team: PLACEHOLDER_HOME.to_string(),
            description: "Goal".to_string(),
        },
        Event {
            minute: 27,
            kind: EventKind::Card,
            team: PLACEHOLDER_AWAY.to_string(),
            description: "Yellow card".to_string(),
        },
        Event {
            minute: 41,
            kind: EventKind::Goal,
            team: PLACEHOLDER_HOME.to_string(),
            description: "Goal".to_string(),
        },
        Event {
            minute: 52,
            kind: EventKind::Sub,
            team: PLACEHOLDER_AWAY.to_string(),
            description: "Substitution".to_string(),
        },
    ];

    let lineups = MatchLineups {
        sides: vec![
            placeholder_lineup_side(
                PLACEHOLDER_HOME,
                "4-3-3",
                vec![
                    placeholder_player("A. Stone", 1, "GK"),
                    placeholder_player("R. Vega", 3, "DF"),
                    placeholder_player("M. Holt", 4, "DF"),
                    placeholder_player("J. Nox", 6, "MF"),
                    placeholder_player("T. Vale", 8, "MF"),
                    placeholder_player("K. Rook", 9, "FW"),
                ],
                vec![
                    placeholder_player("P. Vale", 12, "DF"),
                    placeholder_player("S. Quinn", 18, "FW"),
                ],
            ),
            placeholder_lineup_side(
                PLACEHOLDER_AWAY,
                "4-2-3-1",
                vec![
                    placeholder_player("L. Park", 1, "GK"),
                    placeholder_player("D. Moss", 2, "DF"),
                    placeholder_player("I. Noor", 5, "DF"),
                    placeholder_player("C. Hale", 7, "MF"),
                    placeholder_player("V. Ash", 10, "MF"),
                    placeholder_player("E. Pike", 11, "FW"),
                ],
                vec![
                    placeholder_player("N. Gray", 14, "MF"),
                    placeholder_player("O. Reed", 19, "FW"),
                ],
            ),
        ],
    };

    MatchDetail {
        home_team: Some(PLACEHOLDER_HOME.to_string()),
        away_team: Some(PLACEHOLDER_AWAY.to_string()),
        events,
        commentary: Vec::new(),
        commentary_error: None,
        lineups: Some(lineups),
        stats,
    }
}

fn placeholder_lineup_side(
    team: &str,
    formation: &str,
    starting: Vec<PlayerSlot>,
    subs: Vec<PlayerSlot>,
) -> LineupSide {
    let abbr = team
        .chars()
        .filter(|c| c.is_ascii_alphabetic())
        .take(3)
        .collect::<String>()
        .to_uppercase();
    LineupSide {
        team: team.to_string(),
        team_abbr: if abbr.is_empty() {
            "TMP".to_string()
        } else {
            abbr
        },
        formation: formation.to_string(),
        starting,
        subs,
    }
}

fn placeholder_player(name: &str, number: u32, pos: &str) -> PlayerSlot {
    PlayerSlot {
        id: None,
        name: name.to_string(),
        number: Some(number),
        pos: Some(pos.to_string()),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnalysisTab {
    Teams,
    RoleRankings,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum RoleCategory {
    Goalkeeper,
    Defender,
    Midfielder,
    Attacker,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RankMetric {
    Attacking,
    Defending,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PulseLiveRow {
    Match(usize),
    Upcoming(usize),
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelQuality {
    Basic,
    Event,
    Track,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortMode {
    Hot,
    Time,
    Close,
    Upset,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PulseView {
    Live,
    Upcoming,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum LeagueMode {
    PremierLeague,
    LaLiga,
    Bundesliga,
    SerieA,
    Ligue1,
    ChampionsLeague,
    WorldCup,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[allow(clippy::upper_case_acronyms)]
pub enum Confederation {
    AFC,
    CAF,
    CONCACAF,
    CONMEBOL,
    UEFA,
    OFC,
}

#[derive(Debug, Clone)]
pub struct AppState {
    pub screen: Screen,
    pub sort: SortMode,
    pub league_mode: LeagueMode,
    pub pulse_view: PulseView,
    pub selected: usize,
    pub league_pl_ids: Vec<u32>,
    pub league_ll_ids: Vec<u32>,
    pub league_bl_ids: Vec<u32>,
    pub league_sa_ids: Vec<u32>,
    pub league_l1_ids: Vec<u32>,
    pub league_cl_ids: Vec<u32>,
    pub league_wc_ids: Vec<u32>,
    pub matches: Vec<MatchSummary>,
    pub upcoming: Vec<UpcomingMatch>,
    pub upcoming_scroll: u16,
    pub upcoming_cached_at: Option<SystemTime>,
    pub match_detail: HashMap<String, MatchDetail>,
    pub match_detail_cached_at: HashMap<String, SystemTime>,
    pub logs: VecDeque<String>,
    pub help_overlay: bool,
    pub analysis: Vec<TeamAnalysis>,
    pub analysis_selected: usize,
    pub analysis_loading: bool,
    pub analysis_updated: Option<String>,
    pub analysis_fetched_at: Option<SystemTime>,
    pub analysis_tab: AnalysisTab,
    pub rankings_loading: bool,
    pub rankings: Vec<RoleRankingEntry>,
    pub rankings_selected: usize,
    pub rankings_role: RoleCategory,
    pub rankings_metric: RankMetric,
    pub rankings_search: String,
    pub rankings_search_active: bool,
    pub rankings_progress_current: usize,
    pub rankings_progress_total: usize,
    pub rankings_progress_message: String,
    pub rankings_cache_squads: HashMap<u32, Vec<SquadPlayer>>,
    pub rankings_cache_players: HashMap<u32, PlayerDetail>,
    pub rankings_cache_squads_at: HashMap<u32, SystemTime>,
    pub rankings_cache_players_at: HashMap<u32, SystemTime>,
    pub combined_player_cache: HashMap<u32, PlayerDetail>,
    pub rankings_dirty: bool,
    pub rankings_fetched_at: Option<SystemTime>,
    pub win_prob_history: HashMap<String, Vec<f32>>,
    pub prematch_win: HashMap<String, WinProbRow>,
    pub prematch_locked: HashSet<String>,
    pub prediction_extras: HashMap<String, PredictionExtras>,
    pub prediction_show_why: bool,
    pub placeholder_match_enabled: bool,
    pub squad: Vec<SquadPlayer>,
    pub squad_selected: usize,
    pub squad_loading: bool,
    pub squad_team: Option<String>,
    pub squad_team_id: Option<u32>,
    pub squad_prefetch_pending: Option<Vec<u32>>,
    pub player_detail: Option<PlayerDetail>,
    pub player_loading: bool,
    pub player_last_id: Option<u32>,
    pub player_last_name: Option<String>,
    pub player_detail_back: Screen,
    pub player_detail_scroll: u16,
    pub player_detail_section: usize,
    pub player_detail_section_scrolls: [u16; PLAYER_DETAIL_SECTIONS],
    pub player_detail_expanded: bool,
    pub export: ExportState,
    pub terminal_focus: TerminalFocus,
    pub terminal_detail: Option<TerminalFocus>,
    pub terminal_detail_scroll: u16,
}

impl Default for AppState {
    fn default() -> Self {
        Self::new()
    }
}

impl AppState {
    pub fn maybe_clear_export(&mut self, now: std::time::Instant) {
        self.export.clear_if_done_for(now, 8);
    }

    pub fn new() -> Self {
        const DEFAULT_PREMIER_IDS: &[u32] = &[47];
        const DEFAULT_LALIGA_IDS: &[u32] = &[87];
        const DEFAULT_BUNDESLIGA_IDS: &[u32] = &[54];
        const DEFAULT_SERIE_A_IDS: &[u32] = &[55];
        const DEFAULT_LIGUE1_IDS: &[u32] = &[53];
        const DEFAULT_CHAMPIONS_LEAGUE_IDS: &[u32] = &[42];
        const DEFAULT_WORLDCUP_IDS: &[u32] = &[77];

        let league_pl_ids = parse_ids_env_or_default("APP_LEAGUE_PREMIER_IDS", DEFAULT_PREMIER_IDS);
        let league_ll_ids = parse_ids_env_or_default("APP_LEAGUE_LALIGA_IDS", DEFAULT_LALIGA_IDS);
        let league_bl_ids =
            parse_ids_env_or_default("APP_LEAGUE_BUNDESLIGA_IDS", DEFAULT_BUNDESLIGA_IDS);
        let league_sa_ids = parse_ids_env_or_default("APP_LEAGUE_SERIE_A_IDS", DEFAULT_SERIE_A_IDS);
        let league_l1_ids = parse_ids_env_or_default("APP_LEAGUE_LIGUE1_IDS", DEFAULT_LIGUE1_IDS);
        let league_cl_ids = parse_ids_env_or_default(
            "APP_LEAGUE_CHAMPIONS_LEAGUE_IDS",
            DEFAULT_CHAMPIONS_LEAGUE_IDS,
        );
        let league_wc_ids =
            parse_ids_env_or_default("APP_LEAGUE_WORLDCUP_IDS", DEFAULT_WORLDCUP_IDS);
        Self {
            screen: Screen::Pulse,
            sort: SortMode::Hot,
            league_mode: LeagueMode::PremierLeague,
            pulse_view: PulseView::Live,
            selected: 0,
            league_pl_ids,
            league_ll_ids,
            league_bl_ids,
            league_sa_ids,
            league_l1_ids,
            league_cl_ids,
            league_wc_ids,
            matches: Vec::with_capacity(32),
            upcoming: Vec::with_capacity(32),
            upcoming_scroll: 0,
            upcoming_cached_at: None,
            match_detail: HashMap::with_capacity(16),
            match_detail_cached_at: HashMap::with_capacity(16),
            logs: VecDeque::with_capacity(200),
            help_overlay: false,
            analysis: Vec::new(),
            analysis_selected: 0,
            analysis_loading: false,
            analysis_updated: None,
            analysis_fetched_at: None,
            analysis_tab: AnalysisTab::Teams,
            rankings_loading: false,
            rankings: Vec::new(),
            rankings_selected: 0,
            rankings_role: RoleCategory::Attacker,
            rankings_metric: RankMetric::Attacking,
            rankings_search: String::new(),
            rankings_search_active: false,
            rankings_progress_current: 0,
            rankings_progress_total: 0,
            rankings_progress_message: String::new(),
            rankings_cache_squads: HashMap::with_capacity(32),
            rankings_cache_players: HashMap::with_capacity(256),
            rankings_cache_squads_at: HashMap::with_capacity(32),
            rankings_cache_players_at: HashMap::with_capacity(256),
            combined_player_cache: HashMap::with_capacity(256),
            rankings_dirty: false,
            rankings_fetched_at: None,
            win_prob_history: HashMap::with_capacity(16),
            prematch_win: HashMap::with_capacity(16),
            prematch_locked: HashSet::new(),
            prediction_extras: HashMap::with_capacity(16),
            prediction_show_why: true,
            placeholder_match_enabled: false,
            squad: Vec::new(),
            squad_selected: 0,
            squad_loading: false,
            squad_team: None,
            squad_team_id: None,
            squad_prefetch_pending: None,
            player_detail: None,
            player_loading: false,
            player_last_id: None,
            player_last_name: None,
            player_detail_back: Screen::Squad,
            player_detail_scroll: 0,
            player_detail_section: 0,
            player_detail_section_scrolls: [0; PLAYER_DETAIL_SECTIONS],
            player_detail_expanded: false,
            export: ExportState::new(),
            terminal_focus: TerminalFocus::MatchList,
            terminal_detail: None,
            terminal_detail_scroll: 0,
        }
    }

    pub fn selected_match_id(&self) -> Option<String> {
        match &self.screen {
            // Terminal can be pinned to an id that isn't currently in `self.matches`
            // (e.g. when opening an upcoming fixture from Pulse).
            Screen::Terminal { match_id: Some(id) } => Some(id.clone()),
            Screen::Pulse if self.pulse_view == PulseView::Live => {
                let rows = self.pulse_live_rows();
                match rows.get(self.selected) {
                    Some(PulseLiveRow::Match(idx)) => self.matches.get(*idx).map(|m| m.id.clone()),
                    Some(PulseLiveRow::Upcoming(idx)) => {
                        self.upcoming.get(*idx).map(|u| u.id.clone())
                    }
                    None => None,
                }
            }
            _ => self.selected_match().map(|m| m.id.clone()),
        }
    }

    pub fn selected_match(&self) -> Option<&MatchSummary> {
        match &self.screen {
            Screen::Terminal { match_id: Some(id) } => self.matches.iter().find(|m| &m.id == id),
            Screen::Pulse => {
                if self.pulse_view != PulseView::Live {
                    return None;
                }
                let rows = self.pulse_live_rows();
                match rows.get(self.selected) {
                    Some(PulseLiveRow::Match(idx)) => self.matches.get(*idx),
                    _ => None,
                }
            }
            _ => {
                let filtered = self.filtered_indices();
                filtered
                    .get(self.selected)
                    .and_then(|idx| self.matches.get(*idx))
            }
        }
    }

    pub fn cycle_league_mode(&mut self) {
        self.league_mode = match self.league_mode {
            LeagueMode::PremierLeague => LeagueMode::LaLiga,
            LeagueMode::LaLiga => LeagueMode::Bundesliga,
            LeagueMode::Bundesliga => LeagueMode::SerieA,
            LeagueMode::SerieA => LeagueMode::Ligue1,
            LeagueMode::Ligue1 => LeagueMode::ChampionsLeague,
            LeagueMode::ChampionsLeague => LeagueMode::WorldCup,
            LeagueMode::WorldCup => LeagueMode::PremierLeague,
        };
        self.selected = 0;
        self.upcoming_scroll = 0;
        self.upcoming_cached_at = None;
        self.analysis.clear();
        self.analysis_selected = 0;
        self.analysis_loading = false;
        self.analysis_updated = None;
        self.analysis_fetched_at = None;
        self.analysis_tab = AnalysisTab::Teams;
        self.rankings_loading = false;
        self.rankings.clear();
        self.rankings_selected = 0;
        self.rankings_role = RoleCategory::Attacker;
        self.rankings_metric = RankMetric::Attacking;
        self.rankings_search.clear();
        self.rankings_search_active = false;
        self.rankings_progress_current = 0;
        self.rankings_progress_total = 0;
        self.rankings_progress_message.clear();
        self.rankings_cache_squads.clear();
        self.rankings_cache_players.clear();
        self.rankings_cache_squads_at.clear();
        self.rankings_cache_players_at.clear();
        self.combined_player_cache.clear();
        self.rankings_dirty = false;
        self.rankings_fetched_at = None;
        self.win_prob_history.clear();
        self.prematch_win.clear();
        self.prematch_locked.clear();
        self.placeholder_match_enabled = false;
        self.matches.clear();
        self.match_detail.clear();
        self.match_detail_cached_at.clear();
        self.squad.clear();
        self.squad_selected = 0;
        self.squad_loading = false;
        self.squad_team = None;
        self.squad_team_id = None;
        self.squad_prefetch_pending = None;
        self.player_detail = None;
        self.player_loading = false;
        self.player_last_id = None;
        self.player_last_name = None;
        self.player_detail_back = Screen::Squad;
        self.player_detail_scroll = 0;
        self.player_detail_section = 0;
        self.player_detail_section_scrolls = [0; PLAYER_DETAIL_SECTIONS];
        self.player_detail_expanded = false;
        self.terminal_focus = TerminalFocus::MatchList;
        self.terminal_detail = None;
        self.terminal_detail_scroll = 0;
        self.push_log(format!(
            "[INFO] League mode: {}",
            league_label(self.league_mode)
        ));
    }

    pub fn toggle_pulse_view(&mut self) {
        self.pulse_view = match self.pulse_view {
            PulseView::Live => PulseView::Upcoming,
            PulseView::Upcoming => PulseView::Live,
        };
        self.selected = 0;
        self.upcoming_scroll = 0;
    }

    pub fn cycle_sort(&mut self) {
        self.sort = match self.sort {
            SortMode::Hot => SortMode::Time,
            SortMode::Time => SortMode::Close,
            SortMode::Close => SortMode::Upset,
            SortMode::Upset => SortMode::Hot,
        };
        self.sort_matches();
    }

    pub fn sort_matches(&mut self) {
        self.sort_matches_with_selected_id(None);
    }

    pub fn sort_matches_with_selected_id(&mut self, selected_id: Option<String>) {
        let selected_id = selected_id.or_else(|| self.selected_match_id());
        match self.sort {
            SortMode::Hot => self.matches.sort_by(|a, b| {
                b.win
                    .delta_home
                    .abs()
                    .partial_cmp(&a.win.delta_home.abs())
                    .unwrap_or(std::cmp::Ordering::Equal)
            }),
            SortMode::Time => self.matches.sort_by(|a, b| match (a.is_live, b.is_live) {
                (true, false) => std::cmp::Ordering::Less,
                (false, true) => std::cmp::Ordering::Greater,
                _ => a.minute.cmp(&b.minute),
            }),
            SortMode::Close => self.matches.sort_by(|a, b| {
                b.win
                    .p_draw
                    .partial_cmp(&a.win.p_draw)
                    .unwrap_or(std::cmp::Ordering::Equal)
            }),
            SortMode::Upset => self.matches.sort_by(|a, b| {
                let a_underdog = a.win.p_home.min(a.win.p_away);
                let b_underdog = b.win.p_home.min(b.win.p_away);
                b_underdog
                    .partial_cmp(&a_underdog)
                    .unwrap_or(std::cmp::Ordering::Equal)
            }),
        }

        if matches!(self.screen, Screen::Pulse) && self.pulse_view == PulseView::Live {
            if let Some(id) = selected_id {
                let rows = self.pulse_live_rows();
                if let Some(pos) = rows.iter().position(|row| match row {
                    PulseLiveRow::Match(idx) => self.matches.get(*idx).is_some_and(|m| m.id == id),
                    PulseLiveRow::Upcoming(idx) => {
                        self.upcoming.get(*idx).is_some_and(|u| u.id == id)
                    }
                }) {
                    self.selected = pos;
                    return;
                }
            }
            self.selected = 0;
            return;
        }

        if let Some(id) = selected_id {
            let filtered = self.filtered_indices();
            if let Some(pos) = filtered.iter().position(|idx| self.matches[*idx].id == id) {
                self.selected = pos;
                return;
            }
        }
        self.selected = 0;
    }

    pub fn select_next(&mut self) {
        if matches!(self.screen, Screen::Pulse) && self.pulse_view == PulseView::Upcoming {
            self.scroll_upcoming_down();
            return;
        }
        let total = if matches!(self.screen, Screen::Pulse) && self.pulse_view == PulseView::Live {
            self.pulse_live_rows().len()
        } else {
            self.filtered_indices().len()
        };
        if total == 0 {
            self.selected = 0;
            return;
        }
        self.selected = (self.selected + 1) % total;
    }

    pub fn select_prev(&mut self) {
        if matches!(self.screen, Screen::Pulse) && self.pulse_view == PulseView::Upcoming {
            self.scroll_upcoming_up();
            return;
        }
        let total = if matches!(self.screen, Screen::Pulse) && self.pulse_view == PulseView::Live {
            self.pulse_live_rows().len()
        } else {
            self.filtered_indices().len()
        };
        if total == 0 {
            self.selected = 0;
            return;
        }
        if self.selected == 0 {
            self.selected = total - 1;
        } else {
            self.selected -= 1;
        }
    }

    pub fn clamp_selection(&mut self) {
        let total = if matches!(self.screen, Screen::Pulse) && self.pulse_view == PulseView::Live {
            self.pulse_live_rows().len()
        } else {
            self.filtered_indices().len()
        };
        if total == 0 {
            self.selected = 0;
        } else if self.selected >= total {
            self.selected = total - 1;
        }
    }

    pub fn filtered_indices(&self) -> Vec<usize> {
        self.matches
            .iter()
            .enumerate()
            .filter(|(_, m)| self.matches_mode(m))
            .map(|(idx, _)| idx)
            .collect()
    }

    pub fn filtered_matches(&self) -> Vec<&MatchSummary> {
        let indices = self.filtered_indices();
        indices
            .into_iter()
            .filter_map(|idx| self.matches.get(idx))
            .collect()
    }

    pub fn pulse_live_rows(&self) -> Vec<PulseLiveRow> {
        use std::collections::HashSet;

        // Keep match ordering as already sorted by `self.sort`.
        let match_indices = self.filtered_indices();
        let mut seen_ids: HashSet<&str> = HashSet::new();
        let mut rows = Vec::new();
        for idx in match_indices {
            if let Some(m) = self.matches.get(idx) {
                seen_ids.insert(m.id.as_str());
                rows.push(PulseLiveRow::Match(idx));
            }
        }

        // Upcoming appended (sorted by kickoff text), de-duped by fixture id.
        let mut upcoming_indices: Vec<usize> = self
            .upcoming
            .iter()
            .enumerate()
            .filter(|(_, u)| self.upcoming_matches_mode(u) && !seen_ids.contains(u.id.as_str()))
            .map(|(idx, _)| idx)
            .collect();
        upcoming_indices.sort_by(|a, b| {
            let ka = self
                .upcoming
                .get(*a)
                .map(|u| u.kickoff.as_str())
                .unwrap_or("");
            let kb = self
                .upcoming
                .get(*b)
                .map(|u| u.kickoff.as_str())
                .unwrap_or("");
            ka.cmp(kb)
        });
        for idx in upcoming_indices {
            if let Some(u) = self.upcoming.get(idx) {
                if !seen_ids.insert(u.id.as_str()) {
                    continue;
                }
            }
            rows.push(PulseLiveRow::Upcoming(idx));
        }

        rows
    }

    pub fn filtered_upcoming(&self) -> Vec<&UpcomingMatch> {
        self.upcoming
            .iter()
            .filter(|m| self.upcoming_matches_mode(m))
            .collect()
    }

    pub fn push_log(&mut self, msg: impl Into<String>) {
        const MAX_LOGS: usize = 200;
        self.logs.push_back(msg.into());
        while self.logs.len() > MAX_LOGS {
            self.logs.pop_front();
        }
    }

    pub fn matches_league_mode(&self, m: &MatchSummary) -> bool {
        self.matches_mode(m)
    }

    pub fn upcoming_matches_league_mode(&self, m: &UpcomingMatch) -> bool {
        self.upcoming_matches_mode(m)
    }

    fn matches_mode(&self, m: &MatchSummary) -> bool {
        match self.league_mode {
            LeagueMode::PremierLeague => matches_league(
                m,
                &self.league_pl_ids,
                &["premier league", "premier", "epl"],
            ),
            LeagueMode::LaLiga => matches_league(
                m,
                &self.league_ll_ids,
                &["la liga", "laliga", "primera division"],
            ),
            LeagueMode::Bundesliga => {
                matches_league(m, &self.league_bl_ids, &["bundesliga", "1. bundesliga"])
            }
            LeagueMode::SerieA => matches_league(m, &self.league_sa_ids, &["serie a", "seria a"]),
            LeagueMode::Ligue1 => matches_league(m, &self.league_l1_ids, &["ligue 1", "ligue1"]),
            LeagueMode::ChampionsLeague => matches_league(
                m,
                &self.league_cl_ids,
                &["champions league", "uefa champions league", "ucl"],
            ),
            LeagueMode::WorldCup => {
                matches_league(m, &self.league_wc_ids, &["world cup", "worldcup"])
            }
        }
    }

    fn upcoming_matches_mode(&self, m: &UpcomingMatch) -> bool {
        match self.league_mode {
            LeagueMode::PremierLeague => matches_league_upcoming(
                m,
                &self.league_pl_ids,
                &["premier league", "premier", "epl"],
            ),
            LeagueMode::LaLiga => matches_league_upcoming(
                m,
                &self.league_ll_ids,
                &["la liga", "laliga", "primera division"],
            ),
            LeagueMode::Bundesliga => {
                matches_league_upcoming(m, &self.league_bl_ids, &["bundesliga", "1. bundesliga"])
            }
            LeagueMode::SerieA => {
                matches_league_upcoming(m, &self.league_sa_ids, &["serie a", "seria a"])
            }
            LeagueMode::Ligue1 => {
                matches_league_upcoming(m, &self.league_l1_ids, &["ligue 1", "ligue1"])
            }
            LeagueMode::ChampionsLeague => matches_league_upcoming(
                m,
                &self.league_cl_ids,
                &["champions league", "uefa champions league", "ucl"],
            ),
            LeagueMode::WorldCup => {
                matches_league_upcoming(m, &self.league_wc_ids, &["world cup", "worldcup"])
            }
        }
    }

    fn upcoming_line_count(&self) -> usize {
        self.filtered_upcoming().len()
    }

    fn scroll_upcoming_down(&mut self) {
        let max_lines = self.upcoming_line_count();
        if max_lines == 0 {
            self.upcoming_scroll = 0;
            return;
        }
        let max_scroll = (max_lines - 1).min(u16::MAX as usize) as u16;
        if self.upcoming_scroll < max_scroll {
            self.upcoming_scroll += 1;
        }
    }

    fn scroll_upcoming_up(&mut self) {
        self.upcoming_scroll = self.upcoming_scroll.saturating_sub(1);
    }

    pub fn selected_analysis(&self) -> Option<&TeamAnalysis> {
        self.analysis.get(self.analysis_selected)
    }

    pub fn cycle_analysis_tab(&mut self) {
        self.analysis_tab = match self.analysis_tab {
            AnalysisTab::Teams => AnalysisTab::RoleRankings,
            AnalysisTab::RoleRankings => AnalysisTab::Teams,
        };
        self.analysis_selected = 0;
        self.rankings_selected = 0;
        self.rankings_search_active = false;
    }

    pub fn cycle_terminal_focus_next(&mut self) {
        self.terminal_focus = match self.terminal_focus {
            TerminalFocus::MatchList => TerminalFocus::Pitch,
            TerminalFocus::Pitch => TerminalFocus::EventTape,
            TerminalFocus::EventTape => TerminalFocus::Commentary,
            TerminalFocus::Commentary => TerminalFocus::Stats,
            TerminalFocus::Stats => TerminalFocus::Lineups,
            TerminalFocus::Lineups => TerminalFocus::Prediction,
            TerminalFocus::Prediction => TerminalFocus::Console,
            TerminalFocus::Console => TerminalFocus::MatchList,
        };
    }

    pub fn cycle_terminal_focus_prev(&mut self) {
        self.terminal_focus = match self.terminal_focus {
            TerminalFocus::MatchList => TerminalFocus::Console,
            TerminalFocus::Pitch => TerminalFocus::MatchList,
            TerminalFocus::EventTape => TerminalFocus::Pitch,
            TerminalFocus::Commentary => TerminalFocus::EventTape,
            TerminalFocus::Stats => TerminalFocus::Commentary,
            TerminalFocus::Lineups => TerminalFocus::Stats,
            TerminalFocus::Prediction => TerminalFocus::Lineups,
            TerminalFocus::Console => TerminalFocus::Prediction,
        };
    }

    pub fn cycle_rankings_role_next(&mut self) {
        self.rankings_role = match self.rankings_role {
            RoleCategory::Goalkeeper => RoleCategory::Defender,
            RoleCategory::Defender => RoleCategory::Midfielder,
            RoleCategory::Midfielder => RoleCategory::Attacker,
            RoleCategory::Attacker => RoleCategory::Goalkeeper,
        };
        self.rankings_selected = 0;
    }

    pub fn cycle_rankings_role_prev(&mut self) {
        self.rankings_role = match self.rankings_role {
            RoleCategory::Goalkeeper => RoleCategory::Attacker,
            RoleCategory::Defender => RoleCategory::Goalkeeper,
            RoleCategory::Midfielder => RoleCategory::Defender,
            RoleCategory::Attacker => RoleCategory::Midfielder,
        };
        self.rankings_selected = 0;
    }

    pub fn cycle_rankings_metric(&mut self) {
        self.rankings_metric = match self.rankings_metric {
            RankMetric::Attacking => RankMetric::Defending,
            RankMetric::Defending => RankMetric::Attacking,
        };
        self.rankings_selected = 0;
    }

    pub fn rankings_filtered(&self) -> Vec<&RoleRankingEntry> {
        let league_team_ids: std::collections::HashSet<u32> =
            self.analysis.iter().map(|t| t.id).collect();
        let filter_by_team = !league_team_ids.is_empty();
        let query = self.rankings_search.trim().to_lowercase();
        let has_query = !query.is_empty();
        self.rankings
            .iter()
            .filter(|row| row.role == self.rankings_role)
            .filter(|row| {
                if filter_by_team {
                    league_team_ids.contains(&row.team_id)
                } else {
                    true
                }
            })
            .filter(|row| {
                if !has_query {
                    return true;
                }
                row.player_name.to_lowercase().contains(&query)
                    || row.team_name.to_lowercase().contains(&query)
                    || row.club.to_lowercase().contains(&query)
            })
            .collect()
    }

    pub fn clamp_rankings_selection(&mut self) {
        let total = self.rankings_filtered().len();
        if total == 0 {
            self.rankings_selected = 0;
        } else if self.rankings_selected >= total {
            self.rankings_selected = total.saturating_sub(1);
        }
    }

    pub fn select_rankings_next(&mut self) {
        let total = self.rankings_filtered().len();
        if total == 0 {
            self.rankings_selected = 0;
            return;
        }
        self.rankings_selected = (self.rankings_selected + 1) % total;
    }

    pub fn select_rankings_prev(&mut self) {
        let total = self.rankings_filtered().len();
        if total == 0 {
            self.rankings_selected = 0;
            return;
        }
        if self.rankings_selected == 0 {
            self.rankings_selected = total - 1;
        } else {
            self.rankings_selected -= 1;
        }
    }

    pub fn selected_squad_player(&self) -> Option<&SquadPlayer> {
        self.squad.get(self.squad_selected)
    }

    pub fn select_analysis_next(&mut self) {
        let total = self.analysis.len();
        if total == 0 {
            self.analysis_selected = 0;
            return;
        }
        self.analysis_selected = (self.analysis_selected + 1) % total;
    }

    pub fn select_analysis_prev(&mut self) {
        let total = self.analysis.len();
        if total == 0 {
            self.analysis_selected = 0;
            return;
        }
        if self.analysis_selected == 0 {
            self.analysis_selected = total - 1;
        } else {
            self.analysis_selected -= 1;
        }
    }

    pub fn select_squad_next(&mut self) {
        let total = self.squad.len();
        if total == 0 {
            self.squad_selected = 0;
            return;
        }
        self.squad_selected = (self.squad_selected + 1) % total;
    }

    pub fn select_squad_prev(&mut self) {
        let total = self.squad.len();
        if total == 0 {
            self.squad_selected = 0;
            return;
        }
        if self.squad_selected == 0 {
            self.squad_selected = total - 1;
        } else {
            self.squad_selected -= 1;
        }
    }

    pub fn scroll_player_detail_down(&mut self, max_scroll: u16) {
        if self.player_detail_scroll < max_scroll {
            self.player_detail_scroll = (self.player_detail_scroll + 1).min(max_scroll);
        }
        if let Some(scroll) = self
            .player_detail_section_scrolls
            .get_mut(self.player_detail_section)
            && *scroll < max_scroll
        {
            *scroll = (*scroll + 1).min(max_scroll);
        }
    }

    pub fn scroll_player_detail_up(&mut self) {
        if self.player_detail_scroll > 0 {
            self.player_detail_scroll = self.player_detail_scroll.saturating_sub(1);
        }
        if let Some(scroll) = self
            .player_detail_section_scrolls
            .get_mut(self.player_detail_section)
            && *scroll > 0
        {
            *scroll = scroll.saturating_sub(1);
        }
    }

    pub fn cycle_player_detail_section_next(&mut self) {
        self.player_detail_section = (self.player_detail_section + 1) % PLAYER_DETAIL_SECTIONS;
    }

    pub fn cycle_player_detail_section_prev(&mut self) {
        if self.player_detail_section == 0 {
            self.player_detail_section = PLAYER_DETAIL_SECTIONS - 1;
        } else {
            self.player_detail_section -= 1;
        }
    }
}

pub const PLAYER_DETAIL_SECTIONS: usize = 9;

#[derive(Debug, Clone)]
pub struct ExportState {
    pub active: bool,
    pub done: bool,
    pub path: Option<String>,
    pub current: usize,
    pub total: usize,
    pub message: String,
    pub error_count: usize,
    pub last_updated: Option<std::time::Instant>,
}

impl Default for ExportState {
    fn default() -> Self {
        Self::new()
    }
}

impl ExportState {
    pub fn new() -> Self {
        Self {
            active: false,
            done: false,
            path: None,
            current: 0,
            total: 0,
            message: String::new(),
            error_count: 0,
            last_updated: None,
        }
    }

    pub fn clear_if_done_for(&mut self, now: std::time::Instant, keep_secs: u64) {
        if !self.active || !self.done {
            return;
        }
        let Some(last) = self.last_updated else {
            return;
        };
        if now.duration_since(last).as_secs() >= keep_secs {
            *self = Self::new();
        }
    }
}

#[derive(Debug, Clone)]
pub struct MatchSummary {
    pub id: String,
    pub league_id: Option<u32>,
    pub league_name: String,
    pub home: String,
    pub away: String,
    pub minute: u16,
    pub score_home: u8,
    pub score_away: u8,
    pub win: WinProbRow,
    pub is_live: bool,
}

#[derive(Debug, Clone)]
pub struct WinProbRow {
    pub p_home: f32,
    pub p_draw: f32,
    pub p_away: f32,
    pub delta_home: f32,
    pub quality: ModelQuality,
    pub confidence: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MatchDetail {
    #[serde(default)]
    pub home_team: Option<String>,
    #[serde(default)]
    pub away_team: Option<String>,
    pub events: Vec<Event>,
    #[serde(default)]
    pub commentary: Vec<CommentaryEntry>,
    #[serde(default)]
    pub commentary_error: Option<String>,
    pub lineups: Option<MatchLineups>,
    pub stats: Vec<StatRow>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpcomingMatch {
    #[allow(dead_code)]
    pub id: String,
    pub league_id: Option<u32>,
    pub league_name: String,
    pub round: String,
    pub kickoff: String,
    pub home: String,
    pub away: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub minute: u16,
    pub kind: EventKind,
    pub team: String,
    pub description: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommentaryEntry {
    pub minute: Option<u16>,
    pub minute_plus: Option<u16>,
    pub team: Option<String>,
    pub text: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlayerSlot {
    #[serde(default)]
    pub id: Option<u32>,
    pub name: String,
    pub number: Option<u32>,
    pub pos: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LineupSide {
    pub team: String,
    pub team_abbr: String,
    pub formation: String,
    pub starting: Vec<PlayerSlot>,
    pub subs: Vec<PlayerSlot>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MatchLineups {
    pub sides: Vec<LineupSide>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatRow {
    #[serde(default)]
    pub group: Option<String>,
    pub name: String,
    pub home: String,
    pub away: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TeamAnalysis {
    pub id: u32,
    pub name: String,
    pub confed: Confederation,
    pub host: bool,
    pub fifa_rank: Option<u32>,
    pub fifa_points: Option<u32>,
    pub fifa_updated: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SquadPlayer {
    pub id: u32,
    pub name: String,
    pub role: String,
    pub club: String,
    pub age: Option<u32>,
    pub height: Option<u32>,
    pub shirt_number: Option<u32>,
    pub market_value: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlayerDetail {
    pub id: u32,
    pub name: String,
    pub team: Option<String>,
    pub position: Option<String>,
    pub age: Option<String>,
    pub country: Option<String>,
    pub height: Option<String>,
    pub preferred_foot: Option<String>,
    pub shirt: Option<String>,
    pub market_value: Option<String>,
    pub contract_end: Option<String>,
    pub birth_date: Option<String>,
    pub status: Option<String>,
    pub injury_info: Option<String>,
    pub international_duty: Option<String>,
    pub positions: Vec<String>,
    pub all_competitions: Vec<PlayerStatItem>,
    pub all_competitions_season: Option<String>,
    pub main_league: Option<PlayerLeagueStats>,
    pub top_stats: Vec<PlayerStatItem>,
    pub season_groups: Vec<PlayerStatGroup>,
    pub season_performance: Vec<PlayerSeasonPerformanceGroup>,
    pub traits: Option<PlayerTraitGroup>,
    pub recent_matches: Vec<PlayerMatchStat>,
    pub season_breakdown: Vec<PlayerSeasonTournamentStat>,
    pub career_sections: Vec<PlayerCareerSection>,
    pub trophies: Vec<PlayerTrophyEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlayerSeasonPerformanceGroup {
    pub title: String,
    pub items: Vec<PlayerSeasonPerformanceItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlayerSeasonPerformanceItem {
    pub title: String,
    pub total: String,
    pub per90: Option<String>,
    #[serde(default)]
    pub percentile_rank: Option<f64>,
    #[serde(default)]
    pub percentile_rank_per90: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlayerStatItem {
    pub title: String,
    pub value: String,
    #[serde(default)]
    pub percentile_rank: Option<f64>,
    #[serde(default)]
    pub percentile_rank_per90: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlayerStatGroup {
    pub title: String,
    pub items: Vec<PlayerStatItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlayerLeagueStats {
    pub league_name: String,
    pub season: String,
    pub stats: Vec<PlayerStatItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlayerTraitGroup {
    pub title: String,
    pub items: Vec<PlayerTraitItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlayerTraitItem {
    pub title: String,
    pub value: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlayerMatchStat {
    pub opponent: String,
    pub league: String,
    pub date: String,
    pub goals: u8,
    pub assists: u8,
    pub rating: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlayerSeasonTournamentStat {
    pub league: String,
    pub season: String,
    pub appearances: String,
    pub goals: String,
    pub assists: String,
    pub rating: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlayerCareerSection {
    pub title: String,
    pub entries: Vec<PlayerCareerEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlayerCareerEntry {
    pub team: String,
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    pub appearances: Option<String>,
    pub goals: Option<String>,
    pub assists: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlayerTrophyEntry {
    pub team: String,
    pub league: String,
    pub seasons_won: Vec<String>,
    pub seasons_runner_up: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RankFactor {
    pub label: String,
    pub z: f64,
    pub weight: f64,
    #[serde(default)]
    pub raw: Option<f64>,
    #[serde(default)]
    pub pct: Option<f64>,
    pub source: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoleRankingEntry {
    pub role: RoleCategory,
    pub player_id: u32,
    pub player_name: String,
    pub team_id: u32,
    pub team_name: String,
    pub club: String,
    pub attack_score: f64,
    pub defense_score: f64,
    pub rating: Option<f64>,
    #[serde(default)]
    pub attack_factors: Vec<RankFactor>,
    #[serde(default)]
    pub defense_factors: Vec<RankFactor>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EventKind {
    Shot,
    Card,
    Sub,
    Goal,
}

#[derive(Debug, Clone)]
pub enum Delta {
    SetMatches(Vec<MatchSummary>),
    SetMatchDetails {
        id: String,
        detail: MatchDetail,
    },
    SetMatchDetailsBasic {
        id: String,
        detail: MatchDetail,
    },
    UpsertMatch(MatchSummary),
    SetUpcoming(Vec<UpcomingMatch>),
    AddEvent {
        id: String,
        event: Event,
    },
    SetAnalysis {
        mode: LeagueMode,
        teams: Vec<TeamAnalysis>,
    },
    CacheSquad {
        team_id: u32,
        players: Vec<SquadPlayer>,
    },
    CachePlayerDetail(PlayerDetail),
    RankCacheProgress {
        mode: LeagueMode,
        current: usize,
        total: usize,
        message: String,
    },
    RankCacheFinished {
        mode: LeagueMode,
        errors: Vec<String>,
    },
    SetSquad {
        team_name: String,
        team_id: u32,
        players: Vec<SquadPlayer>,
    },
    SetPlayerDetail(PlayerDetail),
    ExportStarted {
        path: String,
        total: usize,
    },
    ExportProgress {
        current: usize,
        total: usize,
        message: String,
    },
    ExportFinished {
        path: String,
        current: usize,
        total: usize,
        teams: usize,
        players: usize,
        stats: usize,
        info_rows: usize,
        season_breakdown: usize,
        career_rows: usize,
        trophies: usize,
        recent_matches: usize,
        errors: usize,
    },
    Log(String),
}

#[derive(Debug, Clone)]
pub enum ProviderCommand {
    FetchMatchDetails {
        fixture_id: String,
    },
    FetchMatchDetailsBasic {
        fixture_id: String,
    },
    FetchUpcoming,
    FetchAnalysis {
        mode: LeagueMode,
    },
    FetchSquad {
        team_id: u32,
        team_name: String,
    },
    FetchPlayer {
        player_id: u32,
        player_name: String,
    },
    PrefetchPlayers {
        player_ids: Vec<u32>,
    },
    WarmRankCacheFull {
        mode: LeagueMode,
    },
    WarmRankCacheMissing {
        mode: LeagueMode,
        team_ids: Vec<u32>,
        player_ids: Vec<u32>,
    },
    ExportAnalysis {
        path: String,
        mode: LeagueMode,
    },
}

pub fn apply_delta(state: &mut AppState, delta: Delta) {
    match delta {
        Delta::SetMatches(mut matches) => {
            let selected_id = state.selected_match_id();
            let preserve_index = matches!(state.screen, Screen::Pulse)
                && state.pulse_view == PulseView::Live
                && selected_id.is_none();
            let preserved_selected = state.selected;
            for summary in &mut matches {
                if !state.prematch_locked.contains(&summary.id) {
                    // If this match just flipped from not-started into live, freeze the pre-match
                    // snapshot for later reference.
                    if let Some(prev) = state.matches.iter().find(|m| m.id == summary.id)
                        && !prev.is_live
                        && prev.minute == 0
                        && (summary.is_live || summary.minute > 0)
                    {
                        state
                            .prematch_win
                            .entry(summary.id.clone())
                            .or_insert_with(|| prev.win.clone());
                        state.prematch_locked.insert(summary.id.clone());
                    }
                }

                let detail = state.match_detail.get(&summary.id);
                let (win, extras) = win_prob::compute_win_prob_explainable(
                    summary,
                    detail,
                    &state.combined_player_cache,
                    &state.analysis,
                );
                summary.win = win;
                if let Some(extras) = extras {
                    state.prediction_extras.insert(summary.id.clone(), extras);
                }
                if let Some(existing) = state.matches.iter().find(|m| m.id == summary.id) {
                    summary.win.delta_home = summary.win.p_home - existing.win.p_home;
                } else {
                    summary.win.delta_home = 0.0;
                }

                // Keep updating the "pre-match" preview until kickoff, then freeze it.
                if !state.prematch_locked.contains(&summary.id)
                    && !summary.is_live
                    && summary.minute == 0
                {
                    state
                        .prematch_win
                        .insert(summary.id.clone(), summary.win.clone());
                } else if (summary.is_live || summary.minute > 0)
                    && !state.prematch_locked.contains(&summary.id)
                {
                    // If we missed the not-started window, synthesize a pre-match snapshot once.
                    let mut pre = summary.clone();
                    pre.is_live = false;
                    pre.minute = 0;
                    pre.score_home = 0;
                    pre.score_away = 0;
                    let detail = state.match_detail.get(&pre.id);
                    let (prematch, extras) = win_prob::compute_win_prob_explainable(
                        &pre,
                        detail,
                        &state.combined_player_cache,
                        &state.analysis,
                    );
                    if let Some(extras) = extras {
                        state.prediction_extras.insert(pre.id.clone(), extras);
                    }
                    state.prematch_win.entry(pre.id.clone()).or_insert(prematch);
                    state.prematch_locked.insert(pre.id);
                }
            }
            if state.placeholder_match_enabled
                && !matches.iter().any(|m| m.id == PLACEHOLDER_MATCH_ID)
            {
                matches.push(placeholder_match_summary(state.league_mode));
            }
            if state.placeholder_match_enabled
                && !state.match_detail.contains_key(PLACEHOLDER_MATCH_ID)
            {
                state
                    .match_detail
                    .insert(PLACEHOLDER_MATCH_ID.to_string(), placeholder_match_detail());
                state
                    .match_detail_cached_at
                    .insert(PLACEHOLDER_MATCH_ID.to_string(), SystemTime::now());
            }
            state.matches = matches;
            state.sort_matches_with_selected_id(selected_id);
            if preserve_index {
                state.selected =
                    preserved_selected.min(state.pulse_live_rows().len().saturating_sub(1));
            }
        }
        Delta::SetMatchDetails { id, detail } => {
            let match_id = id.clone();
            state.match_detail.insert(id.clone(), detail);
            state
                .match_detail_cached_at
                .insert(id.clone(), SystemTime::now());

            // Update prediction immediately when details (stats/lineups) arrive.
            if let Some(existing) = state.matches.iter_mut().find(|m| m.id == id) {
                let prev_p_home = existing.win.p_home;
                let detail_ref = state.match_detail.get(&id);
                let (win, extras) = win_prob::compute_win_prob_explainable(
                    existing,
                    detail_ref,
                    &state.combined_player_cache,
                    &state.analysis,
                );
                existing.win = win;
                if let Some(extras) = extras {
                    state.prediction_extras.insert(id.clone(), extras);
                }
                existing.win.delta_home = existing.win.p_home - prev_p_home;

                let entry = state.win_prob_history.entry(id).or_default();
                entry.push(existing.win.p_home);
                if entry.len() > 40 {
                    let drain_count = entry.len() - 40;
                    entry.drain(..drain_count);
                }
            }

            // Update pre-match preview if not started and not locked yet.
            if let Some(m) = state.matches.iter().find(|m| m.id == match_id)
                && !state.prematch_locked.contains(&match_id)
                && !m.is_live
                && m.minute == 0
            {
                state.prematch_win.insert(match_id, m.win.clone());
            }
        }
        Delta::SetMatchDetailsBasic { id, detail } => {
            let match_id = id.clone();
            let mut detail = detail;
            if let Some(existing) = state.match_detail.get(&id) {
                // Basic fetches should not clobber commentary a user explicitly fetched.
                if detail.commentary.is_empty() && !existing.commentary.is_empty() {
                    detail.commentary = existing.commentary.clone();
                    detail.commentary_error = existing.commentary_error.clone();
                }

                // Basic fetches may be partial; avoid clobbering richer detail when fields are empty.
                if detail.home_team.is_none() && existing.home_team.is_some() {
                    detail.home_team = existing.home_team.clone();
                }
                if detail.away_team.is_none() && existing.away_team.is_some() {
                    detail.away_team = existing.away_team.clone();
                }
                if detail.events.is_empty() && !existing.events.is_empty() {
                    detail.events = existing.events.clone();
                }
                if detail.stats.is_empty() && !existing.stats.is_empty() {
                    detail.stats = existing.stats.clone();
                }
                if detail.lineups.is_none() && existing.lineups.is_some() {
                    detail.lineups = existing.lineups.clone();
                }

                // Preserve existing commentary error if the new response is silent and we still
                // have no commentary content.
                if detail.commentary.is_empty()
                    && detail.commentary_error.is_none()
                    && existing.commentary_error.is_some()
                {
                    detail.commentary_error = existing.commentary_error.clone();
                }
            }

            state.match_detail.insert(id.clone(), detail);
            state
                .match_detail_cached_at
                .insert(id.clone(), SystemTime::now());

            // Update prediction immediately when details (stats/lineups) arrive.
            if let Some(existing) = state.matches.iter_mut().find(|m| m.id == id) {
                let prev_p_home = existing.win.p_home;
                let detail_ref = state.match_detail.get(&id);
                let (win, extras) = win_prob::compute_win_prob_explainable(
                    existing,
                    detail_ref,
                    &state.combined_player_cache,
                    &state.analysis,
                );
                existing.win = win;
                if let Some(extras) = extras {
                    state.prediction_extras.insert(id.clone(), extras);
                }
                existing.win.delta_home = existing.win.p_home - prev_p_home;

                let entry = state.win_prob_history.entry(id).or_default();
                entry.push(existing.win.p_home);
                if entry.len() > 40 {
                    let drain_count = entry.len() - 40;
                    entry.drain(..drain_count);
                }
            }

            // Update pre-match preview if not started and not locked yet.
            if let Some(m) = state.matches.iter().find(|m| m.id == match_id)
                && !state.prematch_locked.contains(&match_id)
                && !m.is_live
                && m.minute == 0
            {
                state.prematch_win.insert(match_id, m.win.clone());
            }
        }
        Delta::UpsertMatch(mut summary) => {
            let match_id = summary.id.clone();
            let detail = state.match_detail.get(&match_id);
            let (win, extras) = win_prob::compute_win_prob_explainable(
                &summary,
                detail,
                &state.combined_player_cache,
                &state.analysis,
            );
            summary.win = win;
            if let Some(extras) = extras {
                state.prediction_extras.insert(match_id.clone(), extras);
            }
            let home_prob = summary.win.p_home;
            if let Some(existing) = state.matches.iter_mut().find(|m| m.id == summary.id) {
                // Freeze pre-match snapshot when the match starts.
                if !state.prematch_locked.contains(&match_id)
                    && !existing.is_live
                    && existing.minute == 0
                    && (summary.is_live || summary.minute > 0)
                {
                    state
                        .prematch_win
                        .entry(match_id.clone())
                        .or_insert_with(|| existing.win.clone());
                    state.prematch_locked.insert(match_id.clone());
                }
                summary.win.delta_home = summary.win.p_home - existing.win.p_home;
                *existing = summary;
            } else {
                summary.win.delta_home = 0.0;
                state.matches.push(summary);
            }

            // Keep updating the "pre-match" preview until kickoff, then freeze it.
            if let Some(m) = state.matches.iter().find(|m| m.id == match_id) {
                if !state.prematch_locked.contains(&match_id) && !m.is_live && m.minute == 0 {
                    state.prematch_win.insert(match_id.clone(), m.win.clone());
                } else if (m.is_live || m.minute > 0) && !state.prematch_locked.contains(&match_id)
                {
                    // If we missed the not-started window, synthesize a pre-match snapshot once.
                    let mut pre = m.clone();
                    pre.is_live = false;
                    pre.minute = 0;
                    pre.score_home = 0;
                    pre.score_away = 0;
                    let detail = state.match_detail.get(&match_id);
                    let (prematch, extras) = win_prob::compute_win_prob_explainable(
                        &pre,
                        detail,
                        &state.combined_player_cache,
                        &state.analysis,
                    );
                    if let Some(extras) = extras {
                        state.prediction_extras.insert(match_id.clone(), extras);
                    }
                    state
                        .prematch_win
                        .entry(match_id.clone())
                        .or_insert(prematch);
                    state.prematch_locked.insert(match_id.clone());
                }
            }

            let entry = state.win_prob_history.entry(match_id).or_default();
            entry.push(home_prob);
            if entry.len() > 40 {
                let drain_count = entry.len() - 40;
                entry.drain(..drain_count);
            }
            state.clamp_selection();
        }
        Delta::SetUpcoming(fixtures) => {
            state.upcoming = fixtures;
            state.upcoming_cached_at = Some(SystemTime::now());
            // Always reset scroll so new data is immediately visible when the user visits Upcoming.
            state.upcoming_scroll = 0;

            // Seed/refresh pre-match previews for upcoming fixtures (until kickoff).
            // These will be replaced with richer predictions once matchDetails are fetched.
            for u in &state.upcoming {
                if state.prematch_locked.contains(&u.id) {
                    continue;
                }
                let summary = MatchSummary {
                    id: u.id.clone(),
                    league_id: u.league_id,
                    league_name: u.league_name.clone(),
                    home: u.home.clone(),
                    away: u.away.clone(),
                    minute: 0,
                    score_home: 0,
                    score_away: 0,
                    win: WinProbRow {
                        p_home: 0.0,
                        p_draw: 0.0,
                        p_away: 0.0,
                        delta_home: 0.0,
                        quality: ModelQuality::Basic,
                        confidence: 0,
                    },
                    is_live: false,
                };
                let detail = state.match_detail.get(&u.id);
                let (prematch, extras) = win_prob::compute_win_prob_explainable(
                    &summary,
                    detail,
                    &state.combined_player_cache,
                    &state.analysis,
                );
                if let Some(extras) = extras {
                    state.prediction_extras.insert(u.id.clone(), extras);
                }
                state.prematch_win.insert(u.id.clone(), prematch);
            }
        }
        Delta::AddEvent { id, event } => {
            let entry = state.match_detail.entry(id).or_insert_with(|| MatchDetail {
                home_team: None,
                away_team: None,
                events: Vec::new(),
                commentary: Vec::new(),
                commentary_error: None,
                lineups: None,
                stats: Vec::new(),
            });
            entry.events.push(event);
        }
        Delta::SetAnalysis { mode, teams } => {
            if mode != state.league_mode {
                // Stale response from a previously selected league – discard.
                return;
            }
            state.analysis_updated = teams.iter().find_map(|t| t.fifa_updated.clone());
            state.analysis_fetched_at = Some(SystemTime::now());
            state.analysis = teams;
            state.analysis_loading = false;
            state.analysis_selected = 0;
        }
        Delta::CacheSquad { team_id, players } => {
            if !players.is_empty() {
                state.rankings_cache_squads.insert(team_id, players);
                state
                    .rankings_cache_squads_at
                    .insert(team_id, SystemTime::now());
                state.rankings_dirty = true;
            }
        }
        Delta::CachePlayerDetail(detail) => {
            let detail_id = detail.id;
            state
                .combined_player_cache
                .insert(detail_id, detail.clone());
            state.rankings_cache_players.insert(detail_id, detail);
            state
                .rankings_cache_players_at
                .insert(detail_id, SystemTime::now());
            state.rankings_dirty = true;
        }
        Delta::RankCacheProgress {
            mode,
            current,
            total,
            message,
        } => {
            if mode != state.league_mode {
                return;
            }
            state.rankings_loading = true;
            state.rankings_progress_current = current;
            state.rankings_progress_total = total;
            state.rankings_progress_message = message;
        }
        Delta::RankCacheFinished { mode, errors } => {
            if mode != state.league_mode {
                return;
            }
            state.rankings_loading = false;
            state.rankings_progress_current = state
                .rankings_progress_total
                .max(state.rankings_progress_current);
            state.rankings_progress_message = format!("Cache warm done ({} errors)", errors.len());
            for err in errors {
                state.push_log(format!("[WARN] Rankings cache: {err}"));
            }
        }
        Delta::SetSquad {
            team_name,
            team_id,
            players,
        } => {
            // Always cache for rankings reuse, even if stale for the UI.
            if !players.is_empty() {
                state.rankings_cache_squads.insert(team_id, players.clone());
                state
                    .rankings_cache_squads_at
                    .insert(team_id, SystemTime::now());
                state.rankings_dirty = true;
            }

            // Only update the visible squad if this is still the team the user selected.
            if state.squad_team_id.is_some() && state.squad_team_id != Some(team_id) {
                return;
            }

            state.squad = players;
            state.squad_selected = 0;
            state.squad_loading = false;
            state.squad_team = Some(team_name);
            state.squad_team_id = Some(team_id);
            if state.squad.is_empty() {
                state.squad_prefetch_pending = None;
            } else {
                state.squad_prefetch_pending = Some(state.squad.iter().map(|p| p.id).collect());
            }
        }
        Delta::SetPlayerDetail(detail) => {
            let is_stub = player_detail_is_stub(&detail);
            let keep_existing = state
                .player_detail
                .as_ref()
                .map(|existing| existing.id == detail.id && !player_detail_is_stub(existing))
                .unwrap_or(false);
            if !is_stub || !keep_existing {
                state.player_detail = Some(detail);
                state.player_detail_scroll = 0;
                state.player_detail_section = 0;
                state.player_detail_section_scrolls = [0; PLAYER_DETAIL_SECTIONS];
            }
            state.player_loading = false;
            // Cache for rankings reuse.
            if let Some(detail) = state.player_detail.clone()
                && !player_detail_is_stub(&detail)
            {
                let detail_id = detail.id;
                state.rankings_cache_players.insert(detail_id, detail);
                state
                    .rankings_cache_players_at
                    .insert(detail_id, SystemTime::now());
                state.rankings_dirty = true;
            }
        }
        Delta::ExportStarted { path, total } => {
            state.export.active = true;
            state.export.path = Some(path);
            state.export.total = total;
            state.export.current = 0;
            state.export.message = "Starting export".to_string();
            state.export.done = false;
            state.export.error_count = 0;
            state.export.last_updated = Some(std::time::Instant::now());
        }
        Delta::ExportProgress {
            current,
            total,
            message,
        } => {
            state.export.active = true;
            state.export.total = total;
            state.export.current = current;
            state.export.message = message;
            state.export.last_updated = Some(std::time::Instant::now());
        }
        Delta::ExportFinished {
            path,
            current,
            total,
            teams,
            players,
            stats,
            info_rows,
            season_breakdown,
            career_rows,
            trophies,
            recent_matches,
            errors,
        } => {
            state.export.active = true;
            state.export.path = Some(path);
            state.export.current = current;
            state.export.total = total;
            state.export.message = format!(
                "Done: {teams} teams, {players} players, {stats} stats, {info_rows} info, {season_breakdown} seasons, {career_rows} career, {trophies} trophies, {recent_matches} recent ({errors} errors)"
            );
            state.export.done = true;
            state.export.error_count = errors;
            state.export.last_updated = Some(std::time::Instant::now());
            state.push_log(format!("[INFO] Export finished ({errors} errors)"));
        }
        Delta::Log(msg) => state.push_log(msg),
    }
}

pub fn role_label(role: RoleCategory) -> &'static str {
    match role {
        RoleCategory::Goalkeeper => "Goalkeeper",
        RoleCategory::Defender => "Defender",
        RoleCategory::Midfielder => "Midfielder",
        RoleCategory::Attacker => "Attacker",
    }
}

pub fn metric_label(metric: RankMetric) -> &'static str {
    match metric {
        RankMetric::Attacking => "Attacking",
        RankMetric::Defending => "Defending",
    }
}

fn parse_ids_env_or_default(key: &str, default_ids: &[u32]) -> Vec<u32> {
    match env::var(key) {
        Ok(raw) => {
            if raw.trim().is_empty() {
                Vec::new()
            } else {
                parse_ids(raw)
            }
        }
        Err(_) => default_ids.to_vec(),
    }
}

fn parse_ids(raw: String) -> Vec<u32> {
    raw.split([',', ';', ' '])
        .filter_map(|part| part.trim().parse::<u32>().ok())
        .collect()
}

fn matches_league(m: &MatchSummary, ids: &[u32], keywords: &[&str]) -> bool {
    if let Some(id) = m.league_id
        && !ids.is_empty()
    {
        return ids.contains(&id);
    }
    if !m.league_name.is_empty() {
        return keywords
            .iter()
            .any(|kw| contains_ascii_ci(&m.league_name, kw));
    }
    ids.is_empty()
}

fn matches_league_upcoming(m: &UpcomingMatch, ids: &[u32], keywords: &[&str]) -> bool {
    if let Some(id) = m.league_id
        && !ids.is_empty()
    {
        return ids.contains(&id);
    }
    if !m.league_name.is_empty() {
        return keywords
            .iter()
            .any(|kw| contains_ascii_ci(&m.league_name, kw));
    }
    ids.is_empty()
}

/// Case-insensitive ASCII substring search without allocating a lowercased copy.
fn contains_ascii_ci(haystack: &str, needle: &str) -> bool {
    let h = haystack.as_bytes();
    let n = needle.as_bytes();
    if n.len() > h.len() {
        return false;
    }
    if n.is_empty() {
        return true;
    }
    h.windows(n.len())
        .any(|window| window.iter().zip(n).all(|(a, b)| a.eq_ignore_ascii_case(b)))
}

pub fn league_label(mode: LeagueMode) -> &'static str {
    match mode {
        LeagueMode::PremierLeague => "Premier League",
        LeagueMode::LaLiga => "La Liga",
        LeagueMode::Bundesliga => "Bundesliga",
        LeagueMode::SerieA => "Serie A",
        LeagueMode::Ligue1 => "Ligue 1",
        LeagueMode::ChampionsLeague => "Champions League",
        LeagueMode::WorldCup => "World Cup",
    }
}

pub fn confed_label(confed: Confederation) -> &'static str {
    match confed {
        Confederation::AFC => "AFC",
        Confederation::CAF => "CAF",
        Confederation::CONCACAF => "CONCACAF",
        Confederation::CONMEBOL => "CONMEBOL",
        Confederation::UEFA => "UEFA",
        Confederation::OFC => "OFC",
    }
}

pub fn player_detail_is_stub(detail: &PlayerDetail) -> bool {
    detail.team.is_none()
        && detail.position.is_none()
        && detail.age.is_none()
        && detail.country.is_none()
        && detail.height.is_none()
        && detail.preferred_foot.is_none()
        && detail.shirt.is_none()
        && detail.market_value.is_none()
        && detail.contract_end.is_none()
        && detail.birth_date.is_none()
        && detail.status.is_none()
        && detail.injury_info.is_none()
        && detail.international_duty.is_none()
        && detail.positions.is_empty()
        && detail.all_competitions.is_empty()
        && detail.all_competitions_season.is_none()
        && detail.main_league.is_none()
        && detail.top_stats.is_empty()
        && detail.season_groups.is_empty()
        && detail.season_performance.is_empty()
        && detail.traits.is_none()
        && detail.recent_matches.is_empty()
        && detail.season_breakdown.is_empty()
        && detail.career_sections.is_empty()
        && detail.trophies.is_empty()
}
