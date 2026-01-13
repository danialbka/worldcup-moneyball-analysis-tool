use std::collections::{HashMap, VecDeque};
use std::env;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Screen {
    Pulse,
    Terminal { match_id: Option<String> },
    Analysis,
    Squad,
    PlayerDetail,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeagueMode {
    PremierLeague,
    WorldCup,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
    pub league_wc_ids: Vec<u32>,
    pub matches: Vec<MatchSummary>,
    pub upcoming: Vec<UpcomingMatch>,
    pub upcoming_scroll: u16,
    pub match_detail: HashMap<String, MatchDetail>,
    pub logs: VecDeque<String>,
    pub help_overlay: bool,
    pub analysis: Vec<TeamAnalysis>,
    pub analysis_selected: usize,
    pub analysis_loading: bool,
    pub analysis_updated: Option<String>,
    pub squad: Vec<SquadPlayer>,
    pub squad_selected: usize,
    pub squad_loading: bool,
    pub squad_team: Option<String>,
    pub squad_team_id: Option<u32>,
    pub player_detail: Option<PlayerDetail>,
    pub player_loading: bool,
    pub player_last_id: Option<u32>,
    pub player_last_name: Option<String>,
    pub player_detail_scroll: u16,
    pub player_detail_section: usize,
    pub player_detail_section_scrolls: [u16; PLAYER_DETAIL_SECTIONS],
}

impl AppState {
    pub fn new() -> Self {
        let league_pl_ids = parse_ids_env("APP_LEAGUE_PREMIER_IDS");
        let league_wc_ids = parse_ids_env("APP_LEAGUE_WORLDCUP_IDS");
        Self {
            screen: Screen::Pulse,
            sort: SortMode::Hot,
            league_mode: LeagueMode::PremierLeague,
            pulse_view: PulseView::Live,
            selected: 0,
            league_pl_ids,
            league_wc_ids,
            matches: Vec::new(),
            upcoming: Vec::new(),
            upcoming_scroll: 0,
            match_detail: HashMap::new(),
            logs: VecDeque::new(),
            help_overlay: false,
            analysis: Vec::new(),
            analysis_selected: 0,
            analysis_loading: false,
            analysis_updated: None,
            squad: Vec::new(),
            squad_selected: 0,
            squad_loading: false,
            squad_team: None,
            squad_team_id: None,
            player_detail: None,
            player_loading: false,
            player_last_id: None,
            player_last_name: None,
            player_detail_scroll: 0,
            player_detail_section: 0,
            player_detail_section_scrolls: [0; PLAYER_DETAIL_SECTIONS],
        }
    }

    pub fn selected_match_id(&self) -> Option<String> {
        self.selected_match().map(|m| m.id.clone())
    }

    pub fn selected_match(&self) -> Option<&MatchSummary> {
        if matches!(self.screen, Screen::Pulse) && self.pulse_view != PulseView::Live {
            return None;
        }
        let filtered = self.filtered_indices();
        filtered
            .get(self.selected)
            .and_then(|idx| self.matches.get(*idx))
    }

    pub fn cycle_league_mode(&mut self) {
        self.league_mode = match self.league_mode {
            LeagueMode::PremierLeague => LeagueMode::WorldCup,
            LeagueMode::WorldCup => LeagueMode::PremierLeague,
        };
        self.selected = 0;
        self.upcoming_scroll = 0;
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
            SortMode::Time => self.matches.sort_by(|a, b| {
                match (a.is_live, b.is_live) {
                    (true, false) => std::cmp::Ordering::Less,
                    (false, true) => std::cmp::Ordering::Greater,
                    _ => a.minute.cmp(&b.minute),
                }
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

        if let Some(id) = selected_id {
            let filtered = self.filtered_indices();
            if let Some(pos) = filtered
                .iter()
                .position(|idx| self.matches[*idx].id == id)
            {
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
        let total = self.filtered_indices().len();
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
        let total = self.filtered_indices().len();
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
        let total = self.filtered_indices().len();
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

    fn matches_mode(&self, m: &MatchSummary) -> bool {
        match self.league_mode {
            LeagueMode::PremierLeague => {
                matches_league(m, &self.league_pl_ids, &["premier league", "premier", "epl"])
            }
            LeagueMode::WorldCup => {
                matches_league(m, &self.league_wc_ids, &["world cup", "worldcup"])
            }
        }
    }

    fn upcoming_matches_mode(&self, m: &UpcomingMatch) -> bool {
        match self.league_mode {
            LeagueMode::PremierLeague => {
                matches_league_upcoming(m, &self.league_pl_ids, &["premier league", "premier", "epl"])
            }
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
        {
            if *scroll < max_scroll {
                *scroll = (*scroll + 1).min(max_scroll);
            }
        }
    }

    pub fn scroll_player_detail_up(&mut self) {
        if self.player_detail_scroll > 0 {
            self.player_detail_scroll = self.player_detail_scroll.saturating_sub(1);
        }
        if let Some(scroll) = self
            .player_detail_section_scrolls
            .get_mut(self.player_detail_section)
        {
            if *scroll > 0 {
                *scroll = scroll.saturating_sub(1);
            }
        }
    }

    pub fn cycle_player_detail_section_next(&mut self) {
        self.player_detail_section =
            (self.player_detail_section + 1) % PLAYER_DETAIL_SECTIONS;
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

#[derive(Debug, Clone)]
pub struct MatchDetail {
    pub events: Vec<Event>,
    pub lineups: Option<MatchLineups>,
    pub stats: Vec<StatRow>,
}

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
pub struct Event {
    pub minute: u16,
    pub kind: EventKind,
    pub team: String,
    pub description: String,
}

#[derive(Debug, Clone)]
pub struct PlayerSlot {
    pub name: String,
    pub number: Option<u32>,
    pub pos: Option<String>,
}

#[derive(Debug, Clone)]
pub struct LineupSide {
    pub team: String,
    pub team_abbr: String,
    pub formation: String,
    pub starting: Vec<PlayerSlot>,
    pub subs: Vec<PlayerSlot>,
}

#[derive(Debug, Clone)]
pub struct MatchLineups {
    pub sides: Vec<LineupSide>,
}

#[derive(Debug, Clone)]
pub struct StatRow {
    pub name: String,
    pub home: String,
    pub away: String,
}

#[derive(Debug, Clone)]
pub struct TeamAnalysis {
    pub id: u32,
    pub name: String,
    pub confed: Confederation,
    pub host: bool,
    pub fifa_rank: Option<u32>,
    pub fifa_points: Option<u32>,
    pub fifa_updated: Option<String>,
}

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
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
    pub traits: Option<PlayerTraitGroup>,
    pub recent_matches: Vec<PlayerMatchStat>,
    pub season_breakdown: Vec<PlayerSeasonTournamentStat>,
    pub career_sections: Vec<PlayerCareerSection>,
    pub trophies: Vec<PlayerTrophyEntry>,
}

#[derive(Debug, Clone)]
pub struct PlayerStatItem {
    pub title: String,
    pub value: String,
}

#[derive(Debug, Clone)]
pub struct PlayerStatGroup {
    pub title: String,
    pub items: Vec<PlayerStatItem>,
}

#[derive(Debug, Clone)]
pub struct PlayerLeagueStats {
    pub league_name: String,
    pub season: String,
    pub stats: Vec<PlayerStatItem>,
}

#[derive(Debug, Clone)]
pub struct PlayerTraitGroup {
    pub title: String,
    pub items: Vec<PlayerTraitItem>,
}

#[derive(Debug, Clone)]
pub struct PlayerTraitItem {
    pub title: String,
    pub value: f32,
}

#[derive(Debug, Clone)]
pub struct PlayerMatchStat {
    pub opponent: String,
    pub league: String,
    pub date: String,
    pub goals: u8,
    pub assists: u8,
    pub rating: Option<String>,
}

#[derive(Debug, Clone)]
pub struct PlayerSeasonTournamentStat {
    pub league: String,
    pub season: String,
    pub appearances: String,
    pub goals: String,
    pub assists: String,
    pub rating: String,
}

#[derive(Debug, Clone)]
pub struct PlayerCareerSection {
    pub title: String,
    pub entries: Vec<PlayerCareerEntry>,
}

#[derive(Debug, Clone)]
pub struct PlayerCareerEntry {
    pub team: String,
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    pub appearances: Option<String>,
    pub goals: Option<String>,
    pub assists: Option<String>,
}

#[derive(Debug, Clone)]
pub struct PlayerTrophyEntry {
    pub team: String,
    pub league: String,
    pub seasons_won: Vec<String>,
    pub seasons_runner_up: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventKind {
    Shot,
    Card,
    Sub,
    Goal,
}

#[derive(Debug, Clone)]
pub enum Delta {
    SetMatches(Vec<MatchSummary>),
    SetMatchDetails { id: String, detail: MatchDetail },
    UpsertMatch(MatchSummary),
    SetUpcoming(Vec<UpcomingMatch>),
    AddEvent { id: String, event: Event },
    SetAnalysis(Vec<TeamAnalysis>),
    SetSquad { team_name: String, team_id: u32, players: Vec<SquadPlayer> },
    SetPlayerDetail(PlayerDetail),
    Log(String),
}

#[derive(Debug, Clone)]
pub enum ProviderCommand {
    FetchMatchDetails { fixture_id: String },
    FetchUpcoming,
    FetchAnalysis,
    FetchSquad { team_id: u32, team_name: String },
    FetchPlayer { player_id: u32, player_name: String },
}

pub fn apply_delta(state: &mut AppState, delta: Delta) {
    match delta {
        Delta::SetMatches(mut matches) => {
            let selected_id = state.selected_match_id();
            for summary in &mut matches {
                if let Some(existing) = state.matches.iter().find(|m| m.id == summary.id) {
                    summary.win.delta_home = summary.win.p_home - existing.win.p_home;
                } else {
                    summary.win.delta_home = 0.0;
                }
            }
            state.matches = matches;
            state.sort_matches_with_selected_id(selected_id);
        }
        Delta::SetMatchDetails { id, detail } => {
            state.match_detail.insert(id, detail);
        }
        Delta::UpsertMatch(mut summary) => {
            if let Some(existing) = state.matches.iter_mut().find(|m| m.id == summary.id) {
                summary.win.delta_home = summary.win.p_home - existing.win.p_home;
                *existing = summary;
            } else {
                summary.win.delta_home = 0.0;
                state.matches.push(summary);
            }
            state.clamp_selection();
        }
        Delta::SetUpcoming(fixtures) => {
            state.upcoming = fixtures;
            if state.pulse_view == PulseView::Upcoming {
                state.upcoming_scroll = 0;
            }
        }
        Delta::AddEvent { id, event } => {
            let entry = state
                .match_detail
                .entry(id)
                .or_insert_with(|| MatchDetail {
                    events: Vec::new(),
                    lineups: None,
                    stats: Vec::new(),
                });
            entry.events.push(event);
        }
        Delta::SetAnalysis(items) => {
            state.analysis_updated = items.iter().find_map(|t| t.fifa_updated.clone());
            state.analysis = items;
            state.analysis_loading = false;
            state.analysis_selected = 0;
        }
        Delta::SetSquad {
            team_name,
            team_id,
            players,
        } => {
            state.squad = players;
            state.squad_selected = 0;
            state.squad_loading = false;
            state.squad_team = Some(team_name);
            state.squad_team_id = Some(team_id);
        }
        Delta::SetPlayerDetail(detail) => {
            state.player_detail = Some(detail);
            state.player_loading = false;
            state.player_detail_scroll = 0;
            state.player_detail_section = 0;
            state.player_detail_section_scrolls = [0; PLAYER_DETAIL_SECTIONS];
        }
        Delta::Log(msg) => state.push_log(msg),
    }
}

fn parse_ids_env(key: &str) -> Vec<u32> {
    env::var(key)
        .ok()
        .map(parse_ids)
        .unwrap_or_default()
}

fn parse_ids(raw: String) -> Vec<u32> {
    raw.split(|c| c == ',' || c == ';' || c == ' ')
        .filter_map(|part| part.trim().parse::<u32>().ok())
        .collect()
}

fn matches_league(m: &MatchSummary, ids: &[u32], keywords: &[&str]) -> bool {
    if let Some(id) = m.league_id {
        if !ids.is_empty() {
            return ids.contains(&id);
        }
    }
    if !m.league_name.is_empty() {
        let name = m.league_name.to_lowercase();
        return keywords.iter().any(|kw| name.contains(kw));
    }
    ids.is_empty()
}

fn matches_league_upcoming(m: &UpcomingMatch, ids: &[u32], keywords: &[&str]) -> bool {
    if let Some(id) = m.league_id {
        if !ids.is_empty() {
            return ids.contains(&id);
        }
    }
    if !m.league_name.is_empty() {
        let name = m.league_name.to_lowercase();
        return keywords.iter().any(|kw| name.contains(kw));
    }
    ids.is_empty()
}

pub fn league_label(mode: LeagueMode) -> &'static str {
    match mode {
        LeagueMode::PremierLeague => "Premier League",
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
