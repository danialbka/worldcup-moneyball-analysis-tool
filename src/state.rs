use std::collections::{HashMap, VecDeque};
use std::env;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Screen {
    Pulse,
    Terminal { match_id: Option<String> },
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
    Log(String),
}

#[derive(Debug, Clone)]
pub enum ProviderCommand {
    FetchMatchDetails { fixture_id: String },
    FetchUpcoming,
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
