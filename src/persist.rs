use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::state::{
    AppState, LeagueMode, MatchDetail, PlayerDetail, RoleRankingEntry, SquadPlayer, TeamAnalysis,
    UpcomingMatch,
};

const CACHE_DIR: &str = "wc26_terminal";
const CACHE_FILE: &str = "cache.json";
const CACHE_VERSION: u32 = 3;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct CacheFile {
    version: u32,
    leagues: HashMap<String, LeagueCache>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct LeagueCache {
    analysis: Vec<TeamAnalysis>,
    squads: HashMap<u32, Vec<SquadPlayer>>,
    players: HashMap<u32, PlayerDetail>,
    #[serde(default)]
    squads_fetched_at: HashMap<u32, u64>,
    #[serde(default)]
    players_fetched_at: HashMap<u32, u64>,
    #[serde(default)]
    rankings: Vec<RoleRankingEntry>,
    #[serde(default)]
    upcoming: Vec<UpcomingMatch>,
    #[serde(default)]
    upcoming_fetched_at: Option<u64>,
    #[serde(default)]
    match_details: HashMap<String, MatchDetail>,
    #[serde(default)]
    match_detail_fetched_at: HashMap<String, u64>,
}

pub fn load_into_state(state: &mut AppState) {
    let Some(path) = cache_path() else {
        return;
    };
    let Ok(raw) = fs::read_to_string(&path) else {
        return;
    };
    let Ok(cache) = serde_json::from_str::<CacheFile>(&raw) else {
        return;
    };
    if cache.version != CACHE_VERSION {
        return;
    }

    let key = league_key(state.league_mode);
    let Some(league) = cache.leagues.get(key) else {
        return;
    };

    // Load analysis (so Rankings can compute without refetching teams).
    if !league.analysis.is_empty() {
        state.analysis = league.analysis.clone();
        state.analysis_loading = false;
        state.analysis_selected = 0;
    }
    state.rankings_cache_squads = league.squads.clone();
    state.rankings_cache_players = league.players.clone();
    state.rankings_cache_squads_at = league
        .squads_fetched_at
        .iter()
        .filter_map(|(id, ts)| system_time_from_secs(*ts).map(|t| (*id, t)))
        .collect();
    state.rankings_cache_players_at = league
        .players_fetched_at
        .iter()
        .filter_map(|(id, ts)| system_time_from_secs(*ts).map(|t| (*id, t)))
        .collect();
    state.rankings = league.rankings.clone();
    state.rankings_dirty = state.rankings.is_empty();

    state.combined_player_cache.clear();
    state
        .combined_player_cache
        .extend(league.players.clone().into_iter());
    if matches!(
        state.league_mode,
        LeagueMode::PremierLeague | LeagueMode::LaLiga
    ) {
        let other_key = match state.league_mode {
            LeagueMode::PremierLeague => "laliga",
            LeagueMode::LaLiga => "premier_league",
            LeagueMode::WorldCup => "",
        };
        if let Some(other) = cache.leagues.get(other_key) {
            state
                .combined_player_cache
                .extend(other.players.clone().into_iter());
        }
    }

    state.upcoming = league.upcoming.clone();
    state.upcoming_cached_at = league.upcoming_fetched_at.and_then(system_time_from_secs);
    state.match_detail = league.match_details.clone();
    state.match_detail_cached_at = league
        .match_detail_fetched_at
        .iter()
        .filter_map(|(id, ts)| system_time_from_secs(*ts).map(|t| (id.clone(), t)))
        .collect();
}

pub fn save_from_state(state: &AppState) {
    let Some(path) = cache_path() else {
        return;
    };
    let Some(dir) = path.parent() else {
        return;
    };
    let _ = fs::create_dir_all(dir);

    let mut cache = load_cache_file(&path).unwrap_or_else(|| CacheFile {
        version: CACHE_VERSION,
        leagues: HashMap::new(),
    });
    cache.version = CACHE_VERSION;

    let key = league_key(state.league_mode).to_string();
    cache.leagues.insert(
        key,
        LeagueCache {
            analysis: state.analysis.clone(),
            squads: state.rankings_cache_squads.clone(),
            players: state.rankings_cache_players.clone(),
            squads_fetched_at: state
                .rankings_cache_squads_at
                .iter()
                .filter_map(|(id, ts)| system_time_to_secs(*ts).map(|t| (*id, t)))
                .collect(),
            players_fetched_at: state
                .rankings_cache_players_at
                .iter()
                .filter_map(|(id, ts)| system_time_to_secs(*ts).map(|t| (*id, t)))
                .collect(),
            rankings: state.rankings.clone(),
            upcoming: state.upcoming.clone(),
            upcoming_fetched_at: state.upcoming_cached_at.and_then(system_time_to_secs),
            match_details: state.match_detail.clone(),
            match_detail_fetched_at: state
                .match_detail_cached_at
                .iter()
                .filter_map(|(id, ts)| system_time_to_secs(*ts).map(|t| (id.clone(), t)))
                .collect(),
        },
    );

    if let Ok(json) = serde_json::to_string(&cache) {
        let tmp = path.with_extension("json.tmp");
        if fs::write(&tmp, json).is_ok() {
            let _ = fs::rename(&tmp, &path);
        }
    }
}

fn load_cache_file(path: &Path) -> Option<CacheFile> {
    let raw = fs::read_to_string(path).ok()?;
    let cache = serde_json::from_str::<CacheFile>(&raw).ok()?;
    Some(cache)
}

fn cache_path() -> Option<PathBuf> {
    // Prefer XDG cache.
    if let Ok(base) = std::env::var("XDG_CACHE_HOME") {
        if !base.trim().is_empty() {
            return Some(PathBuf::from(base).join(CACHE_DIR).join(CACHE_FILE));
        }
    }
    // Fallback to ~/.cache on linux-like systems.
    let home = std::env::var("HOME").ok()?;
    if home.trim().is_empty() {
        return None;
    }
    Some(
        PathBuf::from(home)
            .join(".cache")
            .join(CACHE_DIR)
            .join(CACHE_FILE),
    )
}

fn system_time_to_secs(time: SystemTime) -> Option<u64> {
    time.duration_since(UNIX_EPOCH).ok().map(|d| d.as_secs())
}

fn system_time_from_secs(secs: u64) -> Option<SystemTime> {
    UNIX_EPOCH.checked_add(std::time::Duration::from_secs(secs))
}

fn league_key(mode: LeagueMode) -> &'static str {
    match mode {
        LeagueMode::PremierLeague => "premier_league",
        LeagueMode::LaLiga => "laliga",
        LeagueMode::WorldCup => "worldcup",
    }
}
