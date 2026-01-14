use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::state::{AppState, LeagueMode, PlayerDetail, SquadPlayer, TeamAnalysis};

const CACHE_DIR: &str = "wc26_terminal";
const CACHE_FILE: &str = "cache.json";
const CACHE_VERSION: u32 = 1;

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
    state.rankings_dirty = true;
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
    Some(PathBuf::from(home).join(".cache").join(CACHE_DIR).join(CACHE_FILE))
}

fn league_key(mode: LeagueMode) -> &'static str {
    match mode {
        LeagueMode::PremierLeague => "premier_league",
        LeagueMode::WorldCup => "worldcup",
    }
}

