use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::calibration;
use crate::http_cache::app_cache_dir;
use crate::team_fixtures::FixtureMatch;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeagueParams {
    pub league_id: u32,
    pub sample_matches: usize,
    pub goals_total_base: f64,
    pub home_adv_goals: f64,
    // Dixon-Coles rho (typically negative to increase low-score draws).
    pub dc_rho: f64,
}

impl LeagueParams {
    pub fn defaults(league_id: u32) -> Self {
        Self {
            league_id,
            sample_matches: 0,
            goals_total_base: 2.60,
            home_adv_goals: 0.0,
            dc_rho: -0.10,
        }
    }
}

pub fn compute_league_params(league_id: u32, fixtures: &[FixtureMatch]) -> LeagueParams {
    let mut total_goals = 0.0;
    let mut home_minus_away = 0.0;
    let mut n = 0usize;

    for m in fixtures {
        if m.league_id != league_id {
            continue;
        }
        if !m.finished || m.cancelled || m.awarded {
            continue;
        }
        if m.is_penalty_decided() {
            continue;
        }
        total_goals += (m.home_goals as f64) + (m.away_goals as f64);
        home_minus_away += (m.home_goals as f64) - (m.away_goals as f64);
        n += 1;
    }

    let mut out = LeagueParams::defaults(league_id);
    out.sample_matches = n;
    if n > 0 {
        out.goals_total_base = total_goals / (n as f64);
        out.home_adv_goals = home_minus_away / (n as f64);
    }

    // Shrink small samples toward defaults to avoid wild swings.
    const MIN_N: f64 = 200.0;
    let w = ((n as f64) / MIN_N).clamp(0.0, 1.0);
    let d = LeagueParams::defaults(league_id);
    out.goals_total_base = (1.0 - w) * d.goals_total_base + w * out.goals_total_base;
    out.home_adv_goals = (1.0 - w) * d.home_adv_goals + w * out.home_adv_goals;
    out.dc_rho = calibration::fit_dc_rho_for_league(
        league_id,
        fixtures,
        out.goals_total_base,
        out.home_adv_goals,
    );
    out
}

pub fn load_cached_params() -> HashMap<u32, LeagueParams> {
    let Some(path) = params_path() else {
        return HashMap::new();
    };
    let Ok(raw) = fs::read_to_string(path) else {
        return HashMap::new();
    };
    serde_json::from_str::<HashMap<u32, LeagueParams>>(&raw).unwrap_or_default()
}

pub fn save_cached_params(params: &HashMap<u32, LeagueParams>) -> Result<()> {
    let Some(path) = params_path() else {
        return Ok(());
    };
    if let Some(parent) = path.parent() {
        let _ = fs::create_dir_all(parent);
    }
    let tmp = path.with_extension("json.tmp");
    let json = serde_json::to_string(params).context("serialize league params")?;
    fs::write(&tmp, json).context("write league params")?;
    fs::rename(&tmp, &path).context("swap league params")?;
    Ok(())
}

fn params_path() -> Option<PathBuf> {
    app_cache_dir().map(|dir| dir.join("league_params.json"))
}
