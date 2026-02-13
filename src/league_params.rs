use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::calibration;
use crate::http_cache::app_cache_dir;
use crate::team_fixtures::FixtureMatch;

const CAL_HALF_LIFE_MATCHES: f64 = 1200.0;
const CAL_SEASON_DECAY: f64 = 0.90;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeagueParams {
    pub league_id: u32,
    pub sample_matches: usize,
    pub goals_total_base: f64,
    pub home_adv_goals: f64,
    // Dixon-Coles rho (typically negative to increase low-score draws).
    pub dc_rho: f64,
    #[serde(default = "default_prematch_logit_scale")]
    pub prematch_logit_scale: f64,
    #[serde(default)]
    pub prematch_draw_bias: f64,
}

impl LeagueParams {
    pub fn defaults(league_id: u32) -> Self {
        Self {
            league_id,
            sample_matches: 0,
            goals_total_base: 2.60,
            home_adv_goals: 0.0,
            dc_rho: -0.10,
            prematch_logit_scale: default_prematch_logit_scale(),
            prematch_draw_bias: 0.0,
        }
    }
}

pub fn compute_league_params(league_id: u32, fixtures: &[FixtureMatch]) -> LeagueParams {
    let filtered: Vec<&FixtureMatch> = fixtures
        .iter()
        .filter(|m| m.league_id == league_id)
        .filter(|m| m.finished && !m.cancelled && !m.awarded)
        .filter(|m| !m.is_penalty_decided())
        .collect();

    let n = filtered.len();
    let mut out = LeagueParams::defaults(league_id);
    out.sample_matches = n;
    if n == 0 {
        return out;
    }

    let weights = build_fixture_weights(&filtered, CAL_HALF_LIFE_MATCHES, CAL_SEASON_DECAY);
    let mut weight_sum = 0.0_f64;
    let mut total_goals_w = 0.0_f64;
    let mut home_minus_away_w = 0.0_f64;
    let mut draw_w = 0.0_f64;
    let mut outcomes: Vec<calibration::Outcome> = Vec::with_capacity(n);

    for (m, w_raw) in filtered.iter().zip(weights.iter()) {
        let w = (*w_raw).max(1e-9);
        weight_sum += w;
        let home_goals = m.home_goals as f64;
        let away_goals = m.away_goals as f64;
        total_goals_w += w * (home_goals + away_goals);
        home_minus_away_w += w * (home_goals - away_goals);
        if m.home_goals == m.away_goals {
            draw_w += w;
        }
        outcomes.push(calibration::classify_outcome(
            m.home_goals as i32,
            m.away_goals as i32,
        ));
    }
    if weight_sum > 0.0 {
        out.goals_total_base = total_goals_w / weight_sum;
        out.home_adv_goals = home_minus_away_w / weight_sum;
    }

    // Shrink small samples toward defaults to avoid wild swings.
    const MIN_N: f64 = 200.0;
    let w = ((n as f64) / MIN_N).clamp(0.0, 1.0);
    let d = LeagueParams::defaults(league_id);
    out.goals_total_base = (1.0 - w) * d.goals_total_base + w * out.goals_total_base;
    out.home_adv_goals = (1.0 - w) * d.home_adv_goals + w * out.home_adv_goals;
    let draw_rate = if weight_sum > 0.0 {
        (draw_w / weight_sum).clamp(0.05, 0.60)
    } else {
        0.25
    };
    out.dc_rho =
        calibration::fit_dc_rho_to_draw_rate(out.goals_total_base, out.home_adv_goals, draw_rate);
    if !outcomes.is_empty() {
        let base = calibration::outcome_probs_from_params(
            out.goals_total_base,
            out.home_adv_goals,
            out.dc_rho,
        );
        let base_vec = vec![base; outcomes.len()];
        let (scale, draw_bias, _) =
            calibration::fit_logit_calibration_weighted(&base_vec, &outcomes, &weights);
        out.prematch_logit_scale = scale;
        out.prematch_draw_bias = draw_bias;
    }
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

fn default_prematch_logit_scale() -> f64 {
    1.0
}

fn build_fixture_weights(
    fixtures: &[&FixtureMatch],
    half_life_matches: f64,
    season_decay: f64,
) -> Vec<f64> {
    if fixtures.is_empty() {
        return Vec::new();
    }
    let latest_season = fixtures
        .iter()
        .map(|m| season_key(&m.utc_time))
        .max()
        .unwrap_or(0);
    let last_idx = fixtures.len().saturating_sub(1);
    let mut out = Vec::with_capacity(fixtures.len());
    for (idx, m) in fixtures.iter().enumerate() {
        let age = (last_idx.saturating_sub(idx)) as f64;
        let recency = (-(std::f64::consts::LN_2 * age / half_life_matches.max(1.0))).exp();
        let delta_season = latest_season.saturating_sub(season_key(&m.utc_time));
        let season_w = season_decay.powi(delta_season);
        out.push((recency * season_w).clamp(0.05, 1.0));
    }
    out
}

fn season_key(utc_time: &str) -> i32 {
    let mut digits = String::new();
    for ch in utc_time.chars() {
        if ch.is_ascii_digit() {
            digits.push(ch);
            if digits.len() == 4 {
                return digits.parse::<i32>().unwrap_or(0);
            }
        } else if !digits.is_empty() {
            break;
        }
    }
    0
}
