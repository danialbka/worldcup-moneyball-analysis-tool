use std::collections::HashMap;
use std::env;
use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::calibration::{self, Prob3};
use crate::league_params::LeagueParams;
use crate::player_impact;
use crate::player_impact::TeamImpactFeatures;
use crate::state::{
    LineupSide, MarketOddsSnapshot, MatchDetail, MatchSummary, ModelQuality, PlayerDetail,
    PlayerSlot, PredictionExplain, PredictionExtras, RoleCategory, SquadPlayer, TeamAnalysis,
    WinProbRow, player_detail_is_stub,
};

const GOALS_TOTAL_BASE: f64 = 2.60;
const K_STRENGTH: f64 = 0.45;

const BASELINE_RATING: f64 = 6.80;
const RATING_STDDEV: f64 = 0.60;
const SEASON_BLEND: f64 = 0.70;
const FORM_BLEND: f64 = 0.30;

const DISC_COVERAGE_MIN: f32 = 0.40;
const K_DISC: f64 = 0.08;
const DISC_MULT_MAX: f64 = 1.06;
const DEFAULT_MODEL_WEIGHT: f32 = 0.65;
const DEFAULT_MARKET_WEIGHT: f32 = 0.35;
const DEFAULT_ODDS_STALE_TTL_SECS: i64 = 30 * 60;

#[derive(Debug, Clone, Copy)]
struct MarketBlendConfig {
    enabled: bool,
    model_weight: f32,
    market_weight: f32,
    stale_ttl_secs: i64,
}

pub fn compute_win_prob(
    summary: &MatchSummary,
    detail: Option<&MatchDetail>,
    players: &HashMap<u32, PlayerDetail>,
    squads: &HashMap<u32, Vec<SquadPlayer>>,
    _analysis: &[TeamAnalysis],
    league_params: Option<&LeagueParams>,
    _elo: Option<&HashMap<u32, f64>>,
) -> WinProbRow {
    compute_win_prob_explainable(
        summary,
        detail,
        players,
        squads,
        _analysis,
        league_params,
        _elo,
    )
    .0
}

pub fn compute_win_prob_explainable(
    summary: &MatchSummary,
    detail: Option<&MatchDetail>,
    players: &HashMap<u32, PlayerDetail>,
    squads: &HashMap<u32, Vec<SquadPlayer>>,
    _analysis: &[TeamAnalysis],
    league_params: Option<&LeagueParams>,
    _elo: Option<&HashMap<u32, f64>>,
) -> (WinProbRow, Option<PredictionExtras>) {
    // If the match is effectively final, just reflect the result.
    if !summary.is_live && summary.minute >= 90 {
        let (p_home, p_draw, p_away) = if summary.score_home > summary.score_away {
            (100.0, 0.0, 0.0)
        } else if summary.score_home < summary.score_away {
            (0.0, 0.0, 100.0)
        } else {
            (0.0, 100.0, 0.0)
        };
        return (
            WinProbRow {
                p_home,
                p_draw,
                p_away,
                delta_home: 0.0,
                quality: ModelQuality::Basic,
                confidence: 95,
            },
            None,
        );
    }

    let is_prematch = !summary.is_live
        && summary.minute == 0
        && summary.score_home == 0
        && summary.score_away == 0;

    let goals_total_base = league_params
        .map(|p| p.goals_total_base)
        .unwrap_or(GOALS_TOTAL_BASE);
    let home_adv_goals = league_params.map(|p| p.home_adv_goals).unwrap_or(0.0);
    let dc_rho = league_params.map(|p| p.dc_rho).unwrap_or(-0.10);
    let prematch_logit_scale = league_params.map(|p| p.prematch_logit_scale).unwrap_or(1.0);
    let prematch_draw_bias = league_params.map(|p| p.prematch_draw_bias).unwrap_or(0.0);

    let lineup = detail.and_then(|d| d.lineups.as_ref());
    let (home_side, away_side): (Option<&LineupSide>, Option<&LineupSide>) =
        if let Some(lineups) = lineup {
            if lineups.sides.is_empty() {
                (None, None)
            } else {
                // Prefer mapping by explicit home/away team name from match details.
                let home_name = detail
                    .and_then(|d| d.home_team.as_deref())
                    .unwrap_or_default();
                let away_name = detail
                    .and_then(|d| d.away_team.as_deref())
                    .unwrap_or_default();

                let home_key = normalize_team_key(home_name);
                let away_key = normalize_team_key(away_name);

                let mut home_side: Option<&LineupSide> = None;
                let mut away_side: Option<&LineupSide> = None;

                if !home_key.is_empty() || !away_key.is_empty() {
                    for side in &lineups.sides {
                        let team_key = normalize_team_key(&side.team);
                        if home_side.is_none() && !home_key.is_empty() && team_key == home_key {
                            home_side = Some(side);
                        }
                        if away_side.is_none() && !away_key.is_empty() && team_key == away_key {
                            away_side = Some(side);
                        }
                    }
                }

                // Fallback: match by abbreviation against match summary labels.
                let home_abbr = normalize_team_key(&summary.home);
                let away_abbr = normalize_team_key(&summary.away);
                if home_side.is_none() || away_side.is_none() {
                    for side in &lineups.sides {
                        let abbr = normalize_team_key(&side.team_abbr);
                        if home_side.is_none() && !home_abbr.is_empty() && abbr == home_abbr {
                            home_side = Some(side);
                        }
                        if away_side.is_none() && !away_abbr.is_empty() && abbr == away_abbr {
                            away_side = Some(side);
                        }
                    }
                }

                // Final fallback: ordering from `upcoming_fetch::parse_lineups` (homeTeam then awayTeam).
                (
                    home_side.or_else(|| lineups.sides.first()),
                    away_side.or_else(|| lineups.sides.get(1)),
                )
            }
        } else {
            (None, None)
        };

    let lineup_home = home_side.and_then(|h| lineup_strength_and_coverage(h, players));
    let lineup_away = away_side.and_then(|a| lineup_strength_and_coverage(a, players));

    let disc_home_lineup = home_side.and_then(|h| discipline_from_slots(&h.starting, players));
    let disc_away_lineup = away_side.and_then(|a| discipline_from_slots(&a.starting, players));

    // Team strength is driven purely by player-level lineup stats.
    // FIFA rank/points and Elo signals have been removed.
    let lineup_s_home = lineup_home.map(|(s, _)| s);
    let lineup_s_away = lineup_away.map(|(s, _)| s);
    let lineup_cov_home = lineup_home.map(|(_, c)| c);
    let lineup_cov_away = lineup_away.map(|(_, c)| c);

    let have_lineups = lineup_s_home.is_some() && lineup_s_away.is_some();

    let (s_home, s_away, blend_w_lineup) = if have_lineups {
        (
            lineup_s_home.unwrap_or(0.0),
            lineup_s_away.unwrap_or(0.0),
            1.0,
        )
    } else {
        // No lineup data available — neutral prior.
        (0.0, 0.0, 0.0)
    };

    let player_impact_home = league_player_impact_side(summary, detail, squads, true);
    let player_impact_away = league_player_impact_side(summary, detail, squads, false);
    let player_impact_signal = match (player_impact_home, player_impact_away) {
        (Some(h), Some(a)) => player_impact::global_registry()
            .map(|r| r.impact_signal_for_league(summary.league_id, h, a))
            .unwrap_or(0.0),
        _ => 0.0,
    };

    let player_impact_cov_home = player_impact_home.map(|v| v.coverage);
    let player_impact_cov_away = player_impact_away.map(|v| v.coverage);

    let diff = K_STRENGTH * ((s_home - s_away) + player_impact_signal);
    let mut lambda_home_pre = clamp(
        (goals_total_base / 2.0) + (home_adv_goals / 2.0) + (diff / 2.0),
        0.20,
        3.80,
    );
    let mut lambda_away_pre = clamp(
        (goals_total_base / 2.0) - (home_adv_goals / 2.0) - (diff / 2.0),
        0.20,
        3.80,
    );

    // Historical discipline proxy (fouls/cards) slightly boosts the opponent's scoring expectation.
    let (disc_home, disc_cov_home) = disc_home_lineup
        .filter(|(_, cov)| *cov >= DISC_COVERAGE_MIN)
        .or_else(|| {
            summary
                .home_team_id
                .and_then(|id| discipline_from_squad(id, squads, players))
                .filter(|(_, cov)| *cov >= DISC_COVERAGE_MIN)
        })
        .unwrap_or((None, 0.0));
    let (disc_away, disc_cov_away) = disc_away_lineup
        .filter(|(_, cov)| *cov >= DISC_COVERAGE_MIN)
        .or_else(|| {
            summary
                .away_team_id
                .and_then(|id| discipline_from_squad(id, squads, players))
                .filter(|(_, cov)| *cov >= DISC_COVERAGE_MIN)
        })
        .unwrap_or((None, 0.0));

    let mut disc_mult_home: f32 = 1.0;
    let mut disc_mult_away: f32 = 1.0;
    if let (Some(dh), Some(da)) = (disc_home, disc_away) {
        let delta = ((dh - da) as f64 / 100.0).clamp(-1.0, 1.0);
        if delta.abs() > 0.001 {
            let mult = clamp(1.0 + K_DISC * delta.abs(), 1.0, DISC_MULT_MAX);
            if delta > 0.0 {
                // Home is more undisciplined -> boost away scoring.
                lambda_away_pre = clamp(lambda_away_pre * mult, 0.20, 3.80);
                disc_mult_away = mult as f32;
            } else {
                // Away is more undisciplined -> boost home scoring.
                lambda_home_pre = clamp(lambda_home_pre * mult, 0.20, 3.80);
                disc_mult_home = mult as f32;
            }
        }
    }

    let effective_total = estimate_total_minutes(summary, detail);
    let minute_raw = summary.minute as f64;
    // Allow true pre-match predictions at minute 0 for non-live fixtures.
    // For live games, clamp to >= 1 to avoid overreacting to missing/0' timestamps.
    let minute = if summary.is_live {
        minute_raw.max(1.0)
    } else {
        minute_raw.max(0.0)
    }
    .min(effective_total);
    let t = minute / effective_total;
    let remain = (effective_total - minute) / effective_total;

    let track_used = have_lineups && blend_w_lineup > 0.10;
    let mut quality = if track_used {
        ModelQuality::Track
    } else {
        ModelQuality::Basic
    };

    let mut xg_present = false;
    let mut used_live_stats = false;

    // Remaining expected goals for each team (from now to FT).
    let (mut lambda_home_rem, mut lambda_away_rem) =
        (lambda_home_pre * remain, lambda_away_pre * remain);

    if summary.is_live {
        if let Some(d) = detail {
            if let Some((xg_h, xg_a)) = extract_xg_pair(d) {
                xg_present = true;
                used_live_stats = true;

                let ex_h = lambda_home_pre * t;
                let ex_a = lambda_away_pre * t;

                let mult_h = clamp((xg_h + 0.10) / (ex_h + 0.10), 0.60, 1.70);
                let mult_a = clamp((xg_a + 0.10) / (ex_a + 0.10), 0.60, 1.70);

                let alpha = clamp(t, 0.0, 0.75);

                let lambda_home_live_total = lambda_home_pre * mult_h.powf(alpha);
                let lambda_away_live_total = lambda_away_pre * mult_a.powf(alpha);

                lambda_home_rem = clamp(lambda_home_live_total * remain, 0.05, 3.00);
                lambda_away_rem = clamp(lambda_away_live_total * remain, 0.05, 3.00);
            } else if let Some((sot_h, sot_a)) =
                extract_stat_f64_pref(d, &["Top stats", "Shots"], &["shots on target"])
            {
                used_live_stats = true;
                let delta = sot_h - sot_a;
                let b = clamp(t, 0.0, 0.50);
                lambda_home_rem = clamp(
                    lambda_home_pre * remain * (1.0 + 0.05 * delta * b),
                    0.05,
                    3.00,
                );
                lambda_away_rem = clamp(
                    lambda_away_pre * remain * (1.0 - 0.05 * delta * b),
                    0.05,
                    3.00,
                );
            }

            // Extra live signals (bounded).
            apply_red_card_adjustment(summary, d, &mut lambda_home_rem, &mut lambda_away_rem);

            // If xG is missing, try other weak signals.
            if !xg_present {
                if let Some((bc_h, bc_a)) =
                    extract_stat_f64_pref(d, &["Top stats", "Shots"], &["big chances"])
                {
                    used_live_stats = true;
                    let delta = bc_h - bc_a;
                    let b = clamp(t, 0.0, 0.50);
                    lambda_home_rem = clamp(lambda_home_rem * (1.0 + 0.06 * delta * b), 0.05, 3.00);
                    lambda_away_rem = clamp(lambda_away_rem * (1.0 - 0.06 * delta * b), 0.05, 3.00);
                } else if let Some((xgot_h, xgot_a)) = extract_stat_f64_pref(
                    d,
                    &["Expected goals (xG)", "Top stats"],
                    &["xg on target", "xgot"],
                ) {
                    used_live_stats = true;
                    let delta = xgot_h - xgot_a;
                    let b = clamp(t, 0.0, 0.50);
                    lambda_home_rem = clamp(lambda_home_rem * (1.0 + 0.04 * delta * b), 0.05, 3.00);
                    lambda_away_rem = clamp(lambda_away_rem * (1.0 - 0.04 * delta * b), 0.05, 3.00);
                }

                if apply_extra_match_stats_signals(d, t, &mut lambda_home_rem, &mut lambda_away_rem)
                {
                    used_live_stats = true;
                }
            }
        }
    }

    // Late-game damping: teams protect a lead.
    if summary.is_live && summary.minute >= 75 && summary.score_home != summary.score_away {
        lambda_home_rem = clamp(lambda_home_rem * 0.90, 0.05, 3.00);
        lambda_away_rem = clamp(lambda_away_rem * 0.90, 0.05, 3.00);
    }

    if quality != ModelQuality::Track && used_live_stats {
        quality = ModelQuality::Event;
    }

    let (mut p_home_prob, mut p_draw_prob, mut p_away_prob) = if is_prematch {
        outcome_probs_poisson_dc(
            summary.score_home as u32,
            summary.score_away as u32,
            lambda_home_rem,
            lambda_away_rem,
            10,
            dc_rho,
        )
    } else {
        outcome_probs_poisson(
            summary.score_home as u32,
            summary.score_away as u32,
            lambda_home_rem,
            lambda_away_rem,
            10,
        )
    };

    if is_prematch {
        (p_home_prob, p_draw_prob, p_away_prob) = apply_prematch_logit_calibration(
            p_home_prob,
            p_draw_prob,
            p_away_prob,
            prematch_logit_scale,
            prematch_draw_bias,
        );
    }

    let mut p_home = (p_home_prob * 100.0) as f32;
    let mut p_draw = (p_draw_prob * 100.0) as f32;
    let mut p_away = (p_away_prob * 100.0) as f32;

    // Normalize to exactly 100.0 to keep UI stable.
    let sum = (p_home + p_draw + p_away).max(0.0001);
    p_home = p_home / sum * 100.0;
    p_draw = p_draw / sum * 100.0;
    p_away = p_away / sum * 100.0;
    // Put any tiny rounding residue into draw (least visually jarring).
    let residue = 100.0 - (p_home + p_draw + p_away);
    p_draw += residue;
    let p_home_model = p_home;
    let p_draw_model = p_draw;
    let p_away_model = p_away;

    let mut market_probs_used: Option<(f32, f32, f32, u8)> = None;
    let mut market_signal: Option<String> = None;
    let mut market_weight_used: Option<f32> = None;
    if is_prematch {
        let market_cfg = market_blend_config();
        if market_cfg.enabled {
            match summary.market_odds.as_ref() {
                Some(snapshot) => {
                    if market_snapshot_stale(snapshot, market_cfg.stale_ttl_secs) {
                        market_signal = Some("MARKET_STALE".to_string());
                    } else if let Some((m_home, m_draw, m_away)) =
                        market_implied_probs_percent(snapshot)
                    {
                        let w_model = market_cfg.model_weight;
                        let w_market = market_cfg.market_weight;
                        p_home = p_home * w_model + m_home * w_market;
                        p_draw = p_draw * w_model + m_draw * w_market;
                        p_away = p_away * w_model + m_away * w_market;
                        let sum = (p_home + p_draw + p_away).max(0.0001);
                        p_home = p_home / sum * 100.0;
                        p_draw = p_draw / sum * 100.0;
                        p_away = p_away / sum * 100.0;
                        let residue = 100.0 - (p_home + p_draw + p_away);
                        p_draw += residue;
                        market_probs_used =
                            Some((m_home, m_draw, m_away, snapshot.bookmakers_used));
                        market_weight_used = Some(w_market);
                        market_signal = Some(format!(
                            "MARKET_BLEND_{w_market:.2}_BK{}",
                            snapshot.bookmakers_used
                        ));
                    } else {
                        market_signal = Some("MARKET_INCOMPLETE".to_string());
                    }
                }
                None => {
                    market_signal = Some("MARKET_UNAVAILABLE".to_string());
                }
            }
        }
    }

    let confidence = if is_prematch {
        compute_confidence_prematch(blend_w_lineup)
    } else {
        compute_confidence(t, xg_present, track_used)
    };

    let win = WinProbRow {
        p_home,
        p_draw,
        p_away,
        delta_home: 0.0,
        quality,
        confidence,
    };

    let extras = if is_prematch {
        let have_disc = disc_home.is_some() && disc_away.is_some();
        Some(build_prematch_extras(
            summary.league_id,
            goals_total_base,
            home_adv_goals,
            dc_rho,
            lambda_home_pre,
            lambda_away_pre,
            lineup_s_home,
            lineup_s_away,
            player_impact_home,
            player_impact_away,
            lineup_cov_home,
            lineup_cov_away,
            player_impact_cov_home,
            player_impact_cov_away,
            blend_w_lineup,
            disc_home,
            disc_away,
            if disc_cov_home > 0.0 {
                Some(disc_cov_home)
            } else {
                None
            },
            if disc_cov_away > 0.0 {
                Some(disc_cov_away)
            } else {
                None
            },
            if have_disc {
                Some(disc_mult_home)
            } else {
                None
            },
            if have_disc {
                Some(disc_mult_away)
            } else {
                None
            },
            p_home_model,
            p_draw_model,
            p_away_model,
            win.p_home,
            win.p_draw,
            win.p_away,
            if is_prematch {
                Some(prematch_logit_scale)
            } else {
                None
            },
            if is_prematch {
                Some(prematch_draw_bias)
            } else {
                None
            },
            market_probs_used,
            market_weight_used,
            market_signal,
        ))
    } else {
        None
    };

    (win, extras)
}

fn compute_confidence(t: f64, xg_present: bool, track: bool) -> u8 {
    let mut score = 30.0 + (50.0 * t);
    if xg_present {
        score += 10.0;
    }
    if track {
        score += 10.0;
    }
    clamp(score, 5.0, 95.0).round() as u8
}

fn compute_confidence_prematch(blend_w_lineup: f32) -> u8 {
    // Confidence driven purely by lineup coverage.
    let score = 35.0 + 60.0 * (blend_w_lineup as f64);
    clamp(score, 5.0, 95.0).round() as u8
}

fn market_blend_config() -> MarketBlendConfig {
    static CONFIG: OnceLock<MarketBlendConfig> = OnceLock::new();
    *CONFIG.get_or_init(|| {
        // Temporarily paused by product request: market odds must not alter model outputs.
        let enabled = false;

        let model_raw = env::var("ODDS_MODEL_WEIGHT")
            .ok()
            .and_then(|v| v.parse::<f32>().ok())
            .unwrap_or(DEFAULT_MODEL_WEIGHT)
            .max(0.0);
        let market_raw = env::var("ODDS_MARKET_WEIGHT")
            .ok()
            .and_then(|v| v.parse::<f32>().ok())
            .unwrap_or(DEFAULT_MARKET_WEIGHT)
            .max(0.0);
        let sum = (model_raw + market_raw).max(0.0001);
        let model_weight = (model_raw / sum).clamp(0.0, 1.0);
        let market_weight = (market_raw / sum).clamp(0.0, 1.0);

        let stale_ttl_secs = env::var("ODDS_STALE_TTL_MIN")
            .ok()
            .and_then(|v| v.parse::<i64>().ok())
            .map(|mins| mins.clamp(1, 24 * 60) * 60)
            .unwrap_or(DEFAULT_ODDS_STALE_TTL_SECS);

        MarketBlendConfig {
            enabled,
            model_weight,
            market_weight,
            stale_ttl_secs,
        }
    })
}

fn market_snapshot_stale(snapshot: &MarketOddsSnapshot, stale_ttl_secs: i64) -> bool {
    if snapshot.stale {
        return true;
    }
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .map(|d| d.as_secs() as i64)
        .unwrap_or(snapshot.fetched_at_unix);
    now.saturating_sub(snapshot.fetched_at_unix) > stale_ttl_secs
}

fn market_implied_probs_percent(snapshot: &MarketOddsSnapshot) -> Option<(f32, f32, f32)> {
    let mut home = snapshot.implied_home?;
    let mut draw = snapshot.implied_draw?;
    let mut away = snapshot.implied_away?;
    if home <= 0.0 || draw <= 0.0 || away <= 0.0 {
        return None;
    }
    let sum = home + draw + away;
    if sum <= 0.0 {
        return None;
    }
    home = home / sum * 100.0;
    draw = draw / sum * 100.0;
    away = away / sum * 100.0;
    let residue = 100.0 - (home + draw + away);
    draw += residue;
    Some((home, draw, away))
}

fn build_prematch_extras(
    league_id: Option<u32>,
    goals_total_base: f64,
    home_adv_goals: f64,
    dc_rho: f64,
    lambda_home_pre: f64,
    lambda_away_pre: f64,
    s_home_lineup: Option<f64>,
    s_away_lineup: Option<f64>,
    s_home_player_impact: Option<TeamImpactFeatures>,
    s_away_player_impact: Option<TeamImpactFeatures>,
    cov_home: Option<f32>,
    cov_away: Option<f32>,
    player_cov_home: Option<f32>,
    player_cov_away: Option<f32>,
    blend_w_lineup: f32,
    disc_home: Option<f32>,
    disc_away: Option<f32>,
    disc_cov_home: Option<f32>,
    disc_cov_away: Option<f32>,
    disc_mult_home: Option<f32>,
    disc_mult_away: Option<f32>,
    p_home_model: f32,
    _p_draw_model: f32,
    _p_away_model: f32,
    p_home_final: f32,
    p_draw_final: f32,
    p_away_final: f32,
    prematch_logit_scale: Option<f64>,
    prematch_draw_bias: Option<f64>,
    market_probs_used: Option<(f32, f32, f32, u8)>,
    market_weight_used: Option<f32>,
    market_signal: Option<String>,
) -> PredictionExtras {
    let (p_home_baseline, p_draw_baseline, p_away_baseline) =
        prematch_probs_from_params(goals_total_base, 0.0, 0.0, 0.0, dc_rho);
    let (p_home_ha, p_draw_ha, p_away_ha) =
        prematch_probs_from_params(goals_total_base, home_adv_goals, 0.0, 0.0, dc_rho);

    let mut signals: Vec<String> = Vec::new();
    if home_adv_goals.abs() > 0.01 {
        signals.push(format!("HA_{:+.2}", home_adv_goals));
    }
    if let (Some(ch), Some(ca)) = (cov_home, cov_away) {
        let n_h = (ch * 11.0).round().clamp(0.0, 11.0) as u8;
        let n_a = (ca * 11.0).round().clamp(0.0, 11.0) as u8;
        signals.push(format!("LINEUP_{n_h}/11_{n_a}/11"));
    }
    if let (Some(dh), Some(da), Some(ch), Some(ca)) =
        (disc_home, disc_away, disc_cov_home, disc_cov_away)
    {
        let n_h = (ch * 11.0).round().clamp(0.0, 11.0) as u8;
        let n_a = (ca * 11.0).round().clamp(0.0, 11.0) as u8;
        let mh = disc_mult_home.unwrap_or(1.0);
        let ma = disc_mult_away.unwrap_or(1.0);
        signals.push(format!(
            "DISC_H{:.0}_A{:.0}_COV{}/{}_M{:.2}/{:.2}",
            dh, da, n_h, n_a, mh, ma
        ));
    }
    if let Some(signal) = market_signal {
        signals.push(signal);
    }
    if let (Some(scale), Some(draw_bias)) = (prematch_logit_scale, prematch_draw_bias) {
        if (scale - 1.0).abs() > 0.01 || draw_bias.abs() > 0.01 {
            signals.push(format!("CAL_S{:.2}_D{:+.2}", scale, draw_bias));
        }
    }
    if let (Some(h), Some(a), Some(ch), Some(ca)) = (
        s_home_player_impact,
        s_away_player_impact,
        player_cov_home,
        player_cov_away,
    ) {
        let (tag, weight, coeff0) = player_impact::global_registry()
            .map(|r| r.model_debug_tag(league_id))
            .unwrap_or(("NA", 0, 0.0));
        signals.push(format!(
            "PLAYER_IMPACT_{}_I{:+.2}/{:+.2}_R{:+.2}/{:+.2}_C{:.0}/{:.0}_W{}_K0{:+.2}",
            tag,
            h.impact,
            a.impact,
            h.rating,
            a.rating,
            (ch * 100.0).round(),
            (ca * 100.0).round(),
            weight,
            coeff0
        ));
    }

    let pp_home_adv = p_home_ha - p_home_baseline;
    let (p_home_ha_lineup, _, _) = prematch_probs_from_params(
        goals_total_base,
        home_adv_goals,
        s_home_lineup.unwrap_or(0.0),
        s_away_lineup.unwrap_or(0.0),
        dc_rho,
    );
    let pp_lineup = p_home_ha_lineup - p_home_ha;
    let pp_player_impact = p_home_model - p_home_ha_lineup;
    let pp_market_blend = p_home_final - p_home_model;

    PredictionExtras {
        prematch_only: true,
        goals_total_base: Some(goals_total_base),
        home_adv_goals: Some(home_adv_goals),
        dc_rho: Some(dc_rho),
        lambda_home_pre,
        lambda_away_pre,
        s_home_analysis: None,
        s_away_analysis: None,
        s_home_elo: None,
        s_away_elo: None,
        s_home_lineup,
        s_away_lineup,
        s_home_player_impact: s_home_player_impact.map(|v| v.impact),
        s_away_player_impact: s_away_player_impact.map(|v| v.impact),
        lineup_coverage_home: cov_home,
        lineup_coverage_away: cov_away,
        player_impact_cov_home: player_cov_home,
        player_impact_cov_away: player_cov_away,
        blend_w_lineup,
        market_weight_used,
        disc_home,
        disc_away,
        disc_cov_home,
        disc_cov_away,
        disc_mult_home,
        disc_mult_away,
        explain: PredictionExplain {
            p_home_baseline,
            p_draw_baseline,
            p_away_baseline,
            p_home_ha,
            p_draw_ha,
            p_away_ha,
            p_home_analysis: p_home_ha,
            p_draw_analysis: p_draw_ha,
            p_away_analysis: p_away_ha,
            p_home_market: market_probs_used.map(|(home, _, _, _)| home),
            p_draw_market: market_probs_used.map(|(_, draw, _, _)| draw),
            p_away_market: market_probs_used.map(|(_, _, away, _)| away),
            p_home_blended: market_probs_used.map(|_| p_home_final),
            p_draw_blended: market_probs_used.map(|_| p_draw_final),
            p_away_blended: market_probs_used.map(|_| p_away_final),
            p_home_final,
            p_draw_final,
            p_away_final,
            pp_home_adv,
            pp_analysis: 0.0,
            pp_lineup,
            pp_player_impact,
            pp_market_blend,
            signals,
        },
    }
}

fn league_player_impact_side(
    summary: &MatchSummary,
    detail: Option<&MatchDetail>,
    squads: &HashMap<u32, Vec<SquadPlayer>>,
    home: bool,
) -> Option<TeamImpactFeatures> {
    let Some(registry) = player_impact::global_registry() else {
        return None;
    };

    let (team_name, team_id) = if home {
        (summary.home.as_str(), summary.home_team_id)
    } else {
        (summary.away.as_str(), summary.away_team_id)
    };

    if let Some(lineups) = detail.and_then(|d| d.lineups.as_ref()) {
        let side = if home {
            lineups.sides.first()
        } else {
            lineups.sides.get(1)
        };
        if let Some(side) = side {
            let names = side
                .starting
                .iter()
                .map(|p| p.name.as_str())
                .collect::<Vec<_>>();
            if let Some(features) = registry.team_features_for_league(
                summary.league_id,
                &side.team,
                names.iter().copied(),
            ) {
                return Some(features);
            }
        }
    }

    if let Some(id) = team_id
        && let Some(squad) = squads.get(&id)
    {
        let names = squad.iter().map(|p| p.name.as_str()).collect::<Vec<_>>();
        if let Some(features) =
            registry.team_features_for_league(summary.league_id, team_name, names.iter().copied())
        {
            return Some(features);
        }
    }
    None
}

fn prematch_probs_from_params(
    goals_total_base: f64,
    home_adv_goals: f64,
    s_home: f64,
    s_away: f64,
    dc_rho: f64,
) -> (f32, f32, f32) {
    let diff = K_STRENGTH * (s_home - s_away);
    let lambda_home = clamp(
        (goals_total_base / 2.0) + (home_adv_goals / 2.0) + (diff / 2.0),
        0.20,
        3.80,
    );
    let lambda_away = clamp(
        (goals_total_base / 2.0) - (home_adv_goals / 2.0) - (diff / 2.0),
        0.20,
        3.80,
    );
    probs_percent_dc(0, 0, lambda_home, lambda_away, 10, dc_rho)
}

fn apply_prematch_logit_calibration(
    p_home: f64,
    p_draw: f64,
    p_away: f64,
    logit_scale: f64,
    draw_bias: f64,
) -> (f64, f64, f64) {
    let q = calibration::apply_logit_calibration(
        Prob3 {
            home: p_home,
            draw: p_draw,
            away: p_away,
        },
        logit_scale,
        draw_bias,
    );
    (q.home, q.draw, q.away)
}

fn probs_percent_dc(
    score_home: u32,
    score_away: u32,
    lambda_home_rem: f64,
    lambda_away_rem: f64,
    max_goals: u32,
    rho: f64,
) -> (f32, f32, f32) {
    let (p_home, p_draw, p_away) = outcome_probs_poisson_dc(
        score_home,
        score_away,
        lambda_home_rem,
        lambda_away_rem,
        max_goals,
        rho,
    );

    let mut p_home = (p_home * 100.0) as f32;
    let mut p_draw = (p_draw * 100.0) as f32;
    let mut p_away = (p_away * 100.0) as f32;
    let sum = (p_home + p_draw + p_away).max(0.0001);
    p_home = p_home / sum * 100.0;
    p_draw = p_draw / sum * 100.0;
    p_away = p_away / sum * 100.0;
    let residue = 100.0 - (p_home + p_draw + p_away);
    p_draw += residue;
    (p_home, p_draw, p_away)
}

fn estimate_total_minutes(summary: &MatchSummary, detail: Option<&MatchDetail>) -> f64 {
    // Conservative stoppage estimate based on event volume after 60'.
    if !summary.is_live {
        return 90.0;
    }
    let Some(d) = detail else {
        return 90.0;
    };

    let mut goals = 0u32;
    let mut cards = 0u32;
    let mut subs = 0u32;

    for e in &d.events {
        if e.minute < 60 {
            continue;
        }
        match e.kind {
            crate::state::EventKind::Goal => goals += 1,
            crate::state::EventKind::Card => cards += 1,
            crate::state::EventKind::Sub => subs += 1,
            crate::state::EventKind::Shot => {}
        }
    }

    let stoppage = (goals + cards + subs).min(7) as f64;
    90.0 + stoppage
}

fn apply_red_card_adjustment(
    summary: &MatchSummary,
    detail: &MatchDetail,
    lambda_home_rem: &mut f64,
    lambda_away_rem: &mut f64,
) {
    // We only have EventKind::Card; infer red via description.
    let mut red_home = 0u32;
    let mut red_away = 0u32;

    let home_name = detail
        .home_team
        .as_deref()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or(&summary.home);
    let away_name = detail
        .away_team
        .as_deref()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or(&summary.away);

    let home_key = normalize_team_key(home_name);
    let away_key = normalize_team_key(away_name);

    for e in &detail.events {
        if e.kind != crate::state::EventKind::Card {
            continue;
        }
        let desc = e.description.to_lowercase();
        if !desc.contains("red") {
            continue;
        }
        let team_key = normalize_team_key(&e.team);
        if !home_key.is_empty() && team_key == home_key {
            red_home += 1;
        } else if !away_key.is_empty() && team_key == away_key {
            red_away += 1;
        }
    }

    if red_home == 0 && red_away == 0 {
        return;
    }

    // Bounded multipliers, per red (stacking but clamped).
    if red_home > 0 {
        let penal = clamp(0.80_f64.powi(red_home as i32), 0.55, 1.0);
        let boost = clamp(1.10_f64.powi(red_home as i32), 1.0, 1.35);
        *lambda_home_rem = clamp(*lambda_home_rem * penal, 0.05, 3.00);
        *lambda_away_rem = clamp(*lambda_away_rem * boost, 0.05, 3.00);
    }
    if red_away > 0 {
        let penal = clamp(0.80_f64.powi(red_away as i32), 0.55, 1.0);
        let boost = clamp(1.10_f64.powi(red_away as i32), 1.0, 1.35);
        *lambda_away_rem = clamp(*lambda_away_rem * penal, 0.05, 3.00);
        *lambda_home_rem = clamp(*lambda_home_rem * boost, 0.05, 3.00);
    }
}

fn extract_stat_f64_pref(
    detail: &MatchDetail,
    group_prefs: &[&str],
    needles: &[&str],
) -> Option<(f64, f64)> {
    for group in group_prefs {
        if let Some(pair) = extract_stat_f64_group(detail, Some(group), needles) {
            return Some(pair);
        }
    }
    extract_stat_f64_group(detail, None, needles)
}

fn extract_xg_pair(detail: &MatchDetail) -> Option<(f64, f64)> {
    // Prefer the new FotMob title.
    if let Some(pair) = extract_stat_f64_pref(
        detail,
        &["Top stats", "Expected goals (xG)"],
        &["expected goals"],
    ) {
        return Some(pair);
    }
    // Fallback: legacy "xG" row.
    for row in &detail.stats {
        let name = row.name.trim();
        if name.eq_ignore_ascii_case("xg") && !contains_ascii_case_insensitive(name, "xgot") {
            let h = parse_stat_cell(&row.home)?;
            let a = parse_stat_cell(&row.away)?;
            return Some((h, a));
        }
    }
    None
}

fn extract_stat_f64_group(
    detail: &MatchDetail,
    group_pref: Option<&str>,
    needles: &[&str],
) -> Option<(f64, f64)> {
    let row = find_stat_row(detail, group_pref, needles)?;
    let h = parse_stat_cell(&row.home)?;
    let a = parse_stat_cell(&row.away)?;
    Some((h, a))
}

fn extract_stat_pct_pref(
    detail: &MatchDetail,
    group_prefs: &[&str],
    needles: &[&str],
) -> Option<(f64, f64)> {
    for group in group_prefs {
        if let Some(pair) = extract_stat_pct_group(detail, Some(group), needles) {
            return Some(pair);
        }
    }
    extract_stat_pct_group(detail, None, needles)
}

fn extract_stat_pct_group(
    detail: &MatchDetail,
    group_pref: Option<&str>,
    needles: &[&str],
) -> Option<(f64, f64)> {
    let row = find_stat_row(detail, group_pref, needles)?;
    let h = parse_stat_percent(&row.home)?;
    let a = parse_stat_percent(&row.away)?;
    Some((h, a))
}

fn find_stat_row<'a>(
    detail: &'a MatchDetail,
    group_pref: Option<&str>,
    needles: &[&str],
) -> Option<&'a crate::state::StatRow> {
    if needles.is_empty() {
        return None;
    }

    if let Some(group_pref) = group_pref {
        for row in &detail.stats {
            let matches_group = row
                .group
                .as_deref()
                .is_some_and(|g| g.eq_ignore_ascii_case(group_pref));
            if !matches_group {
                continue;
            }
            if stat_title_matches(&row.name, needles) {
                return Some(row);
            }
        }
    }

    for row in &detail.stats {
        if stat_title_matches(&row.name, needles) {
            return Some(row);
        }
    }
    None
}

fn stat_title_matches(title: &str, needles: &[&str]) -> bool {
    let t = title.trim();
    if t.is_empty() {
        return false;
    }
    needles
        .iter()
        .any(|n| contains_ascii_case_insensitive(t, n.trim()))
}

fn parse_stat_cell(raw: &str) -> Option<f64> {
    // Parse the first numeric token found (handles "248 (88%)", "58%", etc.)
    let s = raw.trim();
    if s.is_empty() || s == "-" {
        return None;
    }
    let s = s.replace(',', "");

    let mut buf = String::new();
    let mut started = false;
    for ch in s.chars() {
        let is_num = ch.is_ascii_digit() || ch == '.' || (ch == '-' && !started);
        if is_num {
            started = true;
            buf.push(ch);
            continue;
        }
        if started {
            break;
        }
    }
    if buf.is_empty() || buf == "-" {
        return None;
    }
    buf.parse::<f64>().ok()
}

fn parse_stat_percent(raw: &str) -> Option<f64> {
    // Prefer percent in parentheses: "248 (88%)" -> 88
    let s = raw.trim();
    if s.is_empty() || s == "-" {
        return None;
    }
    if let Some(start) = s.find('(') {
        if let Some(end) = s[start..].find(')') {
            let inner = &s[start + 1..start + end];
            if inner.contains('%') {
                let inner = inner.trim_end_matches('%').trim();
                if let Some(v) = parse_stat_cell(inner) {
                    return Some(v);
                }
            }
        }
    }
    if s.contains('%') {
        let cleaned = s.trim_end_matches('%').trim();
        return parse_stat_cell(cleaned);
    }
    None
}

fn apply_extra_match_stats_signals(
    detail: &MatchDetail,
    t: f64,
    lambda_home_rem: &mut f64,
    lambda_away_rem: &mut f64,
) -> bool {
    // Apply weak bounded live signals derived from match stats categories.
    let b = clamp(t, 0.0, 0.50);
    if b <= 0.0 {
        return false;
    }

    let mut applied = false;

    // 1) Possession tilt.
    if let Some((ph, pa)) =
        extract_stat_f64_pref(detail, &["Top stats", "Passes"], &["ball possession"])
    {
        let delta = ph - pa;
        *lambda_home_rem = clamp(*lambda_home_rem * (1.0 + 0.003 * delta * b), 0.05, 3.00);
        *lambda_away_rem = clamp(*lambda_away_rem * (1.0 - 0.003 * delta * b), 0.05, 3.00);
        applied = true;
    }

    // 2) Shot volume (when xG is missing, this is a rough proxy).
    if let Some((sh, sa)) = extract_stat_f64_pref(detail, &["Top stats", "Shots"], &["total shots"])
    {
        let delta = sh - sa;
        *lambda_home_rem = clamp(*lambda_home_rem * (1.0 + 0.02 * delta * b), 0.05, 3.00);
        *lambda_away_rem = clamp(*lambda_away_rem * (1.0 - 0.02 * delta * b), 0.05, 3.00);
        applied = true;
    }

    // 3) Passing quality (use % from "Accurate passes": "248 (88%)").
    if let Some((ah, aa)) =
        extract_stat_pct_pref(detail, &["Top stats", "Passes"], &["accurate passes"])
    {
        let delta = ah - aa;
        *lambda_home_rem = clamp(*lambda_home_rem * (1.0 + 0.002 * delta * b), 0.05, 3.00);
        *lambda_away_rem = clamp(*lambda_away_rem * (1.0 - 0.002 * delta * b), 0.05, 3.00);
        applied = true;
    }

    // 4) Duels control (ground duels %).
    if let Some((dh, da)) = extract_stat_pct_pref(detail, &["Duels"], &["ground duels won"]) {
        let delta = dh - da;
        *lambda_home_rem = clamp(*lambda_home_rem * (1.0 + 0.002 * delta * b), 0.05, 3.00);
        *lambda_away_rem = clamp(*lambda_away_rem * (1.0 - 0.002 * delta * b), 0.05, 3.00);
        applied = true;
    }

    // 5) Defence disruption (tackles+interceptions).
    let tack = extract_stat_f64_pref(detail, &["Defence"], &["tackles"]);
    let ints = extract_stat_f64_pref(detail, &["Defence"], &["interceptions"]);
    if let (Some((th, ta)), Some((ih, ia))) = (tack, ints) {
        let delta = (th + ih) - (ta + ia);
        *lambda_home_rem = clamp(*lambda_home_rem * (1.0 + 0.005 * delta * b), 0.05, 3.00);
        *lambda_away_rem = clamp(*lambda_away_rem * (1.0 - 0.005 * delta * b), 0.05, 3.00);
        applied = true;
    }

    // 6) Discipline (fouls committed, small negative).
    if let Some((fh, fa)) =
        extract_stat_f64_pref(detail, &["Top stats", "Discipline"], &["fouls committed"])
    {
        let delta = fh - fa;
        *lambda_home_rem = clamp(*lambda_home_rem * (1.0 - 0.01 * delta * b), 0.05, 3.00);
        *lambda_away_rem = clamp(*lambda_away_rem * (1.0 + 0.01 * delta * b), 0.05, 3.00);
        applied = true;
    }

    applied
}

fn contains_ascii_case_insensitive(haystack: &str, needle: &str) -> bool {
    if needle.is_empty() {
        return true;
    }
    let h = haystack.as_bytes();
    let n = needle.as_bytes();
    if n.len() > h.len() {
        return false;
    }
    for start in 0..=h.len() - n.len() {
        if h[start].to_ascii_lowercase().eq(&n[0].to_ascii_lowercase()) {
            let mut matched = true;
            for idx in 1..n.len() {
                if !h[start + idx].eq_ignore_ascii_case(&n[idx]) {
                    matched = false;
                    break;
                }
            }
            if matched {
                return true;
            }
        }
    }
    false
}

fn normalize_team_key(raw: &str) -> String {
    raw.trim()
        .to_lowercase()
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || c.is_whitespace())
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn player_form_rating(p: &PlayerDetail, n: usize) -> Option<f64> {
    if p.recent_matches.is_empty() || n == 0 {
        return None;
    }
    let mut weighted = 0.0;
    let mut weight_sum = 0.0;
    let mut count = 0usize;

    for (k, m) in p.recent_matches.iter().take(n).enumerate() {
        let Some(r) = m
            .rating
            .as_deref()
            .and_then(|s| s.trim().parse::<f64>().ok())
        else {
            continue;
        };
        let w = 0.85_f64.powi(k as i32);
        weighted += w * r;
        weight_sum += w;
        count += 1;
    }

    if count == 0 || weight_sum <= 0.0 {
        return None;
    }

    let mean = weighted / weight_sum;
    let shrink = (count as f64 / 5.0).min(1.0);
    Some(shrink * mean + (1.0 - shrink) * BASELINE_RATING)
}

fn player_form_z(p: &PlayerDetail, n: usize) -> Option<f64> {
    let r = player_form_rating(p, n)?;
    let z = (r - BASELINE_RATING) / RATING_STDDEV;
    Some(clamp(z, -2.0, 2.0))
}

#[derive(Debug, Clone, Copy)]
enum Direction {
    HigherBetter,
    LowerBetter,
}

#[derive(Debug, Clone, Copy)]
enum PctStat {
    Goals,
    XgNonPenalty,
    Xa,
    ChancesCreated,
    TouchesInOppBox,
    ShotsOnTarget,
    AccuratePasses,
    PassAccuracy,
    AccurateLongBalls,
    Tackles,
    Interceptions,
    Recoveries,
    DribbledPast,
    DuelsWonPct,
    AerialsWonPct,
    Clearances,
    Blocks,
    BlockedScoringAttempt,
    XgAgainstOnPitch,
    GoalsConcededOnPitch,
    Saves,
    SavePct,
    GoalsConceded,
    CleanSheets,
    ErrorLedToGoal,
    HighClaims,
    ActedAsSweeper,
    PossWonFinalThird,
    Rating,
}

fn infer_role(slot: &PlayerSlot, detail: &PlayerDetail) -> RoleCategory {
    if let Some(pos) = slot.pos.as_deref().filter(|s| !s.trim().is_empty()) {
        if let Some(r) = role_from_pos_label(pos) {
            return r;
        }
    }
    if let Some(pos) = detail.position.as_deref().filter(|s| !s.trim().is_empty()) {
        if let Some(r) = role_from_pos_label(pos) {
            return r;
        }
    }
    for pos in &detail.positions {
        if let Some(r) = role_from_pos_label(pos) {
            return r;
        }
    }
    RoleCategory::Midfielder
}

fn role_from_pos_label(raw: &str) -> Option<RoleCategory> {
    let s = raw.trim().to_ascii_uppercase();
    if s.is_empty() {
        return None;
    }
    if s.contains("GK") || s.contains("KEEP") {
        return Some(RoleCategory::Goalkeeper);
    }
    if s.contains("CB")
        || s.contains("RB")
        || s.contains("LB")
        || s.contains("WB")
        || s.contains("DEF")
        || s.contains("BACK")
    {
        return Some(RoleCategory::Defender);
    }
    if s.contains("DM")
        || s.contains("CM")
        || s.contains("AM")
        || s.contains("MF")
        || s.contains("MID")
    {
        return Some(RoleCategory::Midfielder);
    }
    if s.contains("ST")
        || s.contains("CF")
        || s.contains("FW")
        || s.contains("LW")
        || s.contains("RW")
        || s.contains("WING")
        || s.contains("ATT")
    {
        return Some(RoleCategory::Attacker);
    }
    None
}

fn player_season_strength_z(p: &PlayerDetail, role: RoleCategory) -> Option<f64> {
    let (attack_specs, defense_specs, mix_a, mix_d) = match role {
        RoleCategory::Goalkeeper => (
            &[
                (PctStat::AccuratePasses, Direction::HigherBetter, 0.8),
                (PctStat::PassAccuracy, Direction::HigherBetter, 0.7),
                (PctStat::AccurateLongBalls, Direction::HigherBetter, 0.7),
                (PctStat::ActedAsSweeper, Direction::HigherBetter, 0.5),
                (PctStat::Rating, Direction::HigherBetter, 0.4),
            ][..],
            &[
                (PctStat::SavePct, Direction::HigherBetter, 1.3),
                (PctStat::Saves, Direction::HigherBetter, 0.8),
                (PctStat::GoalsConceded, Direction::LowerBetter, 1.1),
                (PctStat::CleanSheets, Direction::HigherBetter, 0.7),
                (PctStat::ErrorLedToGoal, Direction::LowerBetter, 0.9),
                (PctStat::HighClaims, Direction::HigherBetter, 0.5),
                (PctStat::Rating, Direction::HigherBetter, 0.4),
            ][..],
            0.2,
            0.8,
        ),
        RoleCategory::Defender => (
            &[
                (PctStat::AccuratePasses, Direction::HigherBetter, 0.8),
                (PctStat::PassAccuracy, Direction::HigherBetter, 0.7),
                (PctStat::AccurateLongBalls, Direction::HigherBetter, 0.7),
                (PctStat::Rating, Direction::HigherBetter, 0.4),
            ][..],
            &[
                (PctStat::Tackles, Direction::HigherBetter, 1.0),
                (PctStat::Interceptions, Direction::HigherBetter, 1.0),
                (PctStat::Clearances, Direction::HigherBetter, 0.9),
                (PctStat::Blocks, Direction::HigherBetter, 0.8),
                (PctStat::BlockedScoringAttempt, Direction::HigherBetter, 0.8),
                (PctStat::Recoveries, Direction::HigherBetter, 0.8),
                (PctStat::DuelsWonPct, Direction::HigherBetter, 0.8),
                (PctStat::AerialsWonPct, Direction::HigherBetter, 0.9),
                (PctStat::DribbledPast, Direction::LowerBetter, 0.8),
                (PctStat::GoalsConcededOnPitch, Direction::LowerBetter, 0.7),
                (PctStat::XgAgainstOnPitch, Direction::LowerBetter, 0.7),
                (PctStat::Rating, Direction::HigherBetter, 0.3),
            ][..],
            0.35,
            0.65,
        ),
        RoleCategory::Midfielder => (
            &[
                (PctStat::Xa, Direction::HigherBetter, 1.2),
                (PctStat::ChancesCreated, Direction::HigherBetter, 1.0),
                (PctStat::AccuratePasses, Direction::HigherBetter, 0.9),
                (PctStat::PassAccuracy, Direction::HigherBetter, 0.7),
                (PctStat::AccurateLongBalls, Direction::HigherBetter, 0.6),
                (PctStat::Rating, Direction::HigherBetter, 0.6),
            ][..],
            &[
                (PctStat::Tackles, Direction::HigherBetter, 0.9),
                (PctStat::Interceptions, Direction::HigherBetter, 0.9),
                (PctStat::Recoveries, Direction::HigherBetter, 0.9),
                (PctStat::DuelsWonPct, Direction::HigherBetter, 0.6),
                (PctStat::DribbledPast, Direction::LowerBetter, 0.6),
                (PctStat::PossWonFinalThird, Direction::HigherBetter, 0.6),
                (PctStat::Rating, Direction::HigherBetter, 0.4),
            ][..],
            0.5,
            0.5,
        ),
        RoleCategory::Attacker => (
            &[
                (PctStat::Goals, Direction::HigherBetter, 1.2),
                (PctStat::XgNonPenalty, Direction::HigherBetter, 2.0),
                (PctStat::Xa, Direction::HigherBetter, 1.2),
                (PctStat::ChancesCreated, Direction::HigherBetter, 1.0),
                (PctStat::TouchesInOppBox, Direction::HigherBetter, 0.9),
                (PctStat::ShotsOnTarget, Direction::HigherBetter, 0.7),
                (PctStat::Rating, Direction::HigherBetter, 0.6),
            ][..],
            &[
                (PctStat::PossWonFinalThird, Direction::HigherBetter, 1.0),
                (PctStat::Recoveries, Direction::HigherBetter, 0.6),
                (PctStat::DuelsWonPct, Direction::HigherBetter, 0.4),
                (PctStat::Rating, Direction::HigherBetter, 0.3),
            ][..],
            0.7,
            0.3,
        ),
    };

    let attack = composite_pct_z(p, attack_specs);
    let defense = composite_pct_z(p, defense_specs);

    if attack.is_none() && defense.is_none() {
        return None;
    }

    let denom = (if attack.is_some() { mix_a } else { 0.0 })
        + (if defense.is_some() { mix_d } else { 0.0 });
    if denom <= 0.0 {
        return None;
    }

    let overall = match (attack, defense) {
        (Some(a), Some(d)) => (mix_a * a + mix_d * d) / denom,
        (Some(a), None) => a,
        (None, Some(d)) => d,
        (None, None) => return None,
    };
    Some(clamp(overall, -2.0, 2.0))
}

fn composite_pct_z(p: &PlayerDetail, specs: &[(PctStat, Direction, f64)]) -> Option<f64> {
    const COVERAGE_MIN: f64 = 0.40;
    let mut w_total = 0.0;
    let mut w_used = 0.0;
    let mut sum = 0.0;

    for (stat, dir, w) in specs {
        w_total += *w;
        let Some(pct) = pct_for_stat(p, *stat) else {
            continue;
        };
        let mut z = pct_to_z(pct);
        if matches!(dir, Direction::LowerBetter) {
            z = -z;
        }
        sum += *w * z;
        w_used += *w;
    }

    if w_total <= 0.0 || w_used <= 0.0 {
        return None;
    }
    let coverage = (w_used / w_total).clamp(0.0, 1.0);
    if coverage < COVERAGE_MIN {
        return None;
    }
    Some(sum / w_used)
}

fn pct_to_z(pct: f64) -> f64 {
    ((pct - 50.0) / 15.0).clamp(-3.0, 3.0)
}

fn pct_for_stat(p: &PlayerDetail, stat: PctStat) -> Option<f64> {
    match stat {
        PctStat::Goals => find_stat_pct(p, &["goals"], &["goals conceded"]),
        PctStat::XgNonPenalty => find_stat_pct(
            p,
            &["xg excl. penalty", "xg excl penalty", "xg (excl. penalty)"],
            &[],
        )
        .or_else(|| find_stat_pct(p, &["expected goals", "xg"], &[])),
        PctStat::Xa => find_stat_pct(p, &["expected assists", "xa", "x a"], &[]),
        PctStat::ChancesCreated => find_stat_pct(p, &["chances created"], &[]),
        PctStat::TouchesInOppBox => find_stat_pct(p, &["touches in opposition box"], &[]),
        PctStat::ShotsOnTarget => find_stat_pct(p, &["shots on target"], &[]),
        PctStat::AccuratePasses => find_stat_pct(p, &["accurate passes"], &[]),
        PctStat::PassAccuracy => find_stat_pct(p, &["pass accuracy"], &[]),
        PctStat::AccurateLongBalls => find_stat_pct(p, &["accurate long balls"], &[]),
        PctStat::Tackles => find_stat_pct(p, &["tackles"], &[]),
        PctStat::Interceptions => find_stat_pct(p, &["interceptions"], &[]),
        PctStat::Recoveries => find_stat_pct(p, &["recoveries"], &[]),
        PctStat::DribbledPast => find_stat_pct(p, &["dribbled past"], &[]),
        PctStat::DuelsWonPct => find_stat_pct(p, &["duels won %", "duels won%"], &[]),
        PctStat::AerialsWonPct => find_stat_pct(p, &["aerials won %", "aerials won%"], &[]),
        PctStat::Clearances => find_stat_pct(p, &["clearances"], &[]),
        PctStat::Blocks => find_stat_pct(p, &["blocks"], &[]),
        PctStat::BlockedScoringAttempt => find_stat_pct(p, &["blocked scoring attempt"], &[]),
        PctStat::XgAgainstOnPitch => find_stat_pct(p, &["xg against while on pitch"], &[]),
        PctStat::GoalsConcededOnPitch => find_stat_pct(p, &["goals conceded while on pitch"], &[]),
        PctStat::Saves => find_stat_pct(p, &["saves"], &[]),
        PctStat::SavePct => find_stat_pct(p, &["save percentage", "save%", "save %"], &[]),
        PctStat::GoalsConceded => {
            find_stat_pct(p, &["goals conceded"], &["goals conceded while on pitch"])
        }
        PctStat::CleanSheets => find_stat_pct(p, &["clean sheets"], &[]),
        PctStat::ErrorLedToGoal => find_stat_pct(p, &["error led to goal"], &[]),
        PctStat::HighClaims => find_stat_pct(p, &["high claims"], &[]),
        PctStat::ActedAsSweeper => find_stat_pct(p, &["acted as sweeper"], &[]),
        PctStat::PossWonFinalThird => find_stat_pct(
            p,
            &["possession won final 3rd", "possession won final third"],
            &[],
        ),
        PctStat::Rating => find_stat_pct(p, &["rating"], &[]),
    }
}

#[derive(Debug, Clone, Copy)]
struct StatCandidate<'a> {
    title: &'a str,
    pct_total: Option<f64>,
    pct_per90: Option<f64>,
}

fn find_stat_pct(detail: &PlayerDetail, needles: &[&str], excludes: &[&str]) -> Option<f64> {
    let mut best: Option<(u8, f64)> = None;
    for c in iter_all_stats(detail) {
        if !needles
            .iter()
            .any(|n| contains_ascii_case_insensitive(c.title, n))
        {
            continue;
        }
        if excludes
            .iter()
            .any(|e| contains_ascii_case_insensitive(c.title, e))
        {
            continue;
        }
        if let Some(pct) = c.pct_per90 {
            best = Some((2, pct));
            break;
        }
        if let Some(pct) = c.pct_total {
            match best.as_ref() {
                Some((q, _)) if *q >= 1 => {}
                _ => best = Some((1, pct)),
            }
        }
    }
    best.map(|(_, pct)| pct)
}

fn iter_all_stats<'a>(detail: &'a PlayerDetail) -> impl Iterator<Item = StatCandidate<'a>> + 'a {
    let perf = detail.season_performance.iter().flat_map(|g| {
        g.items.iter().map(|item| StatCandidate {
            title: item.title.as_str(),
            pct_total: item.percentile_rank,
            pct_per90: item.percentile_rank_per90,
        })
    });
    let all_comp = detail.all_competitions.iter().map(|s| StatCandidate {
        title: s.title.as_str(),
        pct_total: s.percentile_rank,
        pct_per90: s.percentile_rank_per90,
    });
    let top = detail.top_stats.iter().map(|s| StatCandidate {
        title: s.title.as_str(),
        pct_total: s.percentile_rank,
        pct_per90: s.percentile_rank_per90,
    });
    let main = detail.main_league.as_ref().into_iter().flat_map(|l| {
        l.stats.iter().map(|s| StatCandidate {
            title: s.title.as_str(),
            pct_total: s.percentile_rank,
            pct_per90: s.percentile_rank_per90,
        })
    });
    let groups = detail.season_groups.iter().flat_map(|g| {
        g.items.iter().map(|s| StatCandidate {
            title: s.title.as_str(),
            pct_total: s.percentile_rank,
            pct_per90: s.percentile_rank_per90,
        })
    });
    perf.chain(all_comp).chain(top).chain(main).chain(groups)
}

fn player_discipline_score(detail: &PlayerDetail) -> Option<f32> {
    if player_detail_is_stub(detail) {
        return None;
    }
    let fouls = find_stat_pct(detail, &["fouls committed"], &[]);
    let yellow = find_stat_pct(detail, &["yellow cards"], &[]);
    let red = find_stat_pct(detail, &["red cards"], &[]);

    let mut sum = 0.0;
    let mut w = 0.0;
    let mut n = 0u8;

    if let Some(v) = fouls {
        sum += 0.50 * v;
        w += 0.50;
        n += 1;
    }
    if let Some(v) = yellow {
        sum += 0.35 * v;
        w += 0.35;
        n += 1;
    }
    if let Some(v) = red {
        sum += 0.15 * v;
        w += 0.15;
        n += 1;
    }

    if n < 2 || w <= 0.0 {
        return None;
    }
    Some((sum / w).clamp(0.0, 100.0) as f32)
}

fn discipline_from_slots(
    slots: &[PlayerSlot],
    players: &HashMap<u32, PlayerDetail>,
) -> Option<(Option<f32>, f32)> {
    let mut sum = 0.0f32;
    let mut used = 0usize;

    for slot in slots {
        let Some(id) = slot.id else {
            continue;
        };
        let Some(p) = players.get(&id) else {
            continue;
        };
        let Some(score) = player_discipline_score(p) else {
            continue;
        };
        sum += score;
        used += 1;
    }

    if used == 0 {
        return None;
    }
    let cov = (used as f32 / 11.0).clamp(0.0, 1.0);
    let score = if used >= 3 {
        Some((sum / used as f32).clamp(0.0, 100.0))
    } else {
        None
    };
    Some((score, cov))
}

fn discipline_from_squad(
    team_id: u32,
    squads: &HashMap<u32, Vec<SquadPlayer>>,
    players: &HashMap<u32, PlayerDetail>,
) -> Option<(Option<f32>, f32)> {
    let squad = squads.get(&team_id)?;
    let mut sum = 0.0f32;
    let mut used = 0usize;
    for sp in squad {
        let Some(p) = players.get(&sp.id) else {
            continue;
        };
        let Some(score) = player_discipline_score(p) else {
            continue;
        };
        sum += score;
        used += 1;
    }
    if used == 0 {
        return None;
    }
    let cov = (used as f32 / 11.0).clamp(0.0, 1.0);
    let score = if used >= 3 {
        Some((sum / used as f32).clamp(0.0, 100.0))
    } else {
        None
    };
    Some((score, cov))
}

fn lineup_strength_and_coverage(
    lineup: &LineupSide,
    players: &HashMap<u32, PlayerDetail>,
) -> Option<(f64, f32)> {
    let mut sum = 0.0;
    let mut cnt = 0usize;

    for slot in &lineup.starting {
        let Some(p) = match_player(slot, players, Some(&lineup.team)) else {
            continue;
        };
        let role = infer_role(slot, p);
        let season_z = player_season_strength_z(p, role);
        let form_z = player_form_z(p, 8);

        let overall_z = match (season_z, form_z) {
            (Some(s), Some(f)) => SEASON_BLEND * s + FORM_BLEND * f,
            (Some(s), None) => s,
            (None, Some(f)) => f,
            (None, None) => continue,
        };
        let overall_z = clamp(overall_z, -2.0, 2.0);
        sum += overall_z / 2.0;
        cnt += 1;
    }

    if cnt >= 3 {
        let strength = clamp(sum / cnt as f64, -1.0, 1.0);
        let coverage = (cnt as f32 / 11.0).clamp(0.0, 1.0);
        Some((strength, coverage))
    } else {
        None
    }
}

fn normalize_player_name(raw: &str) -> String {
    let lowered = raw.trim().to_lowercase();
    let cleaned = lowered
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || c.is_whitespace())
        .collect::<String>();
    let mut parts = cleaned.split_whitespace().collect::<Vec<_>>();
    parts.retain(|p| !matches!(*p, "jr" | "sr" | "ii" | "iii" | "iv"));
    parts.join(" ")
}

fn match_player<'a>(
    slot: &crate::state::PlayerSlot,
    players: &'a HashMap<u32, PlayerDetail>,
    team_hint: Option<&str>,
) -> Option<&'a PlayerDetail> {
    if let Some(id) = slot.id {
        if let Some(p) = players.get(&id) {
            return Some(p);
        }
    }

    let slot_key = normalize_player_name(&slot.name);
    if slot_key.is_empty() {
        return None;
    }

    let team_key = team_hint.map(normalize_team_key);

    let mut exact: Vec<&PlayerDetail> = Vec::new();
    for p in players.values() {
        if normalize_player_name(&p.name) != slot_key {
            continue;
        }
        exact.push(p);
    }
    if exact.is_empty() {
        return None;
    }

    if let Some(team_key) = team_key {
        let mut team_filtered: Vec<&PlayerDetail> = exact
            .iter()
            .copied()
            .filter(|p| {
                p.team
                    .as_deref()
                    .is_some_and(|t| normalize_team_key(t) == team_key)
            })
            .collect();
        if team_filtered.len() == 1 {
            return Some(team_filtered.remove(0));
        }
    }

    if exact.len() == 1 {
        Some(exact[0])
    } else {
        None
    }
}

fn outcome_probs_poisson(
    goals_home: u32,
    goals_away: u32,
    lambda_home_rem: f64,
    lambda_away_rem: f64,
    max_goals: u32,
) -> (f64, f64, f64) {
    let pmf_h = poisson_pmf(lambda_home_rem, max_goals);
    let pmf_a = poisson_pmf(lambda_away_rem, max_goals);

    let mut p_home = 0.0;
    let mut p_draw = 0.0;
    let mut p_away = 0.0;

    for (i, p_i) in pmf_h.iter().enumerate() {
        for (j, p_j) in pmf_a.iter().enumerate() {
            let p = p_i * p_j;
            let fh = goals_home + i as u32;
            let fa = goals_away + j as u32;
            if fh > fa {
                p_home += p;
            } else if fh < fa {
                p_away += p;
            } else {
                p_draw += p;
            }
        }
    }

    // Guard against tiny float drift.
    let sum = p_home + p_draw + p_away;
    if sum > 0.0 {
        (p_home / sum, p_draw / sum, p_away / sum)
    } else {
        (1.0 / 3.0, 1.0 / 3.0, 1.0 / 3.0)
    }
}

fn outcome_probs_poisson_dc(
    goals_home: u32,
    goals_away: u32,
    lambda_home_rem: f64,
    lambda_away_rem: f64,
    max_goals: u32,
    rho: f64,
) -> (f64, f64, f64) {
    let pmf_h = poisson_pmf(lambda_home_rem, max_goals);
    let pmf_a = poisson_pmf(lambda_away_rem, max_goals);

    let mut p_home = 0.0;
    let mut p_draw = 0.0;
    let mut p_away = 0.0;
    let mut total = 0.0;

    for (i, p_i) in pmf_h.iter().enumerate() {
        for (j, p_j) in pmf_a.iter().enumerate() {
            let mut p = p_i * p_j;
            p *= dixon_coles_tau(i as u32, j as u32, lambda_home_rem, lambda_away_rem, rho);
            total += p;

            let fh = goals_home + i as u32;
            let fa = goals_away + j as u32;
            if fh > fa {
                p_home += p;
            } else if fh < fa {
                p_away += p;
            } else {
                p_draw += p;
            }
        }
    }

    if total > 0.0 {
        (p_home / total, p_draw / total, p_away / total)
    } else {
        (1.0 / 3.0, 1.0 / 3.0, 1.0 / 3.0)
    }
}

fn dixon_coles_tau(i: u32, j: u32, lambda_h: f64, lambda_a: f64, rho: f64) -> f64 {
    let raw = match (i, j) {
        (0, 0) => 1.0 - rho * lambda_h * lambda_a,
        (0, 1) => 1.0 + rho * lambda_h,
        (1, 0) => 1.0 + rho * lambda_a,
        (1, 1) => 1.0 - rho,
        _ => 1.0,
    };
    clamp(raw, 0.0, 2.0)
}

fn poisson_pmf(lambda: f64, max_k: u32) -> Vec<f64> {
    let max_k = max_k.max(0) as usize;
    let mut out = vec![0.0; max_k + 1];
    let lambda = lambda.max(0.0);

    let p0 = (-lambda).exp();
    out[0] = p0;
    for k in 1..=max_k {
        out[k] = out[k - 1] * lambda / k as f64;
    }

    let sum: f64 = out.iter().sum();
    if sum < 1.0 {
        out[max_k] += 1.0 - sum;
    }
    out
}

fn clamp(v: f64, lo: f64, hi: f64) -> f64 {
    v.max(lo).min(hi)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{
        MatchLineups, PlayerMatchStat, PlayerSeasonPerformanceGroup, PlayerSeasonPerformanceItem,
        StatRow,
    };

    fn stub_player(id: u32, ratings: &[&str]) -> PlayerDetail {
        PlayerDetail {
            id,
            name: format!("P{id}"),
            team: None,
            position: None,
            age: None,
            country: None,
            height: None,
            preferred_foot: None,
            shirt: None,
            market_value: None,
            contract_end: None,
            birth_date: None,
            status: None,
            injury_info: None,
            international_duty: None,
            positions: Vec::new(),
            all_competitions: Vec::new(),
            all_competitions_season: None,
            main_league: None,
            top_stats: Vec::new(),
            season_groups: Vec::new(),
            season_performance: Vec::new(),
            traits: None,
            recent_matches: ratings
                .iter()
                .enumerate()
                .map(|(i, r)| PlayerMatchStat {
                    opponent: format!("O{i}"),
                    league: "L".to_string(),
                    date: "2024-01-01".to_string(),
                    goals: 0,
                    assists: 0,
                    rating: Some((*r).to_string()),
                })
                .collect(),
            season_breakdown: Vec::new(),
            career_sections: Vec::new(),
            trophies: Vec::new(),
        }
    }

    fn stub_player_with_percentiles(
        id: u32,
        name: &str,
        pct: &[(&str, f64)],
        ratings: &[&str],
    ) -> PlayerDetail {
        let mut p = stub_player(id, ratings);
        p.name = name.to_string();
        p.season_performance = vec![PlayerSeasonPerformanceGroup {
            title: "Stats".to_string(),
            items: pct
                .iter()
                .map(|(title, val)| PlayerSeasonPerformanceItem {
                    title: (*title).to_string(),
                    total: "0".to_string(),
                    per90: Some("0".to_string()),
                    percentile_rank: Some(*val),
                    percentile_rank_per90: Some(*val),
                })
                .collect(),
        }];
        p
    }

    #[test]
    fn parse_stat_cell_handles_percent_and_numbers() {
        assert_eq!(parse_stat_cell("58%").unwrap(), 58.0);
        assert_eq!(parse_stat_cell("1.72").unwrap(), 1.72);
        assert_eq!(parse_stat_cell("14").unwrap(), 14.0);
        assert!(parse_stat_cell("-").is_none());
    }

    #[test]
    fn player_form_rating_ignores_unparseable() {
        let p = stub_player(1, &["7.2", "bad", "6.8"]);
        let r = player_form_rating(&p, 8).unwrap();
        assert!(r > 6.8);
    }

    #[test]
    fn normalize_probabilities_sum_to_100() {
        let summary = MatchSummary {
            id: "m".to_string(),
            league_id: None,
            league_name: "L".to_string(),
            home_team_id: None,
            away_team_id: None,
            home: "H".to_string(),
            away: "A".to_string(),
            minute: 1,
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
            is_live: true,
            market_odds: None,
        };
        let win = compute_win_prob(
            &summary,
            None,
            &HashMap::new(),
            &HashMap::new(),
            &[],
            None,
            None,
        );
        let sum = win.p_home + win.p_draw + win.p_away;
        assert!((sum - 100.0).abs() < 0.01);
    }

    #[test]
    fn big_lead_late_is_overwhelming() {
        let summary = MatchSummary {
            id: "m".to_string(),
            league_id: None,
            league_name: "L".to_string(),
            home_team_id: None,
            away_team_id: None,
            home: "H".to_string(),
            away: "A".to_string(),
            minute: 80,
            score_home: 2,
            score_away: 0,
            win: WinProbRow {
                p_home: 0.0,
                p_draw: 0.0,
                p_away: 0.0,
                delta_home: 0.0,
                quality: ModelQuality::Basic,
                confidence: 0,
            },
            is_live: true,
            market_odds: None,
        };
        let win = compute_win_prob(
            &summary,
            None,
            &HashMap::new(),
            &HashMap::new(),
            &[],
            None,
            None,
        );
        assert!(win.p_home > 95.0);
    }

    #[test]
    fn market_odds_blend_moves_prematch_toward_market_when_enabled() {
        let mut summary = MatchSummary {
            id: "mkt".to_string(),
            league_id: Some(47),
            league_name: "Premier League".to_string(),
            home_team_id: Some(1),
            away_team_id: Some(2),
            home: "LIV".to_string(),
            away: "MCI".to_string(),
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
            market_odds: None,
        };
        let model_only = compute_win_prob(
            &summary,
            None,
            &HashMap::new(),
            &HashMap::new(),
            &[],
            None,
            None,
        );

        summary.market_odds = Some(MarketOddsSnapshot {
            source: "theoddsapi".to_string(),
            fetched_at_unix: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .ok()
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0),
            bookmakers_used: 5,
            home_decimal: Some(1.60),
            draw_decimal: Some(4.20),
            away_decimal: Some(5.20),
            implied_home: Some(68.0),
            implied_draw: Some(18.0),
            implied_away: Some(14.0),
            stale: false,
        });
        let (with_market, extras) = compute_win_prob_explainable(
            &summary,
            None,
            &HashMap::new(),
            &HashMap::new(),
            &[],
            None,
            None,
        );
        let extras = extras.expect("prematch extras");

        if let Some(w_market) = extras.market_weight_used {
            if w_market > 0.0 {
                assert!(with_market.p_home > model_only.p_home);
            }
            assert!(
                extras
                    .explain
                    .signals
                    .iter()
                    .any(|s| s.starts_with("MARKET_BLEND_"))
            );
        } else {
            assert!((with_market.p_home - model_only.p_home).abs() < 0.01);
        }
    }

    #[test]
    fn xg_signal_moves_probabilities() {
        let summary = MatchSummary {
            id: "m".to_string(),
            league_id: None,
            league_name: "L".to_string(),
            home_team_id: None,
            away_team_id: None,
            home: "H".to_string(),
            away: "A".to_string(),
            minute: 45,
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
            is_live: true,
            market_odds: None,
        };

        let detail = MatchDetail {
            home_team: Some("Home".to_string()),
            away_team: Some("Away".to_string()),
            events: Vec::new(),
            commentary: Vec::new(),
            commentary_error: None,
            lineups: Some(MatchLineups {
                sides: vec![
                    LineupSide {
                        team: "Home".to_string(),
                        team_abbr: "HOM".to_string(),
                        formation: "4-3-3".to_string(),
                        starting: vec![PlayerSlot {
                            id: Some(1),
                            name: "P1".to_string(),
                            number: None,
                            pos: None,
                        }],
                        subs: Vec::new(),
                    },
                    LineupSide {
                        team: "Away".to_string(),
                        team_abbr: "AWY".to_string(),
                        formation: "4-3-3".to_string(),
                        starting: vec![PlayerSlot {
                            id: Some(2),
                            name: "P2".to_string(),
                            number: None,
                            pos: None,
                        }],
                        subs: Vec::new(),
                    },
                ],
            }),
            stats: vec![StatRow {
                group: None,
                name: "xG".to_string(),
                home: "1.80".to_string(),
                away: "0.30".to_string(),
            }],
        };

        let mut cache = HashMap::new();
        cache.insert(1, stub_player(1, &["7.2", "7.0", "6.9"]));
        cache.insert(2, stub_player(2, &["6.8", "6.7", "6.6"]));

        let win = compute_win_prob(
            &summary,
            Some(&detail),
            &cache,
            &HashMap::new(),
            &[],
            None,
            None,
        );
        // With heavy xG edge at HT, home should be favored.
        assert!(win.p_home > win.p_away);
    }

    #[test]
    fn season_strength_favors_stronger_lineup_pre_match() {
        let summary = MatchSummary {
            id: "m".to_string(),
            league_id: None,
            league_name: "L".to_string(),
            home_team_id: None,
            away_team_id: None,
            home: "Home".to_string(),
            away: "Away".to_string(),
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
            market_odds: None,
        };

        let lineup_home = LineupSide {
            team: "Home".to_string(),
            team_abbr: "HOM".to_string(),
            formation: "4-3-3".to_string(),
            starting: (1..=7)
                .map(|id| PlayerSlot {
                    id: Some(id),
                    name: format!("H{id}"),
                    number: None,
                    pos: Some("FW".to_string()),
                })
                .collect(),
            subs: Vec::new(),
        };
        let lineup_away = LineupSide {
            team: "Away".to_string(),
            team_abbr: "AWY".to_string(),
            formation: "4-3-3".to_string(),
            starting: (101..=107)
                .map(|id| PlayerSlot {
                    id: Some(id),
                    name: format!("A{id}"),
                    number: None,
                    pos: Some("FW".to_string()),
                })
                .collect(),
            subs: Vec::new(),
        };

        let detail = MatchDetail {
            home_team: Some("Home".to_string()),
            away_team: Some("Away".to_string()),
            events: Vec::new(),
            commentary: Vec::new(),
            commentary_error: None,
            lineups: Some(MatchLineups {
                sides: vec![lineup_home, lineup_away],
            }),
            stats: Vec::new(),
        };

        let home_pct = &[
            ("Goals", 90.0),
            ("xG excl. penalty", 90.0),
            ("xA", 80.0),
            ("Chances created", 80.0),
            ("Touches in opposition box", 85.0),
            ("Shots on target", 80.0),
            ("Rating", 80.0),
        ];
        let away_pct = &[
            ("Goals", 10.0),
            ("xG excl. penalty", 10.0),
            ("xA", 20.0),
            ("Chances created", 20.0),
            ("Touches in opposition box", 15.0),
            ("Shots on target", 20.0),
            ("Rating", 20.0),
        ];

        let mut cache: HashMap<u32, PlayerDetail> = HashMap::new();
        for id in 1..=7 {
            cache.insert(
                id,
                stub_player_with_percentiles(id, &format!("H{id}"), home_pct, &[]),
            );
        }
        for id in 101..=107 {
            cache.insert(
                id,
                stub_player_with_percentiles(id, &format!("A{id}"), away_pct, &[]),
            );
        }

        let win = compute_win_prob(
            &summary,
            Some(&detail),
            &cache,
            &HashMap::new(),
            &[],
            None,
            None,
        );
        assert_eq!(win.quality, ModelQuality::Track);
        assert!(win.p_home > win.p_away);
    }

    #[test]
    fn blend_includes_form_when_season_equal() {
        let summary = MatchSummary {
            id: "m".to_string(),
            league_id: None,
            league_name: "L".to_string(),
            home_team_id: None,
            away_team_id: None,
            home: "Home".to_string(),
            away: "Away".to_string(),
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
            market_odds: None,
        };

        let lineup_home = LineupSide {
            team: "Home".to_string(),
            team_abbr: "HOM".to_string(),
            formation: "4-3-3".to_string(),
            starting: (1..=7)
                .map(|id| PlayerSlot {
                    id: Some(id),
                    name: format!("H{id}"),
                    number: None,
                    pos: Some("FW".to_string()),
                })
                .collect(),
            subs: Vec::new(),
        };
        let lineup_away = LineupSide {
            team: "Away".to_string(),
            team_abbr: "AWY".to_string(),
            formation: "4-3-3".to_string(),
            starting: (101..=107)
                .map(|id| PlayerSlot {
                    id: Some(id),
                    name: format!("A{id}"),
                    number: None,
                    pos: Some("FW".to_string()),
                })
                .collect(),
            subs: Vec::new(),
        };

        let detail = MatchDetail {
            home_team: Some("Home".to_string()),
            away_team: Some("Away".to_string()),
            events: Vec::new(),
            commentary: Vec::new(),
            commentary_error: None,
            lineups: Some(MatchLineups {
                sides: vec![lineup_home, lineup_away],
            }),
            stats: Vec::new(),
        };

        let season_equal = &[
            ("Goals", 50.0),
            ("xG excl. penalty", 50.0),
            ("xA", 50.0),
            ("Chances created", 50.0),
            ("Touches in opposition box", 50.0),
            ("Shots on target", 50.0),
            ("Rating", 50.0),
        ];

        let mut cache: HashMap<u32, PlayerDetail> = HashMap::new();
        for id in 1..=7 {
            cache.insert(
                id,
                stub_player_with_percentiles(
                    id,
                    &format!("H{id}"),
                    season_equal,
                    &["5.6", "5.6", "5.6", "5.6", "5.6", "5.6", "5.6", "5.6"],
                ),
            );
        }
        for id in 101..=107 {
            cache.insert(
                id,
                stub_player_with_percentiles(
                    id,
                    &format!("A{id}"),
                    season_equal,
                    &["8.0", "8.0", "8.0", "8.0", "8.0", "8.0", "8.0", "8.0"],
                ),
            );
        }

        let params = crate::league_params::LeagueParams {
            league_id: 0,
            sample_matches: 0,
            goals_total_base: 2.60,
            home_adv_goals: 0.0,
            dc_rho: -0.10,
            prematch_logit_scale: 1.0,
            prematch_draw_bias: 0.0,
        };
        let win = compute_win_prob(
            &summary,
            Some(&detail),
            &cache,
            &HashMap::new(),
            &[],
            Some(&params),
            None,
        );
        assert_eq!(win.quality, ModelQuality::Track);
        assert!(win.p_away > win.p_home);
    }

    #[test]
    fn insufficient_lineup_strength_falls_back_to_team_analysis() {
        let summary = MatchSummary {
            id: "m".to_string(),
            league_id: None,
            league_name: "L".to_string(),
            home_team_id: None,
            away_team_id: None,
            home: "Home".to_string(),
            away: "Away".to_string(),
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
            market_odds: None,
        };

        let lineup_home = LineupSide {
            team: "Home".to_string(),
            team_abbr: "HOM".to_string(),
            formation: "4-3-3".to_string(),
            starting: (1..=7)
                .map(|id| PlayerSlot {
                    id: Some(id),
                    name: format!("H{id}"),
                    number: None,
                    pos: Some("FW".to_string()),
                })
                .collect(),
            subs: Vec::new(),
        };
        let lineup_away = LineupSide {
            team: "Away".to_string(),
            team_abbr: "AWY".to_string(),
            formation: "4-3-3".to_string(),
            starting: (101..=107)
                .map(|id| PlayerSlot {
                    id: Some(id),
                    name: format!("A{id}"),
                    number: None,
                    pos: Some("FW".to_string()),
                })
                .collect(),
            subs: Vec::new(),
        };

        let detail = MatchDetail {
            home_team: Some("Home".to_string()),
            away_team: Some("Away".to_string()),
            events: Vec::new(),
            commentary: Vec::new(),
            commentary_error: None,
            lineups: Some(MatchLineups {
                sides: vec![lineup_home, lineup_away],
            }),
            stats: Vec::new(),
        };

        // Only 3 players present => lineup_team_strength() should return None.
        let mut cache: HashMap<u32, PlayerDetail> = HashMap::new();
        for id in 1..=3 {
            cache.insert(id, stub_player(id, &[]));
        }
        for id in 101..=103 {
            cache.insert(id, stub_player(id, &[]));
        }

        let analysis = vec![
            TeamAnalysis {
                id: 1,
                name: "Home".to_string(),
                confed: crate::state::Confederation::UEFA,
                host: false,
                fifa_rank: None,
                fifa_points: Some(1800),
                fifa_updated: None,
            },
            TeamAnalysis {
                id: 2,
                name: "Away".to_string(),
                confed: crate::state::Confederation::UEFA,
                host: false,
                fifa_rank: None,
                fifa_points: Some(1400),
                fifa_updated: None,
            },
        ];

        let win_with_lineups = compute_win_prob(
            &summary,
            Some(&detail),
            &cache,
            &HashMap::new(),
            &analysis,
            None,
            None,
        );
        let win_no_lineups = compute_win_prob(
            &summary,
            None,
            &cache,
            &HashMap::new(),
            &analysis,
            None,
            None,
        );

        assert_eq!(win_with_lineups.quality, ModelQuality::Basic);
        assert_eq!(win_no_lineups.quality, ModelQuality::Basic);
        assert!((win_with_lineups.p_home - win_no_lineups.p_home).abs() < 0.001);
        assert!((win_with_lineups.p_draw - win_no_lineups.p_draw).abs() < 0.001);
        assert!((win_with_lineups.p_away - win_no_lineups.p_away).abs() < 0.001);
    }

    #[test]
    fn discipline_history_boosts_opponent_lambda() {
        let summary = MatchSummary {
            id: "m".to_string(),
            league_id: None,
            league_name: "L".to_string(),
            home_team_id: None,
            away_team_id: None,
            home: "Home".to_string(),
            away: "Away".to_string(),
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
            market_odds: None,
        };

        let lineup_home = LineupSide {
            team: "Home".to_string(),
            team_abbr: "HOM".to_string(),
            formation: "4-3-3".to_string(),
            starting: (1..=5)
                .map(|id| PlayerSlot {
                    id: Some(id),
                    name: format!("H{id}"),
                    number: None,
                    pos: None,
                })
                .collect(),
            subs: Vec::new(),
        };
        let lineup_away = LineupSide {
            team: "Away".to_string(),
            team_abbr: "AWY".to_string(),
            formation: "4-3-3".to_string(),
            starting: (101..=105)
                .map(|id| PlayerSlot {
                    id: Some(id),
                    name: format!("A{id}"),
                    number: None,
                    pos: None,
                })
                .collect(),
            subs: Vec::new(),
        };

        let detail = MatchDetail {
            home_team: Some("Home".to_string()),
            away_team: Some("Away".to_string()),
            events: Vec::new(),
            commentary: Vec::new(),
            commentary_error: None,
            lineups: Some(MatchLineups {
                sides: vec![lineup_home, lineup_away],
            }),
            stats: Vec::new(),
        };

        let home_disc = &[
            ("Fouls committed", 90.0),
            ("Yellow cards", 90.0),
            ("Red cards", 90.0),
        ];
        let away_disc = &[
            ("Fouls committed", 10.0),
            ("Yellow cards", 10.0),
            ("Red cards", 10.0),
        ];

        let mut cache: HashMap<u32, PlayerDetail> = HashMap::new();
        for id in 1..=5 {
            cache.insert(
                id,
                stub_player_with_percentiles(id, &format!("H{id}"), home_disc, &[]),
            );
        }
        for id in 101..=105 {
            cache.insert(
                id,
                stub_player_with_percentiles(id, &format!("A{id}"), away_disc, &[]),
            );
        }

        let (win, extras) = compute_win_prob_explainable(
            &summary,
            Some(&detail),
            &cache,
            &HashMap::new(),
            &[],
            None,
            None,
        );
        let extras = extras.expect("prematch extras");
        assert!(win.p_home > 0.0);
        assert!(extras.disc_home.is_some());
        assert!(extras.disc_away.is_some());
        // With home more undisciplined, away lambda should be boosted.
        assert!(extras.disc_mult_away.unwrap_or(1.0) > 1.0);
        assert!(extras.lambda_away_pre > 1.225);
    }

    #[test]
    fn prematch_extras_explainability_is_consistent() {
        let summary = MatchSummary {
            id: "m".to_string(),
            league_id: None,
            league_name: "L".to_string(),
            home_team_id: None,
            away_team_id: None,
            home: "Home".to_string(),
            away: "Away".to_string(),
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
            market_odds: None,
        };

        let analysis = vec![
            TeamAnalysis {
                id: 1,
                name: "Home".to_string(),
                confed: crate::state::Confederation::UEFA,
                host: false,
                fifa_rank: None,
                fifa_points: Some(1800),
                fifa_updated: None,
            },
            TeamAnalysis {
                id: 2,
                name: "Away".to_string(),
                confed: crate::state::Confederation::UEFA,
                host: false,
                fifa_rank: None,
                fifa_points: Some(1500),
                fifa_updated: None,
            },
        ];

        let (win, extras) = compute_win_prob_explainable(
            &summary,
            None,
            &HashMap::new(),
            &HashMap::new(),
            &analysis,
            None,
            None,
        );
        let extras = extras.expect("prematch extras");

        assert!((extras.explain.p_home_final - win.p_home).abs() < 0.01);
        assert!((extras.explain.p_draw_final - win.p_draw).abs() < 0.01);
        assert!((extras.explain.p_away_final - win.p_away).abs() < 0.01);

        let total_shift = extras.explain.p_home_final - extras.explain.p_home_baseline;
        let contrib_sum = extras.explain.pp_home_adv
            + extras.explain.pp_analysis
            + extras.explain.pp_lineup
            + extras.explain.pp_player_impact
            + extras.explain.pp_market_blend;
        assert!((total_shift - contrib_sum).abs() < 0.05);

        // All snapshots should remain normalized.
        let sum_baseline = extras.explain.p_home_baseline
            + extras.explain.p_draw_baseline
            + extras.explain.p_away_baseline;
        let sum_ha = extras.explain.p_home_ha + extras.explain.p_draw_ha + extras.explain.p_away_ha;
        let sum_analysis = extras.explain.p_home_analysis
            + extras.explain.p_draw_analysis
            + extras.explain.p_away_analysis;
        let sum_final =
            extras.explain.p_home_final + extras.explain.p_draw_final + extras.explain.p_away_final;
        assert!((sum_baseline - 100.0).abs() < 0.01);
        assert!((sum_ha - 100.0).abs() < 0.01);
        assert!((sum_analysis - 100.0).abs() < 0.01);
        assert!((sum_final - 100.0).abs() < 0.01);
    }

    #[test]
    fn premier_league_player_impact_uses_squad_fallback() {
        let summary = MatchSummary {
            id: "m".to_string(),
            league_id: Some(crate::pl_dataset::PREMIER_LEAGUE_ID),
            league_name: "Premier League".to_string(),
            home_team_id: Some(1),
            away_team_id: Some(2),
            home: "Home FC".to_string(),
            away: "Away FC".to_string(),
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
            market_odds: None,
        };

        let mut squads: HashMap<u32, Vec<SquadPlayer>> = HashMap::new();
        squads.insert(
            1,
            vec![
                SquadPlayer {
                    id: 11,
                    name: "Player A".to_string(),
                    role: "F".to_string(),
                    club: "Home FC".to_string(),
                    age: None,
                    height: None,
                    shirt_number: None,
                    market_value: None,
                },
                SquadPlayer {
                    id: 12,
                    name: "Player B".to_string(),
                    role: "M".to_string(),
                    club: "Home FC".to_string(),
                    age: None,
                    height: None,
                    shirt_number: None,
                    market_value: None,
                },
            ],
        );
        squads.insert(
            2,
            vec![
                SquadPlayer {
                    id: 21,
                    name: "Player C".to_string(),
                    role: "F".to_string(),
                    club: "Away FC".to_string(),
                    age: None,
                    height: None,
                    shirt_number: None,
                    market_value: None,
                },
                SquadPlayer {
                    id: 22,
                    name: "Player D".to_string(),
                    role: "M".to_string(),
                    club: "Away FC".to_string(),
                    age: None,
                    height: None,
                    shirt_number: None,
                    market_value: None,
                },
            ],
        );

        let (_win, extras) =
            compute_win_prob_explainable(&summary, None, &HashMap::new(), &squads, &[], None, None);
        let extras = extras.expect("prematch extras");
        assert!(extras.s_home_player_impact.is_some());
        assert!(extras.s_away_player_impact.is_some());
        assert!(extras.player_impact_cov_home.is_some());
        assert!(extras.player_impact_cov_away.is_some());
        assert!(
            extras
                .explain
                .signals
                .iter()
                .any(|s| s.starts_with("PLAYER_IMPACT_"))
        );
    }
}
