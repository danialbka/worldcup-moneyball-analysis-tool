use std::collections::HashSet;
use std::path::PathBuf;

use anyhow::{Context, Result, anyhow};

use wc26_terminal::calibration::{self, Outcome, Prob3};
use wc26_terminal::historical_dataset::{self, StoredMatch};
use wc26_terminal::league_params::LeagueParams;
use wc26_terminal::state::{MatchSummary, ModelQuality, WinProbRow};
use wc26_terminal::win_prob;

const DEFAULT_LEAGUE_IDS: &[u32] = &[47, 87, 54, 55, 53, 42, 77];
const DEFAULT_MIN_VAL_GAIN: f64 = 0.0005;
const DEFAULT_CAL_HALF_LIFE_MATCHES: f64 = 1200.0;
const DEFAULT_CAL_SEASON_DECAY: f64 = 0.90;

#[derive(Debug, Clone, Copy)]
struct Config {
    half_life_matches: f64,
    season_decay: f64,
}

#[derive(Debug, Clone)]
struct LeagueReport {
    league_id: u32,
    samples: usize,
    raw: calibration::Metrics,
    cal: calibration::Metrics,
    val_gain: f64,
    val_gain_w: f64,
    ece_raw: f64,
    ece_cal: f64,
    fit_scale: f64,
    fit_draw_bias: f64,
    fitted_goals_total: f64,
    fitted_home_adv: f64,
    fitted_rho: f64,
}

fn main() -> Result<()> {
    let league_ids = parse_league_ids_arg().unwrap_or_else(default_league_ids_from_env);
    if league_ids.is_empty() {
        return Err(anyhow!("no league ids resolved"));
    }

    let db_path = parse_db_path_arg()
        .or_else(|| std::env::var("HIST_DB_PATH").ok().map(PathBuf::from))
        .or_else(historical_dataset::default_db_path)
        .context("unable to resolve sqlite path")?;

    let apply = has_flag("--apply");
    let force_apply = has_flag("--force-apply");
    let min_val_gain = parse_f64_arg("--min-val-gain")
        .unwrap_or(DEFAULT_MIN_VAL_GAIN)
        .clamp(0.0, 0.05);
    let config = Config {
        half_life_matches: parse_f64_arg("--cal-half-life-matches")
            .unwrap_or(DEFAULT_CAL_HALF_LIFE_MATCHES)
            .clamp(200.0, 4000.0),
        season_decay: parse_f64_arg("--cal-season-decay")
            .unwrap_or(DEFAULT_CAL_SEASON_DECAY)
            .clamp(0.50, 1.00),
    };

    let conn = historical_dataset::open_db(&db_path)?;

    let mut reports = Vec::new();
    let mut missing = Vec::new();

    for league_id in &league_ids {
        let rows = historical_dataset::load_finished_matches(&conn, *league_id)?;
        if rows.is_empty() {
            missing.push(*league_id);
            continue;
        }

        let outcomes: Vec<Outcome> = rows
            .iter()
            .filter_map(|m| {
                let (Some(h), Some(a)) = (m.home_goals, m.away_goals) else {
                    return None;
                };
                Some(calibration::classify_outcome(h, a))
            })
            .collect();
        if outcomes.len() != rows.len() || outcomes.len() < 8 {
            missing.push(*league_id);
            continue;
        }

        let walk_raw = walk_forward_predictions(*league_id, &rows);
        let weights =
            build_recency_season_weights(&rows, config.half_life_matches, config.season_decay);
        let split_idx = train_split_index(outcomes.len());

        let (fit_scale, fit_draw_bias, _) = calibration::fit_logit_calibration_weighted(
            &walk_raw[..split_idx],
            &outcomes[..split_idx],
            &weights[..split_idx],
        );

        let walk_cal = walk_raw
            .iter()
            .copied()
            .map(|p| calibration::apply_logit_calibration(p, fit_scale, fit_draw_bias))
            .collect::<Vec<_>>();

        let raw_metrics = calibration::evaluate_probs(&walk_raw, &outcomes);
        let cal_metrics = calibration::evaluate_probs(&walk_cal, &outcomes);

        let val_raw = calibration::evaluate_probs(&walk_raw[split_idx..], &outcomes[split_idx..]);
        let val_cal = calibration::evaluate_probs(&walk_cal[split_idx..], &outcomes[split_idx..]);
        let val_raw_w = calibration::evaluate_probs_weighted(
            &walk_raw[split_idx..],
            &outcomes[split_idx..],
            &weights[split_idx..],
        );
        let val_cal_w = calibration::evaluate_probs_weighted(
            &walk_cal[split_idx..],
            &outcomes[split_idx..],
            &weights[split_idx..],
        );

        let (goals_total, home_adv, draw_rate) = weighted_params_from_rows(&rows, &weights);
        let fitted_rho = calibration::fit_dc_rho_to_draw_rate(goals_total, home_adv, draw_rate);

        reports.push(LeagueReport {
            league_id: *league_id,
            samples: outcomes.len(),
            raw: raw_metrics,
            cal: cal_metrics,
            val_gain: val_raw.log_loss - val_cal.log_loss,
            val_gain_w: val_raw_w.log_loss - val_cal_w.log_loss,
            ece_raw: ece_1x2(&walk_raw, &outcomes, 10),
            ece_cal: ece_1x2(&walk_cal, &outcomes, 10),
            fit_scale,
            fit_draw_bias,
            fitted_goals_total: goals_total,
            fitted_home_adv: home_adv,
            fitted_rho,
        });
    }

    reports.sort_by_key(|r| r.league_id);

    println!("Multi-league pre-match backtest");
    println!("DB: {}", db_path.display());
    println!("Leagues: {:?}", league_ids);
    if !missing.is_empty() {
        println!("Skipped (insufficient data): {:?}", missing);
    }
    println!();

    if reports.is_empty() {
        return Err(anyhow!("no league had enough samples to evaluate"));
    }

    for r in &reports {
        println!(
            "league {} samples={} ll_raw={:.4} ll_cal={:.4} brier_raw={:.4} brier_cal={:.4} ece_raw={:.4} ece_cal={:.4} val_gain={:+.6} val_gain_w={:+.6} fit=({:.2},{:+.2})",
            r.league_id,
            r.samples,
            r.raw.log_loss,
            r.cal.log_loss,
            r.raw.brier,
            r.cal.brier,
            r.ece_raw,
            r.ece_cal,
            r.val_gain,
            r.val_gain_w,
            r.fit_scale,
            r.fit_draw_bias
        );
    }

    let total_samples: usize = reports.iter().map(|r| r.samples).sum();
    let weighted_ll_raw = weighted_mean(&reports, total_samples, |r| r.raw.log_loss);
    let weighted_ll_cal = weighted_mean(&reports, total_samples, |r| r.cal.log_loss);
    let weighted_brier_raw = weighted_mean(&reports, total_samples, |r| r.raw.brier);
    let weighted_brier_cal = weighted_mean(&reports, total_samples, |r| r.cal.brier);
    let weighted_ece_raw = weighted_mean(&reports, total_samples, |r| r.ece_raw);
    let weighted_ece_cal = weighted_mean(&reports, total_samples, |r| r.ece_cal);

    println!();
    println!(
        "aggregate samples={} ll_raw={:.4} ll_cal={:.4} brier_raw={:.4} brier_cal={:.4} ece_raw={:.4} ece_cal={:.4}",
        total_samples,
        weighted_ll_raw,
        weighted_ll_cal,
        weighted_brier_raw,
        weighted_brier_cal,
        weighted_ece_raw,
        weighted_ece_cal
    );

    if apply {
        apply_reports(&reports, min_val_gain, force_apply)?;
    }

    Ok(())
}

fn walk_forward_predictions(league_id: u32, rows: &[StoredMatch]) -> Vec<Prob3> {
    let mut history_count = 0usize;
    let mut history_draws = 0usize;
    let mut history_total_goals = 0.0_f64;
    let mut history_home_minus_away = 0.0_f64;
    let mut predictions: Vec<Prob3> = Vec::with_capacity(rows.len());

    for m in rows {
        let params = cumulative_params(
            league_id,
            history_count,
            history_draws,
            history_total_goals,
            history_home_minus_away,
        );
        let summary = MatchSummary {
            id: m.match_id.to_string(),
            league_id: Some(m.league_id),
            league_name: format!("League {}", m.league_id),
            home_team_id: Some(m.home_team_id),
            away_team_id: Some(m.away_team_id),
            home: m.home_team.clone(),
            away: m.away_team.clone(),
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

        let win = win_prob::compute_win_prob(
            &summary,
            None,
            &std::collections::HashMap::new(),
            &std::collections::HashMap::new(),
            &[],
            Some(&params),
            None,
        );
        predictions.push(Prob3 {
            home: (win.p_home as f64 / 100.0).clamp(0.0, 1.0),
            draw: (win.p_draw as f64 / 100.0).clamp(0.0, 1.0),
            away: (win.p_away as f64 / 100.0).clamp(0.0, 1.0),
        });

        if let (Some(home_goals), Some(away_goals)) = (m.home_goals, m.away_goals) {
            history_total_goals += (home_goals as f64) + (away_goals as f64);
            history_home_minus_away += (home_goals as f64) - (away_goals as f64);
            if home_goals == away_goals {
                history_draws += 1;
            }
            history_count += 1;
        }
    }

    predictions
}

fn cumulative_params(
    league_id: u32,
    sample_matches: usize,
    draw_count: usize,
    total_goals: f64,
    home_minus_away: f64,
) -> LeagueParams {
    let mut goals_total_base = 2.60;
    let mut home_adv_goals = 0.0;
    if sample_matches > 0 {
        goals_total_base = total_goals / sample_matches as f64;
        home_adv_goals = home_minus_away / sample_matches as f64;
    }

    const MIN_N: f64 = 200.0;
    let w = (sample_matches as f64 / MIN_N).clamp(0.0, 1.0);
    goals_total_base = (1.0 - w) * 2.60 + w * goals_total_base;
    home_adv_goals *= w;

    let mut dc_rho = -0.10;
    if sample_matches > 0 {
        let draw_rate = draw_count as f64 / sample_matches as f64;
        dc_rho = calibration::fit_dc_rho_to_draw_rate(goals_total_base, home_adv_goals, draw_rate);
    }
    dc_rho = ((1.0 - w) * -0.10) + (w * dc_rho);

    LeagueParams {
        league_id,
        sample_matches,
        goals_total_base,
        home_adv_goals,
        dc_rho,
        prematch_logit_scale: 1.0,
        prematch_draw_bias: 0.0,
    }
}

fn build_recency_season_weights(
    rows: &[StoredMatch],
    half_life_matches: f64,
    season_decay: f64,
) -> Vec<f64> {
    if rows.is_empty() {
        return Vec::new();
    }
    let latest_season = rows.iter().map(season_key).max().unwrap_or(0);
    let last_idx = rows.len().saturating_sub(1);
    let mut out = Vec::with_capacity(rows.len());
    for (idx, row) in rows.iter().enumerate() {
        let age = (last_idx.saturating_sub(idx)) as f64;
        let recency = (-(std::f64::consts::LN_2 * age / half_life_matches.max(1.0))).exp();
        let delta_season = latest_season.saturating_sub(season_key(row));
        let season_w = season_decay.powi(delta_season);
        out.push((recency * season_w).clamp(0.05, 1.0));
    }
    out
}

fn season_key(row: &StoredMatch) -> i32 {
    parse_leading_year(&row.season)
        .unwrap_or_else(|| parse_leading_year(&row.utc_time).unwrap_or(0))
}

fn parse_leading_year(raw: &str) -> Option<i32> {
    let mut buf = String::new();
    for ch in raw.chars() {
        if ch.is_ascii_digit() {
            buf.push(ch);
            if buf.len() == 4 {
                return buf.parse::<i32>().ok();
            }
        } else if !buf.is_empty() {
            break;
        }
    }
    None
}

fn weighted_params_from_rows(rows: &[StoredMatch], weights: &[f64]) -> (f64, f64, f64) {
    if rows.is_empty() || rows.len() != weights.len() {
        return (2.60, 0.0, 0.25);
    }
    let mut sw = 0.0;
    let mut goals = 0.0;
    let mut diff = 0.0;
    let mut draws = 0.0;

    for (row, w_raw) in rows.iter().zip(weights) {
        let w = (*w_raw).max(1e-9);
        let (Some(h), Some(a)) = (row.home_goals, row.away_goals) else {
            continue;
        };
        sw += w;
        goals += w * ((h as f64) + (a as f64));
        diff += w * ((h as f64) - (a as f64));
        if h == a {
            draws += w;
        }
    }

    if sw <= 0.0 {
        return (2.60, 0.0, 0.25);
    }

    (goals / sw, diff / sw, (draws / sw).clamp(0.05, 0.60))
}

fn ece_1x2(preds: &[Prob3], outcomes: &[Outcome], bins: usize) -> f64 {
    if preds.is_empty() || preds.len() != outcomes.len() || bins == 0 {
        return 0.0;
    }
    let mut sum = 0.0;
    let n = preds.len() as f64;

    for c in [Outcome::Home, Outcome::Draw, Outcome::Away] {
        let rows = calibration::calibration_bins(preds, outcomes, c, bins);
        for b in rows {
            if b.count == 0 {
                continue;
            }
            let w = b.count as f64 / n;
            sum += w * (b.avg_pred - b.actual_rate).abs();
        }
    }

    sum / 3.0
}

fn weighted_mean(
    reports: &[LeagueReport],
    total_samples: usize,
    value: impl Fn(&LeagueReport) -> f64,
) -> f64 {
    if total_samples == 0 {
        return 0.0;
    }
    reports
        .iter()
        .map(|r| value(r) * r.samples as f64)
        .sum::<f64>()
        / total_samples as f64
}

fn apply_reports(reports: &[LeagueReport], min_val_gain: f64, force_apply: bool) -> Result<()> {
    if reports.is_empty() {
        return Ok(());
    }

    let mut params = wc26_terminal::league_params::load_cached_params();

    for r in reports {
        let gate_ok = r.val_gain >= min_val_gain && r.val_gain_w >= min_val_gain;
        if !gate_ok && !force_apply {
            return Err(anyhow!(
                "league {} failed validation gate (val_gain={:+.6}, val_gain_w={:+.6}); pass --force-apply to override",
                r.league_id,
                r.val_gain,
                r.val_gain_w
            ));
        }

        params.insert(
            r.league_id,
            LeagueParams {
                league_id: r.league_id,
                sample_matches: r.samples,
                goals_total_base: r.fitted_goals_total,
                home_adv_goals: r.fitted_home_adv,
                dc_rho: r.fitted_rho,
                prematch_logit_scale: r.fit_scale,
                prematch_draw_bias: r.fit_draw_bias,
            },
        );
    }

    wc26_terminal::league_params::save_cached_params(&params)?;
    println!("Applied fitted params for {} leagues", reports.len());
    Ok(())
}

fn train_split_index(n: usize) -> usize {
    if n <= 2 {
        return 1;
    }
    let idx = ((n as f64) * 0.85).round() as usize;
    idx.clamp(1, n - 1)
}

fn parse_db_path_arg() -> Option<PathBuf> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    for (idx, arg) in args.iter().enumerate() {
        if let Some(path) = arg.strip_prefix("--db=") {
            let trimmed = path.trim();
            if !trimmed.is_empty() {
                return Some(PathBuf::from(trimmed));
            }
        }
        if arg == "--db" {
            let Some(next) = args.get(idx + 1) else {
                continue;
            };
            if !next.trim().is_empty() {
                return Some(PathBuf::from(next));
            }
        }
    }
    None
}

fn parse_f64_arg(name: &str) -> Option<f64> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    for (idx, arg) in args.iter().enumerate() {
        if let Some(raw) = arg.strip_prefix(&format!("{name}="))
            && let Ok(v) = raw.trim().parse::<f64>()
        {
            return Some(v);
        }
        if arg == name
            && let Some(next) = args.get(idx + 1)
            && let Ok(v) = next.trim().parse::<f64>()
        {
            return Some(v);
        }
    }
    None
}

fn has_flag(name: &str) -> bool {
    std::env::args().skip(1).any(|arg| arg == name)
}

fn parse_league_ids_arg() -> Option<Vec<u32>> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    for (idx, arg) in args.iter().enumerate() {
        if let Some(raw) = arg.strip_prefix("--league-ids=") {
            let ids = parse_ids(raw);
            if !ids.is_empty() {
                return Some(ids);
            }
        }
        if arg == "--league-ids"
            && let Some(next) = args.get(idx + 1)
        {
            let ids = parse_ids(next);
            if !ids.is_empty() {
                return Some(ids);
            }
        }
    }
    None
}

fn default_league_ids_from_env() -> Vec<u32> {
    let mut out = Vec::new();
    extend_ids_env_or_default(&mut out, "APP_LEAGUE_PREMIER_IDS", &[47]);
    extend_ids_env_or_default(&mut out, "APP_LEAGUE_LALIGA_IDS", &[87]);
    extend_ids_env_or_default(&mut out, "APP_LEAGUE_BUNDESLIGA_IDS", &[54]);
    extend_ids_env_or_default(&mut out, "APP_LEAGUE_SERIE_A_IDS", &[55]);
    extend_ids_env_or_default(&mut out, "APP_LEAGUE_LIGUE1_IDS", &[53]);
    extend_ids_env_or_default(&mut out, "APP_LEAGUE_CHAMPIONS_LEAGUE_IDS", &[42]);
    extend_ids_env_or_default(&mut out, "APP_LEAGUE_WORLDCUP_IDS", &[77]);
    if out.is_empty() {
        out.extend(DEFAULT_LEAGUE_IDS);
    }
    dedup_ids(out)
}

fn extend_ids_env_or_default(out: &mut Vec<u32>, key: &str, defaults: &[u32]) {
    match std::env::var(key) {
        Ok(raw) => {
            if raw.trim().is_empty() {
                return;
            }
            out.extend(parse_ids(&raw));
        }
        Err(_) => out.extend(defaults.iter().copied()),
    }
}

fn parse_ids(raw: &str) -> Vec<u32> {
    let ids = raw
        .split([',', ';', ' '])
        .filter_map(|part| part.trim().parse::<u32>().ok())
        .filter(|id| *id != 0)
        .collect::<Vec<_>>();
    dedup_ids(ids)
}

fn dedup_ids(ids: Vec<u32>) -> Vec<u32> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for id in ids {
        if seen.insert(id) {
            out.push(id);
        }
    }
    out
}
