use std::cmp::Ordering;
use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::{Context, Result, anyhow};

use wc26_terminal::calibration::{self, Outcome, Prob3};
use wc26_terminal::league_params::LeagueParams;
use wc26_terminal::pl_dataset::{self, PREMIER_LEAGUE_ID, StoredMatch};
use wc26_terminal::state::{MatchSummary, ModelQuality, WinProbRow};
use wc26_terminal::win_prob;

const DEFAULT_MIN_VAL_GAIN: f64 = 0.0005;
const DEFAULT_CAL_HALF_LIFE_MATCHES: f64 = 1200.0;
const DEFAULT_CAL_SEASON_DECAY: f64 = 0.90;

const DEFAULT_SWEEP_HALF_LIVES: [f64; 6] = [600.0, 900.0, 1200.0, 1600.0, 2400.0, 3200.0];
const DEFAULT_SWEEP_SEASON_DECAYS: [f64; 4] = [0.85, 0.90, 0.95, 1.00];
const DEFAULT_SWEEP_TOP: usize = 10;

#[derive(Debug, Clone, Copy)]
struct Config {
    half_life_matches: f64,
    season_decay: f64,
}

#[derive(Debug, Clone)]
struct EvalSummary {
    config: Config,
    split_idx: usize,
    fit_scale: f64,
    fit_draw_bias: f64,
    walk_metrics: calibration::Metrics,
    walk_metrics_w: calibration::Metrics,
    train_cal_metrics: calibration::Metrics,
    train_cal_metrics_w: calibration::Metrics,
    val_raw_metrics: calibration::Metrics,
    val_cal_metrics: calibration::Metrics,
    val_raw_metrics_w: calibration::Metrics,
    val_cal_metrics_w: calibration::Metrics,
    val_gain: f64,
    val_gain_w: f64,
    full_vs_empirical: f64,
    gate_passed: bool,
    fitted_goals_total: f64,
    fitted_home_adv: f64,
    fitted_draw_rate: f64,
    fitted_rho: f64,
    fitted_scale: f64,
    fitted_draw_bias: f64,
    full_fit_w: calibration::Metrics,
}

fn main() -> Result<()> {
    let db_path = parse_db_path_arg()
        .or_else(pl_dataset::default_db_path)
        .context("unable to resolve sqlite path")?;

    let apply = has_flag("--apply");
    let force_apply = has_flag("--force-apply");
    let sweep = has_flag("--sweep");

    let min_val_gain = parse_f64_arg("--min-val-gain")
        .unwrap_or(DEFAULT_MIN_VAL_GAIN)
        .clamp(0.0, 0.05);

    let default_config = Config {
        half_life_matches: parse_f64_arg("--cal-half-life-matches")
            .unwrap_or(DEFAULT_CAL_HALF_LIFE_MATCHES)
            .clamp(200.0, 4000.0),
        season_decay: parse_f64_arg("--cal-season-decay")
            .unwrap_or(DEFAULT_CAL_SEASON_DECAY)
            .clamp(0.50, 1.00),
    };

    let sweep_half_lives = parse_f64_list_arg("--sweep-half-lives")
        .unwrap_or_else(|| DEFAULT_SWEEP_HALF_LIVES.to_vec())
        .into_iter()
        .map(|v| v.clamp(200.0, 4000.0))
        .collect::<Vec<_>>();
    let sweep_season_decays = parse_f64_list_arg("--sweep-season-decays")
        .unwrap_or_else(|| DEFAULT_SWEEP_SEASON_DECAYS.to_vec())
        .into_iter()
        .map(|v| v.clamp(0.50, 1.00))
        .collect::<Vec<_>>();
    let sweep_top = parse_usize_arg("--sweep-top")
        .unwrap_or(DEFAULT_SWEEP_TOP)
        .clamp(1, 100);

    let conn = pl_dataset::open_db(&db_path)?;

    let all = pl_dataset::load_finished_premier_league_matches(&conn)?;
    let rows: Vec<StoredMatch> = all
        .into_iter()
        .filter(|m| !m.is_penalty_decided())
        .collect();
    if rows.is_empty() {
        return Err(anyhow!(
            "no finished Premier League rows found in {}",
            db_path.display()
        ));
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

    if rows.len() != outcomes.len() || rows.is_empty() {
        return Err(anyhow!(
            "unable to build consistent fixtures/outcomes for backtest"
        ));
    }

    let walk_forward_raw = walk_forward_predictions(&rows);
    let walk_metrics_raw = calibration::evaluate_probs(&walk_forward_raw, &outcomes);

    let uniform = vec![Prob3::uniform(); outcomes.len()];
    let uniform_metrics = calibration::evaluate_probs(&uniform, &outcomes);

    let empirical_prob = calibration::empirical_outcome_probs(&outcomes);
    let empirical = vec![empirical_prob; outcomes.len()];
    let empirical_metrics = calibration::evaluate_probs(&empirical, &outcomes);

    let (selected_config, sweep_ranked) = if sweep {
        let mut ranked = Vec::new();
        for hl in &sweep_half_lives {
            for sd in &sweep_season_decays {
                let cfg = Config {
                    half_life_matches: *hl,
                    season_decay: *sd,
                };
                let (summary, _) = evaluate_config(
                    &rows,
                    &outcomes,
                    &walk_forward_raw,
                    empirical_metrics.log_loss,
                    min_val_gain,
                    cfg,
                    false,
                );
                ranked.push(summary);
            }
        }
        ranked.sort_by(compare_eval_summaries);

        let chosen = ranked
            .iter()
            .find(|s| s.gate_passed)
            .map(|s| s.config)
            .unwrap_or_else(|| ranked[0].config);

        (chosen, Some(ranked))
    } else {
        (default_config, None)
    };

    let (selected, walk_forward_opt) = evaluate_config(
        &rows,
        &outcomes,
        &walk_forward_raw,
        empirical_metrics.log_loss,
        min_val_gain,
        selected_config,
        true,
    );
    let walk_forward = walk_forward_opt.unwrap_or_default();

    println!("Premier League pre-match backtest");
    println!("DB: {}", db_path.display());
    println!("Samples: {}", outcomes.len());
    if let (Some(first), Some(last)) = (rows.first(), rows.last()) {
        println!("Range UTC: {} -> {}", first.utc_time, last.utc_time);
    }
    println!();

    print_metrics("Walk-forward raw model", walk_metrics_raw);
    print_metrics("Walk-forward calibrated model", selected.walk_metrics);
    print_metrics(
        "Walk-forward calibrated model (weighted)",
        selected.walk_metrics_w,
    );
    print_metrics("Uniform baseline", uniform_metrics);
    print_metrics("Empirical baseline", empirical_metrics);

    if let Some(ranked) = &sweep_ranked {
        println!();
        println!(
            "Sweep results: combos={} (top {} by gate/pass then validation gains)",
            ranked.len(),
            sweep_top.min(ranked.len())
        );
        for s in ranked.iter().take(sweep_top) {
            println!(
                "  hl={:>4.0} sd={:.2} gate={} val={:+.6} val_w={:+.6} wf_ll={:.4} wf_w_ll={:.4} fit=({:.2},{:+.2})",
                s.config.half_life_matches,
                s.config.season_decay,
                if s.gate_passed { "PASS" } else { "FAIL" },
                s.val_gain,
                s.val_gain_w,
                s.walk_metrics.log_loss,
                s.walk_metrics_w.log_loss,
                s.fit_scale,
                s.fit_draw_bias,
            );
        }
        println!(
            "Selected config: hl={:.0} sd={:.2}{}",
            selected.config.half_life_matches,
            selected.config.season_decay,
            if selected.gate_passed {
                " (best passing)"
            } else {
                " (no passing config found)"
            }
        );
    }

    println!();
    let train_len = selected.split_idx;
    let val_len = outcomes.len().saturating_sub(selected.split_idx);
    println!(
        "Calibration split train={} val={} fit_scale={:.2} fit_draw_bias={:+.2} half_life_matches={:.0} season_decay={:.2}",
        train_len,
        val_len,
        selected.fit_scale,
        selected.fit_draw_bias,
        selected.config.half_life_matches,
        selected.config.season_decay
    );
    print_metrics("Train calibrated", selected.train_cal_metrics);
    print_metrics("Train calibrated (weighted)", selected.train_cal_metrics_w);
    print_metrics("Val raw", selected.val_raw_metrics);
    print_metrics("Val calibrated", selected.val_cal_metrics);
    print_metrics("Val raw (weighted)", selected.val_raw_metrics_w);
    print_metrics("Val calibrated (weighted)", selected.val_cal_metrics_w);
    println!(
        "Validation gains: unweighted={:+.6} weighted={:+.6} required={:.6}",
        selected.val_gain, selected.val_gain_w, min_val_gain
    );
    println!(
        "Gate status: {} (needs val improvements and calibrated full-history <= empirical baseline)",
        if selected.gate_passed { "PASS" } else { "FAIL" }
    );

    println!();
    println!(
        "Empirical outcome probs: H={:.3} D={:.3} A={:.3}",
        empirical_prob.home, empirical_prob.draw, empirical_prob.away
    );
    println!(
        "Fitted params (weighted PL dataset): goals_total={:.3} home_adv_goals={:+.3} draw_rate={:.3} dc_rho={:+.2} scale={:.2} draw_bias={:+.2}",
        selected.fitted_goals_total,
        selected.fitted_home_adv,
        selected.fitted_draw_rate,
        selected.fitted_rho,
        selected.fitted_scale,
        selected.fitted_draw_bias,
    );
    print_metrics("Full-fit weighted quality", selected.full_fit_w);

    if apply {
        if !selected.gate_passed && !force_apply {
            return Err(anyhow!(
                "refused to apply params: validation gate failed (use --force-apply to override)"
            ));
        }
        apply_fitted_params(
            selected.fitted_goals_total,
            selected.fitted_home_adv,
            selected.fitted_rho,
            selected.fitted_scale,
            selected.fitted_draw_bias,
            rows.len(),
        )?;
    }

    println!();
    println!("Home-win calibration bins:");
    for bin in calibration::calibration_bins(&walk_forward, &outcomes, Outcome::Home, 10) {
        if bin.count == 0 {
            continue;
        }
        println!(
            "  [{:.1},{:.1}) n={:<4} pred={:.3} actual={:.3}",
            bin.bucket_start, bin.bucket_end, bin.count, bin.avg_pred, bin.actual_rate
        );
    }

    Ok(())
}

fn evaluate_config(
    rows: &[StoredMatch],
    outcomes: &[Outcome],
    walk_forward_raw: &[Prob3],
    empirical_log_loss: f64,
    min_val_gain: f64,
    config: Config,
    keep_probs: bool,
) -> (EvalSummary, Option<Vec<Prob3>>) {
    let sample_weights =
        build_recency_season_weights(rows, config.half_life_matches, config.season_decay);

    let split_idx = train_split_index(outcomes.len());
    let (train_preds, val_preds) = walk_forward_raw.split_at(split_idx);
    let (train_outcomes, val_outcomes) = outcomes.split_at(split_idx);
    let (train_weights, val_weights) = sample_weights.split_at(split_idx);

    let (fit_scale, fit_draw_bias, train_cal_metrics_w) =
        calibration::fit_logit_calibration_weighted(train_preds, train_outcomes, train_weights);

    let walk_forward: Vec<Prob3> = walk_forward_raw
        .iter()
        .copied()
        .map(|p| calibration::apply_logit_calibration(p, fit_scale, fit_draw_bias))
        .collect();

    let walk_metrics = calibration::evaluate_probs(&walk_forward, outcomes);
    let walk_metrics_w =
        calibration::evaluate_probs_weighted(&walk_forward, outcomes, &sample_weights);

    let val_raw_metrics = calibration::evaluate_probs(val_preds, val_outcomes);
    let val_cal_metrics = calibration::evaluate_probs(&walk_forward[split_idx..], val_outcomes);
    let val_raw_metrics_w =
        calibration::evaluate_probs_weighted(val_preds, val_outcomes, val_weights);
    let val_cal_metrics_w =
        calibration::evaluate_probs_weighted(&walk_forward[split_idx..], val_outcomes, val_weights);

    let train_cal_metrics = calibration::evaluate_probs(&walk_forward[..split_idx], train_outcomes);

    let (fitted_goals_total, fitted_home_adv, fitted_draw_rate) =
        weighted_params_from_rows(rows, &sample_weights);
    let fitted_rho =
        calibration::fit_dc_rho_to_draw_rate(fitted_goals_total, fitted_home_adv, fitted_draw_rate);
    let full_base =
        calibration::outcome_probs_from_params(fitted_goals_total, fitted_home_adv, fitted_rho);
    let full_preds = vec![full_base; outcomes.len()];
    let (fitted_scale, fitted_draw_bias, full_fit_w) =
        calibration::fit_logit_calibration_weighted(&full_preds, outcomes, &sample_weights);

    let val_gain = val_raw_metrics.log_loss - val_cal_metrics.log_loss;
    let val_gain_w = val_raw_metrics_w.log_loss - val_cal_metrics_w.log_loss;
    let full_vs_empirical = empirical_log_loss - walk_metrics.log_loss;
    let gate_passed =
        val_gain >= min_val_gain && val_gain_w >= min_val_gain && full_vs_empirical >= 0.0;

    let summary = EvalSummary {
        config,
        split_idx,
        fit_scale,
        fit_draw_bias,
        walk_metrics,
        walk_metrics_w,
        train_cal_metrics,
        train_cal_metrics_w,
        val_raw_metrics,
        val_cal_metrics,
        val_raw_metrics_w,
        val_cal_metrics_w,
        val_gain,
        val_gain_w,
        full_vs_empirical,
        gate_passed,
        fitted_goals_total,
        fitted_home_adv,
        fitted_draw_rate,
        fitted_rho,
        fitted_scale,
        fitted_draw_bias,
        full_fit_w,
    };

    if keep_probs {
        (summary, Some(walk_forward))
    } else {
        (summary, None)
    }
}

fn compare_eval_summaries(a: &EvalSummary, b: &EvalSummary) -> Ordering {
    b.gate_passed
        .cmp(&a.gate_passed)
        .then_with(|| {
            b.val_gain_w
                .partial_cmp(&a.val_gain_w)
                .unwrap_or(Ordering::Equal)
        })
        .then_with(|| {
            b.val_gain
                .partial_cmp(&a.val_gain)
                .unwrap_or(Ordering::Equal)
        })
        .then_with(|| {
            a.walk_metrics
                .log_loss
                .partial_cmp(&b.walk_metrics.log_loss)
                .unwrap_or(Ordering::Equal)
        })
        .then_with(|| {
            b.full_vs_empirical
                .partial_cmp(&a.full_vs_empirical)
                .unwrap_or(Ordering::Equal)
        })
}

fn walk_forward_predictions(rows: &[StoredMatch]) -> Vec<Prob3> {
    let mut history_count = 0usize;
    let mut history_draws = 0usize;
    let mut history_total_goals = 0.0_f64;
    let mut history_home_minus_away = 0.0_f64;
    let mut predictions: Vec<Prob3> = Vec::with_capacity(rows.len());

    for m in rows {
        let params = cumulative_params(
            history_count,
            history_draws,
            history_total_goals,
            history_home_minus_away,
        );
        let summary = MatchSummary {
            id: m.match_id.to_string(),
            league_id: Some(m.league_id),
            league_name: "Premier League".to_string(),
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
            &HashMap::new(),
            &HashMap::new(),
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
        league_id: PREMIER_LEAGUE_ID,
        sample_matches,
        goals_total_base,
        home_adv_goals,
        dc_rho,
        prematch_logit_scale: 1.0,
        prematch_draw_bias: 0.0,
    }
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

fn parse_usize_arg(name: &str) -> Option<usize> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    for (idx, arg) in args.iter().enumerate() {
        if let Some(raw) = arg.strip_prefix(&format!("{name}="))
            && let Ok(v) = raw.trim().parse::<usize>()
        {
            return Some(v);
        }
        if arg == name
            && let Some(next) = args.get(idx + 1)
            && let Ok(v) = next.trim().parse::<usize>()
        {
            return Some(v);
        }
    }
    None
}

fn parse_f64_list_arg(name: &str) -> Option<Vec<f64>> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let mut raw_value: Option<String> = None;
    for (idx, arg) in args.iter().enumerate() {
        if let Some(raw) = arg.strip_prefix(&format!("{name}=")) {
            raw_value = Some(raw.trim().to_string());
            break;
        }
        if arg == name
            && let Some(next) = args.get(idx + 1)
        {
            raw_value = Some(next.trim().to_string());
            break;
        }
    }

    let raw = raw_value?;
    let mut out = Vec::new();
    for part in raw.split(',') {
        let trimmed = part.trim();
        if trimmed.is_empty() {
            continue;
        }
        if let Ok(v) = trimmed.parse::<f64>() {
            out.push(v);
        }
    }
    if out.is_empty() { None } else { Some(out) }
}

fn has_flag(name: &str) -> bool {
    std::env::args().skip(1).any(|arg| arg == name)
}

fn print_metrics(label: &str, metrics: calibration::Metrics) {
    println!("{label}:");
    println!(
        "  samples={} brier={:.4} log_loss={:.4} accuracy={:.3}",
        metrics.samples, metrics.brier, metrics.log_loss, metrics.accuracy
    );
}

fn apply_fitted_params(
    goals_total_base: f64,
    home_adv_goals: f64,
    dc_rho: f64,
    prematch_logit_scale: f64,
    prematch_draw_bias: f64,
    sample_matches: usize,
) -> Result<()> {
    let mut params = wc26_terminal::league_params::load_cached_params();
    params.insert(
        PREMIER_LEAGUE_ID,
        LeagueParams {
            league_id: PREMIER_LEAGUE_ID,
            sample_matches,
            goals_total_base,
            home_adv_goals,
            dc_rho,
            prematch_logit_scale,
            prematch_draw_bias,
        },
    );
    wc26_terminal::league_params::save_cached_params(&params)?;
    println!(
        "Applied fitted params to cache for league {} (sample_matches={})",
        PREMIER_LEAGUE_ID, sample_matches
    );
    Ok(())
}

fn train_split_index(n: usize) -> usize {
    if n <= 2 {
        return 1;
    }
    let idx = ((n as f64) * 0.85).round() as usize;
    idx.clamp(1, n - 1)
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
