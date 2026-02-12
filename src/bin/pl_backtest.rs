use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::{Context, Result, anyhow};

use wc26_terminal::calibration::{self, Outcome, Prob3};
use wc26_terminal::league_params::LeagueParams;
use wc26_terminal::pl_dataset::{self, PREMIER_LEAGUE_ID, StoredMatch};
use wc26_terminal::state::{MatchSummary, ModelQuality, WinProbRow};
use wc26_terminal::team_fixtures::FixtureMatch;
use wc26_terminal::win_prob;

fn main() -> Result<()> {
    let db_path = parse_db_path_arg()
        .or_else(pl_dataset::default_db_path)
        .context("unable to resolve sqlite path")?;
    let apply = has_flag("--apply");
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

    let fixtures: Vec<FixtureMatch> = rows
        .iter()
        .filter_map(StoredMatch::as_fixture_match)
        .collect();
    let outcomes: Vec<Outcome> = rows
        .iter()
        .filter_map(|m| {
            let (Some(h), Some(a)) = (m.home_goals, m.away_goals) else {
                return None;
            };
            Some(calibration::classify_outcome(h, a))
        })
        .collect();

    if fixtures.len() != outcomes.len() || fixtures.is_empty() {
        return Err(anyhow!(
            "unable to build consistent fixtures/outcomes for backtest"
        ));
    }

    let walk_forward = walk_forward_predictions(&rows);
    let walk_metrics = calibration::evaluate_probs(&walk_forward, &outcomes);

    let uniform = vec![Prob3::uniform(); outcomes.len()];
    let uniform_metrics = calibration::evaluate_probs(&uniform, &outcomes);

    let empirical_prob = calibration::empirical_outcome_probs(&outcomes);
    let empirical = vec![empirical_prob; outcomes.len()];
    let empirical_metrics = calibration::evaluate_probs(&empirical, &outcomes);

    let fitted_home_adv = calibration::fit_home_advantage(&fixtures);
    let fitted_goals_total = mean_goals_total(&fixtures);
    let fitted_rho = calibration::fit_dc_rho_for_league(
        PREMIER_LEAGUE_ID,
        &fixtures,
        fitted_goals_total,
        fitted_home_adv,
    );

    println!("Premier League pre-match backtest");
    println!("DB: {}", db_path.display());
    println!("Samples: {}", outcomes.len());
    if let (Some(first), Some(last)) = (rows.first(), rows.last()) {
        println!("Range UTC: {} -> {}", first.utc_time, last.utc_time);
    }
    println!();

    print_metrics("Walk-forward current model", walk_metrics);
    print_metrics("Uniform baseline", uniform_metrics);
    print_metrics("Empirical baseline", empirical_metrics);

    println!();
    println!(
        "Empirical outcome probs: H={:.3} D={:.3} A={:.3}",
        empirical_prob.home, empirical_prob.draw, empirical_prob.away
    );
    println!(
        "Fitted params (full PL dataset): goals_total={:.3} home_adv_goals={:+.3} dc_rho={:+.2}",
        fitted_goals_total, fitted_home_adv, fitted_rho
    );
    if apply {
        apply_fitted_params(
            fitted_goals_total,
            fitted_home_adv,
            fitted_rho,
            fixtures.len(),
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

fn walk_forward_predictions(rows: &[StoredMatch]) -> Vec<Prob3> {
    let mut history_count = 0usize;
    let mut history_total_goals = 0.0_f64;
    let mut history_home_minus_away = 0.0_f64;
    let mut predictions: Vec<Prob3> = Vec::with_capacity(rows.len());

    for m in rows {
        let params = cumulative_params(history_count, history_total_goals, history_home_minus_away);
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
            history_count += 1;
        }
    }

    predictions
}

fn cumulative_params(
    sample_matches: usize,
    total_goals: f64,
    home_minus_away: f64,
) -> LeagueParams {
    let mut goals_total_base = 2.60;
    let mut home_adv_goals = 0.0;
    if sample_matches > 0 {
        goals_total_base = total_goals / sample_matches as f64;
        home_adv_goals = home_minus_away / sample_matches as f64;
    }

    // Use the same sample-size shrinkage as runtime league params for stable priors.
    const MIN_N: f64 = 200.0;
    let w = (sample_matches as f64 / MIN_N).clamp(0.0, 1.0);
    goals_total_base = (1.0 - w) * 2.60 + w * goals_total_base;
    home_adv_goals *= w;

    LeagueParams {
        league_id: PREMIER_LEAGUE_ID,
        sample_matches,
        goals_total_base,
        home_adv_goals,
        dc_rho: -0.10,
    }
}

fn print_metrics(label: &str, metrics: calibration::Metrics) {
    println!("{label}:");
    println!(
        "  samples={} brier={:.4} log_loss={:.4} accuracy={:.3}",
        metrics.samples, metrics.brier, metrics.log_loss, metrics.accuracy
    );
}

fn mean_goals_total(fixtures: &[FixtureMatch]) -> f64 {
    if fixtures.is_empty() {
        return 2.60;
    }
    let total = fixtures
        .iter()
        .map(|m| (m.home_goals as f64) + (m.away_goals as f64))
        .sum::<f64>();
    total / fixtures.len() as f64
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

fn has_flag(name: &str) -> bool {
    std::env::args().skip(1).any(|arg| arg == name)
}

fn apply_fitted_params(
    goals_total_base: f64,
    home_adv_goals: f64,
    dc_rho: f64,
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
        },
    );
    wc26_terminal::league_params::save_cached_params(&params)?;
    println!(
        "Applied fitted params to cache for league {} (sample_matches={})",
        PREMIER_LEAGUE_ID, sample_matches
    );
    Ok(())
}
