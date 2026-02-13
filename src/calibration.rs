use crate::team_fixtures::FixtureMatch;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Home,
    Draw,
    Away,
}

#[derive(Debug, Clone, Copy)]
pub struct Prob3 {
    pub home: f64,
    pub draw: f64,
    pub away: f64,
}

#[derive(Debug, Clone, Copy)]
pub struct Metrics {
    pub samples: usize,
    pub brier: f64,
    pub log_loss: f64,
    pub accuracy: f64,
}

#[derive(Debug, Clone, Copy)]
pub struct CalibrationBin {
    pub bucket_start: f64,
    pub bucket_end: f64,
    pub count: usize,
    pub avg_pred: f64,
    pub actual_rate: f64,
}

impl Prob3 {
    pub fn uniform() -> Self {
        Self {
            home: 1.0 / 3.0,
            draw: 1.0 / 3.0,
            away: 1.0 / 3.0,
        }
    }
}

pub fn classify_outcome(home_goals: i32, away_goals: i32) -> Outcome {
    if home_goals > away_goals {
        Outcome::Home
    } else if home_goals < away_goals {
        Outcome::Away
    } else {
        Outcome::Draw
    }
}

pub fn empirical_outcome_probs(outcomes: &[Outcome]) -> Prob3 {
    if outcomes.is_empty() {
        return Prob3::uniform();
    }

    let mut home = 0usize;
    let mut draw = 0usize;
    let mut away = 0usize;
    for outcome in outcomes {
        match outcome {
            Outcome::Home => home += 1,
            Outcome::Draw => draw += 1,
            Outcome::Away => away += 1,
        }
    }
    let n = outcomes.len() as f64;
    Prob3 {
        home: home as f64 / n,
        draw: draw as f64 / n,
        away: away as f64 / n,
    }
}

pub fn outcome_probs_from_params(goals_total_base: f64, home_adv_goals: f64, rho: f64) -> Prob3 {
    probs_from_params(goals_total_base, home_adv_goals, rho)
}

pub fn evaluate_probs(predictions: &[Prob3], outcomes: &[Outcome]) -> Metrics {
    if predictions.is_empty() || outcomes.is_empty() || predictions.len() != outcomes.len() {
        return Metrics {
            samples: 0,
            brier: 0.0,
            log_loss: 0.0,
            accuracy: 0.0,
        };
    }

    let mut brier_sum = 0.0_f64;
    let mut log_loss_sum = 0.0_f64;
    let mut correct = 0usize;

    for (p, outcome) in predictions.iter().zip(outcomes) {
        let y = one_hot(*outcome);
        brier_sum +=
            (p.home - y.home).powi(2) + (p.draw - y.draw).powi(2) + (p.away - y.away).powi(2);

        let actual_prob = match outcome {
            Outcome::Home => p.home,
            Outcome::Draw => p.draw,
            Outcome::Away => p.away,
        }
        .clamp(1e-12, 1.0);
        log_loss_sum += -actual_prob.ln();

        if argmax(*p) == *outcome {
            correct += 1;
        }
    }

    let n = predictions.len() as f64;
    Metrics {
        samples: predictions.len(),
        brier: brier_sum / n,
        log_loss: log_loss_sum / n,
        accuracy: correct as f64 / n,
    }
}

pub fn evaluate_probs_weighted(
    predictions: &[Prob3],
    outcomes: &[Outcome],
    weights: &[f64],
) -> Metrics {
    if predictions.is_empty()
        || outcomes.is_empty()
        || predictions.len() != outcomes.len()
        || predictions.len() != weights.len()
    {
        return Metrics {
            samples: 0,
            brier: 0.0,
            log_loss: 0.0,
            accuracy: 0.0,
        };
    }

    let mut brier_sum = 0.0_f64;
    let mut log_loss_sum = 0.0_f64;
    let mut correct = 0.0_f64;
    let mut weight_sum = 0.0_f64;

    for ((p, outcome), w_raw) in predictions.iter().zip(outcomes).zip(weights) {
        let w = (*w_raw).max(1e-9);
        weight_sum += w;
        let y = one_hot(*outcome);
        brier_sum +=
            w * ((p.home - y.home).powi(2) + (p.draw - y.draw).powi(2) + (p.away - y.away).powi(2));

        let actual_prob = match outcome {
            Outcome::Home => p.home,
            Outcome::Draw => p.draw,
            Outcome::Away => p.away,
        }
        .clamp(1e-12, 1.0);
        log_loss_sum += -w * actual_prob.ln();

        if argmax(*p) == *outcome {
            correct += w;
        }
    }

    let n = weight_sum.max(1e-9);
    Metrics {
        samples: predictions.len(),
        brier: brier_sum / n,
        log_loss: log_loss_sum / n,
        accuracy: correct / n,
    }
}

pub fn apply_logit_calibration(prob: Prob3, logit_scale: f64, draw_bias: f64) -> Prob3 {
    let s = logit_scale.clamp(0.50, 1.80);
    let mut lh = prob.home.clamp(1e-9, 1.0).ln();
    let mut ld = prob.draw.clamp(1e-9, 1.0).ln() + draw_bias;
    let mut la = prob.away.clamp(1e-9, 1.0).ln();

    let mean = (lh + ld + la) / 3.0;
    lh = (lh - mean) * s;
    ld = (ld - mean) * s;
    la = (la - mean) * s;

    let mx = lh.max(ld.max(la));
    let eh = (lh - mx).exp();
    let ed = (ld - mx).exp();
    let ea = (la - mx).exp();
    let den = (eh + ed + ea).max(1e-12);

    Prob3 {
        home: (eh / den).clamp(0.0, 1.0),
        draw: (ed / den).clamp(0.0, 1.0),
        away: (ea / den).clamp(0.0, 1.0),
    }
}

pub fn fit_logit_calibration(predictions: &[Prob3], outcomes: &[Outcome]) -> (f64, f64, Metrics) {
    if predictions.is_empty() || outcomes.is_empty() || predictions.len() != outcomes.len() {
        return (
            1.0,
            0.0,
            Metrics {
                samples: 0,
                brier: 0.0,
                log_loss: 0.0,
                accuracy: 0.0,
            },
        );
    }

    let mut best_scale = 1.0;
    let mut best_draw_bias = 0.0;
    let mut best_metrics = evaluate_probs(predictions, outcomes);

    for scale_step in 35..=65 {
        let scale = scale_step as f64 / 50.0; // 0.70..1.30
        for draw_step in -30..=30 {
            let draw_bias = draw_step as f64 / 100.0; // -0.30..0.30
            let metrics = evaluate_calibrated(predictions, outcomes, scale, draw_bias);
            if metrics.log_loss < best_metrics.log_loss {
                best_metrics = metrics;
                best_scale = scale;
                best_draw_bias = draw_bias;
            }
        }
    }

    (best_scale, best_draw_bias, best_metrics)
}

pub fn fit_logit_calibration_weighted(
    predictions: &[Prob3],
    outcomes: &[Outcome],
    weights: &[f64],
) -> (f64, f64, Metrics) {
    if predictions.is_empty()
        || outcomes.is_empty()
        || predictions.len() != outcomes.len()
        || predictions.len() != weights.len()
    {
        return (
            1.0,
            0.0,
            Metrics {
                samples: 0,
                brier: 0.0,
                log_loss: 0.0,
                accuracy: 0.0,
            },
        );
    }

    let mut best_scale = 1.0;
    let mut best_draw_bias = 0.0;
    let mut best_metrics = evaluate_probs_weighted(predictions, outcomes, weights);

    for scale_step in 35..=65 {
        let scale = scale_step as f64 / 50.0; // 0.70..1.30
        for draw_step in -30..=30 {
            let draw_bias = draw_step as f64 / 100.0; // -0.30..0.30
            let metrics =
                evaluate_calibrated_weighted(predictions, outcomes, weights, scale, draw_bias);
            if metrics.log_loss < best_metrics.log_loss {
                best_metrics = metrics;
                best_scale = scale;
                best_draw_bias = draw_bias;
            }
        }
    }

    (best_scale, best_draw_bias, best_metrics)
}

pub fn fit_dc_rho_to_draw_rate(goals_total_base: f64, home_adv_goals: f64, draw_rate: f64) -> f64 {
    let target = draw_rate.clamp(0.05, 0.60);
    let mut best_rho = -0.10;
    let mut best_err = f64::INFINITY;

    for rho_steps in -25..=5 {
        let rho = rho_steps as f64 / 100.0;
        let p = probs_from_params(goals_total_base, home_adv_goals, rho);
        let err = (p.draw - target).abs();
        if err < best_err {
            best_err = err;
            best_rho = rho;
        }
    }
    best_rho
}

pub fn calibration_bins(
    predictions: &[Prob3],
    outcomes: &[Outcome],
    class: Outcome,
    bins: usize,
) -> Vec<CalibrationBin> {
    let bins = bins.max(2);
    let mut counts = vec![0usize; bins];
    let mut pred_sum = vec![0.0_f64; bins];
    let mut actual_sum = vec![0.0_f64; bins];

    for (p, outcome) in predictions.iter().zip(outcomes) {
        let class_prob = match class {
            Outcome::Home => p.home,
            Outcome::Draw => p.draw,
            Outcome::Away => p.away,
        }
        .clamp(0.0, 1.0);

        let idx = ((class_prob * bins as f64).floor() as usize).min(bins - 1);
        counts[idx] += 1;
        pred_sum[idx] += class_prob;
        if *outcome == class {
            actual_sum[idx] += 1.0;
        }
    }

    let mut out = Vec::with_capacity(bins);
    for i in 0..bins {
        let start = i as f64 / bins as f64;
        let end = (i + 1) as f64 / bins as f64;
        let count = counts[i];
        let (avg_pred, actual_rate) = if count > 0 {
            (pred_sum[i] / count as f64, actual_sum[i] / count as f64)
        } else {
            (0.0, 0.0)
        };
        out.push(CalibrationBin {
            bucket_start: start,
            bucket_end: end,
            count,
            avg_pred,
            actual_rate,
        });
    }
    out
}

pub fn fit_home_advantage(fixtures: &[FixtureMatch]) -> f64 {
    let mut n = 0usize;
    let mut sum_diff = 0.0_f64;
    for m in fixtures {
        if !is_valid_fixture(m) {
            continue;
        }
        sum_diff += m.home_goals as f64 - m.away_goals as f64;
        n += 1;
    }
    if n == 0 {
        return 0.0;
    }
    let raw = sum_diff / n as f64;
    const MIN_N: f64 = 300.0;
    let w = (n as f64 / MIN_N).clamp(0.0, 1.0);
    (raw * w).clamp(-0.60, 0.60)
}

pub fn fit_dc_rho_for_league(
    league_id: u32,
    fixtures: &[FixtureMatch],
    goals_total_base: f64,
    home_adv_goals: f64,
) -> f64 {
    let outcomes: Vec<Outcome> = fixtures
        .iter()
        .filter(|m| m.league_id == league_id)
        .filter(|m| is_valid_fixture(m))
        .map(|m| classify_outcome(m.home_goals as i32, m.away_goals as i32))
        .collect();

    if outcomes.is_empty() {
        return -0.10;
    }

    let mut best_rho = -0.10;
    let mut best_brier = f64::INFINITY;

    for rho_steps in -25..=5 {
        let rho = rho_steps as f64 / 100.0;
        let p = probs_from_params(goals_total_base, home_adv_goals, rho);
        let metrics = evaluate_probs(&vec![p; outcomes.len()], &outcomes);
        if metrics.brier < best_brier {
            best_brier = metrics.brier;
            best_rho = rho;
        }
    }

    best_rho
}

fn probs_from_params(goals_total_base: f64, home_adv_goals: f64, rho: f64) -> Prob3 {
    let lambda_home = ((goals_total_base + home_adv_goals) / 2.0).clamp(0.20, 3.80);
    let lambda_away = ((goals_total_base - home_adv_goals) / 2.0).clamp(0.20, 3.80);
    outcome_probs_poisson_dc(lambda_home, lambda_away, 10, rho)
}

fn outcome_probs_poisson_dc(lambda_home: f64, lambda_away: f64, max_goals: u32, rho: f64) -> Prob3 {
    let mut p_home = 0.0_f64;
    let mut p_draw = 0.0_f64;
    let mut p_away = 0.0_f64;

    for h in 0..=max_goals {
        for a in 0..=max_goals {
            let base = poisson_pmf(h, lambda_home) * poisson_pmf(a, lambda_away);
            let p = (base * dc_tau(h, a, lambda_home, lambda_away, rho)).max(0.0);
            if h > a {
                p_home += p;
            } else if h == a {
                p_draw += p;
            } else {
                p_away += p;
            }
        }
    }

    let sum = (p_home + p_draw + p_away).max(1e-12);
    Prob3 {
        home: p_home / sum,
        draw: p_draw / sum,
        away: p_away / sum,
    }
}

fn dc_tau(home_goals: u32, away_goals: u32, lambda_home: f64, lambda_away: f64, rho: f64) -> f64 {
    match (home_goals, away_goals) {
        (0, 0) => 1.0 - lambda_home * lambda_away * rho,
        (0, 1) => 1.0 + lambda_home * rho,
        (1, 0) => 1.0 + lambda_away * rho,
        (1, 1) => 1.0 - rho,
        _ => 1.0,
    }
}

fn poisson_pmf(goals: u32, lambda: f64) -> f64 {
    let numer = lambda.powi(goals as i32) * (-lambda).exp();
    let denom = (1..=goals).fold(1.0_f64, |acc, k| acc * k as f64).max(1.0);
    numer / denom
}

fn is_valid_fixture(m: &FixtureMatch) -> bool {
    m.finished && !m.cancelled && !m.awarded && !m.is_penalty_decided()
}

fn argmax(p: Prob3) -> Outcome {
    if p.home >= p.draw && p.home >= p.away {
        Outcome::Home
    } else if p.draw >= p.away {
        Outcome::Draw
    } else {
        Outcome::Away
    }
}

fn one_hot(outcome: Outcome) -> Prob3 {
    match outcome {
        Outcome::Home => Prob3 {
            home: 1.0,
            draw: 0.0,
            away: 0.0,
        },
        Outcome::Draw => Prob3 {
            home: 0.0,
            draw: 1.0,
            away: 0.0,
        },
        Outcome::Away => Prob3 {
            home: 0.0,
            draw: 0.0,
            away: 1.0,
        },
    }
}

fn evaluate_calibrated(
    predictions: &[Prob3],
    outcomes: &[Outcome],
    logit_scale: f64,
    draw_bias: f64,
) -> Metrics {
    let calibrated: Vec<Prob3> = predictions
        .iter()
        .copied()
        .map(|p| apply_logit_calibration(p, logit_scale, draw_bias))
        .collect();
    evaluate_probs(&calibrated, outcomes)
}

fn evaluate_calibrated_weighted(
    predictions: &[Prob3],
    outcomes: &[Outcome],
    weights: &[f64],
    logit_scale: f64,
    draw_bias: f64,
) -> Metrics {
    let calibrated: Vec<Prob3> = predictions
        .iter()
        .copied()
        .map(|p| apply_logit_calibration(p, logit_scale, draw_bias))
        .collect();
    evaluate_probs_weighted(&calibrated, outcomes, weights)
}

#[cfg(test)]
mod tests {
    use super::{Outcome, Prob3, apply_logit_calibration, evaluate_probs};

    #[test]
    fn perfect_predictions_have_zero_brier() {
        let preds = vec![
            Prob3 {
                home: 1.0,
                draw: 0.0,
                away: 0.0,
            },
            Prob3 {
                home: 0.0,
                draw: 1.0,
                away: 0.0,
            },
            Prob3 {
                home: 0.0,
                draw: 0.0,
                away: 1.0,
            },
        ];
        let outcomes = vec![Outcome::Home, Outcome::Draw, Outcome::Away];
        let m = evaluate_probs(&preds, &outcomes);
        assert_eq!(m.samples, 3);
        assert!(m.brier < 1e-12);
    }

    #[test]
    fn logit_calibration_keeps_distribution_normalized() {
        let p = Prob3 {
            home: 0.44,
            draw: 0.27,
            away: 0.29,
        };
        let q = apply_logit_calibration(p, 1.12, 0.08);
        let sum = q.home + q.draw + q.away;
        assert!((sum - 1.0).abs() < 1e-9);
        assert!(q.home >= 0.0 && q.draw >= 0.0 && q.away >= 0.0);
    }
}
