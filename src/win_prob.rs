use std::collections::HashMap;

use crate::state::{LineupSide, MatchDetail, MatchSummary, ModelQuality, PlayerDetail, WinProbRow};

const GOALS_TOTAL_BASE: f64 = 2.60;
const HOME_ADV_GOALS: f64 = 0.15;
const K_STRENGTH: f64 = 0.45;

const BASELINE_RATING: f64 = 6.80;
const RATING_STDDEV: f64 = 0.60;

pub fn compute_win_prob(
    summary: &MatchSummary,
    detail: Option<&MatchDetail>,
    players: &HashMap<u32, PlayerDetail>,
) -> WinProbRow {
    // If the match is effectively final, just reflect the result.
    if !summary.is_live && summary.minute >= 90 {
        let (p_home, p_draw, p_away) = if summary.score_home > summary.score_away {
            (100.0, 0.0, 0.0)
        } else if summary.score_home < summary.score_away {
            (0.0, 0.0, 100.0)
        } else {
            (0.0, 100.0, 0.0)
        };
        return WinProbRow {
            p_home,
            p_draw,
            p_away,
            delta_home: 0.0,
            quality: ModelQuality::Basic,
            confidence: 95,
        };
    }

    let lineup = detail.and_then(|d| d.lineups.as_ref());
    let (s_home, s_away) = if let Some(lineups) = lineup {
        // `upcoming_fetch::parse_lineups` pushes homeTeam then awayTeam, so treat that as
        // the primary mapping. If that ever changes upstream, we still do a best-effort
        // match by team_abbr against the match summary.
        let home_key = summary.home.trim().to_uppercase();
        let away_key = summary.away.trim().to_uppercase();

        let mut home_side: Option<&LineupSide> = None;
        let mut away_side: Option<&LineupSide> = None;
        for side in &lineups.sides {
            let abbr = side.team_abbr.trim().to_uppercase();
            if home_side.is_none() && abbr == home_key {
                home_side = Some(side);
            }
            if away_side.is_none() && abbr == away_key {
                away_side = Some(side);
            }
        }

        if lineups.sides.is_empty() {
            (None, None)
        } else {
            let home_side = home_side.or_else(|| lineups.sides.first());
            let away_side = away_side.or_else(|| lineups.sides.get(1));
            match (home_side, away_side) {
                (Some(h), Some(a)) => (
                    lineup_team_strength(h, players),
                    lineup_team_strength(a, players),
                ),
                _ => (None, None),
            }
        }
    } else {
        (None, None)
    };

    let track = s_home.is_some() && s_away.is_some();
    let s_home = s_home.unwrap_or(0.0);
    let s_away = s_away.unwrap_or(0.0);

    let diff = HOME_ADV_GOALS + K_STRENGTH * (s_home - s_away);
    let lambda_home_pre = clamp((GOALS_TOTAL_BASE / 2.0) + (diff / 2.0), 0.20, 3.80);
    let lambda_away_pre = clamp((GOALS_TOTAL_BASE / 2.0) - (diff / 2.0), 0.20, 3.80);

    let minute = summary.minute.max(1).min(90) as f64;
    let t = minute / 90.0;
    let remain = (90.0 - minute) / 90.0;

    let mut quality = if track {
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
            if let Some((xg_h, xg_a)) = extract_stat_f64(d, &["xg", "expected goals"]) {
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
            } else if let Some((sot_h, sot_a)) = extract_stat_f64(d, &["shots on target"]) {
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
        }
    }

    if quality != ModelQuality::Track && used_live_stats {
        quality = ModelQuality::Event;
    }

    let (p_home, p_draw, p_away) = outcome_probs_poisson(
        summary.score_home as u32,
        summary.score_away as u32,
        lambda_home_rem,
        lambda_away_rem,
        10,
    );

    let mut p_home = (p_home * 100.0) as f32;
    let mut p_draw = (p_draw * 100.0) as f32;
    let mut p_away = (p_away * 100.0) as f32;

    // Normalize to exactly 100.0 to keep UI stable.
    let sum = (p_home + p_draw + p_away).max(0.0001);
    p_home = p_home / sum * 100.0;
    p_draw = p_draw / sum * 100.0;
    p_away = p_away / sum * 100.0;
    // Put any tiny rounding residue into draw (least visually jarring).
    let residue = 100.0 - (p_home + p_draw + p_away);
    p_draw += residue;

    let confidence = compute_confidence(t, xg_present, track);

    WinProbRow {
        p_home,
        p_draw,
        p_away,
        delta_home: 0.0,
        quality,
        confidence,
    }
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

fn extract_stat_f64(detail: &MatchDetail, keys: &[&str]) -> Option<(f64, f64)> {
    let want = keys
        .iter()
        .map(|k| k.trim().to_lowercase())
        .collect::<Vec<_>>();
    for row in &detail.stats {
        let name = row.name.trim().to_lowercase();
        if !want.iter().any(|k| name == *k) {
            continue;
        }
        let h = parse_stat_cell(&row.home)?;
        let a = parse_stat_cell(&row.away)?;
        return Some((h, a));
    }
    None
}

fn parse_stat_cell(raw: &str) -> Option<f64> {
    let s = raw.trim();
    if s.is_empty() || s == "-" {
        return None;
    }
    let s = s.trim_end_matches('%').replace(',', "");
    s.parse::<f64>().ok()
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

fn lineup_team_strength(lineup: &LineupSide, players: &HashMap<u32, PlayerDetail>) -> Option<f64> {
    let mut sum = 0.0;
    let mut cnt = 0usize;

    for slot in &lineup.starting {
        let Some(id) = slot.id else { continue };
        let Some(p) = players.get(&id) else { continue };
        let Some(r) = player_form_rating(p, 8) else {
            continue;
        };

        sum += (r - BASELINE_RATING) / RATING_STDDEV;
        cnt += 1;
    }

    if cnt >= 7 {
        Some(sum / cnt as f64)
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
    use crate::state::{MatchLineups, PlayerMatchStat, PlayerSlot, StatRow};

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
        };
        let win = compute_win_prob(&summary, None, &HashMap::new());
        let sum = win.p_home + win.p_draw + win.p_away;
        assert!((sum - 100.0).abs() < 0.01);
    }

    #[test]
    fn big_lead_late_is_overwhelming() {
        let summary = MatchSummary {
            id: "m".to_string(),
            league_id: None,
            league_name: "L".to_string(),
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
        };
        let win = compute_win_prob(&summary, None, &HashMap::new());
        assert!(win.p_home > 95.0);
    }

    #[test]
    fn xg_signal_moves_probabilities() {
        let summary = MatchSummary {
            id: "m".to_string(),
            league_id: None,
            league_name: "L".to_string(),
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
        };

        let detail = MatchDetail {
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
                name: "xG".to_string(),
                home: "1.80".to_string(),
                away: "0.30".to_string(),
            }],
        };

        let mut cache = HashMap::new();
        cache.insert(1, stub_player(1, &["7.2", "7.0", "6.9"]));
        cache.insert(2, stub_player(2, &["6.8", "6.7", "6.6"]));

        let win = compute_win_prob(&summary, Some(&detail), &cache);
        // With heavy xG edge at HT, home should be favored.
        assert!(win.p_home > win.p_away);
    }
}
