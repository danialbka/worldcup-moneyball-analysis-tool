use std::collections::HashMap;

use crate::state::{
    LineupSide, MatchDetail, MatchSummary, ModelQuality, PlayerDetail, TeamAnalysis, WinProbRow,
};

const GOALS_TOTAL_BASE: f64 = 2.60;
const HOME_ADV_GOALS: f64 = 0.15;
const K_STRENGTH: f64 = 0.45;

const BASELINE_RATING: f64 = 6.80;
const RATING_STDDEV: f64 = 0.60;

pub fn compute_win_prob(
    summary: &MatchSummary,
    detail: Option<&MatchDetail>,
    players: &HashMap<u32, PlayerDetail>,
    analysis: &[TeamAnalysis],
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
    let (lineup_s_home, lineup_s_away) = if let Some(lineups) = lineup {
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

    let track = lineup_s_home.is_some() && lineup_s_away.is_some();

    // If we don't have enough lineup+player coverage, fall back to TeamAnalysis (FIFA points/rank)
    // for a conservative pre-match prior.
    let (s_home, s_away) = if track {
        (lineup_s_home.unwrap_or(0.0), lineup_s_away.unwrap_or(0.0))
    } else {
        let home_label = detail
            .and_then(|d| d.home_team.as_deref())
            .filter(|s| !s.trim().is_empty())
            .unwrap_or(&summary.home);
        let away_label = detail
            .and_then(|d| d.away_team.as_deref())
            .filter(|s| !s.trim().is_empty())
            .unwrap_or(&summary.away);

        (
            team_strength_from_analysis(home_label, analysis).unwrap_or(0.0),
            team_strength_from_analysis(away_label, analysis).unwrap_or(0.0),
        )
    };

    let diff = HOME_ADV_GOALS + K_STRENGTH * (s_home - s_away);
    let lambda_home_pre = clamp((GOALS_TOTAL_BASE / 2.0) + (diff / 2.0), 0.20, 3.80);
    let lambda_away_pre = clamp((GOALS_TOTAL_BASE / 2.0) - (diff / 2.0), 0.20, 3.80);

    let effective_total = estimate_total_minutes(summary, detail);
    let minute = (summary.minute as f64).max(1.0).min(effective_total);
    let t = minute / effective_total;
    let remain = (effective_total - minute) / effective_total;

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

            // Extra live signals (bounded).
            apply_red_card_adjustment(summary, d, &mut lambda_home_rem, &mut lambda_away_rem);

            // If xG is missing, try other weak signals.
            if !xg_present {
                if let Some((bc_h, bc_a)) = extract_stat_f64(d, &["big chances"]) {
                    used_live_stats = true;
                    let delta = bc_h - bc_a;
                    let b = clamp(t, 0.0, 0.50);
                    lambda_home_rem = clamp(lambda_home_rem * (1.0 + 0.06 * delta * b), 0.05, 3.00);
                    lambda_away_rem = clamp(lambda_away_rem * (1.0 - 0.06 * delta * b), 0.05, 3.00);
                } else if let Some((xgot_h, xgot_a)) =
                    extract_stat_f64(d, &["xgot", "xg on target"])
                {
                    used_live_stats = true;
                    let delta = xgot_h - xgot_a;
                    let b = clamp(t, 0.0, 0.50);
                    lambda_home_rem = clamp(lambda_home_rem * (1.0 + 0.04 * delta * b), 0.05, 3.00);
                    lambda_away_rem = clamp(lambda_away_rem * (1.0 - 0.04 * delta * b), 0.05, 3.00);
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

fn extract_stat_f64(detail: &MatchDetail, keys: &[&str]) -> Option<(f64, f64)> {
    for row in &detail.stats {
        let name = row.name.trim();
        if !keys.iter().any(|k| name.eq_ignore_ascii_case(k.trim())) {
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

fn team_strength_from_analysis(label: &str, analysis: &[TeamAnalysis]) -> Option<f64> {
    let key = normalize_team_key(label);
    if key.is_empty() {
        return None;
    }
    for t in analysis {
        let name_key = normalize_team_key(&t.name);
        if name_key == key {
            return analysis_team_strength(t);
        }
        if abbreviate_team_key(&t.name) == key {
            return analysis_team_strength(t);
        }
    }
    None
}

fn analysis_team_strength(t: &TeamAnalysis) -> Option<f64> {
    if let Some(points) = t.fifa_points {
        let s = (points as f64 - 1600.0) / 400.0;
        return Some(clamp(s, -1.0, 1.0));
    }
    if let Some(rank) = t.fifa_rank {
        let s = (100.0 - rank as f64) / 100.0;
        return Some(clamp(s, -1.0, 1.0));
    }
    None
}

fn abbreviate_team_key(name: &str) -> String {
    let trimmed = normalize_team_key(name);
    if trimmed.len() <= 3 {
        return trimmed;
    }
    let mut abbr = String::new();
    for part in trimmed.split_whitespace() {
        if let Some(ch) = part.chars().next() {
            abbr.push(ch);
        }
        if abbr.len() >= 3 {
            break;
        }
    }
    if abbr.len() >= 2 {
        abbr
    } else {
        trimmed.chars().take(3).collect()
    }
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
        let Some(p) = match_player(slot, players, Some(&lineup.team)) else {
            continue;
        };
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
        let win = compute_win_prob(&summary, None, &HashMap::new(), &[]);
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
        let win = compute_win_prob(&summary, None, &HashMap::new(), &[]);
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
                name: "xG".to_string(),
                home: "1.80".to_string(),
                away: "0.30".to_string(),
            }],
        };

        let mut cache = HashMap::new();
        cache.insert(1, stub_player(1, &["7.2", "7.0", "6.9"]));
        cache.insert(2, stub_player(2, &["6.8", "6.7", "6.6"]));

        let win = compute_win_prob(&summary, Some(&detail), &cache, &[]);
        // With heavy xG edge at HT, home should be favored.
        assert!(win.p_home > win.p_away);
    }
}
