use std::collections::HashMap;

use crate::team_fixtures::FixtureMatch;

#[derive(Debug, Clone, Copy)]
pub struct EloConfig {
    pub k: f64,
    pub home_adv_pts: f64,
}

impl Default for EloConfig {
    fn default() -> Self {
        Self {
            k: 20.0,
            home_adv_pts: 60.0,
        }
    }
}

pub fn compute_elo_for_league(
    league_id: u32,
    fixtures: &[FixtureMatch],
    cfg: EloConfig,
) -> HashMap<u32, f64> {
    let mut matches: Vec<&FixtureMatch> = fixtures
        .iter()
        .filter(|m| m.league_id == league_id)
        .filter(|m| m.finished && !m.cancelled && !m.awarded)
        .filter(|m| !m.is_penalty_decided())
        .collect();

    // Using utc_time string ordering is acceptable here because FotMob uses ISO-ish timestamps.
    matches.sort_by(|a, b| a.utc_time.cmp(&b.utc_time).then(a.id.cmp(&b.id)));

    let mut elo: HashMap<u32, f64> = HashMap::new();
    for m in matches {
        let eh = *elo.entry(m.home_id).or_insert(1500.0);
        let ea = *elo.entry(m.away_id).or_insert(1500.0);

        let expected_home = expected_score(eh + cfg.home_adv_pts, ea);
        let s_home = if m.home_goals > m.away_goals {
            1.0
        } else if m.home_goals < m.away_goals {
            0.0
        } else {
            0.5
        };

        let delta = cfg.k * (s_home - expected_home);
        *elo.entry(m.home_id).or_insert(1500.0) = eh + delta;
        *elo.entry(m.away_id).or_insert(1500.0) = ea - delta;
    }

    elo
}

fn expected_score(r_a: f64, r_b: f64) -> f64 {
    1.0 / (1.0 + 10.0_f64.powf(-(r_a - r_b) / 400.0))
}
