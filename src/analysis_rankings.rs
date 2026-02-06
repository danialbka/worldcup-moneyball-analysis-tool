use std::collections::{HashMap, HashSet};

use crate::state::{
    PlayerDetail, RankFactor, RoleCategory, RoleRankingEntry, SquadPlayer, TeamAnalysis,
    player_detail_is_stub,
};

/// Build role rankings from cached squads + cached player details.
/// This is fast and avoids re-fetching network data.
pub fn compute_role_rankings_from_cache(
    teams: &[TeamAnalysis],
    squads: &HashMap<u32, Vec<SquadPlayer>>,
    players: &HashMap<u32, PlayerDetail>,
) -> Vec<RoleRankingEntry> {
    let team_name_map: HashMap<u32, String> =
        teams.iter().map(|t| (t.id, t.name.clone())).collect();

    let mut features: Vec<PlayerFeatures> = Vec::new();
    let mut capacity = 0usize;
    for team in teams {
        if let Some(team_squad) = squads.get(&team.id) {
            capacity += team_squad.len();
        }
    }
    if capacity > 0 {
        features.reserve(capacity);
    }

    for team in teams {
        let Some(team_squad) = squads.get(&team.id) else {
            continue;
        };
        for sp in team_squad {
            let Some(detail) = players.get(&sp.id) else {
                continue;
            };
            if player_detail_is_stub(detail) {
                continue;
            }
            if let Some(row) = build_player_features(team, &team_name_map, sp, detail) {
                features.push(row);
            }
        }
    }

    build_rankings_from_features(&features)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum CanonStat {
    // Participation / sample size.
    Appearances,
    MinutesPlayed,

    // Attacking / creation.
    Goals,
    Assists,
    Xg,
    XgNonPenalty,
    Xa,
    Xgot,
    Shots,
    ShotsOnTarget,
    KeyPasses,
    ChancesCreated,
    BigChancesCreated,
    Touches,
    TouchesInOppBox,
    Dribbles,
    Dispossessed,

    // Possession / passing.
    AccuratePasses,
    PassAccuracy,
    AccurateLongBalls,
    LongBallAccuracy,
    SuccessfulCrosses,
    CrossAccuracy,

    // Defensive / duels.
    Tackles,
    Interceptions,
    Clearances,
    Blocks,
    Recoveries,
    PossWonFinalThird,
    DuelsWon,
    DuelsWonPct,
    AerialsWon,
    AerialsWonPct,
    DribbledPast,
    BlockedScoringAttempt,
    FoulsCommitted,
    YellowCards,
    RedCards,

    // Team suppression (player on-pitch).
    GoalsConcededOnPitch,
    XgAgainstOnPitch,

    // Goalkeeping.
    Saves,
    SavePct,
    CleanSheets,
    GoalsConceded,
    ErrorLedToGoal,
    ActedAsSweeper,
    HighClaims,

    // Rating.
    Rating,

    // Derived.
    FinishingDelta,
    ShotPlacementDelta,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Direction {
    HigherBetter,
    LowerBetter,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StatSource {
    Percentile,
    Raw,
}

#[derive(Debug, Clone)]
struct StatObs {
    raw: Option<f64>,
    pct: Option<f64>, // 0..=100
}

#[derive(Debug, Clone)]
struct PlayerFeatures {
    pub role: RoleCategory,
    pub player_id: u32,
    pub player_name: String,
    pub team_id: u32,
    pub team_name: String,
    pub club: String,
    pub stats: HashMap<CanonStat, StatObs>,
    pub rating: Option<f64>,
}

fn build_player_features(
    team: &TeamAnalysis,
    team_name_map: &HashMap<u32, String>,
    squad_player: &SquadPlayer,
    detail: &PlayerDetail,
) -> Option<PlayerFeatures> {
    let role = role_category_from_text(&squad_player.role)?;
    let (mut stats, rating) = collect_stat_features(detail);
    insert_derived_stats(&mut stats);
    let team_name = team_name_map
        .get(&team.id)
        .cloned()
        .unwrap_or_else(|| team.name.clone());
    Some(PlayerFeatures {
        role,
        player_id: squad_player.id,
        player_name: squad_player.name.clone(),
        team_id: team.id,
        team_name,
        club: squad_player.club.clone(),
        rating,
        stats,
    })
}

fn role_category_from_text(raw: &str) -> Option<RoleCategory> {
    let s = raw.to_lowercase();
    if s.contains("goalkeeper") || s.contains("keeper") || s == "gk" {
        return Some(RoleCategory::Goalkeeper);
    }
    if s.contains("defender")
        || s.contains("back")
        || s.contains("centre-back")
        || s.contains("center-back")
    {
        return Some(RoleCategory::Defender);
    }
    if s.contains("midfield") || s.contains("midfielder") {
        return Some(RoleCategory::Midfielder);
    }
    if s.contains("attacker")
        || s.contains("forward")
        || s.contains("striker")
        || s.contains("wing")
    {
        return Some(RoleCategory::Attacker);
    }
    None
}

/// Collect stats from `PlayerDetail` across multiple sections.
/// We prefer per-90 values when present.
fn collect_stat_features(detail: &PlayerDetail) -> (HashMap<CanonStat, StatObs>, Option<f64>) {
    let mut out: HashMap<CanonStat, StatObs> = HashMap::new();

    // Participation / sample size.
    insert_stat(
        &mut out,
        CanonStat::Appearances,
        detail,
        &["appearances", "matches played", "apps"],
        &[],
    );
    insert_stat(
        &mut out,
        CanonStat::MinutesPlayed,
        detail,
        &["minutes played", "minutes"],
        &[],
    );

    // Rating (used as extra signal + display).
    let rating = find_stat_observation(detail, &["rating"], &[])
        .and_then(|o| o.raw)
        .or_else(|| {
            detail
                .season_breakdown
                .first()
                .and_then(|row| parse_number(&row.rating))
        });
    if let Some(r) = rating {
        out.insert(
            CanonStat::Rating,
            StatObs {
                raw: Some(r),
                pct: None,
            },
        );
    }

    // Scoring / shooting.
    insert_stat(
        &mut out,
        CanonStat::Goals,
        detail,
        &["goals"],
        &["goals conceded"],
    );
    insert_stat(&mut out, CanonStat::Assists, detail, &["assists"], &[]);
    insert_stat(
        &mut out,
        CanonStat::Xg,
        detail,
        &["expected goals", "xg"],
        &[],
    );
    insert_stat(
        &mut out,
        CanonStat::XgNonPenalty,
        detail,
        &["xg excl. penalty", "xg excl penalty", "xg (excl. penalty)"],
        &[],
    );
    insert_stat(
        &mut out,
        CanonStat::Xa,
        detail,
        &["expected assists", "xa", "x a"],
        &[],
    );
    insert_stat(&mut out, CanonStat::Xgot, detail, &["xgot"], &[]);

    insert_stat(
        &mut out,
        CanonStat::ShotsOnTarget,
        detail,
        &["shots on target"],
        &[],
    );
    insert_stat(
        &mut out,
        CanonStat::Shots,
        detail,
        &["shots"],
        &["shots on target"],
    );

    // Creation / possession.
    insert_stat(&mut out, CanonStat::KeyPasses, detail, &["key passes"], &[]);
    insert_stat(
        &mut out,
        CanonStat::ChancesCreated,
        detail,
        &["chances created"],
        &[],
    );
    insert_stat(
        &mut out,
        CanonStat::BigChancesCreated,
        detail,
        &["big chances created"],
        &[],
    );
    insert_stat(&mut out, CanonStat::Dribbles, detail, &["dribbles"], &[]);
    insert_stat(
        &mut out,
        CanonStat::Dispossessed,
        detail,
        &["dispossessed"],
        &[],
    );
    insert_stat(&mut out, CanonStat::Touches, detail, &["touches"], &[]);
    insert_stat(
        &mut out,
        CanonStat::TouchesInOppBox,
        detail,
        &["touches in opposition box", "touches in opp box"],
        &[],
    );

    // Passing / distribution.
    insert_stat(
        &mut out,
        CanonStat::AccuratePasses,
        detail,
        &["accurate passes"],
        &[],
    );
    insert_stat(
        &mut out,
        CanonStat::PassAccuracy,
        detail,
        &["pass accuracy"],
        &[],
    );
    insert_stat(
        &mut out,
        CanonStat::AccurateLongBalls,
        detail,
        &["accurate long balls"],
        &[],
    );
    insert_stat(
        &mut out,
        CanonStat::LongBallAccuracy,
        detail,
        &["long ball accuracy"],
        &[],
    );
    insert_stat(
        &mut out,
        CanonStat::SuccessfulCrosses,
        detail,
        &["successful crosses"],
        &[],
    );
    insert_stat(
        &mut out,
        CanonStat::CrossAccuracy,
        detail,
        &["cross accuracy"],
        &[],
    );

    // Defending.
    insert_stat(&mut out, CanonStat::Tackles, detail, &["tackles"], &[]);
    insert_stat(
        &mut out,
        CanonStat::Interceptions,
        detail,
        &["interceptions"],
        &[],
    );
    insert_stat(
        &mut out,
        CanonStat::Clearances,
        detail,
        &["clearances"],
        &[],
    );
    insert_stat(&mut out, CanonStat::Blocks, detail, &["blocks"], &[]);
    insert_stat(
        &mut out,
        CanonStat::Recoveries,
        detail,
        &["recoveries"],
        &[],
    );
    insert_stat(
        &mut out,
        CanonStat::PossWonFinalThird,
        detail,
        &["possession won final 3rd", "possession won final third"],
        &[],
    );
    insert_stat(
        &mut out,
        CanonStat::DuelsWon,
        detail,
        &["duels won"],
        &["duels won %"],
    );
    insert_stat(
        &mut out,
        CanonStat::DuelsWonPct,
        detail,
        &["duels won %", "duels won%"],
        &[],
    );
    insert_stat(
        &mut out,
        CanonStat::AerialsWon,
        detail,
        &["aerials won"],
        &["aerials won %"],
    );
    insert_stat(
        &mut out,
        CanonStat::AerialsWonPct,
        detail,
        &["aerials won %", "aerials won%"],
        &[],
    );
    insert_stat(
        &mut out,
        CanonStat::DribbledPast,
        detail,
        &["dribbled past"],
        &[],
    );
    insert_stat(
        &mut out,
        CanonStat::BlockedScoringAttempt,
        detail,
        &["blocked scoring attempt"],
        &[],
    );

    // Discipline / fouls.
    insert_stat(
        &mut out,
        CanonStat::FoulsCommitted,
        detail,
        &["fouls committed"],
        &[],
    );
    insert_stat(
        &mut out,
        CanonStat::YellowCards,
        detail,
        &["yellow cards"],
        &[],
    );
    insert_stat(&mut out, CanonStat::RedCards, detail, &["red cards"], &[]);

    // Team suppression on pitch.
    insert_stat(
        &mut out,
        CanonStat::GoalsConcededOnPitch,
        detail,
        &["goals conceded while on pitch"],
        &[],
    );
    insert_stat(
        &mut out,
        CanonStat::XgAgainstOnPitch,
        detail,
        &["xg against while on pitch"],
        &[],
    );

    // GK.
    insert_stat(&mut out, CanonStat::Saves, detail, &["saves"], &[]);
    insert_stat(
        &mut out,
        CanonStat::SavePct,
        detail,
        &["save percentage", "save%", "save %", "save percentage"],
        &[],
    );
    insert_stat(
        &mut out,
        CanonStat::CleanSheets,
        detail,
        &["clean sheets"],
        &[],
    );
    insert_stat(
        &mut out,
        CanonStat::GoalsConceded,
        detail,
        &["goals conceded"],
        &["goals conceded while on pitch"],
    );
    insert_stat(
        &mut out,
        CanonStat::ErrorLedToGoal,
        detail,
        &["error led to goal"],
        &[],
    );
    insert_stat(
        &mut out,
        CanonStat::ActedAsSweeper,
        detail,
        &["acted as sweeper"],
        &[],
    );
    insert_stat(
        &mut out,
        CanonStat::HighClaims,
        detail,
        &["high claims"],
        &[],
    );

    (out, rating)
}

fn insert_derived_stats(stats: &mut HashMap<CanonStat, StatObs>) {
    let goals = stats.get(&CanonStat::Goals).and_then(|o| o.raw);
    let xg_np = stats
        .get(&CanonStat::XgNonPenalty)
        .and_then(|o| o.raw)
        .or_else(|| stats.get(&CanonStat::Xg).and_then(|o| o.raw));
    if let (Some(g), Some(xg)) = (goals, xg_np) {
        stats.insert(
            CanonStat::FinishingDelta,
            StatObs {
                raw: Some(g - xg),
                pct: None,
            },
        );
    }

    let xg = stats.get(&CanonStat::Xg).and_then(|o| o.raw);
    let xgot = stats.get(&CanonStat::Xgot).and_then(|o| o.raw);
    if let (Some(xgot), Some(xg)) = (xgot, xg) {
        stats.insert(
            CanonStat::ShotPlacementDelta,
            StatObs {
                raw: Some(xgot - xg),
                pct: None,
            },
        );
    }
}

fn build_rankings_from_features(features: &[PlayerFeatures]) -> Vec<RoleRankingEntry> {
    let mut dist: HashMap<(RoleCategory, CanonStat, Direction), (f64, f64)> = HashMap::new();

    // Only build raw distributions for stats that appear in any spec. Percentile-based stats don't
    // need this, but we still want fallback for missing percentiles.
    let mut needed: HashSet<(RoleCategory, CanonStat, Direction)> = HashSet::new();
    for role in [
        RoleCategory::Goalkeeper,
        RoleCategory::Defender,
        RoleCategory::Midfielder,
        RoleCategory::Attacker,
    ] {
        for (s, d, _) in role_attack_specs(role) {
            needed.insert((role, *s, *d));
        }
        for (s, d, _) in role_defense_specs(role) {
            needed.insert((role, *s, *d));
        }
    }

    for (role, stat, dir) in needed {
        if let Some(d) = dist_for_role(features, role, stat, dir) {
            dist.insert((role, stat, dir), d);
        }
    }

    features
        .iter()
        .map(|f| {
            let (attack_score, attack_factors) =
                composite_weighted_score(f, role_attack_specs(f.role), &dist);
            let (defense_score, defense_factors) =
                composite_weighted_score(f, role_defense_specs(f.role), &dist);
            RoleRankingEntry {
                role: f.role,
                player_id: f.player_id,
                player_name: f.player_name.clone(),
                team_id: f.team_id,
                team_name: f.team_name.clone(),
                club: f.club.clone(),
                attack_score,
                defense_score,
                rating: f.rating,
                attack_factors,
                defense_factors,
            }
        })
        .collect()
}

fn role_attack_specs(role: RoleCategory) -> &'static [(CanonStat, Direction, f64)] {
    use CanonStat as S;
    use Direction::{HigherBetter as H, LowerBetter as L};

    // Weights are in "z units"; composite is a weighted mean.
    match role {
        RoleCategory::Attacker => &[
            (S::XgNonPenalty, H, 2.0),
            (S::Goals, H, 1.2),
            (S::FinishingDelta, H, 0.8),
            (S::Xa, H, 1.2),
            (S::Assists, H, 0.8),
            (S::ChancesCreated, H, 1.0),
            (S::BigChancesCreated, H, 1.0),
            (S::ShotsOnTarget, H, 0.7),
            (S::TouchesInOppBox, H, 0.9),
            (S::Dribbles, H, 0.6),
            (S::Dispossessed, L, 0.6),
            (S::Rating, H, 0.6),
        ],
        RoleCategory::Midfielder => &[
            (S::Xa, H, 1.2),
            (S::ChancesCreated, H, 1.0),
            (S::AccuratePasses, H, 0.9),
            (S::PassAccuracy, H, 0.7),
            (S::AccurateLongBalls, H, 0.6),
            (S::LongBallAccuracy, H, 0.5),
            (S::Touches, H, 0.5),
            (S::Dribbles, H, 0.5),
            (S::Dispossessed, L, 0.6),
            (S::Rating, H, 0.6),
        ],
        RoleCategory::Defender => &[
            (S::AccuratePasses, H, 0.8),
            (S::PassAccuracy, H, 0.7),
            (S::AccurateLongBalls, H, 0.7),
            (S::LongBallAccuracy, H, 0.6),
            (S::ChancesCreated, H, 0.4),
            (S::Xa, H, 0.4),
            (S::Touches, H, 0.4),
            (S::Rating, H, 0.4),
        ],
        RoleCategory::Goalkeeper => &[
            (S::AccuratePasses, H, 0.8),
            (S::PassAccuracy, H, 0.7),
            (S::AccurateLongBalls, H, 0.7),
            (S::LongBallAccuracy, H, 0.6),
            (S::ActedAsSweeper, H, 0.5),
        ],
    }
}

fn role_defense_specs(role: RoleCategory) -> &'static [(CanonStat, Direction, f64)] {
    use CanonStat as S;
    use Direction::{HigherBetter as H, LowerBetter as L};

    match role {
        RoleCategory::Attacker => &[
            (S::PossWonFinalThird, H, 1.0),
            (S::Recoveries, H, 0.6),
            (S::DuelsWonPct, H, 0.4),
            (S::AerialsWonPct, H, 0.3),
            (S::FoulsCommitted, L, 0.3),
            (S::YellowCards, L, 0.3),
            (S::RedCards, L, 0.4),
            (S::Rating, H, 0.3),
        ],
        RoleCategory::Midfielder => &[
            (S::Tackles, H, 0.9),
            (S::Interceptions, H, 0.9),
            (S::Recoveries, H, 0.9),
            (S::DuelsWonPct, H, 0.6),
            (S::AerialsWonPct, H, 0.4),
            (S::PossWonFinalThird, H, 0.6),
            (S::DribbledPast, L, 0.6),
            (S::YellowCards, L, 0.3),
            (S::RedCards, L, 0.3),
            (S::Rating, H, 0.4),
        ],
        RoleCategory::Defender => &[
            (S::Tackles, H, 1.0),
            (S::Interceptions, H, 1.0),
            (S::Clearances, H, 0.9),
            (S::Blocks, H, 0.8),
            (S::Recoveries, H, 0.8),
            (S::DuelsWonPct, H, 0.8),
            (S::AerialsWonPct, H, 0.9),
            (S::DribbledPast, L, 0.8),
            (S::GoalsConcededOnPitch, L, 0.7),
            (S::XgAgainstOnPitch, L, 0.7),
            (S::Rating, H, 0.3),
        ],
        RoleCategory::Goalkeeper => &[
            (S::SavePct, H, 1.3),
            (S::Saves, H, 0.8),
            (S::GoalsConceded, L, 1.1),
            (S::CleanSheets, H, 0.7),
            (S::ErrorLedToGoal, L, 0.9),
            (S::HighClaims, H, 0.5),
            (S::Rating, H, 0.4),
        ],
    }
}

fn dist_for_role(
    features: &[PlayerFeatures],
    role: RoleCategory,
    stat: CanonStat,
    dir: Direction,
) -> Option<(f64, f64)> {
    let mut values: Vec<f64> = Vec::new();
    for f in features.iter().filter(|f| f.role == role) {
        let Some(v) = f.stats.get(&stat).and_then(|o| o.raw) else {
            continue;
        };
        values.push(apply_dir(v, dir));
    }
    if values.len() < 2 {
        return None;
    }
    let mean = values.iter().sum::<f64>() / values.len() as f64;
    let var = values
        .iter()
        .map(|v| {
            let d = v - mean;
            d * d
        })
        .sum::<f64>()
        / (values.len() as f64);
    let std = var.sqrt();
    if std <= 1e-9 { None } else { Some((mean, std)) }
}

fn composite_weighted_score(
    f: &PlayerFeatures,
    specs: &[(CanonStat, Direction, f64)],
    dist: &HashMap<(RoleCategory, CanonStat, Direction), (f64, f64)>,
) -> (f64, Vec<RankFactor>) {
    const COVERAGE_MIN: f64 = 0.45;
    const COVERAGE_PENALTY: f64 = 0.8; // in z units
    const PART_PENALTY: f64 = 1.0; // in z units

    let mut w_total = 0.0;
    let mut w_used = 0.0;
    let mut sum = 0.0;
    let mut factors: Vec<RankFactor> = Vec::new();

    for (stat, dir, w) in specs {
        w_total += *w;
        let Some(obs) = f.stats.get(stat) else {
            continue;
        };

        let mut used_source: Option<StatSource> = None;
        let mut z: Option<f64> = None;
        let mut raw: Option<f64> = None;
        let mut pct: Option<f64> = None;

        if let Some(p) = obs.pct {
            let mut z_pct = pct_to_z(p);
            if matches!(dir, Direction::LowerBetter) {
                z_pct = -z_pct;
            }
            used_source = Some(StatSource::Percentile);
            z = Some(z_pct);
            pct = Some(p);
            raw = obs.raw;
        } else if let Some(v) = obs.raw {
            if let Some((mean, std)) = dist.get(&(f.role, *stat, *dir)).copied() {
                let v_dir = apply_dir(v, *dir);
                let z_raw = (v_dir - mean) / std;
                if z_raw.is_finite() {
                    used_source = Some(StatSource::Raw);
                    z = Some(z_raw);
                    raw = Some(v);
                }
            }
        }

        let Some(z) = z else { continue };
        if !z.is_finite() || !w.is_finite() || *w <= 0.0 {
            continue;
        }

        sum += *w * z;
        w_used += *w;
        factors.push(RankFactor {
            label: canon_label(*stat).to_string(),
            z,
            weight: *w,
            raw,
            pct,
            source: match used_source.unwrap_or(StatSource::Raw) {
                StatSource::Percentile => "pct".to_string(),
                StatSource::Raw => "raw".to_string(),
            },
        });
    }

    if w_used <= 0.0 || w_total <= 0.0 {
        return (f64::NEG_INFINITY, Vec::new());
    }

    let coverage = (w_used / w_total).clamp(0.0, 1.0);
    if coverage < COVERAGE_MIN {
        return (f64::NEG_INFINITY, Vec::new());
    }

    let mut score = sum / w_used;
    score -= (1.0 - coverage) * COVERAGE_PENALTY;
    score = apply_participation_adjustment(f, score, PART_PENALTY);

    // Keep top contributors by absolute impact (weight * z).
    factors.sort_by(|a, b| {
        let ia = (a.weight * a.z).abs();
        let ib = (b.weight * b.z).abs();
        ib.total_cmp(&ia)
    });
    factors.truncate(5);

    (score, factors)
}

fn apply_participation_adjustment(f: &PlayerFeatures, base: f64, penalty: f64) -> f64 {
    if !base.is_finite() {
        return base;
    }

    const FULL_MINUTES: f64 = 900.0; // ~10 full matches
    const FULL_APPS: f64 = 10.0;

    let minutes = f
        .stats
        .get(&CanonStat::MinutesPlayed)
        .and_then(|o| o.raw)
        .unwrap_or(0.0);
    let apps = f
        .stats
        .get(&CanonStat::Appearances)
        .and_then(|o| o.raw)
        .unwrap_or(0.0);

    let rel = if minutes > 0.0 {
        (minutes / FULL_MINUTES).clamp(0.0, 1.0).sqrt()
    } else if apps > 0.0 {
        (apps / FULL_APPS).clamp(0.0, 1.0).sqrt()
    } else {
        0.0
    };

    base * rel - (1.0 - rel) * penalty
}

fn pct_to_z(pct: f64) -> f64 {
    // Simple stable mapping: 50 => 0, 65 => +1, 35 => -1.
    ((pct - 50.0) / 15.0).clamp(-3.0, 3.0)
}

fn apply_dir(v: f64, dir: Direction) -> f64 {
    match dir {
        Direction::HigherBetter => v,
        Direction::LowerBetter => -v,
    }
}

fn canon_label(stat: CanonStat) -> &'static str {
    use CanonStat as S;
    match stat {
        S::Appearances => "Appearances",
        S::MinutesPlayed => "Minutes",
        S::Goals => "Goals",
        S::Assists => "Assists",
        S::Xg => "xG",
        S::XgNonPenalty => "xG excl. pen",
        S::Xa => "xA",
        S::Xgot => "xGOT",
        S::Shots => "Shots",
        S::ShotsOnTarget => "Shots on target",
        S::KeyPasses => "Key passes",
        S::ChancesCreated => "Chances created",
        S::BigChancesCreated => "Big chances created",
        S::Touches => "Touches",
        S::TouchesInOppBox => "Touches in box",
        S::Dribbles => "Dribbles",
        S::Dispossessed => "Dispossessed",
        S::AccuratePasses => "Accurate passes",
        S::PassAccuracy => "Pass accuracy",
        S::AccurateLongBalls => "Accurate long balls",
        S::LongBallAccuracy => "Long ball accuracy",
        S::SuccessfulCrosses => "Successful crosses",
        S::CrossAccuracy => "Cross accuracy",
        S::Tackles => "Tackles",
        S::Interceptions => "Interceptions",
        S::Clearances => "Clearances",
        S::Blocks => "Blocks",
        S::Recoveries => "Recoveries",
        S::PossWonFinalThird => "Poss. won final 3rd",
        S::DuelsWon => "Duels won",
        S::DuelsWonPct => "Duels won %",
        S::AerialsWon => "Aerials won",
        S::AerialsWonPct => "Aerials won %",
        S::DribbledPast => "Dribbled past",
        S::BlockedScoringAttempt => "Blocked scoring att.",
        S::FoulsCommitted => "Fouls committed",
        S::YellowCards => "Yellow cards",
        S::RedCards => "Red cards",
        S::GoalsConcededOnPitch => "GC on pitch",
        S::XgAgainstOnPitch => "xGA on pitch",
        S::Saves => "Saves",
        S::SavePct => "Save %",
        S::CleanSheets => "Clean sheets",
        S::GoalsConceded => "Goals conceded",
        S::ErrorLedToGoal => "Error led to goal",
        S::ActedAsSweeper => "Sweeper actions",
        S::HighClaims => "High claims",
        S::Rating => "Rating",
        S::FinishingDelta => "Goals - xG",
        S::ShotPlacementDelta => "xGOT - xG",
    }
}

fn insert_stat(
    out: &mut HashMap<CanonStat, StatObs>,
    key: CanonStat,
    detail: &PlayerDetail,
    needles: &[&str],
    excludes: &[&str],
) {
    if let Some(v) = find_stat_observation(detail, needles, excludes) {
        out.insert(key, v);
    }
}

#[derive(Debug, Clone, Copy)]
struct StatCandidate<'a> {
    title: &'a str,
    total: &'a str,
    per90: Option<&'a str>,
    pct_total: Option<f64>,
    pct_per90: Option<f64>,
}

fn find_stat_observation(
    detail: &PlayerDetail,
    needles: &[&str],
    excludes: &[&str],
) -> Option<StatObs> {
    let mut best: Option<(u8, StatObs)> = None;

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

        let pct = c.pct_per90.or(c.pct_total);
        let raw = c
            .per90
            .and_then(parse_number)
            .or_else(|| parse_number(c.total));
        let obs = StatObs { raw, pct };

        // Prefer percentile-per90 > percentile-total > raw-per90 > raw-total.
        let quality = if c.pct_per90.is_some() {
            4
        } else if c.pct_total.is_some() {
            3
        } else if c.per90.is_some() {
            2
        } else {
            1
        };

        match best.as_ref() {
            Some((q, _)) if *q >= quality => {}
            _ => best = Some((quality, obs)),
        }
    }

    best.map(|(_, obs)| obs)
}

fn iter_all_stats<'a>(detail: &'a PlayerDetail) -> impl Iterator<Item = StatCandidate<'a>> + 'a {
    // season_performance has per90, and is usually the most consistent place for per-90 values.
    let perf = detail.season_performance.iter().flat_map(|g| {
        g.items.iter().map(|item| StatCandidate {
            title: item.title.as_str(),
            total: item.total.as_str(),
            per90: item.per90.as_deref(),
            pct_total: item.percentile_rank,
            pct_per90: item.percentile_rank_per90,
        })
    });

    let all_comp = detail.all_competitions.iter().map(|s| StatCandidate {
        title: s.title.as_str(),
        total: s.value.as_str(),
        per90: None,
        pct_total: s.percentile_rank,
        pct_per90: s.percentile_rank_per90,
    });

    let top = detail.top_stats.iter().map(|s| StatCandidate {
        title: s.title.as_str(),
        total: s.value.as_str(),
        per90: None,
        pct_total: s.percentile_rank,
        pct_per90: s.percentile_rank_per90,
    });

    let main = detail.main_league.as_ref().into_iter().flat_map(|l| {
        l.stats.iter().map(|s| StatCandidate {
            title: s.title.as_str(),
            total: s.value.as_str(),
            per90: None,
            pct_total: s.percentile_rank,
            pct_per90: s.percentile_rank_per90,
        })
    });

    let groups = detail.season_groups.iter().flat_map(|g| {
        g.items.iter().map(|s| StatCandidate {
            title: s.title.as_str(),
            total: s.value.as_str(),
            per90: None,
            pct_total: s.percentile_rank,
            pct_per90: s.percentile_rank_per90,
        })
    });

    perf.chain(all_comp).chain(top).chain(main).chain(groups)
}

fn parse_number(raw: &str) -> Option<f64> {
    let s = raw.trim();
    if s.is_empty() || s == "-" {
        return None;
    }
    // Strip common decorations.
    let cleaned: String = s
        .chars()
        .filter(|c| c.is_ascii_digit() || *c == '.' || *c == '-' || *c == ',')
        .collect();
    let cleaned = cleaned.replace(',', "");
    if cleaned.is_empty() || cleaned == "-" {
        return None;
    }
    cleaned.parse::<f64>().ok()
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
