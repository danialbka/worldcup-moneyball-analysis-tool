use std::collections::HashMap;

use crate::state::{player_detail_is_stub, PlayerDetail, RoleCategory, RoleRankingEntry, SquadPlayer, TeamAnalysis};

/// Build role rankings from cached squads + cached player details.
/// This is fast and avoids re-fetching network data.
pub fn compute_role_rankings_from_cache(
    teams: &[TeamAnalysis],
    squads: &HashMap<u32, Vec<SquadPlayer>>,
    players: &HashMap<u32, PlayerDetail>,
) -> Vec<RoleRankingEntry> {
    let team_name_map: HashMap<u32, String> = teams.iter().map(|t| (t.id, t.name.clone())).collect();
    let mut features: Vec<PlayerFeatures> = Vec::new();

    for team in teams {
        let Some(team_squad) = squads.get(&team.id) else { continue };
        for sp in team_squad {
            let Some(detail) = players.get(&sp.id) else { continue };
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
    Appearances,
    MinutesPlayed,
    Goals,
    Assists,
    Xg,
    Xa,
    Shots,
    ShotsOnTarget,
    KeyPasses,
    ChancesCreated,
    Dribbles,
    Tackles,
    Interceptions,
    Clearances,
    Blocks,
    Recoveries,
    DuelsWon,
    AerialsWon,
    Saves,
    SavePct,
    CleanSheets,
    GoalsConceded,
    Rating,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Direction {
    HigherBetter,
    LowerBetter,
}

#[derive(Debug, Clone)]
struct PlayerFeatures {
    pub role: RoleCategory,
    pub player_id: u32,
    pub player_name: String,
    pub team_id: u32,
    pub team_name: String,
    pub club: String,
    pub stats: HashMap<CanonStat, f64>,
    pub rating: Option<f64>,
}

fn build_player_features(
    team: &TeamAnalysis,
    team_name_map: &HashMap<u32, String>,
    squad_player: &SquadPlayer,
    detail: &PlayerDetail,
) -> Option<PlayerFeatures> {
    let role = role_category_from_text(&squad_player.role)?;
    let (stats, rating) = collect_stat_features(detail);
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
    if s.contains("defender") || s.contains("back") || s.contains("centre-back") || s.contains("center-back") {
        return Some(RoleCategory::Defender);
    }
    if s.contains("midfield") || s.contains("midfielder") {
        return Some(RoleCategory::Midfielder);
    }
    if s.contains("attacker") || s.contains("forward") || s.contains("striker") || s.contains("wing") {
        return Some(RoleCategory::Attacker);
    }
    // FotMob sometimes uses short group titles like "Midfielders" etc; above handles most.
    None
}

/// Build a "collection" of attacking/defending stats (not a hand-tuned weighted sum).
/// We prefer per-90 values when available.
fn collect_stat_features(detail: &PlayerDetail) -> (HashMap<CanonStat, f64>, Option<f64>) {
    let mut out: HashMap<CanonStat, f64> = HashMap::new();

    // Participation / sample size.
    insert_stat(
        &mut out,
        CanonStat::Appearances,
        detail,
        &["appearances", "matches played", "apps"],
        &[],
        Prefer::Per90OrTotal,
    );
    insert_stat(
        &mut out,
        CanonStat::MinutesPlayed,
        detail,
        &["minutes played", "minutes"],
        &[],
        Prefer::Per90OrTotal,
    );

    // Rating (used as extra signal + display).
    let rating = find_stat_value(detail, &["rating"], &[], Prefer::Per90OrTotal)
        .or_else(|| {
            detail
                .season_breakdown
                .first()
                .and_then(|row| parse_number(&row.rating))
        });
    if let Some(r) = rating {
        out.insert(CanonStat::Rating, r);
    }

    insert_stat(&mut out, CanonStat::Goals, detail, &["goals"], &["goals conceded"], Prefer::Per90OrTotal);
    insert_stat(&mut out, CanonStat::Assists, detail, &["assists"], &[], Prefer::Per90OrTotal);
    insert_stat(&mut out, CanonStat::Xg, detail, &["expected goals", "xg"], &[], Prefer::Per90OrTotal);
    insert_stat(&mut out, CanonStat::Xa, detail, &["expected assists", "xa"], &[], Prefer::Per90OrTotal);
    insert_stat(&mut out, CanonStat::ShotsOnTarget, detail, &["shots on target"], &[], Prefer::Per90OrTotal);
    insert_stat(&mut out, CanonStat::Shots, detail, &["shots"], &["shots on target"], Prefer::Per90OrTotal);
    insert_stat(&mut out, CanonStat::KeyPasses, detail, &["key passes"], &[], Prefer::Per90OrTotal);
    insert_stat(&mut out, CanonStat::ChancesCreated, detail, &["chances created"], &[], Prefer::Per90OrTotal);
    insert_stat(&mut out, CanonStat::Dribbles, detail, &["dribbles"], &[], Prefer::Per90OrTotal);

    insert_stat(&mut out, CanonStat::Tackles, detail, &["tackles"], &[], Prefer::Per90OrTotal);
    insert_stat(&mut out, CanonStat::Interceptions, detail, &["interceptions"], &[], Prefer::Per90OrTotal);
    insert_stat(&mut out, CanonStat::Clearances, detail, &["clearances"], &[], Prefer::Per90OrTotal);
    insert_stat(&mut out, CanonStat::Blocks, detail, &["blocks"], &[], Prefer::Per90OrTotal);
    insert_stat(&mut out, CanonStat::Recoveries, detail, &["recoveries"], &[], Prefer::Per90OrTotal);
    insert_stat(&mut out, CanonStat::DuelsWon, detail, &["duels won"], &[], Prefer::Per90OrTotal);
    insert_stat(&mut out, CanonStat::AerialsWon, detail, &["aerial duels won", "aerial won", "aerials won"], &[], Prefer::Per90OrTotal);

    // GK-ish.
    insert_stat(&mut out, CanonStat::Saves, detail, &["saves"], &[], Prefer::Per90OrTotal);
    insert_stat_percent(&mut out, CanonStat::SavePct, detail, &["save%", "save %", "save percentage"], &[]);
    insert_stat(&mut out, CanonStat::CleanSheets, detail, &["clean sheets"], &[], Prefer::Per90OrTotal);
    insert_stat(&mut out, CanonStat::GoalsConceded, detail, &["goals conceded"], &[], Prefer::Per90OrTotal);

    (out, rating)
}

fn build_rankings_from_features(features: &[PlayerFeatures]) -> Vec<RoleRankingEntry> {
    let attack_specs: &[(CanonStat, Direction)] = &[
        (CanonStat::Goals, Direction::HigherBetter),
        (CanonStat::Assists, Direction::HigherBetter),
        (CanonStat::Xg, Direction::HigherBetter),
        (CanonStat::Xa, Direction::HigherBetter),
        (CanonStat::ShotsOnTarget, Direction::HigherBetter),
        (CanonStat::Shots, Direction::HigherBetter),
        (CanonStat::KeyPasses, Direction::HigherBetter),
        (CanonStat::ChancesCreated, Direction::HigherBetter),
        (CanonStat::Dribbles, Direction::HigherBetter),
        (CanonStat::Rating, Direction::HigherBetter),
    ];

    let defend_specs: &[(CanonStat, Direction)] = &[
        (CanonStat::Tackles, Direction::HigherBetter),
        (CanonStat::Interceptions, Direction::HigherBetter),
        (CanonStat::Clearances, Direction::HigherBetter),
        (CanonStat::Blocks, Direction::HigherBetter),
        (CanonStat::Recoveries, Direction::HigherBetter),
        (CanonStat::DuelsWon, Direction::HigherBetter),
        (CanonStat::AerialsWon, Direction::HigherBetter),
        // Goalkeeper-relevant defensive set (still fine for outfield when missing).
        (CanonStat::Saves, Direction::HigherBetter),
        (CanonStat::SavePct, Direction::HigherBetter),
        (CanonStat::CleanSheets, Direction::HigherBetter),
        (CanonStat::GoalsConceded, Direction::LowerBetter),
        (CanonStat::Rating, Direction::HigherBetter),
    ];

    // Precompute per-role mean+stddev per stat (after applying direction transform).
    let mut dist_attack: HashMap<(RoleCategory, CanonStat), (f64, f64)> = HashMap::new();
    let mut dist_defend: HashMap<(RoleCategory, CanonStat), (f64, f64)> = HashMap::new();

    for role in [
        RoleCategory::Goalkeeper,
        RoleCategory::Defender,
        RoleCategory::Midfielder,
        RoleCategory::Attacker,
    ] {
        for (stat, dir) in attack_specs {
            if let Some(d) = dist_for_role(features, role, *stat, *dir) {
                dist_attack.insert((role, *stat), d);
            }
        }
        for (stat, dir) in defend_specs {
            if let Some(d) = dist_for_role(features, role, *stat, *dir) {
                dist_defend.insert((role, *stat), d);
            }
        }
    }

    features
        .iter()
        .map(|f| {
            let attack_score = composite_zscore(
                f,
                attack_specs,
                &dist_attack,
            );
            let defense_score = composite_zscore(
                f,
                defend_specs,
                &dist_defend,
            );
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
            }
        })
        .collect()
}

fn dist_for_role(
    features: &[PlayerFeatures],
    role: RoleCategory,
    stat: CanonStat,
    dir: Direction,
) -> Option<(f64, f64)> {
    let mut values: Vec<f64> = Vec::new();
    for f in features.iter().filter(|f| f.role == role) {
        if let Some(v) = f.stats.get(&stat).copied() {
            values.push(apply_dir(v, dir));
        }
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
    if std <= 1e-9 {
        None
    } else {
        Some((mean, std))
    }
}

fn composite_zscore(
    f: &PlayerFeatures,
    specs: &[(CanonStat, Direction)],
    dist: &HashMap<(RoleCategory, CanonStat), (f64, f64)>,
) -> f64 {
    let mut sum = 0.0;
    let mut n = 0usize;
    for (stat, dir) in specs {
        let Some(v) = f.stats.get(stat).copied() else {
            continue;
        };
        let Some((mean, std)) = dist.get(&(f.role, *stat)).copied() else {
            continue;
        };
        let v = apply_dir(v, *dir);
        let z = (v - mean) / std;
        if z.is_finite() {
            sum += z;
            n += 1;
        }
    }
    if n == 0 {
        f64::NEG_INFINITY
    } else {
        let base = sum / n as f64;
        apply_participation_adjustment(f, base)
    }
}

fn apply_participation_adjustment(f: &PlayerFeatures, base: f64) -> f64 {
    // Penalize small sample sizes so 0–1 match players don't top lists.
    // Prefer minutes if present; otherwise use appearances.
    //
    // reliability ~ 0 (no minutes) → heavy penalty
    // reliability ~ 1 (enough minutes) → no penalty
    const FULL_MINUTES: f64 = 900.0; // ~10 full matches
    const FULL_APPS: f64 = 10.0;
    const PENALTY: f64 = 1.5; // in "z-score units"

    let minutes = f.stats.get(&CanonStat::MinutesPlayed).copied().unwrap_or(0.0);
    let apps = f.stats.get(&CanonStat::Appearances).copied().unwrap_or(0.0);

    let rel = if minutes > 0.0 {
        // sqrt gives a softer ramp; 1 match (~90) => ~0.316
        (minutes / FULL_MINUTES).clamp(0.0, 1.0).sqrt()
    } else if apps > 0.0 {
        (apps / FULL_APPS).clamp(0.0, 1.0).sqrt()
    } else {
        0.0
    };

    if !base.is_finite() {
        return base;
    }

    // Shrink towards 0 and apply a penalty for low participation.
    // This ensures low-minute players can't beat high-minute players just from variance.
    base * rel - (1.0 - rel) * PENALTY
}

fn apply_dir(v: f64, dir: Direction) -> f64 {
    match dir {
        Direction::HigherBetter => v,
        Direction::LowerBetter => -v,
    }
}

#[derive(Debug, Clone, Copy)]
enum Prefer {
    Per90OrTotal,
}

fn insert_stat(
    out: &mut HashMap<CanonStat, f64>,
    key: CanonStat,
    detail: &PlayerDetail,
    needles: &[&str],
    excludes: &[&str],
    _prefer: Prefer,
) {
    if let Some(v) = find_stat_value(detail, needles, excludes, Prefer::Per90OrTotal) {
        out.insert(key, v);
    }
}

fn insert_stat_percent(
    out: &mut HashMap<CanonStat, f64>,
    key: CanonStat,
    detail: &PlayerDetail,
    needles: &[&str],
    excludes: &[&str],
) {
    if let Some(v) = find_stat_percent(detail, needles, excludes) {
        out.insert(key, v);
    }
}

fn find_stat_value(detail: &PlayerDetail, needles: &[&str], excludes: &[&str], _prefer: Prefer) -> Option<f64> {
    let needles: Vec<String> = needles.iter().map(|s| s.to_lowercase()).collect();
    let excludes: Vec<String> = excludes.iter().map(|s| s.to_lowercase()).collect();

    // Prefer per-90 if available (season_performance has per90).
    for (title, total, per90) in iter_all_stats(detail) {
        let t = title.to_lowercase();
        if !needles.iter().any(|n| t.contains(n)) {
            continue;
        }
        if excludes.iter().any(|e| t.contains(e)) {
            continue;
        }
        if let Some(per90) = per90 {
            if let Some(v) = parse_number(per90) {
                return Some(v);
            }
        }
        if let Some(v) = parse_number(total) {
            return Some(v);
        }
    }
    None
}

fn find_stat_percent(detail: &PlayerDetail, needles: &[&str], excludes: &[&str]) -> Option<f64> {
    let needles: Vec<String> = needles.iter().map(|s| s.to_lowercase()).collect();
    let excludes: Vec<String> = excludes.iter().map(|s| s.to_lowercase()).collect();
    for (title, total, per90) in iter_all_stats(detail) {
        let t = title.to_lowercase();
        if !needles.iter().any(|n| t.contains(n)) {
            continue;
        }
        if excludes.iter().any(|e| t.contains(e)) {
            continue;
        }
        if let Some(per90) = per90 {
            if let Some(v) = parse_percent(per90) {
                return Some(v);
            }
        }
        if let Some(v) = parse_percent(total) {
            return Some(v);
        }
    }
    None
}

fn iter_all_stats<'a>(
    detail: &'a PlayerDetail,
) -> impl Iterator<Item = (&'a str, &'a str, Option<&'a str>)> + 'a {
    // season_performance has per90, and is usually the most consistent place for per-90 values.
    let perf = detail.season_performance.iter().flat_map(|g| {
        g.items.iter().map(|item| {
            (
                item.title.as_str(),
                item.total.as_str(),
                item.per90.as_deref(),
            )
        })
    });
    let all_comp = detail
        .all_competitions
        .iter()
        .map(|s| (s.title.as_str(), s.value.as_str(), None));
    let top = detail
        .top_stats
        .iter()
        .map(|s| (s.title.as_str(), s.value.as_str(), None));
    let main = detail
        .main_league
        .as_ref()
        .into_iter()
        .flat_map(|l| l.stats.iter().map(|s| (s.title.as_str(), s.value.as_str(), None)));
    let groups = detail
        .season_groups
        .iter()
        .flat_map(|g| g.items.iter().map(|s| (s.title.as_str(), s.value.as_str(), None)));

    // Prefer perf first, then the rest.
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

fn parse_percent(raw: &str) -> Option<f64> {
    let s = raw.trim();
    if s.is_empty() || s == "-" {
        return None;
    }
    let s = s.trim_end_matches('%');
    parse_number(s)
}

