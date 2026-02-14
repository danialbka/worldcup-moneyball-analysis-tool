use std::collections::HashSet;
use std::path::PathBuf;

use anyhow::{Context, Result, anyhow};

use wc26_terminal::historical_dataset;

const DEFAULT_LEAGUE_IDS: &[u32] = &[47, 87, 54, 55, 53, 42, 77];

fn main() -> Result<()> {
    let league_ids = parse_league_ids_arg().unwrap_or_else(default_league_ids_from_env);
    if league_ids.is_empty() {
        return Err(anyhow!("no league ids resolved for ingest"));
    }

    let db_path = parse_db_path_arg()
        .or_else(historical_dataset::default_db_path)
        .context("unable to resolve sqlite path")?;

    let mut conn = historical_dataset::open_db(&db_path)?;
    let summary =
        historical_dataset::ingest_all_leagues_matches(&mut conn, db_path.clone(), &league_ids)?;

    println!("Historical ingest complete");
    println!("DB: {}", summary.db_path.display());
    println!("Leagues: {:?}", summary.league_ids);
    println!(
        "Seasons: {}/{}",
        summary.seasons_succeeded, summary.seasons_total
    );
    println!("Matches upserted: {}", summary.matches_upserted);

    let mut league_keys = summary.per_league.keys().copied().collect::<Vec<_>>();
    league_keys.sort_unstable();
    for league_id in league_keys {
        let Some(item) = summary.per_league.get(&league_id) else {
            continue;
        };
        println!(
            "league {}: seasons {}/{} matches={} latest={}",
            league_id,
            item.seasons_succeeded,
            item.seasons_total,
            item.matches_upserted,
            item.latest_utc_time.as_deref().unwrap_or("n/a")
        );
        if !item.errors.is_empty() {
            println!("  errors: {}", item.errors.len());
            for err in item.errors.iter().take(6) {
                println!("   - {err}");
            }
        }
    }

    Ok(())
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
