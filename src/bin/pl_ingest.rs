use std::path::PathBuf;

use anyhow::{Context, Result};

use wc26_terminal::pl_dataset;

fn main() -> Result<()> {
    let db_path = parse_db_path_arg()
        .or_else(pl_dataset::default_db_path)
        .context("unable to resolve sqlite path")?;

    let mut conn = pl_dataset::open_db(&db_path)?;
    let summary = pl_dataset::ingest_all_premier_league_matches(&mut conn, db_path.clone())?;

    println!("Premier League ingest complete");
    println!("DB: {}", summary.db_path.display());
    println!(
        "Seasons: {}/{}",
        summary.seasons_succeeded, summary.seasons_total
    );
    println!("Matches upserted: {}", summary.matches_upserted);
    if let Some(latest) = summary.latest_utc_time {
        println!("Latest kickoff UTC: {latest}");
    }
    if !summary.errors.is_empty() {
        println!("Errors: {}", summary.errors.len());
        for err in summary.errors.iter().take(8) {
            println!(" - {err}");
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
