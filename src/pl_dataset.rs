use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow};
use rusqlite::Connection;

use crate::historical_dataset;

pub const PREMIER_LEAGUE_ID: u32 = 47;

pub use crate::historical_dataset::StoredMatch;

#[derive(Debug, Clone)]
pub struct IngestSummary {
    pub db_path: PathBuf,
    pub seasons_total: usize,
    pub seasons_succeeded: usize,
    pub matches_upserted: usize,
    pub latest_utc_time: Option<String>,
    pub errors: Vec<String>,
}

pub fn default_db_path() -> Option<PathBuf> {
    crate::http_cache::app_cache_dir().map(|dir| dir.join("premier_league_matches.sqlite"))
}

pub fn open_db(path: &Path) -> Result<Connection> {
    historical_dataset::open_db(path)
}

pub fn init_schema(conn: &Connection) -> Result<()> {
    historical_dataset::init_schema(conn)
}

pub fn ingest_all_premier_league_matches(
    conn: &mut Connection,
    db_path: PathBuf,
) -> Result<IngestSummary> {
    let summary = historical_dataset::ingest_all_leagues_matches(
        conn,
        db_path.clone(),
        &[PREMIER_LEAGUE_ID],
    )?;
    let Some(league) = summary.per_league.get(&PREMIER_LEAGUE_ID) else {
        return Err(anyhow!(
            "missing premier league ingest summary for league id {}",
            PREMIER_LEAGUE_ID
        ));
    };

    Ok(IngestSummary {
        db_path,
        seasons_total: league.seasons_total,
        seasons_succeeded: league.seasons_succeeded,
        matches_upserted: league.matches_upserted,
        latest_utc_time: league.latest_utc_time.clone(),
        errors: league.errors.clone(),
    })
}

pub fn load_finished_premier_league_matches(conn: &Connection) -> Result<Vec<StoredMatch>> {
    historical_dataset::load_finished_matches(conn, PREMIER_LEAGUE_ID)
        .context("load finished premier league matches")
}
