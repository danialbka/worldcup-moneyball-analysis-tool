use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow};
use chrono::Utc;
use rusqlite::{Connection, params};
use serde_json::Value;

use crate::http_cache::{app_cache_dir, fetch_json_cached};
use crate::http_client::http_client;
use crate::team_fixtures::FixtureMatch;

const FOTMOB_LEAGUE_URL: &str = "https://www.fotmob.com/api/leagues";

#[derive(Debug, Clone)]
pub struct StoredMatch {
    pub match_id: u64,
    pub season: String,
    pub league_id: u32,
    pub round: Option<i64>,
    pub utc_time: String,
    pub home_team_id: u32,
    pub away_team_id: u32,
    pub home_team: String,
    pub away_team: String,
    pub home_goals: Option<i32>,
    pub away_goals: Option<i32>,
    pub started: bool,
    pub finished: bool,
    pub cancelled: bool,
    pub awarded: bool,
    pub status_reason_key: Option<String>,
    pub score_str: Option<String>,
}

impl StoredMatch {
    pub fn outcome(&self) -> Option<char> {
        let (Some(home_goals), Some(away_goals)) = (self.home_goals, self.away_goals) else {
            return None;
        };
        if !self.finished || self.cancelled || self.awarded {
            return None;
        }
        if home_goals > away_goals {
            Some('H')
        } else if home_goals < away_goals {
            Some('A')
        } else {
            Some('D')
        }
    }

    pub fn is_penalty_decided(&self) -> bool {
        self.status_reason_key
            .as_deref()
            .is_some_and(|s| s.to_ascii_lowercase().contains("pen"))
    }

    pub fn as_fixture_match(&self) -> Option<FixtureMatch> {
        let (Some(home_goals), Some(away_goals)) = (self.home_goals, self.away_goals) else {
            return None;
        };
        Some(FixtureMatch {
            id: u32::try_from(self.match_id).ok()?,
            utc_time: self.utc_time.clone(),
            league_id: self.league_id,
            home_id: self.home_team_id,
            away_id: self.away_team_id,
            home_goals: u8::try_from(home_goals).ok()?,
            away_goals: u8::try_from(away_goals).ok()?,
            finished: self.finished,
            cancelled: self.cancelled,
            awarded: self.awarded,
            reason_long_key: self.status_reason_key.clone(),
        })
    }
}

#[derive(Debug, Clone)]
pub struct LeagueIngestSummary {
    pub league_id: u32,
    pub seasons_total: usize,
    pub seasons_succeeded: usize,
    pub matches_upserted: usize,
    pub latest_utc_time: Option<String>,
    pub errors: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct IngestSummary {
    pub db_path: PathBuf,
    pub league_ids: Vec<u32>,
    pub seasons_total: usize,
    pub seasons_succeeded: usize,
    pub matches_upserted: usize,
    pub per_league: HashMap<u32, LeagueIngestSummary>,
}

pub fn default_db_path() -> Option<PathBuf> {
    app_cache_dir().map(|dir| dir.join("historical_matches.sqlite"))
}

pub fn open_db(path: &Path) -> Result<Connection> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).ok();
    }
    let conn =
        Connection::open(path).with_context(|| format!("open sqlite db {}", path.display()))?;
    init_schema(&conn)?;
    Ok(conn)
}

pub fn init_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
        PRAGMA journal_mode = WAL;
        CREATE TABLE IF NOT EXISTS matches (
            match_id INTEGER PRIMARY KEY,
            season TEXT NOT NULL,
            league_id INTEGER NOT NULL,
            round INTEGER NULL,
            utc_time TEXT NOT NULL,
            home_team_id INTEGER NOT NULL,
            away_team_id INTEGER NOT NULL,
            home_team TEXT NOT NULL,
            away_team TEXT NOT NULL,
            home_goals INTEGER NULL,
            away_goals INTEGER NULL,
            started INTEGER NOT NULL,
            finished INTEGER NOT NULL,
            cancelled INTEGER NOT NULL,
            awarded INTEGER NOT NULL,
            status_reason_key TEXT NULL,
            score_str TEXT NULL,
            outcome TEXT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_matches_league ON matches(league_id);
        CREATE INDEX IF NOT EXISTS idx_matches_season ON matches(season);
        CREATE INDEX IF NOT EXISTS idx_matches_utc_time ON matches(utc_time);
        CREATE INDEX IF NOT EXISTS idx_matches_outcome ON matches(outcome);

        CREATE TABLE IF NOT EXISTS ingest_runs (
            run_id INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at TEXT NOT NULL,
            finished_at TEXT NULL,
            league_id INTEGER NOT NULL,
            seasons_total INTEGER NOT NULL,
            seasons_succeeded INTEGER NOT NULL,
            matches_upserted INTEGER NOT NULL,
            errors_json TEXT NOT NULL
        );
        "#,
    )
    .context("create sqlite schema")?;
    Ok(())
}

pub fn ingest_all_leagues_matches(
    conn: &mut Connection,
    db_path: PathBuf,
    league_ids: &[u32],
) -> Result<IngestSummary> {
    if league_ids.is_empty() {
        return Err(anyhow!("no league ids passed to ingest"));
    }

    let mut unique = HashSet::new();
    let mut leagues = Vec::new();
    for id in league_ids {
        if *id != 0 && unique.insert(*id) {
            leagues.push(*id);
        }
    }
    if leagues.is_empty() {
        return Err(anyhow!("no valid league ids after dedup"));
    }

    let client = http_client()?;
    let mut per_league = HashMap::new();

    let mut seasons_total = 0usize;
    let mut seasons_succeeded = 0usize;
    let mut matches_upserted = 0usize;

    for league_id in &leagues {
        let summary = ingest_single_league(conn, client, *league_id)?;
        seasons_total += summary.seasons_total;
        seasons_succeeded += summary.seasons_succeeded;
        matches_upserted += summary.matches_upserted;
        per_league.insert(*league_id, summary);
    }

    Ok(IngestSummary {
        db_path,
        league_ids: leagues,
        seasons_total,
        seasons_succeeded,
        matches_upserted,
        per_league,
    })
}

fn ingest_single_league(
    conn: &mut Connection,
    client: &reqwest::blocking::Client,
    league_id: u32,
) -> Result<LeagueIngestSummary> {
    let seasons = fetch_available_seasons(client, league_id)?;
    if seasons.is_empty() {
        return Err(anyhow!(
            "no seasons available from FotMob league endpoint (league_id={league_id})"
        ));
    }

    let started_at = Utc::now().to_rfc3339();
    conn.execute(
        "INSERT INTO ingest_runs(started_at, finished_at, league_id, seasons_total, seasons_succeeded, matches_upserted, errors_json)
         VALUES (?1, NULL, ?2, ?3, 0, 0, '[]')",
        params![started_at, league_id as i64, seasons.len() as i64],
    )
    .context("insert ingest run")?;
    let run_id = conn.last_insert_rowid();

    let mut seasons_succeeded = 0usize;
    let mut matches_upserted = 0usize;
    let mut errors: Vec<String> = Vec::new();

    for season in &seasons {
        match fetch_season_matches(client, league_id, season) {
            Ok(rows) => {
                let tx = conn.transaction().context("begin ingest transaction")?;
                for row in &rows {
                    upsert_match(&tx, row)?;
                    matches_upserted += 1;
                }
                tx.commit().context("commit ingest transaction")?;
                seasons_succeeded += 1;
            }
            Err(err) => {
                errors.push(format!("season {season}: {err}"));
            }
        }
    }

    let finished_at = Utc::now().to_rfc3339();
    let errors_json = serde_json::to_string(&errors).unwrap_or_else(|_| "[]".to_string());
    conn.execute(
        "UPDATE ingest_runs
         SET finished_at = ?1, seasons_succeeded = ?2, matches_upserted = ?3, errors_json = ?4
         WHERE run_id = ?5",
        params![
            finished_at,
            seasons_succeeded as i64,
            matches_upserted as i64,
            errors_json,
            run_id
        ],
    )
    .context("update ingest run")?;

    let latest_utc_time = conn
        .query_row(
            "SELECT MAX(utc_time) FROM matches WHERE league_id = ?1",
            params![league_id as i64],
            |row| row.get::<_, Option<String>>(0),
        )
        .context("query latest utc_time")?;

    Ok(LeagueIngestSummary {
        league_id,
        seasons_total: seasons.len(),
        seasons_succeeded,
        matches_upserted,
        latest_utc_time,
        errors,
    })
}

pub fn load_finished_matches(conn: &Connection, league_id: u32) -> Result<Vec<StoredMatch>> {
    let mut stmt = conn
        .prepare(
            r#"
            SELECT
                match_id, season, league_id, round, utc_time,
                home_team_id, away_team_id, home_team, away_team,
                home_goals, away_goals, started, finished, cancelled, awarded,
                status_reason_key, score_str
            FROM matches
            WHERE league_id = ?1
              AND finished = 1
              AND cancelled = 0
              AND awarded = 0
              AND home_goals IS NOT NULL
              AND away_goals IS NOT NULL
            ORDER BY utc_time ASC, match_id ASC
            "#,
        )
        .context("prepare load matches query")?;

    let rows = stmt
        .query_map(params![league_id as i64], |row| {
            Ok(StoredMatch {
                match_id: row.get::<_, u64>(0)?,
                season: row.get(1)?,
                league_id: row.get::<_, u32>(2)?,
                round: row.get(3)?,
                utc_time: row.get(4)?,
                home_team_id: row.get::<_, u32>(5)?,
                away_team_id: row.get::<_, u32>(6)?,
                home_team: row.get(7)?,
                away_team: row.get(8)?,
                home_goals: row.get(9)?,
                away_goals: row.get(10)?,
                started: row.get::<_, i64>(11)? != 0,
                finished: row.get::<_, i64>(12)? != 0,
                cancelled: row.get::<_, i64>(13)? != 0,
                awarded: row.get::<_, i64>(14)? != 0,
                status_reason_key: row.get(15)?,
                score_str: row.get(16)?,
            })
        })
        .context("query load matches")?;

    let mut out = Vec::new();
    for row in rows {
        out.push(row.context("decode match row")?);
    }
    Ok(out)
}

fn upsert_match(tx: &rusqlite::Transaction<'_>, m: &StoredMatch) -> Result<()> {
    tx.execute(
        r#"
        INSERT INTO matches (
            match_id, season, league_id, round, utc_time,
            home_team_id, away_team_id, home_team, away_team,
            home_goals, away_goals, started, finished, cancelled, awarded,
            status_reason_key, score_str, outcome, updated_at
        ) VALUES (
            ?1, ?2, ?3, ?4, ?5,
            ?6, ?7, ?8, ?9,
            ?10, ?11, ?12, ?13, ?14, ?15,
            ?16, ?17, ?18, ?19
        )
        ON CONFLICT(match_id) DO UPDATE SET
            season = excluded.season,
            league_id = excluded.league_id,
            round = excluded.round,
            utc_time = excluded.utc_time,
            home_team_id = excluded.home_team_id,
            away_team_id = excluded.away_team_id,
            home_team = excluded.home_team,
            away_team = excluded.away_team,
            home_goals = excluded.home_goals,
            away_goals = excluded.away_goals,
            started = excluded.started,
            finished = excluded.finished,
            cancelled = excluded.cancelled,
            awarded = excluded.awarded,
            status_reason_key = excluded.status_reason_key,
            score_str = excluded.score_str,
            outcome = excluded.outcome,
            updated_at = excluded.updated_at
        "#,
        params![
            m.match_id as i64,
            m.season,
            m.league_id as i64,
            m.round,
            m.utc_time,
            m.home_team_id as i64,
            m.away_team_id as i64,
            m.home_team,
            m.away_team,
            m.home_goals,
            m.away_goals,
            bool_to_i64(m.started),
            bool_to_i64(m.finished),
            bool_to_i64(m.cancelled),
            bool_to_i64(m.awarded),
            m.status_reason_key,
            m.score_str,
            m.outcome().map(|c| c.to_string()),
            Utc::now().to_rfc3339(),
        ],
    )
    .context("upsert match")?;
    Ok(())
}

fn fetch_available_seasons(
    client: &reqwest::blocking::Client,
    league_id: u32,
) -> Result<Vec<String>> {
    let value = fetch_league_payload(client, league_id, None)?;
    let mut seasons = value
        .get("allAvailableSeasons")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    if seasons.is_empty()
        && let Some(current) = value
            .get("details")
            .and_then(|d| d.get("selectedSeason"))
            .and_then(|v| v.as_str())
    {
        seasons.push(current.to_string());
    }
    Ok(seasons)
}

fn fetch_season_matches(
    client: &reqwest::blocking::Client,
    league_id: u32,
    season: &str,
) -> Result<Vec<StoredMatch>> {
    let value = fetch_league_payload(client, league_id, Some(season))?;
    let matches = value
        .get("fixtures")
        .and_then(|v| v.get("allMatches"))
        .and_then(|v| v.as_array())
        .ok_or_else(|| {
            anyhow!("missing fixtures.allMatches for league {league_id} season {season}")
        })?;

    let mut out = Vec::with_capacity(matches.len());
    for m in matches {
        if let Some(row) = parse_stored_match(m, season, league_id) {
            out.push(row);
        }
    }
    Ok(out)
}

fn fetch_league_payload(
    client: &reqwest::blocking::Client,
    league_id: u32,
    season: Option<&str>,
) -> Result<Value> {
    let url = league_fixtures_url(league_id, season);
    let body = fetch_json_cached(client, &url, &[]).context("fetch league fixtures failed")?;
    serde_json::from_str::<Value>(body.trim()).context("invalid league fixtures json")
}

fn league_fixtures_url(league_id: u32, season: Option<&str>) -> String {
    let mut url =
        format!("{FOTMOB_LEAGUE_URL}?id={league_id}&tab=fixtures&type=league&timeZone=UTC");
    if let Some(season) = season {
        let encoded = season.replace('/', "%2F");
        url.push_str("&season=");
        url.push_str(&encoded);
    }
    url
}

fn parse_stored_match(v: &Value, season: &str, fallback_league_id: u32) -> Option<StoredMatch> {
    let match_id = as_u64_any(v.get("id")?)?;
    let league_id = as_u32_any(
        v.get("leagueId")
            .unwrap_or(&Value::from(fallback_league_id)),
    )
    .unwrap_or(fallback_league_id);
    let round = v
        .get("round")
        .and_then(as_i64_any)
        .or_else(|| v.get("roundName").and_then(as_i64_any));
    let status = v.get("status")?;
    let utc_time = status.get("utcTime")?.as_str()?.to_string();

    let home = v.get("home")?;
    let away = v.get("away")?;
    let home_team_id = as_u32_any(home.get("id")?)?;
    let away_team_id = as_u32_any(away.get("id")?)?;
    let home_team = home
        .get("longName")
        .and_then(|x| x.as_str())
        .or_else(|| home.get("name").and_then(|x| x.as_str()))
        .unwrap_or_default()
        .to_string();
    let away_team = away
        .get("longName")
        .and_then(|x| x.as_str())
        .or_else(|| away.get("name").and_then(|x| x.as_str()))
        .unwrap_or_default()
        .to_string();
    if home_team.is_empty() || away_team.is_empty() {
        return None;
    }

    let mut home_goals = home.get("score").and_then(as_i32_any);
    let mut away_goals = away.get("score").and_then(as_i32_any);
    let started = status
        .get("started")
        .and_then(|x| x.as_bool())
        .unwrap_or(false);
    let finished = status
        .get("finished")
        .and_then(|x| x.as_bool())
        .unwrap_or(false);
    let cancelled = status
        .get("cancelled")
        .and_then(|x| x.as_bool())
        .unwrap_or(false);
    let awarded = status
        .get("awarded")
        .and_then(|x| x.as_bool())
        .unwrap_or(false);
    let status_reason_key = status
        .get("reason")
        .and_then(|r| r.get("longKey"))
        .and_then(|x| x.as_str())
        .map(|s| s.to_string());
    let score_str = status
        .get("scoreStr")
        .and_then(|x| x.as_str())
        .map(|s| s.to_string());
    if (home_goals.is_none() || away_goals.is_none())
        && let Some((h, a)) = score_str.as_deref().and_then(parse_score_pair)
    {
        home_goals = home_goals.or(Some(h));
        away_goals = away_goals.or(Some(a));
    }

    Some(StoredMatch {
        match_id,
        season: season.to_string(),
        league_id,
        round,
        utc_time,
        home_team_id,
        away_team_id,
        home_team,
        away_team,
        home_goals,
        away_goals,
        started,
        finished,
        cancelled,
        awarded,
        status_reason_key,
        score_str,
    })
}

fn as_u64_any(v: &Value) -> Option<u64> {
    if let Some(n) = v.as_u64() {
        return Some(n);
    }
    v.as_str()?.trim().parse::<u64>().ok()
}

fn as_u32_any(v: &Value) -> Option<u32> {
    let n = as_u64_any(v)?;
    u32::try_from(n).ok()
}

fn as_i64_any(v: &Value) -> Option<i64> {
    if let Some(n) = v.as_i64() {
        return Some(n);
    }
    v.as_str()?.trim().parse::<i64>().ok()
}

fn as_i32_any(v: &Value) -> Option<i32> {
    let n = as_i64_any(v)?;
    i32::try_from(n).ok()
}

fn bool_to_i64(v: bool) -> i64 {
    if v { 1 } else { 0 }
}

fn parse_score_pair(raw: &str) -> Option<(i32, i32)> {
    let mut nums = raw
        .split(|ch: char| !ch.is_ascii_digit())
        .filter(|s| !s.is_empty())
        .filter_map(|s| s.parse::<i32>().ok());
    let home = nums.next()?;
    let away = nums.next()?;
    Some((home, away))
}

#[cfg(test)]
mod tests {
    use super::parse_score_pair;

    #[test]
    fn parse_score_pair_works() {
        assert_eq!(parse_score_pair("2-1"), Some((2, 1)));
        assert_eq!(parse_score_pair("FT 0 : 0"), Some((0, 0)));
        assert_eq!(parse_score_pair("ab"), None);
    }
}
