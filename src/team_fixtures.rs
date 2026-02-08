use anyhow::{Context, Result};
use serde_json::Value;

use crate::http_cache::{fetch_json_cached, fetch_json_cached_revalidate};
use crate::http_client::http_client;

const FOTMOB_TEAM_URL: &str = "https://www.fotmob.com/api/teams?id=";
const FOTMOB_API_BASE: &str = "https://www.fotmob.com/api";

#[derive(Debug, Clone)]
pub struct FixtureMatch {
    pub id: u32,
    pub utc_time: String,
    pub league_id: u32,
    pub home_id: u32,
    pub away_id: u32,
    pub home_goals: u8,
    pub away_goals: u8,
    pub finished: bool,
    pub cancelled: bool,
    pub awarded: bool,
    pub reason_long_key: Option<String>,
}

impl FixtureMatch {
    pub fn is_penalty_decided(&self) -> bool {
        let Some(key) = self.reason_long_key.as_deref() else {
            return false;
        };
        // Observed: "afterpenalties", also sometimes keys include "pen".
        let k = key.to_ascii_lowercase();
        k.contains("pen")
    }
}

pub fn collect_team_fixtures(
    team_id: u32,
    max_pages: u8,
    revalidate: bool,
) -> Result<Vec<FixtureMatch>> {
    let client = http_client()?;
    let url = format!("{FOTMOB_TEAM_URL}{team_id}");
    let body = if revalidate {
        fetch_json_cached_revalidate(client, &url, &[]).context("team fixtures request failed")?
    } else {
        fetch_json_cached(client, &url, &[]).context("team fixtures request failed")?
    };
    let trimmed = body.trim();
    if trimmed.is_empty() || trimmed == "null" {
        return Err(anyhow::anyhow!("empty team response"));
    }
    let v: Value = serde_json::from_str(trimmed).context("invalid team json")?;

    let mut out = Vec::new();
    if let Some(arr) = v
        .get("fixtures")
        .and_then(|x| x.get("allFixtures"))
        .and_then(|x| x.get("fixtures"))
        .and_then(|x| x.as_array())
    {
        for item in arr {
            if let Some(m) = parse_fixture_match(item) {
                out.push(m);
            }
        }
    }

    let mut prev = v
        .get("fixtures")
        .and_then(|x| x.get("previousFixturesUrl"))
        .and_then(|x| x.as_str())
        .map(|s| s.to_string());

    let mut pages = 0u8;
    while let Some(next) = prev.take() {
        if pages >= max_pages {
            break;
        }
        pages = pages.saturating_add(1);
        let url = if next.starts_with("http") {
            next
        } else {
            format!("{FOTMOB_API_BASE}{next}")
        };
        let body = if revalidate {
            fetch_json_cached_revalidate(client, &url, &[])
                .context("pageable fixtures request failed")?
        } else {
            fetch_json_cached(client, &url, &[]).context("pageable fixtures request failed")?
        };
        let trimmed = body.trim();
        if trimmed.is_empty() || trimmed == "null" {
            break;
        }
        let page: Value =
            serde_json::from_str(trimmed).context("invalid pageable fixtures json")?;

        if let Some(arr) = page.get("matches").and_then(|x| x.as_array()) {
            for item in arr {
                if let Some(m) = parse_fixture_match(item) {
                    out.push(m);
                }
            }
        }
        prev = page
            .get("previous")
            .and_then(|x| x.as_str())
            .map(|s| s.to_string());
    }

    // Dedup by match id (we'll merge across teams later too, but this helps early).
    out.sort_by_key(|m| m.id);
    out.dedup_by_key(|m| m.id);
    Ok(out)
}

fn parse_fixture_match(v: &Value) -> Option<FixtureMatch> {
    let id = v.get("id")?.as_u64()? as u32;

    let league_id = v
        .get("tournament")
        .and_then(|t| t.get("leagueId"))
        .and_then(|x| x.as_u64())
        .unwrap_or(0) as u32;

    let status = v.get("status")?;
    let utc_time = status
        .get("utcTime")
        .and_then(|x| x.as_str())
        .unwrap_or_default()
        .to_string();
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
    let reason_long_key = status
        .get("reason")
        .and_then(|r| r.get("longKey"))
        .and_then(|x| x.as_str())
        .map(|s| s.to_string());

    let home = v.get("home")?;
    let away = v.get("away")?;
    let home_id = home.get("id")?.as_u64()? as u32;
    let away_id = away.get("id")?.as_u64()? as u32;
    let home_goals = home.get("score")?.as_u64()? as u8;
    let away_goals = away.get("score")?.as_u64()? as u8;

    Some(FixtureMatch {
        id,
        utc_time,
        league_id,
        home_id,
        away_id,
        home_goals,
        away_goals,
        finished,
        cancelled,
        awarded,
        reason_long_key,
    })
}
