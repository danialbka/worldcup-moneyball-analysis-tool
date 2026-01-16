use anyhow::{Context, Result};
use serde::Deserialize;
use serde_json::Value;

use crate::http_cache::fetch_json_cached;
use crate::http_client::http_client;
use crate::state::{
    Event, EventKind, LineupSide, MatchDetail, MatchLineups, PlayerSlot, StatRow, UpcomingMatch,
};

const FOTMOB_MATCHES_URL: &str = "https://www.fotmob.com/api/data/matches";

#[derive(Debug, Clone)]
pub struct FotmobMatchRow {
    pub id: String,
    pub league_id: u32,
    pub league_name: String,
    pub home: String,
    pub away: String,
    pub home_score: u8,
    pub away_score: u8,
    pub started: bool,
    pub finished: bool,
    pub cancelled: bool,
}

pub fn fetch_upcoming_from_fotmob(date: Option<&str>) -> Result<Vec<UpcomingMatch>> {
    let data = fetch_fotmob_response(date)?;
    Ok(build_upcoming_from_response(data))
}

pub fn fetch_matches_from_fotmob(date: Option<&str>) -> Result<Vec<FotmobMatchRow>> {
    let data = fetch_fotmob_response(date)?;
    Ok(build_matches_from_response(data))
}

pub fn fetch_match_details_from_fotmob(match_id: &str) -> Result<MatchDetail> {
    let client = http_client()?;

    let url = format!("https://www.fotmob.com/api/data/matchDetails?matchId={match_id}");
    let body = fetch_json_cached(client, &url, &[]).context("request failed")?;
    parse_match_details_json(&body)
}

fn fetch_fotmob_response(date: Option<&str>) -> Result<FotmobResponse> {
    let client = http_client()?;

    let url = if let Some(date) = date.and_then(non_empty) {
        format!("{FOTMOB_MATCHES_URL}?date={date}")
    } else {
        FOTMOB_MATCHES_URL.to_string()
    };

    let body = fetch_json_cached(client, &url, &[]).context("request failed")?;
    parse_fotmob_response_json(&body)
}

#[derive(Debug, Deserialize)]
struct FotmobResponse {
    #[serde(default)]
    leagues: Vec<FotmobLeague>,
}

#[derive(Debug, Deserialize)]
struct FotmobLeague {
    id: u32,
    #[serde(rename = "primaryId")]
    primary_id: Option<u32>,
    name: String,
    #[serde(default)]
    matches: Vec<FotmobMatch>,
}

#[derive(Debug, Deserialize)]
struct FotmobMatch {
    id: u64,
    #[serde(rename = "tournamentStage")]
    tournament_stage: Option<String>,
    #[serde(default)]
    time: Option<String>,
    home: FotmobTeam,
    away: FotmobTeam,
    status: FotmobStatus,
}

pub fn parse_match_details_json(raw: &str) -> Result<MatchDetail> {
    let trimmed = raw.trim();
    if trimmed.is_empty() || trimmed == "null" {
        return Ok(MatchDetail {
            events: Vec::new(),
            lineups: None,
            stats: Vec::new(),
        });
    }

    let root: Value = serde_json::from_str(trimmed).context("invalid matchDetails json")?;
    let general = root.get("general").unwrap_or(&Value::Null);
    let home_name = pick_string(general, &["homeTeam", "home"]).unwrap_or_default();
    let away_name = pick_string(general, &["awayTeam", "away"]).unwrap_or_default();
    let content = root.get("content").unwrap_or(&Value::Null);

    let lineups = parse_lineups(content.get("lineup"));
    let events = parse_events(
        content
            .get("matchFacts")
            .and_then(|v| v.get("events"))
            .and_then(|v| v.get("events")),
        &home_name,
        &away_name,
    );
    let stats = parse_stats(content.get("stats").and_then(|v| v.get("stats")));

    Ok(MatchDetail {
        events,
        lineups,
        stats,
    })
}

fn parse_fotmob_response_json(raw: &str) -> Result<FotmobResponse> {
    let trimmed = raw.trim();
    if trimmed.is_empty() || trimmed == "null" {
        return Ok(FotmobResponse {
            leagues: Vec::new(),
        });
    }
    serde_json::from_str(trimmed).context("invalid fotmob json")
}

fn build_upcoming_from_response(data: FotmobResponse) -> Vec<UpcomingMatch> {
    let mut upcoming = Vec::new();

    for league in data.leagues {
        let league_id = league.primary_id.or(Some(league.id));
        for fixture in league.matches {
            if fixture.status.started || fixture.status.finished || fixture.status.cancelled {
                continue;
            }
            let home = fixture.home.short_name.unwrap_or(fixture.home.name);
            let away = fixture.away.short_name.unwrap_or(fixture.away.name);
            let kickoff = normalize_utc_time(&fixture.status.utc_time)
                .or_else(|| fixture.time.map(normalize_local_time))
                .unwrap_or_default();

            upcoming.push(UpcomingMatch {
                id: fixture.id.to_string(),
                league_id,
                league_name: league.name.clone(),
                round: fixture.tournament_stage.unwrap_or_default(),
                kickoff,
                home,
                away,
            });
        }
    }

    upcoming
}

fn build_matches_from_response(data: FotmobResponse) -> Vec<FotmobMatchRow> {
    let mut matches = Vec::new();

    for league in data.leagues {
        let league_id = league.primary_id.unwrap_or(league.id);
        for fixture in league.matches {
            let home = fixture.home.short_name.unwrap_or(fixture.home.name);
            let away = fixture.away.short_name.unwrap_or(fixture.away.name);
            let home_score = fixture.home.score.unwrap_or(0);
            let away_score = fixture.away.score.unwrap_or(0);

            matches.push(FotmobMatchRow {
                id: fixture.id.to_string(),
                league_id,
                league_name: league.name.clone(),
                home,
                away,
                home_score,
                away_score,
                started: fixture.status.started,
                finished: fixture.status.finished,
                cancelled: fixture.status.cancelled,
            });
        }
    }

    matches
}

#[derive(Debug, Deserialize)]
struct FotmobTeam {
    name: String,
    #[serde(rename = "shortName")]
    short_name: Option<String>,
    #[serde(default)]
    score: Option<u8>,
}

#[derive(Debug, Deserialize)]
struct FotmobStatus {
    #[serde(rename = "utcTime")]
    utc_time: String,
    #[serde(default)]
    started: bool,
    #[serde(default)]
    cancelled: bool,
    #[serde(default)]
    finished: bool,
}

fn non_empty(value: &str) -> Option<&str> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed)
    }
}

fn normalize_utc_time(raw: &str) -> Option<String> {
    let trimmed = raw.trim().trim_end_matches('Z');
    if trimmed.is_empty() {
        return None;
    }
    if trimmed.len() >= 16 {
        return Some(trimmed[..16].to_string());
    }
    Some(trimmed.replace(' ', "T"))
}

fn normalize_local_time(raw: String) -> String {
    let cleaned = raw.replace(' ', "T");
    if cleaned.len() >= 16 {
        cleaned[..16].to_string()
    } else {
        cleaned
    }
}

fn parse_lineups(value: Option<&Value>) -> Option<MatchLineups> {
    let lineup = value?.as_object()?;
    let mut sides = Vec::new();

    if let Some(home) = lineup.get("homeTeam") {
        if let Some(side) = parse_lineup_side(home) {
            sides.push(side);
        }
    }
    if let Some(away) = lineup.get("awayTeam") {
        if let Some(side) = parse_lineup_side(away) {
            sides.push(side);
        }
    }

    if sides.is_empty() {
        None
    } else {
        Some(MatchLineups { sides })
    }
}

fn parse_lineup_side(value: &Value) -> Option<LineupSide> {
    let name = pick_string(value, &["name"]).unwrap_or_default();
    if name.is_empty() {
        return None;
    }
    let formation = pick_string(value, &["formation"]).unwrap_or_default();
    let starters = parse_players(value.get("starters"));
    let subs = parse_players(
        value
            .get("substitutes")
            .or_else(|| value.get("bench"))
            .or_else(|| value.get("subs")),
    );

    Some(LineupSide {
        team: name.clone(),
        team_abbr: abbreviate_team(&name),
        formation,
        starting: starters,
        subs,
    })
}

fn parse_players(value: Option<&Value>) -> Vec<PlayerSlot> {
    let mut out = Vec::new();
    let Some(list) = value.and_then(|v| v.as_array()) else {
        return out;
    };
    for entry in list {
        if let Some(player) = parse_player(entry) {
            out.push(player);
        }
    }
    out
}

fn parse_player(value: &Value) -> Option<PlayerSlot> {
    let name = pick_string(value, &["name", "playerName"])
        .or_else(|| pick_string(value, &["fullName"]))
        .or_else(|| {
            value
                .get("player")
                .and_then(|p| pick_string(p, &["name", "fullName"]))
        })
        .unwrap_or_default();
    if name.is_empty() {
        return None;
    }
    let number = pick_u32(value, &["shirtNumber", "number"]);
    let pos = pick_string(value, &["position", "pos", "role", "positionShort"]);
    Some(PlayerSlot { name, number, pos })
}

fn parse_events(value: Option<&Value>, home: &str, away: &str) -> Vec<Event> {
    let mut out = Vec::new();
    let Some(list) = value.and_then(|v| v.as_array()) else {
        return out;
    };
    for entry in list {
        let Some(kind) = parse_event_kind(entry.get("type").and_then(|v| v.as_str())) else {
            continue;
        };
        let minute = entry.get("time").and_then(|v| v.as_u64()).unwrap_or(0) as u16;
        let is_home = entry
            .get("isHome")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);
        let team = if is_home { home } else { away };
        let player = entry
            .get("player")
            .and_then(|p| pick_string(p, &["name", "fullName"]))
            .unwrap_or_default();
        let event_type = entry
            .get("type")
            .and_then(|v| v.as_str())
            .unwrap_or("Event");
        let description = if player.is_empty() {
            event_type.to_string()
        } else {
            format!("{event_type} {player}")
        };
        out.push(Event {
            minute,
            kind,
            team: team.to_string(),
            description,
        });
    }
    out
}

fn parse_event_kind(event_type: Option<&str>) -> Option<EventKind> {
    let Some(event_type) = event_type else {
        return None;
    };
    let lowered = event_type.to_lowercase();
    if lowered.contains("goal") {
        Some(EventKind::Goal)
    } else if lowered.contains("card") {
        Some(EventKind::Card)
    } else if lowered.contains("sub") {
        Some(EventKind::Sub)
    } else if lowered.contains("shot") {
        Some(EventKind::Shot)
    } else {
        None
    }
}

fn parse_stats(value: Option<&Value>) -> Vec<StatRow> {
    let mut rows = Vec::new();
    let Some(groups) = value.and_then(|v| v.as_array()) else {
        return rows;
    };
    for group in groups {
        let Some(stats) = group.get("stats").and_then(|v| v.as_array()) else {
            continue;
        };
        for stat in stats {
            let name = pick_string(stat, &["title", "name"]).unwrap_or_default();
            if name.is_empty() {
                continue;
            }
            let home = value_to_string(stat.get("homeValue").or_else(|| stat.get("home")));
            let away = value_to_string(stat.get("awayValue").or_else(|| stat.get("away")));
            rows.push(StatRow { name, home, away });
        }
    }
    rows
}

fn value_to_string(value: Option<&Value>) -> String {
    match value {
        Some(Value::String(s)) => s.trim().to_string(),
        Some(Value::Number(n)) => n.to_string(),
        Some(Value::Bool(b)) => {
            if *b {
                "yes".to_string()
            } else {
                "no".to_string()
            }
        }
        Some(Value::Null) | None => "-".to_string(),
        Some(other) => other.to_string(),
    }
}

fn abbreviate_team(name: &str) -> String {
    let trimmed = name.trim();
    if trimmed.len() <= 3 {
        return trimmed.to_uppercase();
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
        return abbr.to_uppercase();
    }
    trimmed.chars().take(3).collect::<String>().to_uppercase()
}

fn pick_string(value: &Value, keys: &[&str]) -> Option<String> {
    for key in keys {
        if let Some(v) = value.get(*key) {
            if let Some(name) = as_string(v) {
                return Some(name);
            }
        }
    }
    None
}

fn pick_u32(value: &Value, keys: &[&str]) -> Option<u32> {
    for key in keys {
        if let Some(v) = value.get(*key) {
            if let Some(num) = v.as_u64() {
                return Some(num as u32);
            }
            if let Some(s) = v.as_str() {
                if let Ok(num) = s.parse::<u32>() {
                    return Some(num);
                }
            }
        }
    }
    None
}

fn as_string(value: &Value) -> Option<String> {
    match value {
        Value::String(s) => Some(s.trim().to_string()),
        Value::Number(n) => Some(n.to_string()),
        Value::Object(map) => {
            if let Some(Value::String(name)) = map.get("name") {
                return Some(name.trim().to_string());
            }
            if let Some(Value::String(name)) = map.get("shortName") {
                return Some(name.trim().to_string());
            }
            if let Some(Value::Object(team)) = map.get("team") {
                if let Some(Value::String(name)) = team.get("name") {
                    return Some(name.trim().to_string());
                }
            }
            None
        }
        _ => None,
    }
}
