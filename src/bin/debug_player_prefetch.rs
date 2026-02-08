use anyhow::{Context, Result};
use chrono::{Duration as ChronoDuration, Utc};

use wc26_terminal::{analysis_fetch, upcoming_fetch};

const LIVERPOOL_ID: u32 = 8650;
const MAN_CITY_ID: u32 = 8456;

fn main() -> Result<()> {
    let limit = std::env::var("DEBUG_PREFETCH_LIMIT")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(22)
        .clamp(1, 40);
    let revalidate = std::env::var("DEBUG_PREFETCH_REVALIDATE")
        .ok()
        .map(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes"))
        .unwrap_or(false);

    // Search forward from "today" for the next Liverpool vs Man City fixture.
    let today = Utc::now().date_naive();
    let mut match_row = None;
    for offset in 0..=60i64 {
        let day = today + ChronoDuration::days(offset);
        let date = day.format("%Y-%m-%d").to_string();
        let rows = upcoming_fetch::fetch_matches_from_fotmob(Some(&date))
            .with_context(|| format!("fetch matches for {date}"))?;
        for row in rows {
            let is_fixture = (row.home_team_id == LIVERPOOL_ID && row.away_team_id == MAN_CITY_ID)
                || (row.home_team_id == MAN_CITY_ID && row.away_team_id == LIVERPOOL_ID);
            if is_fixture {
                match_row = Some(row);
                break;
            }
        }
        if match_row.is_some() {
            break;
        }
    }

    let Some(row) = match_row else {
        eprintln!("No Liverpool vs Man City match found in the next 60 days.");
        return Ok(());
    };

    println!(
        "Found fixture: {} vs {} (matchId={}, kickoff_utc={})",
        row.home, row.away, row.id, row.utc_time
    );

    let detail = upcoming_fetch::fetch_match_details_basic_from_fotmob(&row.id)
        .with_context(|| format!("fetch match details for {}", row.id))?;
    let mut ids = collect_lineup_starter_ids(&detail);
    ids.truncate(22);

    if ids.is_empty() {
        println!("No lineup starter ids available yet; falling back to squads.");
        let home = analysis_fetch::fetch_team_squad(row.home_team_id)
            .with_context(|| format!("fetch home squad {}", row.home_team_id))?;
        let away = analysis_fetch::fetch_team_squad(row.away_team_id)
            .with_context(|| format!("fetch away squad {}", row.away_team_id))?;
        ids.extend(home.players.into_iter().map(|p| p.id));
        ids.extend(away.players.into_iter().map(|p| p.id));
        ids.sort_unstable();
        ids.dedup();
        ids.truncate(limit);
    } else {
        println!("Starter ids found: {}", ids.len());
        ids.sort_unstable();
        ids.dedup();
        ids.truncate(limit);
    }

    println!("Prefetching {} players:", ids.len());
    for id in ids {
        let res = if revalidate {
            analysis_fetch::fetch_player_detail_revalidate(id)
        } else {
            analysis_fetch::fetch_player_detail(id)
        };
        match res {
            Ok(player) => println!("OK  player {}: {}", id, player.name),
            Err(err) => println!("ERR player {}: {}", id, err),
        }
    }

    Ok(())
}

fn collect_lineup_starter_ids(detail: &wc26_terminal::state::MatchDetail) -> Vec<u32> {
    let Some(lineups) = detail.lineups.as_ref() else {
        return Vec::new();
    };
    let mut ids = Vec::new();
    for side in &lineups.sides {
        for slot in &side.starting {
            if let Some(id) = slot.id {
                ids.push(id);
            }
        }
    }
    ids
}
