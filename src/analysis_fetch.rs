use std::env;
use std::time::Duration;

use anyhow::{Context, Result};
use rayon::prelude::*;
use reqwest::blocking::Client;
use serde::Deserialize;

use crate::http_cache::fetch_json_cached;
use crate::http_client::http_client;
use crate::state::{
    Confederation, PlayerDetail, PlayerLeagueStats, PlayerMatchStat, PlayerSeasonPerformanceGroup,
    PlayerSeasonPerformanceItem, PlayerStatGroup, PlayerStatItem, PlayerTraitGroup,
    PlayerTraitItem, SquadPlayer, TeamAnalysis,
};

const FOTMOB_TEAM_URL: &str = "https://www.fotmob.com/api/teams?id=";
const FOTMOB_LEAGUE_URL: &str = "https://www.fotmob.com/api/leagues?id=";
const PREMIER_LEAGUE_FALLBACK: &[(&str, u32)] = &[
    ("AFC Bournemouth", 8678),
    ("Arsenal", 9825),
    ("Aston Villa", 10252),
    ("Brentford", 9937),
    ("Brighton & Hove Albion", 10204),
    ("Burnley", 8191),
    ("Chelsea", 8455),
    ("Crystal Palace", 9826),
    ("Everton", 8668),
    ("Fulham", 9879),
    ("Leeds United", 8463),
    ("Liverpool", 8650),
    ("Manchester City", 8456),
    ("Manchester United", 10260),
    ("Newcastle United", 10261),
    ("Nottingham Forest", 10203),
    ("Sunderland", 8472),
    ("Tottenham Hotspur", 8586),
    ("West Ham United", 8654),
    ("Wolverhampton Wanderers", 8602),
];

pub struct AnalysisFetch {
    pub teams: Vec<TeamAnalysis>,
    pub errors: Vec<String>,
}

#[derive(Clone, Copy)]
struct NationInfo {
    name: &'static str,
    confed: Confederation,
    host: bool,
    team_id: u32,
}

#[derive(Debug, Deserialize, Clone)]
struct LeagueTeam {
    id: u32,
    name: String,
}

const WORLD_CUP_TEAMS: &[NationInfo] = &[
    NationInfo {
        name: "Canada",
        confed: Confederation::CONCACAF,
        host: true,
        team_id: 5810,
    },
    NationInfo {
        name: "Mexico",
        confed: Confederation::CONCACAF,
        host: true,
        team_id: 6710,
    },
    NationInfo {
        name: "United States",
        confed: Confederation::CONCACAF,
        host: true,
        team_id: 6713,
    },
    NationInfo {
        name: "Australia",
        confed: Confederation::AFC,
        host: false,
        team_id: 6716,
    },
    NationInfo {
        name: "IR Iran",
        confed: Confederation::AFC,
        host: false,
        team_id: 6711,
    },
    NationInfo {
        name: "Japan",
        confed: Confederation::AFC,
        host: false,
        team_id: 6715,
    },
    NationInfo {
        name: "Jordan",
        confed: Confederation::AFC,
        host: false,
        team_id: 5816,
    },
    NationInfo {
        name: "Korea Republic (South Korea)",
        confed: Confederation::AFC,
        host: false,
        team_id: 7804,
    },
    NationInfo {
        name: "Qatar",
        confed: Confederation::AFC,
        host: false,
        team_id: 5902,
    },
    NationInfo {
        name: "Saudi Arabia",
        confed: Confederation::AFC,
        host: false,
        team_id: 7795,
    },
    NationInfo {
        name: "Uzbekistan",
        confed: Confederation::AFC,
        host: false,
        team_id: 8700,
    },
    NationInfo {
        name: "Algeria",
        confed: Confederation::CAF,
        host: false,
        team_id: 6317,
    },
    NationInfo {
        name: "Cabo Verde",
        confed: Confederation::CAF,
        host: false,
        team_id: 5888,
    },
    NationInfo {
        name: "Cote d'Ivoire",
        confed: Confederation::CAF,
        host: false,
        team_id: 6709,
    },
    NationInfo {
        name: "Egypt",
        confed: Confederation::CAF,
        host: false,
        team_id: 10255,
    },
    NationInfo {
        name: "Ghana",
        confed: Confederation::CAF,
        host: false,
        team_id: 6714,
    },
    NationInfo {
        name: "Morocco",
        confed: Confederation::CAF,
        host: false,
        team_id: 6262,
    },
    NationInfo {
        name: "Senegal",
        confed: Confederation::CAF,
        host: false,
        team_id: 6395,
    },
    NationInfo {
        name: "South Africa",
        confed: Confederation::CAF,
        host: false,
        team_id: 6316,
    },
    NationInfo {
        name: "Tunisia",
        confed: Confederation::CAF,
        host: false,
        team_id: 6719,
    },
    NationInfo {
        name: "Argentina",
        confed: Confederation::CONMEBOL,
        host: false,
        team_id: 6706,
    },
    NationInfo {
        name: "Brazil",
        confed: Confederation::CONMEBOL,
        host: false,
        team_id: 8256,
    },
    NationInfo {
        name: "Colombia",
        confed: Confederation::CONMEBOL,
        host: false,
        team_id: 8258,
    },
    NationInfo {
        name: "Ecuador",
        confed: Confederation::CONMEBOL,
        host: false,
        team_id: 6707,
    },
    NationInfo {
        name: "Paraguay",
        confed: Confederation::CONMEBOL,
        host: false,
        team_id: 6724,
    },
    NationInfo {
        name: "Uruguay",
        confed: Confederation::CONMEBOL,
        host: false,
        team_id: 5796,
    },
    NationInfo {
        name: "Curacao",
        confed: Confederation::CONCACAF,
        host: false,
        team_id: 287981,
    },
    NationInfo {
        name: "Haiti",
        confed: Confederation::CONCACAF,
        host: false,
        team_id: 5934,
    },
    NationInfo {
        name: "Panama",
        confed: Confederation::CONCACAF,
        host: false,
        team_id: 5922,
    },
    NationInfo {
        name: "Austria",
        confed: Confederation::UEFA,
        host: false,
        team_id: 8255,
    },
    NationInfo {
        name: "Belgium",
        confed: Confederation::UEFA,
        host: false,
        team_id: 8263,
    },
    NationInfo {
        name: "Croatia",
        confed: Confederation::UEFA,
        host: false,
        team_id: 10155,
    },
    NationInfo {
        name: "England",
        confed: Confederation::UEFA,
        host: false,
        team_id: 8491,
    },
    NationInfo {
        name: "France",
        confed: Confederation::UEFA,
        host: false,
        team_id: 6723,
    },
    NationInfo {
        name: "Germany",
        confed: Confederation::UEFA,
        host: false,
        team_id: 8570,
    },
    NationInfo {
        name: "Netherlands",
        confed: Confederation::UEFA,
        host: false,
        team_id: 6708,
    },
    NationInfo {
        name: "Norway",
        confed: Confederation::UEFA,
        host: false,
        team_id: 8492,
    },
    NationInfo {
        name: "Portugal",
        confed: Confederation::UEFA,
        host: false,
        team_id: 8361,
    },
    NationInfo {
        name: "Scotland",
        confed: Confederation::UEFA,
        host: false,
        team_id: 8498,
    },
    NationInfo {
        name: "Spain",
        confed: Confederation::UEFA,
        host: false,
        team_id: 6720,
    },
    NationInfo {
        name: "Switzerland",
        confed: Confederation::UEFA,
        host: false,
        team_id: 6717,
    },
    NationInfo {
        name: "New Zealand",
        confed: Confederation::OFC,
        host: false,
        team_id: 5820,
    },
];

pub fn fetch_worldcup_team_analysis() -> AnalysisFetch {
    let mut errors = Vec::new();
    let client = match http_client() {
        Ok(client) => client,
        Err(err) => {
            errors.push(format!("analysis client build failed: {err}"));
            return AnalysisFetch {
                teams: WORLD_CUP_TEAMS.iter().map(empty_analysis).collect(),
                errors,
            };
        }
    };

    let results: Vec<(TeamAnalysis, Option<String>)> = with_fetch_pool(|| {
        WORLD_CUP_TEAMS
            .par_iter()
            .map(|nation| match fetch_team_overview(client, nation.team_id) {
                Ok(overview) => (
                    TeamAnalysis {
                        id: nation.team_id,
                        name: nation.name.to_string(),
                        confed: nation.confed,
                        host: nation.host,
                        fifa_rank: overview.fifa_rank,
                        fifa_points: overview.fifa_points,
                        fifa_updated: overview.fifa_updated,
                    },
                    None,
                ),
                Err(err) => (
                    empty_analysis(nation),
                    Some(format!("{} fetch failed: {err}", nation.name)),
                ),
            })
            .collect()
    });

    let mut teams = Vec::with_capacity(results.len());
    for (team, err) in results {
        if let Some(err) = err {
            errors.push(err);
        }
        teams.push(team);
    }

    AnalysisFetch { teams, errors }
}

pub fn fetch_premier_league_team_analysis() -> AnalysisFetch {
    let mut errors = Vec::new();
    let client = match http_client() {
        Ok(client) => client,
        Err(err) => {
            errors.push(format!("analysis client build failed: {err}"));
            return AnalysisFetch {
                teams: Vec::new(),
                errors,
            };
        }
    };

    let teams = match fetch_league_teams(&client, 47) {
        Ok(teams) => teams,
        Err(err) => {
            errors.push(format!("premier league teams fetch failed: {err}"));
            fallback_premier_league_teams()
        }
    };

    let results: Vec<(TeamAnalysis, Option<String>)> = with_fetch_pool(|| {
        teams
            .par_iter()
            .map(|team| match fetch_team_overview(client, team.id) {
                Ok(overview) => (
                    TeamAnalysis {
                        id: team.id,
                        name: team.name.clone(),
                        confed: Confederation::UEFA,
                        host: false,
                        fifa_rank: overview.fifa_rank,
                        fifa_points: overview.fifa_points,
                        fifa_updated: overview.fifa_updated,
                    },
                    None,
                ),
                Err(err) => (
                    empty_club_analysis(team),
                    Some(format!("{} fetch failed: {err}", team.name)),
                ),
            })
            .collect()
    });

    let mut analysis = Vec::with_capacity(results.len());
    for (team, err) in results {
        if let Some(err) = err {
            errors.push(err);
        }
        analysis.push(team);
    }

    AnalysisFetch {
        teams: analysis,
        errors,
    }
}

fn empty_analysis(nation: &NationInfo) -> TeamAnalysis {
    TeamAnalysis {
        id: nation.team_id,
        name: nation.name.to_string(),
        confed: nation.confed,
        host: nation.host,
        fifa_rank: None,
        fifa_points: None,
        fifa_updated: None,
    }
}

fn empty_club_analysis(team: &LeagueTeam) -> TeamAnalysis {
    TeamAnalysis {
        id: team.id,
        name: team.name.clone(),
        confed: Confederation::UEFA,
        host: false,
        fifa_rank: None,
        fifa_points: None,
        fifa_updated: None,
    }
}

fn fallback_premier_league_teams() -> Vec<LeagueTeam> {
    PREMIER_LEAGUE_FALLBACK
        .iter()
        .map(|(name, id)| LeagueTeam {
            id: *id,
            name: (*name).to_string(),
        })
        .collect()
}

fn fetch_league_teams(client: &Client, league_id: u32) -> Result<Vec<LeagueTeam>> {
    let url = format!("{FOTMOB_LEAGUE_URL}{league_id}");
    let body = fetch_json_cached(client, &url, &[]).context("league request failed")?;
    let trimmed = body.trim();
    if trimmed.is_empty() || trimmed == "null" {
        return Err(anyhow::anyhow!("empty league response"));
    }

    let parsed: LeagueResponse = serde_json::from_str(trimmed).context("invalid league json")?;
    let mut teams = parsed
        .overview
        .and_then(|overview| overview.matches)
        .and_then(|matches| matches.fixture_info)
        .map(|info| info.teams)
        .unwrap_or_default();

    if teams.is_empty() {
        teams = parsed
            .stats
            .and_then(|stats| stats.teams)
            .unwrap_or_default();
    }

    if teams.is_empty() {
        teams = parsed
            .fixtures
            .and_then(|fixtures| fixtures.fixture_info)
            .map(|info| info.teams)
            .unwrap_or_default();
    }

    if teams.is_empty() {
        return Err(anyhow::anyhow!("no teams found for league {league_id}"));
    }

    let mut seen = std::collections::HashSet::new();
    teams.retain(|team| seen.insert(team.id));
    Ok(teams)
}

fn fetch_team_overview(client: &Client, team_id: u32) -> Result<TeamOverview> {
    let url = format!("{FOTMOB_TEAM_URL}{team_id}");
    let body = fetch_json_cached(client, &url, &[]).context("request failed")?;
    let trimmed = body.trim();
    if trimmed.is_empty() || trimmed == "null" {
        return Err(anyhow::anyhow!("empty team response"));
    }

    let parsed: TeamResponse = serde_json::from_str(trimmed).context("invalid team json")?;
    let details = parsed.details;
    let fifa = details.fifa_ranking;

    Ok(TeamOverview {
        fifa_rank: fifa.as_ref().and_then(|f| f.rank),
        fifa_points: fifa.as_ref().and_then(|f| f.points),
        fifa_updated: fifa.and_then(|f| f.updated),
    })
}

struct TeamOverview {
    fifa_rank: Option<u32>,
    fifa_points: Option<u32>,
    fifa_updated: Option<String>,
}

#[derive(Debug, Deserialize)]
struct LeagueResponse {
    overview: Option<LeagueOverview>,
    fixtures: Option<LeagueFixtures>,
    stats: Option<LeagueStats>,
}

#[derive(Debug, Deserialize)]
struct LeagueOverview {
    matches: Option<LeagueMatches>,
}

#[derive(Debug, Deserialize)]
struct LeagueMatches {
    #[serde(rename = "fixtureInfo")]
    fixture_info: Option<LeagueFixtureInfo>,
}

#[derive(Debug, Deserialize)]
struct LeagueFixtures {
    #[serde(rename = "fixtureInfo")]
    fixture_info: Option<LeagueFixtureInfo>,
}

#[derive(Debug, Deserialize)]
struct LeagueFixtureInfo {
    teams: Vec<LeagueTeam>,
}

#[derive(Debug, Deserialize)]
struct LeagueStats {
    teams: Option<Vec<LeagueTeam>>,
}

#[derive(Debug, Deserialize)]
struct TeamResponse {
    details: TeamDetails,
}

#[derive(Debug, Deserialize)]
struct TeamDetails {
    #[serde(rename = "fifaRanking")]
    fifa_ranking: Option<FifaRanking>,
}

#[derive(Debug, Deserialize)]
struct FifaRanking {
    rank: Option<u32>,
    points: Option<u32>,
    updated: Option<String>,
}

pub struct TeamSquad {
    pub team_name: String,
    pub players: Vec<SquadPlayer>,
}

pub fn fetch_team_squad(team_id: u32) -> Result<TeamSquad> {
    let client = http_client()?;

    let url = format!("{FOTMOB_TEAM_URL}{team_id}");
    let body = fetch_json_cached(client, &url, &[]).context("request failed")?;
    let trimmed = body.trim();
    if trimmed.is_empty() || trimmed == "null" {
        return Err(anyhow::anyhow!("empty team response"));
    }

    let parsed: TeamSquadResponse = serde_json::from_str(trimmed).context("invalid team json")?;
    let mut players = Vec::new();
    for group in parsed.squad.squad {
        if group.title == "coach" {
            continue;
        }
        for member in group.members {
            let role = member
                .role
                .and_then(|r| r.fallback)
                .unwrap_or_else(|| group.title.clone());
            players.push(SquadPlayer {
                id: member.id,
                name: member.name,
                role,
                club: member.cname.unwrap_or_else(|| "-".to_string()),
                age: member.age,
                height: member.height,
                shirt_number: member.shirt_number,
                market_value: member.transfer_value,
            });
        }
    }

    Ok(TeamSquad {
        team_name: parsed.details.name,
        players,
    })
}

pub fn fetch_player_detail(player_id: u32) -> Result<PlayerDetail> {
    let client = http_client()?;

    let url = format!("https://www.fotmob.com/api/playerData?id={player_id}");
    let mut last_err = None;
    let mut parsed: Option<PlayerDetail> = None;
    for attempt in 0..3 {
        let resp = fetch_json_cached(client, &url, &[("Accept-Language", "en-GB,en;q=0.9")]);

        match resp {
            Ok(body) => match parse_player_detail_json(&body) {
                Ok(data) => {
                    parsed = Some(data);
                    break;
                }
                Err(err) => {
                    last_err = Some(err);
                    if attempt < 2 {
                        std::thread::sleep(Duration::from_millis(300));
                        continue;
                    }
                    break;
                }
            },
            Err(err) => {
                last_err = Some(anyhow::anyhow!("request failed: {err}"));
                if attempt < 2 {
                    std::thread::sleep(Duration::from_millis(300));
                    continue;
                }
                break;
            }
        }
    }

    let parsed =
        parsed.ok_or_else(|| last_err.unwrap_or_else(|| anyhow::anyhow!("player fetch failed")))?;
    Ok(parsed)
}

pub fn parse_player_detail_json(raw: &str) -> Result<PlayerDetail> {
    let trimmed = raw.trim();
    if trimmed.is_empty() || trimmed == "null" {
        return Err(anyhow::anyhow!("empty player response"));
    }
    let parsed: PlayerDataResponse =
        serde_json::from_str(trimmed).context("invalid player json")?;
    let mut age: Option<String> = None;
    let mut country: Option<String> = None;
    let mut height: Option<String> = None;
    let mut preferred_foot: Option<String> = None;
    let mut shirt: Option<String> = None;
    let mut market_value: Option<String> = None;
    let mut contract_end: Option<String> = None;
    if let Some(info) = parsed.player_information {
        for row in info {
            let Some(ref value) = row.value else {
                continue;
            };
            let rendered = info_value_to_string(&value.fallback);
            let key = match row.translation_key.as_deref() {
                Some("height_sentencecase") => "Height",
                Some("age_sentencecase") => "Age",
                Some("preferred_foot") => "Preferred foot",
                Some("country_sentencecase") => "Country",
                Some("shirt") => "Shirt",
                Some("transfer_value") => "Market value",
                Some("contract_end") => "Contract end",
                _ => row.title.as_str(),
            };
            match key {
                "Age" => age = Some(rendered),
                "Country" => country = Some(rendered),
                "Height" => height = Some(rendered),
                "Preferred foot" => preferred_foot = Some(rendered),
                "Shirt" => shirt = Some(rendered),
                "Market value" => market_value = Some(rendered),
                "Contract end" => contract_end = Some(rendered),
                _ => {}
            }
        }
    }

    let positions = parsed
        .position_description
        .as_ref()
        .map(|desc| {
            let mut out = Vec::with_capacity(desc.positions.len());
            for pos in &desc.positions {
                if pos.is_main_position {
                    let mut label = pos.str_pos.label.clone();
                    label.push_str(" (primary)");
                    out.push(label);
                } else {
                    out.push(pos.str_pos.label.clone());
                }
            }
            out
        })
        .unwrap_or_default();

    let main_league = parsed.main_league.map(|league| PlayerLeagueStats {
        league_name: league.league_name,
        season: league.season,
        stats: league
            .stats
            .into_iter()
            .map(|stat| PlayerStatItem {
                title: stat.title,
                value: value_to_string(&stat.value),
            })
            .collect(),
    });

    let season_top_items = parsed
        .first_season_stats
        .as_ref()
        .and_then(|season| {
            season
                .top_stat_card
                .as_ref()
                .and_then(|card| card.items.as_ref())
        })
        .cloned()
        .unwrap_or_default();

    let all_competitions = if !season_top_items.is_empty() {
        let mut out = Vec::with_capacity(season_top_items.len());
        for stat in &season_top_items {
            out.push(PlayerStatItem {
                title: stat.title.clone(),
                value: value_to_string(&stat.stat_value),
            });
        }
        out
    } else {
        Vec::new()
    };

    let top_items = match parsed.top_stat_card.and_then(|card| card.items) {
        Some(items) if !items.is_empty() => items,
        _ => season_top_items,
    };

    let mut top_stats = Vec::with_capacity(top_items.len());
    for stat in top_items {
        top_stats.push(PlayerStatItem {
            title: stat.title,
            value: value_to_string(&stat.stat_value),
        });
    }

    let season_items = match parsed
        .stats_section
        .as_ref()
        .and_then(|section| section.items.clone())
    {
        Some(items) if !items.is_empty() => items,
        _ => parsed
            .first_season_stats
            .as_ref()
            .and_then(|season| {
                season
                    .stats_section
                    .as_ref()
                    .and_then(|section| section.items.clone())
            })
            .unwrap_or_default(),
    };

    let season_items = season_items
        .into_iter()
        .filter(|group| {
            group
                .items
                .as_ref()
                .map(|items| !items.is_empty())
                .unwrap_or(false)
        })
        .collect::<Vec<_>>();

    let season_performance = season_items
        .iter()
        .enumerate()
        .filter_map(|(idx, group)| {
            let title = group
                .title
                .as_ref()
                .filter(|t| !t.trim().is_empty())
                .cloned()
                .unwrap_or_else(|| format!("Stats Group {}", idx + 1));
            let items = group
                .items
                .as_ref()
                .map(|items| {
                    items
                        .iter()
                        .map(|stat| PlayerSeasonPerformanceItem {
                            title: stat.title.clone(),
                            total: format_stat_value(&stat.stat_value, stat.stat_format.as_deref()),
                            per90: format_per90(stat.per90, stat.stat_format.as_deref()),
                            percentile_rank: stat.percentile_rank,
                            percentile_rank_per90: stat.percentile_rank_per90,
                        })
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            if items.is_empty() {
                None
            } else {
                Some(PlayerSeasonPerformanceGroup { title, items })
            }
        })
        .collect::<Vec<_>>();

    let season_groups = season_items
        .into_iter()
        .enumerate()
        .filter_map(|(idx, group)| {
            let title = group
                .title
                .filter(|t| !t.trim().is_empty())
                .unwrap_or_else(|| format!("Stats Group {}", idx + 1));
            let items = group
                .items
                .unwrap_or_default()
                .into_iter()
                .map(|stat| PlayerStatItem {
                    title: stat.title,
                    value: format_stat_value(&stat.stat_value, stat.stat_format.as_deref()),
                })
                .collect::<Vec<_>>();
            if items.is_empty() {
                None
            } else {
                Some(PlayerStatGroup { title, items })
            }
        })
        .collect::<Vec<_>>();

    let traits = parsed.traits.map(|traits| PlayerTraitGroup {
        title: traits.title,
        items: traits
            .items
            .into_iter()
            .map(|item| PlayerTraitItem {
                title: item.title,
                value: item.value,
            })
            .collect(),
    });

    let recent_matches = if let Some(items) = parsed.recent_matches {
        let mut out = Vec::with_capacity(items.len());
        for item in items {
            out.push(PlayerMatchStat {
                opponent: empty_to_dash(item.opponent_team_name),
                league: empty_to_dash(item.league_name),
                date: item
                    .match_date
                    .map(|d| empty_to_dash(d.utc_time))
                    .unwrap_or_else(|| "-".to_string()),
                goals: item.goals as u8,
                assists: item.assists as u8,
                rating: item
                    .rating_props
                    .and_then(|r| r.rating)
                    .map(|value| value_to_string(&value)),
            });
        }
        out
    } else {
        Vec::new()
    };

    let career_sections = parsed
        .career_history
        .as_ref()
        .and_then(|history| history.career_items.as_ref())
        .map(|items| {
            let mut ordered = Vec::with_capacity(items.len());
            for key in ["senior", "national team", "youth"] {
                if let Some(section) = items.get(key) {
                    ordered.push((key.to_string(), section));
                }
            }
            for (key, section) in items.iter() {
                if !ordered.iter().any(|(title, _)| title == key) {
                    ordered.push((key.clone(), section));
                }
            }
            ordered
                .into_iter()
                .map(|(title, section)| crate::state::PlayerCareerSection {
                    title,
                    entries: section
                        .team_entries
                        .iter()
                        .map(|entry| crate::state::PlayerCareerEntry {
                            team: entry.team.clone(),
                            start_date: entry.start_date.clone(),
                            end_date: entry.end_date.clone(),
                            appearances: entry.appearances.clone(),
                            goals: entry.goals.clone(),
                            assists: entry.assists.clone(),
                        })
                        .collect(),
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    let season_breakdown = parsed
        .career_history
        .as_ref()
        .and_then(|history| history.career_items.as_ref())
        .and_then(|items| items.get("senior"))
        .and_then(|section| section.season_entries.first())
        .map(|season| {
            let mut out = Vec::with_capacity(season.tournament_stats.len());
            for stat in &season.tournament_stats {
                let rating = stat
                    .rating
                    .as_ref()
                    .and_then(|r| r.rating.as_ref())
                    .map(value_to_string)
                    .unwrap_or_else(|| "-".to_string());
                let rating = normalize_stat_cell(rating);
                out.push(crate::state::PlayerSeasonTournamentStat {
                    league: stat.league_name.clone(),
                    season: stat.season_name.clone(),
                    appearances: normalize_stat_cell(stat.appearances.clone()),
                    goals: normalize_stat_cell(stat.goals.clone()),
                    assists: normalize_stat_cell(stat.assists.clone()),
                    rating,
                });
            }
            out
        })
        .unwrap_or_default();

    let trophies = parsed
        .trophies
        .map(|trophies| {
            trophies
                .player_trophies
                .into_iter()
                .flat_map(|team| {
                    team.tournaments.into_iter().map(move |tournament| {
                        crate::state::PlayerTrophyEntry {
                            team: team.team_name.clone(),
                            league: tournament.league_name,
                            seasons_won: tournament.seasons_won,
                            seasons_runner_up: tournament.seasons_runner_up,
                        }
                    })
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    let contract_end = contract_end.or_else(|| parsed.contract_end.map(|d| d.utc_time));

    Ok(PlayerDetail {
        id: parsed.id,
        name: parsed.name,
        team: parsed.primary_team.map(|t| t.team_name),
        position: parsed
            .position_description
            .and_then(|p| p.primary_position.map(|pos| pos.label)),
        age,
        country,
        height,
        preferred_foot,
        shirt,
        market_value,
        contract_end,
        birth_date: parsed.birth_date.map(|d| d.utc_time),
        status: parsed.status,
        injury_info: optional_info_string(parsed.injury_information.as_ref()),
        international_duty: optional_info_string(parsed.international_duty.as_ref()),
        positions,
        all_competitions,
        all_competitions_season: main_league.as_ref().map(|league| league.season.clone()),
        main_league,
        top_stats,
        season_groups,
        season_performance,
        traits,
        recent_matches,
        season_breakdown,
        career_sections,
        trophies,
    })
}

fn info_value_to_string(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(s) => s.trim().to_string(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Bool(b) => {
            if *b {
                "yes".to_string()
            } else {
                "no".to_string()
            }
        }
        serde_json::Value::Object(map) => {
            if let Some(serde_json::Value::String(utc)) = map.get("utcTime") {
                return utc.trim().to_string();
            }
            value.to_string()
        }
        serde_json::Value::Null => "-".to_string(),
        other => other.to_string(),
    }
}

fn optional_info_string(value: Option<&serde_json::Value>) -> Option<String> {
    let value = value?;
    let rendered = info_value_to_string(value);
    if rendered == "-" || rendered == "null" {
        None
    } else {
        Some(rendered)
    }
}

fn vec_or_default<'de, D, T>(deserializer: D) -> std::result::Result<Vec<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: serde::Deserialize<'de>,
{
    let value = Option::<Vec<T>>::deserialize(deserializer)?;
    Ok(value.unwrap_or_default())
}

fn string_or_default<'de, D>(deserializer: D) -> std::result::Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = serde_json::Value::deserialize(deserializer)?;
    let rendered = match value {
        serde_json::Value::String(s) => s,
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Bool(b) => {
            if b {
                "true".to_string()
            } else {
                "false".to_string()
            }
        }
        serde_json::Value::Null => String::new(),
        other => other.to_string(),
    };
    Ok(rendered)
}

fn float_or_none<'de, D>(deserializer: D) -> std::result::Result<Option<f64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = serde_json::Value::deserialize(deserializer)?;
    match value {
        serde_json::Value::Number(n) => Ok(n.as_f64()),
        serde_json::Value::String(s) => Ok(s.parse::<f64>().ok()),
        serde_json::Value::Null => Ok(None),
        _ => Ok(None),
    }
}

fn value_to_string(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(s) => s.trim().to_string(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Bool(b) => {
            if *b {
                "yes".to_string()
            } else {
                "no".to_string()
            }
        }
        serde_json::Value::Null => "-".to_string(),
        other => other.to_string(),
    }
}

fn format_stat_value(value: &serde_json::Value, stat_format: Option<&str>) -> String {
    let mut rendered = value_to_string(value);
    if stat_format == Some("percent") && rendered != "-" && !rendered.ends_with('%') {
        rendered.push('%');
    }
    rendered
}

fn format_per90(value: Option<f64>, stat_format: Option<&str>) -> Option<String> {
    let value = value?;
    let mut rendered = if value.fract().abs() < 0.005 {
        format!("{value:.0}")
    } else {
        format!("{value:.2}")
    };
    if stat_format == Some("percent") && !rendered.ends_with('%') {
        rendered.push('%');
    }
    Some(rendered)
}

fn empty_to_dash(value: String) -> String {
    if value.trim().is_empty() {
        "-".to_string()
    } else {
        value
    }
}

fn normalize_stat_cell(value: String) -> String {
    let trimmed = value.trim();
    if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("null") {
        "-".to_string()
    } else {
        trimmed.to_string()
    }
}

fn with_fetch_pool<T>(action: impl FnOnce() -> T + Send) -> T
where
    T: Send,
{
    let threads = fetch_parallelism();
    match rayon::ThreadPoolBuilder::new().num_threads(threads).build() {
        Ok(pool) => pool.install(action),
        Err(_) => action(),
    }
}

fn fetch_parallelism() -> usize {
    env::var("FETCH_PARALLELISM")
        .ok()
        .and_then(|val| val.parse::<usize>().ok())
        .unwrap_or(6)
        .clamp(2, 32)
}

#[derive(Debug, Deserialize)]
struct TeamSquadResponse {
    details: TeamSquadDetails,
    squad: TeamSquadBlock,
}

#[derive(Debug, Deserialize)]
struct TeamSquadDetails {
    name: String,
}

#[derive(Debug, Deserialize)]
struct TeamSquadBlock {
    squad: Vec<SquadGroup>,
}

#[derive(Debug, Deserialize)]
struct SquadGroup {
    title: String,
    #[serde(default)]
    members: Vec<SquadMember>,
}

#[derive(Debug, Deserialize)]
struct SquadMember {
    id: u32,
    name: String,
    #[serde(rename = "shirtNumber")]
    shirt_number: Option<u32>,
    role: Option<SquadRole>,
    cname: Option<String>,
    age: Option<u32>,
    height: Option<u32>,
    #[serde(rename = "transferValue")]
    transfer_value: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct SquadRole {
    fallback: Option<String>,
}

#[derive(Debug, Deserialize)]
struct PlayerDataResponse {
    id: u32,
    name: String,
    #[serde(rename = "birthDate")]
    birth_date: Option<PlayerDate>,
    #[serde(rename = "contractEnd")]
    contract_end: Option<PlayerDate>,
    #[serde(rename = "primaryTeam")]
    primary_team: Option<PlayerTeam>,
    #[serde(rename = "positionDescription")]
    position_description: Option<PlayerPositionDescription>,
    #[serde(rename = "playerInformation")]
    player_information: Option<Vec<PlayerInfoRow>>,
    #[serde(rename = "careerHistory")]
    career_history: Option<PlayerCareerHistory>,
    trophies: Option<PlayerTrophies>,
    #[serde(rename = "mainLeague")]
    main_league: Option<PlayerLeague>,
    #[serde(rename = "topStatCard")]
    top_stat_card: Option<PlayerStatCard>,
    #[serde(rename = "statsSection")]
    stats_section: Option<PlayerStatsSection>,
    #[serde(rename = "firstSeasonStats")]
    first_season_stats: Option<PlayerSeasonStats>,
    traits: Option<PlayerTraits>,
    #[serde(rename = "recentMatches")]
    recent_matches: Option<Vec<PlayerRecentMatch>>,
    status: Option<String>,
    #[serde(rename = "injuryInformation")]
    injury_information: Option<serde_json::Value>,
    #[serde(rename = "internationalDuty")]
    international_duty: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct PlayerDate {
    #[serde(rename = "utcTime")]
    utc_time: String,
}

#[derive(Debug, Deserialize)]
struct PlayerCareerHistory {
    #[serde(rename = "careerItems")]
    career_items: Option<std::collections::HashMap<String, PlayerCareerCategory>>,
}

#[derive(Debug, Deserialize)]
struct PlayerCareerCategory {
    #[serde(rename = "teamEntries", default, deserialize_with = "vec_or_default")]
    team_entries: Vec<PlayerCareerTeamEntry>,
    #[serde(rename = "seasonEntries", default, deserialize_with = "vec_or_default")]
    season_entries: Vec<PlayerCareerSeasonEntry>,
}

#[derive(Debug, Deserialize)]
struct PlayerCareerTeamEntry {
    team: String,
    #[serde(rename = "startDate")]
    start_date: Option<String>,
    #[serde(rename = "endDate")]
    end_date: Option<String>,
    appearances: Option<String>,
    goals: Option<String>,
    assists: Option<String>,
}

#[derive(Debug, Deserialize)]
struct PlayerCareerSeasonEntry {
    #[serde(rename = "seasonName")]
    #[allow(dead_code)]
    season_name: String,
    #[serde(rename = "tournamentStats", default)]
    tournament_stats: Vec<PlayerTournamentStat>,
}

#[derive(Debug, Deserialize)]
struct PlayerTournamentStat {
    #[serde(rename = "leagueName")]
    league_name: String,
    #[serde(rename = "seasonName")]
    season_name: String,
    #[serde(default, deserialize_with = "string_or_default")]
    goals: String,
    #[serde(default, deserialize_with = "string_or_default")]
    assists: String,
    #[serde(default, deserialize_with = "string_or_default")]
    appearances: String,
    rating: Option<PlayerRating>,
}

#[derive(Debug, Deserialize)]
struct PlayerRating {
    rating: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct PlayerTrophies {
    #[serde(rename = "playerTrophies", default)]
    player_trophies: Vec<PlayerTrophyTeam>,
}

#[derive(Debug, Deserialize)]
struct PlayerTrophyTeam {
    #[serde(rename = "teamName")]
    team_name: String,
    #[serde(default)]
    tournaments: Vec<PlayerTrophyTournament>,
}

#[derive(Debug, Deserialize)]
struct PlayerTrophyTournament {
    #[serde(rename = "leagueName")]
    league_name: String,
    #[serde(rename = "seasonsWon", default)]
    seasons_won: Vec<String>,
    #[serde(rename = "seasonsRunnerUp", default)]
    seasons_runner_up: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct PlayerSeasonStats {
    #[serde(rename = "topStatCard")]
    top_stat_card: Option<PlayerStatCard>,
    #[serde(rename = "statsSection")]
    stats_section: Option<PlayerStatsSection>,
}

#[derive(Debug, Deserialize)]
struct PlayerTeam {
    #[serde(rename = "teamName")]
    team_name: String,
}

#[derive(Debug, Deserialize)]
struct PlayerPositionDescription {
    #[serde(rename = "primaryPosition")]
    primary_position: Option<PlayerPosition>,
    #[serde(default)]
    positions: Vec<PlayerPositionSummary>,
}

#[derive(Debug, Deserialize)]
struct PlayerPosition {
    label: String,
}

#[derive(Debug, Deserialize)]
struct PlayerPositionSummary {
    #[serde(rename = "strPos")]
    str_pos: PlayerPosition,
    #[serde(rename = "isMainPosition")]
    is_main_position: bool,
}

#[derive(Debug, Deserialize)]
struct PlayerInfoRow {
    title: String,
    #[serde(rename = "translationKey")]
    translation_key: Option<String>,
    value: Option<PlayerInfoValue>,
}

#[derive(Debug, Deserialize)]
struct PlayerInfoValue {
    fallback: serde_json::Value,
}

#[derive(Debug, Deserialize)]
struct PlayerLeague {
    #[serde(rename = "leagueName")]
    league_name: String,
    season: String,
    #[serde(default, deserialize_with = "vec_or_default")]
    stats: Vec<PlayerStatValue>,
}

#[derive(Debug, Deserialize, Clone)]
struct PlayerStatValue {
    #[serde(default)]
    title: String,
    #[serde(default)]
    value: serde_json::Value,
}

#[derive(Debug, Deserialize, Clone)]
struct PlayerStatCard {
    #[serde(default)]
    items: Option<Vec<PlayerStatValueDetail>>,
}

#[derive(Debug, Deserialize, Clone)]
struct PlayerStatValueDetail {
    #[serde(default)]
    title: String,
    #[serde(rename = "statValue")]
    #[serde(default)]
    stat_value: serde_json::Value,
    #[serde(rename = "statFormat")]
    #[serde(default)]
    stat_format: Option<String>,
    #[serde(rename = "per90", default, deserialize_with = "float_or_none")]
    per90: Option<f64>,
    #[serde(rename = "percentileRank", default, deserialize_with = "float_or_none")]
    percentile_rank: Option<f64>,
    #[serde(
        rename = "percentileRankPer90",
        default,
        deserialize_with = "float_or_none"
    )]
    percentile_rank_per90: Option<f64>,
}

#[derive(Debug, Deserialize, Clone)]
struct PlayerStatsSection {
    #[serde(default)]
    items: Option<Vec<PlayerStatsGroup>>,
}

#[derive(Debug, Deserialize, Clone)]
struct PlayerStatsGroup {
    title: Option<String>,
    #[serde(default)]
    items: Option<Vec<PlayerStatValueDetail>>,
}

#[derive(Debug, Deserialize)]
struct PlayerTraits {
    #[serde(default)]
    title: String,
    #[serde(default, deserialize_with = "vec_or_default")]
    items: Vec<PlayerTraitValue>,
}

#[derive(Debug, Deserialize)]
struct PlayerTraitValue {
    #[serde(default)]
    title: String,
    #[serde(default)]
    value: f32,
}

#[derive(Debug, Deserialize)]
struct PlayerRecentMatch {
    #[serde(rename = "opponentTeamName")]
    #[serde(default)]
    opponent_team_name: String,
    #[serde(rename = "leagueName")]
    #[serde(default)]
    league_name: String,
    #[serde(rename = "matchDate")]
    match_date: Option<PlayerMatchDate>,
    #[serde(default)]
    goals: u32,
    #[serde(default)]
    assists: u32,
    #[serde(rename = "ratingProps")]
    rating_props: Option<PlayerMatchRating>,
}

#[derive(Debug, Deserialize)]
struct PlayerMatchDate {
    #[serde(rename = "utcTime")]
    #[serde(default)]
    utc_time: String,
}

#[derive(Debug, Deserialize)]
struct PlayerMatchRating {
    rating: Option<serde_json::Value>,
}
