use std::time::Duration;

use anyhow::{Context, Result};
use reqwest::blocking::Client;
use serde::Deserialize;

use crate::state::{
    Confederation, PlayerDetail, PlayerLeagueStats, PlayerMatchStat, PlayerStatGroup,
    PlayerStatItem, PlayerTraitGroup, PlayerTraitItem, SquadPlayer, TeamAnalysis,
};

const FOTMOB_TEAM_URL: &str = "https://www.fotmob.com/api/teams?id=";

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
    let client = match Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
    {
        Ok(client) => client,
        Err(err) => {
            errors.push(format!("analysis client build failed: {err}"));
            return AnalysisFetch {
                teams: WORLD_CUP_TEAMS.iter().map(empty_analysis).collect(),
                errors,
            };
        }
    };

    let mut teams = Vec::new();
    for nation in WORLD_CUP_TEAMS {
        match fetch_team_overview(&client, nation.team_id) {
            Ok(overview) => teams.push(TeamAnalysis {
                id: nation.team_id,
                name: nation.name.to_string(),
                confed: nation.confed,
                host: nation.host,
                fifa_rank: overview.fifa_rank,
                fifa_points: overview.fifa_points,
                fifa_updated: overview.fifa_updated,
            }),
            Err(err) => {
                errors.push(format!("{} fetch failed: {err}", nation.name));
                teams.push(empty_analysis(nation));
            }
        }
    }

    AnalysisFetch { teams, errors }
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

fn fetch_team_overview(client: &Client, team_id: u32) -> Result<TeamOverview> {
    let url = format!("{FOTMOB_TEAM_URL}{team_id}");
    let resp = client
        .get(url)
        .header("User-Agent", "Mozilla/5.0")
        .send()
        .context("request failed")?;

    let status = resp.status();
    let body = resp.text().context("failed reading body")?;
    if !status.is_success() {
        return Err(anyhow::anyhow!("http {}: {}", status, body));
    }

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
    let client = Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .context("failed to build http client")?;

    let url = format!("{FOTMOB_TEAM_URL}{team_id}");
    let resp = client
        .get(url)
        .header("User-Agent", "Mozilla/5.0")
        .send()
        .context("request failed")?;

    let status = resp.status();
    let body = resp.text().context("failed reading body")?;
    if !status.is_success() {
        return Err(anyhow::anyhow!("http {}: {}", status, body));
    }

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
    let client = Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .context("failed to build http client")?;

    let url = format!("https://www.fotmob.com/api/playerData?id={player_id}");
    let resp = client
        .get(url)
        .header("User-Agent", "Mozilla/5.0")
        .send()
        .context("request failed")?;

    let status = resp.status();
    let body = resp.text().context("failed reading body")?;
    if !status.is_success() {
        return Err(anyhow::anyhow!("http {}: {}", status, body));
    }

    let trimmed = body.trim();
    if trimmed.is_empty() || trimmed == "null" {
        return Err(anyhow::anyhow!("empty player response"));
    }

    let parsed: PlayerDataResponse = serde_json::from_str(trimmed)
        .map_err(|err| anyhow::anyhow!("invalid player json: {err}"))?;
    let mut info_map = std::collections::HashMap::new();
    if let Some(info) = parsed.player_information {
        for row in info {
            if let Some(value) = row.value {
                info_map.insert(row.title, info_value_to_string(&value.fallback));
            }
        }
    }

    let positions = parsed
        .position_description
        .as_ref()
        .map(|desc| {
            desc.positions
                .iter()
                .map(|pos| {
                    if pos.is_main_position {
                        format!("{} (primary)", pos.str_pos.label)
                    } else {
                        pos.str_pos.label.clone()
                    }
                })
                .collect::<Vec<_>>()
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
        season_top_items
            .iter()
            .map(|stat| PlayerStatItem {
                title: stat.title.clone(),
                value: value_to_string(&stat.stat_value),
            })
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };

    let top_items = match parsed.top_stat_card.and_then(|card| card.items) {
        Some(items) if !items.is_empty() => items,
        _ => season_top_items,
    };

    let top_stats = top_items
        .into_iter()
        .map(|stat| PlayerStatItem {
            title: stat.title,
            value: value_to_string(&stat.stat_value),
        })
        .collect::<Vec<_>>();

    let season_items = match parsed.stats_section.and_then(|section| section.items) {
        Some(items) if !items.is_empty() => items,
        _ => parsed
            .first_season_stats
            .as_ref()
            .and_then(|season| {
                season
                    .stats_section
                    .as_ref()
                    .and_then(|section| section.items.as_ref())
            })
            .cloned()
            .unwrap_or_default(),
    };

    let season_groups = season_items
        .into_iter()
        .filter_map(|group| {
            let title = group.title?;
            let items = group
                .items
                .unwrap_or_default()
                .into_iter()
                .map(|stat| PlayerStatItem {
                    title: stat.title,
                    value: value_to_string(&stat.stat_value),
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

    let recent_matches = parsed
        .recent_matches
        .unwrap_or_default()
        .into_iter()
        .map(|item| PlayerMatchStat {
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
        })
        .collect::<Vec<_>>();

    let career_sections = parsed
        .career_history
        .as_ref()
        .and_then(|history| history.career_items.as_ref())
        .map(|items| {
            let mut ordered = Vec::new();
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
            season
                .tournament_stats
                .iter()
                .map(|stat| {
                    let rating = stat
                        .rating
                        .as_ref()
                        .and_then(|r| r.rating.as_ref())
                        .map(value_to_string)
                        .unwrap_or_else(|| "-".to_string());
                    crate::state::PlayerSeasonTournamentStat {
                        league: stat.league_name.clone(),
                        season: stat.season_name.clone(),
                        appearances: empty_to_dash(stat.appearances.clone()),
                        goals: empty_to_dash(stat.goals.clone()),
                        assists: empty_to_dash(stat.assists.clone()),
                        rating,
                    }
                })
                .collect::<Vec<_>>()
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

    let contract_end = info_map
        .get("Contract end")
        .cloned()
        .or_else(|| parsed.contract_end.map(|d| d.utc_time));

    Ok(PlayerDetail {
        id: parsed.id,
        name: parsed.name,
        team: parsed.primary_team.map(|t| t.team_name),
        position: parsed
            .position_description
            .and_then(|p| p.primary_position.map(|pos| pos.label)),
        age: info_map.get("Age").cloned(),
        country: info_map.get("Country").cloned(),
        height: info_map.get("Height").cloned(),
        preferred_foot: info_map.get("Preferred foot").cloned(),
        shirt: info_map.get("Shirt").cloned(),
        market_value: info_map.get("Market value").cloned(),
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

fn empty_to_dash(value: String) -> String {
    if value.trim().is_empty() {
        "-".to_string()
    } else {
        value
    }
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
    #[serde(rename = "teamEntries", default)]
    team_entries: Vec<PlayerCareerTeamEntry>,
    #[serde(rename = "seasonEntries", default)]
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
    #[serde(default)]
    goals: String,
    #[serde(default)]
    assists: String,
    #[serde(default)]
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
