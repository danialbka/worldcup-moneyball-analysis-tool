use std::path::Path;

use anyhow::{Context, Result};
use rust_xlsxwriter::{Workbook, Worksheet};

use crate::analysis_fetch;
use crate::state::{
    LeagueMode, PlayerCareerEntry, PlayerCareerSection, PlayerDetail, PlayerMatchStat,
    PlayerSeasonTournamentStat, PlayerStatItem, PlayerTraitGroup, PlayerTrophyEntry, SquadPlayer,
    TeamAnalysis,
};

pub struct ExportReport {
    pub teams: usize,
    pub players: usize,
    pub stats: usize,
    pub info_rows: usize,
    pub season_breakdown: usize,
    pub career_rows: usize,
    pub trophies: usize,
    pub recent_matches: usize,
    pub errors: Vec<String>,
}

pub struct ExportProgress {
    pub current: usize,
    pub total: usize,
    pub message: String,
}

pub fn export_analysis_with_progress(
    path: &Path,
    mode: LeagueMode,
    mut on_progress: impl FnMut(ExportProgress),
) -> Result<ExportReport> {
    let analysis = match mode {
        LeagueMode::PremierLeague => analysis_fetch::fetch_premier_league_team_analysis(),
        LeagueMode::LaLiga => analysis_fetch::fetch_la_liga_team_analysis(),
        LeagueMode::Bundesliga => analysis_fetch::fetch_bundesliga_team_analysis(),
        LeagueMode::SerieA => analysis_fetch::fetch_serie_a_team_analysis(),
        LeagueMode::Ligue1 => analysis_fetch::fetch_ligue1_team_analysis(),
        LeagueMode::ChampionsLeague => analysis_fetch::fetch_champions_league_team_analysis(),
        LeagueMode::WorldCup => analysis_fetch::fetch_worldcup_team_analysis(),
    };
    let mut errors = analysis.errors;
    let mut total = analysis.teams.len();
    let mut current = 0usize;

    on_progress(ExportProgress {
        current,
        total,
        message: "Loaded analysis".to_string(),
    });

    let mut teams_rows = vec![vec![
        "Team ID".to_string(),
        "Team".to_string(),
        "Confed".to_string(),
        "Host".to_string(),
        "FIFA Rank".to_string(),
        "FIFA Points".to_string(),
        "FIFA Updated".to_string(),
    ]];

    let mut players_rows = vec![vec![
        "Team".to_string(),
        "Team ID".to_string(),
        "Player ID".to_string(),
        "Player".to_string(),
        "Role".to_string(),
        "Club".to_string(),
        "Age".to_string(),
        "Height (cm)".to_string(),
        "Shirt #".to_string(),
        "Market Value".to_string(),
    ]];

    let mut info_rows = vec![vec![
        "Team".to_string(),
        "Team ID".to_string(),
        "Player ID".to_string(),
        "Player".to_string(),
        "Team/Club".to_string(),
        "Position".to_string(),
        "Age".to_string(),
        "Country".to_string(),
        "Height".to_string(),
        "Preferred Foot".to_string(),
        "Shirt".to_string(),
        "Market Value".to_string(),
        "Contract End".to_string(),
        "Birth Date".to_string(),
        "Status".to_string(),
        "Injury Info".to_string(),
        "International Duty".to_string(),
        "Positions".to_string(),
    ]];

    let mut stats_rows = vec![vec![
        "Team".to_string(),
        "Team ID".to_string(),
        "Player ID".to_string(),
        "Player".to_string(),
        "Group".to_string(),
        "Context".to_string(),
        "Stat".to_string(),
        "Value".to_string(),
    ]];

    let mut season_rows = vec![vec![
        "Team".to_string(),
        "Team ID".to_string(),
        "Player ID".to_string(),
        "Player".to_string(),
        "League".to_string(),
        "Season".to_string(),
        "Appearances".to_string(),
        "Goals".to_string(),
        "Assists".to_string(),
        "Rating".to_string(),
    ]];

    let mut career_rows = vec![vec![
        "Team".to_string(),
        "Team ID".to_string(),
        "Player ID".to_string(),
        "Player".to_string(),
        "Section".to_string(),
        "Club".to_string(),
        "Start".to_string(),
        "End".to_string(),
        "Appearances".to_string(),
        "Goals".to_string(),
        "Assists".to_string(),
    ]];

    let mut trophies_rows = vec![vec![
        "Team".to_string(),
        "Team ID".to_string(),
        "Player ID".to_string(),
        "Player".to_string(),
        "Club".to_string(),
        "Competition".to_string(),
        "Seasons Won".to_string(),
        "Seasons Runner Up".to_string(),
    ]];

    let mut recent_rows = vec![vec![
        "Team".to_string(),
        "Team ID".to_string(),
        "Player ID".to_string(),
        "Player".to_string(),
        "Opponent".to_string(),
        "League".to_string(),
        "Date".to_string(),
        "Goals".to_string(),
        "Assists".to_string(),
        "Rating".to_string(),
    ]];

    for team in &analysis.teams {
        teams_rows.push(team_row(team));

        on_progress(ExportProgress {
            current,
            total,
            message: format!("Fetching squad: {}", team.name),
        });

        match analysis_fetch::fetch_team_squad(team.id) {
            Ok(squad) => {
                total = total.saturating_add(squad.players.len());
                current = current.saturating_add(1);
                on_progress(ExportProgress {
                    current,
                    total,
                    message: format!(
                        "Squad loaded: {} ({} players)",
                        team.name,
                        squad.players.len()
                    ),
                });

                for player in squad.players {
                    players_rows.push(player_row(team, &player));

                    match analysis_fetch::fetch_player_detail(player.id) {
                        Ok(detail) => {
                            info_rows.push(player_info_row(team, &detail));
                            stats_rows.extend(player_stats_rows(team, &detail));
                            season_rows.extend(player_season_rows(team, &detail));
                            career_rows.extend(player_career_rows(team, &detail));
                            trophies_rows.extend(player_trophy_rows(team, &detail));
                            recent_rows.extend(player_recent_rows(team, &detail));
                        }
                        Err(err) => errors.push(format!(
                            "player detail {} ({}): {err}",
                            player.name, player.id
                        )),
                    }

                    current = current.saturating_add(1);
                    on_progress(ExportProgress {
                        current,
                        total,
                        message: format!("Player: {} ({})", player.name, team.name),
                    });
                }
            }
            Err(err) => {
                errors.push(format!("squad {} ({}): {err}", team.name, team.id));
                current = current.saturating_add(1);
                on_progress(ExportProgress {
                    current,
                    total,
                    message: format!("Squad failed: {}", team.name),
                });
            }
        }
    }

    let mut workbook = Workbook::new();
    {
        let sheet = workbook.add_worksheet();
        sheet.set_name("Teams")?;
        write_rows(sheet, &teams_rows)?;
    }
    {
        let sheet = workbook.add_worksheet();
        sheet.set_name("Players")?;
        write_rows(sheet, &players_rows)?;
    }
    {
        let sheet = workbook.add_worksheet();
        sheet.set_name("PlayerInfo")?;
        write_rows(sheet, &info_rows)?;
    }
    {
        let sheet = workbook.add_worksheet();
        sheet.set_name("PlayerStats")?;
        write_rows(sheet, &stats_rows)?;
    }
    {
        let sheet = workbook.add_worksheet();
        sheet.set_name("SeasonBreakdown")?;
        write_rows(sheet, &season_rows)?;
    }
    {
        let sheet = workbook.add_worksheet();
        sheet.set_name("Career")?;
        write_rows(sheet, &career_rows)?;
    }
    {
        let sheet = workbook.add_worksheet();
        sheet.set_name("Trophies")?;
        write_rows(sheet, &trophies_rows)?;
    }
    {
        let sheet = workbook.add_worksheet();
        sheet.set_name("RecentMatches")?;
        write_rows(sheet, &recent_rows)?;
    }

    workbook
        .save(path)
        .with_context(|| format!("failed writing workbook to {}", path.display()))?;

    Ok(ExportReport {
        teams: analysis.teams.len(),
        players: players_rows.len().saturating_sub(1),
        stats: stats_rows.len().saturating_sub(1),
        info_rows: info_rows.len().saturating_sub(1),
        season_breakdown: season_rows.len().saturating_sub(1),
        career_rows: career_rows.len().saturating_sub(1),
        trophies: trophies_rows.len().saturating_sub(1),
        recent_matches: recent_rows.len().saturating_sub(1),
        errors,
    })
}

fn team_row(team: &TeamAnalysis) -> Vec<String> {
    vec![
        team.id.to_string(),
        team.name.clone(),
        format!("{:?}", team.confed),
        if team.host {
            "yes".to_string()
        } else {
            "no".to_string()
        },
        opt_to_string(team.fifa_rank),
        opt_to_string(team.fifa_points),
        team.fifa_updated.clone().unwrap_or_default(),
    ]
}

fn player_row(team: &TeamAnalysis, player: &SquadPlayer) -> Vec<String> {
    vec![
        team.name.clone(),
        team.id.to_string(),
        player.id.to_string(),
        player.name.clone(),
        player.role.clone(),
        player.club.clone(),
        opt_to_string(player.age),
        opt_to_string(player.height),
        opt_to_string(player.shirt_number),
        opt_to_string(player.market_value),
    ]
}

fn player_info_row(team: &TeamAnalysis, detail: &PlayerDetail) -> Vec<String> {
    let positions = if detail.positions.is_empty() {
        String::new()
    } else {
        detail.positions.join(", ")
    };

    vec![
        team.name.clone(),
        team.id.to_string(),
        detail.id.to_string(),
        detail.name.clone(),
        detail.team.clone().unwrap_or_default(),
        detail.position.clone().unwrap_or_default(),
        detail.age.clone().unwrap_or_default(),
        detail.country.clone().unwrap_or_default(),
        detail.height.clone().unwrap_or_default(),
        detail.preferred_foot.clone().unwrap_or_default(),
        detail.shirt.clone().unwrap_or_default(),
        detail.market_value.clone().unwrap_or_default(),
        detail.contract_end.clone().unwrap_or_default(),
        detail.birth_date.clone().unwrap_or_default(),
        detail.status.clone().unwrap_or_default(),
        detail.injury_info.clone().unwrap_or_default(),
        detail.international_duty.clone().unwrap_or_default(),
        positions,
    ]
}

fn player_stats_rows(team: &TeamAnalysis, detail: &PlayerDetail) -> Vec<Vec<String>> {
    let mut rows = Vec::new();

    let season_context = detail.all_competitions_season.as_deref().unwrap_or("");

    rows.extend(stat_items_rows(
        team,
        detail,
        "All Competitions",
        season_context,
        &detail.all_competitions,
    ));

    if let Some(league) = detail.main_league.as_ref() {
        let context = format!("{} {}", league.league_name, league.season);
        rows.extend(stat_items_rows(
            team,
            detail,
            "Main League",
            &context,
            &league.stats,
        ));
    }

    rows.extend(stat_items_rows(
        team,
        detail,
        "Top Stats",
        "",
        &detail.top_stats,
    ));

    for group in &detail.season_groups {
        rows.extend(stat_items_rows(
            team,
            detail,
            &group.title,
            "",
            &group.items,
        ));
    }

    if let Some(traits) = detail.traits.as_ref() {
        rows.extend(trait_rows(team, detail, traits));
    }

    rows
}

fn stat_items_rows(
    team: &TeamAnalysis,
    detail: &PlayerDetail,
    group: &str,
    context: &str,
    items: &[PlayerStatItem],
) -> Vec<Vec<String>> {
    items
        .iter()
        .map(|item| {
            vec![
                team.name.clone(),
                team.id.to_string(),
                detail.id.to_string(),
                detail.name.clone(),
                group.to_string(),
                context.to_string(),
                item.title.clone(),
                item.value.clone(),
            ]
        })
        .collect()
}

fn trait_rows(
    team: &TeamAnalysis,
    detail: &PlayerDetail,
    traits: &PlayerTraitGroup,
) -> Vec<Vec<String>> {
    traits
        .items
        .iter()
        .map(|item| {
            vec![
                team.name.clone(),
                team.id.to_string(),
                detail.id.to_string(),
                detail.name.clone(),
                traits.title.clone(),
                "Traits".to_string(),
                item.title.clone(),
                format!("{:.2}", item.value),
            ]
        })
        .collect()
}

fn player_season_rows(team: &TeamAnalysis, detail: &PlayerDetail) -> Vec<Vec<String>> {
    detail
        .season_breakdown
        .iter()
        .map(|row| season_row(team, detail, row))
        .collect()
}

fn season_row(
    team: &TeamAnalysis,
    detail: &PlayerDetail,
    row: &PlayerSeasonTournamentStat,
) -> Vec<String> {
    vec![
        team.name.clone(),
        team.id.to_string(),
        detail.id.to_string(),
        detail.name.clone(),
        row.league.clone(),
        row.season.clone(),
        row.appearances.clone(),
        row.goals.clone(),
        row.assists.clone(),
        row.rating.clone(),
    ]
}

fn player_career_rows(team: &TeamAnalysis, detail: &PlayerDetail) -> Vec<Vec<String>> {
    let mut rows = Vec::new();
    for section in &detail.career_sections {
        rows.extend(
            section
                .entries
                .iter()
                .map(|entry| career_row(team, detail, section, entry)),
        );
    }
    rows
}

fn career_row(
    team: &TeamAnalysis,
    detail: &PlayerDetail,
    section: &PlayerCareerSection,
    entry: &PlayerCareerEntry,
) -> Vec<String> {
    vec![
        team.name.clone(),
        team.id.to_string(),
        detail.id.to_string(),
        detail.name.clone(),
        section.title.clone(),
        entry.team.clone(),
        entry.start_date.clone().unwrap_or_default(),
        entry.end_date.clone().unwrap_or_default(),
        entry.appearances.clone().unwrap_or_default(),
        entry.goals.clone().unwrap_or_default(),
        entry.assists.clone().unwrap_or_default(),
    ]
}

fn player_trophy_rows(team: &TeamAnalysis, detail: &PlayerDetail) -> Vec<Vec<String>> {
    detail
        .trophies
        .iter()
        .map(|entry| trophy_row(team, detail, entry))
        .collect()
}

fn trophy_row(team: &TeamAnalysis, detail: &PlayerDetail, row: &PlayerTrophyEntry) -> Vec<String> {
    vec![
        team.name.clone(),
        team.id.to_string(),
        detail.id.to_string(),
        detail.name.clone(),
        row.team.clone(),
        row.league.clone(),
        row.seasons_won.join(", "),
        row.seasons_runner_up.join(", "),
    ]
}

fn player_recent_rows(team: &TeamAnalysis, detail: &PlayerDetail) -> Vec<Vec<String>> {
    detail
        .recent_matches
        .iter()
        .map(|entry| recent_row(team, detail, entry))
        .collect()
}

fn recent_row(team: &TeamAnalysis, detail: &PlayerDetail, row: &PlayerMatchStat) -> Vec<String> {
    vec![
        team.name.clone(),
        team.id.to_string(),
        detail.id.to_string(),
        detail.name.clone(),
        row.opponent.clone(),
        row.league.clone(),
        row.date.clone(),
        row.goals.to_string(),
        row.assists.to_string(),
        row.rating.clone().unwrap_or_default(),
    ]
}

fn opt_to_string<T: std::fmt::Display>(value: Option<T>) -> String {
    value.map(|v| v.to_string()).unwrap_or_default()
}

fn write_rows(worksheet: &mut Worksheet, rows: &[Vec<String>]) -> Result<()> {
    for (row_idx, row) in rows.iter().enumerate() {
        for (col_idx, value) in row.iter().enumerate() {
            worksheet
                .write_string(row_idx as u32, col_idx as u16, value)
                .with_context(|| format!("write cell ({row_idx},{col_idx})"))?;
        }
    }
    Ok(())
}
