use std::collections::{HashMap, HashSet};
use std::env;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use chrono::{Duration as ChronoDuration, NaiveDate, Utc};
use rand::Rng;
use rayon::prelude::*;

use crate::analysis_fetch;
use crate::state::{
    Delta, Event, EventKind, LineupSide, MatchDetail, MatchLineups, MatchSummary, ModelQuality,
    PlayerSlot, ProviderCommand, UpcomingMatch, WinProbRow,
};
use crate::upcoming_fetch::{self, FotmobMatchRow};

pub fn spawn_provider(tx: Sender<Delta>, cmd_rx: Receiver<ProviderCommand>) {
    thread::spawn(move || {
        let mut rng = rand::thread_rng();
        let lineups = Arc::new(seed_lineups().into_iter().collect::<HashMap<_, _>>());
        let pool = build_fetch_pool();
        let inflight_max = env::var("DETAILS_INFLIGHT_MAX")
            .ok()
            .and_then(|val| val.parse::<usize>().ok())
            .unwrap_or(8)
            .clamp(1, 64);
        let inflight_match_details: Arc<Mutex<HashSet<String>>> =
            Arc::new(Mutex::new(HashSet::new()));

        let allowed_league_ids = allowed_league_ids();

        let upcoming_source = env::var("UPCOMING_SOURCE")
            .unwrap_or_else(|_| "fotmob".to_string())
            .to_lowercase();
        let upcoming_date = opt_date_env("UPCOMING_DATE");
        let upcoming_window_days = env::var("UPCOMING_WINDOW_DAYS")
            .ok()
            .and_then(|val| val.parse::<usize>().ok())
            .unwrap_or(7)
            .clamp(1, 14);
        let upcoming_expand_days = env::var("UPCOMING_EXPAND_DAYS")
            .ok()
            .and_then(|val| val.parse::<usize>().ok())
            .unwrap_or(7)
            .clamp(1, 14);
        let upcoming_interval = Duration::from_secs(
            env::var("UPCOMING_POLL_SECS")
                .ok()
                .and_then(|val| val.parse::<u64>().ok())
                .unwrap_or(60)
                .max(10),
        );
        let mut last_upcoming = Instant::now() - upcoming_interval;

        let pulse_date = opt_date_env("PULSE_DATE");
        let live_interval = Duration::from_secs(
            env::var("PULSE_POLL_SECS")
                .ok()
                .and_then(|val| val.parse::<u64>().ok())
                .unwrap_or(15)
                .max(5),
        );
        let mut last_live_fetch = Instant::now() - live_interval;
        let mut last_minute_tick = Instant::now();
        let minute_interval = Duration::from_secs(60);
        let mut matches: Vec<MatchSummary> = Vec::new();

        if let Err(err) = refresh_live_matches(&mut matches, pulse_date.as_deref(), &tx) {
            let _ = tx.send(Delta::Log(format!("[WARN] Live fetch error: {err}")));
        }

        loop {
            thread::sleep(Duration::from_millis(900));

            if last_live_fetch.elapsed() >= live_interval {
                if let Err(err) = refresh_live_matches(&mut matches, pulse_date.as_deref(), &tx) {
                    let _ = tx.send(Delta::Log(format!("[WARN] Live fetch error: {err}")));
                }
                last_live_fetch = Instant::now();
            }

            if last_minute_tick.elapsed() >= minute_interval {
                let mut updated = false;
                for summary in &mut matches {
                    if summary.is_live && summary.minute < 90 {
                        summary.minute = summary.minute.saturating_add(1);
                        updated = true;
                    }
                }
                if updated {
                    for summary in matches.iter().cloned() {
                        let _ = tx.send(Delta::UpsertMatch(summary));
                    }
                }
                last_minute_tick = Instant::now();
            } else if !matches.is_empty() {
                let idx = rng.gen_range(0..matches.len());
                let summary = &mut matches[idx];
                if summary.is_live {
                    jitter_probs(&mut summary.win, &mut rng);
                    let _ = tx.send(Delta::UpsertMatch(summary.clone()));

                    if rng.gen_bool(0.12) {
                        let kind = match rng.gen_range(0..3) {
                            0 => EventKind::Shot,
                            1 => EventKind::Card,
                            _ => EventKind::Sub,
                        };
                        let desc = match kind {
                            EventKind::Shot => "Shot on target",
                            EventKind::Card => "Yellow card",
                            EventKind::Sub => "Substitution",
                            EventKind::Goal => "Goal",
                        };
                        let event = Event {
                            minute: summary.minute,
                            kind,
                            team: summary.home.clone(),
                            description: desc.to_string(),
                        };
                        let _ = tx.send(Delta::AddEvent {
                            id: summary.id.clone(),
                            event,
                        });
                        if kind == EventKind::Card {
                            let _ = tx.send(Delta::Log(format!(
                                "[INFO] Card: {} {}-{} {}",
                                summary.home, summary.score_home, summary.score_away, summary.away
                            )));
                        }
                    }
                }
            }

            while let Ok(cmd) = cmd_rx.try_recv() {
                match cmd {
                    ProviderCommand::FetchMatchDetails { fixture_id } => {
                        {
                            let mut inflight = inflight_match_details
                                .lock()
                                .expect("inflight match details lock poisoned");
                            if inflight.contains(&fixture_id) {
                                continue;
                            }
                            if inflight.len() >= inflight_max {
                                continue;
                            }
                            inflight.insert(fixture_id.clone());
                        }

                        let tx = tx.clone();
                        let lineups = lineups.clone();
                        let inflight_match_details = inflight_match_details.clone();
                        let fixture_id = fixture_id.clone();
                        let job = move || {
                            match upcoming_fetch::fetch_match_details_from_fotmob(&fixture_id) {
                                Ok(detail) => {
                                    let _ = tx.send(Delta::SetMatchDetails {
                                        id: fixture_id.clone(),
                                        detail,
                                    });
                                }
                                Err(err) => {
                                    let _ = tx.send(Delta::Log(format!(
                                        "[WARN] Match details error: {err}"
                                    )));
                                    if let Some(lineups) = lineups.get(&fixture_id) {
                                        let detail = MatchDetail {
                                            home_team: None,
                                            away_team: None,
                                            events: Vec::new(),
                                            commentary: Vec::new(),
                                            commentary_error: None,
                                            lineups: Some(lineups.clone()),
                                            stats: Vec::new(),
                                        };
                                        let _ = tx.send(Delta::SetMatchDetails {
                                            id: fixture_id.clone(),
                                            detail,
                                        });
                                    }
                                }
                            }
                            let mut inflight = inflight_match_details
                                .lock()
                                .expect("inflight match details lock poisoned");
                            inflight.remove(&fixture_id);
                        };

                        if let Some(pool) = pool.as_ref() {
                            pool.spawn(job);
                        } else {
                            std::thread::spawn(job);
                        }
                    }
                    ProviderCommand::FetchUpcoming => {
                        if last_upcoming.elapsed() < upcoming_interval {
                            let _ = tx.send(Delta::Log(format!(
                                "[INFO] Upcoming throttled ({}s)",
                                upcoming_interval.as_secs()
                            )));
                            continue;
                        }

                        let mut fetched = false;
                        if upcoming_source == "fotmob" || upcoming_source == "auto" {
                            match fetch_upcoming_window(
                                upcoming_date.as_deref(),
                                upcoming_window_days,
                                &allowed_league_ids,
                            ) {
                                Ok(items) if !items.is_empty() => {
                                    let _ = tx.send(Delta::SetUpcoming(items));
                                    fetched = true;
                                }
                                Ok(_) => {
                                    let _ = tx.send(Delta::Log(
                                        "[WARN] FotMob matchday returned no items for configured leagues"
                                            .to_string(),
                                    ));
                                    if upcoming_expand_days > upcoming_window_days {
                                        match fetch_upcoming_window(
                                            upcoming_date.as_deref(),
                                            upcoming_expand_days,
                                            &allowed_league_ids,
                                        ) {
                                            Ok(items) if !items.is_empty() => {
                                                let _ = tx.send(Delta::SetUpcoming(items));
                                                fetched = true;
                                            }
                                            Ok(_) => {}
                                            Err(err) => {
                                                let _ = tx.send(Delta::Log(format!(
                                                    "[WARN] FotMob expanded upcoming error: {err}"
                                                )));
                                            }
                                        }
                                    }
                                }
                                Err(err) => {
                                    let _ = tx.send(Delta::Log(format!(
                                        "[WARN] FotMob matchday error: {err}"
                                    )));
                                }
                            }
                        }

                        if !fetched {
                            let _ = tx.send(Delta::SetUpcoming(seed_upcoming()));
                        }
                        last_upcoming = Instant::now();
                    }
                    ProviderCommand::FetchAnalysis { mode } => {
                        let result = match mode {
                            crate::state::LeagueMode::PremierLeague => {
                                analysis_fetch::fetch_premier_league_team_analysis()
                            }
                            crate::state::LeagueMode::LaLiga => {
                                analysis_fetch::fetch_la_liga_team_analysis()
                            }
                            crate::state::LeagueMode::Bundesliga => {
                                analysis_fetch::fetch_bundesliga_team_analysis()
                            }
                            crate::state::LeagueMode::SerieA => {
                                analysis_fetch::fetch_serie_a_team_analysis()
                            }
                            crate::state::LeagueMode::Ligue1 => {
                                analysis_fetch::fetch_ligue1_team_analysis()
                            }
                            crate::state::LeagueMode::ChampionsLeague => {
                                analysis_fetch::fetch_champions_league_team_analysis()
                            }
                            crate::state::LeagueMode::WorldCup => {
                                analysis_fetch::fetch_worldcup_team_analysis()
                            }
                        };
                        for err in result.errors {
                            let _ = tx.send(Delta::Log(format!("[WARN] Analysis fetch: {err}")));
                        }
                        let _ = tx.send(Delta::SetAnalysis {
                            mode,
                            teams: result.teams,
                        });
                    }
                    ProviderCommand::WarmRankCacheFull { mode } => {
                        let tx = tx.clone();
                        std::thread::spawn(move || {
                            // Warm cache by fetching all squads + all player details once.
                            // The TUI will re-use cached data to compute rankings without re-fetching.
                            let analysis = match mode {
                                crate::state::LeagueMode::PremierLeague => {
                                    analysis_fetch::fetch_premier_league_team_analysis()
                                }
                                crate::state::LeagueMode::LaLiga => {
                                    analysis_fetch::fetch_la_liga_team_analysis()
                                }
                                crate::state::LeagueMode::Bundesliga => {
                                    analysis_fetch::fetch_bundesliga_team_analysis()
                                }
                                crate::state::LeagueMode::SerieA => {
                                    analysis_fetch::fetch_serie_a_team_analysis()
                                }
                                crate::state::LeagueMode::Ligue1 => {
                                    analysis_fetch::fetch_ligue1_team_analysis()
                                }
                                crate::state::LeagueMode::ChampionsLeague => {
                                    analysis_fetch::fetch_champions_league_team_analysis()
                                }
                                crate::state::LeagueMode::WorldCup => {
                                    analysis_fetch::fetch_worldcup_team_analysis()
                                }
                            };
                            let errors = std::sync::Mutex::new(analysis.errors);
                            let total = AtomicUsize::new(analysis.teams.len());
                            let current = AtomicUsize::new(0);
                            let pool = build_fetch_pool();
                            let _ = tx.send(Delta::RankCacheProgress {
                                mode,
                                current: 0,
                                total: total.load(Ordering::SeqCst),
                                message: "Loaded teams".to_string(),
                            });

                            for team in analysis.teams {
                                let _ = tx.send(Delta::RankCacheProgress {
                                    mode,
                                    current: current.load(Ordering::SeqCst),
                                    total: total.load(Ordering::SeqCst),
                                    message: format!("Fetching squad: {}", team.name),
                                });
                                match analysis_fetch::fetch_team_squad(team.id) {
                                    Ok(squad) => {
                                        total.fetch_add(squad.players.len(), Ordering::SeqCst);
                                        let current_val =
                                            current.fetch_add(1, Ordering::SeqCst) + 1;
                                        let _ = tx.send(Delta::CacheSquad {
                                            team_id: team.id,
                                            players: squad.players.clone(),
                                        });
                                        let _ = tx.send(Delta::RankCacheProgress {
                                            mode,
                                            current: current_val,
                                            total: total.load(Ordering::SeqCst),
                                            message: format!(
                                                "Squad loaded: {} ({} players)",
                                                team.name,
                                                squad.players.len()
                                            ),
                                        });

                                        let players = squad.players;
                                        let tx_players = tx.clone();
                                        let total_ref = &total;
                                        let current_ref = &current;
                                        let errors_ref = &errors;
                                        with_fetch_pool(&pool, || {
                                            players.par_iter().for_each(|player| {
                                                match analysis_fetch::fetch_player_detail(player.id)
                                                {
                                                    Ok(detail) => {
                                                        let _ = tx_players
                                                            .send(Delta::CachePlayerDetail(detail));
                                                    }
                                                    Err(err) => {
                                                        let mut guard = errors_ref.lock().unwrap();
                                                        guard.push(format!(
                                                            "player detail {} ({}): {err}",
                                                            player.name, player.id
                                                        ));
                                                    }
                                                }
                                                let current_val =
                                                    current_ref.fetch_add(1, Ordering::SeqCst) + 1;
                                                let _ = tx_players.send(Delta::RankCacheProgress {
                                                    mode,
                                                    current: current_val,
                                                    total: total_ref.load(Ordering::SeqCst),
                                                    message: format!(
                                                        "Player: {} ({})",
                                                        player.name, team.name
                                                    ),
                                                });
                                            });
                                        });
                                    }
                                    Err(err) => {
                                        let mut guard = errors.lock().unwrap();
                                        guard.push(format!(
                                            "squad {} ({}): {err}",
                                            team.name, team.id
                                        ));
                                        let current_val =
                                            current.fetch_add(1, Ordering::SeqCst) + 1;
                                        let _ = tx.send(Delta::RankCacheProgress {
                                            mode,
                                            current: current_val,
                                            total: total.load(Ordering::SeqCst),
                                            message: format!("Squad failed: {}", team.name),
                                        });
                                    }
                                }
                            }

                            let errors = errors.into_inner().unwrap_or_default();
                            let _ = tx.send(Delta::RankCacheFinished { mode, errors });
                        });
                    }
                    ProviderCommand::WarmRankCacheMissing {
                        mode,
                        team_ids,
                        player_ids,
                    } => {
                        let tx = tx.clone();
                        std::thread::spawn(move || {
                            let errors = std::sync::Mutex::new(Vec::<String>::new());
                            let total = AtomicUsize::new(team_ids.len() + player_ids.len());
                            let current = AtomicUsize::new(0);
                            let pool = build_fetch_pool();
                            if total.load(Ordering::SeqCst) == 0 {
                                let _ = tx.send(Delta::RankCacheFinished {
                                    mode,
                                    errors: Vec::new(),
                                });
                                return;
                            }
                            let _ = tx.send(Delta::RankCacheProgress {
                                mode,
                                current: 0,
                                total: total.load(Ordering::SeqCst),
                                message: "Warming missing cache".to_string(),
                            });

                            for team_id in team_ids {
                                let _ = tx.send(Delta::RankCacheProgress {
                                    mode,
                                    current: current.load(Ordering::SeqCst),
                                    total: total.load(Ordering::SeqCst),
                                    message: format!("Fetching squad: {team_id}"),
                                });
                                match analysis_fetch::fetch_team_squad(team_id) {
                                    Ok(squad) => {
                                        total.fetch_add(squad.players.len(), Ordering::SeqCst);
                                        let current_val =
                                            current.fetch_add(1, Ordering::SeqCst) + 1;
                                        let _ = tx.send(Delta::CacheSquad {
                                            team_id,
                                            players: squad.players.clone(),
                                        });
                                        let _ = tx.send(Delta::RankCacheProgress {
                                            mode,
                                            current: current_val,
                                            total: total.load(Ordering::SeqCst),
                                            message: format!(
                                                "Squad loaded: {team_id} ({} players)",
                                                squad.players.len()
                                            ),
                                        });

                                        let players = squad.players;
                                        let tx_players = tx.clone();
                                        let total_ref = &total;
                                        let current_ref = &current;
                                        let errors_ref = &errors;
                                        with_fetch_pool(&pool, || {
                                            players.par_iter().for_each(|player| {
                                                match analysis_fetch::fetch_player_detail(player.id)
                                                {
                                                    Ok(detail) => {
                                                        let _ = tx_players
                                                            .send(Delta::CachePlayerDetail(detail));
                                                    }
                                                    Err(err) => {
                                                        let mut guard = errors_ref.lock().unwrap();
                                                        guard.push(format!(
                                                            "player detail {} ({}): {err}",
                                                            player.name, player.id
                                                        ));
                                                    }
                                                }
                                                let current_val =
                                                    current_ref.fetch_add(1, Ordering::SeqCst) + 1;
                                                let _ = tx_players.send(Delta::RankCacheProgress {
                                                    mode,
                                                    current: current_val,
                                                    total: total_ref.load(Ordering::SeqCst),
                                                    message: format!(
                                                        "Player: {} ({team_id})",
                                                        player.name
                                                    ),
                                                });
                                            });
                                        });
                                    }
                                    Err(err) => {
                                        let mut guard = errors.lock().unwrap();
                                        guard.push(format!("squad {team_id}: {err}"));
                                        let current_val =
                                            current.fetch_add(1, Ordering::SeqCst) + 1;
                                        let _ = tx.send(Delta::RankCacheProgress {
                                            mode,
                                            current: current_val,
                                            total: total.load(Ordering::SeqCst),
                                            message: format!("Squad failed: {team_id}"),
                                        });
                                    }
                                }
                            }

                            let tx_players = tx.clone();
                            let total_ref = &total;
                            let current_ref = &current;
                            let errors_ref = &errors;
                            with_fetch_pool(&pool, || {
                                player_ids.par_iter().for_each(|player_id| {
                                    let _ = tx_players.send(Delta::RankCacheProgress {
                                        mode,
                                        current: current_ref.load(Ordering::SeqCst),
                                        total: total_ref.load(Ordering::SeqCst),
                                        message: format!("Fetching player: {player_id}"),
                                    });
                                    match analysis_fetch::fetch_player_detail(*player_id) {
                                        Ok(detail) => {
                                            let _ =
                                                tx_players.send(Delta::CachePlayerDetail(detail));
                                        }
                                        Err(err) => {
                                            let mut guard = errors_ref.lock().unwrap();
                                            guard.push(format!("player detail {player_id}: {err}"));
                                        }
                                    }
                                    let current_val =
                                        current_ref.fetch_add(1, Ordering::SeqCst) + 1;
                                    let _ = tx_players.send(Delta::RankCacheProgress {
                                        mode,
                                        current: current_val,
                                        total: total_ref.load(Ordering::SeqCst),
                                        message: format!("Player cached: {player_id}"),
                                    });
                                });
                            });

                            let errors = errors.into_inner().unwrap_or_default();
                            let _ = tx.send(Delta::RankCacheFinished { mode, errors });
                        });
                    }
                    ProviderCommand::FetchSquad { team_id, team_name } => {
                        match analysis_fetch::fetch_team_squad(team_id) {
                            Ok(squad) => {
                                let _ = tx.send(Delta::SetSquad {
                                    team_name: squad.team_name,
                                    team_id,
                                    players: squad.players,
                                });
                            }
                            Err(err) => {
                                let _ = tx
                                    .send(Delta::Log(format!("[WARN] Squad fetch failed: {err}")));
                                let _ = tx.send(Delta::SetSquad {
                                    team_name,
                                    team_id,
                                    players: Vec::new(),
                                });
                            }
                        }
                    }
                    ProviderCommand::FetchPlayer {
                        player_id,
                        player_name,
                    } => match analysis_fetch::fetch_player_detail(player_id) {
                        Ok(detail) => {
                            let _ = tx.send(Delta::SetPlayerDetail(detail));
                        }
                        Err(err) => {
                            let _ =
                                tx.send(Delta::Log(format!("[WARN] Player fetch failed: {err}")));
                            let _ = tx.send(Delta::SetPlayerDetail(crate::state::PlayerDetail {
                                id: player_id,
                                name: player_name,
                                team: None,
                                position: None,
                                age: None,
                                country: None,
                                height: None,
                                preferred_foot: None,
                                shirt: None,
                                market_value: None,
                                contract_end: None,
                                birth_date: None,
                                status: None,
                                injury_info: None,
                                international_duty: None,
                                positions: Vec::new(),
                                all_competitions: Vec::new(),
                                all_competitions_season: None,
                                main_league: None,
                                top_stats: Vec::new(),
                                season_groups: Vec::new(),
                                season_performance: Vec::new(),
                                traits: None,
                                recent_matches: Vec::new(),
                                season_breakdown: Vec::new(),
                                career_sections: Vec::new(),
                                trophies: Vec::new(),
                            }));
                        }
                    },
                    ProviderCommand::PrefetchPlayers { player_ids } => {
                        let tx = tx.clone();
                        std::thread::spawn(move || {
                            let errors = std::sync::Mutex::new(Vec::<String>::new());
                            let pool = build_fetch_pool();
                            with_fetch_pool(&pool, || {
                                player_ids.par_iter().for_each(|player_id| {
                                    match analysis_fetch::fetch_player_detail(*player_id) {
                                        Ok(detail) => {
                                            let _ = tx.send(Delta::CachePlayerDetail(detail));
                                        }
                                        Err(err) => {
                                            let mut guard = errors.lock().unwrap();
                                            guard.push(format!(
                                                "prefetch player {player_id}: {err}"
                                            ));
                                        }
                                    }
                                });
                            });
                            let errors = errors.into_inner().unwrap_or_default();
                            if !errors.is_empty() {
                                let _ = tx.send(Delta::Log(format!(
                                    "[WARN] Player prefetch: {} errors",
                                    errors.len()
                                )));
                            }
                        });
                    }
                    ProviderCommand::ExportAnalysis { path, mode } => {
                        let tx = tx.clone();
                        std::thread::spawn(move || {
                            let _ = tx.send(Delta::ExportStarted {
                                path: path.clone(),
                                total: 0,
                            });

                            let progress_tx = tx.clone();
                            let progress_path = path.clone();
                            let mut last_current = 0usize;
                            let mut last_total = 0usize;

                            let report = crate::analysis_export::export_analysis_with_progress(
                                path.as_ref(),
                                mode,
                                |progress| {
                                    last_current = progress.current;
                                    last_total = progress.total;
                                    let _ = progress_tx.send(Delta::ExportProgress {
                                        current: progress.current,
                                        total: progress.total,
                                        message: progress.message,
                                    });
                                },
                            );

                            match report {
                                Ok(report) => {
                                    let _ = tx.send(Delta::ExportFinished {
                                        path: progress_path,
                                        current: last_current.max(last_total),
                                        total: last_total,
                                        teams: report.teams,
                                        players: report.players,
                                        stats: report.stats,
                                        info_rows: report.info_rows,
                                        season_breakdown: report.season_breakdown,
                                        career_rows: report.career_rows,
                                        trophies: report.trophies,
                                        recent_matches: report.recent_matches,
                                        errors: report.errors.len(),
                                    });
                                }
                                Err(err) => {
                                    let _ =
                                        tx.send(Delta::Log(format!("[WARN] Export failed: {err}")));
                                    let _ = tx.send(Delta::ExportFinished {
                                        path: progress_path,
                                        current: last_current,
                                        total: last_total,
                                        teams: 0,
                                        players: 0,
                                        stats: 0,
                                        info_rows: 0,
                                        season_breakdown: 0,
                                        career_rows: 0,
                                        trophies: 0,
                                        recent_matches: 0,
                                        errors: 1,
                                    });
                                }
                            }
                        });
                    }
                }
            }
        }
    });
}

fn refresh_live_matches(
    matches: &mut Vec<MatchSummary>,
    date: Option<&str>,
    tx: &Sender<Delta>,
) -> anyhow::Result<()> {
    let rows = upcoming_fetch::fetch_matches_from_fotmob(date)?;
    let updated = merge_fotmob_matches(rows, std::mem::take(matches), tx);
    *matches = updated;
    let _ = tx.send(Delta::SetMatches(matches.clone()));
    Ok(())
}

fn merge_fotmob_matches(
    rows: Vec<FotmobMatchRow>,
    existing: Vec<MatchSummary>,
    tx: &Sender<Delta>,
) -> Vec<MatchSummary> {
    let mut previous: HashMap<String, MatchSummary> =
        existing.into_iter().map(|m| (m.id.clone(), m)).collect();
    let mut output = Vec::new();

    for row in rows {
        let prev = previous.remove(&row.id);
        let is_live = row.started && !row.finished && !row.cancelled;
        let minute = if is_live {
            row.minute
                .or_else(|| prev.as_ref().map(|m| m.minute))
                .unwrap_or(1)
        } else if row.finished {
            90
        } else {
            0
        };

        let mut win = prev
            .as_ref()
            .map(|m| m.win.clone())
            .unwrap_or_else(|| seed_win_prob(row.home_score, row.away_score, is_live));
        win.quality = if is_live {
            ModelQuality::Event
        } else {
            ModelQuality::Basic
        };

        if let Some(prev) = &prev
            && (row.home_score != prev.score_home || row.away_score != prev.score_away)
        {
            let scoring_team = if row.home_score > prev.score_home {
                row.home.clone()
            } else {
                row.away.clone()
            };
            let event = Event {
                minute,
                kind: EventKind::Goal,
                team: scoring_team.clone(),
                description: "Goal".to_string(),
            };
            let _ = tx.send(Delta::AddEvent {
                id: row.id.clone(),
                event,
            });
            let _ = tx.send(Delta::Log(format!(
                "[ALERT] Goal: {} {}-{} {}",
                scoring_team, row.home_score, row.away_score, row.away
            )));
            win = seed_win_prob(row.home_score, row.away_score, is_live);
            win.quality = if is_live {
                ModelQuality::Event
            } else {
                ModelQuality::Basic
            };
        }

        output.push(MatchSummary {
            id: row.id.clone(),
            league_id: Some(row.league_id),
            league_name: row.league_name.clone(),
            home: abbreviate_team(&row.home),
            away: abbreviate_team(&row.away),
            minute,
            score_home: row.home_score,
            score_away: row.away_score,
            win,
            is_live,
        });
    }

    output
}

fn opt_env(key: &str) -> Option<String> {
    env::var(key).ok().and_then(|val| {
        if val.trim().is_empty() {
            None
        } else {
            Some(val)
        }
    })
}

fn opt_date_env(key: &str) -> Option<String> {
    opt_env(key).map(|val| normalize_fotmob_date(&val))
}

fn normalize_fotmob_date(raw: &str) -> String {
    let digits: String = raw.chars().filter(|c| c.is_ascii_digit()).collect();
    if digits.len() == 8 {
        digits
    } else {
        raw.trim().to_string()
    }
}

fn fetch_upcoming_window(
    base_date: Option<&str>,
    days: usize,
    allowed_league_ids: &HashSet<u32>,
) -> anyhow::Result<Vec<UpcomingMatch>> {
    let mut all = Vec::new();
    let mut seen: HashMap<String, bool> = HashMap::new();
    let dates = upcoming_dates(base_date, days);

    for date in dates {
        match upcoming_fetch::fetch_upcoming_from_fotmob(Some(&date)) {
            Ok(items) => {
                for item in items {
                    if let Some(id) = item.league_id
                        && !allowed_league_ids.is_empty()
                        && !allowed_league_ids.contains(&id)
                    {
                        continue;
                    }
                    if seen.insert(item.id.clone(), true).is_none() {
                        all.push(item);
                    }
                }
            }
            Err(err) => {
                return Err(err);
            }
        }
    }

    Ok(all)
}

fn allowed_league_ids() -> HashSet<u32> {
    let mut ids = HashSet::new();
    extend_ids_env_or_default(&mut ids, "APP_LEAGUE_PREMIER_IDS", &[47]);
    extend_ids_env_or_default(&mut ids, "APP_LEAGUE_LALIGA_IDS", &[87]);
    extend_ids_env_or_default(&mut ids, "APP_LEAGUE_BUNDESLIGA_IDS", &[54]);
    extend_ids_env_or_default(&mut ids, "APP_LEAGUE_SERIE_A_IDS", &[55]);
    extend_ids_env_or_default(&mut ids, "APP_LEAGUE_LIGUE1_IDS", &[53]);
    extend_ids_env_or_default(&mut ids, "APP_LEAGUE_CHAMPIONS_LEAGUE_IDS", &[42]);
    extend_ids_env_or_default(&mut ids, "APP_LEAGUE_WORLDCUP_IDS", &[77]);
    ids
}

fn extend_ids_env_or_default(out: &mut HashSet<u32>, key: &str, defaults: &[u32]) {
    match env::var(key) {
        Ok(raw) => {
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                return;
            }
            for part in trimmed.split([',', ';', ' ']) {
                if let Ok(id) = part.trim().parse::<u32>() {
                    out.insert(id);
                }
            }
        }
        Err(_) => {
            for id in defaults {
                out.insert(*id);
            }
        }
    }
}

fn upcoming_dates(base_date: Option<&str>, days: usize) -> Vec<String> {
    let base = parse_date(base_date).unwrap_or_else(|| Utc::now().date_naive());
    let total = days.max(1);
    (0..total)
        .filter_map(|offset| base.checked_add_signed(ChronoDuration::days(offset as i64)))
        .map(|date| date.format("%Y%m%d").to_string())
        .collect()
}

fn parse_date(raw: Option<&str>) -> Option<NaiveDate> {
    let raw = raw?;
    let digits: String = raw.chars().filter(|c| c.is_ascii_digit()).collect();
    if digits.len() == 8 {
        NaiveDate::parse_from_str(&digits, "%Y%m%d").ok()
    } else {
        None
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

fn seed_upcoming() -> Vec<UpcomingMatch> {
    vec![
        UpcomingMatch {
            id: "upc-pl-1".to_string(),
            league_id: Some(47),
            league_name: "Premier League".to_string(),
            round: "Matchday 12".to_string(),
            kickoff: "2024-11-09T17:30".to_string(),
            home: "ARS".to_string(),
            away: "CHE".to_string(),
        },
        UpcomingMatch {
            id: "upc-pl-2".to_string(),
            league_id: Some(47),
            league_name: "Premier League".to_string(),
            round: "Matchday 12".to_string(),
            kickoff: "2024-11-10T14:00".to_string(),
            home: "MCI".to_string(),
            away: "LIV".to_string(),
        },
        UpcomingMatch {
            id: "upc-ll-1".to_string(),
            league_id: Some(87),
            league_name: "La Liga".to_string(),
            round: "Matchday 12".to_string(),
            kickoff: "2024-11-09T20:00".to_string(),
            home: "RMA".to_string(),
            away: "BAR".to_string(),
        },
        UpcomingMatch {
            id: "upc-ll-2".to_string(),
            league_id: Some(87),
            league_name: "La Liga".to_string(),
            round: "Matchday 12".to_string(),
            kickoff: "2024-11-10T16:15".to_string(),
            home: "ATM".to_string(),
            away: "SEV".to_string(),
        },
        UpcomingMatch {
            id: "upc-bl-1".to_string(),
            league_id: Some(54),
            league_name: "Bundesliga".to_string(),
            round: "Matchday 12".to_string(),
            kickoff: "2024-11-09T15:30".to_string(),
            home: "BAY".to_string(),
            away: "DOR".to_string(),
        },
        UpcomingMatch {
            id: "upc-bl-2".to_string(),
            league_id: Some(54),
            league_name: "Bundesliga".to_string(),
            round: "Matchday 12".to_string(),
            kickoff: "2024-11-10T17:30".to_string(),
            home: "RBL".to_string(),
            away: "LEV".to_string(),
        },
        UpcomingMatch {
            id: "upc-sa-1".to_string(),
            league_id: Some(55),
            league_name: "Serie A".to_string(),
            round: "Matchday 12".to_string(),
            kickoff: "2024-11-09T18:00".to_string(),
            home: "JUV".to_string(),
            away: "INT".to_string(),
        },
        UpcomingMatch {
            id: "upc-sa-2".to_string(),
            league_id: Some(55),
            league_name: "Serie A".to_string(),
            round: "Matchday 12".to_string(),
            kickoff: "2024-11-10T20:45".to_string(),
            home: "ACM".to_string(),
            away: "NAP".to_string(),
        },
        UpcomingMatch {
            id: "upc-l1-1".to_string(),
            league_id: Some(53),
            league_name: "Ligue 1".to_string(),
            round: "Matchday 12".to_string(),
            kickoff: "2024-11-09T21:00".to_string(),
            home: "PSG".to_string(),
            away: "MAR".to_string(),
        },
        UpcomingMatch {
            id: "upc-l1-2".to_string(),
            league_id: Some(53),
            league_name: "Ligue 1".to_string(),
            round: "Matchday 12".to_string(),
            kickoff: "2024-11-10T15:00".to_string(),
            home: "LYO".to_string(),
            away: "MON".to_string(),
        },
        UpcomingMatch {
            id: "upc-cl-1".to_string(),
            league_id: Some(42),
            league_name: "Champions League".to_string(),
            round: "Round of 16".to_string(),
            kickoff: "2025-03-04T20:00".to_string(),
            home: "RMA".to_string(),
            away: "MCI".to_string(),
        },
        UpcomingMatch {
            id: "upc-cl-2".to_string(),
            league_id: Some(42),
            league_name: "Champions League".to_string(),
            round: "Round of 16".to_string(),
            kickoff: "2025-03-05T20:00".to_string(),
            home: "BAR".to_string(),
            away: "BAY".to_string(),
        },
        UpcomingMatch {
            id: "upc-wc-1".to_string(),
            league_id: Some(77),
            league_name: "World Cup".to_string(),
            round: "Group Stage - 1".to_string(),
            kickoff: "2026-06-12T20:00".to_string(),
            home: "USA".to_string(),
            away: "CAN".to_string(),
        },
        UpcomingMatch {
            id: "upc-wc-2".to_string(),
            league_id: Some(77),
            league_name: "World Cup".to_string(),
            round: "Group Stage - 1".to_string(),
            kickoff: "2026-06-13T18:00".to_string(),
            home: "MEX".to_string(),
            away: "BRA".to_string(),
        },
    ]
}

fn seed_lineups() -> Vec<(String, MatchLineups)> {
    let bra = LineupSide {
        team: "Brazil".to_string(),
        team_abbr: "BRA".to_string(),
        formation: "4-3-3".to_string(),
        starting: vec![
            player("Alisson", 1, "GK"),
            player("Marquinhos", 4, "DF"),
            player("Thiago Silva", 3, "DF"),
            player("Casemiro", 5, "MF"),
            player("Paqueta", 7, "MF"),
            player("Vini Jr", 10, "FW"),
        ],
        subs: vec![player("Rodrygo", 11, "FW"), player("Bruno G", 8, "MF")],
    };

    let ger = LineupSide {
        team: "Germany".to_string(),
        team_abbr: "GER".to_string(),
        formation: "4-2-3-1".to_string(),
        starting: vec![
            player("Neuer", 1, "GK"),
            player("Rudiger", 2, "DF"),
            player("Kimmich", 6, "MF"),
            player("Musiala", 10, "MF"),
            player("Gnabry", 11, "FW"),
        ],
        subs: vec![player("Havertz", 9, "FW"), player("Wirtz", 17, "MF")],
    };

    vec![(
        "bra-ger".to_string(),
        MatchLineups {
            sides: vec![bra, ger],
        },
    )]
}

fn player(name: &str, number: u32, pos: &str) -> PlayerSlot {
    PlayerSlot {
        id: None,
        name: name.to_string(),
        number: Some(number),
        pos: Some(pos.to_string()),
    }
}

fn seed_win_prob(home_score: u8, away_score: u8, is_live: bool) -> WinProbRow {
    let diff = home_score as i16 - away_score as i16;
    let (p_home, p_draw, p_away) = if !is_live && diff != 0 {
        if diff > 0 {
            (100.0, 0.0, 0.0)
        } else {
            (0.0, 0.0, 100.0)
        }
    } else if diff == 0 {
        (42.0, 30.0, 28.0)
    } else if diff == 1 {
        (58.0, 25.0, 17.0)
    } else if diff == -1 {
        (22.0, 28.0, 50.0)
    } else if diff >= 2 {
        (75.0, 15.0, 10.0)
    } else {
        (10.0, 15.0, 75.0)
    };

    WinProbRow {
        p_home,
        p_draw,
        p_away,
        delta_home: 0.0,
        quality: ModelQuality::Basic,
        confidence: if is_live { 68 } else { 84 },
    }
}

fn jitter_probs(win: &mut WinProbRow, rng: &mut impl Rng) {
    let home = (win.p_home + rng.gen_range(-2.5..2.5)).max(1.0);
    let draw = (win.p_draw + rng.gen_range(-1.5..1.5)).max(1.0);
    let away = (win.p_away + rng.gen_range(-2.5..2.5)).max(1.0);
    let sum = home + draw + away;

    win.p_home = home / sum * 100.0;
    win.p_draw = draw / sum * 100.0;
    win.p_away = away / sum * 100.0;
}

fn build_fetch_pool() -> Option<rayon::ThreadPool> {
    let threads = fetch_parallelism();
    rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .build()
        .ok()
}

fn with_fetch_pool<T>(pool: &Option<rayon::ThreadPool>, action: impl FnOnce() -> T + Send) -> T
where
    T: Send,
{
    if let Some(pool) = pool.as_ref() {
        pool.install(action)
    } else {
        action()
    }
}

fn fetch_parallelism() -> usize {
    env::var("FETCH_PARALLELISM")
        .ok()
        .and_then(|val| val.parse::<usize>().ok())
        .unwrap_or(6)
        .clamp(2, 32)
}
