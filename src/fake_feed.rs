use std::collections::HashMap;
use std::env;
use std::sync::mpsc::{Receiver, Sender};
use std::thread;
use std::time::{Duration, Instant};

use chrono::{Duration as ChronoDuration, NaiveDate, Utc};
use rand::Rng;

use crate::state::{
    Delta, Event, EventKind, LineupSide, MatchDetail, MatchLineups, MatchSummary, ModelQuality,
    PlayerSlot, ProviderCommand, UpcomingMatch, WinProbRow,
};
use crate::upcoming_fetch::{self, FotmobMatchRow};

pub fn spawn_fake_provider(tx: Sender<Delta>, cmd_rx: Receiver<ProviderCommand>) {
    thread::spawn(move || {
        let mut rng = rand::thread_rng();
        let lineups = seed_lineups()
            .into_iter()
            .map(|(id, lineup)| (id, lineup))
            .collect::<HashMap<_, _>>();

        let upcoming_source = env::var("UPCOMING_SOURCE")
            .unwrap_or_else(|_| "fotmob".to_string())
            .to_lowercase();
        let upcoming_date = opt_date_env("UPCOMING_DATE");
        let upcoming_window_days = env::var("UPCOMING_WINDOW_DAYS")
            .ok()
            .and_then(|val| val.parse::<usize>().ok())
            .unwrap_or(1)
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

        if let Err(err) = refresh_live_matches(
            &mut matches,
            pulse_date.as_deref(),
            &tx,
        ) {
            let _ = tx.send(Delta::Log(format!(
                "[WARN] Live fetch error: {err}"
            )));
        }

        loop {
            thread::sleep(Duration::from_millis(900));

            if last_live_fetch.elapsed() >= live_interval {
                if let Err(err) = refresh_live_matches(
                    &mut matches,
                    pulse_date.as_deref(),
                    &tx,
                ) {
                    let _ = tx.send(Delta::Log(format!(
                        "[WARN] Live fetch error: {err}"
                    )));
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
                        match upcoming_fetch::fetch_match_details_from_fotmob(&fixture_id) {
                            Ok(detail) => {
                                let _ = tx.send(Delta::SetMatchDetails {
                                    id: fixture_id,
                                    detail,
                                });
                            }
                            Err(err) => {
                                let _ = tx.send(Delta::Log(format!(
                                    "[WARN] Match details error: {err}"
                                )));
                                if let Some(lineups) = lineups.get(&fixture_id) {
                                    let detail = MatchDetail {
                                        events: Vec::new(),
                                        lineups: Some(lineups.clone()),
                                        stats: Vec::new(),
                                    };
                                    let _ = tx.send(Delta::SetMatchDetails {
                                        id: fixture_id,
                                        detail,
                                    });
                                }
                            }
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
                            ) {
                                Ok(items) if !items.is_empty() => {
                                    let _ = tx.send(Delta::SetUpcoming(items));
                                    fetched = true;
                                }
                                Ok(_) => {
                                    let _ = tx.send(Delta::Log(
                                        "[WARN] FotMob matchday returned no items".to_string(),
                                    ));
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
            prev.as_ref().map(|m| m.minute).unwrap_or(1)
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

        if let Some(prev) = &prev {
            if row.home_score != prev.score_home || row.away_score != prev.score_away {
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
    env::var(key)
        .ok()
        .and_then(|val| if val.trim().is_empty() { None } else { Some(val) })
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
) -> anyhow::Result<Vec<UpcomingMatch>> {
    let mut all = Vec::new();
    let mut seen: HashMap<String, bool> = HashMap::new();
    let dates = upcoming_dates(base_date, days);

    for date in dates {
        match upcoming_fetch::fetch_upcoming_from_fotmob(Some(&date)) {
            Ok(items) => {
                for item in items {
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
        subs: vec![
            player("Rodrygo", 11, "FW"),
            player("Bruno G", 8, "MF"),
        ],
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
