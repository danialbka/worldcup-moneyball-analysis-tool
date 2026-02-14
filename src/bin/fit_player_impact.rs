use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::RowAccessor;

use wc26_terminal::analysis_fetch;
use wc26_terminal::historical_dataset;
use wc26_terminal::player_impact::{
    LeaguePlayerImpactArtifact, PlayerImpactEntry, PlayerImpactRegistryArtifact,
};

const MATCHES_PARQUET_URL: &str = "https://588738577887-baselight-crawlers-prod-ue1-datasets.s3.us-east-1.amazonaws.com/iceberg_catalog/blt-blt109x9ors0t7l8jzs3oy8sc6wjhai06r3x7pkf39773uevkegqe4wyqct7907ky3jmo/ultimate_soccer_dataset/matches/matches_v2026-02-13_40fa0e3d.parquet";
const PLAYER_STATS_PARQUET_URL: &str = "https://588738577887-baselight-crawlers-prod-ue1-datasets.s3.us-east-1.amazonaws.com/iceberg_catalog/blt-blt109x9ors0t7l8jzs3oy8sc6wjhai06r3x7pkf39773uevkegqe4wyqct7907ky3jmo/ultimate_soccer_dataset/match_player_stats/match_player_stats_v2026-02-13_03cb9794.parquet";

const DEFAULT_LEAGUE_IDS: &[u32] = &[47, 87, 54, 55, 53, 42, 77];
const DEFAULT_MIN_SAMPLES: u32 = 4;

#[derive(Debug, Clone)]
struct MatchMeta {
    league_id: u32,
    home_norm: String,
    away_norm: String,
    outcome_home: i8,
}

#[derive(Debug, Clone, Default)]
struct OnlineStat {
    weighted_sum: f64,
    weighted_n: f64,
    samples: u32,
    minutes: f64,
    rating_sum: f64,
    shots_on_target_sum: f64,
    key_passes_sum: f64,
    tackles_interceptions_sum: f64,
    duel_win_rate_sum: f64,
    cards_sum: f64,
}

impl OnlineStat {
    fn push(&mut self, blended: f64, minutes_w: f64, feats: FeatureObs) {
        self.weighted_sum += blended * minutes_w;
        self.weighted_n += minutes_w;
        self.samples = self.samples.saturating_add(1);
        self.minutes += minutes_w * 90.0;
        self.rating_sum += feats.rating * minutes_w;
        self.shots_on_target_sum += feats.shots_on_target * minutes_w;
        self.key_passes_sum += feats.key_passes * minutes_w;
        self.tackles_interceptions_sum += feats.tackles_interceptions * minutes_w;
        self.duel_win_rate_sum += feats.duel_win_rate * minutes_w;
        self.cards_sum += feats.cards * minutes_w;
    }

    fn as_entry(
        &self,
        team_norm: &str,
        player_norm: &str,
        min_samples: u32,
    ) -> Option<PlayerImpactEntry> {
        if self.samples < min_samples || self.weighted_n <= 0.0 {
            return None;
        }
        let shrink = (self.samples as f64 / 24.0).clamp(0.15, 1.0);
        Some(PlayerImpactEntry {
            team_norm: team_norm.to_string(),
            player_norm: player_norm.to_string(),
            prior: ((self.weighted_sum / self.weighted_n) * shrink).clamp(-1.0, 1.0),
            samples: self.samples,
            minutes: self.minutes,
            rating: self.rating_sum / self.weighted_n,
            shots_on_target: self.shots_on_target_sum / self.weighted_n,
            key_passes: self.key_passes_sum / self.weighted_n,
            tackles_interceptions: self.tackles_interceptions_sum / self.weighted_n,
            duel_win_rate: self.duel_win_rate_sum / self.weighted_n,
            cards: self.cards_sum / self.weighted_n,
        })
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct FeatureObs {
    rating: f64,
    shots_on_target: f64,
    key_passes: f64,
    tackles_interceptions: f64,
    duel_win_rate: f64,
    cards: f64,
}

#[derive(Debug, Clone, Copy)]
struct TeamAgg {
    sum_outcome: f64,
    samples: u32,
}

fn main() -> Result<()> {
    let out_path =
        parse_out_arg().unwrap_or_else(|| PathBuf::from("assets/player_impact_registry_v1.json"));
    let league_ids = parse_league_ids_arg().unwrap_or_else(default_league_ids_from_env);
    if league_ids.is_empty() {
        return Err(anyhow!("no league ids resolved"));
    }
    let min_samples = parse_u32_arg("--min-samples")
        .unwrap_or(DEFAULT_MIN_SAMPLES)
        .max(1);

    let mut artifacts = train_from_baselight(&league_ids, min_samples).unwrap_or_else(|err| {
        eprintln!("[WARN] baselight training failed: {err}");
        HashMap::new()
    });

    let mut missing = Vec::new();
    for league_id in &league_ids {
        let absent = artifacts
            .get(league_id)
            .map(|a| a.entries.is_empty())
            .unwrap_or(true);
        if absent {
            missing.push(*league_id);
        }
    }

    if !missing.is_empty() {
        eprintln!(
            "[INFO] falling back to fotmob-derived player priors for leagues {:?}",
            missing
        );
        let fallback = train_from_fotmob_fallback(&missing, min_samples)?;
        for (league_id, artifact) in fallback {
            artifacts.insert(league_id, artifact);
        }
    }

    let mut leagues = artifacts.into_values().collect::<Vec<_>>();
    leagues.sort_by_key(|a| a.league_id);
    if leagues.is_empty() {
        return Err(anyhow!("no league artifacts produced"));
    }

    let shared_prior = build_shared_prior(&leagues, min_samples);
    let out = PlayerImpactRegistryArtifact {
        version: 1,
        generated_at: chrono::Utc::now().to_rfc3339(),
        source: Some("baselight_plus_fotmob_fallback".to_string()),
        leagues,
        shared_prior,
    };

    if let Some(parent) = out_path.parent() {
        fs::create_dir_all(parent).ok();
    }
    let raw = serde_json::to_string_pretty(&out).context("serialize registry")?;
    fs::write(&out_path, raw).with_context(|| format!("write {}", out_path.display()))?;

    println!("player-impact registry written: {}", out_path.display());
    Ok(())
}

fn train_from_baselight(
    league_ids: &[u32],
    min_samples: u32,
) -> Result<HashMap<u32, LeaguePlayerImpactArtifact>> {
    let tmp_dir = std::env::temp_dir().join("wc26_fit_player_impact");
    fs::create_dir_all(&tmp_dir).context("create temp directory")?;

    let matches_url = std::env::var("BASELIGHT_MATCHES_PARQUET_URL")
        .unwrap_or_else(|_| MATCHES_PARQUET_URL.to_string());
    let stats_url = std::env::var("BASELIGHT_PLAYER_STATS_PARQUET_URL")
        .unwrap_or_else(|_| PLAYER_STATS_PARQUET_URL.to_string());

    let matches_path = download_file(&matches_url, &tmp_dir.join("matches.parquet"))?;
    let stats_path = download_file(&stats_url, &tmp_dir.join("match_player_stats.parquet"))?;

    let league_set = league_ids.iter().copied().collect::<HashSet<_>>();
    let (match_meta, match_counts) = read_matches(&matches_path, &league_set)?;

    if match_meta.is_empty() {
        return Err(anyhow!(
            "no target-league matches found in baselight parquet"
        ));
    }

    let mut stats: HashMap<(u32, String, String), OnlineStat> = HashMap::new();
    ingest_player_rows(&stats_path, &match_meta, &mut stats)?;

    let mut by_league_entries: HashMap<u32, Vec<PlayerImpactEntry>> = HashMap::new();
    for ((league_id, team_norm, player_norm), agg) in stats {
        if let Some(entry) = agg.as_entry(&team_norm, &player_norm, min_samples) {
            by_league_entries.entry(league_id).or_default().push(entry);
        }
    }

    let mut out = HashMap::new();
    for league_id in league_ids {
        let mut entries = by_league_entries.remove(league_id).unwrap_or_default();
        entries.sort_by(|a, b| {
            a.team_norm
                .cmp(&b.team_norm)
                .then(a.player_norm.cmp(&b.player_norm))
        });
        let k = estimate_k_from_entries(&entries);
        let artifact = LeaguePlayerImpactArtifact {
            league_id: *league_id,
            k_player_impact: k,
            min_player_samples: min_samples,
            model_v2: None,
            entries,
        };
        eprintln!(
            "[INFO] baselight league {} matches={} entries={}",
            league_id,
            match_counts.get(league_id).copied().unwrap_or(0),
            artifact.entries.len()
        );
        out.insert(*league_id, artifact);
    }

    Ok(out)
}

fn read_matches(
    path: &Path,
    target_leagues: &HashSet<u32>,
) -> Result<(HashMap<String, MatchMeta>, HashMap<u32, usize>)> {
    let file = fs::File::open(path).with_context(|| format!("open {}", path.display()))?;
    let reader = SerializedFileReader::new(file).context("open parquet reader matches")?;
    let iter = reader.get_row_iter(None).context("iterate match rows")?;

    let mut out = HashMap::new();
    let mut counts = HashMap::new();

    for row in iter {
        let Ok(row) = row else {
            continue;
        };
        let comp = row.get_string(2).ok().map(|s| s.as_str()).unwrap_or("");
        let Some(league_id) = map_competition_to_league_id(comp) else {
            continue;
        };
        if !target_leagues.contains(&league_id) {
            continue;
        }

        let match_id = row
            .get_string(0)
            .ok()
            .map(|s| s.as_str())
            .unwrap_or("")
            .to_string();
        if match_id.is_empty() {
            continue;
        }

        let home_name = row.get_string(6).ok().map(|s| s.as_str()).unwrap_or("");
        let away_name = row.get_string(8).ok().map(|s| s.as_str()).unwrap_or("");
        let home_norm = normalize_name(home_name);
        let away_norm = normalize_name(away_name);
        if home_norm.is_empty() || away_norm.is_empty() {
            continue;
        }

        let home_score = read_num(&row, 14);
        let away_score = read_num(&row, 15);
        if !home_score.is_finite() || !away_score.is_finite() {
            continue;
        }

        let outcome_home = if home_score > away_score {
            1
        } else if home_score < away_score {
            -1
        } else {
            0
        };

        out.insert(
            match_id,
            MatchMeta {
                league_id,
                home_norm,
                away_norm,
                outcome_home,
            },
        );
        *counts.entry(league_id).or_insert(0) += 1;
    }

    Ok((out, counts))
}

fn ingest_player_rows(
    path: &Path,
    matches: &HashMap<String, MatchMeta>,
    out: &mut HashMap<(u32, String, String), OnlineStat>,
) -> Result<()> {
    let file = fs::File::open(path).with_context(|| format!("open {}", path.display()))?;
    let reader = SerializedFileReader::new(file).context("open parquet reader player stats")?;
    let iter = reader.get_row_iter(None).context("iterate player rows")?;

    for row in iter {
        let Ok(row) = row else {
            continue;
        };
        let match_id = row.get_string(0).ok().map(|s| s.as_str()).unwrap_or("");
        let Some(meta) = matches.get(match_id) else {
            continue;
        };

        let team_name = row.get_string(2).ok().map(|s| s.as_str()).unwrap_or("");
        let player_name = row.get_string(4).ok().map(|s| s.as_str()).unwrap_or("");
        if team_name.trim().is_empty() || player_name.trim().is_empty() {
            continue;
        }

        let team_norm = normalize_name(team_name);
        let player_norm = normalize_name(player_name);
        if team_norm.is_empty() || player_norm.is_empty() {
            continue;
        }

        let outcome = if team_norm == meta.home_norm {
            meta.outcome_home as f64
        } else if team_norm == meta.away_norm {
            -(meta.outcome_home as f64)
        } else {
            continue;
        };

        let mins = read_num(&row, 8);
        let minutes_w = (mins / 90.0).clamp(0.2, 1.0);

        let feats = parse_feature_obs(&row);
        let stat_score = performance_stat_score(feats, &row);
        let blended = (0.75 * outcome + 0.25 * stat_score).clamp(-1.0, 1.0);

        out.entry((meta.league_id, team_norm, player_norm))
            .or_default()
            .push(blended, minutes_w, feats);
    }

    Ok(())
}

fn train_from_fotmob_fallback(
    league_ids: &[u32],
    min_samples: u32,
) -> Result<HashMap<u32, LeaguePlayerImpactArtifact>> {
    let db_path = parse_db_path_arg()
        .or_else(|| std::env::var("HIST_DB_PATH").ok().map(PathBuf::from))
        .or_else(historical_dataset::default_db_path)
        .context("unable to resolve historical db path for fallback")?;

    let conn = historical_dataset::open_db(&db_path)?;

    let max_players_per_team = parse_usize_arg("--fallback-max-players")
        .unwrap_or(18)
        .clamp(8, 30);
    let mut out = HashMap::new();

    for league_id in league_ids {
        let rows = historical_dataset::load_finished_matches(&conn, *league_id)
            .with_context(|| format!("load finished matches for league {}", league_id))?;
        if rows.is_empty() {
            continue;
        }

        let mut team_agg: HashMap<u32, TeamAgg> = HashMap::new();
        let mut team_names: HashMap<u32, String> = HashMap::new();
        for m in &rows {
            let outcome_h = if let (Some(h), Some(a)) = (m.home_goals, m.away_goals) {
                if h > a {
                    1.0
                } else if h < a {
                    -1.0
                } else {
                    0.0
                }
            } else {
                0.0
            };

            let entry_h = team_agg.entry(m.home_team_id).or_insert(TeamAgg {
                sum_outcome: 0.0,
                samples: 0,
            });
            entry_h.sum_outcome += outcome_h;
            entry_h.samples = entry_h.samples.saturating_add(1);
            team_names
                .entry(m.home_team_id)
                .or_insert_with(|| m.home_team.clone());

            let entry_a = team_agg.entry(m.away_team_id).or_insert(TeamAgg {
                sum_outcome: 0.0,
                samples: 0,
            });
            entry_a.sum_outcome -= outcome_h;
            entry_a.samples = entry_a.samples.saturating_add(1);
            team_names
                .entry(m.away_team_id)
                .or_insert_with(|| m.away_team.clone());
        }

        let mut entries = Vec::new();
        for (team_id, agg) in team_agg {
            let Some(team_name) = team_names.get(&team_id) else {
                continue;
            };
            let team_norm = normalize_name(team_name);
            if team_norm.is_empty() {
                continue;
            }
            let team_prior = if agg.samples > 0 {
                (agg.sum_outcome / agg.samples as f64).clamp(-1.0, 1.0)
            } else {
                0.0
            };

            let squad = match analysis_fetch::fetch_team_squad(team_id) {
                Ok(squad) => squad,
                Err(err) => {
                    eprintln!("[WARN] fallback squad {} failed: {}", team_id, err);
                    continue;
                }
            };

            let mut players = squad.players;
            players.truncate(max_players_per_team);
            for p in players {
                let player_norm = normalize_name(&p.name);
                if player_norm.is_empty() || p.id == 0 {
                    continue;
                }

                let detail = analysis_fetch::fetch_player_detail(p.id).ok();
                let feats = detail
                    .as_ref()
                    .map(extract_features_from_player_detail)
                    .unwrap_or_default();
                let form = ((feats.rating - 6.8) / 1.2).clamp(-1.0, 1.0);
                let prior = (0.80 * team_prior + 0.20 * form).clamp(-1.0, 1.0);
                let samples = agg.samples.max(min_samples);
                let minutes = 90.0 * (samples as f64) * 0.8;

                entries.push(PlayerImpactEntry {
                    team_norm: team_norm.clone(),
                    player_norm,
                    prior,
                    samples,
                    minutes,
                    rating: feats.rating,
                    shots_on_target: feats.shots_on_target,
                    key_passes: feats.key_passes,
                    tackles_interceptions: feats.tackles_interceptions,
                    duel_win_rate: feats.duel_win_rate,
                    cards: feats.cards,
                });
            }
        }

        entries.sort_by(|a, b| {
            a.team_norm
                .cmp(&b.team_norm)
                .then(a.player_norm.cmp(&b.player_norm))
        });

        let k = estimate_k_from_entries(&entries);
        eprintln!(
            "[INFO] fotmob fallback league {} entries={}",
            league_id,
            entries.len()
        );

        out.insert(
            *league_id,
            LeaguePlayerImpactArtifact {
                league_id: *league_id,
                k_player_impact: k,
                min_player_samples: min_samples,
                model_v2: None,
                entries,
            },
        );
    }

    Ok(out)
}

fn build_shared_prior(
    leagues: &[LeaguePlayerImpactArtifact],
    min_samples: u32,
) -> Option<LeaguePlayerImpactArtifact> {
    let mut merged: HashMap<(String, String), OnlineStat> = HashMap::new();
    for league in leagues {
        for entry in &league.entries {
            let stat = merged
                .entry((entry.team_norm.clone(), entry.player_norm.clone()))
                .or_default();
            let w = (entry.minutes / 90.0).clamp(0.2, 1.0);
            let feats = FeatureObs {
                rating: entry.rating,
                shots_on_target: entry.shots_on_target,
                key_passes: entry.key_passes,
                tackles_interceptions: entry.tackles_interceptions,
                duel_win_rate: entry.duel_win_rate,
                cards: entry.cards,
            };
            stat.push(entry.prior, w, feats);
        }
    }

    let mut entries = Vec::new();
    for ((team_norm, player_norm), stat) in merged {
        if let Some(entry) = stat.as_entry(&team_norm, &player_norm, min_samples) {
            entries.push(entry);
        }
    }
    entries.sort_by(|a, b| {
        a.team_norm
            .cmp(&b.team_norm)
            .then(a.player_norm.cmp(&b.player_norm))
    });

    if entries.is_empty() {
        return None;
    }

    Some(LeaguePlayerImpactArtifact {
        league_id: 0,
        k_player_impact: estimate_k_from_entries(&entries),
        min_player_samples: min_samples,
        model_v2: None,
        entries,
    })
}

fn parse_feature_obs(row: &parquet::record::Row) -> FeatureObs {
    let rating = read_num(row, 10);
    let shots_on_target = read_num(row, 12);
    let key_passes = read_num(row, 18);
    let tackles_interceptions = read_num(row, 21);
    let duels_total = read_num(row, 22);
    let duels_won = read_num(row, 23);
    let yellow_cards = read_num(row, 29);
    let red_cards = read_num(row, 30);

    let duel_win_rate = if duels_total > 0.0 {
        (duels_won / duels_total).clamp(0.0, 1.0)
    } else {
        0.0
    };

    FeatureObs {
        rating,
        shots_on_target,
        key_passes,
        tackles_interceptions,
        duel_win_rate,
        cards: yellow_cards + (2.0 * red_cards),
    }
}

fn performance_stat_score(x: FeatureObs, row: &parquet::record::Row) -> f64 {
    let goals_scored = read_num(row, 13);
    let assists = read_num(row, 14);
    let goals_conceded = read_num(row, 15);
    let saves = read_num(row, 16);
    let tackles_total = read_num(row, 19);
    let fouls_drawn = read_num(row, 27);
    let fouls_committed = read_num(row, 28);
    let yellow_cards = read_num(row, 29);
    let red_cards = read_num(row, 30);

    let rating_term = ((x.rating - 6.8) / 1.2).clamp(-2.0, 2.0) * 0.35;
    let attack_raw = (0.90 * goals_scored)
        + (0.55 * assists)
        + (0.20 * x.shots_on_target)
        + (0.12 * x.key_passes);
    let attack_term = attack_raw.clamp(-1.0, 3.0) * 0.30;
    let defense_raw = (0.10 * tackles_total)
        + (0.14 * x.tackles_interceptions)
        + (0.80 * x.duel_win_rate)
        + (0.12 * saves)
        - (0.20 * goals_conceded);
    let defense_term = defense_raw.clamp(-1.0, 3.0) * 0.20;
    let discipline_raw = (0.06 * fouls_drawn)
        - (0.12 * fouls_committed)
        - (0.25 * yellow_cards)
        - (0.75 * red_cards);
    let discipline_term = discipline_raw.clamp(-2.0, 1.5) * 0.15;
    (rating_term + attack_term + defense_term + discipline_term).clamp(-1.0, 1.0)
}

fn extract_features_from_player_detail(detail: &wc26_terminal::state::PlayerDetail) -> FeatureObs {
    let rating = average_recent_rating(detail)
        .unwrap_or_else(|| find_stat_numeric(detail, &["rating", "average rating"]).unwrap_or(6.8));
    let shots_on_target = find_stat_numeric(detail, &["shots on target"]).unwrap_or(0.0);
    let key_passes = find_stat_numeric(detail, &["key passes"]).unwrap_or(0.0);
    let tackles_interceptions =
        find_stat_numeric(detail, &["interceptions", "tackles won"]).unwrap_or(0.0);
    let duel_win_rate = find_stat_numeric(detail, &["duel", "duels won %"])
        .map(|v| {
            if v > 1.0 {
                (v / 100.0).clamp(0.0, 1.0)
            } else {
                v.clamp(0.0, 1.0)
            }
        })
        .unwrap_or(0.0);
    let yellow = find_stat_numeric(detail, &["yellow cards"]).unwrap_or(0.0);
    let red = find_stat_numeric(detail, &["red cards"]).unwrap_or(0.0);

    FeatureObs {
        rating,
        shots_on_target,
        key_passes,
        tackles_interceptions,
        duel_win_rate,
        cards: yellow + (2.0 * red),
    }
}

fn average_recent_rating(detail: &wc26_terminal::state::PlayerDetail) -> Option<f64> {
    let mut sum = 0.0;
    let mut n = 0.0;
    for m in detail.recent_matches.iter().take(8) {
        let Some(raw) = m.rating.as_deref() else {
            continue;
        };
        let Ok(v) = raw.trim().parse::<f64>() else {
            continue;
        };
        if v.is_finite() {
            sum += v;
            n += 1.0;
        }
    }
    if n > 0.0 { Some(sum / n) } else { None }
}

fn find_stat_numeric(detail: &wc26_terminal::state::PlayerDetail, needles: &[&str]) -> Option<f64> {
    let needles = needles
        .iter()
        .map(|s| s.to_ascii_lowercase())
        .collect::<Vec<_>>();

    for item in &detail.top_stats {
        if title_matches(&item.title, &needles)
            && let Some(v) = parse_numeric(&item.value)
        {
            return Some(v);
        }
    }
    if let Some(main) = &detail.main_league {
        for item in &main.stats {
            if title_matches(&item.title, &needles)
                && let Some(v) = parse_numeric(&item.value)
            {
                return Some(v);
            }
        }
    }
    for group in &detail.season_performance {
        for item in &group.items {
            if title_matches(&item.title, &needles) {
                if let Some(v) = parse_numeric(&item.total) {
                    return Some(v);
                }
                if let Some(v) = item.per90.as_deref().and_then(parse_numeric) {
                    return Some(v);
                }
            }
        }
    }

    None
}

fn title_matches(title: &str, needles: &[String]) -> bool {
    let t = title.to_ascii_lowercase();
    needles.iter().any(|n| t.contains(n))
}

fn parse_numeric(raw: &str) -> Option<f64> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }

    let mut out = String::new();
    let mut seen_digit = false;
    for ch in trimmed.chars() {
        if ch.is_ascii_digit() || ch == '.' || ch == '-' {
            out.push(ch);
            if ch.is_ascii_digit() {
                seen_digit = true;
            }
        } else if seen_digit {
            break;
        }
    }

    if !seen_digit {
        return None;
    }
    out.parse::<f64>().ok()
}

fn estimate_k_from_entries(entries: &[PlayerImpactEntry]) -> f64 {
    if entries.is_empty() {
        return 0.35;
    }
    let mut weighted_abs = 0.0;
    let mut wsum = 0.0;
    for e in entries {
        let w = (e.samples as f64).clamp(1.0, 50.0);
        weighted_abs += e.prior.abs() * w;
        wsum += w;
    }
    let mean_abs = if wsum > 0.0 {
        weighted_abs / wsum
    } else {
        0.20
    };
    (0.20 + 0.60 * mean_abs).clamp(0.20, 0.65)
}

fn map_competition_to_league_id(raw: &str) -> Option<u32> {
    let s = raw.to_ascii_lowercase();
    if s.contains("premier league") {
        Some(47)
    } else if s.contains("la liga") {
        Some(87)
    } else if s.contains("bundesliga") {
        Some(54)
    } else if s.contains("serie a") {
        Some(55)
    } else if s.contains("ligue 1") {
        Some(53)
    } else if s.contains("champions league") {
        Some(42)
    } else if s.contains("world cup") {
        Some(77)
    } else {
        None
    }
}

fn download_file(url: &str, path: &Path) -> Result<PathBuf> {
    let client = reqwest::blocking::Client::builder()
        .user_agent("wc26-terminal/1.0")
        .timeout(std::time::Duration::from_secs(180))
        .build()
        .context("build http client")?;
    let mut last_err: Option<anyhow::Error> = None;
    for attempt in 1..=4 {
        let fetched = client
            .get(url)
            .send()
            .with_context(|| format!("request {url}"))
            .and_then(|res| {
                res.error_for_status()
                    .with_context(|| format!("status for {url}"))
            })
            .and_then(|res| res.bytes().with_context(|| format!("read body {url}")));
        match fetched {
            Ok(bytes) => {
                fs::write(path, &bytes).with_context(|| format!("write {}", path.display()))?;
                return Ok(path.to_path_buf());
            }
            Err(err) => {
                last_err = Some(err);
                if attempt < 4 {
                    let sleep_ms = 500_u64.saturating_mul(attempt as u64);
                    std::thread::sleep(std::time::Duration::from_millis(sleep_ms));
                }
            }
        }
    }
    Err(last_err.unwrap_or_else(|| anyhow!("download failed for {url}")))
}

fn read_num(row: &parquet::record::Row, idx: usize) -> f64 {
    if let Ok(v) = row.get_double(idx) {
        return v;
    }
    if let Ok(v) = row.get_long(idx) {
        return v as f64;
    }
    if let Ok(v) = row.get_int(idx) {
        return v as f64;
    }
    0.0
}

fn parse_out_arg() -> Option<PathBuf> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    for (idx, arg) in args.iter().enumerate() {
        if let Some(v) = arg.strip_prefix("--out=")
            && !v.trim().is_empty()
        {
            return Some(PathBuf::from(v));
        }
        if arg == "--out"
            && let Some(next) = args.get(idx + 1)
            && !next.trim().is_empty()
        {
            return Some(PathBuf::from(next));
        }
    }
    None
}

fn parse_db_path_arg() -> Option<PathBuf> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    for (idx, arg) in args.iter().enumerate() {
        if let Some(path) = arg.strip_prefix("--db=") {
            let trimmed = path.trim();
            if !trimmed.is_empty() {
                return Some(PathBuf::from(trimmed));
            }
        }
        if arg == "--db" {
            let Some(next) = args.get(idx + 1) else {
                continue;
            };
            if !next.trim().is_empty() {
                return Some(PathBuf::from(next));
            }
        }
    }
    None
}

fn parse_u32_arg(name: &str) -> Option<u32> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    for (idx, arg) in args.iter().enumerate() {
        if let Some(raw) = arg.strip_prefix(&format!("{name}="))
            && let Ok(v) = raw.trim().parse::<u32>()
        {
            return Some(v);
        }
        if arg == name
            && let Some(next) = args.get(idx + 1)
            && let Ok(v) = next.trim().parse::<u32>()
        {
            return Some(v);
        }
    }
    None
}

fn parse_usize_arg(name: &str) -> Option<usize> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    for (idx, arg) in args.iter().enumerate() {
        if let Some(raw) = arg.strip_prefix(&format!("{name}="))
            && let Ok(v) = raw.trim().parse::<usize>()
        {
            return Some(v);
        }
        if arg == name
            && let Some(next) = args.get(idx + 1)
            && let Ok(v) = next.trim().parse::<usize>()
        {
            return Some(v);
        }
    }
    None
}

fn parse_league_ids_arg() -> Option<Vec<u32>> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    for (idx, arg) in args.iter().enumerate() {
        if let Some(raw) = arg.strip_prefix("--league-ids=") {
            let ids = parse_ids(raw);
            if !ids.is_empty() {
                return Some(ids);
            }
        }
        if arg == "--league-ids"
            && let Some(next) = args.get(idx + 1)
        {
            let ids = parse_ids(next);
            if !ids.is_empty() {
                return Some(ids);
            }
        }
    }
    None
}

fn default_league_ids_from_env() -> Vec<u32> {
    let mut out = Vec::new();
    extend_ids_env_or_default(&mut out, "APP_LEAGUE_PREMIER_IDS", &[47]);
    extend_ids_env_or_default(&mut out, "APP_LEAGUE_LALIGA_IDS", &[87]);
    extend_ids_env_or_default(&mut out, "APP_LEAGUE_BUNDESLIGA_IDS", &[54]);
    extend_ids_env_or_default(&mut out, "APP_LEAGUE_SERIE_A_IDS", &[55]);
    extend_ids_env_or_default(&mut out, "APP_LEAGUE_LIGUE1_IDS", &[53]);
    extend_ids_env_or_default(&mut out, "APP_LEAGUE_CHAMPIONS_LEAGUE_IDS", &[42]);
    extend_ids_env_or_default(&mut out, "APP_LEAGUE_WORLDCUP_IDS", &[77]);
    if out.is_empty() {
        out.extend(DEFAULT_LEAGUE_IDS);
    }
    dedup_ids(out)
}

fn extend_ids_env_or_default(out: &mut Vec<u32>, key: &str, defaults: &[u32]) {
    match std::env::var(key) {
        Ok(raw) => {
            if raw.trim().is_empty() {
                return;
            }
            out.extend(parse_ids(&raw));
        }
        Err(_) => out.extend(defaults.iter().copied()),
    }
}

fn parse_ids(raw: &str) -> Vec<u32> {
    let ids = raw
        .split([',', ';', ' '])
        .filter_map(|part| part.trim().parse::<u32>().ok())
        .filter(|id| *id != 0)
        .collect::<Vec<_>>();
    dedup_ids(ids)
}

fn dedup_ids(ids: Vec<u32>) -> Vec<u32> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for id in ids {
        if seen.insert(id) {
            out.push(id);
        }
    }
    out
}

fn normalize_name(input: &str) -> String {
    let lower = input.trim().to_ascii_lowercase();
    let mut out = String::with_capacity(lower.len());
    let mut prev_us = false;
    for ch in lower.chars() {
        let mapped = if ch.is_ascii_alphanumeric() {
            Some(ch)
        } else if ch == '&' {
            Some('a')
        } else {
            None
        };

        if let Some(c) = mapped {
            out.push(c);
            prev_us = false;
        } else if !prev_us && !out.is_empty() {
            out.push('_');
            prev_us = true;
        }
    }
    while out.ends_with('_') {
        out.pop();
    }
    out
}
