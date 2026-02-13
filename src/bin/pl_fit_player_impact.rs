use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::RowAccessor;

use wc26_terminal::pl_player_impact::{
    PLAYER_IMPACT_FEATURE_NAMES, PlayerImpactArtifact, PlayerImpactEntry,
    PlayerImpactLinearModelV2, TeamImpactFeatures, normalize_name,
};

const BASELIGHT_DATASET_URL: &str = "https://baselight.app/u/blt/dataset/ultimate_soccer_dataset";
const MATCHES_PARQUET_URL: &str = "https://588738577887-baselight-crawlers-prod-ue1-datasets.s3.us-east-1.amazonaws.com/iceberg_catalog/blt-blt109x9ors0t7l8jzs3oy8sc6wjhai06r3x7pkf39773uevkegqe4wyqct7907ky3jmo/ultimate_soccer_dataset/matches/matches_v2026-02-13_40fa0e3d.parquet";
const PLAYER_STATS_PARQUET_URL: &str = "https://588738577887-baselight-crawlers-prod-ue1-datasets.s3.us-east-1.amazonaws.com/iceberg_catalog/blt-blt109x9ors0t7l8jzs3oy8sc6wjhai06r3x7pkf39773uevkegqe4wyqct7907ky3jmo/ultimate_soccer_dataset/match_player_stats/match_player_stats_v2026-02-13_03cb9794.parquet";

const FEATURE_COUNT: usize = 7;
const TRAIN_SPLIT: f64 = 0.85;
const MIN_TRAIN_SAMPLES: usize = 250;
const RECENCY_HALF_LIFE_DAYS: f64 = 420.0;
const L2_REG: f64 = 0.06;
const MAX_ITERS: usize = 2200;
const LR_START: f64 = 0.08;
const IMPROVEMENT_EPS: f64 = 1e-4;

#[derive(Debug, Clone)]
struct MatchRow {
    match_id: String,
    kickoff_ord: i64,
    home_norm: String,
    away_norm: String,
    outcome_home: i8,
}

#[derive(Debug, Clone, Copy)]
struct PlayerObs {
    key_id: u32,
    is_home: bool,
    minutes_w: f32,
    stat_score: f32,
    rating: f32,
    shots_on_target: f32,
    key_passes: f32,
    tackles_interceptions: f32,
    duel_win_rate: f32,
    cards: f32,
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
    fn is_empty(&self) -> bool {
        self.samples == 0 || self.weighted_n <= 0.0
    }

    fn mean_prior(&self) -> f64 {
        self.weighted_sum / self.weighted_n
    }

    fn mean_rating(&self) -> f64 {
        self.rating_sum / self.weighted_n
    }

    fn mean_shots_on_target(&self) -> f64 {
        self.shots_on_target_sum / self.weighted_n
    }

    fn mean_key_passes(&self) -> f64 {
        self.key_passes_sum / self.weighted_n
    }

    fn mean_tackles_interceptions(&self) -> f64 {
        self.tackles_interceptions_sum / self.weighted_n
    }

    fn mean_duel_win_rate(&self) -> f64 {
        self.duel_win_rate_sum / self.weighted_n
    }

    fn mean_cards(&self) -> f64 {
        self.cards_sum / self.weighted_n
    }
}

#[derive(Debug, Clone, Copy)]
struct TrainSample {
    x: [f64; FEATURE_COUNT],
    base_h: f64,
    base_d: f64,
    base_a: f64,
    outcome: i8,
    weight: f64,
}

#[derive(Debug, Clone, Copy, Default)]
struct SideFeatures {
    rating: f64,
    shots_on_target: f64,
    key_passes: f64,
    tackles_interceptions: f64,
    duel_win_rate: f64,
    cards: f64,
}

#[derive(Debug, Clone, Copy, Default)]
struct OutcomeSummary {
    samples: usize,
    rating: f64,
    shots_on_target: f64,
    key_passes: f64,
    tackles_interceptions: f64,
    duel_win_rate: f64,
    cards: f64,
}

fn main() -> Result<()> {
    let out_path =
        parse_out_arg().unwrap_or_else(|| PathBuf::from("assets/pl_player_impact_v2.json"));
    let force = has_flag("--force");

    let tmp_dir = std::env::temp_dir().join("wc26_pl_fit_player_impact");
    fs::create_dir_all(&tmp_dir).context("create temp directory")?;

    let matches_path = download_file(MATCHES_PARQUET_URL, &tmp_dir.join("matches.parquet"))?;
    let stats_path = download_file(
        PLAYER_STATS_PARQUET_URL,
        &tmp_dir.join("match_player_stats.parquet"),
    )?;

    let mut matches = read_premier_league_matches(&matches_path)?;
    if matches.is_empty() {
        return Err(anyhow!(
            "no premier league matches decoded from matches parquet"
        ));
    }
    matches.sort_by(|a, b| {
        a.kickoff_ord
            .cmp(&b.kickoff_ord)
            .then(a.match_id.cmp(&b.match_id))
    });

    let index_by_match: HashMap<String, usize> = matches
        .iter()
        .enumerate()
        .map(|(idx, m)| (m.match_id.clone(), idx))
        .collect();

    let mut key_intern: HashMap<String, u32> = HashMap::new();
    let mut key_rev: Vec<(String, String)> = Vec::new();
    let mut participants: Vec<Vec<PlayerObs>> = vec![Vec::new(); matches.len()];
    ingest_player_rows(
        &stats_path,
        &matches,
        &index_by_match,
        &mut key_intern,
        &mut key_rev,
        &mut participants,
    )?;

    let min_samples = 4u32;
    let mut priors: Vec<OnlineStat> = vec![OnlineStat::default(); key_rev.len()];
    let mut samples: Vec<TrainSample> = Vec::with_capacity(matches.len());

    let mut hist_home = 0u64;
    let mut hist_draw = 0u64;
    let mut hist_away = 0u64;
    let mut summary_win = OutcomeSummary::default();
    let mut summary_draw = OutcomeSummary::default();
    let mut summary_loss = OutcomeSummary::default();

    let (min_kickoff, max_kickoff) = kickoff_bounds(&matches);
    let kickoff_to_secs = if max_kickoff.saturating_sub(min_kickoff) > 100_000_000_000 {
        0.001
    } else {
        1.0
    };

    for (idx, m) in matches.iter().enumerate() {
        let rows = &participants[idx];

        let home_pre = side_features_from_priors(rows, true, &priors, min_samples);
        let away_pre = side_features_from_priors(rows, false, &priors, min_samples);
        let raw_features = feature_diff(home_pre, away_pre);

        let home_feat_now = side_features(rows, true);
        let away_feat_now = side_features(rows, false);
        apply_outcome_summary(
            m.outcome_home,
            home_feat_now,
            &mut summary_win,
            &mut summary_draw,
            &mut summary_loss,
        );
        apply_outcome_summary(
            -m.outcome_home,
            away_feat_now,
            &mut summary_win,
            &mut summary_draw,
            &mut summary_loss,
        );

        let n_hist = hist_home + hist_draw + hist_away;
        let (ph, pd, pa) = if n_hist < 40 {
            (0.45, 0.27, 0.28)
        } else {
            let h = hist_home as f64 + 2.0;
            let d = hist_draw as f64 + 2.0;
            let a = hist_away as f64 + 2.0;
            let s = h + d + a;
            (h / s, d / s, a / s)
        };

        let weight = recency_weight(
            m.kickoff_ord,
            max_kickoff,
            kickoff_to_secs,
            RECENCY_HALF_LIFE_DAYS,
        );
        samples.push(TrainSample {
            x: raw_features,
            base_h: ph.max(1e-6).ln(),
            base_d: pd.max(1e-6).ln(),
            base_a: pa.max(1e-6).ln(),
            outcome: m.outcome_home,
            weight,
        });

        update_priors(rows, m.outcome_home, &mut priors);

        match m.outcome_home {
            1 => hist_home += 1,
            0 => hist_draw += 1,
            -1 => hist_away += 1,
            _ => {}
        }
    }

    if samples.len() < MIN_TRAIN_SAMPLES {
        return Err(anyhow!(
            "insufficient training samples: {} (need at least {})",
            samples.len(),
            MIN_TRAIN_SAMPLES
        ));
    }

    let split_idx = split_train_index(samples.len());
    let (train_raw, val_raw) = samples.split_at(split_idx);
    if train_raw.is_empty() || val_raw.is_empty() {
        return Err(anyhow!(
            "failed to split train/validation samples train={} val={}",
            train_raw.len(),
            val_raw.len()
        ));
    }

    let (feature_means, feature_stds) = feature_norm_stats(train_raw);
    for sample in &mut samples {
        for i in 0..FEATURE_COUNT {
            sample.x[i] = standardized(sample.x[i], feature_means[i], feature_stds[i]);
        }
    }
    let (train, val) = samples.split_at(split_idx);

    let baseline_train = log_loss_for_coeffs(&[0.0; FEATURE_COUNT], train);
    let baseline_val = log_loss_for_coeffs(&[0.0; FEATURE_COUNT], val);

    let coeffs = fit_coeffs(train, val, L2_REG);
    let fit_train = log_loss_for_coeffs(&coeffs, train);
    let fit_val = log_loss_for_coeffs(&coeffs, val);

    println!(
        "PL player-impact v2 fit train={} val={} half_life_days={:.1} l2={:.3}",
        train.len(),
        val.len(),
        RECENCY_HALF_LIFE_DAYS,
        L2_REG
    );
    println!(
        "train log_loss baseline={:.6} fit={:.6} delta={:+.6}",
        baseline_train,
        fit_train,
        baseline_train - fit_train
    );
    println!(
        "val   log_loss baseline={:.6} fit={:.6} delta={:+.6}",
        baseline_val,
        fit_val,
        baseline_val - fit_val
    );
    println!();
    println!("Feature coefficients (z = sum(coeff_i * std_feature_i)):");
    for (idx, name) in PLAYER_IMPACT_FEATURE_NAMES.iter().enumerate() {
        println!(
            "  {:28} coeff={:+.4} mean={:+.4} std={:.4}",
            name, coeffs[idx], feature_means[idx], feature_stds[idx]
        );
    }
    println!();
    println!("Player-stats cross-check (team-side means by result):");
    print_outcome_summary("WIN ", summary_win);
    print_outcome_summary("DRAW", summary_draw);
    print_outcome_summary("LOSS", summary_loss);

    if !(fit_val + IMPROVEMENT_EPS < baseline_val) && !force {
        return Err(anyhow!(
            "validation log-loss did not improve (pass --force to still write artifact)"
        ));
    }

    let mut entries = Vec::new();
    for (idx, (team_norm, player_norm)) in key_rev.iter().enumerate() {
        let stat = &priors[idx];
        if stat.samples < min_samples || stat.weighted_n <= 0.0 {
            continue;
        }
        let mean = stat.mean_prior();
        let shrink = (stat.samples as f64 / 24.0).clamp(0.15, 1.0);
        entries.push(PlayerImpactEntry {
            team_norm: team_norm.clone(),
            player_norm: player_norm.clone(),
            prior: (mean * shrink).clamp(-1.0, 1.0),
            samples: stat.samples,
            minutes: stat.minutes,
            rating: stat.mean_rating(),
            shots_on_target: stat.mean_shots_on_target(),
            key_passes: stat.mean_key_passes(),
            tackles_interceptions: stat.mean_tackles_interceptions(),
            duel_win_rate: stat.mean_duel_win_rate(),
            cards: stat.mean_cards(),
        });
    }
    entries.sort_by(|a, b| {
        a.team_norm
            .cmp(&b.team_norm)
            .then(a.player_norm.cmp(&b.player_norm))
    });

    let k_player_impact = coeffs[0] / feature_stds[0].max(1e-6);
    let artifact = PlayerImpactArtifact {
        version: 2,
        generated_at: chrono::Utc::now().to_rfc3339(),
        dataset_source_url: BASELIGHT_DATASET_URL.to_string(),
        dataset_version: parquet_version_from_url(MATCHES_PARQUET_URL),
        k_player_impact,
        min_player_samples: min_samples,
        model_v2: Some(PlayerImpactLinearModelV2 {
            feature_names: PLAYER_IMPACT_FEATURE_NAMES
                .iter()
                .map(|s| s.to_string())
                .collect(),
            feature_means: feature_means.to_vec(),
            feature_stds: feature_stds.to_vec(),
            coeffs: coeffs.to_vec(),
            recency_half_life_days: RECENCY_HALF_LIFE_DAYS,
            l2: L2_REG,
            train_log_loss: fit_train,
            val_log_loss: fit_val,
            baseline_val_log_loss: baseline_val,
            train_samples: train.len(),
            val_samples: val.len(),
        }),
        entries,
    };

    if let Some(parent) = out_path.parent() {
        fs::create_dir_all(parent).ok();
    }
    let raw = serde_json::to_string_pretty(&artifact).context("serialize artifact")?;
    fs::write(&out_path, raw).with_context(|| format!("write {}", out_path.display()))?;
    println!();
    println!("artifact written: {}", out_path.display());
    Ok(())
}

fn parse_out_arg() -> Option<PathBuf> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    for (idx, arg) in args.iter().enumerate() {
        if let Some(v) = arg.strip_prefix("--out=") {
            if !v.trim().is_empty() {
                return Some(PathBuf::from(v));
            }
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

fn has_flag(flag: &str) -> bool {
    std::env::args().skip(1).any(|a| a == flag)
}

fn parquet_version_from_url(url: &str) -> String {
    url.split('/').next_back().unwrap_or("unknown").to_string()
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

fn read_premier_league_matches(path: &Path) -> Result<Vec<MatchRow>> {
    let file = fs::File::open(path).with_context(|| format!("open {}", path.display()))?;
    let reader = SerializedFileReader::new(file).context("open parquet reader matches")?;
    let iter = reader.get_row_iter(None).context("iterate matches rows")?;

    let mut out = Vec::new();
    for row in iter {
        let Ok(row) = row else {
            continue;
        };
        let competition = row.get_string(2).ok().map(|s| s.as_str()).unwrap_or("");
        if competition != "Premier League" {
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

        let home_score = row.get_double(14).ok().unwrap_or(f64::NAN);
        let away_score = row.get_double(15).ok().unwrap_or(f64::NAN);
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

        let kickoff_ord = row
            .get_long(26)
            .ok()
            .or_else(|| row.get_long(4).ok())
            .unwrap_or(0);

        out.push(MatchRow {
            match_id,
            kickoff_ord,
            home_norm,
            away_norm,
            outcome_home,
        });
    }
    Ok(out)
}

fn ingest_player_rows(
    path: &Path,
    matches: &[MatchRow],
    index_by_match: &HashMap<String, usize>,
    key_intern: &mut HashMap<String, u32>,
    key_rev: &mut Vec<(String, String)>,
    participants: &mut [Vec<PlayerObs>],
) -> Result<()> {
    let file = fs::File::open(path).with_context(|| format!("open {}", path.display()))?;
    let reader = SerializedFileReader::new(file).context("open parquet reader player stats")?;
    let iter = reader.get_row_iter(None).context("iterate player rows")?;

    for row in iter {
        let Ok(row) = row else {
            continue;
        };
        let match_id = row.get_string(0).ok().map(|s| s.as_str()).unwrap_or("");
        let Some(&match_idx) = index_by_match.get(match_id) else {
            continue;
        };
        let team_name = row.get_string(2).ok().map(|s| s.as_str()).unwrap_or("");
        let player_name = row.get_string(4).ok().map(|s| s.as_str()).unwrap_or("");
        if player_name.trim().is_empty() || team_name.trim().is_empty() {
            continue;
        }

        let team_norm = normalize_name(team_name);
        let player_norm = normalize_name(player_name);
        if team_norm.is_empty() || player_norm.is_empty() {
            continue;
        }

        let m = &matches[match_idx];
        let is_home = if team_norm == m.home_norm {
            true
        } else if team_norm == m.away_norm {
            false
        } else {
            continue;
        };

        let mins = read_num(&row, 8);
        let minutes_w = (mins / 90.0).clamp(0.2, 1.0) as f32;
        let rating = read_num(&row, 10);
        let shots_on_target = read_num(&row, 12);
        let goals_scored = read_num(&row, 13);
        let assists = read_num(&row, 14);
        let goals_conceded = read_num(&row, 15);
        let saves = read_num(&row, 16);
        let key_passes = read_num(&row, 18);
        let tackles_total = read_num(&row, 19);
        let tackles_interceptions = read_num(&row, 21);
        let duels_total = read_num(&row, 22);
        let duels_won = read_num(&row, 23);
        let fouls_drawn = read_num(&row, 27);
        let fouls_committed = read_num(&row, 28);
        let yellow_cards = read_num(&row, 29);
        let red_cards = read_num(&row, 30);

        let duel_win_rate = if duels_total > 0.0 {
            (duels_won / duels_total).clamp(0.0, 1.0)
        } else {
            0.0
        };
        let cards = yellow_cards + (2.0 * red_cards);
        let stat_score = performance_stat_score(PerformanceInputs {
            rating,
            shots_on_target,
            goals_scored,
            assists,
            goals_conceded,
            saves,
            key_passes,
            tackles_total,
            tackles_interceptions,
            duel_win_rate,
            fouls_drawn,
            fouls_committed,
            yellow_cards,
            red_cards,
        });

        let compound = format!("{team_norm}|{player_norm}");
        let key_id = if let Some(id) = key_intern.get(&compound).copied() {
            id
        } else {
            let id = key_rev.len() as u32;
            key_intern.insert(compound, id);
            key_rev.push((team_norm, player_norm));
            id
        };

        participants[match_idx].push(PlayerObs {
            key_id,
            is_home,
            minutes_w,
            stat_score: stat_score as f32,
            rating: rating as f32,
            shots_on_target: shots_on_target as f32,
            key_passes: key_passes as f32,
            tackles_interceptions: tackles_interceptions as f32,
            duel_win_rate: duel_win_rate as f32,
            cards: cards as f32,
        });
    }

    Ok(())
}

#[derive(Debug, Clone, Copy)]
struct PerformanceInputs {
    rating: f64,
    shots_on_target: f64,
    goals_scored: f64,
    assists: f64,
    goals_conceded: f64,
    saves: f64,
    key_passes: f64,
    tackles_total: f64,
    tackles_interceptions: f64,
    duel_win_rate: f64,
    fouls_drawn: f64,
    fouls_committed: f64,
    yellow_cards: f64,
    red_cards: f64,
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

fn performance_stat_score(x: PerformanceInputs) -> f64 {
    let rating_term = ((x.rating - 6.8) / 1.2).clamp(-2.0, 2.0) * 0.35;
    let attack_raw = (0.90 * x.goals_scored)
        + (0.55 * x.assists)
        + (0.20 * x.shots_on_target)
        + (0.12 * x.key_passes);
    let attack_term = attack_raw.clamp(-1.0, 3.0) * 0.30;
    let defense_raw = (0.10 * x.tackles_total)
        + (0.14 * x.tackles_interceptions)
        + (0.80 * x.duel_win_rate)
        + (0.12 * x.saves)
        - (0.20 * x.goals_conceded);
    let defense_term = defense_raw.clamp(-1.0, 3.0) * 0.20;
    let discipline_raw = (0.06 * x.fouls_drawn)
        - (0.12 * x.fouls_committed)
        - (0.25 * x.yellow_cards)
        - (0.75 * x.red_cards);
    let discipline_term = discipline_raw.clamp(-2.0, 1.5) * 0.15;
    (rating_term + attack_term + defense_term + discipline_term).clamp(-1.0, 1.0)
}

fn side_features(rows: &[PlayerObs], home: bool) -> SideFeatures {
    let mut weight = 0.0_f64;
    let mut feat = SideFeatures::default();
    for row in rows {
        if row.is_home != home {
            continue;
        }
        let w = row.minutes_w as f64;
        weight += w;
        feat.rating += row.rating as f64 * w;
        feat.shots_on_target += row.shots_on_target as f64 * w;
        feat.key_passes += row.key_passes as f64 * w;
        feat.tackles_interceptions += row.tackles_interceptions as f64 * w;
        feat.duel_win_rate += row.duel_win_rate as f64 * w;
        feat.cards += row.cards as f64 * w;
    }
    if weight <= 0.0 {
        return SideFeatures::default();
    }
    SideFeatures {
        rating: feat.rating / weight,
        shots_on_target: feat.shots_on_target / weight,
        key_passes: feat.key_passes / weight,
        tackles_interceptions: feat.tackles_interceptions / weight,
        duel_win_rate: feat.duel_win_rate / weight,
        cards: feat.cards / weight,
    }
}

fn apply_outcome_summary(
    outcome: i8,
    feat: SideFeatures,
    summary_win: &mut OutcomeSummary,
    summary_draw: &mut OutcomeSummary,
    summary_loss: &mut OutcomeSummary,
) {
    match outcome {
        1 => push_summary(summary_win, feat),
        0 => push_summary(summary_draw, feat),
        -1 => push_summary(summary_loss, feat),
        _ => {}
    }
}

fn push_summary(summary: &mut OutcomeSummary, feat: SideFeatures) {
    summary.samples = summary.samples.saturating_add(1);
    summary.rating += feat.rating;
    summary.shots_on_target += feat.shots_on_target;
    summary.key_passes += feat.key_passes;
    summary.tackles_interceptions += feat.tackles_interceptions;
    summary.duel_win_rate += feat.duel_win_rate;
    summary.cards += feat.cards;
}

fn print_outcome_summary(label: &str, summary: OutcomeSummary) {
    if summary.samples == 0 {
        println!("{label} n=0");
        return;
    }
    let n = summary.samples as f64;
    println!(
        "{label} n={} rating={:.2} sot={:.2} keyp={:.2} tint={:.2} duel%={:.2} cards={:.2}",
        summary.samples,
        summary.rating / n,
        summary.shots_on_target / n,
        summary.key_passes / n,
        summary.tackles_interceptions / n,
        (summary.duel_win_rate / n) * 100.0,
        summary.cards / n,
    );
}

fn side_features_from_priors(
    rows: &[PlayerObs],
    home: bool,
    priors: &[OnlineStat],
    min_samples: u32,
) -> TeamImpactFeatures {
    let mut total_w = 0.0;
    let mut seen = 0usize;
    let mut matched = 0usize;
    let mut out = TeamImpactFeatures::default();

    for row in rows {
        if row.is_home != home {
            continue;
        }
        seen += 1;
        let stat = &priors[row.key_id as usize];
        if stat.is_empty() {
            continue;
        }
        let shrink = (stat.samples as f64 / min_samples.max(1) as f64).clamp(0.2, 1.0);
        let w = row.minutes_w as f64 * shrink;
        total_w += w;
        out.impact += stat.mean_prior() * w;
        out.rating += stat.mean_rating() * w;
        out.shots_on_target += stat.mean_shots_on_target() * w;
        out.key_passes += stat.mean_key_passes() * w;
        out.tackles_interceptions += stat.mean_tackles_interceptions() * w;
        out.duel_win_rate += stat.mean_duel_win_rate() * w;
        out.cards += stat.mean_cards() * w;
        matched += 1;
    }

    if total_w > 0.0 {
        out.impact /= total_w;
        out.rating /= total_w;
        out.shots_on_target /= total_w;
        out.key_passes /= total_w;
        out.tackles_interceptions /= total_w;
        out.duel_win_rate /= total_w;
        out.cards /= total_w;
    }
    if seen > 0 {
        out.coverage = matched as f32 / seen as f32;
    }
    out
}

fn update_priors(rows: &[PlayerObs], outcome_home: i8, priors: &mut [OnlineStat]) {
    for row in rows {
        let stat = &mut priors[row.key_id as usize];
        let outcome = if row.is_home {
            outcome_home as f64
        } else {
            -(outcome_home as f64)
        };
        let w = row.minutes_w as f64;
        let blended = (0.70 * outcome + 0.30 * row.stat_score as f64).clamp(-1.0, 1.0);
        stat.weighted_sum += blended * w;
        stat.weighted_n += w;
        stat.samples = stat.samples.saturating_add(1);
        stat.minutes += w * 90.0;
        stat.rating_sum += row.rating as f64 * w;
        stat.shots_on_target_sum += row.shots_on_target as f64 * w;
        stat.key_passes_sum += row.key_passes as f64 * w;
        stat.tackles_interceptions_sum += row.tackles_interceptions as f64 * w;
        stat.duel_win_rate_sum += row.duel_win_rate as f64 * w;
        stat.cards_sum += row.cards as f64 * w;
    }
}

fn kickoff_bounds(matches: &[MatchRow]) -> (i64, i64) {
    let mut min_ord = i64::MAX;
    let mut max_ord = i64::MIN;
    for m in matches {
        min_ord = min_ord.min(m.kickoff_ord);
        max_ord = max_ord.max(m.kickoff_ord);
    }
    if min_ord == i64::MAX {
        (0, 0)
    } else {
        (min_ord, max_ord)
    }
}

fn recency_weight(kickoff_ord: i64, max_ord: i64, ord_to_secs: f64, half_life_days: f64) -> f64 {
    if half_life_days <= 0.0 {
        return 1.0;
    }
    let age_ord = max_ord.saturating_sub(kickoff_ord) as f64;
    let age_days = (age_ord * ord_to_secs / 86_400.0).max(0.0);
    let decay = -(std::f64::consts::LN_2 * age_days / half_life_days);
    decay.exp().clamp(0.05, 1.0)
}

fn split_train_index(n: usize) -> usize {
    let mut idx = ((n as f64) * TRAIN_SPLIT).round() as usize;
    idx = idx.clamp(64, n.saturating_sub(1));
    idx
}

fn feature_diff(home: TeamImpactFeatures, away: TeamImpactFeatures) -> [f64; FEATURE_COUNT] {
    [
        home.impact - away.impact,
        home.rating - away.rating,
        home.shots_on_target - away.shots_on_target,
        home.key_passes - away.key_passes,
        home.tackles_interceptions - away.tackles_interceptions,
        home.duel_win_rate - away.duel_win_rate,
        home.cards - away.cards,
    ]
}

fn feature_norm_stats(samples: &[TrainSample]) -> ([f64; FEATURE_COUNT], [f64; FEATURE_COUNT]) {
    let mut mean = [0.0; FEATURE_COUNT];
    let mut var = [0.0; FEATURE_COUNT];
    let mut wsum = 0.0;

    for sample in samples {
        let w = sample.weight.max(1e-6);
        wsum += w;
        for i in 0..FEATURE_COUNT {
            mean[i] += w * sample.x[i];
        }
    }
    if wsum > 0.0 {
        for v in &mut mean {
            *v /= wsum;
        }
    }

    for sample in samples {
        let w = sample.weight.max(1e-6);
        for i in 0..FEATURE_COUNT {
            let d = sample.x[i] - mean[i];
            var[i] += w * d * d;
        }
    }
    if wsum > 0.0 {
        for v in &mut var {
            *v = (*v / wsum).sqrt().max(1e-6);
        }
    } else {
        var = [1.0; FEATURE_COUNT];
    }

    (mean, var)
}

fn standardized(x: f64, mean: f64, std: f64) -> f64 {
    (x - mean) / std.max(1e-6)
}

fn fit_coeffs(train: &[TrainSample], val: &[TrainSample], l2: f64) -> [f64; FEATURE_COUNT] {
    let mut coeffs = [0.0; FEATURE_COUNT];
    let mut best = coeffs;
    let mut best_val = log_loss_for_coeffs(&coeffs, val);
    let mut no_improve = 0usize;

    for iter in 0..MAX_ITERS {
        let mut grad = [0.0; FEATURE_COUNT];
        let mut wsum = 0.0;

        for sample in train {
            let z = dot(coeffs, sample.x);
            let (p_h, _p_d, p_a) = probs_from_shift(sample.base_h, sample.base_d, sample.base_a, z);
            let target = match sample.outcome {
                1 => 1.0,
                -1 => -1.0,
                _ => 0.0,
            };
            let dz = (p_h - p_a) - target;
            let w = sample.weight.max(1e-6);
            wsum += w;
            for j in 0..FEATURE_COUNT {
                grad[j] += w * dz * sample.x[j];
            }
        }

        let lr = LR_START / (1.0 + (iter as f64 * 0.003));
        for j in 0..FEATURE_COUNT {
            let g = grad[j] / wsum.max(1e-6) + l2 * coeffs[j];
            coeffs[j] -= lr * g;
        }

        if iter % 20 == 0 || iter + 1 == MAX_ITERS {
            let val_ll = log_loss_for_coeffs(&coeffs, val);
            if val_ll + IMPROVEMENT_EPS < best_val {
                best_val = val_ll;
                best = coeffs;
                no_improve = 0;
            } else {
                no_improve = no_improve.saturating_add(1);
                if no_improve >= 20 {
                    break;
                }
            }
        }
    }

    best
}

fn log_loss_for_coeffs(coeffs: &[f64; FEATURE_COUNT], samples: &[TrainSample]) -> f64 {
    if samples.is_empty() {
        return f64::INFINITY;
    }

    let mut sum = 0.0;
    let mut wsum = 0.0;
    for sample in samples {
        let z = dot(*coeffs, sample.x);
        let (p_h, p_d, p_a) = probs_from_shift(sample.base_h, sample.base_d, sample.base_a, z);
        let p = match sample.outcome {
            1 => p_h,
            0 => p_d,
            -1 => p_a,
            _ => 1.0 / 3.0,
        };
        let w = sample.weight.max(1e-6);
        sum += -w * p.max(1e-9).ln();
        wsum += w;
    }
    sum / wsum.max(1e-6)
}

fn probs_from_shift(base_h: f64, base_d: f64, base_a: f64, z: f64) -> (f64, f64, f64) {
    let l_h = base_h + z;
    let l_d = base_d;
    let l_a = base_a - z;
    let mx = l_h.max(l_d.max(l_a));
    let eh = (l_h - mx).exp();
    let ed = (l_d - mx).exp();
    let ea = (l_a - mx).exp();
    let den = (eh + ed + ea).max(1e-9);
    (
        (eh / den).clamp(1e-9, 1.0),
        (ed / den).clamp(1e-9, 1.0),
        (ea / den).clamp(1e-9, 1.0),
    )
}

fn dot(a: [f64; FEATURE_COUNT], b: [f64; FEATURE_COUNT]) -> f64 {
    let mut out = 0.0;
    for i in 0..FEATURE_COUNT {
        out += a[i] * b[i];
    }
    out
}
