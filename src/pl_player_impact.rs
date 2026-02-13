use std::collections::HashMap;
use std::sync::OnceLock;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

pub const PLAYER_IMPACT_FEATURE_NAMES: [&str; 7] = [
    "impact_diff",
    "rating_diff",
    "shots_on_target_diff",
    "key_passes_diff",
    "tackles_interceptions_diff",
    "duel_win_rate_diff",
    "cards_diff",
];

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlayerImpactEntry {
    pub team_norm: String,
    pub player_norm: String,
    pub prior: f64,
    pub samples: u32,
    pub minutes: f64,
    #[serde(default)]
    pub rating: f64,
    #[serde(default)]
    pub shots_on_target: f64,
    #[serde(default)]
    pub key_passes: f64,
    #[serde(default)]
    pub tackles_interceptions: f64,
    #[serde(default)]
    pub duel_win_rate: f64,
    #[serde(default)]
    pub cards: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlayerImpactLinearModelV2 {
    #[serde(default)]
    pub feature_names: Vec<String>,
    #[serde(default)]
    pub feature_means: Vec<f64>,
    #[serde(default)]
    pub feature_stds: Vec<f64>,
    #[serde(default)]
    pub coeffs: Vec<f64>,
    #[serde(default)]
    pub recency_half_life_days: f64,
    #[serde(default)]
    pub l2: f64,
    #[serde(default)]
    pub train_log_loss: f64,
    #[serde(default)]
    pub val_log_loss: f64,
    #[serde(default)]
    pub baseline_val_log_loss: f64,
    #[serde(default)]
    pub train_samples: usize,
    #[serde(default)]
    pub val_samples: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlayerImpactArtifact {
    pub version: u32,
    pub generated_at: String,
    pub dataset_source_url: String,
    pub dataset_version: String,
    pub k_player_impact: f64,
    pub min_player_samples: u32,
    #[serde(default)]
    pub model_v2: Option<PlayerImpactLinearModelV2>,
    pub entries: Vec<PlayerImpactEntry>,
}

#[derive(Debug, Clone)]
pub struct PlayerImpactModel {
    artifact: PlayerImpactArtifact,
    by_key: HashMap<String, PlayerImpactEntry>,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct TeamImpactFeatures {
    pub impact: f64,
    pub rating: f64,
    pub shots_on_target: f64,
    pub key_passes: f64,
    pub tackles_interceptions: f64,
    pub duel_win_rate: f64,
    pub cards: f64,
    pub coverage: f32,
}

impl PlayerImpactModel {
    pub fn from_artifact(artifact: PlayerImpactArtifact) -> Self {
        let mut by_key = HashMap::with_capacity(artifact.entries.len());
        for entry in &artifact.entries {
            by_key.insert(key(&entry.team_norm, &entry.player_norm), entry.clone());
        }
        Self { artifact, by_key }
    }

    pub fn k_player_impact(&self) -> f64 {
        self.artifact.k_player_impact
    }

    pub fn min_player_samples(&self) -> u32 {
        self.artifact.min_player_samples
    }

    pub fn has_v2(&self) -> bool {
        self.artifact.model_v2.is_some()
    }

    pub fn v2_model(&self) -> Option<&PlayerImpactLinearModelV2> {
        self.artifact.model_v2.as_ref()
    }

    pub fn lookup(&self, team_name: &str, player_name: &str) -> Option<&PlayerImpactEntry> {
        let team_norm = normalize_name(team_name);
        let player_norm = normalize_name(player_name);
        self.by_key.get(&key(&team_norm, &player_norm))
    }

    pub fn team_impact<'a, I>(&self, team_name: &str, players: I) -> Option<(f64, f32)>
    where
        I: IntoIterator<Item = &'a str>,
    {
        self.team_features(team_name, players)
            .map(|f| (f.impact, f.coverage))
    }

    pub fn team_features<'a, I>(&self, team_name: &str, players: I) -> Option<TeamImpactFeatures>
    where
        I: IntoIterator<Item = &'a str>,
    {
        let team_norm = normalize_name(team_name);
        if team_norm.is_empty() {
            return None;
        }

        let mut total_w = 0.0;
        let mut total = TeamImpactFeatures::default();
        let mut matched = 0usize;
        let mut seen = 0usize;

        for player in players {
            let player_norm = normalize_name(player);
            if player_norm.is_empty() {
                continue;
            }
            seen += 1;
            if let Some(entry) = self.by_key.get(&key(&team_norm, &player_norm)) {
                let n = entry.samples.max(1) as f64;
                let w_samples =
                    (n / self.artifact.min_player_samples.max(1) as f64).clamp(0.2, 1.0);
                let w_minutes = (entry.minutes / 900.0).clamp(0.4, 1.0);
                let w = w_samples * w_minutes;
                total_w += w;
                total.impact += entry.prior * w;
                total.rating += entry.rating * w;
                total.shots_on_target += entry.shots_on_target * w;
                total.key_passes += entry.key_passes * w;
                total.tackles_interceptions += entry.tackles_interceptions * w;
                total.duel_win_rate += entry.duel_win_rate * w;
                total.cards += entry.cards * w;
                matched += 1;
            }
        }

        if seen == 0 {
            return None;
        }
        let coverage = (matched as f32) / (seen as f32);
        if matched == 0 || total_w <= 0.0 {
            return Some(TeamImpactFeatures {
                coverage,
                ..Default::default()
            });
        }
        Some(TeamImpactFeatures {
            impact: total.impact / total_w,
            rating: total.rating / total_w,
            shots_on_target: total.shots_on_target / total_w,
            key_passes: total.key_passes / total_w,
            tackles_interceptions: total.tackles_interceptions / total_w,
            duel_win_rate: total.duel_win_rate / total_w,
            cards: total.cards / total_w,
            coverage,
        })
    }

    pub fn impact_signal(&self, home: TeamImpactFeatures, away: TeamImpactFeatures) -> f64 {
        if let Some(v2) = self.v2_model()
            && !v2.coeffs.is_empty()
        {
            let raw = feature_diff(home, away);
            let mut sum = 0.0;
            for (idx, c) in v2.coeffs.iter().enumerate() {
                if idx >= raw.len() {
                    break;
                }
                sum += c * standardized(raw[idx], idx, v2);
            }
            return sum.clamp(-1.5, 1.5);
        }
        (self.artifact.k_player_impact * (home.impact - away.impact)).clamp(-1.5, 1.5)
    }
}

pub fn load_player_impact_model() -> Result<PlayerImpactModel> {
    let raw_v2 = include_str!("../assets/pl_player_impact_v2.json");
    if let Ok(artifact_v2) = serde_json::from_str::<PlayerImpactArtifact>(raw_v2) {
        return Ok(PlayerImpactModel::from_artifact(artifact_v2));
    }
    let raw_v1 = include_str!("../assets/pl_player_impact_v1.json");
    let artifact_v1: PlayerImpactArtifact =
        serde_json::from_str(raw_v1).context("parse pl_player_impact_v1 artifact")?;
    Ok(PlayerImpactModel::from_artifact(artifact_v1))
}

pub fn global_model() -> Option<&'static PlayerImpactModel> {
    static MODEL: OnceLock<Option<PlayerImpactModel>> = OnceLock::new();
    MODEL
        .get_or_init(|| load_player_impact_model().ok())
        .as_ref()
}

pub fn normalize_name(input: &str) -> String {
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

fn key(team_norm: &str, player_norm: &str) -> String {
    format!("{team_norm}|{player_norm}")
}

fn feature_diff(home: TeamImpactFeatures, away: TeamImpactFeatures) -> [f64; 7] {
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

fn standardized(x: f64, idx: usize, v2: &PlayerImpactLinearModelV2) -> f64 {
    let mu = v2.feature_means.get(idx).copied().unwrap_or(0.0);
    let sigma = v2.feature_stds.get(idx).copied().unwrap_or(1.0).max(1e-6);
    (x - mu) / sigma
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_basic() {
        assert_eq!(normalize_name("Manchester City"), "manchester_city");
        assert_eq!(normalize_name("  Bukayo Saka "), "bukayo_saka");
        assert_eq!(normalize_name("A.B-C"), "a_b_c");
    }

    #[test]
    fn lookup_and_team_impact() {
        let model = PlayerImpactModel::from_artifact(PlayerImpactArtifact {
            version: 1,
            generated_at: "x".to_string(),
            dataset_source_url: "x".to_string(),
            dataset_version: "x".to_string(),
            k_player_impact: 0.4,
            min_player_samples: 4,
            model_v2: None,
            entries: vec![
                PlayerImpactEntry {
                    team_norm: "arsenal".to_string(),
                    player_norm: "bukayo_saka".to_string(),
                    prior: 0.3,
                    samples: 20,
                    minutes: 1500.0,
                    rating: 7.3,
                    shots_on_target: 1.2,
                    key_passes: 2.1,
                    tackles_interceptions: 0.8,
                    duel_win_rate: 0.57,
                    cards: 0.05,
                },
                PlayerImpactEntry {
                    team_norm: "arsenal".to_string(),
                    player_norm: "martin_odegaard".to_string(),
                    prior: 0.2,
                    samples: 20,
                    minutes: 1500.0,
                    rating: 7.2,
                    shots_on_target: 0.9,
                    key_passes: 2.3,
                    tackles_interceptions: 0.7,
                    duel_win_rate: 0.55,
                    cards: 0.08,
                },
            ],
        });

        let saka = model.lookup("Arsenal", "Bukayo Saka").unwrap();
        assert!(saka.prior > 0.0);

        let (impact, cov) = model
            .team_impact(
                "Arsenal",
                ["Bukayo Saka", "Martin Odegaard", "Unknown"]
                    .iter()
                    .copied(),
            )
            .unwrap();
        assert!(impact > 0.0);
        assert!(cov > 0.5);
    }

    #[test]
    fn v2_signal_uses_feature_coeffs() {
        let model = PlayerImpactModel::from_artifact(PlayerImpactArtifact {
            version: 2,
            generated_at: "x".to_string(),
            dataset_source_url: "x".to_string(),
            dataset_version: "x".to_string(),
            k_player_impact: 0.0,
            min_player_samples: 4,
            model_v2: Some(PlayerImpactLinearModelV2 {
                feature_names: PLAYER_IMPACT_FEATURE_NAMES
                    .iter()
                    .map(|s| s.to_string())
                    .collect(),
                feature_means: vec![0.0; 7],
                feature_stds: vec![1.0; 7],
                coeffs: vec![1.0, 0.5, 0.0, 0.0, 0.0, 0.0, -0.2],
                recency_half_life_days: 365.0,
                l2: 0.05,
                train_log_loss: 0.0,
                val_log_loss: 0.0,
                baseline_val_log_loss: 0.0,
                train_samples: 0,
                val_samples: 0,
            }),
            entries: vec![],
        });

        let home = TeamImpactFeatures {
            impact: 0.4,
            rating: 7.2,
            cards: 0.10,
            ..Default::default()
        };
        let away = TeamImpactFeatures {
            impact: 0.1,
            rating: 6.9,
            cards: 0.20,
            ..Default::default()
        };
        let z = model.impact_signal(home, away);
        assert!(z > 0.0);
    }

    #[test]
    fn bundled_loader_prefers_v2_artifact() {
        let model = load_player_impact_model().expect("load artifact");
        assert!(model.has_v2());
    }
}
