use std::collections::HashMap;
use std::env;
use std::fs;
use std::path::PathBuf;
use std::sync::OnceLock;

use crate::http_cache::app_cache_dir;
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
pub struct LeaguePlayerImpactArtifact {
    pub league_id: u32,
    pub k_player_impact: f64,
    pub min_player_samples: u32,
    #[serde(default)]
    pub model_v2: Option<PlayerImpactLinearModelV2>,
    #[serde(default)]
    pub entries: Vec<PlayerImpactEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlayerImpactRegistryArtifact {
    pub version: u32,
    pub generated_at: String,
    #[serde(default)]
    pub source: Option<String>,
    #[serde(default)]
    pub leagues: Vec<LeaguePlayerImpactArtifact>,
    #[serde(default)]
    pub shared_prior: Option<LeaguePlayerImpactArtifact>,
}

#[derive(Debug, Clone)]
pub struct LeaguePlayerImpactModel {
    artifact: LeaguePlayerImpactArtifact,
    by_key: HashMap<String, PlayerImpactEntry>,
}

#[derive(Debug, Clone)]
pub struct PlayerImpactRegistry {
    leagues: HashMap<u32, LeaguePlayerImpactModel>,
    shared_prior: Option<LeaguePlayerImpactModel>,
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

impl LeaguePlayerImpactModel {
    pub fn from_artifact(artifact: LeaguePlayerImpactArtifact) -> Self {
        let mut by_key = HashMap::with_capacity(artifact.entries.len());
        for entry in &artifact.entries {
            by_key.insert(key(&entry.team_norm, &entry.player_norm), entry.clone());
        }
        Self { artifact, by_key }
    }

    pub fn league_id(&self) -> u32 {
        self.artifact.league_id
    }

    pub fn k_player_impact(&self) -> f64 {
        self.artifact.k_player_impact
    }

    pub fn min_player_samples(&self) -> u32 {
        self.artifact.min_player_samples
    }

    pub fn v2_model(&self) -> Option<&PlayerImpactLinearModelV2> {
        self.artifact.model_v2.as_ref()
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

impl PlayerImpactRegistry {
    pub fn from_artifact(artifact: PlayerImpactRegistryArtifact) -> Self {
        let mut leagues = HashMap::new();
        for item in artifact.leagues {
            leagues.insert(item.league_id, LeaguePlayerImpactModel::from_artifact(item));
        }
        let shared_prior = artifact
            .shared_prior
            .map(LeaguePlayerImpactModel::from_artifact);
        Self {
            leagues,
            shared_prior,
        }
    }

    pub fn model_for_league(&self, league_id: u32) -> Option<&LeaguePlayerImpactModel> {
        self.leagues.get(&league_id)
    }

    pub fn fallback_model(&self, league_id: Option<u32>) -> Option<&LeaguePlayerImpactModel> {
        let id = league_id?;
        if let Some(m) = self.model_for_league(id) {
            return Some(m);
        }
        if use_shared_prior_enabled() {
            self.shared_prior.as_ref()
        } else {
            None
        }
    }

    pub fn team_features_for_league<'a, I>(
        &self,
        league_id: Option<u32>,
        team_name: &str,
        players: I,
    ) -> Option<TeamImpactFeatures>
    where
        I: IntoIterator<Item = &'a str>,
    {
        let names: Vec<&str> = players.into_iter().collect();
        let model = self.fallback_model(league_id)?;
        model.team_features(team_name, names.iter().copied())
    }

    pub fn impact_signal_for_league(
        &self,
        league_id: Option<u32>,
        home: TeamImpactFeatures,
        away: TeamImpactFeatures,
    ) -> f64 {
        self.fallback_model(league_id)
            .map(|m| m.impact_signal(home, away))
            .unwrap_or(0.0)
    }

    pub fn model_debug_tag(&self, league_id: Option<u32>) -> (&'static str, i32, f64) {
        let Some(model) = self.fallback_model(league_id) else {
            return ("NA", 0, 0.0);
        };
        if let Some(v2) = model.v2_model() {
            (
                "V2",
                v2.coeffs.len() as i32,
                v2.coeffs.first().copied().unwrap_or(0.0),
            )
        } else {
            ("V1", 1, model.k_player_impact())
        }
    }
}

pub fn load_player_impact_registry() -> Result<PlayerImpactRegistry> {
    if let Some(path) = registry_path_override()
        && path.exists()
    {
        let raw = fs::read_to_string(&path)
            .with_context(|| format!("read player impact registry {}", path.display()))?;
        let artifact = serde_json::from_str::<PlayerImpactRegistryArtifact>(&raw)
            .with_context(|| format!("parse player impact registry {}", path.display()))?;
        return Ok(PlayerImpactRegistry::from_artifact(artifact));
    }

    if let Some(path) = default_registry_cache_path()
        && path.exists()
    {
        let raw = fs::read_to_string(&path)
            .with_context(|| format!("read player impact registry {}", path.display()))?;
        let artifact = serde_json::from_str::<PlayerImpactRegistryArtifact>(&raw)
            .with_context(|| format!("parse player impact registry {}", path.display()))?;
        return Ok(PlayerImpactRegistry::from_artifact(artifact));
    }

    if let Ok(raw) = fs::read_to_string("assets/player_impact_registry_v1.json")
        && let Ok(artifact) = serde_json::from_str::<PlayerImpactRegistryArtifact>(&raw)
    {
        return Ok(PlayerImpactRegistry::from_artifact(artifact));
    }

    fallback_registry_from_legacy_assets()
}

pub fn global_registry() -> Option<&'static PlayerImpactRegistry> {
    static REGISTRY: OnceLock<Option<PlayerImpactRegistry>> = OnceLock::new();
    REGISTRY
        .get_or_init(|| load_player_impact_registry().ok())
        .as_ref()
}

fn registry_path_override() -> Option<PathBuf> {
    env::var("PLAYER_IMPACT_ARTIFACT_PATH")
        .ok()
        .map(|s| PathBuf::from(s.trim()))
}

fn default_registry_cache_path() -> Option<PathBuf> {
    app_cache_dir().map(|dir| dir.join("player_impact_registry_v1.json"))
}

fn use_shared_prior_enabled() -> bool {
    match env::var("PLAYER_IMPACT_USE_SHARED_PRIOR") {
        Ok(raw) => !matches!(
            raw.trim().to_ascii_lowercase().as_str(),
            "0" | "false" | "off" | "no"
        ),
        Err(_) => true,
    }
}

fn fallback_registry_from_legacy_assets() -> Result<PlayerImpactRegistry> {
    use crate::pl_dataset::PREMIER_LEAGUE_ID;
    use crate::pl_player_impact::PlayerImpactArtifact as LegacyArtifact;

    let raw_v2 = include_str!("../assets/pl_player_impact_v2.json");
    let legacy = serde_json::from_str::<LegacyArtifact>(raw_v2).or_else(|_| {
        let raw_v1 = include_str!("../assets/pl_player_impact_v1.json");
        serde_json::from_str::<LegacyArtifact>(raw_v1)
    })?;

    let league = LeaguePlayerImpactArtifact {
        league_id: PREMIER_LEAGUE_ID,
        k_player_impact: legacy.k_player_impact,
        min_player_samples: legacy.min_player_samples,
        model_v2: legacy.model_v2.map(|m| PlayerImpactLinearModelV2 {
            feature_names: m.feature_names,
            feature_means: m.feature_means,
            feature_stds: m.feature_stds,
            coeffs: m.coeffs,
            recency_half_life_days: m.recency_half_life_days,
            l2: m.l2,
            train_log_loss: m.train_log_loss,
            val_log_loss: m.val_log_loss,
            baseline_val_log_loss: m.baseline_val_log_loss,
            train_samples: m.train_samples,
            val_samples: m.val_samples,
        }),
        entries: legacy
            .entries
            .into_iter()
            .map(|e| PlayerImpactEntry {
                team_norm: e.team_norm,
                player_norm: e.player_norm,
                prior: e.prior,
                samples: e.samples,
                minutes: e.minutes,
                rating: e.rating,
                shots_on_target: e.shots_on_target,
                key_passes: e.key_passes,
                tackles_interceptions: e.tackles_interceptions,
                duel_win_rate: e.duel_win_rate,
                cards: e.cards,
            })
            .collect(),
    };

    let artifact = PlayerImpactRegistryArtifact {
        version: 1,
        generated_at: chrono::Utc::now().to_rfc3339(),
        source: Some("legacy_pl_assets".to_string()),
        leagues: vec![league.clone()],
        shared_prior: Some(league),
    };

    Ok(PlayerImpactRegistry::from_artifact(artifact))
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

fn standardized(raw: f64, idx: usize, model: &PlayerImpactLinearModelV2) -> f64 {
    let mean = model.feature_means.get(idx).copied().unwrap_or(0.0);
    let std = model
        .feature_stds
        .get(idx)
        .copied()
        .unwrap_or(1.0)
        .max(1e-6);
    (raw - mean) / std
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_name_compacts() {
        assert_eq!(normalize_name(" Man City "), "man_city");
        assert_eq!(normalize_name("AC-Milan"), "ac_milan");
    }

    #[test]
    fn registry_fallback_model_prefers_shared_prior() {
        let shared = LeaguePlayerImpactArtifact {
            league_id: 0,
            k_player_impact: 0.1,
            min_player_samples: 1,
            model_v2: None,
            entries: vec![PlayerImpactEntry {
                team_norm: "x".into(),
                player_norm: "y".into(),
                prior: 0.2,
                samples: 10,
                minutes: 900.0,
                rating: 7.0,
                shots_on_target: 1.0,
                key_passes: 1.0,
                tackles_interceptions: 1.0,
                duel_win_rate: 0.5,
                cards: 0.2,
            }],
        };
        let reg = PlayerImpactRegistry::from_artifact(PlayerImpactRegistryArtifact {
            version: 1,
            generated_at: "t".into(),
            source: None,
            leagues: Vec::new(),
            shared_prior: Some(shared),
        });
        assert!(reg.fallback_model(Some(99)).is_some());
    }
}
