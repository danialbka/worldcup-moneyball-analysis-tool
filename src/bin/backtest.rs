use std::fs;
use std::path::PathBuf;

use wc26_terminal::state::{MatchDetail, MatchSummary, ModelQuality, TeamAnalysis, WinProbRow};
use wc26_terminal::win_prob;

#[derive(Debug, serde::Deserialize)]
struct BacktestCase {
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    league_name: Option<String>,
    #[serde(default)]
    league_id: Option<u32>,
    home: String,
    away: String,
    minute: u16,
    score_home: u8,
    score_away: u8,
    #[serde(default)]
    is_live: bool,
    detail: Option<MatchDetail>,
    #[serde(default)]
    analysis: Vec<TeamAnalysis>,
}

fn main() -> anyhow::Result<()> {
    let path = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("tests/fixtures/backtest_case.json"));

    let raw = fs::read_to_string(&path)?;
    let case: BacktestCase = serde_json::from_str(&raw)?;

    let summary = MatchSummary {
        id: case.id.unwrap_or_else(|| "backtest".to_string()),
        league_id: case.league_id,
        league_name: case.league_name.unwrap_or_else(|| "Backtest".to_string()),
        home: case.home,
        away: case.away,
        minute: case.minute,
        score_home: case.score_home,
        score_away: case.score_away,
        win: WinProbRow {
            p_home: 0.0,
            p_draw: 0.0,
            p_away: 0.0,
            delta_home: 0.0,
            quality: ModelQuality::Basic,
            confidence: 0,
        },
        is_live: case.is_live,
    };

    // This binary is intentionally simple: it loads one snapshot and prints the model output.
    // It avoids network calls and is meant for quick manual calibration/tuning iterations.
    let win = win_prob::compute_win_prob(
        &summary,
        case.detail.as_ref(),
        &std::collections::HashMap::new(),
        &case.analysis,
    );

    println!("Home: {:.1}%", win.p_home);
    println!("Draw: {:.1}%", win.p_draw);
    println!("Away: {:.1}%", win.p_away);
    println!("Quality: {:?}", win.quality);
    println!("Confidence: {}", win.confidence);

    Ok(())
}
