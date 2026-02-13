use wc26_terminal::state::{AppState, Delta, MatchSummary, ModelQuality, WinProbRow, apply_delta};

#[test]
fn prematch_snapshot_is_frozen_on_kickoff_transition() {
    let mut state = AppState::new();
    let id = "m1".to_string();

    state.matches.push(MatchSummary {
        id: id.clone(),
        league_id: Some(47),
        league_name: "Premier League".to_string(),
        home_team_id: None,
        away_team_id: None,
        home: "LIV".to_string(),
        away: "MCI".to_string(),
        minute: 0,
        score_home: 0,
        score_away: 0,
        win: WinProbRow {
            p_home: 12.0,
            p_draw: 34.0,
            p_away: 54.0,
            delta_home: 0.0,
            quality: ModelQuality::Basic,
            confidence: 11,
        },
        is_live: false,
        market_odds: None,
    });

    // Kickoff: the live match arrives.
    apply_delta(
        &mut state,
        Delta::UpsertMatch(MatchSummary {
            id: id.clone(),
            league_id: Some(47),
            league_name: "Premier League".to_string(),
            home_team_id: None,
            away_team_id: None,
            home: "LIV".to_string(),
            away: "MCI".to_string(),
            minute: 1,
            score_home: 0,
            score_away: 0,
            win: WinProbRow {
                p_home: 0.0,
                p_draw: 0.0,
                p_away: 0.0,
                delta_home: 0.0,
                quality: ModelQuality::Event,
                confidence: 0,
            },
            is_live: true,
            market_odds: None,
        }),
    );

    assert!(state.prematch_locked.contains(&id));
    let pre = state.prematch_win.get(&id).expect("prematch stored");
    assert!((pre.p_home - 12.0).abs() < f32::EPSILON);
    assert!((pre.p_draw - 34.0).abs() < f32::EPSILON);
    assert!((pre.p_away - 54.0).abs() < f32::EPSILON);
}
