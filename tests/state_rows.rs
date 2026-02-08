use wc26_terminal::state::{AppState, PulseLiveRow, PulseView, Screen, UpcomingMatch};

#[test]
fn pulse_rows_dedup_upcoming_ids() {
    let mut state = AppState::new();
    state.screen = Screen::Pulse;
    state.pulse_view = PulseView::Live;

    state.upcoming = vec![
        UpcomingMatch {
            id: "u1".to_string(),
            league_id: Some(47),
            league_name: "Premier League".to_string(),
            round: "R".to_string(),
            kickoff: "2026-01-01 12:00".to_string(),
            home_team_id: None,
            away_team_id: None,
            home: "H".to_string(),
            away: "A".to_string(),
        },
        UpcomingMatch {
            id: "u1".to_string(),
            league_id: Some(47),
            league_name: "Premier League".to_string(),
            round: "R".to_string(),
            kickoff: "2026-01-01 12:00".to_string(),
            home_team_id: None,
            away_team_id: None,
            home: "H".to_string(),
            away: "A".to_string(),
        },
    ];

    let rows = state.pulse_live_rows();
    let upcoming_count = rows
        .iter()
        .filter(|row| matches!(row, PulseLiveRow::Upcoming(_)))
        .count();
    assert_eq!(upcoming_count, 1);
}

#[test]
fn selected_match_id_returns_upcoming_id_in_pulse_live_rows() {
    let mut state = AppState::new();
    state.screen = Screen::Pulse;
    state.pulse_view = PulseView::Live;

    state.upcoming = vec![UpcomingMatch {
        id: "u123".to_string(),
        league_id: Some(47),
        league_name: "Premier League".to_string(),
        round: "R".to_string(),
        kickoff: "2026-01-01 12:00".to_string(),
        home_team_id: None,
        away_team_id: None,
        home: "LIV".to_string(),
        away: "MCI".to_string(),
    }];

    // With no matches, pulse_live_rows() will consist solely of upcoming rows.
    state.selected = 0;
    assert_eq!(state.selected_match_id().as_deref(), Some("u123"));
    assert!(matches!(
        state.pulse_live_rows().first(),
        Some(PulseLiveRow::Upcoming(_))
    ));
}
