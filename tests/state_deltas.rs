use wc26_terminal::state::{
    AppState, CommentaryEntry, Delta, Event, EventKind, LineupSide, MatchDetail, MatchLineups,
    PlayerSlot, Screen, StatRow, apply_delta,
};

fn rich_detail() -> MatchDetail {
    MatchDetail {
        home_team: Some("HOME".to_string()),
        away_team: Some("AWAY".to_string()),
        events: vec![Event {
            minute: 12,
            kind: EventKind::Goal,
            team: "HOME".to_string(),
            description: "Goal".to_string(),
        }],
        commentary: vec![CommentaryEntry {
            minute: Some(12),
            minute_plus: None,
            team: Some("HOME".to_string()),
            text: "Scored!".to_string(),
        }],
        commentary_error: Some("previous error".to_string()),
        lineups: Some(MatchLineups {
            sides: vec![
                LineupSide {
                    team: "HOME".to_string(),
                    team_abbr: "HOM".to_string(),
                    formation: "4-3-3".to_string(),
                    starting: vec![PlayerSlot {
                        id: Some(1),
                        name: "P1".to_string(),
                        number: Some(1),
                        pos: Some("GK".to_string()),
                    }],
                    subs: Vec::new(),
                },
                LineupSide {
                    team: "AWAY".to_string(),
                    team_abbr: "AWY".to_string(),
                    formation: "4-4-2".to_string(),
                    starting: vec![PlayerSlot {
                        id: Some(2),
                        name: "P2".to_string(),
                        number: Some(9),
                        pos: Some("FW".to_string()),
                    }],
                    subs: Vec::new(),
                },
            ],
        }),
        stats: vec![StatRow {
            group: Some("Top stats".to_string()),
            name: "Possession".to_string(),
            home: "55%".to_string(),
            away: "45%".to_string(),
        }],
    }
}

#[test]
fn set_match_details_basic_does_not_clobber_richer_existing_detail() {
    let mut state = AppState::new();
    state.screen = Screen::Pulse;

    let id = "m1".to_string();
    state.match_detail.insert(id.clone(), rich_detail());

    let incoming = MatchDetail {
        home_team: None,
        away_team: None,
        events: Vec::new(),
        commentary: Vec::new(),
        commentary_error: None,
        lineups: None,
        stats: Vec::new(),
    };

    apply_delta(
        &mut state,
        Delta::SetMatchDetailsBasic {
            id: id.clone(),
            detail: incoming,
        },
    );

    let out = state.match_detail.get(&id).expect("detail should exist");
    assert_eq!(out.home_team.as_deref(), Some("HOME"));
    assert_eq!(out.away_team.as_deref(), Some("AWAY"));
    assert!(!out.events.is_empty());
    assert!(!out.stats.is_empty());
    assert!(out.lineups.is_some());
    assert!(!out.commentary.is_empty());
    assert_eq!(out.commentary_error.as_deref(), Some("previous error"));
}

#[test]
fn set_match_details_basic_clears_commentary_error_when_commentary_is_present() {
    let mut state = AppState::new();
    let id = "m2".to_string();
    state.match_detail.insert(id.clone(), rich_detail());

    let incoming = MatchDetail {
        home_team: None,
        away_team: None,
        events: Vec::new(),
        commentary: vec![CommentaryEntry {
            minute: Some(13),
            minute_plus: None,
            team: None,
            text: "New entry".to_string(),
        }],
        commentary_error: None,
        lineups: None,
        stats: Vec::new(),
    };

    apply_delta(
        &mut state,
        Delta::SetMatchDetailsBasic {
            id: id.clone(),
            detail: incoming,
        },
    );

    let out = state.match_detail.get(&id).expect("detail should exist");
    assert!(!out.commentary.is_empty());
    assert!(out.commentary_error.is_none());
}
