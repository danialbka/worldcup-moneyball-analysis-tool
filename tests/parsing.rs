use std::fs;
use std::path::PathBuf;

use wc26_terminal::state::EventKind;
use wc26_terminal::upcoming_fetch::{
    parse_fotmob_matches_json, parse_fotmob_upcoming_json, parse_match_details_json,
};

fn read_fixture(name: &str) -> String {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("tests");
    path.push("fixtures");
    path.push(name);
    fs::read_to_string(path).expect("fixture file should be readable")
}

#[test]
fn parses_fotmob_matches_fixture() {
    let raw = read_fixture("fotmob_matches.json");
    let rows = parse_fotmob_matches_json(&raw).expect("fixture should parse");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].id, "1001");
    assert!(rows[0].started);
    assert!(rows[0].finished);
    assert_eq!(rows[0].home_score, 2);
    assert_eq!(rows[0].away_score, 1);
}

#[test]
fn parses_fotmob_upcoming_fixture() {
    let raw = read_fixture("fotmob_matches.json");
    let upcoming = parse_fotmob_upcoming_json(&raw).expect("fixture should parse");
    assert_eq!(upcoming.len(), 1);
    assert_eq!(upcoming[0].id, "1002");
    assert_eq!(upcoming[0].home, "LIV");
    assert_eq!(upcoming[0].away, "MCI");
}

#[test]
fn parses_match_details_fixture() {
    let raw = read_fixture("match_details.json");
    let detail = parse_match_details_json(&raw).expect("fixture should parse");
    assert_eq!(detail.events.len(), 2);
    assert_eq!(detail.events[0].minute, 12);
    assert_eq!(detail.events[0].kind, EventKind::Goal);
    assert_eq!(detail.events[1].kind, EventKind::Card);
    assert!(detail.lineups.as_ref().is_some_and(|l| l.sides.len() == 2));
    assert!(!detail.stats.is_empty());
}

#[test]
fn match_details_null_is_empty() {
    let detail = parse_match_details_json("null").expect("null should parse");
    assert!(detail.events.is_empty());
    assert!(detail.stats.is_empty());
    assert!(detail.lineups.is_none());
}

#[test]
fn fotmob_null_is_empty() {
    assert!(
        parse_fotmob_matches_json("null")
            .expect("null should parse")
            .is_empty()
    );
    assert!(
        parse_fotmob_upcoming_json("null")
            .expect("null should parse")
            .is_empty()
    );
}
