use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

use wc26_terminal::analysis_fetch::parse_player_detail_json;
use wc26_terminal::analysis_rankings::compute_role_rankings_from_cache;
use wc26_terminal::state::{Confederation, SquadPlayer, TeamAnalysis};

fn read_fixture(name: &str) -> String {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("tests");
    path.push("fixtures");
    path.push(name);
    fs::read_to_string(path).expect("fixture file should be readable")
}

#[test]
fn player_detail_parses_percentiles() {
    let raw = read_fixture("player_detail_rich_alpha.json");
    let detail = parse_player_detail_json(&raw).expect("fixture should parse");
    let goals = detail
        .season_performance
        .iter()
        .flat_map(|g| g.items.iter())
        .find(|item| item.title.eq_ignore_ascii_case("Goals"))
        .expect("goals stat should exist");
    assert_eq!(goals.percentile_rank, Some(92.0));
    assert_eq!(goals.percentile_rank_per90, Some(92.0));
}

#[test]
fn player_detail_parses_when_main_league_is_partial() {
    // FotMob sometimes returns `{ "mainLeague": { "stats": null } }` (missing leagueName/season).
    // That should not fail player parsing.
    let raw = r#"{"id":1,"name":"Test Player","mainLeague":{"stats":null}}"#;
    let detail = parse_player_detail_json(raw).expect("partial mainLeague should parse");
    assert_eq!(detail.id, 1);
    assert_eq!(detail.name, "Test Player");
    assert!(detail.main_league.is_none());
}

#[test]
fn rankings_weighted_and_explainable() {
    let team = TeamAnalysis {
        id: 1,
        name: "Test FC".to_string(),
        confed: Confederation::UEFA,
        host: false,
        fifa_rank: None,
        fifa_points: None,
        fifa_updated: None,
    };

    let alpha = parse_player_detail_json(&read_fixture("player_detail_rich_alpha.json"))
        .expect("alpha should parse");
    let beta = parse_player_detail_json(&read_fixture("player_detail_rich_beta.json"))
        .expect("beta should parse");
    let gamma = parse_player_detail_json(&read_fixture("player_detail_rich_gamma_sparse.json"))
        .expect("gamma should parse");

    let squads: HashMap<u32, Vec<SquadPlayer>> = HashMap::from([(
        team.id,
        vec![
            SquadPlayer {
                id: alpha.id,
                name: alpha.name.clone(),
                role: "Attacker".to_string(),
                club: "Test Club".to_string(),
                age: None,
                height: None,
                shirt_number: None,
                market_value: None,
            },
            SquadPlayer {
                id: beta.id,
                name: beta.name.clone(),
                role: "Attacker".to_string(),
                club: "Test Club".to_string(),
                age: None,
                height: None,
                shirt_number: None,
                market_value: None,
            },
            SquadPlayer {
                id: gamma.id,
                name: gamma.name.clone(),
                role: "Attacker".to_string(),
                club: "Test Club".to_string(),
                age: None,
                height: None,
                shirt_number: None,
                market_value: None,
            },
        ],
    )]);

    let players = HashMap::from([(alpha.id, alpha), (beta.id, beta), (gamma.id, gamma)]);

    let rows = compute_role_rankings_from_cache(&[team], &squads, &players);
    assert_eq!(rows.len(), 3);

    let alpha_row = rows.iter().find(|r| r.player_id == 101).unwrap();
    let beta_row = rows.iter().find(|r| r.player_id == 102).unwrap();
    let gamma_row = rows.iter().find(|r| r.player_id == 103).unwrap();

    assert!(alpha_row.attack_score.is_finite());
    assert!(beta_row.attack_score.is_finite());
    assert!(!gamma_row.attack_score.is_finite());

    // Low minutes should not outrank high minutes with similar-ish stats.
    assert!(alpha_row.attack_score > beta_row.attack_score);

    // Explainability fields should be populated for players with enough coverage.
    assert!(!alpha_row.attack_factors.is_empty());
    assert!(!beta_row.attack_factors.is_empty());
    assert!(alpha_row.attack_factors.len() <= 5);
}
