use std::collections::HashMap;

use criterion::{Criterion, criterion_group, criterion_main};
use std::hint::black_box;

use wc26_terminal::analysis_fetch::parse_player_detail_json;
use wc26_terminal::analysis_rankings::compute_role_rankings_from_cache;
use wc26_terminal::state::{AppState, Confederation, PlayerDetail, SquadPlayer, TeamAnalysis};
use wc26_terminal::upcoming_fetch::{
    parse_fotmob_matches_json, parse_fotmob_upcoming_json, parse_match_details_json,
};

fn sample_player_detail(id: u32, name: &str) -> PlayerDetail {
    let base = parse_player_detail_json(PLAYER_JSON).expect("valid fixture json");
    PlayerDetail {
        id,
        name: name.to_string(),
        team: Some("Test FC".to_string()),
        position: base.position.clone(),
        age: base.age.clone(),
        country: base.country.clone(),
        height: base.height.clone(),
        preferred_foot: base.preferred_foot.clone(),
        shirt: base.shirt.clone(),
        market_value: base.market_value.clone(),
        contract_end: base.contract_end.clone(),
        birth_date: base.birth_date.clone(),
        status: base.status.clone(),
        injury_info: base.injury_info.clone(),
        international_duty: base.international_duty.clone(),
        positions: base.positions.clone(),
        all_competitions: base.all_competitions.clone(),
        all_competitions_season: base.all_competitions_season.clone(),
        main_league: base.main_league.clone(),
        top_stats: base.top_stats.clone(),
        season_groups: base.season_groups.clone(),
        season_performance: base.season_performance.clone(),
        traits: base.traits.clone(),
        recent_matches: base.recent_matches.clone(),
        season_breakdown: base.season_breakdown.clone(),
        career_sections: base.career_sections.clone(),
        trophies: base.trophies.clone(),
    }
}

fn bench_player_detail_parse(c: &mut Criterion) {
    c.bench_function("player_detail_parse", |b| {
        b.iter(|| {
            let detail = parse_player_detail_json(black_box(PLAYER_JSON)).unwrap();
            black_box(detail.id);
        })
    });
}

fn bench_rankings_compute(c: &mut Criterion) {
    let team = TeamAnalysis {
        id: 1,
        name: "Test FC".to_string(),
        confed: Confederation::UEFA,
        host: false,
        fifa_rank: Some(5),
        fifa_points: Some(1820),
        fifa_updated: Some("2024-01-01".to_string()),
    };

    let players: Vec<SquadPlayer> = (0..30)
        .map(|idx| SquadPlayer {
            id: idx + 1,
            name: format!("Player {}", idx + 1),
            role: if idx % 4 == 0 {
                "Goalkeeper".to_string()
            } else if idx % 4 == 1 {
                "Defender".to_string()
            } else if idx % 4 == 2 {
                "Midfielder".to_string()
            } else {
                "Attacker".to_string()
            },
            club: "Test FC".to_string(),
            age: Some(24),
            height: Some(180),
            shirt_number: Some(idx + 1),
            market_value: Some(5_000_000),
        })
        .collect();

    let mut squads = HashMap::new();
    squads.insert(team.id, players.clone());

    let mut player_details = HashMap::new();
    for player in players {
        player_details.insert(player.id, sample_player_detail(player.id, &player.name));
    }

    c.bench_function("rankings_compute", |b| {
        b.iter(|| {
            let rows = compute_role_rankings_from_cache(
                black_box(&[team.clone()]),
                black_box(&squads),
                black_box(&player_details),
            );
            black_box(rows.len());
        })
    });
}

fn bench_prefetch_filtering(c: &mut Criterion) {
    let mut state = AppState::new();
    let now = std::time::SystemTime::now();
    for id in 1..=500u32 {
        state.rankings_cache_players_at.insert(id, now);
        state
            .rankings_cache_players
            .insert(id, sample_player_detail(id, "Cached"));
    }
    let candidates: Vec<u32> = (1..=500).collect();

    c.bench_function("prefetch_filtering", |b| {
        b.iter(|| {
            let mut ids: Vec<u32> = candidates
                .iter()
                .copied()
                .filter(|id| state.rankings_cache_players_at.get(id).is_none())
                .collect();
            ids.sort_unstable();
            ids.dedup();
            ids.truncate(10);
            black_box(ids.len());
        })
    });
}

fn bench_prefetch_queue_build(c: &mut Criterion) {
    let mut state = AppState::new();
    let mut players = Vec::new();
    for id in 1..=40u32 {
        players.push(SquadPlayer {
            id,
            name: format!("Player {id}"),
            role: "Attacker".to_string(),
            club: "Test FC".to_string(),
            age: Some(24),
            height: Some(180),
            shirt_number: Some(id),
            market_value: Some(5_000_000),
        });
    }
    state.squad = players;

    c.bench_function("prefetch_queue_build", |b| {
        b.iter(|| {
            let ids: Vec<u32> = state.squad.iter().map(|p| p.id).collect();
            black_box(ids.len());
        })
    });
}

fn bench_match_details_parse(c: &mut Criterion) {
    c.bench_function("match_details_parse", |b| {
        b.iter(|| {
            let detail = parse_match_details_json(black_box(MATCH_DETAILS_JSON)).unwrap();
            black_box(detail.events.len());
        })
    });
}

fn bench_fotmob_matches_parse(c: &mut Criterion) {
    c.bench_function("fotmob_matches_parse", |b| {
        b.iter(|| {
            let rows = parse_fotmob_matches_json(black_box(FOTMOB_MATCHES_JSON)).unwrap();
            black_box(rows.len());
        })
    });
}

fn bench_fotmob_upcoming_parse(c: &mut Criterion) {
    c.bench_function("fotmob_upcoming_parse", |b| {
        b.iter(|| {
            let rows = parse_fotmob_upcoming_json(black_box(FOTMOB_MATCHES_JSON)).unwrap();
            black_box(rows.len());
        })
    });
}

criterion_group!(
    perf,
    bench_player_detail_parse,
    bench_rankings_compute,
    bench_prefetch_filtering,
    bench_prefetch_queue_build,
    bench_match_details_parse,
    bench_fotmob_matches_parse,
    bench_fotmob_upcoming_parse
);
criterion_main!(perf);

static PLAYER_JSON: &str = include_str!("../tests/fixtures/player_detail.json");
static MATCH_DETAILS_JSON: &str = include_str!("../tests/fixtures/match_details.json");
static FOTMOB_MATCHES_JSON: &str = include_str!("../tests/fixtures/fotmob_matches.json");
