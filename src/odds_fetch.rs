use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::env;

use aes::Aes256;
use anyhow::{Context, Result};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use cbc::cipher::{BlockDecryptMut, KeyIvInit, block_padding::Pkcs7};
use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use pbkdf2::pbkdf2_hmac;
use reqwest::header::USER_AGENT;
use serde::Deserialize;
use sha2::Sha256;

use crate::http_client::http_client;
use crate::state::{LeagueMode, MarketOddsSnapshot};

type Aes256CbcDec = cbc::Decryptor<Aes256>;

const DEFAULT_TIME_TOLERANCE_MIN: i64 = 90;

#[derive(Debug, Clone)]
pub struct OddsFetchConfig {
    pub enabled: bool,
    pub provider: String,
    pub api_key: Option<String>,
    pub regions: String,
    pub time_tolerance_secs: i64,
}

impl OddsFetchConfig {
    pub fn from_env() -> Self {
        let enabled = env_bool("ODDS_ENABLED", true);
        let provider = env::var("ODDS_PROVIDER")
            .unwrap_or_else(|_| "oddsportal".to_string())
            .trim()
            .to_ascii_lowercase();
        let api_key = env::var("ODDS_API_KEY")
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());
        let regions = env::var("ODDS_REGIONS")
            .unwrap_or_else(|_| "us".to_string())
            .trim()
            .to_ascii_lowercase();
        let time_tolerance_min = env::var("ODDS_MATCH_TIME_TOLERANCE_MIN")
            .ok()
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(DEFAULT_TIME_TOLERANCE_MIN)
            .clamp(5, 360);

        Self {
            enabled,
            provider,
            api_key,
            regions,
            time_tolerance_secs: time_tolerance_min * 60,
        }
    }
}

#[derive(Debug, Clone)]
pub struct OddsFixtureRef {
    pub id: String,
    pub league_id: Option<u32>,
    pub home: String,
    pub away: String,
    pub kickoff: Option<String>,
}

#[derive(Debug, Deserialize)]
struct OddsEvent {
    #[serde(rename = "commence_time")]
    commence_time: Option<String>,
    home_team: String,
    away_team: String,
    #[serde(default)]
    bookmakers: Vec<OddsBookmaker>,
}

#[derive(Debug, Deserialize)]
struct OddsBookmaker {
    #[serde(default)]
    markets: Vec<OddsMarket>,
}

#[derive(Debug, Deserialize)]
struct OddsMarket {
    key: String,
    #[serde(default)]
    outcomes: Vec<OddsOutcome>,
}

#[derive(Debug, Deserialize)]
struct OddsOutcome {
    name: String,
    price: f64,
}

#[derive(Debug, Clone)]
struct OddsEventCandidate {
    kickoff_ts: Option<i64>,
    home_aliases: HashSet<String>,
    away_aliases: HashSet<String>,
    snapshot: MarketOddsSnapshot,
}

pub fn fetch_market_odds_for_fixtures(
    fixtures: &[OddsFixtureRef],
    mode: LeagueMode,
    active_league_ids: &HashSet<u32>,
    cfg: &OddsFetchConfig,
) -> Result<HashMap<String, MarketOddsSnapshot>> {
    if !cfg.enabled {
        return Ok(HashMap::new());
    }

    let filtered_fixtures: Vec<&OddsFixtureRef> = fixtures
        .iter()
        .filter(|f| {
            if active_league_ids.is_empty() {
                return true;
            }
            match f.league_id {
                Some(id) => active_league_ids.contains(&id),
                None => false,
            }
        })
        .collect();
    if filtered_fixtures.is_empty() {
        return Ok(HashMap::new());
    }

    match cfg.provider.as_str() {
        "oddsportal" => {
            fetch_oddsportal_for_fixtures(&filtered_fixtures, mode, cfg.time_tolerance_secs)
        }
        "theoddsapi" => fetch_theoddsapi_for_fixtures(&filtered_fixtures, mode, cfg),
        other => Err(anyhow::anyhow!(
            "unsupported ODDS_PROVIDER={other}, expected oddsportal or theoddsapi"
        )),
    }
}

fn fetch_theoddsapi_for_fixtures(
    fixtures: &[&OddsFixtureRef],
    mode: LeagueMode,
    cfg: &OddsFetchConfig,
) -> Result<HashMap<String, MarketOddsSnapshot>> {
    let Some(api_key) = cfg.api_key.as_ref() else {
        return Err(anyhow::anyhow!("ODDS_API_KEY missing"));
    };
    let Some(sport_key) = sport_key_for_mode(mode) else {
        return Ok(HashMap::new());
    };

    let url = format!("https://api.the-odds-api.com/v4/sports/{sport_key}/odds");
    let client = http_client()?;
    let resp = client
        .get(&url)
        .query(&[
            ("apiKey", api_key.as_str()),
            ("regions", cfg.regions.as_str()),
            ("markets", "h2h"),
            ("oddsFormat", "decimal"),
            ("dateFormat", "iso"),
        ])
        .header(USER_AGENT, "wc26-terminal/0.1")
        .send()
        .context("odds request failed")?;
    let status = resp.status();
    let body = resp.text().context("failed reading odds body")?;
    if !status.is_success() {
        let snippet = body
            .trim()
            .replace('\n', " ")
            .replace('\r', " ")
            .chars()
            .take(220)
            .collect::<String>();
        return Err(anyhow::anyhow!("odds http {}: {}", status, snippet));
    }

    let parsed: Vec<OddsEvent> = serde_json::from_str(&body).context("invalid odds json")?;
    let candidates: Vec<OddsEventCandidate> =
        parsed.iter().filter_map(event_to_candidate).collect();

    Ok(match_candidates_to_fixtures(
        fixtures,
        &candidates,
        cfg.time_tolerance_secs,
    ))
}

fn sport_key_for_mode(mode: LeagueMode) -> Option<&'static str> {
    match mode {
        LeagueMode::PremierLeague => Some("soccer_epl"),
        LeagueMode::LaLiga => Some("soccer_spain_la_liga"),
        LeagueMode::Bundesliga => Some("soccer_germany_bundesliga"),
        LeagueMode::SerieA => Some("soccer_italy_serie_a"),
        LeagueMode::Ligue1 => Some("soccer_france_ligue_one"),
        LeagueMode::ChampionsLeague => Some("soccer_uefa_champs_league"),
        LeagueMode::WorldCup => Some("soccer_fifa_world_cup"),
    }
}

fn event_to_candidate(event: &OddsEvent) -> Option<OddsEventCandidate> {
    let mut book_home_probs = Vec::new();
    let mut book_draw_probs = Vec::new();
    let mut book_away_probs = Vec::new();
    let mut book_home_decimal = Vec::new();
    let mut book_draw_decimal = Vec::new();
    let mut book_away_decimal = Vec::new();

    for bookmaker in &event.bookmakers {
        let Some(market) = bookmaker
            .markets
            .iter()
            .find(|m| m.key.eq_ignore_ascii_case("h2h"))
        else {
            continue;
        };

        let maybe_triplet =
            extract_hda_prices(&market.outcomes, &event.home_team, &event.away_team)
                .and_then(|(home, draw, away)| no_vig_from_decimal(home, draw, away));
        let Some((home_prob, draw_prob, away_prob, home_dec, draw_dec, away_dec)) = maybe_triplet
        else {
            continue;
        };

        book_home_probs.push(home_prob);
        book_draw_probs.push(draw_prob);
        book_away_probs.push(away_prob);
        book_home_decimal.push(home_dec);
        book_draw_decimal.push(draw_dec);
        book_away_decimal.push(away_dec);
    }

    if book_home_probs.is_empty() || book_draw_probs.is_empty() || book_away_probs.is_empty() {
        return None;
    }

    let fetched_at_unix = Utc::now().timestamp();
    Some(OddsEventCandidate {
        kickoff_ts: event.commence_time.as_deref().and_then(parse_timestamp),
        home_aliases: team_aliases(&event.home_team),
        away_aliases: team_aliases(&event.away_team),
        snapshot: MarketOddsSnapshot {
            source: "theoddsapi".to_string(),
            fetched_at_unix,
            bookmakers_used: book_home_probs.len().min(u8::MAX as usize) as u8,
            home_decimal: median_f64(&book_home_decimal),
            draw_decimal: median_f64(&book_draw_decimal),
            away_decimal: median_f64(&book_away_decimal),
            implied_home: median_f64(&book_home_probs).map(|v| (v * 100.0) as f32),
            implied_draw: median_f64(&book_draw_probs).map(|v| (v * 100.0) as f32),
            implied_away: median_f64(&book_away_probs).map(|v| (v * 100.0) as f32),
            stale: false,
        },
    })
}

fn extract_hda_prices(
    outcomes: &[OddsOutcome],
    home_team: &str,
    away_team: &str,
) -> Option<(f64, f64, f64)> {
    let home_aliases = team_aliases(home_team);
    let away_aliases = team_aliases(away_team);

    let mut home: Option<f64> = None;
    let mut draw: Option<f64> = None;
    let mut away: Option<f64> = None;

    for outcome in outcomes {
        let name = outcome.name.trim();
        if is_draw_label(name) {
            draw = Some(outcome.price);
            continue;
        }
        let aliases = team_aliases(name);
        if aliases_intersect(&aliases, &home_aliases) {
            home = Some(outcome.price);
            continue;
        }
        if aliases_intersect(&aliases, &away_aliases) {
            away = Some(outcome.price);
        }
    }

    match (home, draw, away) {
        (Some(home), Some(draw), Some(away)) => Some((home, draw, away)),
        _ => None,
    }
}

fn no_vig_from_decimal(home: f64, draw: f64, away: f64) -> Option<(f64, f64, f64, f64, f64, f64)> {
    if home <= 1.0 || draw <= 1.0 || away <= 1.0 {
        return None;
    }
    let ih = 1.0 / home;
    let id = 1.0 / draw;
    let ia = 1.0 / away;
    let sum = ih + id + ia;
    if sum <= 0.0 {
        return None;
    }
    Some((ih / sum, id / sum, ia / sum, home, draw, away))
}

fn match_candidates_to_fixtures(
    fixtures: &[&OddsFixtureRef],
    candidates: &[OddsEventCandidate],
    time_tolerance_secs: i64,
) -> HashMap<String, MarketOddsSnapshot> {
    let mut out = HashMap::new();
    let mut used: HashSet<usize> = HashSet::new();

    for fixture in fixtures {
        let f_home_aliases = team_aliases(&fixture.home);
        let f_away_aliases = team_aliases(&fixture.away);
        let fixture_ts = fixture.kickoff.as_deref().and_then(parse_timestamp);

        let mut best: Option<(usize, i64)> = None;
        for (idx, candidate) in candidates.iter().enumerate() {
            if used.contains(&idx) {
                continue;
            }
            if !aliases_intersect(&f_home_aliases, &candidate.home_aliases)
                || !aliases_intersect(&f_away_aliases, &candidate.away_aliases)
            {
                continue;
            }
            let score = match (fixture_ts, candidate.kickoff_ts) {
                (Some(f), Some(c)) => {
                    let diff = (f - c).abs();
                    if diff > time_tolerance_secs {
                        continue;
                    }
                    diff
                }
                _ => time_tolerance_secs / 2,
            };
            if let Some((_, best_score)) = best {
                if score >= best_score {
                    continue;
                }
            }
            best = Some((idx, score));
        }

        if let Some((idx, _)) = best {
            used.insert(idx);
            out.insert(fixture.id.clone(), candidates[idx].snapshot.clone());
        }
    }

    out
}

fn env_bool(key: &str, default: bool) -> bool {
    env::var(key)
        .ok()
        .map(|v| {
            let t = v.trim().to_ascii_lowercase();
            !(t.is_empty() || t == "0" || t == "false" || t == "off" || t == "no")
        })
        .unwrap_or(default)
}

fn median_f64(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
    let mid = sorted.len() / 2;
    if sorted.len() % 2 == 0 {
        Some((sorted[mid - 1] + sorted[mid]) / 2.0)
    } else {
        Some(sorted[mid])
    }
}

fn parse_timestamp(raw: &str) -> Option<i64> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    if let Ok(dt) = DateTime::parse_from_rfc3339(trimmed) {
        return Some(dt.timestamp());
    }
    for fmt in [
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
    ] {
        if let Ok(naive) = NaiveDateTime::parse_from_str(trimmed, fmt) {
            return Some(Utc.from_utc_datetime(&naive).timestamp());
        }
    }
    None
}

fn is_draw_label(name: &str) -> bool {
    let n = normalize_word(name);
    n == "draw" || n == "tie" || n == "x"
}

fn team_aliases(name: &str) -> HashSet<String> {
    let mut out = HashSet::new();
    let words = canonical_words(name);
    if words.is_empty() {
        return out;
    }

    let collapsed = words.join("");
    if collapsed.len() >= 2 {
        out.insert(collapsed.clone());
    }
    if let Some(prefix) = prefix(&collapsed, 3) {
        out.insert(prefix);
    }

    let acronym: String = words.iter().filter_map(|w| w.chars().next()).collect();
    if acronym.len() >= 2 {
        out.insert(acronym);
    }
    if words.len() >= 2 {
        let mut first_plus_last = String::new();
        if let Some(ch) = words[0].chars().next() {
            first_plus_last.push(ch);
        }
        if let Some(last) = words.last()
            && let Some(s) = prefix(last, 2)
        {
            first_plus_last.push_str(&s);
        }
        if first_plus_last.len() >= 2 {
            out.insert(first_plus_last);
        }
    }

    for w in &words {
        if w.len() >= 2 {
            out.insert(w.clone());
        }
        if let Some(p3) = prefix(w, 3) {
            out.insert(p3);
        }
    }

    out
}

fn aliases_intersect(a: &HashSet<String>, b: &HashSet<String>) -> bool {
    a.iter().any(|x| b.contains(x))
}

fn canonical_words(name: &str) -> Vec<String> {
    let mut cleaned = String::with_capacity(name.len());
    for ch in name.chars() {
        if ch.is_ascii_alphanumeric() {
            cleaned.push(ch.to_ascii_lowercase());
        } else {
            cleaned.push(' ');
        }
    }
    cleaned
        .split_whitespace()
        .filter_map(|w| {
            let w = normalize_word(w);
            if w.is_empty() {
                return None;
            }
            if matches!(w.as_str(), "fc" | "cf" | "afc" | "sc" | "ac" | "club") {
                return None;
            }
            Some(w)
        })
        .collect()
}

fn normalize_word(raw: &str) -> String {
    raw.chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .map(|c| c.to_ascii_lowercase())
        .collect()
}

fn prefix(raw: &str, n: usize) -> Option<String> {
    if raw.is_empty() {
        return None;
    }
    Some(raw.chars().take(n).collect())
}

fn with_cache_buster(url: &str) -> String {
    if url.contains("_=") {
        return url.to_string();
    }
    let ts = Utc::now().timestamp_millis();
    let separator = if url.contains('?') { '&' } else { '?' };
    format!("{url}{separator}_={ts}")
}

// ---------------------------------------------------------------------------
// OddsPortal provider
// ---------------------------------------------------------------------------

const OP_BASE_URL: &str = "https://www.oddsportal.com";
const OP_PASSPHRASE: &str = "J*8sQ!p$7aD_fR2yW@gHn*3bVp#sAdLd_k";
const OP_SALT: &str = "5b9a8f2c3e6d1a4b7c8e9d0f1a2b3c4d";
const OP_PBKDF2_ITERATIONS: u32 = 1000;

fn oddsportal_page_url_for_mode(mode: LeagueMode) -> Option<&'static str> {
    match mode {
        LeagueMode::PremierLeague => Some("/football/england/premier-league/"),
        LeagueMode::LaLiga => Some("/football/spain/laliga/"),
        LeagueMode::Bundesliga => Some("/football/germany/bundesliga/"),
        LeagueMode::SerieA => Some("/football/italy/serie-a/"),
        LeagueMode::Ligue1 => Some("/football/france/ligue-1/"),
        LeagueMode::ChampionsLeague => Some("/football/europe/champions-league/"),
        LeagueMode::WorldCup => Some("/football/world/world-cup/"),
    }
}

// -- Deserialization structs for the decrypted AJAX JSON --

#[derive(Debug, Deserialize)]
struct OPResponse {
    #[allow(dead_code)]
    s: Option<i32>,
    d: Option<OPData>,
}

#[derive(Debug, Deserialize)]
struct OPData {
    #[serde(rename = "oddsData")]
    odds_data: Option<HashMap<String, OPEventOdds>>,
}

#[derive(Debug, Deserialize)]
struct OPEventOdds {
    #[allow(dead_code)]
    event: Option<u64>,
    #[serde(default)]
    odds: Vec<OPOutcome>,
}

#[derive(Debug, Deserialize)]
struct OPOutcome {
    #[serde(rename = "avgOdds")]
    avg_odds: Option<f64>,
    #[serde(rename = "maxOdds")]
    #[allow(dead_code)]
    max_odds: Option<f64>,
    #[serde(rename = "cntActive")]
    cnt_active: Option<u32>,
}

/// Match metadata extracted from JSON-LD blocks in the HTML page.
#[derive(Debug, Clone)]
struct OPMatch {
    event_id: String,
    home: String,
    away: String,
    start_ts: Option<i64>,
}

// -- Helpers --

fn hex_decode(hex: &str) -> Result<Vec<u8>> {
    if hex.len() % 2 != 0 {
        return Err(anyhow::anyhow!("odd-length hex string"));
    }
    (0..hex.len())
        .step_by(2)
        .map(|i| {
            u8::from_str_radix(&hex[i..i + 2], 16)
                .map_err(|_| anyhow::anyhow!("invalid hex byte at offset {i}"))
        })
        .collect()
}

/// Decode common HTML entities produced by Vue SSR prop encoding.
fn html_decode(s: &str) -> String {
    s.replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
        .replace("&#x27;", "'")
        .replace("&#x2F;", "/")
}

/// Decrypt an OddsPortal AJAX response body.
///
/// The body is base64-encoded.  After decoding:  `<base64_ciphertext>:<hex_iv>`.
fn decrypt_oddsportal_response(body: &str) -> Result<String> {
    let decoded = BASE64
        .decode(body.trim().as_bytes())
        .context("outer base64 decode failed")?;
    let decoded_str = String::from_utf8(decoded).context("outer base64 not utf8")?;

    let (ct_b64, iv_hex) = decoded_str
        .rsplit_once(':')
        .context("expected ciphertext:iv format")?;

    let ciphertext = BASE64
        .decode(ct_b64.as_bytes())
        .context("ciphertext base64 decode failed")?;
    let iv = hex_decode(iv_hex).context("iv hex decode failed")?;

    if iv.len() != 16 {
        return Err(anyhow::anyhow!("IV must be 16 bytes, got {}", iv.len()));
    }

    // Derive key via PBKDF2(SHA-256)
    let mut key = [0u8; 32];
    pbkdf2_hmac::<Sha256>(
        OP_PASSPHRASE.as_bytes(),
        OP_SALT.as_bytes(),
        OP_PBKDF2_ITERATIONS,
        &mut key,
    );

    // AES-256-CBC decrypt with PKCS7 padding
    let mut buf = ciphertext.clone();
    let iv_arr: [u8; 16] = iv.try_into().unwrap();
    let decryptor = Aes256CbcDec::new(&key.into(), &iv_arr.into());
    let plaintext = decryptor
        .decrypt_padded_mut::<Pkcs7>(&mut buf)
        .map_err(|e| anyhow::anyhow!("AES decrypt failed: {e}"))?;

    String::from_utf8(plaintext.to_vec()).context("decrypted payload is not utf8")
}

/// Extract the AJAX odds URL from the HTML (`:odds-request="..."` Vue prop).
fn extract_odds_request_url(html: &str) -> Option<String> {
    // The prop looks like:  :odds-request="{ ... &quot;url&quot;:&quot;/ajax-...&quot; ... }"
    let marker = ":odds-request=\"";
    if let Some(start) = html.find(marker) {
        let start = start + marker.len();
        let end = html[start..].find('"')? + start;
        let raw = &html[start..end];
        let decoded = html_decode(raw);

        // Parse as JSON to extract "url" field
        let obj: serde_json::Value = serde_json::from_str(&decoded).ok()?;
        let url = obj.get("url")?.as_str()?;
        return Some(url.to_string());
    }

    // Newer OddsPortal pages embed this inside encoded sport-data blobs.
    for marker in [
        "&quot;oddsRequest&quot;:{&quot;url&quot;:&quot;",
        "\"oddsRequest\":{\"url\":\"",
    ] {
        if let Some(start) = html.find(marker) {
            let start = start + marker.len();
            let end_marker = if marker.starts_with("&quot;") {
                "&quot;"
            } else {
                "\""
            };
            let end = html[start..].find(end_marker)? + start;
            let raw = &html[start..end];
            let decoded = html_decode(raw).replace("\\/", "/");
            return Some(decoded);
        }
    }

    None
}

/// Parse JSON-LD `<script>` blocks from the page HTML to extract match metadata.
fn extract_jsonld_matches(html: &str) -> Vec<OPMatch> {
    let mut matches = Vec::new();
    let tag = "<script type=\"application/ld+json\">";
    let mut search_from = 0;

    while let Some(start) = html[search_from..].find(tag) {
        let json_start = search_from + start + tag.len();
        let Some(end_offset) = html[json_start..].find("</script>") else {
            break;
        };
        let json_str = &html[json_start..json_start + end_offset];
        search_from = json_start + end_offset;

        let Ok(val) = serde_json::from_str::<serde_json::Value>(json_str) else {
            continue;
        };

        // Support either "@type": "SportsEvent" or "@type": ["Event","SportsEvent"].
        let has_sports_event = match val.get("@type") {
            Some(serde_json::Value::String(kind)) => kind == "SportsEvent",
            Some(serde_json::Value::Array(kinds)) => kinds
                .iter()
                .filter_map(|k| k.as_str())
                .any(|k| k == "SportsEvent"),
            _ => false,
        };
        if !has_sports_event {
            continue;
        }

        let name = match val.get("name").and_then(|n| n.as_str()) {
            Some(n) => n,
            None => continue,
        };

        // Name format: "Home - Away"
        let parts: Vec<&str> = name.splitn(2, " - ").collect();
        if parts.len() != 2 {
            continue;
        }
        let home = parts[0].trim().to_string();
        let away = parts[1].trim().to_string();

        // Extract event_id from url: last segment of the slug, e.g. "brentford-arsenal-lKNJm8ak"
        let event_id = val
            .get("url")
            .and_then(|u| u.as_str())
            .and_then(|u| u.trim_end_matches('/').rsplit('/').next())
            .and_then(|slug| slug.rsplit('-').next())
            .unwrap_or("")
            .to_string();

        if event_id.is_empty() {
            continue;
        }

        let start_ts = val
            .get("startDate")
            .and_then(|d| d.as_str())
            .and_then(parse_timestamp);

        matches.push(OPMatch {
            event_id,
            home,
            away,
            start_ts,
        });
    }

    matches
}

/// Convert an OddsPortal event (with avgOdds array) into an OddsEventCandidate.
fn op_event_to_candidate(op_match: &OPMatch, odds: &OPEventOdds) -> Option<OddsEventCandidate> {
    // OddsPortal provides exactly 3 outcomes in order: Home(0), Draw(1), Away(2)
    if odds.odds.len() < 3 {
        return None;
    }

    let home_dec = odds.odds[0].avg_odds?;
    let draw_dec = odds.odds[1].avg_odds?;
    let away_dec = odds.odds[2].avg_odds?;

    let (home_prob, draw_prob, away_prob, h_d, d_d, a_d) =
        no_vig_from_decimal(home_dec, draw_dec, away_dec)?;

    let bookmakers_used = odds.odds[0].cnt_active.unwrap_or(1).min(255) as u8;
    let fetched_at_unix = Utc::now().timestamp();

    Some(OddsEventCandidate {
        kickoff_ts: op_match.start_ts,
        home_aliases: team_aliases(&op_match.home),
        away_aliases: team_aliases(&op_match.away),
        snapshot: MarketOddsSnapshot {
            source: "oddsportal".to_string(),
            fetched_at_unix,
            bookmakers_used,
            home_decimal: Some(h_d),
            draw_decimal: Some(d_d),
            away_decimal: Some(a_d),
            implied_home: Some((home_prob * 100.0) as f32),
            implied_draw: Some((draw_prob * 100.0) as f32),
            implied_away: Some((away_prob * 100.0) as f32),
            stale: false,
        },
    })
}

/// Fetch odds from OddsPortal for the given league mode and match them to fixtures.
fn fetch_oddsportal_for_fixtures(
    fixtures: &[&OddsFixtureRef],
    mode: LeagueMode,
    time_tolerance_secs: i64,
) -> Result<HashMap<String, MarketOddsSnapshot>> {
    let page_path = oddsportal_page_url_for_mode(mode)
        .context("no OddsPortal URL mapping for this league mode")?;

    let page_url = format!("{OP_BASE_URL}{page_path}");
    let client = http_client()?;

    // 1. Fetch the league page HTML
    let html = client
        .get(&page_url)
        .header(USER_AGENT, "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        .header("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
        .header("Accept-Language", "en-US,en;q=0.5")
        .send()
        .context("OddsPortal page request failed")?
        .text()
        .context("failed reading OddsPortal page body")?;

    // 2. Extract match metadata from JSON-LD blocks
    let op_matches = extract_jsonld_matches(&html);
    if op_matches.is_empty() {
        return Ok(HashMap::new());
    }

    // Build a lookup from event_id -> OPMatch
    let match_by_id: HashMap<&str, &OPMatch> = op_matches
        .iter()
        .map(|m| (m.event_id.as_str(), m))
        .collect();

    // 3. Extract the AJAX odds URL from the HTML
    let ajax_path =
        extract_odds_request_url(&html).context("could not extract odds-request URL from page")?;

    let ajax_url = if ajax_path.starts_with("http") {
        ajax_path.clone()
    } else {
        format!("{OP_BASE_URL}{ajax_path}")
    };

    // Append timestamp parameter only when the page URL doesn't already include one.
    let ajax_url_with_ts = with_cache_buster(&ajax_url);

    // 4. Fetch and decrypt the AJAX response
    let ajax_body = client
        .get(&ajax_url_with_ts)
        .header(USER_AGENT, "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        .header("Accept", "application/json, text/plain, */*")
        .header("Referer", &page_url)
        .header("X-Requested-With", "XMLHttpRequest")
        .send()
        .context("OddsPortal AJAX request failed")?
        .text()
        .context("failed reading OddsPortal AJAX body")?;

    let decrypted_json =
        decrypt_oddsportal_response(&ajax_body).context("OddsPortal decrypt failed")?;

    let response: OPResponse =
        serde_json::from_str(&decrypted_json).context("invalid OddsPortal odds JSON")?;

    let odds_data = response.d.and_then(|d| d.odds_data).unwrap_or_default();

    // 5. Build candidates from the decrypted odds + JSON-LD match metadata
    let mut candidates: Vec<OddsEventCandidate> = Vec::new();
    for (event_id, event_odds) in &odds_data {
        if let Some(op_match) = match_by_id.get(event_id.as_str()) {
            if let Some(candidate) = op_event_to_candidate(op_match, event_odds) {
                candidates.push(candidate);
            }
        }
    }

    // 6. Match candidates to fixtures using the shared matching logic
    Ok(match_candidates_to_fixtures(
        fixtures,
        &candidates,
        time_tolerance_secs,
    ))
}

#[cfg(test)]
mod tests {
    use super::{
        aliases_intersect, extract_jsonld_matches, extract_odds_request_url, no_vig_from_decimal,
        team_aliases, with_cache_buster,
    };

    #[test]
    fn no_vig_probs_sum_to_one() {
        let (h, d, a, ..) = no_vig_from_decimal(2.10, 3.40, 3.60).expect("valid");
        let sum = h + d + a;
        assert!((sum - 1.0).abs() < 1e-9);
    }

    #[test]
    fn aliases_match_abbreviation_to_full_name() {
        let a = team_aliases("MCI");
        let b = team_aliases("Manchester City");
        assert!(aliases_intersect(&a, &b));
    }

    #[test]
    fn extract_odds_request_url_supports_legacy_prop() {
        let html = r#"<div :odds-request="{&quot;url&quot;:&quot;/ajax-legacy/&quot;}"></div>"#;
        let url = extract_odds_request_url(html).expect("url");
        assert_eq!(url, "/ajax-legacy/");
    }

    #[test]
    fn extract_odds_request_url_supports_encoded_odds_request_blob() {
        let html = r#"<div data="{&quot;oddsRequest&quot;:{&quot;url&quot;:&quot;\/ajax-new\/path\/&quot;,&quot;refresh&quot;:30000}}"></div>"#;
        let url = extract_odds_request_url(html).expect("url");
        assert_eq!(url, "/ajax-new/path/");
    }

    #[test]
    fn extract_jsonld_matches_accepts_array_type() {
        let html = r#"
<script type="application/ld+json">
{
  "@type": ["Event", "SportsEvent"],
  "name": "Home Team - Away Team",
  "startDate": "2026-02-12T20:00:00Z",
  "url": "/football/test/home-away-AbCd1234/"
}
</script>
"#;
        let matches = extract_jsonld_matches(html);
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].event_id, "AbCd1234");
        assert_eq!(matches[0].home, "Home Team");
        assert_eq!(matches[0].away, "Away Team");
    }

    #[test]
    fn with_cache_buster_keeps_existing_param() {
        let input = "https://example.com/ajax?x=1&_=123";
        let out = with_cache_buster(input);
        assert_eq!(out, input);
    }

    #[test]
    fn with_cache_buster_appends_when_missing() {
        let input = "https://example.com/ajax?x=1";
        let out = with_cache_buster(input);
        assert!(out.starts_with("https://example.com/ajax?x=1&_="));
    }
}
