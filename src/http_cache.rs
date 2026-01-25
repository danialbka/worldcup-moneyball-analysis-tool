use std::collections::HashMap;
use std::env;
use std::fs;
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use reqwest::StatusCode;
use reqwest::blocking::Client;
use reqwest::header::{ETAG, IF_MODIFIED_SINCE, IF_NONE_MATCH, LAST_MODIFIED, USER_AGENT};
use serde::{Deserialize, Serialize};

const CACHE_VERSION: u32 = 1;
const CACHE_DIR: &str = "wc26_terminal";
const CACHE_FILE: &str = "http_cache.json";
const DEFAULT_CACHE_TTL_SECS: u64 = 7 * 24 * 60 * 60;
const DEFAULT_CACHE_MAX_BYTES: usize = 24 * 1024 * 1024;
const DEFAULT_CACHE_FLUSH_SECS: u64 = 20;

static CACHE: Mutex<Option<CacheState>> = Mutex::new(None);

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct HttpCacheFile {
    version: u32,
    entries: HashMap<String, CacheEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CacheEntry {
    body: String,
    etag: Option<String>,
    last_modified: Option<String>,
    fetched_at: u64,
}

struct CacheState {
    cache: HttpCacheFile,
    dirty: bool,
    last_saved: SystemTime,
}

pub fn fetch_json_cached(
    client: &Client,
    url: &str,
    extra_headers: &[(&str, &str)],
) -> Result<String> {
    let cached_entry = {
        let mut guard = CACHE.lock().expect("http cache lock poisoned");
        let state = guard.get_or_insert_with(load_cache_state);
        state.cache.entries.get(url).cloned()
    };

    let mut req = client.get(url).header(USER_AGENT, "Mozilla/5.0");
    for (name, value) in extra_headers {
        req = req.header(*name, *value);
    }
    if let Some(entry) = cached_entry.as_ref() {
        if let Some(etag) = entry.etag.as_ref() {
            req = req.header(IF_NONE_MATCH, etag);
        }
        if let Some(last_modified) = entry.last_modified.as_ref() {
            req = req.header(IF_MODIFIED_SINCE, last_modified);
        }
    }

    let resp = req.send().context("request failed")?;
    let status = resp.status();
    let headers = resp.headers().clone();
    if status == StatusCode::NOT_MODIFIED {
        if let Some(entry) = cached_entry {
            refresh_cache_entry(url, entry.clone());
            return Ok(entry.body);
        }
        return Err(anyhow::anyhow!("received 304 without cache body"));
    }

    let body = resp.text().context("failed reading body")?;
    if !status.is_success() {
        return Err(anyhow::anyhow!("http {}: {}", status, body));
    }

    let etag = headers
        .get(ETAG)
        .and_then(|v| v.to_str().ok())
        .map(|v| v.to_string());
    let last_modified = headers
        .get(LAST_MODIFIED)
        .and_then(|v| v.to_str().ok())
        .map(|v| v.to_string());

    let entry = CacheEntry {
        body: body.clone(),
        etag,
        last_modified,
        fetched_at: system_time_to_secs(SystemTime::now()).unwrap_or_default(),
    };
    refresh_cache_entry(url, entry);
    Ok(body)
}

fn refresh_cache_entry(key: &str, entry: CacheEntry) {
    let mut guard = CACHE.lock().expect("http cache lock poisoned");
    let state = guard.get_or_insert_with(load_cache_state);
    state.cache.version = CACHE_VERSION;
    state.cache.entries.insert(key.to_string(), entry);
    state.dirty = true;
    maybe_flush_cache(state);
}

pub fn flush_http_cache() {
    let mut guard = CACHE.lock().expect("http cache lock poisoned");
    let Some(state) = guard.as_mut() else {
        return;
    };
    if state.dirty {
        prune_cache(&mut state.cache);
        if save_cache_file(&state.cache).is_ok() {
            state.last_saved = SystemTime::now();
            state.dirty = false;
        }
    }
}

fn load_cache_state() -> CacheState {
    CacheState {
        cache: load_cache_file(),
        dirty: false,
        last_saved: SystemTime::now(),
    }
}

fn load_cache_file() -> HttpCacheFile {
    let Some(path) = cache_path() else {
        return HttpCacheFile::default();
    };
    let raw = fs::read_to_string(path).ok();
    let Some(raw) = raw else {
        return HttpCacheFile::default();
    };
    let mut cache = serde_json::from_str::<HttpCacheFile>(&raw).unwrap_or_default();
    if cache.version != CACHE_VERSION {
        return HttpCacheFile::default();
    }
    let pruned = prune_cache(&mut cache);
    if pruned {
        let _ = save_cache_file(&cache);
    }
    cache
}

fn save_cache_file(cache: &HttpCacheFile) -> Result<()> {
    let Some(path) = cache_path() else {
        return Ok(());
    };
    let Some(dir) = path.parent() else {
        return Ok(());
    };
    fs::create_dir_all(dir).ok();
    let tmp = path.with_extension("json.tmp");
    let json = serde_json::to_string(cache).context("serialize http cache")?;
    fs::write(&tmp, json).context("write http cache")?;
    fs::rename(&tmp, &path).context("swap http cache")?;
    Ok(())
}

fn cache_path() -> Option<PathBuf> {
    if let Ok(base) = env::var("XDG_CACHE_HOME") {
        if !base.trim().is_empty() {
            return Some(PathBuf::from(base).join(CACHE_DIR).join(CACHE_FILE));
        }
    }
    let home = env::var("HOME").ok()?;
    if home.trim().is_empty() {
        return None;
    }
    Some(
        PathBuf::from(home)
            .join(".cache")
            .join(CACHE_DIR)
            .join(CACHE_FILE),
    )
}

fn prune_cache(cache: &mut HttpCacheFile) -> bool {
    let mut pruned = false;
    let ttl_secs = cache_ttl_secs();
    if ttl_secs > 0 {
        let now = system_time_to_secs(SystemTime::now()).unwrap_or_default();
        let before = cache.entries.len();
        cache
            .entries
            .retain(|_, entry| now.saturating_sub(entry.fetched_at) <= ttl_secs);
        pruned |= cache.entries.len() != before;
    }

    let max_bytes = cache_max_bytes();
    if max_bytes > 0 {
        let mut entries: Vec<(String, u64, usize)> = cache
            .entries
            .iter()
            .map(|(key, entry)| (key.clone(), entry.fetched_at, approx_entry_size(key, entry)))
            .collect();
        let mut total_size: usize = entries.iter().map(|(_, _, size)| *size).sum();
        if total_size > max_bytes {
            entries.sort_by_key(|(_, fetched_at, _)| *fetched_at);
            for (key, _, size) in entries {
                if total_size <= max_bytes {
                    break;
                }
                if cache.entries.remove(&key).is_some() {
                    total_size = total_size.saturating_sub(size);
                    pruned = true;
                }
            }
        }
    }

    pruned
}

fn approx_entry_size(key: &str, entry: &CacheEntry) -> usize {
    let mut size = key.len() + entry.body.len() + 32;
    if let Some(etag) = entry.etag.as_ref() {
        size += etag.len();
    }
    if let Some(last_modified) = entry.last_modified.as_ref() {
        size += last_modified.len();
    }
    size
}

fn maybe_flush_cache(state: &mut CacheState) {
    if !state.dirty {
        return;
    }
    let flush_secs = cache_flush_secs();
    let should_flush = if flush_secs == 0 {
        true
    } else {
        state
            .last_saved
            .elapsed()
            .map(|elapsed| elapsed.as_secs() >= flush_secs)
            .unwrap_or(true)
    };
    if should_flush {
        prune_cache(&mut state.cache);
        if save_cache_file(&state.cache).is_ok() {
            state.last_saved = SystemTime::now();
            state.dirty = false;
        }
    }
}

fn cache_ttl_secs() -> u64 {
    env::var("HTTP_CACHE_TTL_SECS")
        .ok()
        .and_then(|val| val.parse::<u64>().ok())
        .unwrap_or(DEFAULT_CACHE_TTL_SECS)
}

fn cache_max_bytes() -> usize {
    env::var("HTTP_CACHE_MAX_BYTES")
        .ok()
        .and_then(|val| val.parse::<usize>().ok())
        .unwrap_or(DEFAULT_CACHE_MAX_BYTES)
}

fn cache_flush_secs() -> u64 {
    env::var("HTTP_CACHE_FLUSH_SECS")
        .ok()
        .and_then(|val| val.parse::<u64>().ok())
        .unwrap_or(DEFAULT_CACHE_FLUSH_SECS)
}

fn system_time_to_secs(time: SystemTime) -> Option<u64> {
    time.duration_since(UNIX_EPOCH).ok().map(|d| d.as_secs())
}
