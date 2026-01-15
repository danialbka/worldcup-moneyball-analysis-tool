use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use reqwest::blocking::Client;
use reqwest::header::{ETAG, IF_MODIFIED_SINCE, IF_NONE_MATCH, LAST_MODIFIED, USER_AGENT};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};

const CACHE_VERSION: u32 = 1;
const CACHE_DIR: &str = "wc26_terminal";
const CACHE_FILE: &str = "http_cache.json";

static CACHE: Mutex<Option<HttpCacheFile>> = Mutex::new(None);

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

pub fn fetch_json_cached(
    client: &Client,
    url: &str,
    extra_headers: &[(&str, &str)],
) -> Result<String> {
    let cached_entry = {
        let mut guard = CACHE.lock().expect("http cache lock poisoned");
        let cache = guard.get_or_insert_with(load_cache_file);
        cache.entries.get(url).cloned()
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
    let cache = guard.get_or_insert_with(load_cache_file);
    cache.version = CACHE_VERSION;
    cache.entries.insert(key.to_string(), entry);
    let _ = save_cache_file(cache);
}

fn load_cache_file() -> HttpCacheFile {
    let Some(path) = cache_path() else {
        return HttpCacheFile::default();
    };
    let raw = fs::read_to_string(path).ok();
    let Some(raw) = raw else {
        return HttpCacheFile::default();
    };
    let cache = serde_json::from_str::<HttpCacheFile>(&raw).unwrap_or_default();
    if cache.version != CACHE_VERSION {
        return HttpCacheFile::default();
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
    if let Ok(base) = std::env::var("XDG_CACHE_HOME") {
        if !base.trim().is_empty() {
            return Some(PathBuf::from(base).join(CACHE_DIR).join(CACHE_FILE));
        }
    }
    let home = std::env::var("HOME").ok()?;
    if home.trim().is_empty() {
        return None;
    }
    Some(PathBuf::from(home).join(".cache").join(CACHE_DIR).join(CACHE_FILE))
}

fn system_time_to_secs(time: SystemTime) -> Option<u64> {
    time.duration_since(UNIX_EPOCH).ok().map(|d| d.as_secs())
}
