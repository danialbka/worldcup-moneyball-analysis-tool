use std::time::Duration;

use anyhow::{Context, Result};
use once_cell::sync::OnceCell;
use reqwest::blocking::Client;

const REQUEST_TIMEOUT_SECS: u64 = 10;

static CLIENT: OnceCell<Client> = OnceCell::new();

pub fn http_client() -> Result<&'static Client> {
    CLIENT.get_or_try_init(|| {
        Client::builder()
            .timeout(Duration::from_secs(REQUEST_TIMEOUT_SECS))
            .build()
            .context("failed to build http client")
    })
}
