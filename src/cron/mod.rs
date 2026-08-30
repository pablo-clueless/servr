use crate::config::Config;
use reqwest::Client;
use std::time::Duration;
use tokio::time::interval;
use tracing::{error, info};

pub async fn start_self_ping(endpoint: String) {
    info!("Self-ping background task started targeting {}", endpoint);

    let cfg = Config::from_env();
    let mut timer = interval(Duration::from_secs(cfg.self_ping_interval));
    let client = Client::new();

    loop {
        timer.tick().await;
        info!("Executing self-ping...");
        match client.get(&endpoint).send().await {
            Ok(resp) => {
                if resp.status().is_success() {
                    info!("Self-ping successful: {}", resp.status());
                } else {
                    error!("Self-ping returned non-success status: {}", resp.status());
                }
            }
            Err(e) => {
                error!("Self-ping request failed: {}", e);
            }
        }
    }
}
