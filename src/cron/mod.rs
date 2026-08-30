use reqwest::Client;
use std::time::Duration;
use tokio::time::interval;
use tracing::{error, info};

pub async fn start_self_ping(endpoint: String) {
    info!("Self-ping background task started targeting {}", endpoint);
    let mut timer = interval(Duration::from_secs(600)); // 10 minutes
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
