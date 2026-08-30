use serde::Deserialize;
use std::env;

#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    pub database_url: String,
    pub server_port: u16,
    pub self_ping_interval: u64,
    pub self_ping_url: String,
    pub smtp_host: String,
    pub smtp_port: u16,
    pub smtp_user: Option<String>,
    pub smtp_pass: Option<String>,
}

impl Config {
    pub fn from_env() -> Self {
        dotenvy::dotenv().ok();
        Self {
            server_port: env::var("SERVER_PORT")
                .unwrap_or_else(|_| "8080".to_string())
                .parse()
                .unwrap_or(8080),
            self_ping_interval: env::var("SELF_PING_INTERVAL")
                .unwrap_or_else(|_| "30".to_string())
                .parse()
                .unwrap_or(300),
            self_ping_url: env::var("SELF_PING_URL")
                .unwrap_or_else(|_| "http://127.0.0.1:8080/ping".to_string()),
            smtp_host: env::var("SMTP_HOST").unwrap_or_else(|_| "smtp.example.com".to_string()),
            smtp_port: env::var("SMTP_PORT")
                .unwrap_or_else(|_| "587".to_string())
                .parse()
                .unwrap_or(587),
            smtp_user: env::var("SMTP_USER").ok(),
            smtp_pass: env::var("SMTP_PASS").ok(),
            database_url: env::var("DATABASE_URL")
                .unwrap_or_else(|_| "postgres://localhost/servr".to_string()),
        }
    }
}
