use std::{fs, path::Path};

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub data_dir: String,
    pub repos_dir: String,
    pub database: DatabaseConfig,
    pub server: ServerConfig,
    pub auth: AuthConfig,
    pub scheduler: SchedulerConfig,
    pub runners: RunnersConfig,
}

impl Config {
    pub fn load_from_path(path: impl AsRef<Path>) -> Result<Self, Box<dyn std::error::Error>> {
        let raw = fs::read_to_string(path)?;
        Ok(toml::from_str(&raw)?)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct DatabaseConfig {
    pub path: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    pub listen: String,
    pub public_base_url: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AuthConfig {
    pub session_secret: String,
    pub bootstrap_admin: BootstrapAdminConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BootstrapAdminConfig {
    pub username: String,
    pub password: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SchedulerConfig {
    pub poll_interval_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RunnersConfig {
    pub healthcheck_interval_seconds: u64,
}
