use std::{fmt, fs, path::Path};

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub data_dir: String,
    pub repos_dir: String,
    pub database: DatabaseConfig,
    pub server: ServerConfig,
    #[serde(default)]
    pub control: ControlConfig,
    pub auth: AuthConfig,
    pub runner_auth: RunnerAuthConfig,
    pub scheduler: SchedulerConfig,
    pub runners: RunnersConfig,
    pub runner_url_policy: RunnerUrlPolicyConfig,
    pub limits: LimitsConfig,
}

impl Config {
    pub fn load_from_path(path: impl AsRef<Path>) -> Result<Self, Box<dyn std::error::Error>> {
        let path = path.as_ref();
        let raw = fs::read_to_string(path)?;
        toml::from_str(&raw).map_err(|source| {
            if source.message().contains("missing field `runner_auth`") {
                Box::<dyn std::error::Error>::from(MissingRunnerAuthConfig {
                    path: path.display().to_string(),
                })
            } else {
                Box::<dyn std::error::Error>::from(source)
            }
        })
    }
}

#[derive(Debug)]
struct MissingRunnerAuthConfig {
    path: String,
}

impl fmt::Display for MissingRunnerAuthConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "missing [runner_auth] in {}; run `admin runner-key init --config {}` first",
            self.path, self.path
        )
    }
}

impl std::error::Error for MissingRunnerAuthConfig {}

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
pub struct ControlConfig {
    #[serde(default = "default_control_socket_path")]
    pub socket_path: String,
    #[serde(default = "default_control_socket_mode")]
    pub socket_mode: u32,
}

impl Default for ControlConfig {
    fn default() -> Self {
        Self {
            socket_path: default_control_socket_path(),
            socket_mode: default_control_socket_mode(),
        }
    }
}

fn default_control_socket_path() -> String {
    crate::control::DEFAULT_SOCKET_PATH.to_string()
}

fn default_control_socket_mode() -> u32 {
    0o660
}

#[derive(Debug, Clone, Deserialize)]
pub struct AuthConfig {
    pub session_secret: String,
    pub session_ttl_days: u64,
    pub session_cookie_secure: bool,
    pub login_rate_limit_per_minute: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RunnerAuthConfig {
    pub key_id: String,
    pub private_key_path: String,
    pub public_key_path: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SchedulerConfig {
    pub poll_interval_ms: u64,
    pub cancel_stuck_timeout_seconds: u64,
    pub max_cancel_retries: u32,
    pub max_infra_retries: u32,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RunnersConfig {
    pub healthcheck_interval_seconds: u64,
    pub connect_timeout_seconds: u64,
    pub request_timeout_seconds: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RunnerUrlPolicyConfig {
    pub require_https: bool,
    pub allow_credentials: bool,
    pub allow_query: bool,
    pub allow_fragment: bool,
    pub allow_path: bool,
    pub allow_localhost: bool,
    pub allow_private_ips: bool,
    pub allow_link_local_ips: bool,
    pub allow_documentation_ips: bool,
    pub allow_multicast_ips: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LimitsConfig {
    pub request_body_bytes: usize,
    pub runner_json_bytes: usize,
    pub runner_logs_bytes: usize,
    pub runner_artifact_bytes: usize,
    pub runner_error_bytes: usize,
    pub server_artifact_bytes: usize,
}
