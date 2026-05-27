use std::{fs, path::Path};

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct Config {
    pub data_dir: String,
    pub manifests_dir: String,
    pub server: ServerConfig,
    pub auth: AuthConfig,
    pub artifacts: ArtifactsConfig,
    pub jobs: JobsConfig,
}

impl Config {
    pub fn load_from_path(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let raw = fs::read_to_string(path).map_err(|source| ConfigError::Read {
            path: path.display().to_string(),
            source,
        })?;

        toml::from_str(&raw).map_err(|source| ConfigError::Parse {
            path: path.display().to_string(),
            source,
        })
    }
}

#[derive(Debug)]
pub enum ConfigError {
    Read {
        path: String,
        source: std::io::Error,
    },
    Parse {
        path: String,
        source: toml::de::Error,
    },
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Read { path, source } => {
                write!(f, "failed to read config at {path}: {source}")
            }
            Self::Parse { path, source } => {
                write!(f, "failed to parse config at {path}: {source}")
            }
        }
    }
}

impl std::error::Error for ConfigError {}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct ServerConfig {
    pub listen: String,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct AuthConfig {
    pub mode: String,
    #[serde(default)]
    pub tokens: Vec<AuthTokenConfig>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct AuthTokenConfig {
    pub name: String,
    pub token_env: String,
    pub permissions: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct ArtifactsConfig {
    pub max_size_mb: u64,
    pub ttl_seconds: u64,
    pub cleanup_interval_seconds: u64,
    pub require_checksum_on_upload: bool,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct JobsConfig {
    pub default_log_limit_mb: u64,
    pub cleanup_successful_workdirs: bool,
    pub keep_failed_workdirs: bool,
}

#[cfg(test)]
mod tests {
    use super::Config;

    #[test]
    fn parses_runner_config() {
        let raw = r#"
data_dir = "/var/lib/strait-runner"
manifests_dir = "/etc/strait-runner/jobs"

[server]
listen = "127.0.0.1:8080"

[auth]
mode = "bearer"

[[auth.tokens]]
name = "git-orchestrator"
token_env = "STRAIT_RUNNER_TOKEN_GIT"
permissions = ["artifacts:write", "jobs:run"]

[artifacts]
max_size_mb = 500
ttl_seconds = 86400
cleanup_interval_seconds = 600
require_checksum_on_upload = true

[jobs]
default_log_limit_mb = 50
cleanup_successful_workdirs = true
keep_failed_workdirs = true
"#;

        let parsed: Config = toml::from_str(raw).expect("config should parse");

        assert_eq!(parsed.data_dir, "/var/lib/strait-runner");
        assert_eq!(parsed.server.listen, "127.0.0.1:8080");
        assert_eq!(parsed.auth.tokens.len(), 1);
        assert!(parsed.artifacts.require_checksum_on_upload);
        assert!(parsed.jobs.keep_failed_workdirs);
    }
}
