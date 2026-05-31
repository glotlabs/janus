use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const HEADER_IDEMPOTENCY_KEY: &str = "x-idempotency-key";
pub const HEADER_SHA256: &str = "x-sha256";

pub const ROUTE_RUNNER_JOBS: &str = "/jobs";
pub const ROUTE_RUNNER_ARTIFACTS: &str = "/artifacts";
pub const ROUTE_RUNNER_ARTIFACT_BY_ID: &str = "/artifacts/{artifact_id}";
pub const ROUTE_RUNNER_JOB_RUNS: &str = "/jobs/{name}/runs";
pub const ROUTE_RUNNER_RUN_BY_ID: &str = "/runs/{job_id}";
pub const ROUTE_RUNNER_RUN_LOGS: &str = "/runs/{job_id}/logs";

pub fn runner_job_run_path(job_name: &str) -> String {
    format!("/jobs/{job_name}/runs")
}

pub fn runner_run_path(job_id: &str) -> String {
    format!("/runs/{job_id}")
}

pub fn runner_run_logs_path(job_id: &str) -> String {
    format!("/runs/{job_id}/logs")
}

pub fn runner_artifact_path(artifact_id: &str) -> String {
    format!("/artifacts/{artifact_id}")
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArtifactUploadResponse {
    pub artifact_id: String,
    pub sha256: String,
    pub size: u64,
    pub expires_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JobCreatedResponse {
    pub job_id: String,
    pub status: JobStatus,
    pub started_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct JobStatusResponse {
    pub job_id: String,
    pub name: String,
    pub status: JobStatus,
    pub started_at: String,
    pub finished_at: Option<String>,
    #[serde(default)]
    pub duration_ms: Option<u64>,
    pub exit_code: Option<i32>,
    #[serde(default)]
    pub terminal_reason: Option<TerminalReason>,
    #[serde(default)]
    pub failure_category: Option<FailureCategory>,
    #[serde(default)]
    pub outputs: BTreeMap<String, JobOutput>,
    #[serde(default)]
    pub output_metadata: JobOutputMetadata,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JobLogsResponse {
    pub stdout: String,
    pub stderr: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum JobOutput {
    Artifact {
        artifact_id: String,
        sha256: String,
        size: u64,
    },
    String {
        value: String,
    },
    Integer {
        value: i64,
    },
    Boolean {
        value: bool,
    },
    Json {
        value: Value,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TerminalReason {
    Success,
    ExitCode,
    Timeout,
    Canceled,
    Shutdown,
    SpawnError,
    WaitError,
    CaptureError,
    LogLimitExceeded,
    MissingRequiredOutput,
    OutputRegistrationFailed,
}

impl TerminalReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::ExitCode => "exit_code",
            Self::Timeout => "timeout",
            Self::Canceled => "canceled",
            Self::Shutdown => "shutdown",
            Self::SpawnError => "spawn_error",
            Self::WaitError => "wait_error",
            Self::CaptureError => "capture_error",
            Self::LogLimitExceeded => "log_limit_exceeded",
            Self::MissingRequiredOutput => "missing_required_output",
            Self::OutputRegistrationFailed => "output_registration_failed",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FailureCategory {
    Job,
    Infra,
    Timeout,
    Canceled,
}

impl FailureCategory {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Job => "job",
            Self::Infra => "infra",
            Self::Timeout => "timeout",
            Self::Canceled => "canceled",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct JobOutputMetadata {
    #[serde(default)]
    pub stdout: JobStreamMetadata,
    #[serde(default)]
    pub stderr: JobStreamMetadata,
    #[serde(default)]
    pub artifacts: JobArtifactMetadata,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct JobStreamMetadata {
    pub bytes: u64,
    #[serde(default)]
    pub truncated: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct JobArtifactMetadata {
    pub count: u64,
    pub bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JobDefinitionResponse {
    pub name: String,
    #[serde(default)]
    pub concurrency: Concurrency,
    #[serde(default)]
    pub timeout_seconds: u64,
    #[serde(default)]
    pub inputs: BTreeMap<String, JobInputDefinitionResponse>,
    #[serde(default)]
    pub outputs: BTreeMap<String, JobOutputDefinitionResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JobInputDefinitionResponse {
    #[serde(rename = "type")]
    pub kind: InputType,
    pub required: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub sensitive: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_length: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pattern: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_json_bytes: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JobOutputDefinitionResponse {
    #[serde(rename = "type")]
    pub kind: OutputType,
    #[serde(default)]
    pub path: String,
    pub required: bool,
}

impl Default for JobDefinitionResponse {
    fn default() -> Self {
        Self {
            name: String::new(),
            concurrency: Concurrency::Parallel,
            timeout_seconds: 0,
            inputs: BTreeMap::new(),
            outputs: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum InputType {
    String,
    Integer,
    Boolean,
    Artifact,
    Json,
}

impl InputType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::String => "string",
            Self::Integer => "integer",
            Self::Boolean => "boolean",
            Self::Artifact => "artifact",
            Self::Json => "json",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "string" => Some(Self::String),
            "integer" => Some(Self::Integer),
            "boolean" => Some(Self::Boolean),
            "artifact" => Some(Self::Artifact),
            "json" => Some(Self::Json),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OutputType {
    Artifact,
    String,
    Integer,
    Boolean,
    Json,
}

impl OutputType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Artifact => "artifact",
            Self::String => "string",
            Self::Integer => "integer",
            Self::Boolean => "boolean",
            Self::Json => "json",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "artifact" => Some(Self::Artifact),
            "string" => Some(Self::String),
            "integer" => Some(Self::Integer),
            "boolean" => Some(Self::Boolean),
            "json" => Some(Self::Json),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Concurrency {
    Parallel,
    JobExclusive,
    GlobalExclusive,
}

impl Default for Concurrency {
    fn default() -> Self {
        Self::Parallel
    }
}

impl Concurrency {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Parallel => "parallel",
            Self::JobExclusive => "job_exclusive",
            Self::GlobalExclusive => "global_exclusive",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "parallel" => Some(Self::Parallel),
            "job_exclusive" => Some(Self::JobExclusive),
            "global_exclusive" => Some(Self::GlobalExclusive),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    Running,
    CancelRequested,
    Canceling,
    Success,
    Failed,
    TimedOut,
    Canceled,
    Rejected,
}

impl JobStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::CancelRequested => "cancel_requested",
            Self::Canceling => "canceling",
            Self::Success => "success",
            Self::Failed => "failed",
            Self::TimedOut => "timed_out",
            Self::Canceled => "canceled",
            Self::Rejected => "rejected",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "running" => Some(Self::Running),
            "cancel_requested" => Some(Self::CancelRequested),
            "canceling" => Some(Self::Canceling),
            "success" => Some(Self::Success),
            "failed" => Some(Self::Failed),
            "timed_out" => Some(Self::TimedOut),
            "canceled" => Some(Self::Canceled),
            "rejected" => Some(Self::Rejected),
            _ => None,
        }
    }

    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Success | Self::Failed | Self::TimedOut | Self::Canceled | Self::Rejected
        )
    }
}

fn is_false(value: &bool) -> bool {
    !*value
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serializes_protocol_enum_values_as_snake_case() {
        assert_eq!(
            serde_json::to_value(JobStatus::CancelRequested).expect("status"),
            "cancel_requested"
        );
        assert_eq!(
            serde_json::to_value(TerminalReason::MissingRequiredOutput).expect("reason"),
            "missing_required_output"
        );
        assert_eq!(
            serde_json::to_value(FailureCategory::Infra).expect("category"),
            "infra"
        );
        assert_eq!(
            serde_json::to_value(InputType::Artifact).expect("input type"),
            "artifact"
        );
        assert_eq!(
            serde_json::to_value(OutputType::Json).expect("output type"),
            "json"
        );
    }

    #[test]
    fn job_definition_defaults_collections() {
        let value: JobDefinitionResponse = serde_json::from_str(
            r#"{"name":"build","concurrency":"parallel","timeout_seconds":60}"#,
        )
        .expect("definition");

        assert!(value.inputs.is_empty());
        assert!(value.outputs.is_empty());
    }

    #[test]
    fn job_status_terminal_helper_matches_runner_contract() {
        assert!(!JobStatus::Running.is_terminal());
        assert!(!JobStatus::Canceling.is_terminal());
        assert!(JobStatus::Success.is_terminal());
        assert!(JobStatus::Failed.is_terminal());
        assert!(JobStatus::TimedOut.is_terminal());
        assert!(JobStatus::Canceled.is_terminal());
        assert!(JobStatus::Rejected.is_terminal());
    }

    #[test]
    fn runner_path_builders_match_route_templates() {
        assert_eq!(runner_job_run_path("build-app"), "/jobs/build-app/runs");
        assert_eq!(runner_run_path("job_123"), "/runs/job_123");
        assert_eq!(runner_run_logs_path("job_123"), "/runs/job_123/logs");
        assert_eq!(runner_artifact_path("art_123"), "/artifacts/art_123");
    }
}
