use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const HEADER_IDEMPOTENCY_KEY: &str = "x-idempotency-key";
pub const HEADER_SHA256: &str = "x-sha256";
pub const RUNNER_PROTOCOL_VERSION: u32 = 1;
pub const SUPPORTED_RUNNER_PROTOCOL_VERSIONS: &[u32] = &[RUNNER_PROTOCOL_VERSION];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunnerRouteTemplate {
    Capabilities,
    Jobs,
    Artifacts,
    Artifact,
    JobRuns,
    Run,
    RunLogs,
}

impl RunnerRouteTemplate {
    pub fn path(self) -> &'static str {
        match self {
            Self::Capabilities => "/capabilities",
            Self::Jobs => "/jobs",
            Self::Artifacts => "/artifacts",
            Self::Artifact => "/artifacts/{artifact_id}",
            Self::JobRuns => "/jobs/{name}/runs",
            Self::Run => "/runs/{job_id}",
            Self::RunLogs => "/runs/{job_id}/logs",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunnerRoute<'a> {
    Capabilities,
    Jobs,
    Artifacts,
    Artifact { artifact_id: &'a str },
    JobRuns { job_name: &'a str },
    Run { job_id: &'a str },
    RunLogs { job_id: &'a str },
}

impl RunnerRoute<'_> {
    pub fn path(self) -> String {
        match self {
            Self::Capabilities => RunnerRouteTemplate::Capabilities.path().to_string(),
            Self::Jobs => RunnerRouteTemplate::Jobs.path().to_string(),
            Self::Artifacts => RunnerRouteTemplate::Artifacts.path().to_string(),
            Self::Artifact { artifact_id } => format!("/artifacts/{artifact_id}"),
            Self::JobRuns { job_name } => format!("/jobs/{job_name}/runs"),
            Self::Run { job_id } => format!("/runs/{job_id}"),
            Self::RunLogs { job_id } => format!("/runs/{job_id}/logs"),
        }
    }

    pub fn template(self) -> RunnerRouteTemplate {
        match self {
            Self::Capabilities => RunnerRouteTemplate::Capabilities,
            Self::Jobs => RunnerRouteTemplate::Jobs,
            Self::Artifacts => RunnerRouteTemplate::Artifacts,
            Self::Artifact { .. } => RunnerRouteTemplate::Artifact,
            Self::JobRuns { .. } => RunnerRouteTemplate::JobRuns,
            Self::Run { .. } => RunnerRouteTemplate::Run,
            Self::RunLogs { .. } => RunnerRouteTemplate::RunLogs,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RunnerCapabilitiesResponse {
    pub protocol_version: u32,
    pub supported_protocol_versions: Vec<u32>,
}

impl RunnerCapabilitiesResponse {
    pub fn current() -> Self {
        Self {
            protocol_version: RUNNER_PROTOCOL_VERSION,
            supported_protocol_versions: SUPPORTED_RUNNER_PROTOCOL_VERSIONS.to_vec(),
        }
    }

    pub fn is_compatible_with_supported_versions(&self, supported_versions: &[u32]) -> bool {
        self.supported_protocol_versions
            .iter()
            .any(|version| supported_versions.contains(version))
    }
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
        assert_eq!(RunnerRoute::Capabilities.path(), "/capabilities");
        assert_eq!(RunnerRoute::Jobs.path(), "/jobs");
        assert_eq!(RunnerRoute::Artifacts.path(), "/artifacts");
        assert_eq!(
            RunnerRoute::JobRuns {
                job_name: "build-app"
            }
            .path(),
            "/jobs/build-app/runs"
        );
        assert_eq!(
            RunnerRoute::Run { job_id: "job_123" }.path(),
            "/runs/job_123"
        );
        assert_eq!(
            RunnerRoute::RunLogs { job_id: "job_123" }.path(),
            "/runs/job_123/logs"
        );
        assert_eq!(
            RunnerRoute::Artifact {
                artifact_id: "art_123"
            }
            .path(),
            "/artifacts/art_123"
        );
    }

    #[test]
    fn runner_route_templates_match_axum_paths() {
        assert_eq!(RunnerRouteTemplate::Capabilities.path(), "/capabilities");
        assert_eq!(RunnerRouteTemplate::Jobs.path(), "/jobs");
        assert_eq!(RunnerRouteTemplate::Artifacts.path(), "/artifacts");
        assert_eq!(
            RunnerRouteTemplate::Artifact.path(),
            "/artifacts/{artifact_id}"
        );
        assert_eq!(RunnerRouteTemplate::JobRuns.path(), "/jobs/{name}/runs");
        assert_eq!(RunnerRouteTemplate::Run.path(), "/runs/{job_id}");
        assert_eq!(RunnerRouteTemplate::RunLogs.path(), "/runs/{job_id}/logs");
        assert_eq!(
            RunnerRoute::RunLogs { job_id: "job_123" }.template(),
            RunnerRouteTemplate::RunLogs
        );
    }

    #[test]
    fn current_capabilities_advertise_supported_protocol_version() {
        let capabilities = RunnerCapabilitiesResponse::current();

        assert_eq!(capabilities.protocol_version, RUNNER_PROTOCOL_VERSION);
        assert!(
            capabilities.is_compatible_with_supported_versions(SUPPORTED_RUNNER_PROTOCOL_VERSIONS)
        );
        assert!(!capabilities.is_compatible_with_supported_versions(&[999]));
    }
}
