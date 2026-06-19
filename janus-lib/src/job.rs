use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serializes_job_protocol_enum_values_as_snake_case() {
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
}
