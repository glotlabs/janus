use std::collections::BTreeMap;

pub use janus_lib::{
    FailureCategory, JobArtifactMetadata, JobCreatedResponse, JobDefinitionResponse,
    JobInputDefinitionResponse, JobLogsResponse, JobOutput, JobOutputDefinitionResponse,
    JobOutputMetadata, JobStatus, JobStatusResponse, JobStreamMetadata, TerminalReason,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::manifest::JobManifest;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JobCreated {
    pub job_id: String,
    pub status: JobStatus,
    pub started_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct JobMetadata {
    pub job_id: String,
    pub name: String,
    pub idempotency_key: String,
    pub request_hash: String,
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
    pub inputs: Map<String, Value>,
    pub resolved_artifacts: BTreeMap<String, String>,
    #[serde(default)]
    pub outputs: BTreeMap<String, JobOutput>,
    #[serde(default)]
    pub output_metadata: JobOutputMetadata,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JobLogs {
    pub stdout: String,
    pub stderr: String,
}

pub(super) struct JobExecution {
    pub(super) artifacts: std::sync::Arc<crate::artifacts::ArtifactStore>,
    pub(super) manifest: JobManifest,
    pub(super) job_id: String,
    pub(super) metadata_path: std::path::PathBuf,
    pub(super) work_dir: std::path::PathBuf,
    pub(super) output_dir: std::path::PathBuf,
    pub(super) stdout_path: std::path::PathBuf,
    pub(super) stderr_path: std::path::PathBuf,
    pub(super) log_limit_bytes: u64,
    pub(super) cleanup_successful_workdirs: bool,
    pub(super) keep_failed_workdirs: bool,
    pub(super) metadata: JobMetadata,
    pub(super) raw_inputs: Map<String, Value>,
    pub(super) cancel_rx: tokio::sync::watch::Receiver<bool>,
}

pub(super) struct ExecutionOutcome {
    pub(super) status: JobStatus,
    pub(super) exit_code: Option<i32>,
    pub(super) message: Option<String>,
    pub(super) terminal_reason: TerminalReason,
    pub(super) failure_category: Option<FailureCategory>,
    pub(super) stdout_truncated: bool,
    pub(super) stderr_truncated: bool,
}

impl From<JobCreated> for JobCreatedResponse {
    fn from(value: JobCreated) -> Self {
        Self {
            job_id: value.job_id,
            status: value.status,
            started_at: value.started_at,
        }
    }
}

impl From<JobLogs> for JobLogsResponse {
    fn from(value: JobLogs) -> Self {
        Self {
            stdout: value.stdout,
            stderr: value.stderr,
        }
    }
}

impl From<JobMetadata> for JobStatusResponse {
    fn from(value: JobMetadata) -> Self {
        Self {
            job_id: value.job_id,
            name: value.name,
            status: value.status,
            started_at: value.started_at,
            finished_at: value.finished_at,
            duration_ms: value.duration_ms,
            exit_code: value.exit_code,
            terminal_reason: value.terminal_reason,
            failure_category: value.failure_category,
            outputs: value.outputs,
            output_metadata: value.output_metadata,
        }
    }
}

impl From<JobManifest> for JobDefinitionResponse {
    fn from(value: JobManifest) -> Self {
        Self {
            name: value.name,
            concurrency: value.concurrency,
            timeout_seconds: value.timeout_seconds,
            inputs: value
                .inputs
                .into_iter()
                .map(|(name, spec)| {
                    (
                        name,
                        JobInputDefinitionResponse {
                            kind: spec.kind,
                            required: spec.required,
                            sensitive: spec.sensitive,
                            max_length: spec.max_length,
                            pattern: spec.pattern,
                            max_json_bytes: spec.max_json_bytes,
                        },
                    )
                })
                .collect(),
            outputs: value
                .outputs
                .into_iter()
                .map(|(name, output)| {
                    (
                        name,
                        JobOutputDefinitionResponse {
                            kind: output.kind,
                            path: output.path,
                            required: output.required,
                        },
                    )
                })
                .collect(),
        }
    }
}
