use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::manifest::{Concurrency, JobManifest, InputType};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JobCreated {
    pub job_id: String,
    pub status: JobStatus,
    pub started_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JobCreatedResponse {
    pub job_id: String,
    pub status: JobStatus,
    pub started_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JobMetadata {
    pub job_id: String,
    pub name: String,
    pub idempotency_key: String,
    pub request_hash: String,
    pub status: JobStatus,
    pub started_at: String,
    pub finished_at: Option<String>,
    pub exit_code: Option<i32>,
    pub inputs: Map<String, Value>,
    pub resolved_artifacts: BTreeMap<String, String>,
    #[serde(default)]
    pub outputs: BTreeMap<String, JobOutputArtifact>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JobLogs {
    pub stdout: String,
    pub stderr: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JobLogsResponse {
    pub stdout: String,
    pub stderr: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JobOutputArtifact {
    pub artifact_id: String,
    pub sha256: String,
    pub size: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JobOutputResponse {
    pub artifact_id: String,
    pub sha256: String,
    pub size: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JobStatusResponse {
    pub job_id: String,
    pub name: String,
    pub status: JobStatus,
    pub started_at: String,
    pub finished_at: Option<String>,
    pub exit_code: Option<i32>,
    pub outputs: BTreeMap<String, JobOutputResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JobDefinitionResponse {
    pub name: String,
    pub concurrency: Concurrency,
    pub timeout_seconds: u64,
    pub inputs: BTreeMap<String, JobInputDefinitionResponse>,
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
    pub path: String,
    pub required: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    Running,
    Success,
    Failed,
    TimedOut,
    Canceled,
    Rejected,
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
            exit_code: value.exit_code,
            outputs: value
                .outputs
                .into_iter()
                .map(|(name, output)| {
                    (
                        name,
                        JobOutputResponse {
                            artifact_id: output.artifact_id,
                            sha256: output.sha256,
                            size: output.size,
                        },
                    )
                })
                .collect(),
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
                            path: output.path,
                            required: output.required,
                        },
                    )
                })
                .collect(),
        }
    }
}

fn is_false(value: &bool) -> bool {
    !*value
}
