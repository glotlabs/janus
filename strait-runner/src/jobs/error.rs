use std::fmt;

use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::Serialize;

use crate::artifacts::ArtifactError;

#[derive(Debug)]
pub enum JobError {
    CreateDir {
        path: String,
        source: std::io::Error,
    },
    ReadFile {
        path: String,
        source: std::io::Error,
    },
    WriteFile {
        path: String,
        source: std::io::Error,
    },
    ParseMetadata {
        path: String,
        source: serde_json::Error,
    },
    SerializeMetadata {
        source: serde_json::Error,
    },
    SpawnProcess {
        script: String,
        source: std::io::Error,
    },
    WaitProcess {
        script: String,
        source: std::io::Error,
    },
    JobNotFound(String),
    JobNotRunning(String),
    UnknownJob(String),
    MissingParam(String),
    UnknownParam(String),
    InvalidParamType {
        name: String,
        expected: &'static str,
    },
    InvalidLogLimit {
        max_size_mb: u64,
    },
    ShuttingDown,
    Artifact(ArtifactError),
    ExpiredArtifact {
        name: String,
        artifact_id: String,
    },
    MissingOutput {
        name: String,
        path: String,
    },
    ConcurrencyConflict {
        reason: String,
    },
    InvalidBody(&'static str),
}

impl fmt::Display for JobError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CreateDir { path, source } => {
                write!(f, "failed to create job directory {path}: {source}")
            }
            Self::ReadFile { path, source } => {
                write!(f, "failed to read job file {path}: {source}")
            }
            Self::WriteFile { path, source } => {
                write!(f, "failed to write job file {path}: {source}")
            }
            Self::ParseMetadata { path, source } => {
                write!(f, "failed to parse job metadata {path}: {source}")
            }
            Self::SerializeMetadata { source } => {
                write!(f, "failed to serialize job metadata: {source}")
            }
            Self::SpawnProcess { script, source } => {
                write!(f, "failed to spawn job script {script}: {source}")
            }
            Self::WaitProcess { script, source } => {
                write!(f, "failed while waiting for job script {script}: {source}")
            }
            Self::JobNotFound(job_id) => write!(f, "job not found: {job_id}"),
            Self::JobNotRunning(job_id) => write!(f, "job is not running: {job_id}"),
            Self::UnknownJob(name) => write!(f, "job not found: {name}"),
            Self::MissingParam(name) => write!(f, "missing required param: {name}"),
            Self::UnknownParam(name) => write!(f, "unknown param: {name}"),
            Self::InvalidParamType { name, expected } => {
                write!(f, "invalid param type for {name}: expected {expected}")
            }
            Self::InvalidLogLimit { max_size_mb } => {
                write!(f, "invalid job log limit in mb: {max_size_mb}")
            }
            Self::ShuttingDown => write!(f, "runner is shutting down and not accepting new jobs"),
            Self::Artifact(source) => write!(f, "{source}"),
            Self::ExpiredArtifact { name, artifact_id } => {
                write!(
                    f,
                    "artifact param {name} references expired artifact {artifact_id}"
                )
            }
            Self::MissingOutput { name, path } => {
                write!(f, "required output {name} is missing at {path}")
            }
            Self::ConcurrencyConflict { reason } => write!(f, "{reason}"),
            Self::InvalidBody(message) => write!(f, "invalid job request body: {message}"),
        }
    }
}

impl std::error::Error for JobError {}

impl From<ArtifactError> for JobError {
    fn from(value: ArtifactError) -> Self {
        Self::Artifact(value)
    }
}

impl IntoResponse for JobError {
    fn into_response(self) -> Response {
        let status = match self {
            Self::UnknownJob(_) | Self::JobNotFound(_) => StatusCode::NOT_FOUND,
            Self::JobNotRunning(_) => StatusCode::CONFLICT,
            Self::ShuttingDown => StatusCode::SERVICE_UNAVAILABLE,
            Self::MissingParam(_)
            | Self::UnknownParam(_)
            | Self::InvalidParamType { .. }
            | Self::InvalidLogLimit { .. }
            | Self::Artifact(ArtifactError::NotFound(_))
            | Self::Artifact(ArtifactError::MissingChecksum)
            | Self::Artifact(ArtifactError::ChecksumMismatch { .. })
            | Self::Artifact(ArtifactError::TooLarge { .. })
            | Self::Artifact(ArtifactError::InvalidHeader { .. })
            | Self::Artifact(ArtifactError::ParseExpiry { .. })
            | Self::MissingOutput { .. }
            | Self::ExpiredArtifact { .. }
            | Self::InvalidBody(_) => StatusCode::BAD_REQUEST,
            Self::ConcurrencyConflict { .. } => StatusCode::CONFLICT,
            Self::Artifact(ArtifactError::ParseMetadata { .. })
            | Self::Artifact(ArtifactError::CreateDir { .. })
            | Self::Artifact(ArtifactError::ReadDir { .. })
            | Self::Artifact(ArtifactError::ReadFile { .. })
            | Self::Artifact(ArtifactError::RemoveDir { .. })
            | Self::Artifact(ArtifactError::WriteFile { .. })
            | Self::Artifact(ArtifactError::SerializeMetadata { .. })
            | Self::Artifact(ArtifactError::InvalidMaxSize { .. })
            | Self::Artifact(ArtifactError::InvalidBody(_))
            | Self::Artifact(ArtifactError::TimeOverflow)
            | Self::CreateDir { .. }
            | Self::ReadFile { .. }
            | Self::WriteFile { .. }
            | Self::ParseMetadata { .. }
            | Self::SpawnProcess { .. }
            | Self::WaitProcess { .. }
            | Self::SerializeMetadata { .. } => StatusCode::INTERNAL_SERVER_ERROR,
        };

        (status, Json(JobErrorResponse::from_job_error(&self))).into_response()
    }
}

#[derive(Debug, Serialize)]
struct JobErrorResponse {
    code: &'static str,
    message: String,
}

impl JobErrorResponse {
    fn from_job_error(error: &JobError) -> Self {
        let code = match error {
            JobError::CreateDir { .. } => "job_create_dir_failed",
            JobError::ReadFile { .. } => "job_read_failed",
            JobError::WriteFile { .. } => "job_write_failed",
            JobError::ParseMetadata { .. } => "job_metadata_parse_failed",
            JobError::SerializeMetadata { .. } => "job_metadata_serialize_failed",
            JobError::SpawnProcess { .. } => "job_spawn_failed",
            JobError::WaitProcess { .. } => "job_wait_failed",
            JobError::JobNotFound(_) => "job_not_found",
            JobError::JobNotRunning(_) => "job_not_running",
            JobError::UnknownJob(_) => "job_name_not_found",
            JobError::MissingParam(_) => "job_missing_param",
            JobError::UnknownParam(_) => "job_unknown_param",
            JobError::InvalidParamType { .. } => "job_invalid_param_type",
            JobError::InvalidLogLimit { .. } => "job_invalid_log_limit",
            JobError::ShuttingDown => "job_runner_shutting_down",
            JobError::Artifact(_) => "artifact_error",
            JobError::ExpiredArtifact { .. } => "artifact_expired",
            JobError::MissingOutput { .. } => "job_missing_output",
            JobError::ConcurrencyConflict { .. } => "job_concurrency_conflict",
            JobError::InvalidBody(_) => "job_invalid_body",
        };

        Self {
            code,
            message: error.to_string(),
        }
    }
}
