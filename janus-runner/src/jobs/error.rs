use std::fmt;

use axum::{
    Json,
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};
use serde::Serialize;
use tracing::warn;

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
    MissingInput(String),
    UnknownInput(String),
    InvalidInputType {
        name: String,
        expected: &'static str,
    },
    InvalidInputValue {
        name: String,
        reason: String,
    },
    InvalidLogLimit {
        max_size_mb: u64,
    },
    InvalidRequestBodyLimit {
        max_size_kb: u64,
    },
    RateLimitExceeded {
        token_name: String,
        limit: u32,
        window_seconds: u64,
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
    RequestTooLarge {
        max_bytes: usize,
    },
    ParseRequestBody {
        source: serde_json::Error,
    },
    InvalidJobId(String),
    MissingIdempotencyKey,
    InvalidIdempotencyKey(String),
    IdempotencyConflict {
        key: String,
    },
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
            Self::MissingInput(name) => write!(f, "missing required input: {name}"),
            Self::UnknownInput(name) => write!(f, "unknown input: {name}"),
            Self::InvalidInputType { name, expected } => {
                write!(f, "invalid input type for {name}: expected {expected}")
            }
            Self::InvalidInputValue { name, reason } => {
                write!(f, "invalid input value for {name}: {reason}")
            }
            Self::InvalidLogLimit { max_size_mb } => {
                write!(f, "invalid job log limit in mb: {max_size_mb}")
            }
            Self::InvalidRequestBodyLimit { max_size_kb } => {
                write!(f, "invalid job request body limit in kb: {max_size_kb}")
            }
            Self::RateLimitExceeded {
                token_name,
                limit,
                window_seconds,
            } => {
                write!(
                    f,
                    "token {token_name} exceeded job run rate limit of {limit} requests per {window_seconds} seconds"
                )
            }
            Self::ShuttingDown => write!(f, "runner is shutting down and not accepting new jobs"),
            Self::Artifact(source) => write!(f, "{source}"),
            Self::ExpiredArtifact { name, artifact_id } => {
                write!(
                    f,
                    "artifact input {name} references expired artifact {artifact_id}"
                )
            }
            Self::MissingOutput { name, path } => {
                write!(f, "required output {name} is missing at {path}")
            }
            Self::ConcurrencyConflict { reason } => write!(f, "{reason}"),
            Self::InvalidBody(message) => write!(f, "invalid job request body: {message}"),
            Self::RequestTooLarge { max_bytes } => {
                write!(
                    f,
                    "job request body exceeds configured limit of {max_bytes} bytes"
                )
            }
            Self::ParseRequestBody { source } => {
                write!(f, "failed to parse job request body as json: {source}")
            }
            Self::InvalidJobId(job_id) => write!(f, "invalid job id: {job_id}"),
            Self::MissingIdempotencyKey => write!(f, "missing idempotency key"),
            Self::InvalidIdempotencyKey(key) => write!(f, "invalid idempotency key: {key}"),
            Self::IdempotencyConflict { key } => {
                write!(
                    f,
                    "idempotency key {key} does not match the original request"
                )
            }
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
        let retry_after = match &self {
            Self::RateLimitExceeded {
                token_name,
                limit,
                window_seconds,
            } => {
                warn!(
                    token_name = %token_name,
                    limit,
                    window_seconds,
                    reason = "job_rate_limit_exceeded",
                    "request rejected"
                );
                Some(*window_seconds)
            }
            Self::Artifact(ArtifactError::RateLimitExceeded {
                token_name,
                limit,
                window_seconds,
            }) => {
                warn!(
                    token_name = %token_name,
                    limit,
                    window_seconds,
                    reason = "artifact_rate_limit_exceeded",
                    "request rejected"
                );
                Some(*window_seconds)
            }
            _ => None,
        };

        let status = match self {
            Self::UnknownJob(_) | Self::JobNotFound(_) => StatusCode::NOT_FOUND,
            Self::JobNotRunning(_) => StatusCode::CONFLICT,
            Self::ShuttingDown => StatusCode::SERVICE_UNAVAILABLE,
            Self::MissingInput(_)
            | Self::UnknownInput(_)
            | Self::InvalidInputType { .. }
            | Self::InvalidInputValue { .. }
            | Self::InvalidLogLimit { .. }
            | Self::InvalidRequestBodyLimit { .. }
            | Self::Artifact(ArtifactError::NotFound(_))
            | Self::Artifact(ArtifactError::MissingChecksum)
            | Self::Artifact(ArtifactError::ChecksumMismatch { .. })
            | Self::Artifact(ArtifactError::TooLarge { .. })
            | Self::Artifact(ArtifactError::InvalidArtifactId(_))
            | Self::Artifact(ArtifactError::InvalidHeader { .. })
            | Self::Artifact(ArtifactError::ParseExpiry { .. })
            | Self::MissingOutput { .. }
            | Self::ExpiredArtifact { .. }
            | Self::InvalidBody(_)
            | Self::ParseRequestBody { .. }
            | Self::InvalidJobId(_)
            | Self::MissingIdempotencyKey
            | Self::InvalidIdempotencyKey(_) => StatusCode::BAD_REQUEST,
            Self::Artifact(ArtifactError::RateLimitExceeded { .. }) => {
                StatusCode::TOO_MANY_REQUESTS
            }
            Self::RateLimitExceeded { .. } => StatusCode::TOO_MANY_REQUESTS,
            Self::RequestTooLarge { .. } => StatusCode::PAYLOAD_TOO_LARGE,
            Self::ConcurrencyConflict { .. } | Self::IdempotencyConflict { .. } => {
                StatusCode::CONFLICT
            }
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

        let mut response = (status, Json(JobErrorResponse::from_job_error(&self))).into_response();
        if let Some(retry_after) = retry_after {
            response.headers_mut().insert(
                header::RETRY_AFTER,
                header::HeaderValue::from_str(&retry_after.to_string())
                    .expect("retry-after header should be valid"),
            );
        }

        response
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
            JobError::MissingInput(_) => "job_missing_input",
            JobError::UnknownInput(_) => "job_unknown_input",
            JobError::InvalidInputType { .. } => "job_invalid_input_type",
            JobError::InvalidInputValue { .. } => "job_invalid_input_value",
            JobError::InvalidLogLimit { .. } => "job_invalid_log_limit",
            JobError::InvalidRequestBodyLimit { .. } => "job_invalid_request_body_limit",
            JobError::RateLimitExceeded { .. } => "job_rate_limit_exceeded",
            JobError::ShuttingDown => "job_runner_shutting_down",
            JobError::Artifact(_) => "artifact_error",
            JobError::ExpiredArtifact { .. } => "artifact_expired",
            JobError::MissingOutput { .. } => "job_missing_output",
            JobError::ConcurrencyConflict { .. } => "job_concurrency_conflict",
            JobError::InvalidBody(_) => "job_invalid_body",
            JobError::RequestTooLarge { .. } => "job_request_body_too_large",
            JobError::ParseRequestBody { .. } => "job_request_body_parse_failed",
            JobError::InvalidJobId(_) => "job_invalid_id",
            JobError::MissingIdempotencyKey => "job_missing_idempotency_key",
            JobError::InvalidIdempotencyKey(_) => "job_invalid_idempotency_key",
            JobError::IdempotencyConflict { .. } => "job_idempotency_conflict",
        };

        Self {
            code,
            message: error.to_string(),
        }
    }
}
