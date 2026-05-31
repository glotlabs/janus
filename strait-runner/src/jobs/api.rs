use axum::{
    Json,
    body::{Body, Bytes},
    extract::{Path as AxumPath, State},
    http::{HeaderMap, StatusCode},
};
use serde_json::Value;
use std::time::Duration;
use strait_lib::HEADER_IDEMPOTENCY_KEY;
use tracing::{info, warn};

use crate::auth::{Authorized, JobsRead, JobsRun, LogsRead};

use super::{
    JobCreatedResponse, JobDefinitionResponse, JobError, JobLogsResponse, JobStatus,
    JobStatusResponse,
};

pub async fn create_job(
    auth: Authorized<JobsRun>,
    State(state): State<crate::AppState>,
    AxumPath(name): AxumPath<String>,
    headers: HeaderMap,
    body: Body,
) -> Result<(StatusCode, Json<JobCreatedResponse>), JobError> {
    info!(job_name = %name, "job run requested");
    state
        .rate_limiter
        .check(
            "jobs_run",
            auth.token_name(),
            state.config.jobs.max_run_requests_per_minute,
            Duration::from_secs(60),
        )
        .map_err(|error| JobError::RateLimitExceeded {
            token_name: auth.token_name().to_string(),
            limit: error.limit,
            window_seconds: error.window_seconds,
        })?;
    let max_bytes = state
        .config
        .jobs
        .max_request_body_kb
        .checked_mul(1024)
        .and_then(|value| usize::try_from(value).ok())
        .ok_or(JobError::InvalidRequestBodyLimit {
            max_size_kb: state.config.jobs.max_request_body_kb,
        })?;
    let bytes: Bytes = axum::body::to_bytes(body, max_bytes)
        .await
        .map_err(|_| JobError::RequestTooLarge { max_bytes })?;
    let idempotency_key = headers
        .get(HEADER_IDEMPOTENCY_KEY)
        .ok_or(JobError::MissingIdempotencyKey)?
        .to_str()
        .map_err(|_| JobError::InvalidIdempotencyKey("<invalid header>".to_string()))?
        .to_string();
    let body: Value =
        serde_json::from_slice(&bytes).map_err(|source| JobError::ParseRequestBody { source })?;
    let inputs = body
        .as_object()
        .cloned()
        .ok_or(JobError::InvalidBody("expected a JSON object"))?;
    let created = state.jobs.create_job(
        &name,
        &idempotency_key,
        &bytes,
        inputs,
        &state.manifests,
        &state.artifacts,
        state.config.jobs.default_log_limit_mb,
        state.config.jobs.cleanup_successful_workdirs,
        state.config.jobs.keep_failed_workdirs,
    )?;
    info!(
        job_id = %created.job_id,
        job_name = %name,
        idempotency_key = %idempotency_key,
        "job created"
    );

    let status = if created.status == JobStatus::Running {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };

    Ok((status, Json(created)))
}

pub async fn list_jobs(
    _: Authorized<JobsRead>,
    State(state): State<crate::AppState>,
) -> Json<Vec<JobDefinitionResponse>> {
    let jobs = state
        .manifests
        .all()
        .cloned()
        .map(JobDefinitionResponse::from)
        .collect();

    Json(jobs)
}

pub async fn get_job(
    _: Authorized<JobsRead>,
    State(state): State<crate::AppState>,
    AxumPath(job_id): AxumPath<String>,
) -> Result<Json<JobStatusResponse>, JobError> {
    info!(job_id = %job_id, "job status requested");
    Ok(Json(JobStatusResponse::from(state.jobs.read_job(&job_id)?)))
}

pub async fn get_job_logs(
    _: Authorized<LogsRead>,
    State(state): State<crate::AppState>,
    AxumPath(job_id): AxumPath<String>,
) -> Result<Json<JobLogsResponse>, JobError> {
    info!(job_id = %job_id, "job logs requested");
    Ok(Json(JobLogsResponse::from(state.jobs.read_logs(&job_id)?)))
}

pub async fn cancel_job(
    _: Authorized<JobsRun>,
    State(state): State<crate::AppState>,
    AxumPath(job_id): AxumPath<String>,
) -> Result<StatusCode, JobError> {
    warn!(job_id = %job_id, "job cancellation requested");
    state.jobs.cancel_job(&job_id)?;
    Ok(StatusCode::ACCEPTED)
}
