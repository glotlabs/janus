use axum::{
    Json,
    body::{Body, Bytes},
    extract::{Path as AxumPath, State},
    http::StatusCode,
};
use serde_json::Value;
use tracing::{info, warn};

use crate::auth::{Authorized, JobsRead, JobsRun, LogsRead};

use super::{
    JobCreatedResponse, JobDefinitionResponse, JobError, JobLogsResponse, JobStatusResponse,
};

pub async fn create_job(
    _: Authorized<JobsRun>,
    State(state): State<crate::AppState>,
    AxumPath(name): AxumPath<String>,
    body: Body,
) -> Result<(StatusCode, Json<JobCreatedResponse>), JobError> {
    info!(job_name = %name, "job run requested");
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
    let body: Value =
        serde_json::from_slice(&bytes).map_err(|source| JobError::ParseRequestBody { source })?;
    let params = body
        .as_object()
        .cloned()
        .ok_or(JobError::InvalidBody("expected a JSON object"))?;
    let created = state.jobs.create_job(
        &name,
        params,
        &state.manifests,
        &state.artifacts,
        state.config.jobs.default_log_limit_mb,
        state.config.jobs.cleanup_successful_workdirs,
        state.config.jobs.keep_failed_workdirs,
    )?;
    info!(job_id = %created.job_id, job_name = %name, "job created");

    Ok((StatusCode::CREATED, Json(created)))
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
