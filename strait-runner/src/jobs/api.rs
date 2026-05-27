use axum::{
    Json,
    extract::{Path as AxumPath, State},
    http::StatusCode,
};
use serde_json::Value;

use crate::auth::{Authorized, JobsRead, JobsRun, LogsRead};

use super::{
    JobCreatedResponse, JobDefinitionResponse, JobError, JobLogsResponse, JobStatusResponse,
};

pub async fn create_job(
    _: Authorized<JobsRun>,
    State(state): State<crate::AppState>,
    AxumPath(name): AxumPath<String>,
    Json(body): Json<Value>,
) -> Result<(StatusCode, Json<JobCreatedResponse>), JobError> {
    let params = body
        .as_object()
        .cloned()
        .ok_or(JobError::InvalidBody("expected a JSON object"))?;
    let created = state.jobs.create_job(
        &name,
        params,
        &state.manifests,
        &state.artifacts,
        state.config.jobs.cleanup_successful_workdirs,
        state.config.jobs.keep_failed_workdirs,
    )?;

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
    Ok(Json(JobStatusResponse::from(state.jobs.read_job(&job_id)?)))
}

pub async fn get_job_logs(
    _: Authorized<LogsRead>,
    State(state): State<crate::AppState>,
    AxumPath(job_id): AxumPath<String>,
) -> Result<Json<JobLogsResponse>, JobError> {
    Ok(Json(JobLogsResponse::from(state.jobs.read_logs(&job_id)?)))
}

pub async fn cancel_job(
    _: Authorized<JobsRun>,
    State(state): State<crate::AppState>,
    AxumPath(job_id): AxumPath<String>,
) -> Result<StatusCode, JobError> {
    state.jobs.cancel_job(&job_id)?;
    Ok(StatusCode::ACCEPTED)
}
