use std::{
    collections::BTreeMap,
    fmt, fs,
    path::{Path, PathBuf},
    process::Stdio,
    sync::{Arc, Mutex},
};

use axum::{
    Json,
    extract::{Path as AxumPath, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use chrono::{SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use tokio::{
    process::Command,
    sync::watch,
    time::{Duration, timeout},
};
use uuid::Uuid;

use crate::{
    artifacts::{ArtifactError, ArtifactStore},
    auth::{Authorized, JobsRead, JobsRun, LogsRead},
    manifest::{Concurrency, JobManifest, ManifestStore, OutputSpec, ParamType},
};

#[derive(Debug)]
pub struct JobStore {
    root_dir: PathBuf,
    running_jobs: Mutex<BTreeMap<String, RunningJob>>,
}

impl JobStore {
    pub fn new(data_dir: impl AsRef<Path>) -> Result<Self, JobError> {
        let root_dir = data_dir.as_ref().join("jobs");
        fs::create_dir_all(&root_dir).map_err(|source| JobError::CreateDir {
            path: root_dir.display().to_string(),
            source,
        })?;

        Ok(Self {
            root_dir,
            running_jobs: Mutex::new(BTreeMap::new()),
        })
    }

    pub fn create_job(
        self: &Arc<Self>,
        name: &str,
        params: Map<String, Value>,
        manifests: &ManifestStore,
        artifacts: &ArtifactStore,
        cleanup_successful_workdirs: bool,
        keep_failed_workdirs: bool,
    ) -> Result<JobCreatedResponse, JobError> {
        let manifest = manifests
            .get(name)
            .cloned()
            .ok_or_else(|| JobError::UnknownJob(name.to_string()))?;

        let resolved = validate_params(&manifest, &params, artifacts)?;

        let job_id = format!("job_{}", Uuid::now_v7().simple());
        let started_at = now_rfc3339();
        let job = JobMetadata {
            job_id: job_id.clone(),
            name: manifest.name.clone(),
            status: JobStatus::Running,
            started_at: started_at.clone(),
            finished_at: None,
            exit_code: None,
            params,
            resolved_artifacts: resolved,
            outputs: BTreeMap::new(),
        };

        let (cancel_tx, cancel_rx) = watch::channel(false);

        {
            let mut running_jobs = self.running_jobs.lock().expect("job mutex poisoned");
            enforce_concurrency(&manifest, &running_jobs)?;
            running_jobs.insert(
                job_id.clone(),
                RunningJob {
                    job_id: job_id.clone(),
                    name: manifest.name.clone(),
                    concurrency: manifest.concurrency.clone(),
                    cancel_tx,
                },
            );
        }

        let setup_result: Result<PathBuf, JobError> = (|| {
            let job_dir = self.root_dir.join(&job_id);
            fs::create_dir_all(job_dir.join("work")).map_err(|source| JobError::CreateDir {
                path: job_dir.join("work").display().to_string(),
                source,
            })?;
            fs::create_dir_all(job_dir.join("output")).map_err(|source| JobError::CreateDir {
                path: job_dir.join("output").display().to_string(),
                source,
            })?;
            fs::write(job_dir.join("stdout.log"), []).map_err(|source| JobError::WriteFile {
                path: job_dir.join("stdout.log").display().to_string(),
                source,
            })?;
            fs::write(job_dir.join("stderr.log"), []).map_err(|source| JobError::WriteFile {
                path: job_dir.join("stderr.log").display().to_string(),
                source,
            })?;
            let metadata_json = serde_json::to_vec_pretty(&job)
                .map_err(|source| JobError::SerializeMetadata { source })?;
            fs::write(job_dir.join("metadata.json"), metadata_json).map_err(|source| {
                JobError::WriteFile {
                    path: job_dir.join("metadata.json").display().to_string(),
                    source,
                }
            })?;

            Ok(job_dir)
        })();

        let job_dir = match setup_result {
            Ok(job_dir) => job_dir,
            Err(error) => {
                let mut running_jobs = self.running_jobs.lock().expect("job mutex poisoned");
                running_jobs.remove(&job_id);
                return Err(error);
            }
        };

        let execution = JobExecution {
            artifacts: Arc::new(artifacts.clone()),
            manifest,
            job_id: job_id.clone(),
            metadata_path: job_dir.join("metadata.json"),
            work_dir: job_dir.join("work"),
            output_dir: job_dir.join("output"),
            stdout_path: job_dir.join("stdout.log"),
            stderr_path: job_dir.join("stderr.log"),
            cleanup_successful_workdirs,
            keep_failed_workdirs,
            metadata: job,
            cancel_rx,
        };

        let store = Arc::clone(self);
        tokio::spawn(async move {
            store.run_job(execution).await;
        });

        Ok(JobCreatedResponse::from(JobCreated {
            job_id,
            status: JobStatus::Running,
            started_at,
        }))
    }

    pub fn read_job(&self, job_id: &str) -> Result<JobMetadata, JobError> {
        let metadata_path = self.root_dir.join(job_id).join("metadata.json");
        let bytes = fs::read(&metadata_path).map_err(|source| {
            if source.kind() == std::io::ErrorKind::NotFound {
                JobError::JobNotFound(job_id.to_string())
            } else {
                JobError::ReadFile {
                    path: metadata_path.display().to_string(),
                    source,
                }
            }
        })?;

        serde_json::from_slice(&bytes).map_err(|source| JobError::ParseMetadata {
            path: metadata_path.display().to_string(),
            source,
        })
    }

    pub fn read_logs(&self, job_id: &str) -> Result<JobLogs, JobError> {
        let job_dir = self.root_dir.join(job_id);
        let stdout_path = job_dir.join("stdout.log");
        let stderr_path = job_dir.join("stderr.log");

        let stdout = fs::read_to_string(&stdout_path).map_err(|source| {
            if source.kind() == std::io::ErrorKind::NotFound {
                JobError::JobNotFound(job_id.to_string())
            } else {
                JobError::ReadFile {
                    path: stdout_path.display().to_string(),
                    source,
                }
            }
        })?;
        let stderr = fs::read_to_string(&stderr_path).map_err(|source| {
            if source.kind() == std::io::ErrorKind::NotFound {
                JobError::JobNotFound(job_id.to_string())
            } else {
                JobError::ReadFile {
                    path: stderr_path.display().to_string(),
                    source,
                }
            }
        })?;

        Ok(JobLogs { stdout, stderr })
    }

    pub fn cancel_job(&self, job_id: &str) -> Result<(), JobError> {
        let running_jobs = self.running_jobs.lock().expect("job mutex poisoned");
        let running_job = running_jobs
            .get(job_id)
            .ok_or_else(|| match self.read_job(job_id) {
                Ok(_) => JobError::JobNotRunning(job_id.to_string()),
                Err(JobError::JobNotFound(_)) => JobError::JobNotFound(job_id.to_string()),
                Err(error) => error,
            })?;

        running_job
            .cancel_tx
            .send(true)
            .map_err(|_| JobError::JobNotRunning(job_id.to_string()))?;
        Ok(())
    }

    pub fn recover_interrupted_jobs(&self) -> Result<usize, JobError> {
        let mut recovered = 0;
        let entries = fs::read_dir(&self.root_dir).map_err(|source| JobError::ReadFile {
            path: self.root_dir.display().to_string(),
            source,
        })?;

        for entry in entries {
            let entry = entry.map_err(|source| JobError::ReadFile {
                path: self.root_dir.display().to_string(),
                source,
            })?;
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }

            let metadata_path = path.join("metadata.json");
            let stderr_path = path.join("stderr.log");
            let mut metadata = match fs::read(&metadata_path) {
                Ok(bytes) => serde_json::from_slice::<JobMetadata>(&bytes).map_err(|source| {
                    JobError::ParseMetadata {
                        path: metadata_path.display().to_string(),
                        source,
                    }
                })?,
                Err(source) if source.kind() == std::io::ErrorKind::NotFound => continue,
                Err(source) => {
                    return Err(JobError::ReadFile {
                        path: metadata_path.display().to_string(),
                        source,
                    });
                }
            };

            if metadata.status == JobStatus::Running {
                metadata.status = JobStatus::Failed;
                metadata.finished_at = Some(now_rfc3339());
                metadata.exit_code = None;
                self.persist_metadata(&metadata_path, &metadata)?;
                append_recovery_message(&stderr_path)?;
                recovered += 1;
            }
        }

        Ok(recovered)
    }
}

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
    pub status: JobStatus,
    pub started_at: String,
    pub finished_at: Option<String>,
    pub exit_code: Option<i32>,
    pub params: Map<String, Value>,
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
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    Running,
    Success,
    Failed,
    TimedOut,
    Canceled,
    Rejected,
}

#[derive(Debug, Clone)]
struct RunningJob {
    job_id: String,
    name: String,
    concurrency: Concurrency,
    cancel_tx: watch::Sender<bool>,
}

#[derive(Debug, Clone)]
struct JobExecution {
    artifacts: Arc<ArtifactStore>,
    manifest: JobManifest,
    job_id: String,
    metadata_path: PathBuf,
    work_dir: PathBuf,
    output_dir: PathBuf,
    stdout_path: PathBuf,
    stderr_path: PathBuf,
    cleanup_successful_workdirs: bool,
    keep_failed_workdirs: bool,
    metadata: JobMetadata,
    cancel_rx: watch::Receiver<bool>,
}

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
            Self::MissingParam(_)
            | Self::UnknownParam(_)
            | Self::InvalidParamType { .. }
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

fn validate_params(
    manifest: &JobManifest,
    params: &Map<String, Value>,
    artifacts: &ArtifactStore,
) -> Result<BTreeMap<String, String>, JobError> {
    for (param_name, spec) in &manifest.params {
        if spec.required && !params.contains_key(param_name) {
            return Err(JobError::MissingParam(param_name.clone()));
        }
    }

    for name in params.keys() {
        if !manifest.params.contains_key(name) {
            return Err(JobError::UnknownParam(name.clone()));
        }
    }

    let mut resolved = BTreeMap::new();

    for (name, value) in params {
        let spec = &manifest.params[name];

        match spec.kind {
            ParamType::String => {
                if !value.is_string() {
                    return Err(JobError::InvalidParamType {
                        name: name.clone(),
                        expected: "string",
                    });
                }
            }
            ParamType::Integer => {
                if value.as_i64().is_none() {
                    return Err(JobError::InvalidParamType {
                        name: name.clone(),
                        expected: "integer",
                    });
                }
            }
            ParamType::Boolean => {
                if !value.is_boolean() {
                    return Err(JobError::InvalidParamType {
                        name: name.clone(),
                        expected: "boolean",
                    });
                }
            }
            ParamType::Artifact => {
                let artifact_id = value.as_str().ok_or_else(|| JobError::InvalidParamType {
                    name: name.clone(),
                    expected: "artifact id string",
                })?;
                let metadata = artifacts.load_metadata(artifact_id)?;

                if metadata.is_expired()? {
                    return Err(JobError::ExpiredArtifact {
                        name: name.clone(),
                        artifact_id: artifact_id.to_string(),
                    });
                }

                resolved.insert(
                    name.clone(),
                    artifacts
                        .artifact_blob_path(artifact_id)
                        .display()
                        .to_string(),
                );
            }
            ParamType::Json => {
                if value.is_null() {
                    return Err(JobError::InvalidParamType {
                        name: name.clone(),
                        expected: "json",
                    });
                }
            }
        }
    }

    Ok(resolved)
}

fn enforce_concurrency(
    manifest: &JobManifest,
    running_jobs: &BTreeMap<String, RunningJob>,
) -> Result<(), JobError> {
    match manifest.concurrency {
        Concurrency::Parallel => {
            if running_jobs
                .values()
                .any(|job| matches!(job.concurrency, Concurrency::GlobalExclusive))
            {
                return Err(JobError::ConcurrencyConflict {
                    reason: "cannot start job while a global_exclusive job is running".to_string(),
                });
            }
        }
        Concurrency::JobExclusive => {
            if let Some(job) = running_jobs.values().find(|job| {
                matches!(job.concurrency, Concurrency::GlobalExclusive) || job.name == manifest.name
            }) {
                let reason = if matches!(job.concurrency, Concurrency::GlobalExclusive) {
                    "cannot start job while a global_exclusive job is running".to_string()
                } else {
                    format!(
                        "cannot start job {} while another instance is running ({})",
                        manifest.name, job.job_id
                    )
                };
                return Err(JobError::ConcurrencyConflict { reason });
            }
        }
        Concurrency::GlobalExclusive => {
            if !running_jobs.is_empty() {
                return Err(JobError::ConcurrencyConflict {
                    reason: "cannot start global_exclusive job while another job is running"
                        .to_string(),
                });
            }
        }
    }

    Ok(())
}

fn now_rfc3339() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true)
}

impl JobStore {
    async fn run_job(self: Arc<Self>, execution: JobExecution) {
        let outcome = self.execute_process(&execution).await;
        self.finish_job(execution, outcome);
    }

    async fn execute_process(&self, execution: &JobExecution) -> ExecutionOutcome {
        let stdout = match fs::File::create(&execution.stdout_path) {
            Ok(file) => file,
            Err(source) => {
                return ExecutionOutcome::FailedToStart(JobError::WriteFile {
                    path: execution.stdout_path.display().to_string(),
                    source,
                });
            }
        };
        let stderr = match fs::File::create(&execution.stderr_path) {
            Ok(file) => file,
            Err(source) => {
                return ExecutionOutcome::FailedToStart(JobError::WriteFile {
                    path: execution.stderr_path.display().to_string(),
                    source,
                });
            }
        };

        let mut command = Command::new(&execution.manifest.script);
        command
            .current_dir(&execution.work_dir)
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr));

        for (key, value) in build_job_env(&execution.metadata, execution) {
            command.env(key, value);
        }

        let mut child = match command.spawn() {
            Ok(child) => child,
            Err(source) => {
                return ExecutionOutcome::FailedToStart(JobError::SpawnProcess {
                    script: execution.manifest.script.clone(),
                    source,
                });
            }
        };

        let cancel_wait = async {
            let mut cancel_rx = execution.cancel_rx.clone();
            if *cancel_rx.borrow() {
                Ok(())
            } else {
                cancel_rx.changed().await.map_err(|_| ())
            }
        };

        tokio::select! {
            result = timeout(
                Duration::from_secs(execution.manifest.timeout_seconds),
                child.wait(),
            ) => match result {
                Ok(Ok(status)) => {
                    let code = status.code();
                    if status.success() {
                        ExecutionOutcome::Completed {
                            status: JobStatus::Success,
                            exit_code: code,
                        }
                    } else {
                        ExecutionOutcome::Completed {
                            status: JobStatus::Failed,
                            exit_code: code,
                        }
                    }
                }
                Ok(Err(source)) => ExecutionOutcome::FailedToStart(JobError::WaitProcess {
                    script: execution.manifest.script.clone(),
                    source,
                }),
                Err(_) => {
                    let _ = child.kill().await;
                    let _ = child.wait().await;
                    ExecutionOutcome::Completed {
                        status: JobStatus::TimedOut,
                        exit_code: None,
                    }
                }
            },
            _ = cancel_wait => {
                let _ = child.kill().await;
                let _ = child.wait().await;
                ExecutionOutcome::Completed {
                    status: JobStatus::Canceled,
                    exit_code: None,
                }
            }
        }
    }

    fn finish_job(&self, execution: JobExecution, outcome: ExecutionOutcome) {
        let mut metadata = execution.metadata;
        let finished_at = now_rfc3339();

        match outcome {
            ExecutionOutcome::Completed { status, exit_code } => {
                let final_status = if matches!(status, JobStatus::Success) {
                    match register_outputs(
                        &execution.artifacts,
                        &execution.manifest.outputs,
                        &execution.output_dir,
                        &mut metadata,
                    ) {
                        Ok(()) => JobStatus::Success,
                        Err(error) => {
                            let _ = append_error(&execution.stderr_path, &error);
                            JobStatus::Failed
                        }
                    }
                } else {
                    status
                };

                metadata.status = final_status.clone();
                metadata.exit_code = exit_code;
                metadata.finished_at = Some(finished_at);

                let _ = self.persist_metadata(&execution.metadata_path, &metadata);

                match final_status {
                    JobStatus::Success if execution.cleanup_successful_workdirs => {
                        let _ = fs::remove_dir_all(&execution.work_dir);
                    }
                    JobStatus::Failed | JobStatus::TimedOut if !execution.keep_failed_workdirs => {
                        let _ = fs::remove_dir_all(&execution.work_dir);
                    }
                    _ => {}
                }
            }
            ExecutionOutcome::FailedToStart(error) => {
                metadata.status = JobStatus::Failed;
                metadata.exit_code = None;
                metadata.finished_at = Some(finished_at);
                let _ = self.persist_metadata(&execution.metadata_path, &metadata);
                let _ = fs::write(&execution.stderr_path, format!("{error}\n"));
            }
        }

        let mut running_jobs = self.running_jobs.lock().expect("job mutex poisoned");
        running_jobs.remove(&execution.job_id);
    }

    fn persist_metadata(&self, path: &Path, metadata: &JobMetadata) -> Result<(), JobError> {
        let metadata_json = serde_json::to_vec_pretty(metadata)
            .map_err(|source| JobError::SerializeMetadata { source })?;
        fs::write(path, metadata_json).map_err(|source| JobError::WriteFile {
            path: path.display().to_string(),
            source,
        })
    }
}

#[derive(Debug)]
enum ExecutionOutcome {
    Completed {
        status: JobStatus,
        exit_code: Option<i32>,
    },
    FailedToStart(JobError),
}

fn build_job_env(metadata: &JobMetadata, execution: &JobExecution) -> BTreeMap<String, String> {
    let mut env = BTreeMap::from([
        ("JOB_ID".to_string(), metadata.job_id.clone()),
        ("JOB_NAME".to_string(), metadata.name.clone()),
        (
            "JOB_WORKDIR".to_string(),
            execution.work_dir.display().to_string(),
        ),
        (
            "JOB_OUTPUT_DIR".to_string(),
            execution.output_dir.display().to_string(),
        ),
        (
            "JOB_METADATA_PATH".to_string(),
            execution.metadata_path.display().to_string(),
        ),
    ]);

    for (name, value) in &metadata.params {
        let env_name = normalize_param_env(name);
        let env_value = if let Some(path) = metadata.resolved_artifacts.get(name) {
            path.clone()
        } else if let Some(raw) = value.as_str() {
            raw.to_string()
        } else {
            value.to_string()
        };
        env.insert(env_name, env_value);
    }

    env
}

fn normalize_param_env(name: &str) -> String {
    format!("JOB_{}", name.replace('-', "_").to_ascii_uppercase())
}

fn register_outputs(
    artifacts: &ArtifactStore,
    specs: &BTreeMap<String, OutputSpec>,
    output_dir: &Path,
    metadata: &mut JobMetadata,
) -> Result<(), JobError> {
    for (name, spec) in specs {
        let path = output_dir.join(&spec.path);

        match fs::metadata(&path) {
            Ok(file_type) if file_type.is_file() => {}
            Ok(_) => {
                return Err(JobError::MissingOutput {
                    name: name.clone(),
                    path: path.display().to_string(),
                });
            }
            Err(source) if source.kind() == std::io::ErrorKind::NotFound && spec.required => {
                return Err(JobError::MissingOutput {
                    name: name.clone(),
                    path: path.display().to_string(),
                });
            }
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => continue,
            Err(source) => {
                return Err(JobError::ReadFile {
                    path: path.display().to_string(),
                    source,
                });
            }
        }

        let stored = artifacts.store_file(&path)?;
        metadata.outputs.insert(
            name.clone(),
            JobOutputArtifact {
                artifact_id: stored.artifact_id,
                sha256: stored.sha256,
                size: stored.size,
            },
        );
    }

    Ok(())
}

fn append_error(path: &Path, error: &JobError) -> Result<(), JobError> {
    let mut stderr = fs::read_to_string(path).unwrap_or_default();
    if !stderr.is_empty() && !stderr.ends_with('\n') {
        stderr.push('\n');
    }
    stderr.push_str(&error.to_string());
    stderr.push('\n');

    fs::write(path, stderr).map_err(|source| JobError::WriteFile {
        path: path.display().to_string(),
        source,
    })
}

fn append_recovery_message(path: &Path) -> Result<(), JobError> {
    let message = "runner restarted before job completion; marking job as failed";
    let mut stderr = fs::read_to_string(path).unwrap_or_default();
    if !stderr.is_empty() && !stderr.ends_with('\n') {
        stderr.push('\n');
    }
    stderr.push_str(message);
    stderr.push('\n');

    fs::write(path, stderr).map_err(|source| JobError::WriteFile {
        path: path.display().to_string(),
        source,
    })
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

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        fs,
        path::{Path, PathBuf},
        sync::Arc,
        time::{SystemTime, UNIX_EPOCH},
    };

    use axum::{
        Router,
        body::Body,
        http::{Request, StatusCode},
        routing::{get, post},
    };
    use serde_json::{Map, json};
    use sha2::Digest;
    use tokio::time::{Duration, sleep};
    use tower::util::ServiceExt;

    use super::{
        JobCreatedResponse, JobLogsResponse, JobMetadata, JobStatusResponse, JobStore, cancel_job,
        create_job, get_job, get_job_logs,
    };
    use crate::{
        AppState,
        artifacts::ArtifactStore,
        auth::AuthStore,
        config::{ArtifactsConfig, AuthConfig, Config, JobsConfig, ServerConfig},
        manifest::ManifestStore,
    };

    #[tokio::test]
    async fn creates_job_metadata_for_valid_request() {
        let temp = temp_dir("job_create");
        let state = test_state(&temp);
        let app = Router::new()
            .route("/jobs/{id}", post(create_job))
            .with_state(state);

        let response = app
            .oneshot(
                Request::post("/jobs/build-app")
                    .header("authorization", "Bearer runner-token")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({
                            "commit": "abc123",
                            "branch": "main"
                        })
                        .to_string(),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");

        assert_eq!(response.status(), StatusCode::CREATED);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("response body");
        let created: JobCreatedResponse = serde_json::from_slice(&body).expect("created job body");
        let metadata_path = temp
            .join("jobs")
            .join(&created.job_id)
            .join("metadata.json");
        let metadata: JobMetadata =
            serde_json::from_slice(&fs::read(metadata_path).expect("metadata should be written"))
                .expect("metadata should parse");

        assert_eq!(metadata.name, "build-app");
        assert_eq!(metadata.params["commit"], "abc123");
    }

    #[tokio::test]
    async fn rejects_missing_required_param() {
        let temp = temp_dir("job_missing_param");
        let state = test_state(&temp);
        let app = Router::new()
            .route("/jobs/{id}", post(create_job))
            .with_state(state);

        let response = app
            .oneshot(
                Request::post("/jobs/build-app")
                    .header("authorization", "Bearer runner-token")
                    .header("content-type", "application/json")
                    .body(Body::from(json!({ "commit": "abc123" }).to_string()))
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn rejects_unknown_param() {
        let temp = temp_dir("job_unknown_param");
        let state = test_state(&temp);
        let app = Router::new()
            .route("/jobs/{id}", post(create_job))
            .with_state(state);

        let response = app
            .oneshot(
                Request::post("/jobs/build-app")
                    .header("authorization", "Bearer runner-token")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({
                            "commit": "abc123",
                            "branch": "main",
                            "extra": "nope"
                        })
                        .to_string(),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn resolves_artifact_params() {
        let temp = temp_dir("job_artifact_param");
        let state = test_state_with_artifact_manifest(&temp);
        let artifact_id = store_artifact(&state.artifacts, b"src");
        let app = Router::new()
            .route("/jobs/{id}", post(create_job))
            .with_state(state);

        let response = app
            .oneshot(
                Request::post("/jobs/build-with-artifact")
                    .header("authorization", "Bearer runner-token")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({
                            "source": artifact_id
                        })
                        .to_string(),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");

        assert_eq!(response.status(), StatusCode::CREATED);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("response body");
        let created: JobCreatedResponse = serde_json::from_slice(&body).expect("created job body");
        let metadata_path = temp
            .join("jobs")
            .join(&created.job_id)
            .join("metadata.json");
        let metadata: JobMetadata =
            serde_json::from_slice(&fs::read(metadata_path).expect("metadata should be written"))
                .expect("metadata should parse");

        assert!(metadata.resolved_artifacts["source"].ends_with("/blob"));
    }

    #[tokio::test]
    async fn executes_successful_script_and_cleans_workdir() {
        let temp = temp_dir("job_execute_success");
        let state = test_state_with_script(
            &temp,
            "build-app",
            r#"#!/bin/sh
printf '%s' "$JOB_COMMIT"
printf '%s' "$JOB_SOURCE" >&2
exit 0
"#,
            600,
        );
        let artifact_id = store_artifact(&state.artifacts, b"src");
        let app = Router::new()
            .route("/jobs/{id}", post(create_job))
            .with_state(state);

        let response = app
            .oneshot(
                Request::post("/jobs/build-app")
                    .header("authorization", "Bearer runner-token")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({
                            "commit": "abc123",
                            "source": artifact_id
                        })
                        .to_string(),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");

        let created = read_created_job(response).await;
        let metadata = wait_for_terminal_metadata(&temp, &created.job_id).await;

        assert_eq!(metadata.status, super::JobStatus::Success);
        assert_eq!(metadata.exit_code, Some(0));
        assert_eq!(
            fs::read_to_string(temp.join("jobs").join(&created.job_id).join("stdout.log"))
                .expect("stdout log"),
            "abc123"
        );
        assert!(
            fs::read_to_string(temp.join("jobs").join(&created.job_id).join("stderr.log"))
                .expect("stderr log")
                .ends_with("/blob")
        );
        assert!(
            !temp
                .join("jobs")
                .join(&created.job_id)
                .join("work")
                .exists()
        );
    }

    #[tokio::test]
    async fn marks_failed_script_as_failed() {
        let temp = temp_dir("job_execute_failed");
        let state = test_state_with_script(
            &temp,
            "build-app",
            "#!/bin/sh\nprintf 'boom' >&2\nexit 7\n",
            600,
        );
        let app = Router::new()
            .route("/jobs/{id}", post(create_job))
            .with_state(state);

        let response = app
            .oneshot(
                Request::post("/jobs/build-app")
                    .header("authorization", "Bearer runner-token")
                    .header("content-type", "application/json")
                    .body(Body::from(json!({ "commit": "abc123" }).to_string()))
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");

        let created = read_created_job(response).await;
        let metadata = wait_for_terminal_metadata(&temp, &created.job_id).await;

        assert_eq!(metadata.status, super::JobStatus::Failed);
        assert_eq!(metadata.exit_code, Some(7));
        assert!(
            temp.join("jobs")
                .join(&created.job_id)
                .join("work")
                .exists()
        );
    }

    #[tokio::test]
    async fn times_out_long_running_script() {
        let temp = temp_dir("job_execute_timeout");
        let state = test_state_with_script(&temp, "build-app", "#!/bin/sh\nsleep 2\n", 1);
        let app = Router::new()
            .route("/jobs/{id}", post(create_job))
            .with_state(state);

        let response = app
            .oneshot(
                Request::post("/jobs/build-app")
                    .header("authorization", "Bearer runner-token")
                    .header("content-type", "application/json")
                    .body(Body::from(json!({ "commit": "abc123" }).to_string()))
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");

        let created = read_created_job(response).await;
        let metadata = wait_for_terminal_metadata(&temp, &created.job_id).await;

        assert_eq!(metadata.status, super::JobStatus::TimedOut);
        assert_eq!(metadata.exit_code, None);
    }

    #[tokio::test]
    async fn registers_declared_outputs_as_artifacts() {
        let temp = temp_dir("job_output_artifact");
        let state = test_state_from_manifest(
            &temp,
            "build-app",
            r#"
[params.commit]
type = "string"
required = false
"#,
            r#"
[outputs.app]
path = "app.tar.gz"
required = true
"#,
            "#!/bin/sh\nprintf 'bundle' > \"$JOB_OUTPUT_DIR/app.tar.gz\"\n",
            600,
        );
        let app = Router::new()
            .route("/jobs/{id}", post(create_job))
            .with_state(state);

        let created = read_created_job(
            app.oneshot(
                Request::post("/jobs/build-app")
                    .header("authorization", "Bearer runner-token")
                    .header("content-type", "application/json")
                    .body(Body::from(json!({ "commit": "abc123" }).to_string()))
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed"),
        )
        .await;
        let metadata = wait_for_terminal_metadata(&temp, &created.job_id).await;

        assert_eq!(metadata.status, super::JobStatus::Success);
        let output = &metadata.outputs["app"];
        assert_eq!(output.size, 6);
        let stored = fs::read_to_string(
            temp.join("artifacts")
                .join(&output.artifact_id)
                .join("blob"),
        )
        .expect("stored output artifact");
        assert_eq!(stored, "bundle");
    }

    #[tokio::test]
    async fn fails_successful_script_when_required_output_is_missing() {
        let temp = temp_dir("job_output_missing");
        let state = test_state_from_manifest(
            &temp,
            "build-app",
            r#"
[params.commit]
type = "string"
required = false
"#,
            r#"
[outputs.app]
path = "app.tar.gz"
required = true
"#,
            "#!/bin/sh\nexit 0\n",
            600,
        );
        let app = Router::new()
            .route("/jobs/{id}", post(create_job))
            .with_state(state);

        let created = read_created_job(
            app.oneshot(
                Request::post("/jobs/build-app")
                    .header("authorization", "Bearer runner-token")
                    .header("content-type", "application/json")
                    .body(Body::from(json!({ "commit": "abc123" }).to_string()))
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed"),
        )
        .await;
        let metadata = wait_for_terminal_metadata(&temp, &created.job_id).await;

        assert_eq!(metadata.status, super::JobStatus::Failed);
        assert!(
            fs::read_to_string(temp.join("jobs").join(&created.job_id).join("stderr.log"))
                .expect("stderr log")
                .contains("required output app is missing")
        );
    }

    #[tokio::test]
    async fn reads_job_status_over_http() {
        let temp = temp_dir("job_status_http");
        let state = test_state_with_script(&temp, "build-app", "#!/bin/sh\nexit 0\n", 600);
        let app = Router::new()
            .route("/jobs/{id}", get(get_job).post(create_job))
            .with_state(state);

        let created = read_created_job(
            app.clone()
                .oneshot(
                    Request::post("/jobs/build-app")
                        .header("authorization", "Bearer runner-token")
                        .header("content-type", "application/json")
                        .body(Body::from(json!({ "commit": "abc123" }).to_string()))
                        .expect("request should build"),
                )
                .await
                .expect("request should succeed"),
        )
        .await;

        let metadata = wait_for_terminal_metadata(&temp, &created.job_id).await;

        let response = app
            .oneshot(
                Request::get(format!("/jobs/{}", created.job_id))
                    .header("authorization", "Bearer runner-token")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("response body");
        let fetched: JobStatusResponse = serde_json::from_slice(&body).expect("job metadata body");

        assert_eq!(fetched.job_id, created.job_id);
        assert_eq!(fetched.status, metadata.status);
        assert_eq!(fetched.finished_at, metadata.finished_at);
    }

    #[tokio::test]
    async fn reads_job_logs_over_http() {
        let temp = temp_dir("job_logs_http");
        let state = test_state_with_script(
            &temp,
            "build-app",
            "#!/bin/sh\nprintf 'out'\nprintf 'err' >&2\n",
            600,
        );
        let app = Router::new()
            .route("/jobs/{id}", post(create_job))
            .route("/jobs/{job_id}/logs", get(get_job_logs))
            .with_state(state);

        let created = read_created_job(
            app.clone()
                .oneshot(
                    Request::post("/jobs/build-app")
                        .header("authorization", "Bearer runner-token")
                        .header("content-type", "application/json")
                        .body(Body::from(json!({ "commit": "abc123" }).to_string()))
                        .expect("request should build"),
                )
                .await
                .expect("request should succeed"),
        )
        .await;

        let _ = wait_for_terminal_metadata(&temp, &created.job_id).await;

        let response = app
            .oneshot(
                Request::get(format!("/jobs/{}/logs", created.job_id))
                    .header("authorization", "Bearer runner-token")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("response body");
        let logs: JobLogsResponse = serde_json::from_slice(&body).expect("job logs body");

        assert_eq!(logs.stdout, "out");
        assert_eq!(logs.stderr, "err");
    }

    #[tokio::test]
    async fn cancels_running_job_over_http() {
        let temp = temp_dir("job_cancel_http");
        let state = test_state_with_script(&temp, "build-app", "#!/bin/sh\nsleep 5\n", 600);
        let app = Router::new()
            .route(
                "/jobs/{id}",
                get(get_job).post(create_job).delete(cancel_job),
            )
            .with_state(state);

        let created = read_created_job(
            app.clone()
                .oneshot(
                    Request::post("/jobs/build-app")
                        .header("authorization", "Bearer runner-token")
                        .header("content-type", "application/json")
                        .body(Body::from(json!({ "commit": "abc123" }).to_string()))
                        .expect("request should build"),
                )
                .await
                .expect("request should succeed"),
        )
        .await;

        let cancel = app
            .oneshot(
                Request::delete(format!("/jobs/{}", created.job_id))
                    .header("authorization", "Bearer runner-token")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");

        assert_eq!(cancel.status(), StatusCode::ACCEPTED);

        let metadata = wait_for_terminal_metadata(&temp, &created.job_id).await;
        assert_eq!(metadata.status, super::JobStatus::Canceled);
        assert_eq!(metadata.exit_code, None);
    }

    #[test]
    fn recovers_running_jobs_on_startup() {
        let temp = temp_dir("job_recovery_startup");
        let jobs_dir = temp.join("jobs").join("job_recover");
        fs::create_dir_all(&jobs_dir).expect("job dir should be created");
        fs::write(jobs_dir.join("stderr.log"), "").expect("stderr should exist");
        fs::write(
            jobs_dir.join("metadata.json"),
            serde_json::to_vec_pretty(&JobMetadata {
                job_id: "job_recover".to_string(),
                name: "build-app".to_string(),
                status: super::JobStatus::Running,
                started_at: "2026-01-01T00:00:00Z".to_string(),
                finished_at: None,
                exit_code: None,
                params: Map::new(),
                resolved_artifacts: BTreeMap::new(),
                outputs: BTreeMap::new(),
            })
            .expect("metadata"),
        )
        .expect("metadata written");

        let store = JobStore::new(&temp).expect("job store should init");
        let recovered = store
            .recover_interrupted_jobs()
            .expect("recovery should succeed");

        assert_eq!(recovered, 1);
        let metadata = store.read_job("job_recover").expect("job should load");
        assert_eq!(metadata.status, super::JobStatus::Failed);
        assert!(metadata.finished_at.is_some());
        assert!(
            fs::read_to_string(jobs_dir.join("stderr.log"))
                .expect("stderr should read")
                .contains("runner restarted before job completion")
        );
    }

    #[tokio::test]
    async fn parallel_jobs_can_run_together() {
        let temp = temp_dir("job_parallel_concurrency");
        let state = test_state_with_manifests(
            &temp,
            vec![
                TestManifest {
                    job_name: "job-a",
                    concurrency: "parallel",
                    params_toml: OPTIONAL_COMMIT_PARAM,
                    outputs_toml: "",
                },
                TestManifest {
                    job_name: "job-b",
                    concurrency: "parallel",
                    params_toml: OPTIONAL_COMMIT_PARAM,
                    outputs_toml: "",
                },
            ],
            "#!/bin/sh\nsleep 1\n",
            600,
        );
        let app = Router::new()
            .route("/jobs/{id}", post(create_job))
            .with_state(state);

        let first = app
            .clone()
            .oneshot(
                Request::post("/jobs/job-a")
                    .header("authorization", "Bearer runner-token")
                    .header("content-type", "application/json")
                    .body(Body::from(json!({ "commit": "a" }).to_string()))
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");
        let second = app
            .oneshot(
                Request::post("/jobs/job-b")
                    .header("authorization", "Bearer runner-token")
                    .header("content-type", "application/json")
                    .body(Body::from(json!({ "commit": "b" }).to_string()))
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");

        assert_eq!(first.status(), StatusCode::CREATED);
        assert_eq!(second.status(), StatusCode::CREATED);
    }

    #[tokio::test]
    async fn job_exclusive_rejects_second_instance_while_running() {
        let temp = temp_dir("job_exclusive_concurrency");
        let state = test_state_with_manifests(
            &temp,
            vec![TestManifest {
                job_name: "build-app",
                concurrency: "job_exclusive",
                params_toml: OPTIONAL_COMMIT_PARAM,
                outputs_toml: "",
            }],
            "#!/bin/sh\nsleep 1\n",
            600,
        );
        let app = Router::new()
            .route("/jobs/{id}", post(create_job))
            .with_state(state);

        let first = app
            .clone()
            .oneshot(
                Request::post("/jobs/build-app")
                    .header("authorization", "Bearer runner-token")
                    .header("content-type", "application/json")
                    .body(Body::from(json!({ "commit": "a" }).to_string()))
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");
        let second = app
            .oneshot(
                Request::post("/jobs/build-app")
                    .header("authorization", "Bearer runner-token")
                    .header("content-type", "application/json")
                    .body(Body::from(json!({ "commit": "b" }).to_string()))
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");

        assert_eq!(first.status(), StatusCode::CREATED);
        assert_eq!(second.status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn global_exclusive_rejects_when_other_job_is_running() {
        let temp = temp_dir("job_global_exclusive_conflict");
        let state = test_state_with_manifests(
            &temp,
            vec![
                TestManifest {
                    job_name: "job-a",
                    concurrency: "parallel",
                    params_toml: OPTIONAL_COMMIT_PARAM,
                    outputs_toml: "",
                },
                TestManifest {
                    job_name: "job-b",
                    concurrency: "global_exclusive",
                    params_toml: OPTIONAL_COMMIT_PARAM,
                    outputs_toml: "",
                },
            ],
            "#!/bin/sh\nsleep 1\n",
            600,
        );
        let app = Router::new()
            .route("/jobs/{id}", post(create_job))
            .with_state(state);

        let first = app
            .clone()
            .oneshot(
                Request::post("/jobs/job-a")
                    .header("authorization", "Bearer runner-token")
                    .header("content-type", "application/json")
                    .body(Body::from(json!({ "commit": "a" }).to_string()))
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");
        let second = app
            .oneshot(
                Request::post("/jobs/job-b")
                    .header("authorization", "Bearer runner-token")
                    .header("content-type", "application/json")
                    .body(Body::from(json!({ "commit": "b" }).to_string()))
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");

        assert_eq!(first.status(), StatusCode::CREATED);
        assert_eq!(second.status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn parallel_job_rejects_while_global_exclusive_is_running() {
        let temp = temp_dir("job_global_exclusive_running");
        let state = test_state_with_manifests(
            &temp,
            vec![
                TestManifest {
                    job_name: "job-a",
                    concurrency: "global_exclusive",
                    params_toml: OPTIONAL_COMMIT_PARAM,
                    outputs_toml: "",
                },
                TestManifest {
                    job_name: "job-b",
                    concurrency: "parallel",
                    params_toml: OPTIONAL_COMMIT_PARAM,
                    outputs_toml: "",
                },
            ],
            "#!/bin/sh\nsleep 1\n",
            600,
        );
        let app = Router::new()
            .route("/jobs/{id}", post(create_job))
            .with_state(state);

        let first = app
            .clone()
            .oneshot(
                Request::post("/jobs/job-a")
                    .header("authorization", "Bearer runner-token")
                    .header("content-type", "application/json")
                    .body(Body::from(json!({ "commit": "a" }).to_string()))
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");
        let second = app
            .oneshot(
                Request::post("/jobs/job-b")
                    .header("authorization", "Bearer runner-token")
                    .header("content-type", "application/json")
                    .body(Body::from(json!({ "commit": "b" }).to_string()))
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");

        assert_eq!(first.status(), StatusCode::CREATED);
        assert_eq!(second.status(), StatusCode::CONFLICT);
    }

    fn test_state(temp: &Path) -> AppState {
        test_state_from_manifest(
            temp,
            "build-app",
            REQUIRED_COMMIT_AND_BRANCH_PARAMS,
            "",
            "#!/bin/sh\nexit 0\n",
            600,
        )
    }

    fn test_state_with_artifact_manifest(temp: &Path) -> AppState {
        test_state_from_manifest(
            temp,
            "build-with-artifact",
            REQUIRED_SOURCE_PARAM,
            "",
            "#!/bin/sh\nexit 0\n",
            600,
        )
    }

    fn test_state_with_script(
        temp: &Path,
        job_name: &str,
        script_body: &str,
        timeout_seconds: u64,
    ) -> AppState {
        test_state_from_manifest(
            temp,
            job_name,
            OPTIONAL_COMMIT_AND_SOURCE_PARAMS,
            "",
            script_body,
            timeout_seconds,
        )
    }

    fn test_state_from_manifest(
        temp: &Path,
        job_name: &str,
        params_toml: &str,
        outputs_toml: &str,
        script_body: &str,
        timeout_seconds: u64,
    ) -> AppState {
        let manifests_dir = temp.join("manifests");
        let scripts_dir = temp.join("scripts");
        fs::create_dir_all(&manifests_dir).expect("manifests dir should be created");
        fs::create_dir_all(&scripts_dir).expect("scripts dir should be created");
        let script = write_executable_script(&scripts_dir, "build.sh", script_body);
        fs::write(
            manifests_dir.join(format!("{job_name}.toml")),
            format!(
                r#"
name = "{job_name}"
script = "{}"
timeout_seconds = {timeout_seconds}
concurrency = "parallel"

{params_toml}
{outputs_toml}
"#,
                script.display()
            ),
        )
        .expect("manifest should be written");

        let config = Config {
            data_dir: temp.display().to_string(),
            manifests_dir: manifests_dir.display().to_string(),
            server: ServerConfig {
                listen: "127.0.0.1:0".to_string(),
            },
            auth: AuthConfig {
                mode: "bearer".to_string(),
                tokens: Vec::new(),
            },
            artifacts: ArtifactsConfig {
                max_size_mb: 1,
                ttl_seconds: 3600,
                cleanup_interval_seconds: 600,
                require_checksum_on_upload: true,
            },
            jobs: JobsConfig {
                default_log_limit_mb: 50,
                cleanup_successful_workdirs: true,
                keep_failed_workdirs: true,
            },
        };

        build_state(config)
    }

    fn test_state_with_manifests(
        temp: &Path,
        manifests: Vec<TestManifest<'_>>,
        script_body: &str,
        timeout_seconds: u64,
    ) -> AppState {
        let manifests_dir = temp.join("manifests");
        let scripts_dir = temp.join("scripts");
        fs::create_dir_all(&manifests_dir).expect("manifests dir should be created");
        fs::create_dir_all(&scripts_dir).expect("scripts dir should be created");
        let script = write_executable_script(&scripts_dir, "build.sh", script_body);

        for manifest in manifests {
            fs::write(
                manifests_dir.join(format!("{}.toml", manifest.job_name)),
                format!(
                    r#"
name = "{}"
script = "{}"
timeout_seconds = {}
concurrency = "{}"

{}
{}
"#,
                    manifest.job_name,
                    script.display(),
                    timeout_seconds,
                    manifest.concurrency,
                    manifest.params_toml,
                    manifest.outputs_toml
                ),
            )
            .expect("manifest should be written");
        }

        let config = Config {
            data_dir: temp.display().to_string(),
            manifests_dir: manifests_dir.display().to_string(),
            server: ServerConfig {
                listen: "127.0.0.1:0".to_string(),
            },
            auth: AuthConfig {
                mode: "bearer".to_string(),
                tokens: Vec::new(),
            },
            artifacts: ArtifactsConfig {
                max_size_mb: 1,
                ttl_seconds: 3600,
                cleanup_interval_seconds: 600,
                require_checksum_on_upload: true,
            },
            jobs: JobsConfig {
                default_log_limit_mb: 50,
                cleanup_successful_workdirs: true,
                keep_failed_workdirs: true,
            },
        };

        build_state(config)
    }

    fn build_state(config: Config) -> AppState {
        AppState {
            config: Arc::new(config.clone()),
            auth: Arc::new(
                AuthStore::load_from_config(
                    &AuthConfig {
                        mode: "bearer".to_string(),
                        tokens: vec![crate::config::AuthTokenConfig {
                            name: "runner".to_string(),
                            token_env: "TOKEN_RUNNER".to_string(),
                            permissions: vec![
                                "jobs:run".to_string(),
                                "jobs:read".to_string(),
                                "logs:read".to_string(),
                                "artifacts:read".to_string(),
                                "artifacts:write".to_string(),
                            ],
                        }],
                    },
                    |name| match name {
                        "TOKEN_RUNNER" => Some("runner-token".to_string()),
                        _ => None,
                    },
                )
                .expect("auth should load"),
            ),
            manifests: Arc::new(
                ManifestStore::load_from_dir(&config.manifests_dir).expect("manifests should load"),
            ),
            artifacts: Arc::new(
                ArtifactStore::new(
                    &config.data_dir,
                    config.artifacts.ttl_seconds,
                    config.artifacts.max_size_mb,
                    config.artifacts.require_checksum_on_upload,
                )
                .expect("artifact store should init"),
            ),
            jobs: Arc::new(JobStore::new(&config.data_dir).expect("job store should init")),
        }
    }

    async fn read_created_job(response: axum::response::Response) -> JobCreatedResponse {
        assert_eq!(response.status(), StatusCode::CREATED);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("response body");
        serde_json::from_slice(&body).expect("created job body")
    }

    async fn wait_for_terminal_metadata(temp: &Path, job_id: &str) -> JobMetadata {
        let metadata_path = temp.join("jobs").join(job_id).join("metadata.json");

        for _ in 0..100 {
            let metadata: JobMetadata = serde_json::from_slice(
                &fs::read(&metadata_path).expect("metadata should be readable"),
            )
            .expect("metadata should parse");

            if metadata.finished_at.is_some() {
                return metadata;
            }

            sleep(Duration::from_millis(25)).await;
        }

        panic!("job did not reach a terminal state");
    }

    fn store_artifact(store: &ArtifactStore, bytes: &[u8]) -> String {
        let checksum = hex::encode(sha2::Sha256::digest(bytes));
        store
            .store_bytes(bytes, Some(&checksum))
            .expect("artifact should store")
            .artifact_id
    }

    fn write_executable_script(dir: &Path, name: &str, body: &str) -> PathBuf {
        let path = dir.join(name);
        fs::write(&path, body).expect("script should be written");

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            let mut permissions = fs::metadata(&path).expect("metadata").permissions();
            permissions.set_mode(0o755);
            fs::set_permissions(&path, permissions).expect("permissions should be set");
        }

        path
    }

    fn temp_dir(label: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time should work")
            .as_nanos();
        let path = std::env::temp_dir().join(format!("strait-runner-{label}-{unique}"));
        fs::create_dir_all(&path).expect("temp dir should be created");
        path
    }

    const REQUIRED_COMMIT_AND_BRANCH_PARAMS: &str = r#"
[params.commit]
type = "string"
required = true

[params.branch]
type = "string"
required = true
"#;

    const REQUIRED_SOURCE_PARAM: &str = r#"
[params.source]
type = "artifact"
required = true
"#;

    const OPTIONAL_COMMIT_AND_SOURCE_PARAMS: &str = r#"
[params.commit]
type = "string"
required = false

[params.source]
type = "artifact"
required = false
"#;

    const OPTIONAL_COMMIT_PARAM: &str = r#"
[params.commit]
type = "string"
required = false
"#;

    struct TestManifest<'a> {
        job_name: &'a str,
        concurrency: &'a str,
        params_toml: &'a str,
        outputs_toml: &'a str,
    }
}
