use std::{
    collections::BTreeMap,
    fmt, fs,
    path::{Path, PathBuf},
    sync::Mutex,
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
use uuid::Uuid;

use crate::{
    artifacts::{ArtifactError, ArtifactStore},
    manifest::{Concurrency, JobManifest, ManifestStore, ParamType},
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
        &self,
        name: &str,
        params: Map<String, Value>,
        manifests: &ManifestStore,
        artifacts: &ArtifactStore,
    ) -> Result<JobCreated, JobError> {
        let manifest = manifests
            .get(name)
            .ok_or_else(|| JobError::UnknownJob(name.to_string()))?;

        let resolved = validate_params(manifest, &params, artifacts)?;

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
        };

        {
            let mut running_jobs = self.running_jobs.lock().expect("job mutex poisoned");
            enforce_concurrency(manifest, &running_jobs)?;
            running_jobs.insert(
                job_id.clone(),
                RunningJob {
                    job_id: job_id.clone(),
                    name: manifest.name.clone(),
                    concurrency: manifest.concurrency.clone(),
                },
            );
        }

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

        Ok(JobCreated {
            job_id,
            status: JobStatus::Running,
            started_at,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JobCreated {
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
}

#[derive(Debug)]
pub enum JobError {
    CreateDir {
        path: String,
        source: std::io::Error,
    },
    WriteFile {
        path: String,
        source: std::io::Error,
    },
    SerializeMetadata {
        source: serde_json::Error,
    },
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
            Self::WriteFile { path, source } => {
                write!(f, "failed to write job file {path}: {source}")
            }
            Self::SerializeMetadata { source } => {
                write!(f, "failed to serialize job metadata: {source}")
            }
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
            Self::UnknownJob(_) => StatusCode::NOT_FOUND,
            Self::MissingParam(_)
            | Self::UnknownParam(_)
            | Self::InvalidParamType { .. }
            | Self::Artifact(ArtifactError::NotFound(_))
            | Self::Artifact(ArtifactError::MissingChecksum)
            | Self::Artifact(ArtifactError::ChecksumMismatch { .. })
            | Self::Artifact(ArtifactError::TooLarge { .. })
            | Self::Artifact(ArtifactError::InvalidHeader { .. })
            | Self::Artifact(ArtifactError::ParseExpiry { .. })
            | Self::ExpiredArtifact { .. }
            | Self::InvalidBody(_) => StatusCode::BAD_REQUEST,
            Self::ConcurrencyConflict { .. } => StatusCode::CONFLICT,
            Self::Artifact(ArtifactError::ParseMetadata { .. })
            | Self::Artifact(ArtifactError::CreateDir { .. })
            | Self::Artifact(ArtifactError::WriteFile { .. })
            | Self::Artifact(ArtifactError::SerializeMetadata { .. })
            | Self::Artifact(ArtifactError::InvalidMaxSize { .. })
            | Self::Artifact(ArtifactError::InvalidBody(_))
            | Self::Artifact(ArtifactError::TimeOverflow)
            | Self::CreateDir { .. }
            | Self::WriteFile { .. }
            | Self::SerializeMetadata { .. } => StatusCode::INTERNAL_SERVER_ERROR,
        };

        (
            status,
            Json(JobErrorResponse {
                error: self.to_string(),
            }),
        )
            .into_response()
    }
}

#[derive(Debug, Serialize)]
struct JobErrorResponse {
    error: String,
}

pub async fn create_job(
    State(state): State<crate::AppState>,
    AxumPath(name): AxumPath<String>,
    Json(body): Json<Value>,
) -> Result<(StatusCode, Json<JobCreated>), JobError> {
    let params = body
        .as_object()
        .cloned()
        .ok_or(JobError::InvalidBody("expected a JSON object"))?;
    let created = state
        .jobs
        .create_job(&name, params, &state.manifests, &state.artifacts)?;

    Ok((StatusCode::CREATED, Json(created)))
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

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::{Path, PathBuf},
        sync::Arc,
        time::{SystemTime, UNIX_EPOCH},
    };

    use axum::{
        Router,
        body::Body,
        http::{Request, StatusCode},
        routing::post,
    };
    use serde_json::json;
    use sha2::Digest;
    use tower::util::ServiceExt;

    use super::{JobMetadata, JobStore, create_job};
    use crate::{
        AppState,
        artifacts::ArtifactStore,
        config::{ArtifactsConfig, AuthConfig, Config, JobsConfig, ServerConfig},
        manifest::ManifestStore,
    };

    #[tokio::test]
    async fn creates_job_metadata_for_valid_request() {
        let temp = temp_dir("job_create");
        let state = test_state(&temp);
        let app = Router::new()
            .route("/jobs/{name}", post(create_job))
            .with_state(state);

        let response = app
            .oneshot(
                Request::post("/jobs/build-app")
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
        let created: super::JobCreated = serde_json::from_slice(&body).expect("created job body");
        let metadata_path = temp
            .join("jobs")
            .join(&created.job_id)
            .join("metadata.json");
        let metadata: JobMetadata =
            serde_json::from_slice(&fs::read(metadata_path).expect("metadata should be written"))
                .expect("metadata should parse");

        assert_eq!(metadata.name, "build-app");
        assert_eq!(metadata.status, super::JobStatus::Running);
        assert_eq!(metadata.params["commit"], "abc123");
    }

    #[tokio::test]
    async fn rejects_missing_required_param() {
        let temp = temp_dir("job_missing_param");
        let state = test_state(&temp);
        let app = Router::new()
            .route("/jobs/{name}", post(create_job))
            .with_state(state);

        let response = app
            .oneshot(
                Request::post("/jobs/build-app")
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
            .route("/jobs/{name}", post(create_job))
            .with_state(state);

        let response = app
            .oneshot(
                Request::post("/jobs/build-app")
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
            .route("/jobs/{name}", post(create_job))
            .with_state(state);

        let response = app
            .oneshot(
                Request::post("/jobs/build-with-artifact")
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
        let created: super::JobCreated = serde_json::from_slice(&body).expect("created job body");
        let metadata_path = temp
            .join("jobs")
            .join(&created.job_id)
            .join("metadata.json");
        let metadata: JobMetadata =
            serde_json::from_slice(&fs::read(metadata_path).expect("metadata should be written"))
                .expect("metadata should parse");

        assert!(metadata.resolved_artifacts["source"].ends_with("/blob"));
    }

    fn test_state(temp: &Path) -> AppState {
        let manifests_dir = temp.join("manifests");
        let scripts_dir = temp.join("scripts");
        fs::create_dir_all(&manifests_dir).expect("manifests dir should be created");
        fs::create_dir_all(&scripts_dir).expect("scripts dir should be created");
        let script = write_executable_script(&scripts_dir, "build.sh");
        fs::write(
            manifests_dir.join("build-app.toml"),
            format!(
                r#"
name = "build-app"
script = "{}"
timeout_seconds = 600
concurrency = "parallel"

[params.commit]
type = "string"
required = true

[params.branch]
type = "string"
required = true
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

    fn test_state_with_artifact_manifest(temp: &Path) -> AppState {
        let manifests_dir = temp.join("manifests");
        let scripts_dir = temp.join("scripts");
        fs::create_dir_all(&manifests_dir).expect("manifests dir should be created");
        fs::create_dir_all(&scripts_dir).expect("scripts dir should be created");
        let script = write_executable_script(&scripts_dir, "build.sh");
        fs::write(
            manifests_dir.join("build-with-artifact.toml"),
            format!(
                r#"
name = "build-with-artifact"
script = "{}"
timeout_seconds = 600
concurrency = "parallel"

[params.source]
type = "artifact"
required = true
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

    fn build_state(config: Config) -> AppState {
        AppState {
            config: Arc::new(config.clone()),
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

    fn store_artifact(store: &ArtifactStore, bytes: &[u8]) -> String {
        let checksum = hex::encode(sha2::Sha256::digest(bytes));
        store
            .store_bytes(bytes, Some(&checksum))
            .expect("artifact should store")
            .artifact_id
    }

    fn write_executable_script(dir: &Path, name: &str) -> PathBuf {
        let path = dir.join(name);
        fs::write(&path, "#!/bin/sh\nexit 0\n").expect("script should be written");

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
}
