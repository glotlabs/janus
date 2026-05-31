mod artifacts;
mod auth;
mod config;
mod jobs;
mod manifest;
mod rate_limit;
mod storage;

use std::{env, net::SocketAddr, sync::Arc};

use artifacts::ArtifactStore;
use auth::AuthStore;
use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    routing::{get, post},
};
use chrono::{SecondsFormat, Utc};
use config::Config;
use jobs::JobStore;
use manifest::ManifestStore;
use rate_limit::RateLimiter;
use serde::Serialize;
use strait_lib::{RunnerCapabilitiesResponse, RunnerRouteTemplate};
use tokio::time::{self, MissedTickBehavior};
use tracing::{info, warn};

const SHUTDOWN_DRAIN_TIMEOUT_SECONDS: u64 = 10;

#[derive(Clone)]
pub(crate) struct AppState {
    config: Arc<Config>,
    auth: Arc<AuthStore>,
    manifests: Arc<ManifestStore>,
    artifacts: Arc<ArtifactStore>,
    jobs: Arc<JobStore>,
    rate_limiter: Arc<RateLimiter>,
    runtime_status: Arc<RuntimeStatus>,
}

#[derive(Serialize)]
struct HealthResponse {
    status: String,
    listen: String,
    manifest_count: usize,
}

#[derive(Clone, Serialize)]
struct StartupValidationStatus {
    completed: bool,
    recovered_artifacts: usize,
    recovered_jobs: usize,
}

#[derive(Clone, Serialize)]
struct BackgroundTaskStatus {
    name: &'static str,
    running: bool,
    last_success_at: Option<String>,
    last_error: Option<String>,
}

#[derive(Clone, Serialize)]
struct ReadinessResponse {
    status: String,
    listen: String,
    manifest_count: usize,
    startup: StartupValidationStatus,
    background_tasks: Vec<BackgroundTaskStatus>,
}

struct RuntimeStatus {
    startup: StartupValidationStatus,
    artifact_cleanup: std::sync::Mutex<BackgroundTaskHealth>,
}

struct BackgroundTaskHealth {
    running: bool,
    last_success_at: Option<String>,
    last_error: Option<String>,
}

impl RuntimeStatus {
    fn new(recovered_artifacts: usize, recovered_jobs: usize) -> Self {
        Self {
            startup: StartupValidationStatus {
                completed: true,
                recovered_artifacts,
                recovered_jobs,
            },
            artifact_cleanup: std::sync::Mutex::new(BackgroundTaskHealth {
                running: true,
                last_success_at: None,
                last_error: None,
            }),
        }
    }

    fn record_artifact_cleanup_success(&self) {
        let mut status = self
            .artifact_cleanup
            .lock()
            .expect("artifact cleanup mutex poisoned");
        status.last_success_at = Some(Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true));
        status.last_error = None;
    }

    fn record_artifact_cleanup_error(&self, error: &str) {
        let mut status = self
            .artifact_cleanup
            .lock()
            .expect("artifact cleanup mutex poisoned");
        status.last_error = Some(error.to_string());
    }

    fn mark_artifact_cleanup_stopped(&self) {
        let mut status = self
            .artifact_cleanup
            .lock()
            .expect("artifact cleanup mutex poisoned");
        status.running = false;
    }

    fn artifact_cleanup_status(&self) -> BackgroundTaskStatus {
        let status = self
            .artifact_cleanup
            .lock()
            .expect("artifact cleanup mutex poisoned");
        BackgroundTaskStatus {
            name: "artifact_cleanup",
            running: status.running,
            last_success_at: status.last_success_at.clone(),
            last_error: status.last_error.clone(),
        }
    }

    fn is_ready(&self) -> bool {
        let artifact_cleanup = self
            .artifact_cleanup
            .lock()
            .expect("artifact cleanup mutex poisoned");
        self.startup.completed && artifact_cleanup.running && artifact_cleanup.last_error.is_none()
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_tracing();

    let config_path = config_path();
    let config = Arc::new(Config::load_from_path(&config_path)?);
    let auth = Arc::new(AuthStore::load_from_config(&config.auth, |name| {
        env::var(name).ok()
    })?);
    let manifests = Arc::new(ManifestStore::load_from_dir(&config.manifests_dir)?);
    let artifacts = Arc::new(ArtifactStore::new(
        &config.data_dir,
        config.artifacts.ttl_seconds,
        config.artifacts.max_size_mb,
        config.artifacts.require_checksum_on_upload,
    )?);
    let recovered_artifacts = artifacts.recover_incomplete_artifacts()?;
    let artifacts_cleanup = Arc::clone(&artifacts);
    let cleanup_interval_seconds = config.artifacts.cleanup_interval_seconds;
    let jobs = Arc::new(JobStore::new(&config.data_dir)?);
    let recovered_jobs = jobs.recover_interrupted_jobs()?;
    let runtime_status = Arc::new(RuntimeStatus::new(recovered_artifacts, recovered_jobs));
    let state = AppState {
        config: Arc::clone(&config),
        auth,
        manifests: Arc::clone(&manifests),
        artifacts,
        jobs,
        rate_limiter: Arc::new(RateLimiter::new()),
        runtime_status: Arc::clone(&runtime_status),
    };
    let cleanup_runtime_status = Arc::clone(&runtime_status);
    let artifacts_cleanup_task = tokio::spawn(async move {
        run_artifact_cleanup_loop(
            artifacts_cleanup,
            cleanup_interval_seconds,
            cleanup_runtime_status,
        )
        .await;
    });
    let jobs_for_shutdown = Arc::clone(&state.jobs);
    let app = build_app(state);

    let address: SocketAddr = config.server.listen.parse()?;
    let listener = tokio::net::TcpListener::bind(address).await?;

    info!(
        listen = %config.server.listen,
        config_path = %config_path,
        manifest_count = manifests.len(),
        recovered_artifacts,
        recovered_jobs,
        "strait-runner listening"
    );

    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            shutdown_signal().await;

            let canceled = jobs_for_shutdown.begin_shutdown();
            info!(canceled, "shutdown started; canceling active jobs");

            let drained = jobs_for_shutdown
                .wait_for_drain(std::time::Duration::from_secs(
                    SHUTDOWN_DRAIN_TIMEOUT_SECONDS,
                ))
                .await;
            if !drained {
                warn!(
                    timeout_seconds = SHUTDOWN_DRAIN_TIMEOUT_SECONDS,
                    "shutdown drain timed out with jobs still active"
                );
            }
        })
        .await?;
    runtime_status.mark_artifact_cleanup_stopped();
    artifacts_cleanup_task.abort();

    Ok(())
}

fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "strait_runner=info,axum=info".into()),
        )
        .json()
        .flatten_event(true)
        .init();
}

fn config_path() -> String {
    env::args()
        .nth(1)
        .or_else(|| env::var("STRAIT_RUNNER_CONFIG").ok())
        .unwrap_or_else(|| "runner.toml".to_string())
}

fn build_app(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/ready", get(readiness))
        .route("/readiness", get(readiness))
        .route(RunnerRouteTemplate::Capabilities.path(), get(capabilities))
        .route(RunnerRouteTemplate::Jobs.path(), get(jobs::list_jobs))
        .route(
            RunnerRouteTemplate::Artifacts.path(),
            post(artifacts::upload_artifact),
        )
        .route(
            RunnerRouteTemplate::Artifact.path(),
            get(artifacts::download_artifact),
        )
        .route(RunnerRouteTemplate::JobRuns.path(), post(jobs::create_job))
        .route(
            RunnerRouteTemplate::Run.path(),
            get(jobs::get_job).delete(jobs::cancel_job),
        )
        .route(RunnerRouteTemplate::RunLogs.path(), get(jobs::get_job_logs))
        .with_state(state)
}

async fn capabilities() -> Json<RunnerCapabilitiesResponse> {
    Json(RunnerCapabilitiesResponse::current())
}

async fn health(State(state): State<AppState>) -> Json<HealthResponse> {
    Json(HealthResponse {
        status: if state.jobs.is_shutting_down() {
            "shutting_down".to_string()
        } else {
            "ok".to_string()
        },
        listen: state.config.server.listen.clone(),
        manifest_count: state.manifests.len(),
    })
}

async fn readiness(State(state): State<AppState>) -> (StatusCode, Json<ReadinessResponse>) {
    let ready = !state.jobs.is_shutting_down() && state.runtime_status.is_ready();
    let status = if ready { "ready" } else { "not_ready" };

    (
        if ready {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        },
        Json(ReadinessResponse {
            status: status.to_string(),
            listen: state.config.server.listen.clone(),
            manifest_count: state.manifests.len(),
            startup: state.runtime_status.startup.clone(),
            background_tasks: vec![state.runtime_status.artifact_cleanup_status()],
        }),
    )
}

async fn run_artifact_cleanup_loop(
    artifacts: Arc<ArtifactStore>,
    cleanup_interval_seconds: u64,
    runtime_status: Arc<RuntimeStatus>,
) {
    let mut interval = time::interval(std::time::Duration::from_secs(
        cleanup_interval_seconds.max(1),
    ));
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        interval.tick().await;
        match artifacts.cleanup_expired() {
            Ok(removed) => {
                runtime_status.record_artifact_cleanup_success();
                if removed > 0 {
                    info!(removed, "cleaned expired artifacts");
                }
            }
            Err(error) => {
                runtime_status.record_artifact_cleanup_error(&error.to_string());
                warn!(%error, "artifact cleanup failed");
            }
        }
    }
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        let mut terminate =
            signal(SignalKind::terminate()).expect("failed to install SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {},
            _ = terminate.recv() => {},
        }
    }

    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
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
        body::Body,
        http::{Request, StatusCode},
    };
    use sha2::{Digest, Sha256};
    use strait_lib::{RUNNER_PROTOCOL_VERSION, RunnerRoute};
    use tower::util::ServiceExt;

    use super::{AppState, RuntimeStatus, build_app};
    use crate::{
        artifacts::ArtifactStore,
        auth::AuthStore,
        config::{ArtifactsConfig, AuthConfig, AuthTokenConfig, Config, JobsConfig, ServerConfig},
        jobs::JobStore,
        manifest::ManifestStore,
    };

    #[tokio::test]
    async fn health_reports_shutting_down_state() {
        let temp = temp_dir("health_shutdown");
        let state = test_state(&temp);
        state.jobs.begin_shutdown();
        let app = build_app(state);

        let response = app
            .oneshot(
                Request::get("/health")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("response body");
        let payload: serde_json::Value = serde_json::from_slice(&body).expect("health json");
        assert_eq!(payload["status"], "shutting_down");
    }

    #[tokio::test]
    async fn capabilities_reports_runner_protocol_versions() {
        let temp = temp_dir("capabilities");
        let state = test_state(&temp);
        let app = build_app(state);

        let response = app
            .oneshot(
                Request::get(RunnerRoute::Capabilities.path())
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("response body");
        let payload: strait_lib::RunnerCapabilitiesResponse =
            serde_json::from_slice(&body).expect("capabilities json");
        assert_eq!(payload.protocol_version, RUNNER_PROTOCOL_VERSION);
        assert!(
            payload
                .supported_protocol_versions
                .contains(&RUNNER_PROTOCOL_VERSION)
        );
    }

    #[tokio::test]
    async fn readiness_reports_ready_after_startup_validation() {
        let temp = temp_dir("readiness_ready");
        let state = test_state(&temp);
        state.runtime_status.record_artifact_cleanup_success();
        let app = build_app(state);

        let response = app
            .oneshot(
                Request::get("/ready")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("response body");
        let payload: serde_json::Value = serde_json::from_slice(&body).expect("readiness json");
        assert_eq!(payload["status"], "ready");
        assert_eq!(payload["startup"]["completed"], true);
        assert_eq!(payload["background_tasks"][0]["name"], "artifact_cleanup");
        assert_eq!(payload["background_tasks"][0]["running"], true);
        assert!(payload["background_tasks"][0]["last_success_at"].is_string());
        assert!(payload["background_tasks"][0]["last_error"].is_null());
    }

    #[tokio::test]
    async fn readiness_reports_background_task_failure() {
        let temp = temp_dir("readiness_background_failure");
        let state = test_state(&temp);
        state
            .runtime_status
            .record_artifact_cleanup_error("disk unavailable");
        let app = build_app(state);

        let response = app
            .oneshot(
                Request::get("/readiness")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("response body");
        let payload: serde_json::Value = serde_json::from_slice(&body).expect("readiness json");
        assert_eq!(payload["status"], "not_ready");
        assert_eq!(
            payload["background_tasks"][0]["last_error"],
            "disk unavailable"
        );
    }

    #[tokio::test]
    async fn readiness_reports_not_ready_while_shutting_down() {
        let temp = temp_dir("readiness_shutdown");
        let state = test_state(&temp);
        state.runtime_status.record_artifact_cleanup_success();
        state.jobs.begin_shutdown();
        let app = build_app(state);

        let response = app
            .oneshot(
                Request::get("/ready")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("response body");
        let payload: serde_json::Value = serde_json::from_slice(&body).expect("readiness json");
        assert_eq!(payload["status"], "not_ready");
    }

    #[tokio::test]
    async fn protected_routes_enforce_permissions() {
        let temp = temp_dir("route_auth_matrix");
        let state = test_state(&temp);
        let artifact_id = store_artifact(&state.artifacts, b"artifact-body");
        let app = build_app(state);

        let cases = vec![
            (
                Request::get("/jobs")
                    .header("authorization", "Bearer jobs-run-token")
                    .body(Body::empty())
                    .expect("request should build"),
                StatusCode::FORBIDDEN,
            ),
            (
                Request::post("/jobs/build-app/runs")
                    .header("authorization", "Bearer jobs-read-token")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "commit": "abc123",
                            "branch": "main"
                        })
                        .to_string(),
                    ))
                    .expect("request should build"),
                StatusCode::FORBIDDEN,
            ),
            (
                Request::get("/runs/job_missing")
                    .header("authorization", "Bearer logs-read-token")
                    .body(Body::empty())
                    .expect("request should build"),
                StatusCode::FORBIDDEN,
            ),
            (
                Request::delete("/runs/job_missing")
                    .header("authorization", "Bearer jobs-read-token")
                    .body(Body::empty())
                    .expect("request should build"),
                StatusCode::FORBIDDEN,
            ),
            (
                Request::get("/runs/job_missing/logs")
                    .header("authorization", "Bearer jobs-read-token")
                    .body(Body::empty())
                    .expect("request should build"),
                StatusCode::FORBIDDEN,
            ),
            (
                Request::post("/artifacts")
                    .header("authorization", "Bearer artifacts-read-token")
                    .body(Body::from("body"))
                    .expect("request should build"),
                StatusCode::FORBIDDEN,
            ),
            (
                Request::get(format!("/artifacts/{artifact_id}"))
                    .header("authorization", "Bearer jobs-read-token")
                    .body(Body::empty())
                    .expect("request should build"),
                StatusCode::FORBIDDEN,
            ),
            (
                Request::get("/jobs")
                    .header("authorization", "Bearer wrong-token")
                    .body(Body::empty())
                    .expect("request should build"),
                StatusCode::UNAUTHORIZED,
            ),
        ];

        for (request, expected_status) in cases {
            let response = app
                .clone()
                .oneshot(request)
                .await
                .expect("request should succeed");
            assert_eq!(response.status(), expected_status);
        }
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

[inputs.commit]
type = "string"
required = true

[inputs.branch]
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
                max_upload_requests_per_minute: 60,
            },
            jobs: JobsConfig {
                default_log_limit_mb: 50,
                max_request_body_kb: 64,
                max_run_requests_per_minute: 60,
                cleanup_successful_workdirs: true,
                keep_failed_workdirs: true,
            },
        };

        AppState {
            config: Arc::new(config.clone()),
            auth: Arc::new(
                AuthStore::load_from_config(
                    &AuthConfig {
                        mode: "bearer".to_string(),
                        tokens: vec![
                            AuthTokenConfig {
                                name: "jobs-run".to_string(),
                                token_env: "TOKEN_JOBS_RUN".to_string(),
                                permissions: vec!["jobs:run".to_string()],
                            },
                            AuthTokenConfig {
                                name: "jobs-read".to_string(),
                                token_env: "TOKEN_JOBS_READ".to_string(),
                                permissions: vec!["jobs:read".to_string()],
                            },
                            AuthTokenConfig {
                                name: "logs-read".to_string(),
                                token_env: "TOKEN_LOGS_READ".to_string(),
                                permissions: vec!["logs:read".to_string()],
                            },
                            AuthTokenConfig {
                                name: "artifacts-write".to_string(),
                                token_env: "TOKEN_ARTIFACTS_WRITE".to_string(),
                                permissions: vec!["artifacts:write".to_string()],
                            },
                            AuthTokenConfig {
                                name: "artifacts-read".to_string(),
                                token_env: "TOKEN_ARTIFACTS_READ".to_string(),
                                permissions: vec!["artifacts:read".to_string()],
                            },
                        ],
                    },
                    |name| match name {
                        "TOKEN_JOBS_RUN" => Some("jobs-run-token".to_string()),
                        "TOKEN_JOBS_READ" => Some("jobs-read-token".to_string()),
                        "TOKEN_LOGS_READ" => Some("logs-read-token".to_string()),
                        "TOKEN_ARTIFACTS_WRITE" => Some("artifacts-write-token".to_string()),
                        "TOKEN_ARTIFACTS_READ" => Some("artifacts-read-token".to_string()),
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
            rate_limiter: Arc::new(crate::rate_limit::RateLimiter::new()),
            runtime_status: Arc::new(RuntimeStatus::new(0, 0)),
        }
    }

    fn store_artifact(store: &ArtifactStore, bytes: &[u8]) -> String {
        let checksum = hex::encode(Sha256::digest(bytes));
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
        let path = std::env::temp_dir().join(format!("strait-runner-main-{label}-{unique}"));
        fs::create_dir_all(&path).expect("temp dir should be created");
        path
    }
}
