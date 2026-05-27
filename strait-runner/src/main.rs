mod artifacts;
mod auth;
mod config;
mod jobs;
mod manifest;
mod storage;

use std::{env, net::SocketAddr, sync::Arc};

use artifacts::ArtifactStore;
use auth::AuthStore;
use axum::{
    Json, Router,
    extract::State,
    routing::{get, post},
};
use config::Config;
use jobs::JobStore;
use manifest::ManifestStore;
use serde::Serialize;
use tracing::info;

#[derive(Clone)]
pub(crate) struct AppState {
    config: Arc<Config>,
    auth: Arc<AuthStore>,
    manifests: Arc<ManifestStore>,
    artifacts: Arc<ArtifactStore>,
    jobs: Arc<JobStore>,
}

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
    listen: String,
    manifest_count: usize,
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
    let state = AppState {
        config: Arc::clone(&config),
        auth,
        manifests: Arc::clone(&manifests),
        artifacts,
        jobs,
    };
    tokio::spawn(async move {
        artifacts_cleanup
            .run_cleanup_loop(cleanup_interval_seconds)
            .await;
    });
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

    axum::serve(listener, app).await?;

    Ok(())
}

fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "strait_runner=info,axum=info".into()),
        )
        .compact()
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
        .route("/jobs", get(jobs::list_jobs))
        .route("/artifacts", post(artifacts::upload_artifact))
        .route(
            "/artifacts/{artifact_id}",
            get(artifacts::download_artifact),
        )
        .route("/jobs/{name}/runs", post(jobs::create_job))
        .route(
            "/runs/{job_id}",
            get(jobs::get_job).delete(jobs::cancel_job),
        )
        .route("/runs/{job_id}/logs", get(jobs::get_job_logs))
        .with_state(state)
}

async fn health(State(state): State<AppState>) -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok",
        listen: state.config.server.listen.clone(),
        manifest_count: state.manifests.len(),
    })
}
