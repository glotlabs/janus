mod config;

use std::{env, net::SocketAddr, sync::Arc};

use axum::{Json, Router, extract::State, routing::get};
use config::Config;
use serde::Serialize;
use tracing::info;

#[derive(Clone)]
struct AppState {
    config: Arc<Config>,
}

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
    listen: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_tracing();

    let config_path = config_path();
    let config = Arc::new(Config::load_from_path(&config_path)?);
    let state = AppState {
        config: Arc::clone(&config),
    };
    let app = build_app(state);

    let address: SocketAddr = config.server.listen.parse()?;
    let listener = tokio::net::TcpListener::bind(address).await?;

    info!(
        listen = %config.server.listen,
        config_path = %config_path,
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
        .with_state(state)
}

async fn health(State(state): State<AppState>) -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok",
        listen: state.config.server.listen.clone(),
    })
}
