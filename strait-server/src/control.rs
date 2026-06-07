use std::{io::Cursor, path::Path, sync::Arc};

use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::post,
};
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::UnixStream,
};

use crate::{app::AppState, git};

pub(crate) const DEFAULT_SOCKET_PATH: &str = "/var/run/strait_server/control.sock";

#[derive(Deserialize, Serialize)]
struct ResolveRepoRequest {
    repo: String,
}

#[derive(Serialize, Deserialize)]
struct ResolveRepoResponse {
    bare_path: String,
}

#[derive(Deserialize, Serialize)]
struct PostReceiveRequest {
    repo_id: String,
    refs_raw: String,
}

#[derive(Serialize, Deserialize)]
struct ErrorResponse {
    error: String,
}

pub(crate) fn build_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/git/resolve-repo", post(resolve_repo))
        .route("/git/post-receive", post(post_receive))
        .with_state(state)
}

async fn resolve_repo(
    State(state): State<Arc<AppState>>,
    Json(request): Json<ResolveRepoRequest>,
) -> Response {
    let normalized = match git::validate_repo_name(&request.repo) {
        Ok(repo) => repo,
        Err(error) => return control_error(StatusCode::BAD_REQUEST, error),
    };
    match state.db.get_repo_by_normalized_name(&normalized) {
        Ok(Some(repo)) => Json(ResolveRepoResponse {
            bare_path: repo.bare_path,
        })
        .into_response(),
        Ok(None) => control_error(StatusCode::NOT_FOUND, "repository not found"),
        Err(error) => control_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
    }
}

async fn post_receive(
    State(state): State<Arc<AppState>>,
    Json(request): Json<PostReceiveRequest>,
) -> Response {
    let refs = match git::read_push_refs(&mut Cursor::new(request.refs_raw)) {
        Ok(refs) => refs,
        Err(error) => return control_error(StatusCode::BAD_REQUEST, error.to_string()),
    };
    let event_key = git::event_key(&request.repo_id, &refs);
    match state
        .db
        .create_push_event(&request.repo_id, &event_key, &refs)
    {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => control_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
    }
}

fn control_error(status: StatusCode, error: impl Into<String>) -> Response {
    (
        status,
        Json(ErrorResponse {
            error: error.into(),
        }),
    )
        .into_response()
}

pub(crate) async fn resolve_repo_path(
    socket_path: &Path,
    repo: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let body = serde_json::to_vec(&ResolveRepoRequest {
        repo: repo.to_string(),
    })?;
    let response = request(socket_path, "/git/resolve-repo", &body).await?;
    if !(200..300).contains(&response.status) {
        return Err(response.error_message().into());
    }
    let response: ResolveRepoResponse = serde_json::from_slice(&response.body)?;
    Ok(response.bare_path)
}

pub(crate) async fn send_post_receive(
    socket_path: &Path,
    repo_id: &str,
    refs_raw: String,
) -> Result<(), Box<dyn std::error::Error>> {
    let body = serde_json::to_vec(&PostReceiveRequest {
        repo_id: repo_id.to_string(),
        refs_raw,
    })?;
    let response = request(socket_path, "/git/post-receive", &body).await?;
    if !(200..300).contains(&response.status) {
        return Err(response.error_message().into());
    }
    Ok(())
}

struct ClientResponse {
    status: u16,
    body: Vec<u8>,
}

impl ClientResponse {
    fn error_message(&self) -> String {
        serde_json::from_slice::<ErrorResponse>(&self.body)
            .map(|response| response.error)
            .unwrap_or_else(|_| String::from_utf8_lossy(&self.body).trim().to_string())
    }
}

async fn request(
    socket_path: &Path,
    path: &str,
    body: &[u8],
) -> Result<ClientResponse, Box<dyn std::error::Error>> {
    let mut stream = UnixStream::connect(socket_path).await?;
    let header = format!(
        "POST {path} HTTP/1.1\r\nHost: strait-control\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(header.as_bytes()).await?;
    stream.write_all(body).await?;

    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await?;
    parse_http_response(&raw)
}

fn parse_http_response(raw: &[u8]) -> Result<ClientResponse, Box<dyn std::error::Error>> {
    let separator = raw
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .ok_or("malformed control response")?;
    let (head, body) = raw.split_at(separator + 4);
    let head = std::str::from_utf8(head)?;
    let status = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .ok_or("malformed control response status")?
        .parse()?;
    Ok(ClientResponse {
        status,
        body: body.to_vec(),
    })
}
