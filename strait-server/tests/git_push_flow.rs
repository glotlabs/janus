use std::{
    collections::BTreeMap,
    fs,
    net::TcpListener,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{Arc, Mutex},
    time::Duration,
};

use axum::{
    Json, Router,
    body::{Body, to_bytes},
    extract::{Path as AxumPath, State},
    http::StatusCode,
    routing::{get, post},
};
use reqwest::Client;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use strait_lib::{RunnerCapabilitiesResponse, RunnerRouteTemplate};
use tokio::time::sleep;
use uuid::Uuid;

#[tokio::test]
async fn git_push_triggers_pipeline_end_to_end() {
    let temp = temp_dir("git-push-e2e");
    let runner = spawn_mock_runner().await;
    let server_port = free_port();
    let config_path = write_config(&temp, server_port, &runner.base_url);
    let server = ServerProcess::spawn(&config_path);

    wait_for_http(&format!("http://127.0.0.1:{server_port}/health")).await;

    let client = Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("client");
    let login_response = client
        .post(format!("http://127.0.0.1:{server_port}/login"))
        .header("content-type", "application/x-www-form-urlencoded")
        .body("username=admin&password=password123")
        .send()
        .await
        .expect("login request");
    assert_eq!(login_response.status(), StatusCode::SEE_OTHER);
    let session_cookie = login_response
        .headers()
        .get("set-cookie")
        .expect("set-cookie")
        .to_str()
        .expect("cookie string")
        .split(';')
        .next()
        .expect("cookie pair")
        .to_string();

    let session = client
        .get(format!("http://127.0.0.1:{server_port}/api/me"))
        .header("cookie", &session_cookie)
        .send()
        .await
        .expect("session request")
        .json::<Value>()
        .await
        .expect("session json");
    let csrf = session["csrf_token"].as_str().expect("csrf");

    let runner = client
        .post(format!("http://127.0.0.1:{server_port}/api/runners"))
        .header("cookie", &session_cookie)
        .json(&json!({
            "csrf_token": csrf,
            "name": "mock-runner",
            "base_url": runner.base_url
        }))
        .send()
        .await
        .expect("create runner")
        .json::<Value>()
        .await
        .expect("runner json");
    let runner_id = runner["id"].as_str().expect("runner id");

    let repo = client
        .post(format!("http://127.0.0.1:{server_port}/api/repos"))
        .header("cookie", &session_cookie)
        .json(&json!({
            "csrf_token": csrf,
            "name": "demo",
            "default_branch": "main"
        }))
        .send()
        .await
        .expect("create repo")
        .json::<Value>()
        .await
        .expect("repo json");
    let repo_id = repo["id"].as_str().expect("repo id");
    let bare_path = repo["bare_path"].as_str().expect("bare path");

    let _workflow = client
        .post(format!("http://127.0.0.1:{server_port}/api/workflows"))
        .header("cookie", &session_cookie)
        .json(&json!({
            "csrf_token": csrf,
            "repo_id": repo_id,
            "name": "build",
            "enabled": true,
            "trigger_kind": "push",
            "branches": ["main"],
            "jobs": [
                {
                    "runner_id": runner_id,
                    "runner_job_name": "build-app",
                    "inputs": {
                        "commit": { "kind": "commit" },
                        "branch": { "kind": "branch" }
                    },
                    "outcome_policy": "required"
                }
            ]
        }))
        .send()
        .await
        .expect("create workflow");

    let worktree = temp.join("worktree");
    fs::create_dir_all(&worktree).expect("worktree");
    git(&worktree, &["init", "-b", "main"]).expect("git init");
    git(&worktree, &["config", "user.email", "admin@example.test"]).expect("git config email");
    git(&worktree, &["config", "user.name", "Admin"]).expect("git config name");
    fs::write(worktree.join("README.md"), "hello\n").expect("write readme");
    git(&worktree, &["add", "README.md"]).expect("git add");
    git(&worktree, &["commit", "-m", "initial"]).expect("git commit");
    git(&worktree, &["remote", "add", "origin", bare_path]).expect("git remote");
    git(&worktree, &["push", "origin", "main"]).expect("git push");

    let pipeline = wait_for_pipeline_success(&client, server_port, &session_cookie, repo_id).await;
    let detail = client
        .get(format!(
            "http://127.0.0.1:{server_port}/api/pipelines/{}",
            pipeline["id"].as_str().unwrap()
        ))
        .header("cookie", &session_cookie)
        .send()
        .await
        .expect("pipeline detail")
        .json::<Value>()
        .await
        .expect("detail json");
    assert_eq!(detail["pipeline"]["status"], "success");
    assert_eq!(detail["jobs"][0]["run"]["status"], "success");

    drop(server);
}

async fn wait_for_pipeline_success(
    client: &Client,
    server_port: u16,
    cookie: &str,
    repo_id: &str,
) -> Value {
    for _ in 0..60 {
        let pipelines = client
            .get(format!("http://127.0.0.1:{server_port}/api/pipelines"))
            .header("cookie", cookie)
            .send()
            .await
            .expect("pipelines")
            .json::<Vec<Value>>()
            .await
            .expect("pipelines json");
        if let Some(pipeline) = pipelines
            .into_iter()
            .find(|pipeline| pipeline["repo_id"].as_str() == Some(repo_id))
        {
            if pipeline["status"] == "success" {
                return pipeline;
            }
        }
        sleep(Duration::from_millis(250)).await;
    }
    panic!("pipeline did not reach success");
}

fn write_config(temp: &Path, port: u16, runner_base_url: &str) -> PathBuf {
    let config = temp.join("server.toml");
    fs::create_dir_all(temp.join("data")).expect("data dir");
    fs::create_dir_all(temp.join("repos")).expect("repos dir");
    fs::write(
        &config,
        format!(
            r#"data_dir = "{}"
repos_dir = "{}"

[database]
path = "{}"

[server]
listen = "127.0.0.1:{}"
public_base_url = "ci.test"

[auth]
session_secret = "test-secret"
session_ttl_days = 1
session_cookie_secure = false
login_rate_limit_per_minute = 100

[auth.bootstrap_admin]
username = "admin"
password = "password123"

[runner_auth]
key_id = "test-server"
private_key_path = "{}/runner-signing.key"
public_key_path = "{}/runner-signing.pub"

[scheduler]
poll_interval_ms = 100
cancel_stuck_timeout_seconds = 1
max_cancel_retries = 2
max_infra_retries = 2

[runners]
healthcheck_interval_seconds = 3600
"#,
            temp.join("data").display(),
            temp.join("repos").display(),
            temp.join("data/server.sqlite3").display(),
            port,
            temp.join("data").display(),
            temp.join("data").display(),
        ),
    )
    .expect("config");
    let _ = runner_base_url;
    config
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind")
        .local_addr()
        .expect("addr")
        .port()
}

async fn wait_for_http(url: &str) {
    let client = Client::new();
    for _ in 0..80 {
        if client.get(url).send().await.is_ok() {
            return;
        }
        sleep(Duration::from_millis(100)).await;
    }
    panic!("server did not become ready");
}

fn git(workdir: &Path, args: &[&str]) -> Result<(), String> {
    let output = Command::new("git")
        .args(args)
        .current_dir(workdir)
        .output()
        .map_err(|error| error.to_string())?;
    if output.status.success() {
        Ok(())
    } else {
        Err(format!(
            "git {:?} failed: {}",
            args,
            String::from_utf8_lossy(&output.stderr)
        ))
    }
}

struct ServerProcess {
    child: Child,
}

impl ServerProcess {
    fn spawn(config_path: &Path) -> Self {
        let child = Command::new(env!("CARGO_BIN_EXE_strait-server"))
            .args(["serve", "--config"])
            .arg(config_path)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn server");
        Self { child }
    }
}

impl Drop for ServerProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

struct MockRunner {
    base_url: String,
}

struct MockRunnerState {
    runs: Mutex<BTreeMap<String, usize>>,
}

async fn spawn_mock_runner() -> MockRunner {
    let state = Arc::new(MockRunnerState {
        runs: Mutex::new(BTreeMap::new()),
    });
    let app = Router::new()
        .route(
            RunnerRouteTemplate::Capabilities.path(),
            get(mock_capabilities),
        )
        .route(RunnerRouteTemplate::Jobs.path(), get(mock_list_jobs))
        .route(RunnerRouteTemplate::JobRuns.path(), post(mock_create_run))
        .route(RunnerRouteTemplate::Run.path(), get(mock_get_run))
        .route(RunnerRouteTemplate::RunLogs.path(), get(mock_logs))
        .route(
            RunnerRouteTemplate::Artifacts.path(),
            post(mock_artifact_upload),
        )
        .route(
            RunnerRouteTemplate::Artifact.path(),
            get(mock_artifact_download),
        )
        .with_state(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock runner");
    let addr = listener.local_addr().expect("mock addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("mock serve");
    });
    MockRunner {
        base_url: format!("http://{}", addr),
    }
}

async fn mock_capabilities() -> Json<RunnerCapabilitiesResponse> {
    Json(RunnerCapabilitiesResponse::current())
}

async fn mock_list_jobs() -> Json<Value> {
    Json(json!([{
        "name":"build-app",
        "timeout_seconds":60,
        "inputs":{
            "commit":{"type":"string","required":true},
            "branch":{"type":"string","required":true}
        },
        "outputs":{}
    }]))
}

async fn mock_create_run(
    State(state): State<Arc<MockRunnerState>>,
    AxumPath(_name): AxumPath<String>,
) -> (StatusCode, Json<Value>) {
    let job_id = Uuid::now_v7().to_string();
    state.runs.lock().expect("runs").insert(job_id.clone(), 0);
    (
        StatusCode::CREATED,
        Json(json!({"job_id":job_id,"status":"running","started_at":"2026-01-01T00:00:00Z"})),
    )
}

async fn mock_get_run(
    State(state): State<Arc<MockRunnerState>>,
    AxumPath(job_id): AxumPath<String>,
) -> Json<Value> {
    let mut runs = state.runs.lock().expect("runs");
    let count = runs.entry(job_id.clone()).or_insert(0);
    *count += 1;
    if *count >= 2 {
        Json(json!({
            "job_id": job_id,
            "name": "build-app",
            "status": "success",
            "started_at": "2026-01-01T00:00:00Z",
            "finished_at": "2026-01-01T00:00:01Z",
            "duration_ms": 1000,
            "exit_code": 0,
            "terminal_reason": "success",
            "failure_category": null,
            "outputs": {},
            "output_metadata": {
                "stdout": {"bytes": 3, "truncated": false},
                "stderr": {"bytes": 0, "truncated": false},
                "artifacts": {"count": 0, "bytes": 0}
            }
        }))
    } else {
        Json(json!({
            "job_id": job_id,
            "name": "build-app",
            "status": "running",
            "started_at": "2026-01-01T00:00:00Z",
            "finished_at": null,
            "duration_ms": null,
            "exit_code": null,
            "terminal_reason": null,
            "failure_category": null,
            "outputs": {},
            "output_metadata": {
                "stdout": {"bytes": 0, "truncated": false},
                "stderr": {"bytes": 0, "truncated": false},
                "artifacts": {"count": 0, "bytes": 0}
            }
        }))
    }
}

async fn mock_logs() -> Json<Value> {
    Json(json!({"stdout":"ok\n","stderr":""}))
}

async fn mock_artifact_upload(body: Body) -> (StatusCode, Json<Value>) {
    let bytes = to_bytes(body, usize::MAX).await.expect("bytes");
    (
        StatusCode::CREATED,
        Json(json!({
            "artifact_id":"artifact-1",
            "sha256":format!("{:x}", Sha256::digest(&bytes)),
            "size": bytes.len(),
            "expires_at": "2026-01-01T01:00:00Z"
        })),
    )
}

async fn mock_artifact_download() -> Body {
    Body::from("artifact")
}

fn temp_dir(label: &str) -> PathBuf {
    let suffix = Uuid::now_v7().simple().to_string();
    let dir = std::env::temp_dir().join(format!("strait-server-{label}-{suffix}"));
    fs::create_dir_all(&dir).expect("temp dir");
    dir
}
