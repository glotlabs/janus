use super::routes::{build_router, csrf_token};
use crate::{app::build_state, auth::{hash_password, session_cookie}, git, models::{Repo, User, WorkflowDefinition, WorkflowJobDefinition, WorkflowTrigger}, scheduler};
use axum::{
    Json, Router,
    body::{Body, to_bytes},
    extract::{Path as AxumPath, State},
    http::{Request, StatusCode},
    routing::{get, post},
};
use chrono::{Duration, Utc};
use serde_json::{Value as JsonValue, json};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::time::sleep;
use tower::util::ServiceExt;
use uuid::Uuid;

#[tokio::test]
async fn repo_creation_installs_hook() {
    let fixture = test_fixture().await;
    let user = fixture.user.clone();
    let token = csrf_token(&fixture.state, &user);
    let cookie = session_cookie_value(&fixture.state, &user.id);
    let response = fixture
        .app
        .clone()
        .oneshot(
            Request::post("/repos")
                .header("content-type", "application/x-www-form-urlencoded")
                .header("cookie", cookie)
                .body(Body::from(format!(
                    "csrf_token={}&owner_id={}&name=demo&default_branch=main",
                    token, user.id
                )))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::SEE_OTHER);
    let repos = fixture.state.db.list_repos().expect("repos");
    let repo = repos.iter().find(|repo| repo.name == "demo").expect("repo");
    let hook = fs::read_to_string(PathBuf::from(&repo.bare_path).join("hooks/post-receive"))
        .expect("hook");
    assert!(hook.contains("hook post-receive"));
    assert!(hook.contains(&repo.id));
}

#[test]
fn hook_ingestion_is_idempotent() {
    let dir = temp_dir("hook_idempotent");
    let config_path = write_test_config(&dir);
    let state = build_state(config_path, PathBuf::from("/bin/strait-server")).expect("state");
    let hash = hash_password("password123").expect("hash");
    state.db.create_user("alice", &hash, "developer").expect("user");
    let user = state.db.get_user_credentials("alice").expect("user").unwrap().0;
    let repo_id = state
        .db
        .create_repo(
            &user.id,
            "demo",
            "demo",
            &dir.join("repos/demo.git").display().to_string(),
            "main",
        )
        .expect("repo");
    let refs = vec![crate::models::PushEventRef {
        old_rev: "0".repeat(40),
        new_rev: "1".repeat(40),
        ref_name: "refs/heads/main".to_string(),
    }];
    let key = git::event_key(&repo_id, &refs);
    state.db.create_push_event(&repo_id, &key, &refs).expect("push1");
    state.db.create_push_event(&repo_id, &key, &refs).expect("push2");
    let events = state.db.list_unprocessed_push_events().expect("events");
    assert_eq!(events.len(), 1);
}

#[tokio::test]
async fn push_event_creates_pipeline_and_dispatches_job() {
    let mock = spawn_mock_runner().await;
    let fixture = test_fixture_with_runner(&mock.base_url).await;
    let repo = create_repo_direct(&fixture.state, &fixture.user, "demo");
    create_workflow_direct(&fixture.state, &repo.id, &fixture.runner_id);
    fixture
        .state
        .db
        .create_push_event(
            &repo.id,
            "event-1",
            &[crate::models::PushEventRef {
                old_rev: "0".repeat(40),
                new_rev: "1".repeat(40),
                ref_name: "refs/heads/main".to_string(),
            }],
        )
        .expect("push event");
    scheduler::reconcile_once(Arc::clone(&fixture.state))
        .await
        .expect("reconcile");
    let pipelines = fixture.state.db.list_pipeline_runs().expect("pipelines");
    assert_eq!(pipelines.len(), 1);
    let snapshot = fixture
        .state
        .db
        .pipeline_snapshot(&pipelines[0].id)
        .expect("snapshot")
        .unwrap();
    assert_eq!(snapshot.jobs.len(), 1);
    assert_eq!(snapshot.jobs[0].run.status, "running");
}

#[tokio::test]
async fn scheduler_persists_terminal_runner_state() {
    let mock = spawn_mock_runner().await;
    let fixture = test_fixture_with_runner(&mock.base_url).await;
    let repo = create_repo_direct(&fixture.state, &fixture.user, "demo");
    create_workflow_direct(&fixture.state, &repo.id, &fixture.runner_id);
    fixture
        .state
        .db
        .create_push_event(
            &repo.id,
            "event-2",
            &[crate::models::PushEventRef {
                old_rev: "0".repeat(40),
                new_rev: "1".repeat(40),
                ref_name: "refs/heads/main".to_string(),
            }],
        )
        .expect("push event");
    scheduler::reconcile_once(Arc::clone(&fixture.state))
        .await
        .expect("reconcile1");
    scheduler::reconcile_once(Arc::clone(&fixture.state))
        .await
        .expect("reconcile2");
    let pipeline = fixture
        .state
        .db
        .list_pipeline_runs()
        .expect("pipelines")
        .remove(0);
    let snapshot = fixture
        .state
        .db
        .pipeline_snapshot(&pipeline.id)
        .expect("snapshot")
        .unwrap();
    assert_eq!(snapshot.pipeline.status, "success");
    assert_eq!(snapshot.jobs[0].run.status, "success");
    assert!(snapshot.jobs[0].stdout.contains("ok"));
}

#[tokio::test]
async fn scheduler_reuses_dispatch_key_after_ambiguous_runner_create_failure() {
    let mock = spawn_mock_runner_with_fail_first_dispatch().await;
    let fixture = test_fixture_with_runner(&mock.base_url).await;
    let repo = create_repo_direct(&fixture.state, &fixture.user, "demo");
    create_workflow_direct(&fixture.state, &repo.id, &fixture.runner_id);
    let workflow = fixture
        .state
        .db
        .workflows_for_repo(&repo.id)
        .expect("workflows")
        .remove(0);
    let commit_sha = "1".repeat(40);
    let pipeline_id = scheduler::enqueue_workflow_run(
        Arc::clone(&fixture.state),
        &workflow,
        "push",
        Some("refs/heads/main"),
        Some(&commit_sha),
    )
    .expect("enqueue");

    scheduler::reconcile_once(Arc::clone(&fixture.state))
        .await
        .expect_err("first dispatch should fail ambiguously");
    let pipeline = fixture
        .state
        .db
        .get_pipeline_run(&pipeline_id)
        .expect("pipeline")
        .expect("pipeline should exist");
    let snapshot = fixture
        .state
        .db
        .pipeline_snapshot(&pipeline.id)
        .expect("snapshot")
        .unwrap();
    assert_eq!(snapshot.jobs[0].run.status, "pending");
    assert_eq!(mock.dispatch_count(), 1);

    scheduler::reconcile_once(Arc::clone(&fixture.state))
        .await
        .expect("reconcile2");
    scheduler::reconcile_once(Arc::clone(&fixture.state))
        .await
        .expect("reconcile3");
    scheduler::reconcile_once(Arc::clone(&fixture.state))
        .await
        .expect("reconcile4");

    let snapshot = fixture
        .state
        .db
        .pipeline_snapshot(&pipeline.id)
        .expect("snapshot")
        .unwrap();
    assert_eq!(snapshot.pipeline.status, "success");
    assert_eq!(snapshot.jobs[0].run.status, "success");
    assert_eq!(mock.dispatch_count(), 1);
}

#[tokio::test]
async fn cancel_pipeline_tracks_runner_cancel_progress() {
    let mock = spawn_mock_runner().await;
    let fixture = test_fixture_with_runner(&mock.base_url).await;
    let repo = create_repo_direct(&fixture.state, &fixture.user, "demo");
    create_workflow_direct(&fixture.state, &repo.id, &fixture.runner_id);
    fixture
        .state
        .db
        .create_push_event(
            &repo.id,
            "event-cancel",
            &[crate::models::PushEventRef {
                old_rev: "0".repeat(40),
                new_rev: "1".repeat(40),
                ref_name: "refs/heads/main".to_string(),
            }],
        )
        .expect("push event");

    scheduler::reconcile_once(Arc::clone(&fixture.state))
        .await
        .expect("dispatch");
    let pipeline = fixture
        .state
        .db
        .list_pipeline_runs()
        .expect("pipelines")
        .remove(0);

    scheduler::cancel_pipeline(Arc::clone(&fixture.state), &pipeline.id)
        .await
        .expect("cancel");
    let snapshot = fixture
        .state
        .db
        .pipeline_snapshot(&pipeline.id)
        .expect("snapshot")
        .unwrap();
    assert_eq!(snapshot.pipeline.status, "cancel_requested");
    assert_eq!(snapshot.pipeline.cancel_reason.as_deref(), Some("user_requested"));
    assert_eq!(snapshot.jobs[0].run.status, "cancel_requested");
    assert_eq!(snapshot.jobs[0].run.cancel_reason.as_deref(), Some("user_requested"));

    scheduler::reconcile_once(Arc::clone(&fixture.state))
        .await
        .expect("poll cancel requested");
    let snapshot = fixture
        .state
        .db
        .pipeline_snapshot(&pipeline.id)
        .expect("snapshot")
        .unwrap();
    assert_eq!(snapshot.pipeline.status, "cancel_requested");
    assert_eq!(snapshot.jobs[0].run.status, "cancel_requested");

    scheduler::reconcile_once(Arc::clone(&fixture.state))
        .await
        .expect("poll canceling");
    let snapshot = fixture
        .state
        .db
        .pipeline_snapshot(&pipeline.id)
        .expect("snapshot")
        .unwrap();
    assert_eq!(snapshot.pipeline.status, "canceling");
    assert_eq!(snapshot.jobs[0].run.status, "canceling");

    scheduler::reconcile_once(Arc::clone(&fixture.state))
        .await
        .expect("poll canceled");
    let snapshot = fixture
        .state
        .db
        .pipeline_snapshot(&pipeline.id)
        .expect("snapshot")
        .unwrap();
    assert_eq!(snapshot.pipeline.status, "canceled");
    assert_eq!(snapshot.jobs[0].run.status, "canceled");
}

#[tokio::test]
async fn scheduler_retries_stuck_cancellation() {
    let mock = spawn_mock_runner_with_stuck_cancellation().await;
    let fixture = test_fixture_with_runner(&mock.base_url).await;
    let repo = create_repo_direct(&fixture.state, &fixture.user, "demo");
    create_workflow_direct(&fixture.state, &repo.id, &fixture.runner_id);
    fixture
        .state
        .db
        .create_push_event(
            &repo.id,
            "event-stuck-cancel",
            &[crate::models::PushEventRef {
                old_rev: "0".repeat(40),
                new_rev: "1".repeat(40),
                ref_name: "refs/heads/main".to_string(),
            }],
        )
        .expect("push event");

    scheduler::reconcile_once(Arc::clone(&fixture.state))
        .await
        .expect("dispatch");
    let pipeline = fixture
        .state
        .db
        .list_pipeline_runs()
        .expect("pipelines")
        .remove(0);

    scheduler::cancel_pipeline(Arc::clone(&fixture.state), &pipeline.id)
        .await
        .expect("cancel");
    let snapshot = fixture
        .state
        .db
        .pipeline_snapshot(&pipeline.id)
        .expect("snapshot")
        .unwrap();
    let runner_run_id = snapshot.jobs[0]
        .run
        .runner_run_id
        .clone()
        .expect("runner run id");
    assert_eq!(mock.cancel_count(&runner_run_id), 1);
    assert_eq!(snapshot.jobs[0].run.cancel_retry_count, 0);

    sleep(std::time::Duration::from_millis(1100)).await;
    scheduler::reconcile_once(Arc::clone(&fixture.state))
        .await
        .expect("reconcile stuck cancel");

    let snapshot = fixture
        .state
        .db
        .pipeline_snapshot(&pipeline.id)
        .expect("snapshot")
        .unwrap();
    assert_eq!(snapshot.pipeline.status, "cancel_requested");
    assert_eq!(snapshot.jobs[0].run.status, "cancel_requested");
    assert_eq!(snapshot.jobs[0].run.cancel_retry_count, 1);
    assert!(snapshot.jobs[0].run.last_cancel_retry_at.is_some());
    assert!(mock.cancel_count(&runner_run_id) >= 2);
}

#[tokio::test]
async fn scheduler_fails_job_after_cancel_retry_budget_exhausted() {
    let mock = spawn_mock_runner_with_stuck_cancellation().await;
    let fixture = test_fixture_with_runner(&mock.base_url).await;
    let repo = create_repo_direct(&fixture.state, &fixture.user, "demo");
    create_workflow_direct(&fixture.state, &repo.id, &fixture.runner_id);
    fixture
        .state
        .db
        .create_push_event(
            &repo.id,
            "event-exhaust-cancel",
            &[crate::models::PushEventRef {
                old_rev: "0".repeat(40),
                new_rev: "1".repeat(40),
                ref_name: "refs/heads/main".to_string(),
            }],
        )
        .expect("push event");

    scheduler::reconcile_once(Arc::clone(&fixture.state))
        .await
        .expect("dispatch");
    let pipeline = fixture
        .state
        .db
        .list_pipeline_runs()
        .expect("pipelines")
        .remove(0);

    scheduler::cancel_pipeline(Arc::clone(&fixture.state), &pipeline.id)
        .await
        .expect("cancel");

    sleep(std::time::Duration::from_millis(1100)).await;
    scheduler::reconcile_once(Arc::clone(&fixture.state))
        .await
        .expect("retry 1");
    sleep(std::time::Duration::from_millis(1100)).await;
    scheduler::reconcile_once(Arc::clone(&fixture.state))
        .await
        .expect("retry 2");
    sleep(std::time::Duration::from_millis(1100)).await;
    scheduler::reconcile_once(Arc::clone(&fixture.state))
        .await
        .expect("exhaust");

    let snapshot = fixture
        .state
        .db
        .pipeline_snapshot(&pipeline.id)
        .expect("snapshot")
        .unwrap();
    assert_eq!(snapshot.pipeline.status, "failed");
    assert_eq!(snapshot.jobs[0].run.status, "failed");
    assert_eq!(
        snapshot.jobs[0].run.cancel_reason.as_deref(),
        Some("stuck_retry_exhausted")
    );
    assert_eq!(snapshot.jobs[0].run.cancel_retry_count, 2);
}

struct TestFixture {
    state: Arc<crate::app::AppState>,
    app: Router,
    user: User,
    runner_id: String,
}

async fn test_fixture() -> TestFixture {
    test_fixture_with_runner("http://127.0.0.1:9").await
}

async fn test_fixture_with_runner(base_url: &str) -> TestFixture {
    let dir = temp_dir("fixture");
    let config_path = write_test_config(&dir);
    let state = build_state(config_path, PathBuf::from("/bin/strait-server")).expect("state");
    let hash = hash_password("password123").expect("hash");
    let username = format!("alice-{}", Uuid::now_v7());
    state
        .db
        .create_user(&username, &hash, "developer")
        .expect("user");
    let user = state
        .db
        .get_user_credentials(&username)
        .expect("creds")
        .unwrap()
        .0;
    let runner_name = format!("runner-{}", Uuid::now_v7());
    let runner_id = state
        .db
        .create_runner(&runner_name, base_url, "token")
        .expect("runner");
    if base_url != "http://127.0.0.1:9" {
        state
            .db
            .replace_runner_jobs(
                &runner_id,
                &[(
                    "build-app".to_string(),
                    r#"{"name":"build-app","timeout_seconds":60}"#.to_string(),
                )],
            )
            .expect("runner jobs");
        state.db.update_runner_health(&runner_id, "healthy").expect("health");
    }
    let session_id = state
        .db
        .create_session(&user.id, &(Utc::now() + Duration::days(1)).to_rfc3339())
        .expect("session");
    let app = build_router(Arc::clone(&state));
    let _cookie = session_cookie(
        &state.config.auth.session_secret,
        &session_id,
        state.config.auth.session_cookie_secure,
    );
    TestFixture {
        state,
        app,
        user,
        runner_id,
    }
}

fn create_repo_direct(state: &Arc<crate::app::AppState>, user: &User, name: &str) -> Repo {
    let path = PathBuf::from(&state.config.repos_dir).join(format!("{name}.git"));
    let repo_id = state
        .db
        .create_repo(&user.id, name, name, &path.display().to_string(), "main")
        .expect("repo");
    state.db.get_repo(&repo_id).expect("repo").unwrap()
}

fn create_workflow_direct(state: &Arc<crate::app::AppState>, repo_id: &str, runner_id: &str) {
    let trigger = serde_json::to_string(&WorkflowTrigger {
        kind: "push".to_string(),
        branches: vec!["main".to_string()],
    })
    .expect("trigger");
    let definition = serde_json::to_string(&WorkflowDefinition {
        jobs: vec![WorkflowJobDefinition {
            id: "build".to_string(),
            name: "Build".to_string(),
            runner_id: runner_id.to_string(),
            runner_job_name: "build-app".to_string(),
            needs: Vec::new(),
            inputs: BTreeMap::from([
                ("commit".to_string(), json!("$commit")),
                ("branch".to_string(), json!("$branch")),
            ]),
            artifacts_from: Vec::new(),
            allow_failure: false,
        }],
    })
    .expect("definition");
    state
        .db
        .create_workflow(repo_id, "wf", true, &trigger, &definition)
        .expect("workflow");
}

fn write_test_config(dir: &Path) -> PathBuf {
    let config_path = dir.join("server.toml");
    fs::create_dir_all(dir.join("data")).expect("data dir");
    fs::create_dir_all(dir.join("repos")).expect("repos dir");
    fs::write(
        &config_path,
        format!(
            r#"data_dir = "{}"
repos_dir = "{}"

[database]
path = "{}"

[server]
listen = "127.0.0.1:0"
public_base_url = "ci.test"

[auth]
session_secret = "test-secret"
session_ttl_days = 1
session_cookie_secure = false
login_rate_limit_per_minute = 100

[auth.bootstrap_admin]
username = "admin"
password = "password123"

[scheduler]
poll_interval_ms = 50
cancel_stuck_timeout_seconds = 1
max_cancel_retries = 2

[runners]
healthcheck_interval_seconds = 60
"#,
            dir.join("data").display(),
            dir.join("repos").display(),
            dir.join("data/server.sqlite3").display(),
        ),
    )
    .expect("config");
    config_path
}

fn temp_dir(label: &str) -> PathBuf {
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time")
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("strait-server-{label}-{suffix}"));
    fs::create_dir_all(&dir).expect("temp dir");
    dir
}

fn session_cookie_value(state: &Arc<crate::app::AppState>, user_id: &str) -> String {
    let session_id = state
        .db
        .create_session(user_id, &(Utc::now() + Duration::days(1)).to_rfc3339())
        .expect("session");
    session_cookie(
        &state.config.auth.session_secret,
        &session_id,
        state.config.auth.session_cookie_secure,
    )
    .to_string()
}

struct MockRunnerState {
    runs: Mutex<BTreeMap<String, MockRun>>,
    dispatches: Mutex<BTreeMap<String, String>>,
    cancel_requests: Mutex<BTreeMap<String, usize>>,
    fail_first_dispatch: AtomicBool,
    stall_cancellation: AtomicBool,
}

#[derive(Debug, Clone, Copy)]
struct MockRun {
    polls: usize,
    cancel_stage: Option<u8>,
}

struct MockRunner {
    base_url: String,
    state: Arc<MockRunnerState>,
}

async fn spawn_mock_runner() -> MockRunner {
    spawn_mock_runner_with_options(false, false).await
}

async fn spawn_mock_runner_with_fail_first_dispatch() -> MockRunner {
    spawn_mock_runner_with_options(true, false).await
}

async fn spawn_mock_runner_with_stuck_cancellation() -> MockRunner {
    spawn_mock_runner_with_options(false, true).await
}

async fn spawn_mock_runner_with_options(
    fail_first_dispatch: bool,
    stall_cancellation: bool,
) -> MockRunner {
    let state = Arc::new(MockRunnerState {
        runs: Mutex::new(BTreeMap::new()),
        dispatches: Mutex::new(BTreeMap::new()),
        cancel_requests: Mutex::new(BTreeMap::new()),
        fail_first_dispatch: AtomicBool::new(fail_first_dispatch),
        stall_cancellation: AtomicBool::new(stall_cancellation),
    });
    let app = Router::new()
        .route("/jobs", get(mock_list_jobs))
        .route("/jobs/{name}/runs", post(mock_create_run))
        .route("/runs/{job_id}", get(mock_get_run).delete(mock_cancel_run))
        .route("/runs/{job_id}/logs", get(mock_logs))
        .route("/artifacts", post(mock_artifact_upload))
        .route("/artifacts/{artifact_id}", get(mock_artifact_download))
        .with_state(Arc::clone(&state));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });
    MockRunner {
        base_url: format!("http://{}", addr),
        state,
    }
}

impl MockRunner {
    fn dispatch_count(&self) -> usize {
        self.state.dispatches.lock().expect("dispatches").len()
    }

    fn cancel_count(&self, job_id: &str) -> usize {
        self.state
            .cancel_requests
            .lock()
            .expect("cancel requests")
            .get(job_id)
            .copied()
            .unwrap_or(0)
    }
}

async fn mock_list_jobs() -> Json<JsonValue> {
    Json(json!([{"name":"build-app","timeout_seconds":60}]))
}

async fn mock_create_run(
    State(state): State<Arc<MockRunnerState>>,
    AxumPath(_name): AxumPath<String>,
    headers: axum::http::HeaderMap,
) -> (StatusCode, Json<JsonValue>) {
    let key = headers
        .get("x-idempotency-key")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("missing")
        .to_string();
    let job_id = {
        let mut dispatches = state.dispatches.lock().expect("dispatches");
        dispatches
            .entry(key)
            .or_insert_with(|| Uuid::now_v7().to_string())
            .clone()
    };
    state
        .runs
        .lock()
        .expect("runs")
        .entry(job_id.clone())
        .or_insert(MockRun {
            polls: 0,
            cancel_stage: None,
        });
    if state.fail_first_dispatch.swap(false, Ordering::SeqCst) {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error":"simulated ambiguous create failure"})),
        );
    }
    (
        StatusCode::CREATED,
        Json(json!({"job_id":job_id,"status":"running","started_at":Utc::now().to_rfc3339()})),
    )
}

async fn mock_get_run(
    State(state): State<Arc<MockRunnerState>>,
    AxumPath(job_id): AxumPath<String>,
) -> Json<JsonValue> {
    let mut runs = state.runs.lock().expect("runs");
    let run = runs.entry(job_id.clone()).or_insert(MockRun {
        polls: 0,
        cancel_stage: None,
    });
    if let Some(stage) = run.cancel_stage {
        if state.stall_cancellation.load(Ordering::SeqCst) {
            return Json(json!({
                "job_id": job_id,
                "name": "build-app",
                "status": "running",
                "started_at": Utc::now().to_rfc3339(),
                "finished_at": null,
                "exit_code": null,
                "outputs": {}
            }));
        }
        run.cancel_stage = Some(stage.saturating_add(1));
        let status = match stage {
            0 => "cancel_requested",
            1 => "canceling",
            _ => "canceled",
        };
        return Json(json!({
            "job_id": job_id,
            "name": "build-app",
            "status": status,
            "started_at": Utc::now().to_rfc3339(),
            "finished_at": if status == "canceled" { json!(Utc::now().to_rfc3339()) } else { JsonValue::Null },
            "exit_code": null,
            "outputs": {}
        }));
    }
    run.polls += 1;
    if run.polls >= 2 {
        Json(json!({
            "job_id": job_id,
            "name": "build-app",
            "status": "success",
            "started_at": Utc::now().to_rfc3339(),
            "finished_at": Utc::now().to_rfc3339(),
            "exit_code": 0,
            "outputs": {}
        }))
    } else {
        Json(json!({
            "job_id": job_id,
            "name": "build-app",
            "status": "running",
            "started_at": Utc::now().to_rfc3339(),
            "finished_at": null,
            "exit_code": null,
            "outputs": {}
        }))
    }
}

async fn mock_logs() -> Json<JsonValue> {
    Json(json!({"stdout":"ok\n","stderr":""}))
}

async fn mock_cancel_run(
    State(state): State<Arc<MockRunnerState>>,
    AxumPath(job_id): AxumPath<String>,
) -> StatusCode {
    {
        let mut cancel_requests = state.cancel_requests.lock().expect("cancel requests");
        *cancel_requests.entry(job_id.clone()).or_insert(0) += 1;
    }
    if let Some(run) = state.runs.lock().expect("runs").get_mut(&job_id) {
        run.cancel_stage = Some(0);
    }
    StatusCode::ACCEPTED
}

async fn mock_artifact_upload(body: Body) -> (StatusCode, Json<JsonValue>) {
    let bytes = to_bytes(body, usize::MAX).await.expect("bytes");
    (
        StatusCode::CREATED,
        Json(json!({
            "artifact_id":"artifact-1",
            "sha256":format!("{:x}", Sha256::digest(&bytes)),
            "size": bytes.len(),
            "expires_at": Utc::now().to_rfc3339()
        })),
    )
}

async fn mock_artifact_download() -> Body {
    Body::from("artifact")
}
