use super::routes::{build_router, csrf_token};
use crate::{
    app::build_state,
    auth::{hash_password, session_cookie},
    git,
    models::{
        Repo, RunnerJobSchema, User, WorkflowDefinition, WorkflowInputBinding,
        WorkflowJobDefinition, WorkflowTrigger,
    },
    scheduler,
};
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
};
use tokio::time::sleep;
use tower::util::ServiceExt;
use url::form_urlencoded;
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

#[tokio::test]
async fn workflows_page_renders_runner_job_builder() {
    let fixture = test_fixture_with_runner("http://127.0.0.1:1").await;
    fixture
        .state
        .db
        .replace_runner_jobs(
            &fixture.runner_id,
            &[
                runner_job_schema(
                    r#"{"name":"build-app","timeout_seconds":60,"inputs":{"commit":{"type":"string","required":true},"branch":{"type":"string","required":true},"source":{"type":"artifact","required":true}},"outputs":{"app":{"type":"artifact","required":true}}}"#,
                ),
                runner_job_schema(
                    r#"{"name":"test-app","timeout_seconds":60,"inputs":{"commit":{"type":"string","required":true}},"outputs":{}}"#,
                ),
            ],
        )
        .expect("runner jobs");
    fixture
        .state
        .db
        .update_runner_health(&fixture.runner_id, "healthy")
        .expect("health");

    let cookie = session_cookie_value(&fixture.state, &fixture.user.id);
    let response = fixture
        .app
        .clone()
        .oneshot(
            Request::get("/workflows")
                .header("cookie", cookie)
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);

    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let html = String::from_utf8(body.to_vec()).expect("html");
    assert!(html.contains("workflow-runner-catalog"));
    assert!(html.contains("workflow-job-list"));
    assert!(html.contains("Job"));
    assert!(html.contains("build-app"));
    assert!(html.contains("test-app"));
    assert!(html.contains("\"commit\""));
}

#[tokio::test]
async fn workflows_page_marks_stale_workflow_schemas() {
    let fixture = test_fixture_with_runner("http://127.0.0.1:1").await;
    let repo = create_repo_direct(&fixture.state, &fixture.user, "stale-workflow-page");
    create_workflow_direct(&fixture.state, &repo.id, &fixture.runner_id);
    fixture
        .state
        .db
        .replace_runner_jobs(
            &fixture.runner_id,
            &[runner_job_schema(
                r#"{"name":"build-app","timeout_seconds":60,"inputs":{"commit":{"type":"string","required":true},"branch":{"type":"string","required":true},"source":{"type":"artifact","required":true},"published":{"type":"boolean","required":false}},"outputs":{"app":{"type":"artifact","required":true}}}"#,
            )],
        )
        .expect("runner jobs");

    let cookie = session_cookie_value(&fixture.state, &fixture.user.id);
    let response = fixture
        .app
        .clone()
        .oneshot(
            Request::get("/workflows")
                .header("cookie", cookie)
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);

    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let html = String::from_utf8(body.to_vec()).expect("html");
    assert!(html.contains("stale"));
}

#[tokio::test]
async fn api_workflow_includes_schema_status() {
    let fixture = test_fixture_with_runner("http://127.0.0.1:1").await;
    let repo = create_repo_direct(&fixture.state, &fixture.user, "stale-workflow-api");
    create_workflow_direct(&fixture.state, &repo.id, &fixture.runner_id);
    fixture
        .state
        .db
        .replace_runner_jobs(
            &fixture.runner_id,
            &[runner_job_schema(
                r#"{"name":"build-app","timeout_seconds":60,"inputs":{"commit":{"type":"string","required":true},"branch":{"type":"string","required":true}},"outputs":{}}"#,
            )],
        )
        .expect("runner jobs");

    let workflow = fixture
        .state
        .db
        .workflows_for_repo(&repo.id)
        .expect("workflows")
        .into_iter()
        .find(|workflow| workflow.name == "wf")
        .expect("workflow");
    let cookie = session_cookie_value(&fixture.state, &fixture.user.id);
    let response = fixture
        .app
        .clone()
        .oneshot(
            Request::get(format!("/api/workflows/{}", workflow.id))
                .header("cookie", cookie)
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);

    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let payload: JsonValue = serde_json::from_slice(&body).expect("json");
    assert_eq!(payload["schema_status"], json!("stale"));
    assert_eq!(payload["id"], json!(workflow.id));
}

#[tokio::test]
async fn workflow_form_submission_accepts_structured_jobs_json() {
    let fixture = test_fixture_with_runner("http://127.0.0.1:1").await;
    let repo = create_repo_direct(&fixture.state, &fixture.user, "demo");
    let token = csrf_token(&fixture.state, &fixture.user);
    let cookie = session_cookie_value(&fixture.state, &fixture.user.id);
    let jobs_json = serde_json::to_string(&vec![json!({
        "runner_id": fixture.runner_id,
        "runner_job_name": "build-app",
        "inputs": {
            "commit": { "kind": "commit" },
            "branch": { "kind": "branch" },
            "source": { "kind": "source_artifact" }
        },
        "allow_failure": false
    })])
    .expect("jobs json");
    let body = form_urlencoded::Serializer::new(String::new())
        .append_pair("csrf_token", &token)
        .append_pair("repo_id", &repo.id)
        .append_pair("name", "wf")
        .append_pair("trigger_kind", "push")
        .append_pair("branch_name", "main")
        .append_pair("jobs_json", &jobs_json)
        .finish();

    let response = fixture
        .app
        .clone()
        .oneshot(
            Request::post("/workflows")
                .header("content-type", "application/x-www-form-urlencoded")
                .header("cookie", cookie)
                .body(Body::from(body))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::SEE_OTHER);

    let workflow = fixture
        .state
        .db
        .workflows_for_repo(&repo.id)
        .expect("workflows")
        .into_iter()
        .find(|workflow| workflow.name == "wf")
        .expect("workflow");
    let definition: WorkflowDefinition =
        serde_json::from_str(&workflow.definition_json).expect("definition");
    assert_eq!(definition.jobs.len(), 1);
    assert_eq!(definition.jobs[0].runner_job_name, "build-app");
    assert_eq!(
        definition.jobs[0].inputs.get("source"),
        Some(&WorkflowInputBinding::SourceArtifact)
    );
}

#[tokio::test]
async fn workflow_form_rejects_missing_job_output_reference() {
    let fixture = test_fixture_with_runner("http://127.0.0.1:1").await;
    fixture
        .state
        .db
        .replace_runner_jobs(
            &fixture.runner_id,
            &[
                runner_job_schema(
                    r#"{"name":"produce","timeout_seconds":60,"inputs":{},"outputs":{"version":{"type":"string","required":true}}}"#,
                ),
                runner_job_schema(
                    r#"{"name":"consume","timeout_seconds":60,"inputs":{"version":{"type":"string","required":true}},"outputs":{}}"#,
                ),
            ],
        )
        .expect("runner jobs");
    let repo = create_repo_direct(&fixture.state, &fixture.user, "demo-missing-output");
    let token = csrf_token(&fixture.state, &fixture.user);
    let cookie = session_cookie_value(&fixture.state, &fixture.user.id);
    let jobs_json = serde_json::to_string(&vec![
        json!({
            "runner_id": fixture.runner_id,
            "runner_job_name": "produce",
            "inputs": {},
            "allow_failure": false
        }),
        json!({
            "runner_id": fixture.runner_id,
            "runner_job_name": "consume",
            "inputs": {
                "version": { "kind": "job_output", "job_index": 0, "output_name": "missing" }
            },
            "allow_failure": false
        }),
    ])
    .expect("jobs json");
    let body = form_urlencoded::Serializer::new(String::new())
        .append_pair("csrf_token", &token)
        .append_pair("repo_id", &repo.id)
        .append_pair("name", "wf")
        .append_pair("trigger_kind", "push")
        .append_pair("branch_name", "main")
        .append_pair("jobs_json", &jobs_json)
        .finish();

    let response = fixture
        .app
        .clone()
        .oneshot(
            Request::post("/workflows")
                .header("content-type", "application/x-www-form-urlencoded")
                .header("cookie", cookie)
                .body(Body::from(body))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let text = String::from_utf8(body.to_vec()).expect("text");
    assert!(text.contains("workflow input version references missing output job-1.missing"));
}

#[tokio::test]
async fn workflow_form_rejects_typed_output_input_mismatch() {
    let fixture = test_fixture_with_runner("http://127.0.0.1:1").await;
    fixture
        .state
        .db
        .replace_runner_jobs(
            &fixture.runner_id,
            &[
                runner_job_schema(
                    r#"{"name":"produce","timeout_seconds":60,"inputs":{},"outputs":{"build_number":{"type":"integer","required":true}}}"#,
                ),
                runner_job_schema(
                    r#"{"name":"consume","timeout_seconds":60,"inputs":{"build_number":{"type":"string","required":true}},"outputs":{}}"#,
                ),
            ],
        )
        .expect("runner jobs");
    let repo = create_repo_direct(&fixture.state, &fixture.user, "demo-mismatch");
    let token = csrf_token(&fixture.state, &fixture.user);
    let cookie = session_cookie_value(&fixture.state, &fixture.user.id);
    let jobs_json = serde_json::to_string(&vec![
        json!({
            "runner_id": fixture.runner_id,
            "runner_job_name": "produce",
            "inputs": {},
            "allow_failure": false
        }),
        json!({
            "runner_id": fixture.runner_id,
            "runner_job_name": "consume",
            "inputs": {
                "build_number": { "kind": "job_output", "job_index": 0, "output_name": "build_number" }
            },
            "allow_failure": false
        }),
    ])
    .expect("jobs json");
    let body = form_urlencoded::Serializer::new(String::new())
        .append_pair("csrf_token", &token)
        .append_pair("repo_id", &repo.id)
        .append_pair("name", "wf")
        .append_pair("trigger_kind", "push")
        .append_pair("branch_name", "main")
        .append_pair("jobs_json", &jobs_json)
        .finish();

    let response = fixture
        .app
        .clone()
        .oneshot(
            Request::post("/workflows")
                .header("content-type", "application/x-www-form-urlencoded")
                .header("cookie", cookie)
                .body(Body::from(body))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let text = String::from_utf8(body.to_vec()).expect("text");
    assert!(
        text.contains(
            "workflow input build_number expects string but job-1.build_number is integer"
        )
    );
}

#[tokio::test]
async fn workflow_form_rejects_literal_input_type_mismatch() {
    let fixture = test_fixture_with_runner("http://127.0.0.1:1").await;
    fixture
        .state
        .db
        .replace_runner_jobs(
            &fixture.runner_id,
            &[runner_job_schema(
                r#"{"name":"build-app","timeout_seconds":60,"inputs":{"published":{"type":"boolean","required":true}},"outputs":{}}"#,
            )],
        )
        .expect("runner jobs");
    let repo = create_repo_direct(&fixture.state, &fixture.user, "demo-literal-mismatch");
    let token = csrf_token(&fixture.state, &fixture.user);
    let cookie = session_cookie_value(&fixture.state, &fixture.user.id);
    let jobs_json = serde_json::to_string(&vec![json!({
        "runner_id": fixture.runner_id,
        "runner_job_name": "build-app",
        "inputs": {
            "published": { "kind": "literal", "value": "true" }
        },
        "allow_failure": false
    })])
    .expect("jobs json");
    let body = form_urlencoded::Serializer::new(String::new())
        .append_pair("csrf_token", &token)
        .append_pair("repo_id", &repo.id)
        .append_pair("name", "wf")
        .append_pair("trigger_kind", "push")
        .append_pair("branch_name", "main")
        .append_pair("jobs_json", &jobs_json)
        .finish();

    let response = fixture
        .app
        .clone()
        .oneshot(
            Request::post("/workflows")
                .header("content-type", "application/x-www-form-urlencoded")
                .header("cookie", cookie)
                .body(Body::from(body))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let text = String::from_utf8(body.to_vec()).expect("text");
    assert!(text.contains("workflow input published expects boolean but got string"));
}

#[tokio::test]
async fn workflow_form_rejects_unknown_literal_input_name() {
    let fixture = test_fixture_with_runner("http://127.0.0.1:1").await;
    let repo = create_repo_direct(&fixture.state, &fixture.user, "demo-unknown-input");
    let token = csrf_token(&fixture.state, &fixture.user);
    let cookie = session_cookie_value(&fixture.state, &fixture.user.id);
    let jobs_json = serde_json::to_string(&vec![json!({
        "runner_id": fixture.runner_id,
        "runner_job_name": "build-app",
        "inputs": {
            "bogus": { "kind": "literal", "value": 123 }
        },
        "allow_failure": false
    })])
    .expect("jobs json");
    let body = form_urlencoded::Serializer::new(String::new())
        .append_pair("csrf_token", &token)
        .append_pair("repo_id", &repo.id)
        .append_pair("name", "wf")
        .append_pair("trigger_kind", "push")
        .append_pair("branch_name", "main")
        .append_pair("jobs_json", &jobs_json)
        .finish();

    let response = fixture
        .app
        .clone()
        .oneshot(
            Request::post("/workflows")
                .header("content-type", "application/x-www-form-urlencoded")
                .header("cookie", cookie)
                .body(Body::from(body))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let text = String::from_utf8(body.to_vec()).expect("text");
    assert!(text.contains("workflow job 1 provides unknown input bogus for runner job build-app"));
}

#[test]
fn hook_ingestion_is_idempotent() {
    let dir = temp_dir("hook_idempotent");
    let config_path = write_test_config(&dir);
    let state = build_state(config_path, PathBuf::from("/bin/strait-server")).expect("state");
    let hash = hash_password("password123").expect("hash");
    state
        .db
        .create_user("alice", &hash, "developer")
        .expect("user");
    let user = state
        .db
        .get_user_credentials("alice")
        .expect("user")
        .unwrap()
        .0;
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
    state
        .db
        .create_push_event(&repo_id, &key, &refs)
        .expect("push1");
    state
        .db
        .create_push_event(&repo_id, &key, &refs)
        .expect("push2");
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
async fn scheduler_passes_typed_outputs_to_downstream_job_inputs() {
    let mock = spawn_mock_runner().await;
    let fixture = test_fixture_with_runner(&mock.base_url).await;
    let producer_runner_id = fixture.runner_id.clone();
    fixture
        .state
        .db
        .replace_runner_jobs(
            &producer_runner_id,
            &[runner_job_schema(
                r#"{"name":"produce-typed","timeout_seconds":60,"inputs":{},"outputs":{"version":{"type":"string","required":true},"build_number":{"type":"integer","required":true},"published":{"type":"boolean","required":true},"metadata":{"type":"json","required":true}}}"#,
            )],
        )
        .expect("producer jobs");
    let consumer_runner_id = fixture
        .state
        .db
        .create_runner("consumer-runner", &mock.base_url, "token")
        .expect("consumer runner");
    fixture
        .state
        .db
        .replace_runner_jobs(
            &consumer_runner_id,
            &[runner_job_schema(
                r#"{"name":"consume-typed","timeout_seconds":60,"inputs":{"version":{"type":"string","required":true},"build_number":{"type":"integer","required":true},"published":{"type":"boolean","required":true},"metadata":{"type":"json","required":true}},"outputs":{}}"#,
            )],
        )
        .expect("consumer jobs");
    fixture
        .state
        .db
        .update_runner_health(&consumer_runner_id, "healthy")
        .expect("consumer health");

    let repo = create_repo_direct(&fixture.state, &fixture.user, "typed-chain");
    create_workflow_with_jobs_direct(
        &fixture.state,
        &repo.id,
        vec![
            WorkflowJobDefinition {
                runner_id: producer_runner_id,
                runner_job_name: "produce-typed".to_string(),
                inputs: BTreeMap::new(),
                allow_failure: false,
            },
            WorkflowJobDefinition {
                runner_id: consumer_runner_id,
                runner_job_name: "consume-typed".to_string(),
                inputs: BTreeMap::from([
                    (
                        "version".to_string(),
                        binding(
                            json!({ "kind": "job_output", "job_index": 0, "output_name": "version" }),
                        ),
                    ),
                    (
                        "build_number".to_string(),
                        binding(
                            json!({ "kind": "job_output", "job_index": 0, "output_name": "build_number" }),
                        ),
                    ),
                    (
                        "published".to_string(),
                        binding(
                            json!({ "kind": "job_output", "job_index": 0, "output_name": "published" }),
                        ),
                    ),
                    (
                        "metadata".to_string(),
                        binding(
                            json!({ "kind": "job_output", "job_index": 0, "output_name": "metadata" }),
                        ),
                    ),
                ]),
                allow_failure: false,
            },
        ],
    );
    fixture
        .state
        .db
        .create_push_event(
            &repo.id,
            "typed-event-1",
            &[crate::models::PushEventRef {
                old_rev: "0".repeat(40),
                new_rev: "1".repeat(40),
                ref_name: "refs/heads/main".to_string(),
            }],
        )
        .expect("push event");

    scheduler::reconcile_once(Arc::clone(&fixture.state))
        .await
        .expect("dispatch producer");
    scheduler::reconcile_once(Arc::clone(&fixture.state))
        .await
        .expect("poll producer 1");
    scheduler::reconcile_once(Arc::clone(&fixture.state))
        .await
        .expect("poll producer 2");
    scheduler::reconcile_once(Arc::clone(&fixture.state))
        .await
        .expect("dispatch consumer");

    let requests = mock.requests_for("consume-typed");
    assert_eq!(requests.len(), 1);
    let request = &requests[0];
    assert_eq!(request["version"], json!("v1.2.3"));
    assert_eq!(request["build_number"], json!(42));
    assert_eq!(request["published"], json!(true));
    assert_eq!(request["metadata"], json!({ "commit": "abc123" }));
    assert!(request["strait_job_run_id"].is_string());
}

#[tokio::test]
async fn scheduler_rejects_mismatched_typed_output_binding() {
    let mock = spawn_mock_runner().await;
    let fixture = test_fixture_with_runner(&mock.base_url).await;
    let producer_runner_id = fixture.runner_id.clone();
    fixture
        .state
        .db
        .replace_runner_jobs(
            &producer_runner_id,
            &[runner_job_schema(
                r#"{"name":"produce-int","timeout_seconds":60,"inputs":{},"outputs":{"build_number":{"type":"integer","required":true}}}"#,
            )],
        )
        .expect("producer jobs");
    let consumer_runner_id = fixture
        .state
        .db
        .create_runner("consumer-mismatch", &mock.base_url, "token")
        .expect("consumer runner");
    fixture
        .state
        .db
        .replace_runner_jobs(
            &consumer_runner_id,
            &[runner_job_schema(
                r#"{"name":"consume-string","timeout_seconds":60,"inputs":{"build_number":{"type":"string","required":true}},"outputs":{}}"#,
            )],
        )
        .expect("consumer jobs");
    fixture
        .state
        .db
        .update_runner_health(&consumer_runner_id, "healthy")
        .expect("consumer health");

    let repo = create_repo_direct(&fixture.state, &fixture.user, "typed-mismatch");
    create_workflow_with_jobs_direct(
        &fixture.state,
        &repo.id,
        vec![
            WorkflowJobDefinition {
                runner_id: producer_runner_id,
                runner_job_name: "produce-int".to_string(),
                inputs: BTreeMap::new(),
                allow_failure: false,
            },
            WorkflowJobDefinition {
                runner_id: consumer_runner_id,
                runner_job_name: "consume-string".to_string(),
                inputs: BTreeMap::from([(
                    "build_number".to_string(),
                    binding(
                        json!({ "kind": "job_output", "job_index": 0, "output_name": "build_number" }),
                    ),
                )]),
                allow_failure: false,
            },
        ],
    );
    fixture
        .state
        .db
        .create_push_event(
            &repo.id,
            "typed-event-2",
            &[crate::models::PushEventRef {
                old_rev: "0".repeat(40),
                new_rev: "1".repeat(40),
                ref_name: "refs/heads/main".to_string(),
            }],
        )
        .expect("push event");

    scheduler::reconcile_once(Arc::clone(&fixture.state))
        .await
        .expect("dispatch producer");
    scheduler::reconcile_once(Arc::clone(&fixture.state))
        .await
        .expect("poll producer 1");
    let error = scheduler::reconcile_once(Arc::clone(&fixture.state))
        .await
        .expect_err("mismatch should fail before dispatch");
    assert!(
        error.to_string().contains(
            "workflow input build_number expects string but job-1.build_number is integer"
        )
    );
    assert!(mock.requests_for("consume-string").is_empty());
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
    assert_eq!(snapshot.jobs[0].run.exit_code, Some(0));
    assert_eq!(
        snapshot.jobs[0].run.terminal_reason.as_deref(),
        Some("success")
    );
    assert_eq!(snapshot.jobs[0].run.failure_category, None);
    assert_eq!(snapshot.jobs[0].run.output_metadata.artifacts.count, 0);
    assert!(snapshot.jobs[0].stdout.contains("ok"));
}

#[tokio::test]
async fn scheduler_persists_timeout_runner_state() {
    let mock = spawn_mock_runner_with_timeout().await;
    let fixture = test_fixture_with_runner(&mock.base_url).await;
    let repo = create_repo_direct(&fixture.state, &fixture.user, "demo");
    create_workflow_direct(&fixture.state, &repo.id, &fixture.runner_id);
    fixture
        .state
        .db
        .create_push_event(
            &repo.id,
            "event-timeout",
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
    assert_eq!(snapshot.pipeline.status, "failed");
    assert_eq!(snapshot.jobs[0].run.status, "failed");
    assert_eq!(
        snapshot.jobs[0].run.terminal_reason.as_deref(),
        Some("timeout")
    );
    assert_eq!(
        snapshot.jobs[0].run.failure_category.as_deref(),
        Some("timeout")
    );
    assert_eq!(snapshot.jobs[0].run.exit_code, None);
}

#[tokio::test]
async fn scheduler_persists_job_failure_runner_state() {
    let mock = spawn_mock_runner_with_job_failure().await;
    let fixture = test_fixture_with_runner(&mock.base_url).await;
    let repo = create_repo_direct(&fixture.state, &fixture.user, "demo");
    create_workflow_direct(&fixture.state, &repo.id, &fixture.runner_id);
    fixture
        .state
        .db
        .create_push_event(
            &repo.id,
            "event-job-failed",
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
    assert_eq!(snapshot.pipeline.status, "failed");
    assert_eq!(snapshot.jobs[0].run.status, "failed");
    assert_eq!(
        snapshot.jobs[0].run.terminal_reason.as_deref(),
        Some("exit_code")
    );
    assert_eq!(
        snapshot.jobs[0].run.failure_category.as_deref(),
        Some("job")
    );
    assert_eq!(snapshot.jobs[0].run.exit_code, Some(7));
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
async fn enqueue_workflow_run_allows_stale_schema() {
    let fixture = test_fixture_with_runner("http://127.0.0.1:1").await;
    let repo = create_repo_direct(&fixture.state, &fixture.user, "stale-enqueue");
    create_workflow_direct(&fixture.state, &repo.id, &fixture.runner_id);
    fixture
        .state
        .db
        .replace_runner_jobs(
            &fixture.runner_id,
            &[runner_job_schema(
                r#"{"name":"build-app","timeout_seconds":60,"inputs":{"commit":{"type":"string","required":true},"branch":{"type":"string","required":true},"source":{"type":"artifact","required":true},"published":{"type":"boolean","required":false}},"outputs":{"app":{"type":"artifact","required":true}}}"#,
            )],
        )
        .expect("runner jobs");
    let workflow = fixture
        .state
        .db
        .workflows_for_repo(&repo.id)
        .expect("workflows")
        .remove(0);

    let pipeline_id = scheduler::enqueue_workflow_run(
        Arc::clone(&fixture.state),
        &workflow,
        "push",
        Some("refs/heads/main"),
        Some(&"1".repeat(40)),
    )
    .expect("stale workflow should still enqueue");

    assert!(!pipeline_id.is_empty());
}

#[tokio::test]
async fn enqueue_workflow_run_blocks_incompatible_schema() {
    let fixture = test_fixture_with_runner("http://127.0.0.1:1").await;
    let repo = create_repo_direct(&fixture.state, &fixture.user, "incompatible-enqueue");
    create_workflow_direct(&fixture.state, &repo.id, &fixture.runner_id);
    fixture
        .state
        .db
        .replace_runner_jobs(&fixture.runner_id, &[])
        .expect("runner jobs");
    let workflow = fixture
        .state
        .db
        .workflows_for_repo(&repo.id)
        .expect("workflows")
        .remove(0);

    let error = scheduler::enqueue_workflow_run(
        Arc::clone(&fixture.state),
        &workflow,
        "push",
        Some("refs/heads/main"),
        Some(&"1".repeat(40)),
    )
    .expect_err("incompatible workflow should be blocked");

    assert!(
        error
            .to_string()
            .contains("incompatible workflow schema for workflow")
    );
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
    assert_eq!(
        snapshot.pipeline.cancel_reason.as_deref(),
        Some("user_requested")
    );
    assert_eq!(snapshot.jobs[0].run.status, "cancel_requested");
    assert_eq!(
        snapshot.jobs[0].run.cancel_reason.as_deref(),
        Some("user_requested")
    );

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
    assert_eq!(
        snapshot.jobs[0].run.terminal_reason.as_deref(),
        Some("canceled")
    );
    assert_eq!(
        snapshot.jobs[0].run.failure_category.as_deref(),
        Some("canceled")
    );
    assert_eq!(snapshot.jobs[0].run.duration_ms, Some(1000));
}

#[tokio::test]
async fn scheduler_retries_infra_failure_and_recovers() {
    let mock = spawn_mock_runner_with_infra_failure_once().await;
    let fixture = test_fixture_with_runner(&mock.base_url).await;
    let repo = create_repo_direct(&fixture.state, &fixture.user, "demo");
    create_workflow_direct(&fixture.state, &repo.id, &fixture.runner_id);
    fixture
        .state
        .db
        .create_push_event(
            &repo.id,
            "event-infra-retry",
            &[crate::models::PushEventRef {
                old_rev: "0".repeat(40),
                new_rev: "1".repeat(40),
                ref_name: "refs/heads/main".to_string(),
            }],
        )
        .expect("push event");

    scheduler::reconcile_once(Arc::clone(&fixture.state))
        .await
        .expect("dispatch1");
    scheduler::reconcile_once(Arc::clone(&fixture.state))
        .await
        .expect("poll infra failure");
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
    assert_eq!(snapshot.pipeline.status, "running");
    assert_eq!(snapshot.jobs[0].run.status, "pending");
    assert_eq!(snapshot.jobs[0].run.infra_retry_count, 1);
    assert!(snapshot.jobs[0].run.last_infra_retry_at.is_some());

    scheduler::reconcile_once(Arc::clone(&fixture.state))
        .await
        .expect("dispatch2");
    scheduler::reconcile_once(Arc::clone(&fixture.state))
        .await
        .expect("poll success");
    let snapshot = fixture
        .state
        .db
        .pipeline_snapshot(&pipeline.id)
        .expect("snapshot")
        .unwrap();
    assert_eq!(snapshot.pipeline.status, "success");
    assert_eq!(snapshot.jobs[0].run.status, "success");
    assert_eq!(snapshot.jobs[0].run.infra_retry_count, 1);
    assert_eq!(mock.dispatch_count(), 2);
}

#[tokio::test]
async fn scheduler_fails_job_after_infra_retry_budget_exhausted() {
    let mock = spawn_mock_runner_with_infra_failure().await;
    let fixture = test_fixture_with_runner(&mock.base_url).await;
    let repo = create_repo_direct(&fixture.state, &fixture.user, "demo");
    create_workflow_direct(&fixture.state, &repo.id, &fixture.runner_id);
    fixture
        .state
        .db
        .create_push_event(
            &repo.id,
            "event-infra-exhaust",
            &[crate::models::PushEventRef {
                old_rev: "0".repeat(40),
                new_rev: "1".repeat(40),
                ref_name: "refs/heads/main".to_string(),
            }],
        )
        .expect("push event");

    scheduler::reconcile_once(Arc::clone(&fixture.state))
        .await
        .expect("dispatch1");
    scheduler::reconcile_once(Arc::clone(&fixture.state))
        .await
        .expect("poll1");
    scheduler::reconcile_once(Arc::clone(&fixture.state))
        .await
        .expect("dispatch2");
    scheduler::reconcile_once(Arc::clone(&fixture.state))
        .await
        .expect("poll2");
    scheduler::reconcile_once(Arc::clone(&fixture.state))
        .await
        .expect("dispatch3");
    scheduler::reconcile_once(Arc::clone(&fixture.state))
        .await
        .expect("poll3");

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
    assert_eq!(snapshot.pipeline.status, "failed");
    assert_eq!(snapshot.jobs[0].run.status, "failed");
    assert_eq!(snapshot.jobs[0].run.infra_retry_count, 2);
    assert_eq!(
        snapshot.jobs[0].run.failure_category.as_deref(),
        Some("infra")
    );
    assert_eq!(
        snapshot.jobs[0].run.terminal_reason.as_deref(),
        Some("spawn_error")
    );
    assert_eq!(mock.dispatch_count(), 3);
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
                &[runner_job_schema(
                    r#"{"name":"build-app","timeout_seconds":60,"inputs":{"commit":{"type":"string","required":true},"branch":{"type":"string","required":true},"source":{"type":"artifact","required":true}},"outputs":{"app":{"type":"artifact","required":true}}}"#,
                )],
            )
            .expect("runner jobs");
        state
            .db
            .update_runner_health(&runner_id, "healthy")
            .expect("health");
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
    let jobs = vec![WorkflowJobDefinition {
        runner_id: runner_id.to_string(),
        runner_job_name: "build-app".to_string(),
        inputs: BTreeMap::from([
            ("commit".to_string(), WorkflowInputBinding::Commit),
            ("branch".to_string(), WorkflowInputBinding::Branch),
        ]),
        allow_failure: false,
    }];
    let definition =
        serde_json::to_string(&WorkflowDefinition { jobs: jobs.clone() }).expect("definition");
    let job_schemas = workflow_job_schemas(state, &jobs);
    state
        .db
        .create_workflow(repo_id, "wf", true, &trigger, &definition, &job_schemas)
        .expect("workflow");
}

fn create_workflow_with_jobs_direct(
    state: &Arc<crate::app::AppState>,
    repo_id: &str,
    jobs: Vec<WorkflowJobDefinition>,
) {
    let trigger = serde_json::to_string(&WorkflowTrigger {
        kind: "push".to_string(),
        branches: vec!["main".to_string()],
    })
    .expect("trigger");
    let definition =
        serde_json::to_string(&WorkflowDefinition { jobs: jobs.clone() }).expect("definition");
    let job_schemas = workflow_job_schemas(state, &jobs);
    state
        .db
        .create_workflow(repo_id, "wf", true, &trigger, &definition, &job_schemas)
        .expect("workflow");
}

fn binding(value: JsonValue) -> WorkflowInputBinding {
    serde_json::from_value(value).expect("workflow input binding")
}

fn workflow_job_schemas(
    state: &Arc<crate::app::AppState>,
    jobs: &[WorkflowJobDefinition],
) -> Vec<RunnerJobSchema> {
    jobs.iter()
        .map(|job| {
            state
                .db
                .list_runner_jobs(&job.runner_id)
                .expect("runner jobs")
                .into_iter()
                .find(|schema| schema.name == job.runner_job_name)
                .expect("runner job schema for workflow job")
        })
        .collect::<Vec<_>>()
}

fn runner_job_schema(schema: &str) -> RunnerJobSchema {
    serde_json::from_str(schema).expect("runner job schema")
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
max_infra_retries = 2

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
    let suffix = Uuid::now_v7().simple().to_string();
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
    requests: Mutex<Vec<MockRequest>>,
    cancel_requests: Mutex<BTreeMap<String, usize>>,
    fail_first_dispatch: AtomicBool,
    stall_cancellation: AtomicBool,
    terminal_outcome: MockTerminalOutcome,
}

#[derive(Debug, Clone)]
struct MockRun {
    polls: usize,
    cancel_stage: Option<u8>,
    job_name: String,
}

#[derive(Debug, Clone)]
struct MockRequest {
    job_name: String,
    body: JsonValue,
}

#[derive(Debug, Clone, Copy)]
enum MockTerminalOutcome {
    Success,
    Timeout,
    JobFailed,
    InfraFailed,
    InfraFailOnceThenSuccess,
}

struct MockRunner {
    base_url: String,
    state: Arc<MockRunnerState>,
}

async fn spawn_mock_runner() -> MockRunner {
    spawn_mock_runner_with_options(false, false, MockTerminalOutcome::Success).await
}

async fn spawn_mock_runner_with_fail_first_dispatch() -> MockRunner {
    spawn_mock_runner_with_options(true, false, MockTerminalOutcome::Success).await
}

async fn spawn_mock_runner_with_stuck_cancellation() -> MockRunner {
    spawn_mock_runner_with_options(false, true, MockTerminalOutcome::Success).await
}

async fn spawn_mock_runner_with_timeout() -> MockRunner {
    spawn_mock_runner_with_options(false, false, MockTerminalOutcome::Timeout).await
}

async fn spawn_mock_runner_with_job_failure() -> MockRunner {
    spawn_mock_runner_with_options(false, false, MockTerminalOutcome::JobFailed).await
}

async fn spawn_mock_runner_with_infra_failure() -> MockRunner {
    spawn_mock_runner_with_options(false, false, MockTerminalOutcome::InfraFailed).await
}

async fn spawn_mock_runner_with_infra_failure_once() -> MockRunner {
    spawn_mock_runner_with_options(false, false, MockTerminalOutcome::InfraFailOnceThenSuccess)
        .await
}

async fn spawn_mock_runner_with_options(
    fail_first_dispatch: bool,
    stall_cancellation: bool,
    terminal_outcome: MockTerminalOutcome,
) -> MockRunner {
    let state = Arc::new(MockRunnerState {
        runs: Mutex::new(BTreeMap::new()),
        dispatches: Mutex::new(BTreeMap::new()),
        requests: Mutex::new(Vec::new()),
        cancel_requests: Mutex::new(BTreeMap::new()),
        fail_first_dispatch: AtomicBool::new(fail_first_dispatch),
        stall_cancellation: AtomicBool::new(stall_cancellation),
        terminal_outcome,
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

    fn requests_for(&self, job_name: &str) -> Vec<JsonValue> {
        self.state
            .requests
            .lock()
            .expect("requests")
            .iter()
            .filter(|request| request.job_name == job_name)
            .map(|request| request.body.clone())
            .collect()
    }
}

async fn mock_list_jobs() -> Json<JsonValue> {
    Json(json!([{"name":"build-app","timeout_seconds":60}]))
}

async fn mock_create_run(
    State(state): State<Arc<MockRunnerState>>,
    AxumPath(name): AxumPath<String>,
    headers: axum::http::HeaderMap,
    body: Body,
) -> (StatusCode, Json<JsonValue>) {
    let body = to_bytes(body, usize::MAX).await.expect("bytes");
    let request_body: JsonValue = serde_json::from_slice(&body).expect("json request body");
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
            job_name: name.clone(),
        });
    state.requests.lock().expect("requests").push(MockRequest {
        job_name: name,
        body: request_body,
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
        job_name: "build-app".to_string(),
    });
    if let Some(stage) = run.cancel_stage {
        if state.stall_cancellation.load(Ordering::SeqCst) {
            return Json(json!({
                "job_id": job_id,
                "name": "build-app",
                "status": "running",
                "started_at": Utc::now().to_rfc3339(),
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
            "duration_ms": if status == "canceled" { json!(1000) } else { JsonValue::Null },
            "exit_code": null,
            "terminal_reason": if status == "canceled" { json!("canceled") } else { JsonValue::Null },
            "failure_category": if status == "canceled" { json!("canceled") } else { JsonValue::Null },
            "outputs": {},
            "output_metadata": {
                "stdout": {"bytes": 0, "truncated": false},
                "stderr": {"bytes": 0, "truncated": false},
                "artifacts": {"count": 0, "bytes": 0}
            }
        }));
    }
    run.polls += 1;
    if run.polls >= 2 {
        let dispatch_count = state.dispatches.lock().expect("dispatches").len();
        let (status, exit_code, terminal_reason, failure_category, stdout_bytes) = match state
            .terminal_outcome
        {
            MockTerminalOutcome::Success => ("success", Some(0), Some("success"), None, 3),
            MockTerminalOutcome::Timeout => ("failed", None, Some("timeout"), Some("timeout"), 0),
            MockTerminalOutcome::JobFailed => {
                ("failed", Some(7), Some("exit_code"), Some("job"), 0)
            }
            MockTerminalOutcome::InfraFailed => {
                ("failed", None, Some("spawn_error"), Some("infra"), 0)
            }
            MockTerminalOutcome::InfraFailOnceThenSuccess => {
                if dispatch_count >= 2 {
                    ("success", Some(0), Some("success"), None, 3)
                } else {
                    ("failed", None, Some("spawn_error"), Some("infra"), 0)
                }
            }
        };
        let outputs = match run.job_name.as_str() {
            "produce-typed" => json!({
                "version": { "type": "string", "value": "v1.2.3" },
                "build_number": { "type": "integer", "value": 42 },
                "published": { "type": "boolean", "value": true },
                "metadata": { "type": "json", "value": { "commit": "abc123" } }
            }),
            "produce-int" => json!({
                "build_number": { "type": "integer", "value": 42 }
            }),
            _ => json!({}),
        };
        Json(json!({
            "job_id": job_id,
            "name": run.job_name,
            "status": status,
            "started_at": Utc::now().to_rfc3339(),
            "finished_at": Utc::now().to_rfc3339(),
            "duration_ms": 1000,
            "exit_code": exit_code,
            "terminal_reason": terminal_reason,
            "failure_category": failure_category,
            "outputs": outputs,
            "output_metadata": {
                "stdout": {"bytes": stdout_bytes, "truncated": false},
                "stderr": {"bytes": 0, "truncated": false},
                "artifacts": {"count": 0, "bytes": 0}
            }
        }))
    } else {
        Json(json!({
            "job_id": job_id,
            "name": "build-app",
            "status": "running",
            "started_at": Utc::now().to_rfc3339(),
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
