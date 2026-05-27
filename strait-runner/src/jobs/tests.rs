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
    JobCreatedResponse, JobDefinitionResponse, JobLogsResponse, JobMetadata, JobStatus,
    JobStatusResponse, JobStore, cancel_job, create_job, get_job, get_job_logs, list_jobs,
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
        .route("/jobs/{name}/runs", post(create_job))
        .with_state(state);

    let response = app
        .oneshot(
            Request::post("/jobs/build-app/runs")
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
        .route("/jobs/{name}/runs", post(create_job))
        .with_state(state);

    let response = app
        .oneshot(
            Request::post("/jobs/build-app/runs")
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
        .route("/jobs/{name}/runs", post(create_job))
        .with_state(state);

    let response = app
        .oneshot(
            Request::post("/jobs/build-app/runs")
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
        .route("/jobs/{name}/runs", post(create_job))
        .with_state(state);

    let response = app
        .oneshot(
            Request::post("/jobs/build-with-artifact/runs")
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
        .route("/jobs/{name}/runs", post(create_job))
        .with_state(state);

    let response = app
        .oneshot(
            Request::post("/jobs/build-app/runs")
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

    assert_eq!(metadata.status, JobStatus::Success);
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
        .route("/jobs/{name}/runs", post(create_job))
        .with_state(state);

    let response = app
        .oneshot(
            Request::post("/jobs/build-app/runs")
                .header("authorization", "Bearer runner-token")
                .header("content-type", "application/json")
                .body(Body::from(json!({ "commit": "abc123" }).to_string()))
                .expect("request should build"),
        )
        .await
        .expect("request should succeed");

    let created = read_created_job(response).await;
    let metadata = wait_for_terminal_metadata(&temp, &created.job_id).await;

    assert_eq!(metadata.status, JobStatus::Failed);
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
        .route("/jobs/{name}/runs", post(create_job))
        .with_state(state);

    let response = app
        .oneshot(
            Request::post("/jobs/build-app/runs")
                .header("authorization", "Bearer runner-token")
                .header("content-type", "application/json")
                .body(Body::from(json!({ "commit": "abc123" }).to_string()))
                .expect("request should build"),
        )
        .await
        .expect("request should succeed");

    let created = read_created_job(response).await;
    let metadata = wait_for_terminal_metadata(&temp, &created.job_id).await;

    assert_eq!(metadata.status, JobStatus::TimedOut);
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
        50,
    );
    let app = Router::new()
        .route("/jobs/{name}/runs", post(create_job))
        .with_state(state);

    let created = read_created_job(
        app.oneshot(
            Request::post("/jobs/build-app/runs")
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

    assert_eq!(metadata.status, JobStatus::Success);
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
        50,
    );
    let app = Router::new()
        .route("/jobs/{name}/runs", post(create_job))
        .with_state(state);

    let created = read_created_job(
        app.oneshot(
            Request::post("/jobs/build-app/runs")
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

    assert_eq!(metadata.status, JobStatus::Failed);
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
        .route("/jobs/{name}/runs", post(create_job))
        .route("/runs/{job_id}", get(get_job))
        .with_state(state);

    let created = read_created_job(
        app.clone()
            .oneshot(
                Request::post("/jobs/build-app/runs")
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
            Request::get(format!("/runs/{}", created.job_id))
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
        .route("/jobs/{name}/runs", post(create_job))
        .route("/runs/{job_id}/logs", get(get_job_logs))
        .with_state(state);

    let created = read_created_job(
        app.clone()
            .oneshot(
                Request::post("/jobs/build-app/runs")
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
            Request::get(format!("/runs/{}/logs", created.job_id))
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
async fn lists_job_definitions_over_http() {
    let temp = temp_dir("job_list_http");
    let state = test_state(&temp);
    let app = Router::new()
        .route("/jobs", get(list_jobs))
        .with_state(state);

    let response = app
        .oneshot(
            Request::get("/jobs")
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
    let jobs: Vec<JobDefinitionResponse> =
        serde_json::from_slice(&body).expect("job definitions body");

    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].name, "build-app");
    assert_eq!(jobs[0].timeout_seconds, 600);
    assert!(jobs[0].params.contains_key("commit"));
    assert!(jobs[0].params["commit"].required);
}

#[tokio::test]
async fn cancels_running_job_over_http() {
    let temp = temp_dir("job_cancel_http");
    let state = test_state_with_script(&temp, "build-app", "#!/bin/sh\nsleep 5\n", 600);
    let app = Router::new()
        .route("/jobs/{name}/runs", post(create_job))
        .route("/runs/{job_id}", get(get_job).delete(cancel_job))
        .with_state(state);

    let created = read_created_job(
        app.clone()
            .oneshot(
                Request::post("/jobs/build-app/runs")
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
            Request::delete(format!("/runs/{}", created.job_id))
                .header("authorization", "Bearer runner-token")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("request should succeed");

    assert_eq!(cancel.status(), StatusCode::ACCEPTED);

    let metadata = wait_for_terminal_metadata(&temp, &created.job_id).await;
    assert_eq!(metadata.status, JobStatus::Canceled);
    assert_eq!(metadata.exit_code, None);
}

#[cfg(unix)]
#[tokio::test]
async fn cancel_kills_child_process_tree() {
    let temp = temp_dir("job_cancel_process_tree");
    let state = test_state_with_script(
        &temp,
        "build-app",
        "#!/bin/sh\nsleep 30 &\necho $! > \"$JOB_WORKDIR/child.pid\"\nwait\n",
        600,
    );
    let app = Router::new()
        .route("/jobs/{name}/runs", post(create_job))
        .route("/runs/{job_id}", get(get_job).delete(cancel_job))
        .with_state(state);

    let created = read_created_job(
        app.clone()
            .oneshot(
                Request::post("/jobs/build-app/runs")
                    .header("authorization", "Bearer runner-token")
                    .header("content-type", "application/json")
                    .body(Body::from(json!({ "commit": "abc123" }).to_string()))
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed"),
    )
    .await;

    let child_pid_path = temp
        .join("jobs")
        .join(&created.job_id)
        .join("work")
        .join("child.pid");
    let child_pid = wait_for_file(&child_pid_path).await;

    let cancel = app
        .oneshot(
            Request::delete(format!("/runs/{}", created.job_id))
                .header("authorization", "Bearer runner-token")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("request should succeed");

    assert_eq!(cancel.status(), StatusCode::ACCEPTED);

    let metadata = wait_for_terminal_metadata(&temp, &created.job_id).await;
    assert_eq!(metadata.status, JobStatus::Canceled);
    assert!(!unix_process_exists(
        child_pid.trim().parse().expect("child pid should parse")
    ));
}

#[tokio::test]
async fn fails_job_when_log_limit_is_exceeded() {
    let temp = temp_dir("job_log_limit");
    let state =
        test_state_with_script_and_log_limit(&temp, "build-app", "#!/bin/sh\nprintf 'a'\n", 600, 0);
    let app = Router::new()
        .route("/jobs/{name}/runs", post(create_job))
        .with_state(state);

    let created = read_created_job(
        app.oneshot(
            Request::post("/jobs/build-app/runs")
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

    assert_eq!(metadata.status, JobStatus::Failed);
    assert_eq!(metadata.exit_code, None);
    assert_eq!(
        fs::read(temp.join("jobs").join(&created.job_id).join("stdout.log"))
            .expect("stdout should read")
            .len(),
        0
    );
    assert!(
        fs::read_to_string(temp.join("jobs").join(&created.job_id).join("stderr.log"))
            .expect("stderr should read")
            .contains("job stdout log exceeded configured limit")
    );
}

#[tokio::test]
async fn job_process_sees_only_deliberate_environment() {
    let temp = temp_dir("job_env_isolation");
    let state = test_state_with_script(&temp, "build-app", "#!/bin/sh\nenv | sort\n", 600);
    let app = Router::new()
        .route("/jobs/{name}/runs", post(create_job))
        .with_state(state);

    let created = read_created_job(
        app.oneshot(
            Request::post("/jobs/build-app/runs")
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

    assert_eq!(metadata.status, JobStatus::Success);
    let stdout = fs::read_to_string(temp.join("jobs").join(&created.job_id).join("stdout.log"))
        .expect("stdout log");
    assert!(stdout.contains("JOB_NAME=build-app"));
    assert!(stdout.contains("JOB_COMMIT=abc123"));
    assert!(stdout.contains("PATH=/usr/local/bin:/usr/bin:/bin"));
    assert!(!stdout.contains("HOME="));
}

#[tokio::test]
async fn begin_shutdown_cancels_active_jobs() {
    let temp = temp_dir("job_begin_shutdown");
    let state = test_state_with_script(&temp, "build-app", "#!/bin/sh\nsleep 5\n", 600);
    let app = Router::new()
        .route("/jobs/{name}/runs", post(create_job))
        .with_state(state.clone());

    let created = read_created_job(
        app.oneshot(
            Request::post("/jobs/build-app/runs")
                .header("authorization", "Bearer runner-token")
                .header("content-type", "application/json")
                .body(Body::from(json!({ "commit": "abc123" }).to_string()))
                .expect("request should build"),
        )
        .await
        .expect("request should succeed"),
    )
    .await;

    let canceled = state.jobs.begin_shutdown();
    assert_eq!(canceled, 1);

    let metadata = wait_for_terminal_metadata(&temp, &created.job_id).await;
    assert_eq!(metadata.status, JobStatus::Canceled);
}

#[tokio::test]
async fn rejects_new_jobs_after_shutdown_starts() {
    let temp = temp_dir("job_reject_shutdown");
    let state = test_state(&temp);
    state.jobs.begin_shutdown();

    let error = state
        .jobs
        .create_job(
            "build-app",
            json!({
                "commit": "abc123",
                "branch": "main"
            })
            .as_object()
            .expect("body should be object")
            .clone(),
            &state.manifests,
            &state.artifacts,
            state.config.jobs.default_log_limit_mb,
            state.config.jobs.cleanup_successful_workdirs,
            state.config.jobs.keep_failed_workdirs,
        )
        .expect_err("create_job should reject while shutting down");

    assert!(matches!(error, super::JobError::ShuttingDown));
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
            status: JobStatus::Running,
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
    assert_eq!(metadata.status, JobStatus::Failed);
    assert!(metadata.finished_at.is_some());
    assert!(
        fs::read_to_string(jobs_dir.join("stderr.log"))
            .expect("stderr should read")
            .contains("runner restarted before job completion")
    );
}

#[test]
fn removes_job_dir_missing_metadata_on_startup() {
    let temp = temp_dir("job_recovery_missing_metadata");
    let jobs_dir = temp.join("jobs").join("job_incomplete");
    fs::create_dir_all(jobs_dir.join("work")).expect("work dir should be created");
    fs::write(jobs_dir.join("stdout.log"), "").expect("stdout should exist");

    let store = JobStore::new(&temp).expect("job store should init");
    let recovered = store
        .recover_interrupted_jobs()
        .expect("recovery should succeed");

    assert_eq!(recovered, 1);
    assert!(!jobs_dir.exists());
}

#[test]
fn removes_job_dir_with_invalid_metadata_on_startup() {
    let temp = temp_dir("job_recovery_invalid_metadata");
    let jobs_dir = temp.join("jobs").join("job_invalid");
    fs::create_dir_all(&jobs_dir).expect("job dir should be created");
    fs::write(jobs_dir.join("metadata.json"), b"{not-json").expect("metadata should exist");

    let store = JobStore::new(&temp).expect("job store should init");
    let recovered = store
        .recover_interrupted_jobs()
        .expect("recovery should succeed");

    assert_eq!(recovered, 1);
    assert!(!jobs_dir.exists());
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
        .route("/jobs/{name}/runs", post(create_job))
        .with_state(state);

    let first = app
        .clone()
        .oneshot(
            Request::post("/jobs/job-a/runs")
                .header("authorization", "Bearer runner-token")
                .header("content-type", "application/json")
                .body(Body::from(json!({ "commit": "a" }).to_string()))
                .expect("request should build"),
        )
        .await
        .expect("request should succeed");
    let second = app
        .oneshot(
            Request::post("/jobs/job-b/runs")
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
        .route("/jobs/{name}/runs", post(create_job))
        .with_state(state);

    let first = app
        .clone()
        .oneshot(
            Request::post("/jobs/build-app/runs")
                .header("authorization", "Bearer runner-token")
                .header("content-type", "application/json")
                .body(Body::from(json!({ "commit": "a" }).to_string()))
                .expect("request should build"),
        )
        .await
        .expect("request should succeed");
    let second = app
        .oneshot(
            Request::post("/jobs/build-app/runs")
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
        .route("/jobs/{name}/runs", post(create_job))
        .with_state(state);

    let first = app
        .clone()
        .oneshot(
            Request::post("/jobs/job-a/runs")
                .header("authorization", "Bearer runner-token")
                .header("content-type", "application/json")
                .body(Body::from(json!({ "commit": "a" }).to_string()))
                .expect("request should build"),
        )
        .await
        .expect("request should succeed");
    let second = app
        .oneshot(
            Request::post("/jobs/job-b/runs")
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
        .route("/jobs/{name}/runs", post(create_job))
        .with_state(state);

    let first = app
        .clone()
        .oneshot(
            Request::post("/jobs/job-a/runs")
                .header("authorization", "Bearer runner-token")
                .header("content-type", "application/json")
                .body(Body::from(json!({ "commit": "a" }).to_string()))
                .expect("request should build"),
        )
        .await
        .expect("request should succeed");
    let second = app
        .oneshot(
            Request::post("/jobs/job-b/runs")
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
        50,
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
        50,
    )
}

fn test_state_with_script(
    temp: &Path,
    job_name: &str,
    script_body: &str,
    timeout_seconds: u64,
) -> AppState {
    test_state_with_script_and_log_limit(temp, job_name, script_body, timeout_seconds, 50)
}

fn test_state_with_script_and_log_limit(
    temp: &Path,
    job_name: &str,
    script_body: &str,
    timeout_seconds: u64,
    default_log_limit_mb: u64,
) -> AppState {
    test_state_from_manifest(
        temp,
        job_name,
        OPTIONAL_COMMIT_AND_SOURCE_PARAMS,
        "",
        script_body,
        timeout_seconds,
        default_log_limit_mb,
    )
}

fn test_state_from_manifest(
    temp: &Path,
    job_name: &str,
    params_toml: &str,
    outputs_toml: &str,
    script_body: &str,
    timeout_seconds: u64,
    default_log_limit_mb: u64,
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
            default_log_limit_mb,
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
        runtime_status: Arc::new(crate::RuntimeStatus::new(0, 0)),
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
        let metadata: JobMetadata =
            serde_json::from_slice(&fs::read(&metadata_path).expect("metadata should be readable"))
                .expect("metadata should parse");

        if metadata.finished_at.is_some() {
            return metadata;
        }

        sleep(Duration::from_millis(25)).await;
    }

    panic!("job did not reach a terminal state");
}

async fn wait_for_file(path: &Path) -> String {
    for _ in 0..100 {
        if let Ok(contents) = fs::read_to_string(path) {
            return contents;
        }

        sleep(Duration::from_millis(25)).await;
    }

    panic!("file was not written: {}", path.display());
}

#[cfg(unix)]
fn unix_process_exists(pid: i32) -> bool {
    unsafe extern "C" {
        fn kill(pid: i32, sig: i32) -> i32;
    }

    let result = unsafe { kill(pid, 0) };
    if result == 0 {
        true
    } else {
        std::io::Error::last_os_error().raw_os_error() != Some(3)
    }
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
