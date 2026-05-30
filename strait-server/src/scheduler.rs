use std::{collections::BTreeMap, fs, path::PathBuf, sync::Arc};

use glob::Pattern;
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use tokio::time::{self, Duration, MissedTickBehavior};
use tracing::{error, info, warn};

use crate::{AppState, git, models::{WorkflowDefinition, WorkflowTrigger}};

pub fn spawn(state: Arc<AppState>) {
    let scheduler_state = Arc::clone(&state);
    tokio::spawn(async move {
        let mut interval =
            time::interval(Duration::from_millis(scheduler_state.config.scheduler.poll_interval_ms.max(200)));
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            if let Err(error) = reconcile(Arc::clone(&scheduler_state)).await {
                warn!(%error, "scheduler loop failed");
            }
        }
    });

    let health_state = Arc::clone(&state);
    tokio::spawn(async move {
        let mut interval = time::interval(Duration::from_secs(
            health_state.config.runners.healthcheck_interval_seconds.max(5),
        ));
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            if let Err(error) = refresh_runner_health(Arc::clone(&health_state)).await {
                warn!(%error, "runner health refresh failed");
            }
        }
    });
}

pub(crate) async fn reconcile_once(
    state: Arc<AppState>,
) -> Result<(), Box<dyn std::error::Error>> {
    recover_incomplete_pipelines(Arc::clone(&state))?;
    process_push_events(Arc::clone(&state)).await?;
    dispatch_pending_jobs(Arc::clone(&state)).await?;
    poll_running_jobs(state).await?;
    Ok(())
}

async fn reconcile(state: Arc<AppState>) -> Result<(), Box<dyn std::error::Error>> {
    reconcile_once(state).await
}

pub(crate) fn enqueue_workflow_run(
    state: Arc<AppState>,
    workflow: &crate::models::Workflow,
    trigger_type: &str,
    trigger_ref: Option<&str>,
    commit_sha: Option<&str>,
) -> Result<String, Box<dyn std::error::Error>> {
    let definition: WorkflowDefinition = serde_json::from_str(&workflow.definition_json)?;
    definition.validate().map_err(std::io::Error::other)?;
    let pipeline_id = state.db.create_pipeline_run(
        &workflow.repo_id,
        &workflow.id,
        &workflow.version_id,
        trigger_type,
        trigger_ref,
        commit_sha,
    )?;
    let mut run_ids = BTreeMap::new();
    for job in &definition.jobs {
        let run_id = state.db.create_job_run(
            &pipeline_id,
            &job.id,
            &job.name,
            &job.runner_id,
            &job.runner_job_name,
            job.allow_failure,
        )?;
        run_ids.insert(job.id.clone(), run_id);
    }
    for job in &definition.jobs {
        for need in &job.needs {
            if let (Some(job_run_id), Some(dep_run_id)) = (run_ids.get(&job.id), run_ids.get(need))
            {
                state.db.add_job_dependency(job_run_id, dep_run_id)?;
            }
        }
    }
    Ok(pipeline_id)
}

pub(crate) async fn cancel_pipeline(
    state: Arc<AppState>,
    pipeline_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let jobs = state
        .db
        .list_job_runs_for_pipeline(pipeline_id)
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    for job in jobs {
        let runner = state
            .db
            .get_runner(&job.runner_id)
            .map_err(|error| std::io::Error::other(error.to_string()))?;
        if job.status == "running"
            && let (Some(runner), Some(runner_run_id)) = (runner, job.runner_run_id.clone())
        {
            let _ = state
                .runner_client
                .cancel_job_run(&runner, &runner_run_id)
                .await;
        }
        if matches!(job.status.as_str(), "pending" | "running") {
            state.db.set_job_run_status(&job.id, "canceled")?;
        }
    }
    state.db.set_pipeline_status(pipeline_id, "canceled")?;
    Ok(())
}

async fn process_push_events(state: Arc<AppState>) -> Result<(), Box<dyn std::error::Error>> {
    let events = state.db.list_unprocessed_push_events()?;
    for event in events {
        let workflows = state.db.workflows_for_repo(&event.repo_id)?;
        for workflow in workflows.into_iter().filter(|item| item.enabled) {
            let trigger: WorkflowTrigger = serde_json::from_str(&workflow.trigger_json)?;
            if trigger.kind != "push" {
                continue;
            }
            for ref_update in &event.refs {
                if !matches_branch(&trigger.branches, &ref_update.ref_name) {
                    continue;
                }
                let pipeline_id = enqueue_workflow_run(
                    Arc::clone(&state),
                    &workflow,
                    "push",
                    Some(&ref_update.ref_name),
                    Some(&ref_update.new_rev),
                )?;
                info!(pipeline_id, workflow = %workflow.name, "pipeline created from push event");
            }
        }
        state.db.mark_push_event_processed(&event.id)?;
    }
    Ok(())
}

fn recover_incomplete_pipelines(state: Arc<AppState>) -> Result<(), Box<dyn std::error::Error>> {
    for pipeline in state.db.pipelines_requiring_recovery()? {
        state.db.set_pipeline_status(&pipeline.id, "running")?;
    }
    Ok(())
}

async fn dispatch_pending_jobs(state: Arc<AppState>) -> Result<(), Box<dyn std::error::Error>> {
    let jobs = state.db.list_job_runs_by_status(&["pending"])?;
    for job in jobs {
        let dependencies = state.db.dependencies_for_job_run(&job.id)?;
        if dependencies.iter().any(|item| item.status == "failed" && !item.allow_failure) {
            state.db.set_job_run_status(&job.id, "blocked")?;
            state.db.finalize_pipeline_status(&job.pipeline_run_id)?;
            continue;
        }
        if dependencies.iter().any(|item| matches!(item.status.as_str(), "pending" | "running")) {
            continue;
        }
        let Some(runner) = state.db.get_runner(&job.runner_id)? else {
            state.db.set_job_run_status(&job.id, "failed")?;
            continue;
        };
        if !runner.enabled {
            continue;
        }
        let Some(pipeline) = state.db.pipeline_for_job_run(&job.id)? else {
            continue;
        };
        let definition_json = state.db.workflow_definition_json(&pipeline.workflow_version_id)?;
        let definition: WorkflowDefinition = serde_json::from_str(&definition_json)?;
        let Some(job_definition) = definition.jobs.iter().find(|item| item.id == job.job_id) else {
            continue;
        };
        let payload = resolve_job_inputs(Arc::clone(&state), &pipeline, &job.id, job_definition).await?;
        let created = state
            .runner_client
            .create_job_run(&runner, &job.runner_job_name, Value::Object(payload))
            .await
            .map_err(|error| std::io::Error::other(error.to_string()))?;
        state.db.set_job_run_started(&job.id, &created.job_id)?;
    }
    Ok(())
}

async fn poll_running_jobs(state: Arc<AppState>) -> Result<(), Box<dyn std::error::Error>> {
    let jobs = state.db.list_job_runs_by_status(&["running"])?;
    for job in jobs {
        let Some(runner) = state.db.get_runner(&job.runner_id)? else {
            continue;
        };
        let Some(runner_run_id) = &job.runner_run_id else {
            continue;
        };
        match state.runner_client.get_job_run(&runner, runner_run_id).await {
            Ok(status) => {
                if !matches!(status.status.as_str(), "running") {
                    let logs = state.runner_client.get_job_logs(&runner, runner_run_id).await.unwrap_or_else(|error| {
                        error!(%error, "failed to fetch runner logs");
                        crate::runner::JobLogsResponse { stdout: String::new(), stderr: String::new() }
                    });
                    let outputs = status
                        .outputs
                        .into_iter()
                        .map(|(name, value)| (name, value.artifact_id, value.sha256, value.size))
                        .collect::<Vec<_>>();
                    let terminal = match status.status.as_str() {
                        "success" => "success",
                        "canceled" => "canceled",
                        _ => "failed",
                    };
                    state.db.finish_job_run(&job.id, terminal, &logs.stdout, &logs.stderr, &outputs)?;
                    state.db.finalize_pipeline_status(&job.pipeline_run_id)?;
                }
            }
            Err(error) => {
                warn!(%error, job_run_id = %job.id, "runner status polling failed");
            }
        }
    }
    Ok(())
}

async fn resolve_job_inputs(
    state: Arc<AppState>,
    pipeline: &crate::models::PipelineRun,
    job_run_id: &str,
    job_definition: &crate::models::WorkflowJobDefinition,
) -> Result<Map<String, Value>, Box<dyn std::error::Error>> {
    let mut resolved = Map::new();
    for (key, value) in &job_definition.inputs {
        match value {
            Value::String(raw) if raw == "$commit" => {
                resolved.insert(key.clone(), json!(pipeline.commit_sha.clone().unwrap_or_default()));
            }
            Value::String(raw) if raw == "$branch" => {
                resolved.insert(key.clone(), json!(pipeline.trigger_ref.clone().unwrap_or_default()));
            }
            Value::String(raw) if raw == "$source" => {
                let artifact_id = ensure_source_artifact(Arc::clone(&state), pipeline, &job_definition.runner_id).await?;
                resolved.insert(key.clone(), json!(artifact_id));
            }
            Value::String(raw) if raw.starts_with("$job.") => {
                let mut parts = raw.trim_start_matches("$job.").split('.');
                let source_job = parts.next().unwrap_or_default();
                let output_name = parts.next().unwrap_or_default();
                let upstream_run_id = find_job_run_id(Arc::clone(&state), &pipeline.id, source_job)?;
                let output = state
                    .db
                    .job_outputs(&upstream_run_id)?
                    .into_iter()
                    .find(|item| item.artifact_name == output_name)
                    .ok_or_else(|| format!("missing output {output_name} from {source_job}"))?;
                let artifact_id = ensure_runner_has_artifact(
                    Arc::clone(&state),
                    &job_definition.runner_id,
                    &output.runner_artifact_id,
                    &upstream_run_id,
                )
                .await?;
                resolved.insert(key.clone(), json!(artifact_id));
            }
            other => {
                resolved.insert(key.clone(), other.clone());
            }
        }
    }

    if !resolved.contains_key("strait_job_run_id") {
        resolved.insert("strait_job_run_id".to_string(), json!(job_run_id));
    }
    Ok(resolved)
}

fn find_job_run_id(
    state: Arc<AppState>,
    pipeline_id: &str,
    job_id: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let snapshot = state
        .db
        .pipeline_snapshot(pipeline_id)?
        .ok_or_else(|| format!("missing pipeline {pipeline_id}"))?;
    snapshot
        .jobs
        .into_iter()
        .find(|item| item.run.job_id == job_id)
        .map(|item| item.run.id)
        .ok_or_else(|| format!("missing upstream job {job_id}").into())
}

async fn ensure_source_artifact(
    state: Arc<AppState>,
    pipeline: &crate::models::PipelineRun,
    runner_id: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let Some(repo) = state.db.get_repo(&pipeline.repo_id)? else {
        return Err(format!("missing repo {}", pipeline.repo_id).into());
    };
    let commit_sha = pipeline
        .commit_sha
        .clone()
        .ok_or_else(|| "pipeline missing commit sha".to_string())?;
    let archive_path = PathBuf::from(&state.config.data_dir)
        .join("source-archives")
        .join(format!("{}-{}.tar.gz", pipeline.id, commit_sha));
    if !archive_path.exists() {
        git::create_source_archive(PathBuf::from(repo.bare_path).as_path(), &commit_sha, &archive_path)?;
    }
    let bytes = fs::read(&archive_path)?;
    let sha256 = format!("{:x}", Sha256::digest(&bytes));
    let Some(runner) = state.db.get_runner(runner_id)? else {
        return Err(format!("missing runner {runner_id}").into());
    };
    let upload = state
        .runner_client
        .upload_artifact(&runner, bytes, &sha256)
        .await
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    Ok(upload.artifact_id)
}

async fn ensure_runner_has_artifact(
    state: Arc<AppState>,
    target_runner_id: &str,
    source_artifact_id: &str,
    upstream_job_run_id: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let snapshot = state
        .db
        .pipeline_snapshot(
            &state
                .db
                .pipeline_for_job_run(upstream_job_run_id)?
                .ok_or("missing pipeline for job")?
                .id,
        )?
        .ok_or("missing pipeline snapshot")?;
    let upstream = snapshot
        .jobs
        .into_iter()
        .find(|item| item.run.id == upstream_job_run_id)
        .ok_or("missing upstream run")?;
    if upstream.run.runner_id == target_runner_id {
        return Ok(source_artifact_id.to_string());
    }
    let Some(source_runner) = state.db.get_runner(&upstream.run.runner_id)? else {
        return Err("missing source runner".into());
    };
    let Some(target_runner) = state.db.get_runner(target_runner_id)? else {
        return Err("missing target runner".into());
    };
    let bytes = state
        .runner_client
        .download_artifact(&source_runner, source_artifact_id)
        .await
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    let sha256 = format!("{:x}", Sha256::digest(&bytes));
    let upload = state
        .runner_client
        .upload_artifact(&target_runner, bytes, &sha256)
        .await
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    Ok(upload.artifact_id)
}

fn matches_branch(patterns: &[String], ref_name: &str) -> bool {
    if patterns.is_empty() {
        return true;
    }
    let branch = ref_name.trim_start_matches("refs/heads/");
    patterns.iter().any(|pattern| {
        Pattern::new(pattern)
            .map(|compiled| compiled.matches(branch) || compiled.matches(ref_name))
            .unwrap_or(false)
    })
}

async fn refresh_runner_health(state: Arc<AppState>) -> Result<(), Box<dyn std::error::Error>> {
    let runners = state.db.list_runners()?;
    for runner in runners.into_iter().filter(|item| item.enabled) {
        match state.runner_client.list_jobs(&runner).await {
            Ok(jobs) => {
                let payloads = jobs
                    .into_iter()
                    .map(|job| (job.name, serde_json::to_string(&job.definition).unwrap_or_else(|_| "{}".to_string())))
                    .collect::<Vec<_>>();
                state.db.replace_runner_jobs(&runner.id, &payloads)?;
                state.db.update_runner_health(&runner.id, "healthy")?;
            }
            Err(error) => {
                warn!(runner = %runner.name, %error, "runner health check failed");
                state.db.update_runner_health(&runner.id, "unreachable")?;
            }
        }
    }
    Ok(())
}
