use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::runner::JobOutputMetadata;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    pub id: String,
    pub username: String,
    pub role: String,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Repo {
    pub id: String,
    pub owner_id: String,
    pub owner_username: String,
    pub name: String,
    pub normalized_name: String,
    pub bare_path: String,
    pub default_branch: String,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Runner {
    pub id: String,
    pub name: String,
    pub base_url: String,
    pub token: String,
    pub enabled: bool,
    pub last_health_state: String,
    pub last_seen_at: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Workflow {
    pub id: String,
    pub repo_id: String,
    pub name: String,
    pub enabled: bool,
    pub created_at: String,
    pub version: i64,
    pub version_id: String,
    pub trigger_json: String,
    pub definition_json: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PushEvent {
    pub id: String,
    pub repo_id: String,
    pub received_at: String,
    pub event_key: String,
    pub processed_at: Option<String>,
    pub refs: Vec<PushEventRef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PushEventRef {
    pub old_rev: String,
    pub new_rev: String,
    pub ref_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineRun {
    pub id: String,
    pub repo_id: String,
    pub workflow_id: String,
    pub workflow_version_id: String,
    pub trigger_type: String,
    pub trigger_ref: Option<String>,
    pub commit_sha: Option<String>,
    pub status: String,
    pub started_at: String,
    pub cancel_reason: Option<String>,
    pub cancel_requested_at: Option<String>,
    pub cancel_started_at: Option<String>,
    pub finished_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobRun {
    pub id: String,
    pub pipeline_run_id: String,
    pub job_id: String,
    pub job_name: String,
    pub runner_id: String,
    pub runner_job_name: String,
    pub dispatch_idempotency_key: String,
    pub runner_run_id: Option<String>,
    pub status: String,
    pub allow_failure: bool,
    pub started_at: Option<String>,
    pub duration_ms: Option<i64>,
    pub exit_code: Option<i32>,
    pub terminal_reason: Option<String>,
    pub failure_category: Option<String>,
    pub cancel_reason: Option<String>,
    pub cancel_requested_at: Option<String>,
    pub cancel_started_at: Option<String>,
    pub cancel_retry_count: i64,
    pub last_cancel_retry_at: Option<String>,
    pub infra_retry_count: i64,
    pub last_infra_retry_at: Option<String>,
    pub finished_at: Option<String>,
    pub output_metadata: JobOutputMetadata,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowTrigger {
    pub kind: String,
    #[serde(default)]
    pub branches: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowDefinition {
    pub jobs: Vec<WorkflowJobDefinition>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowJobDefinition {
    pub id: String,
    pub name: String,
    pub runner_id: String,
    pub runner_job_name: String,
    #[serde(default)]
    pub needs: Vec<String>,
    #[serde(default)]
    pub inputs: BTreeMap<String, Value>,
    #[serde(default)]
    pub artifacts_from: Vec<String>,
    #[serde(default)]
    pub allow_failure: bool,
}

impl WorkflowDefinition {
    pub fn validate(&self) -> Result<(), String> {
        if self.jobs.is_empty() {
            return Err("workflow must contain at least one job".to_string());
        }

        let ids: BTreeSet<_> = self.jobs.iter().map(|job| job.id.as_str()).collect();
        if ids.len() != self.jobs.len() {
            return Err("workflow job ids must be unique".to_string());
        }

        for job in &self.jobs {
            for need in &job.needs {
                if !ids.contains(need.as_str()) {
                    return Err(format!("job {} depends on unknown job {}", job.id, need));
                }
                if need == &job.id {
                    return Err(format!("job {} cannot depend on itself", job.id));
                }
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineSnapshot {
    pub pipeline: PipelineRun,
    pub jobs: Vec<JobRunDetail>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobRunDetail {
    pub run: JobRun,
    pub stdout: String,
    pub stderr: String,
    pub outputs: Vec<JobRunOutput>,
    pub dependencies: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobRunOutput {
    pub output_name: String,
    #[serde(rename = "type")]
    pub kind: String,
    pub runner_artifact_id: Option<String>,
    pub server_artifact_id: Option<String>,
    pub value: Option<Value>,
    pub sha256: Option<String>,
    pub size_bytes: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerArtifact {
    pub id: String,
    pub scope_type: String,
    pub scope_id: String,
    pub artifact_name: String,
    pub sha256: String,
    pub size_bytes: i64,
    pub storage_path: String,
    pub created_at: String,
}
