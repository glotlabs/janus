use std::collections::BTreeMap;

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
    pub job_index: i64,
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
    pub runner_id: String,
    pub runner_job_name: String,
    #[serde(default)]
    pub inputs: BTreeMap<String, WorkflowInputBinding>,
    #[serde(default)]
    pub allow_failure: bool,
}

impl WorkflowDefinition {
    pub fn validate(&self) -> Result<(), String> {
        if self.jobs.is_empty() {
            return Err("workflow must contain at least one job".to_string());
        }
        Ok(())
    }
}

impl WorkflowJobDefinition {
    pub fn display_name(&self, job_index: usize) -> String {
        format!("job-{} / {}", job_index + 1, self.runner_job_name)
    }
}

impl JobRun {
    pub fn display_name(&self) -> String {
        format!("job-{} / {}", self.job_index + 1, self.runner_job_name)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WorkflowInputBinding {
    Literal {
        value: Value,
    },
    Commit,
    Branch,
    SourceArtifact,
    JobOutput {
        job_index: usize,
        output_name: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobOutputBinding {
    pub job_index: usize,
    pub output_name: String,
}

pub fn parse_job_output_binding(value: &WorkflowInputBinding) -> Option<JobOutputBinding> {
    let WorkflowInputBinding::JobOutput {
        job_index,
        output_name,
    } = value
    else {
        return None;
    };
    if output_name.is_empty() {
        return None;
    }
    Some(JobOutputBinding {
        job_index: *job_index,
        output_name: output_name.clone(),
    })
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
    pub previous_jobs: Vec<PreviousJobSummary>,
    pub resolved_inputs: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreviousJobSummary {
    pub job_run_id: String,
    pub job_index: i64,
    pub runner_job_name: String,
    pub status: String,
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
