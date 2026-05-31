use std::collections::BTreeSet;
use std::sync::Arc;

use serde::Serialize;

use crate::{
    app::AppState,
    models::{RunnerJobDefinition, Workflow, WorkflowDefinition, WorkflowInputBinding},
};

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum WorkflowSchemaStatus {
    Current,
    Stale,
    Incompatible,
}

impl WorkflowSchemaStatus {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Current => "current",
            Self::Stale => "stale",
            Self::Incompatible => "incompatible",
        }
    }

    pub(crate) fn tone(self) -> &'static str {
        match self {
            Self::Current => "success",
            Self::Stale => "warning",
            Self::Incompatible => "danger",
        }
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct WorkflowSchemaReport {
    pub(crate) status: WorkflowSchemaStatus,
    pub(crate) diff: Vec<WorkflowSchemaDiff>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct WorkflowSchemaDiff {
    pub(crate) kind: WorkflowSchemaDiffKind,
    pub(crate) job_index: usize,
    pub(crate) job_name: String,
    pub(crate) field_name: Option<String>,
    pub(crate) saved: Option<String>,
    pub(crate) current: Option<String>,
    pub(crate) incompatible: bool,
    pub(crate) message: String,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum WorkflowSchemaDiffKind {
    JobRemoved,
    InputAdded,
    InputRemoved,
    OutputAdded,
    OutputRemoved,
    InputTypeChanged,
    OutputTypeChanged,
    InputRequirednessChanged,
    OutputRequirednessChanged,
}

pub(crate) fn workflow_schema_report(
    state: &Arc<AppState>,
    workflow: &Workflow,
) -> Result<WorkflowSchemaReport, Box<dyn std::error::Error>> {
    let definition: WorkflowDefinition = serde_json::from_str(&workflow.definition_json)?;
    let snapshot = state.db.workflow_job_schemas(&workflow.version_id)?;
    let mut diff = Vec::new();

    for (job_index, job) in definition.jobs.iter().enumerate() {
        let Some(saved_schema) = snapshot.get(job_index) else {
            diff.push(WorkflowSchemaDiff {
                kind: WorkflowSchemaDiffKind::JobRemoved,
                job_index,
                job_name: job.runner_job_name.clone(),
                field_name: None,
                saved: Some(job.runner_job_name.clone()),
                current: None,
                incompatible: true,
                message: format!(
                    "job-{} {} is missing from the saved schema snapshot",
                    job_index + 1,
                    job.runner_job_name
                ),
            });
            continue;
        };
        let current_schema = state
            .db
            .list_runner_jobs(&job.runner_id)?
            .into_iter()
            .find(|schema| schema.name == job.runner_job_name);
        let Some(current_schema) = current_schema else {
            diff.push(WorkflowSchemaDiff {
                kind: WorkflowSchemaDiffKind::JobRemoved,
                job_index,
                job_name: job.runner_job_name.clone(),
                field_name: None,
                saved: Some(saved_schema.name.clone()),
                current: None,
                incompatible: true,
                message: format!(
                    "job-{} {} is no longer advertised by its runner",
                    job_index + 1,
                    job.runner_job_name
                ),
            });
            continue;
        };

        diff.extend(diff_job_schema(
            job_index,
            &job.runner_job_name,
            saved_schema,
            &current_schema,
            workflow_bound_inputs(&definition, job_index),
            workflow_referenced_outputs(&definition, job_index),
        ));
    }

    for (extra_index, extra_schema) in snapshot.iter().enumerate().skip(definition.jobs.len()) {
        diff.push(WorkflowSchemaDiff {
            kind: WorkflowSchemaDiffKind::JobRemoved,
            job_index: extra_index,
            job_name: extra_schema.name.clone(),
            field_name: None,
            saved: Some(extra_schema.name.clone()),
            current: None,
            incompatible: true,
            message: format!(
                "job-{} {} exists in the saved schema snapshot but not in the workflow definition",
                extra_index + 1,
                extra_schema.name
            ),
        });
    }

    let status = if diff.iter().any(|item| item.incompatible) {
        WorkflowSchemaStatus::Incompatible
    } else if diff.iter().any(|item| item.is_stale()) {
        WorkflowSchemaStatus::Stale
    } else {
        WorkflowSchemaStatus::Current
    };

    Ok(WorkflowSchemaReport { status, diff })
}

fn diff_job_schema(
    job_index: usize,
    job_name: &str,
    saved_schema: &RunnerJobDefinition,
    current_schema: &RunnerJobDefinition,
    bound_inputs: BTreeSet<String>,
    referenced_outputs: BTreeSet<String>,
) -> Vec<WorkflowSchemaDiff> {
    let mut diff = Vec::new();

    for (name, current_input) in &current_schema.inputs {
        if !saved_schema.inputs.contains_key(name) {
            diff.push(WorkflowSchemaDiff {
                kind: WorkflowSchemaDiffKind::InputAdded,
                job_index,
                job_name: job_name.to_string(),
                field_name: Some(name.clone()),
                saved: None,
                current: Some(requiredness(current_input.required).to_string()),
                incompatible: current_input.required,
                message: if current_input.required {
                    format!(
                        "job-{} input {} was added and is required",
                        job_index + 1,
                        name
                    )
                } else {
                    format!(
                        "job-{} input {} was added and is optional",
                        job_index + 1,
                        name
                    )
                },
            });
        }
    }

    for (name, saved_input) in &saved_schema.inputs {
        let Some(current_input) = current_schema.inputs.get(name) else {
            let incompatible = saved_input.required || bound_inputs.contains(name);
            diff.push(WorkflowSchemaDiff {
                kind: WorkflowSchemaDiffKind::InputRemoved,
                job_index,
                job_name: job_name.to_string(),
                field_name: Some(name.clone()),
                saved: Some(saved_input.kind.as_str().to_string()),
                current: None,
                incompatible,
                message: format!("job-{} input {} was removed", job_index + 1, name),
            });
            continue;
        };

        if saved_input.kind != current_input.kind {
            let incompatible = saved_input.required || bound_inputs.contains(name);
            diff.push(WorkflowSchemaDiff {
                kind: WorkflowSchemaDiffKind::InputTypeChanged,
                job_index,
                job_name: job_name.to_string(),
                field_name: Some(name.clone()),
                saved: Some(saved_input.kind.as_str().to_string()),
                current: Some(current_input.kind.as_str().to_string()),
                incompatible,
                message: format!(
                    "job-{} input {} changed type from {} to {}",
                    job_index + 1,
                    name,
                    saved_input.kind.as_str(),
                    current_input.kind.as_str()
                ),
            });
        }

        if saved_input.required != current_input.required {
            diff.push(WorkflowSchemaDiff {
                kind: WorkflowSchemaDiffKind::InputRequirednessChanged,
                job_index,
                job_name: job_name.to_string(),
                field_name: Some(name.clone()),
                saved: Some(requiredness(saved_input.required).to_string()),
                current: Some(requiredness(current_input.required).to_string()),
                incompatible: current_input.required && !saved_input.required,
                message: format!(
                    "job-{} input {} changed requiredness from {} to {}",
                    job_index + 1,
                    name,
                    requiredness(saved_input.required),
                    requiredness(current_input.required)
                ),
            });
        }
    }

    for (name, current_output) in &current_schema.outputs {
        if !saved_schema.outputs.contains_key(name) {
            diff.push(WorkflowSchemaDiff {
                kind: WorkflowSchemaDiffKind::OutputAdded,
                job_index,
                job_name: job_name.to_string(),
                field_name: Some(name.clone()),
                saved: None,
                current: Some(format!(
                    "{} {}",
                    current_output.kind.as_str(),
                    requiredness(current_output.required)
                )),
                incompatible: false,
                message: format!("job-{} output {} was added", job_index + 1, name),
            });
        }
    }

    for (name, saved_output) in &saved_schema.outputs {
        let Some(current_output) = current_schema.outputs.get(name) else {
            let incompatible = saved_output.required || referenced_outputs.contains(name);
            diff.push(WorkflowSchemaDiff {
                kind: WorkflowSchemaDiffKind::OutputRemoved,
                job_index,
                job_name: job_name.to_string(),
                field_name: Some(name.clone()),
                saved: Some(saved_output.kind.as_str().to_string()),
                current: None,
                incompatible,
                message: format!("job-{} output {} was removed", job_index + 1, name),
            });
            continue;
        };

        if saved_output.kind != current_output.kind {
            let incompatible = saved_output.required || referenced_outputs.contains(name);
            diff.push(WorkflowSchemaDiff {
                kind: WorkflowSchemaDiffKind::OutputTypeChanged,
                job_index,
                job_name: job_name.to_string(),
                field_name: Some(name.clone()),
                saved: Some(saved_output.kind.as_str().to_string()),
                current: Some(current_output.kind.as_str().to_string()),
                incompatible,
                message: format!(
                    "job-{} output {} changed type from {} to {}",
                    job_index + 1,
                    name,
                    saved_output.kind.as_str(),
                    current_output.kind.as_str()
                ),
            });
        }

        if saved_output.required != current_output.required {
            diff.push(WorkflowSchemaDiff {
                kind: WorkflowSchemaDiffKind::OutputRequirednessChanged,
                job_index,
                job_name: job_name.to_string(),
                field_name: Some(name.clone()),
                saved: Some(requiredness(saved_output.required).to_string()),
                current: Some(requiredness(current_output.required).to_string()),
                incompatible: saved_output.required && !current_output.required,
                message: format!(
                    "job-{} output {} changed requiredness from {} to {}",
                    job_index + 1,
                    name,
                    requiredness(saved_output.required),
                    requiredness(current_output.required)
                ),
            });
        }
    }

    diff
}

impl WorkflowSchemaDiff {
    fn is_stale(&self) -> bool {
        !self.incompatible
            && !matches!(
                self.kind,
                WorkflowSchemaDiffKind::InputAdded | WorkflowSchemaDiffKind::OutputAdded
            )
    }
}

fn workflow_bound_inputs(definition: &WorkflowDefinition, job_index: usize) -> BTreeSet<String> {
    definition
        .jobs
        .get(job_index)
        .map(|job| job.inputs.keys().cloned().collect())
        .unwrap_or_default()
}

fn workflow_referenced_outputs(
    definition: &WorkflowDefinition,
    upstream_job_index: usize,
) -> BTreeSet<String> {
    definition
        .jobs
        .iter()
        .flat_map(|job| job.inputs.values())
        .filter_map(|binding| match binding {
            WorkflowInputBinding::JobOutput {
                job_index,
                output_name,
            } if *job_index == upstream_job_index => Some(output_name.clone()),
            _ => None,
        })
        .collect()
}

fn requiredness(required: bool) -> &'static str {
    if required { "required" } else { "optional" }
}
