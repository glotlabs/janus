use maud::{Markup, PreEscaped, html};

use crate::models::{Repo, Workflow, WorkflowDefinition, WorkflowTrigger};

use super::components::{badge, csrf_input, layout, page_intro, render_workflow_job_chips};
use crate::web::routes::WorkflowSchemaStatus;

pub(crate) struct WorkflowCard {
    pub workflow: Workflow,
    pub schema_status: WorkflowSchemaStatus,
    pub trigger: WorkflowTrigger,
    pub definition: WorkflowDefinition,
}

pub(crate) struct WorkflowFormView {
    pub name: String,
    pub trigger_kind: String,
    pub branch_name: String,
    pub jobs_json: String,
    pub repo_field: Markup,
    pub runner_catalog_json: PreEscaped<String>,
    pub initial_jobs_json: PreEscaped<String>,
    pub is_edit: bool,
}

pub(crate) fn workflows_page(
    form: WorkflowFormView,
    workflows: Vec<WorkflowCard>,
    csrf: &str,
) -> Markup {
    layout(
        "Workflows",
        html! {
            (page_intro(
                "Workflows",
                "Define reusable CI pipelines with structured jobs, serial execution order, and runner bindings.",
            ))
            section class="card" {
                div class="section-head" {
                    div {
                        div class="eyebrow" { "Automation" }
                        h2 { "Create workflow" }
                    }
                }
                form method="post" action="/workflows" class="stack-lg" {
                    (csrf_input(csrf))
                    (workflow_form_fields(form))
                }
            }
            section class="card" {
                div class="section-head" {
                    div {
                        div class="eyebrow" { "Catalog" }
                        h2 { "Existing workflows" }
                    }
                }
                div class="card-grid" {
                    @for card in &workflows {
                        article class="entity-card" {
                            div class="entity-head" {
                                div {
                                    h3 {
                                        a href=(format!("/workflows/{}", card.workflow.id)) { (card.workflow.name) }
                                    }
                                    p class="muted" { "Repo: " code { (card.workflow.repo_id) } }
                                }
                                div class="badge-row" {
                                    (badge("workflow", "neutral"))
                                    (badge(card.schema_status.as_str(), card.schema_status.tone()))
                                }
                            }
                            div class="meta-grid" {
                                div class="meta-pair" { span { "Trigger" } strong { (card.trigger.kind) } }
                                div class="meta-pair" { span { "Branches" } strong { (card.trigger.branches.join(", ")) } }
                                div class="meta-pair" { span { "Version" } strong { (card.workflow.version) } }
                                div class="meta-pair" { span { "Jobs" } strong { (card.definition.jobs.len()) } }
                            }
                            div class="chip-row" {
                                (render_workflow_job_chips(&card.definition))
                            }
                        }
                    }
                }
            }
        },
    )
}

pub(crate) fn workflow_detail_page(
    workflow: &Workflow,
    repo: &Repo,
    schema_status: WorkflowSchemaStatus,
    form: WorkflowFormView,
    csrf: &str,
) -> Markup {
    layout(
        "Workflow",
        html! {
            (page_intro(
                "Workflow Detail",
                "Adjust trigger behavior, job order, and runner/job bindings.",
            ))
            section class="card" {
                div class="section-head" {
                    div {
                        div class="eyebrow" { "Editing" }
                        h2 { (workflow.name) }
                        p class="muted" { "Repository: " (repo.owner_username) "/" (repo.name) }
                    }
                    div class="badge-row" {
                        (badge(schema_status.as_str(), schema_status.tone()))
                    }
                }
                form method="post" action=(format!("/workflows/{}/update", workflow.id)) class="stack-lg" {
                    (csrf_input(csrf))
                    (workflow_form_fields(form))
                }
            }
        },
    )
}

pub(crate) fn repo_selector(repos: &[Repo]) -> Markup {
    html! {
        select name="repo_id" {
            @for repo in repos {
                option value=(repo.id) { (repo.owner_username) "/" (repo.name) }
            }
        }
    }
}

pub(crate) fn fixed_repo_field(workflow: &Workflow, repo: &Repo) -> Markup {
    html! {
        input type="hidden" name="repo_id" value=(workflow.repo_id);
        input value=(format!("{}/{}", repo.owner_username, repo.name)) disabled;
    }
}

pub(crate) fn script_json(input: &str) -> PreEscaped<String> {
    PreEscaped(input.replace("</script", "<\\/script"))
}

fn workflow_form_fields(form: WorkflowFormView) -> Markup {
    html! {
        div class="form-grid form-grid-2" {
            label {
                span { "Workflow name" }
                input name="name" value=(form.name);
            }
            label {
                span { "Trigger" }
                select name="trigger_kind" {
                    option value="push" selected[form.trigger_kind == "push"] { "push" }
                    option value="manual" selected[form.trigger_kind == "manual"] { "manual" }
                }
            }
        }
        div class="form-grid form-grid-2" {
            label {
                span { "Repository" }
                (form.repo_field)
            }
            label {
                span { "Branch" }
                input name="branch_name" value=(form.branch_name);
            }
        }
        div class="card soft-card" {
            div class="section-head" {
                div {
                    div class="eyebrow" { "Jobs" }
                    h3 { "Workflow builder" }
                    p class="muted" {
                        "Jobs run one by one in the order shown below. Inputs are rendered from the selected runner job manifest."
                    }
                }
            }
            div id="workflow-builder" class="stack-md" {
                div id="workflow-job-list" class="stack-md" {}
                div class="actions" {
                    button type="button" id="workflow-add-job" class="secondary" { "Add job" }
                }
            }
        }
        textarea id="workflow-jobs-json" name="jobs_json" hidden {
            (form.jobs_json)
        }
        div class="inline-note" {
            "Artifact inputs can point to " code { "source.tar.gz" }
            ". Typed inputs can bind to matching outputs from earlier jobs in the workflow."
        }
        script type="application/json" id="workflow-runner-catalog" {
            (form.runner_catalog_json)
        }
        script type="application/json" id="workflow-initial-jobs" {
            (form.initial_jobs_json)
        }
        script type="module" src="/assets/workflow_builder.js" {}
        div class="actions" {
            button type="submit" {
                @if form.is_edit { "Save workflow" } @else { "Create workflow" }
            }
        }
    }
}
