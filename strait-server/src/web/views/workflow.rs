use maud::{Markup, PreEscaped, html};

use crate::models::{Repo, Workflow, WorkflowTrigger};

use super::components::{badge, csrf_input, layout, page_intro, x_mark};
use crate::web::routes::WorkflowSchemaStatus;

pub(crate) struct WorkflowCard {
    pub workflow: Workflow,
    pub repo: Repo,
    pub schema_status: WorkflowSchemaStatus,
    pub trigger: WorkflowTrigger,
    pub job_count: usize,
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
                                }
                                div class="badge-row" {
                                    (badge(card.schema_status.as_str(), card.schema_status.tone()))
                                    (badge(&format!("v{}", card.workflow.version), "neutral"))
                                }
                            }
                            div class="meta-grid" {
                                div class="meta-pair" { span { "Repo" } strong { (card.repo.name) } }
                                div class="meta-pair" { span { "Branches" } strong { (card.trigger.branches.join(", ")) } }
                                div class="meta-pair" { span { "Trigger" } strong { (card.trigger.kind) } }
                                div class="meta-pair" { span { "Jobs" } strong { (card.job_count) } }
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
                template id="workflow-job-row-template" {
                    fieldset class="job-builder-row" data-workflow-job-row="true" {
                        label {
                            span { "Runner" }
                            select data-field="runner_id" {}
                        }
                        label {
                            span { "Job" }
                            select data-field="runner_job_name" {}
                        }
                        label {
                            span { "Outcome" }
                            select data-field="outcome_policy" {}
                        }
                        label {
                            span { "Inputs" }
                            button type="button" class="input-summary-trigger ghost" data-input-summary="true" {}
                        }
                        label {
                            span { "Outputs" }
                            button type="button" class="input-summary-trigger ghost" data-output-summary="true" {}
                        }
                        dialog class="inputs-dialog" data-inputs-dialog="true" {
                            div class="dialog-card" {
                                div class="section-head" {
                                    div {
                                        div class="eyebrow" { "Inputs" }
                                        h3 { "Configure job inputs" }
                                        p class="muted" {
                                            "Bindings are saved back into the workflow when you submit the form."
                                        }
                                    }
                                    button type="button" class="ghost" data-dialog-close="inputs" { "Close" }
                                }
                                div class="inputs-wrap" data-inputs-wrap="true" {}
                                div class="actions" {
                                    button type="button" data-dialog-close="inputs" { "Done" }
                                }
                            }
                        }
                        dialog class="inputs-dialog" data-outputs-dialog="true" {
                            div class="dialog-card" {
                                div class="section-head" {
                                    div {
                                        div class="eyebrow" { "Outputs" }
                                        h3 { "Declared job outputs" }
                                        p class="muted" {
                                            "Runner jobs can expose artifact or typed outputs, which downstream jobs can consume."
                                        }
                                    }
                                    button type="button" class="ghost" data-dialog-close="outputs" { "Close" }
                                }
                                div class="inputs-wrap" data-outputs-wrap="true" {}
                                div class="actions" {
                                    button type="button" data-dialog-close="outputs" { "Done" }
                                }
                            }
                        }
                        div class="job-row-remove" {
                            button type="button" class="job-remove-button ghost" data-remove-job="true" aria-label="Remove job" title="Remove job" { (x_mark()) }
                        }
                    }
                }
                template id="workflow-inputs-empty-template" {
                    div class="muted" { "None" }
                }
                template id="workflow-outputs-empty-template" {
                    div class="muted" { "None" }
                }
                template id="workflow-inputs-table-template" {
                    table class="inputs-table" {
                        thead {
                            tr {
                                th { "Input" }
                                th { "Type" }
                                th { "Binding" }
                                th { "Value" }
                            }
                        }
                        tbody data-table-body="true" {}
                    }
                }
                template id="workflow-outputs-table-template" {
                    table class="inputs-table" {
                        thead {
                            tr {
                                th { "Output" }
                                th { "Type" }
                                th { "Required" }
                            }
                        }
                        tbody data-table-body="true" {}
                    }
                }
                div id="workflow-job-list" class="stack-md" {}
                div class="actions" {
                    button type="button" id="workflow-add-job" class="secondary" { "Add job" }
                }
            }
        }
        textarea id="workflow-jobs-json" name="jobs_json" hidden {
            (form.jobs_json)
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
