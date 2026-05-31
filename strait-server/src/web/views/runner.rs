use maud::{Markup, html};

use crate::models::{Runner, RunnerJobDefinition};

use super::components::{badge, csrf_input, layout, page_intro, runner_state_tone};

pub(crate) fn runners_page(runners: Vec<(Runner, Vec<RunnerJobDefinition>)>, csrf: &str) -> Markup {
    layout(
        "Runners",
        html! {
            (page_intro(
                "Runners",
                "Register execution backends, refresh their advertised jobs, and control availability.",
            ))
            section class="card" {
                div class="section-head" {
                    div {
                        div class="eyebrow" { "Execution" }
                        h2 { "Add runner" }
                    }
                }
                form method="post" action="/runners" class="stack-lg" {
                    (csrf_input(csrf))
                    div class="form-grid form-grid-3" {
                        label { span { "Name" } input name="name"; }
                        label {
                            span { "Base URL" }
                            input name="base_url" placeholder="http://127.0.0.1:8080";
                        }
                        label { span { "Token" } input name="token"; }
                    }
                    div class="actions" {
                        button type="submit" { "Add runner" }
                    }
                }
            }
            section class="card" {
                div class="section-head" {
                    div {
                        div class="eyebrow" { "Fleet" }
                        h2 { "Connected runners" }
                    }
                }
                div class="card-grid" {
                    @for (runner, jobs) in &runners {
                        article class="entity-card" {
                            div class="entity-head" {
                                div {
                                    h3 { (runner.name) }
                                    p class="muted" { (runner.base_url) }
                                }
                                div class="badge-row" {
                                    (badge(&runner.last_health_state, runner_state_tone(&runner.last_health_state)))
                                    (badge(
                                        if runner.enabled { "enabled" } else { "disabled" },
                                        if runner.enabled { "success" } else { "danger" },
                                    ))
                                }
                            }
                            div class="meta-pair" {
                                span { "Runner ID" }
                                code { (runner.id) }
                            }
                            form method="post" action=(format!("/runners/{}/update", runner.id)) class="stack-md" {
                                (csrf_input(csrf))
                                label {
                                    span { "Runner name" }
                                    input name="name" value=(runner.name);
                                }
                                div class="actions" {
                                    button type="submit" class="secondary" { "Save name" }
                                }
                            }
                            div class="actions" {
                                form method="post" action=(format!("/runners/{}/test", runner.id)) {
                                    (csrf_input(csrf))
                                    button type="submit" class="secondary" { "Refresh jobs" }
                                }
                                form method="post" action=(format!("/runners/{}/toggle", runner.id)) {
                                    (csrf_input(csrf))
                                    button type="submit" class="ghost" {
                                        @if runner.enabled { "Disable runner" } @else { "Enable runner" }
                                    }
                                }
                            }
                            @if !jobs.is_empty() {
                                div class="subsection" {
                                    span class="subsection-title" { "Advertised jobs" }
                                    div class="chip-row" {
                                        @for job in jobs {
                                            span class="chip" { (job.name) }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        },
    )
}
