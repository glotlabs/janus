use maud::{Markup, html};

use crate::models::Repo;

use super::components::{badge, csrf_input, form_error, layout, page_intro};

pub(crate) struct RepoCard {
    pub repo: Repo,
    pub clone_url: String,
}

pub(crate) struct RepoFormView {
    pub name: String,
    pub default_branch: String,
}

pub(crate) fn repos_page(
    repos: Vec<RepoCard>,
    csrf: &str,
    error: Option<&str>,
    form: RepoFormView,
) -> Markup {
    layout(
        "Repos",
        html! {
            (page_intro(
                "Repositories",
                "Register repos, copy clone URLs, and manually trigger pipeline runs.",
            ))
            section class="card" {
                div class="section-head" {
                    div {
                        div class="eyebrow" { "Source Control" }
                        h2 { "Create repository" }
                    }
                }
                form method="post" action="/repos" class="stack-lg" {
                    (csrf_input(csrf))
                    (form_error(error))
                    div class="form-grid form-grid-2" {
                        label { span { "Name" } input name="name" value=(form.name) maxlength="80" required data-validate data-trim-required="true"; }
                        label {
                            span { "Default branch" }
                            input name="default_branch" value=(form.default_branch) maxlength="255" required data-validate data-trim-required="true" data-no-whitespace="true";
                        }
                    }
                    div class="actions" {
                        button type="submit" { "Create repository" }
                    }
                }
            }
            section class="card" {
                div class="section-head" {
                    div {
                        div class="eyebrow" { "Inventory" }
                        h2 { "Available repositories" }
                    }
                }
                div class="card-grid" {
                    @for card in &repos {
                        article class="entity-card" {
                            div class="entity-head" {
                                div {
                                    h3 { (card.repo.name) }
                                    p class="muted" {
                                        "Default branch: " code { (card.repo.default_branch) }
                                    }
                                }
                                (badge("active", "success"))
                            }
                            div class="meta-pair" {
                                span { "Clone URL" }
                                code { (card.clone_url) }
                            }
                            form method="post" action=(format!("/repos/{}/trigger", card.repo.id)) class="stack-md inset-panel" {
                                (csrf_input(csrf))
                                div class="inline-fields" {
                                    label {
                                        span { "Branch ref" }
                                        input name="branch" value=(format!("refs/heads/{}", card.repo.default_branch)) data-validate data-no-whitespace="true";
                                    }
                                    label {
                                        span { "Commit" }
                                        input name="commit" value="HEAD" data-validate data-no-whitespace="true";
                                    }
                                }
                                div class="actions" {
                                    button type="submit" { "Trigger pipeline" }
                                }
                            }
                        }
                    }
                }
            }
        },
    )
}
