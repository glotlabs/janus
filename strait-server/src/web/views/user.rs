use maud::{Markup, html};

use crate::models::User;

use super::components::{badge, csrf_input, form_error, layout, layout_public, page_intro};

#[derive(Default)]
pub(crate) struct CreateUserFormView {
    pub username: String,
    pub role: String,
}

pub(crate) fn login_page(error: Option<&str>, username: &str) -> Markup {
    layout_public(
        "Login",
        html! {
            section class="auth-shell" {
                div class="hero-card auth-card" {
                    div class="eyebrow" { "Strait CI" }
                    h1 { "Sign in" }
                    p class="muted" {
                        "Manage repositories, runners, workflows, and pipeline execution from one place."
                    }
                    (form_error(error))
                    form method="post" action="/login" class="stack-lg" {
                        label { span { "Username" } input name="username" value=(username) autocomplete="username" required data-validate data-trim-required="true"; }
                        label { span { "Password" } input name="password" type="password" autocomplete="current-password" required data-validate; }
                        div class="actions" {
                            button type="submit" { "Login" }
                        }
                    }
                }
            }
        },
    )
}

pub(crate) fn users_page(
    users: Vec<User>,
    csrf: &str,
    error: Option<&str>,
    form: CreateUserFormView,
) -> Markup {
    layout(
        "Users",
        html! {
            (page_intro(
                "Users",
                "Create accounts and manage access levels for the CI instance.",
            ))
            section class="card" {
                div class="section-head" {
                    div {
                        div class="eyebrow" { "Access" }
                        h2 { "Create user" }
                    }
                }
                form method="post" action="/users" class="stack-lg" {
                    (csrf_input(csrf))
                    (form_error(error))
                    div class="form-grid form-grid-3" {
                        label { span { "Username" } input name="username" value=(form.username) required minlength="3" maxlength="64" data-validate data-trim-required="true" data-username="true"; }
                        label { span { "Password" } input name="password" type="password" required minlength="8" data-validate; }
                        label {
                            span { "Role" }
                            select name="role" required {
                                option value="developer" selected[form.role != "admin"] { "developer" }
                                option value="admin" selected[form.role == "admin"] { "admin" }
                            }
                        }
                    }
                    div class="actions" {
                        button type="submit" { "Create user" }
                    }
                }
            }
            section class="card" {
                div class="section-head" {
                    div {
                        div class="eyebrow" { "Directory" }
                        h2 { "Current users" }
                    }
                }
                div class="table-wrap" {
                    table {
                        thead {
                            tr { th { "Username" } th { "Role" } }
                        }
                        tbody {
                            @for item in users {
                                tr {
                                    td { strong { (item.username) } }
                                    td { (badge(&item.role, "neutral")) }
                                }
                            }
                        }
                    }
                }
            }
        },
    )
}
