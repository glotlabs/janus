use maud::{Markup, html};

use crate::models::User;

use super::components::{badge, csrf_input, layout, layout_public, page_intro};

pub(crate) fn login_page() -> Markup {
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
                    form method="post" action="/login" class="stack-lg" {
                        label { span { "Username" } input name="username" autocomplete="username"; }
                        label { span { "Password" } input name="password" type="password" autocomplete="current-password"; }
                        div class="actions" {
                            button type="submit" { "Login" }
                        }
                    }
                }
            }
        },
    )
}

pub(crate) fn users_page(users: Vec<User>, csrf: &str) -> Markup {
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
                    div class="form-grid form-grid-3" {
                        label { span { "Username" } input name="username"; }
                        label { span { "Password" } input name="password" type="password"; }
                        label {
                            span { "Role" }
                            select name="role" {
                                option value="developer" { "developer" }
                                option value="admin" { "admin" }
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
