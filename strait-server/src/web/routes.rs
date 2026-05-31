use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::Arc,
};

use axum::{
    Form, Json, Router,
    extract::{Path as AxumPath, State},
    http::{HeaderMap, StatusCode, header::SET_COOKIE},
    response::{Html, IntoResponse, Redirect, Response, Sse},
    routing::{get, post},
};
use chrono::{Duration, Utc};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::Sha256;
use tokio::time;
use uuid::Uuid;

use crate::{
    app::AppState,
    auth::{
        AdminUser, CurrentUser, clear_session_cookie, hash_password, parse_session_cookie,
        session_cookie, verify_password,
    },
    git,
    models::{
        self, PipelineRun, Repo, RunnerJobSchema, User, Workflow, WorkflowDefinition,
        WorkflowInputBinding, WorkflowJobDefinition, WorkflowTrigger, parse_job_output_binding,
    },
    scheduler,
};

type HmacSha256 = Hmac<Sha256>;

pub(crate) fn build_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/login", get(login_form).post(login))
        .route("/logout", post(logout))
        .route("/health", get(health))
        .route("/ready", get(ready))
        .route("/users", get(users_page).post(create_user))
        .route("/repos", get(repos_page).post(create_repo))
        .route("/repos/{repo_id}/trigger", post(trigger_repo))
        .route("/runners", get(runners_page).post(create_runner))
        .route("/runners/{runner_id}/toggle", post(toggle_runner))
        .route("/runners/{runner_id}/test", post(test_runner))
        .route("/workflows", get(workflows_page).post(create_workflow))
        .route("/workflows/{workflow_id}", get(workflow_detail_page))
        .route("/workflows/{workflow_id}/update", post(update_workflow))
        .route("/pipelines", get(pipelines_page))
        .route("/pipelines/{pipeline_id}", get(pipeline_detail))
        .route("/pipelines/{pipeline_id}/events", get(pipeline_events))
        .route("/pipelines/{pipeline_id}/rerun", post(rerun_pipeline))
        .route(
            "/pipelines/{pipeline_id}/cancel",
            post(cancel_pipeline_route),
        )
        .route("/api/me", get(api_me))
        .route("/api/repos", get(api_list_repos).post(api_create_repo))
        .route(
            "/api/runners",
            get(api_list_runners).post(api_create_runner),
        )
        .route(
            "/api/workflows",
            get(api_list_workflows).post(api_create_workflow),
        )
        .route(
            "/api/workflows/{workflow_id}",
            get(api_get_workflow).put(api_update_workflow),
        )
        .route("/api/pipelines", get(api_list_pipelines))
        .route("/api/pipelines/{pipeline_id}", get(api_get_pipeline))
        .with_state(state)
}

async fn health() -> Html<String> {
    Html("<pre>ok</pre>".to_string())
}
async fn ready() -> Html<String> {
    Html("<pre>ready</pre>".to_string())
}
async fn index() -> impl IntoResponse {
    Redirect::to("/repos")
}

async fn login_form() -> Html<String> {
    Html(layout_public(
        "Login",
        r#"<section class="auth-shell"><div class="hero-card auth-card"><div class="eyebrow">Strait CI</div><h1>Sign in</h1><p class="muted">Manage repositories, runners, workflows, and pipeline execution from one place.</p><form method="post" action="/login" class="stack-lg"><label><span>Username</span><input name="username" autocomplete="username" /></label><label><span>Password</span><input name="password" type="password" autocomplete="current-password" /></label><div class="actions"><button type="submit">Login</button></div></form></div></section>"#,
    ))
}

#[derive(Deserialize)]
struct LoginForm {
    username: String,
    password: String,
}

async fn login(State(state): State<Arc<AppState>>, Form(form): Form<LoginForm>) -> Response {
    if !state.allow_login_attempt(&form.username) {
        return (
            StatusCode::TOO_MANY_REQUESTS,
            "too many login attempts, try again later",
        )
            .into_response();
    }
    let Ok(Some((user, hash))) = state.db.get_user_credentials(&form.username) else {
        return crate::auth::unauthorized();
    };
    if !verify_password(&form.password, &hash) {
        return crate::auth::unauthorized();
    }
    let _ = state.db.cleanup_expired_sessions();
    let _ = state.db.delete_sessions_for_user(&user.id);
    let expires_at =
        (Utc::now() + Duration::days(state.config.auth.session_ttl_days as i64)).to_rfc3339();
    let Ok(session_id) = state.db.create_session(&user.id, &expires_at) else {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to create session",
        )
            .into_response();
    };
    let mut response = Redirect::to("/repos").into_response();
    response.headers_mut().append(
        SET_COOKIE,
        session_cookie(
            &state.config.auth.session_secret,
            &session_id,
            state.config.auth.session_cookie_secure,
        )
        .to_string()
        .parse()
        .expect("cookie header"),
    );
    response
}

async fn logout(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    if let Some(cookie) = headers.get("cookie").and_then(|value| value.to_str().ok())
        && let Some(session_id) = parse_session_cookie(&state.config.auth.session_secret, cookie)
    {
        let _ = state.db.delete_session(&session_id);
    }
    let mut response = Redirect::to("/login").into_response();
    response.headers_mut().append(
        SET_COOKIE,
        clear_session_cookie()
            .to_string()
            .parse()
            .expect("cookie header"),
    );
    response
}

#[derive(Deserialize)]
struct CsrfOnlyForm {
    csrf_token: String,
}
#[derive(Deserialize)]
struct CreateUserForm {
    csrf_token: String,
    username: String,
    password: String,
    role: String,
}
#[derive(Deserialize)]
struct CreateRepoForm {
    csrf_token: String,
    owner_id: String,
    name: String,
    default_branch: String,
}
#[derive(Deserialize)]
struct CreateRunnerForm {
    csrf_token: String,
    name: String,
    base_url: String,
    token: String,
}
#[derive(Deserialize)]
struct WorkflowForm {
    csrf_token: String,
    repo_id: String,
    name: String,
    trigger_kind: String,
    branch_name: String,
    jobs_json: String,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum WorkflowSchemaStatus {
    Current,
    Stale,
    Incompatible,
}

impl WorkflowSchemaStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Current => "current",
            Self::Stale => "stale",
            Self::Incompatible => "incompatible",
        }
    }

    fn tone(self) -> &'static str {
        match self {
            Self::Current => "success",
            Self::Stale => "warning",
            Self::Incompatible => "danger",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct WorkflowApiView {
    #[serde(flatten)]
    workflow: Workflow,
    schema_status: WorkflowSchemaStatus,
}
#[derive(Deserialize)]
struct ManualTriggerForm {
    csrf_token: String,
    branch: Option<String>,
    commit: Option<String>,
}

async fn users_page(
    _: AdminUser,
    CurrentUser(user): CurrentUser,
    State(state): State<Arc<AppState>>,
) -> Result<Html<String>, Response> {
    let csrf = csrf_token(&state, &user);
    let users = state.db.list_users().map_err(internal_error)?;
    let mut body = page_intro(
        "Users",
        "Create accounts and manage access levels for the CI instance.",
    );
    body.push_str(&format!(
        r#"<section class="card"><div class="section-head"><div><div class="eyebrow">Access</div><h2>Create user</h2></div></div><form method="post" action="/users" class="stack-lg">{}<div class="form-grid form-grid-3"><label><span>Username</span><input name="username" /></label><label><span>Password</span><input name="password" type="password" /></label><label><span>Role</span><select name="role"><option value="developer">developer</option><option value="admin">admin</option></select></label></div><div class="actions"><button type="submit">Create user</button></div></form></section>"#,
        csrf_input(&csrf)
    ));
    body.push_str(r#"<section class="card"><div class="section-head"><div><div class="eyebrow">Directory</div><h2>Current users</h2></div></div><div class="table-wrap"><table><thead><tr><th>Username</th><th>Role</th></tr></thead><tbody>"#);
    for item in users {
        body.push_str(&format!(
            "<tr><td><strong>{}</strong></td><td>{}</td></tr>",
            html_escape(&item.username),
            badge(&item.role, "neutral")
        ));
    }
    body.push_str("</tbody></table></div></section>");
    Ok(Html(layout("Users", &body)))
}

async fn create_user(
    _: AdminUser,
    CurrentUser(user): CurrentUser,
    State(state): State<Arc<AppState>>,
    Form(form): Form<CreateUserForm>,
) -> Result<Redirect, Response> {
    verify_csrf(&state, &user, &form.csrf_token)?;
    validate_username(&form.username)?;
    validate_password(&form.password)?;
    validate_role(&form.role)?;
    let hash = hash_password(&form.password).map_err(internal_error_text)?;
    state
        .db
        .create_user(&form.username, &hash, &form.role)
        .map_err(internal_error)?;
    Ok(Redirect::to("/users"))
}

async fn repos_page(
    CurrentUser(user): CurrentUser,
    State(state): State<Arc<AppState>>,
) -> Result<Html<String>, Response> {
    let repos = state.db.list_repos().map_err(internal_error)?;
    let users = state.db.list_users().map_err(internal_error)?;
    let csrf = csrf_token(&state, &user);
    let mut body = page_intro(
        "Repositories",
        "Register repos, copy clone URLs, and manually trigger pipeline runs.",
    );
    body.push_str(&format!(
        r#"<section class="card"><div class="section-head"><div><div class="eyebrow">Source Control</div><h2>Create repository</h2></div></div><form method="post" action="/repos" class="stack-lg">{}<div class="form-grid form-grid-3"><label><span>Name</span><input name="name" /></label>"#,
        csrf_input(&csrf)
    ));
    if user.role == "admin" {
        body.push_str(r#"<label><span>Owner</span><select name="owner_id">"#);
        for candidate in users {
            let selected = if candidate.id == user.id {
                " selected"
            } else {
                ""
            };
            body.push_str(&format!(
                "<option value=\"{}\"{}>{}</option>",
                candidate.id,
                selected,
                html_escape(&candidate.username)
            ));
        }
        body.push_str("</select></label>");
    } else {
        body.push_str(&format!(
            "<input type=\"hidden\" name=\"owner_id\" value=\"{}\" /><label><span>Owner</span><input value=\"{}\" disabled /></label>",
            user.id,
            html_escape(&user.username)
        ));
    }
    body.push_str(r#"<label><span>Default branch</span><input name="default_branch" value="main" /></label></div><div class="actions"><button type="submit">Create repository</button></div></form></section>"#);
    body.push_str(r#"<section class="card"><div class="section-head"><div><div class="eyebrow">Inventory</div><h2>Available repositories</h2></div></div><div class="card-grid">"#);
    for repo in repos.into_iter().filter(|repo| can_view_repo(&user, repo)) {
        let clone_url = repo_clone_url(&state, &repo);
        body.push_str(&format!(
            r#"<article class="entity-card"><div class="entity-head"><div><h3>{}/{}</h3><p class="muted">Default branch: <code>{}</code></p></div>{}</div><div class="meta-pair"><span>Clone URL</span><code>{}</code></div><form method="post" action="/repos/{}/trigger" class="stack-md inset-panel">{}<div class="inline-fields"><label><span>Branch ref</span><input name="branch" value="refs/heads/{}" /></label><label><span>Commit</span><input name="commit" value="HEAD" /></label></div><div class="actions"><button type="submit">Trigger pipeline</button></div></form></article>"#,
            html_escape(&repo.owner_username),
            html_escape(&repo.name),
            html_escape(&repo.default_branch),
            badge("active", "success"),
            html_escape(&clone_url),
            repo.id,
            csrf_input(&csrf),
            html_escape(&repo.default_branch)
        ));
    }
    body.push_str("</div></section>");
    Ok(Html(layout("Repos", &body)))
}

async fn create_repo(
    CurrentUser(user): CurrentUser,
    State(state): State<Arc<AppState>>,
    Form(form): Form<CreateRepoForm>,
) -> Result<Redirect, Response> {
    verify_csrf(&state, &user, &form.csrf_token)?;
    if user.role != "admin" && form.owner_id != user.id {
        return Err(forbidden("developers can only create their own repos"));
    }
    let owner = state
        .db
        .get_user(&form.owner_id)
        .map_err(internal_error)?
        .ok_or_else(|| bad_request("owner not found"))?;
    let normalized = git::validate_repo_name(&form.name).map_err(bad_request)?;
    validate_branch_name(&form.default_branch)?;
    let bare_path = PathBuf::from(&state.config.repos_dir).join(format!("{}.git", Uuid::now_v7()));
    let repo_id = state
        .db
        .create_repo(
            &owner.id,
            &form.name,
            &normalized,
            &bare_path.display().to_string(),
            &form.default_branch,
        )
        .map_err(internal_error)?;
    let repo = state
        .db
        .get_repo(&repo_id)
        .map_err(internal_error)?
        .ok_or_else(|| internal_error_text("missing repo after create"))?;
    git::init_bare_repo(Path::new(&repo.bare_path)).map_err(internal_error_text)?;
    git::install_post_receive_hook(
        Path::new(&repo.bare_path),
        state.server_bin.as_path(),
        state.config_path.as_path(),
        &repo.id,
    )
    .map_err(internal_error_text)?;
    Ok(Redirect::to("/repos"))
}

async fn trigger_repo(
    CurrentUser(user): CurrentUser,
    State(state): State<Arc<AppState>>,
    AxumPath(repo_id): AxumPath<String>,
    Form(form): Form<ManualTriggerForm>,
) -> Result<Redirect, Response> {
    verify_csrf(&state, &user, &form.csrf_token)?;
    let repo = authorized_repo(&state, &user, &repo_id)?;
    let branch = form
        .branch
        .unwrap_or_else(|| format!("refs/heads/{}", repo.default_branch));
    let commit = form.commit.unwrap_or_else(|| "HEAD".to_string());
    let refs = vec![models::PushEventRef {
        old_rev: "0000000000000000000000000000000000000000".to_string(),
        new_rev: commit,
        ref_name: branch,
    }];
    let key = git::event_key(&repo_id, &refs);
    state
        .db
        .create_push_event(&repo_id, &key, &refs)
        .map_err(internal_error)?;
    Ok(Redirect::to("/pipelines"))
}

async fn runners_page(
    _: AdminUser,
    CurrentUser(user): CurrentUser,
    State(state): State<Arc<AppState>>,
) -> Result<Html<String>, Response> {
    let runners = state.db.list_runners().map_err(internal_error)?;
    let csrf = csrf_token(&state, &user);
    let mut body = page_intro(
        "Runners",
        "Register execution backends, refresh their advertised jobs, and control availability.",
    );
    body.push_str(&format!(
        r#"<section class="card"><div class="section-head"><div><div class="eyebrow">Execution</div><h2>Add runner</h2></div></div><form method="post" action="/runners" class="stack-lg">{}<div class="form-grid form-grid-3"><label><span>Name</span><input name="name" /></label><label><span>Base URL</span><input name="base_url" placeholder="http://127.0.0.1:8080" /></label><label><span>Token</span><input name="token" /></label></div><div class="actions"><button type="submit">Add runner</button></div></form></section>"#,
        csrf_input(&csrf)
    ));
    body.push_str(r#"<section class="card"><div class="section-head"><div><div class="eyebrow">Fleet</div><h2>Connected runners</h2></div></div><div class="card-grid">"#);
    for runner in runners {
        body.push_str(&format!(
            r#"<article class="entity-card"><div class="entity-head"><div><h3>{}</h3><p class="muted">{}</p></div><div class="badge-row">{}{}</div></div><div class="meta-pair"><span>Runner ID</span><code>{}</code></div><div class="actions"><form method="post" action="/runners/{}/test">{}<button type="submit" class="secondary">Refresh jobs</button></form><form method="post" action="/runners/{}/toggle">{}<button type="submit" class="ghost">{}</button></form></div>"#,
            html_escape(&runner.name),
            html_escape(&runner.base_url),
            badge(&runner.last_health_state, runner_state_tone(&runner.last_health_state)),
            badge(if runner.enabled { "enabled" } else { "disabled" }, if runner.enabled { "success" } else { "danger" }),
            html_escape(&runner.id),
            runner.id,
            csrf_input(&csrf),
            runner.id,
            csrf_input(&csrf),
            if runner.enabled { "Disable runner" } else { "Enable runner" }
        ));
        let jobs = state
            .db
            .list_runner_jobs(&runner.id)
            .map_err(internal_error)?;
        if !jobs.is_empty() {
            body.push_str(r#"<div class="subsection"><span class="subsection-title">Advertised jobs</span><div class="chip-row">"#);
            for (job_name, _) in jobs {
                body.push_str(&format!(
                    r#"<span class="chip">{}</span>"#,
                    html_escape(&job_name)
                ));
            }
            body.push_str("</div></div>");
        }
        body.push_str("</article>");
    }
    body.push_str("</div></section>");
    Ok(Html(layout("Runners", &body)))
}

async fn create_runner(
    _: AdminUser,
    CurrentUser(user): CurrentUser,
    State(state): State<Arc<AppState>>,
    Form(form): Form<CreateRunnerForm>,
) -> Result<Redirect, Response> {
    verify_csrf(&state, &user, &form.csrf_token)?;
    validate_runner_name(&form.name)?;
    validate_base_url(&form.base_url)?;
    if form.token.trim().is_empty() {
        return Err(bad_request("runner token cannot be empty"));
    }
    let runner_id = state
        .db
        .create_runner(&form.name, &form.base_url, &form.token)
        .map_err(internal_error)?;
    refresh_single_runner(&state, &runner_id)
        .await
        .map_err(internal_error_text)?;
    Ok(Redirect::to("/runners"))
}

async fn toggle_runner(
    _: AdminUser,
    CurrentUser(user): CurrentUser,
    State(state): State<Arc<AppState>>,
    AxumPath(runner_id): AxumPath<String>,
    Form(form): Form<CsrfOnlyForm>,
) -> Result<Redirect, Response> {
    verify_csrf(&state, &user, &form.csrf_token)?;
    let runner = state
        .db
        .get_runner(&runner_id)
        .map_err(internal_error)?
        .ok_or_else(|| not_found("runner"))?;
    state
        .db
        .set_runner_enabled(&runner_id, !runner.enabled)
        .map_err(internal_error)?;
    Ok(Redirect::to("/runners"))
}

async fn test_runner(
    _: AdminUser,
    CurrentUser(user): CurrentUser,
    State(state): State<Arc<AppState>>,
    AxumPath(runner_id): AxumPath<String>,
    Form(form): Form<CsrfOnlyForm>,
) -> Result<Redirect, Response> {
    verify_csrf(&state, &user, &form.csrf_token)?;
    refresh_single_runner(&state, &runner_id)
        .await
        .map_err(internal_error_text)?;
    Ok(Redirect::to("/runners"))
}

async fn workflows_page(
    CurrentUser(user): CurrentUser,
    State(state): State<Arc<AppState>>,
) -> Result<Html<String>, Response> {
    let repos = visible_repos_for_user(&state, &user)?;
    let workflows = state.db.list_workflows().map_err(internal_error)?;
    let runner_catalog = workflow_runner_catalog(&state)?;
    let csrf = csrf_token(&state, &user);
    let mut body = page_intro(
        "Workflows",
        "Define reusable CI pipelines with structured jobs, serial execution order, and runner bindings.",
    );
    let mut repo_select = String::from(r#"<select name="repo_id">"#);
    for repo in &repos {
        repo_select.push_str(&format!(
            "<option value=\"{}\">{}/{}</option>",
            repo.id,
            html_escape(&repo.owner_username),
            html_escape(&repo.name)
        ));
    }
    repo_select.push_str("</select>");
    body.push_str(&format!(
        r#"<section class="card"><div class="section-head"><div><div class="eyebrow">Automation</div><h2>Create workflow</h2></div></div><form method="post" action="/workflows" class="stack-lg">{}{}</form></section>"#,
        csrf_input(&csrf),
        workflow_form_fields(None, &runner_catalog, Some(&repo_select))
    ));
    body.push_str(r#"<section class="card"><div class="section-head"><div><div class="eyebrow">Catalog</div><h2>Existing workflows</h2></div></div><div class="card-grid">"#);
    for workflow in workflows
        .into_iter()
        .filter(|item| repo_ids_contains(&repos, &item.repo_id))
    {
        let schema_status = workflow_schema_status(&state, &workflow).map_err(internal_error)?;
        let trigger: WorkflowTrigger =
            serde_json::from_str(&workflow.trigger_json).unwrap_or(WorkflowTrigger {
                kind: "push".to_string(),
                branches: Vec::new(),
            });
        let definition: WorkflowDefinition = serde_json::from_str(&workflow.definition_json)
            .unwrap_or(WorkflowDefinition { jobs: Vec::new() });
        body.push_str(&format!(
            r#"<article class="entity-card"><div class="entity-head"><div><h3><a href="/workflows/{}">{}</a></h3><p class="muted">Repo: <code>{}</code></p></div><div class="badge-row">{}{}</div></div><div class="meta-grid"><div class="meta-pair"><span>Trigger</span><strong>{}</strong></div><div class="meta-pair"><span>Branches</span><strong>{}</strong></div><div class="meta-pair"><span>Version</span><strong>{}</strong></div><div class="meta-pair"><span>Jobs</span><strong>{}</strong></div></div><div class="chip-row">{}</div></article>"#,
            workflow.id,
            html_escape(&workflow.name),
            html_escape(&workflow.repo_id),
            badge("workflow", "neutral"),
            badge(schema_status.as_str(), schema_status.tone()),
            html_escape(&trigger.kind),
            html_escape(&trigger.branches.join(", ")),
            workflow.version,
            definition.jobs.len(),
            render_workflow_job_chips(&definition)
        ));
    }
    body.push_str("</div></section>");
    Ok(Html(layout("Workflows", &body)))
}

async fn workflow_detail_page(
    CurrentUser(user): CurrentUser,
    State(state): State<Arc<AppState>>,
    AxumPath(workflow_id): AxumPath<String>,
) -> Result<Html<String>, Response> {
    let workflow = authorized_workflow(&state, &user, &workflow_id)?;
    let csrf = csrf_token(&state, &user);
    let repo = state
        .db
        .get_repo(&workflow.repo_id)
        .map_err(internal_error)?
        .ok_or_else(|| not_found("repo"))?;
    let runner_catalog = workflow_runner_catalog(&state)?;
    let repo_field = format!(
        r#"<input type="hidden" name="repo_id" value="{}" /><input value="{}/{}" disabled />"#,
        workflow.repo_id,
        html_escape(&repo.owner_username),
        html_escape(&repo.name)
    );
    let mut body = page_intro(
        "Workflow Detail",
        "Adjust trigger behavior, job order, and runner/job bindings.",
    );
    let schema_status = workflow_schema_status(&state, &workflow).map_err(internal_error)?;
    body.push_str(&format!(
        r#"<section class="card"><div class="section-head"><div><div class="eyebrow">Editing</div><h2>{}</h2><p class="muted">Repository: {}/{}</p></div><div class="badge-row">{}</div></div><form method="post" action="/workflows/{}/update" class="stack-lg">{}{}</form></section>"#,
        html_escape(&workflow.name),
        html_escape(&repo.owner_username),
        html_escape(&repo.name),
        badge(schema_status.as_str(), schema_status.tone()),
        workflow.id,
        csrf_input(&csrf),
        workflow_form_fields(Some(&workflow), &runner_catalog, Some(&repo_field))
    ));
    Ok(Html(layout("Workflow", &body)))
}

async fn create_workflow(
    CurrentUser(user): CurrentUser,
    State(state): State<Arc<AppState>>,
    Form(form): Form<WorkflowForm>,
) -> Result<Redirect, Response> {
    verify_csrf(&state, &user, &form.csrf_token)?;
    let repo = authorized_repo(&state, &user, &form.repo_id)?;
    let parsed = parse_workflow_form(&state, &form)?;
    state
        .db
        .create_workflow(
            &repo.id,
            &form.name.trim(),
            true,
            &parsed.trigger_json,
            &parsed.definition_json,
            &parsed.job_schemas_json,
        )
        .map_err(internal_error)?;
    Ok(Redirect::to("/workflows"))
}

async fn update_workflow(
    CurrentUser(user): CurrentUser,
    State(state): State<Arc<AppState>>,
    AxumPath(workflow_id): AxumPath<String>,
    Form(form): Form<WorkflowForm>,
) -> Result<Redirect, Response> {
    verify_csrf(&state, &user, &form.csrf_token)?;
    let workflow = authorized_workflow(&state, &user, &workflow_id)?;
    if workflow.repo_id != form.repo_id {
        return Err(bad_request("workflow repo cannot be changed"));
    }
    let parsed = parse_workflow_form(&state, &form)?;
    state
        .db
        .update_workflow(
            &workflow.id,
            form.name.trim(),
            true,
            &parsed.trigger_json,
            &parsed.definition_json,
            &parsed.job_schemas_json,
        )
        .map_err(internal_error)?;
    Ok(Redirect::to(&format!("/workflows/{}", workflow.id)))
}

async fn pipelines_page(
    CurrentUser(user): CurrentUser,
    State(state): State<Arc<AppState>>,
) -> Result<Html<String>, Response> {
    let pipelines = state.db.list_pipeline_runs().map_err(internal_error)?;
    let repos = state.db.list_repos().map_err(internal_error)?;
    let repo_by_id = repos
        .into_iter()
        .map(|repo| (repo.id.clone(), repo))
        .collect::<BTreeMap<_, _>>();
    let mut body = page_intro(
        "Pipelines",
        "Track workflow executions across repositories and inspect current run state.",
    );
    body.push_str(r#"<section class="card"><div class="section-head"><div><div class="eyebrow">Runs</div><h2>Recent pipelines</h2></div></div><div class="stack-md">"#);
    for pipeline in pipelines {
        if let Some(repo) = repo_by_id.get(&pipeline.repo_id) {
            if !can_view_repo(&user, repo) {
                continue;
            }
            body.push_str(&format!(
                r#"<article class="list-row"><div><h3><a href="/pipelines/{}">{}</a></h3><p class="muted">{}/{}</p></div><div class="list-row-meta">{}<span>{}</span></div></article>"#,
                pipeline.id,
                pipeline.id,
                html_escape(&repo.owner_username),
                html_escape(&repo.name),
                badge(
                    &display_status(&pipeline.status),
                    status_tone(&pipeline.status)
                ),
                html_escape(&pipeline.trigger_ref.clone().unwrap_or_default())
            ));
        }
    }
    body.push_str("</div></section>");
    Ok(Html(layout("Pipelines", &body)))
}

async fn pipeline_detail(
    CurrentUser(user): CurrentUser,
    State(state): State<Arc<AppState>>,
    AxumPath(pipeline_id): AxumPath<String>,
) -> Result<Html<String>, Response> {
    let pipeline = authorized_pipeline(&state, &user, &pipeline_id)?;
    let snapshot = state
        .db
        .pipeline_snapshot(&pipeline.id)
        .map_err(internal_error)?
        .ok_or_else(|| not_found("pipeline"))?;
    let csrf = csrf_token(&state, &user);
    let mut body = page_intro(
        "Pipeline Detail",
        "Inspect execution state, retry behavior, logs, and artifact/output metadata per job.",
    );
    body.push_str(&format!(
        r#"<section class="card"><div class="section-head"><div><div class="eyebrow">Execution</div><h2>{}</h2></div><div class="badge-row">{}</div></div><div class="meta-grid"><div class="meta-pair"><span>Trigger ref</span><strong>{}</strong></div><div class="meta-pair"><span>Cancel reason</span><strong>{}</strong></div><div class="meta-pair"><span>Cancel requested</span><strong>{}</strong></div><div class="meta-pair"><span>Cancel started</span><strong>{}</strong></div></div><div class="actions"><form method="post" action="/pipelines/{}/rerun">{}<button type="submit" class="secondary">Rerun pipeline</button></form><form method="post" action="/pipelines/{}/cancel">{}<button type="submit" class="ghost">Cancel pipeline</button></form></div></section>"#,
        html_escape(&snapshot.pipeline.id),
        badge(&display_status(&snapshot.pipeline.status), status_tone(&snapshot.pipeline.status)),
        html_escape(&snapshot.pipeline.trigger_ref.clone().unwrap_or_default()),
        html_escape(&snapshot.pipeline.cancel_reason.clone().unwrap_or_default()),
        html_escape(&snapshot.pipeline.cancel_requested_at.clone().unwrap_or_default()),
        html_escape(&snapshot.pipeline.cancel_started_at.clone().unwrap_or_default()),
        snapshot.pipeline.id,
        csrf_input(&csrf),
        snapshot.pipeline.id,
        csrf_input(&csrf)
    ));
    body.push_str(r#"<section class="card"><div class="section-head"><div><div class="eyebrow">Jobs</div><h2>Job runs</h2></div></div><div class="stack-lg">"#);
    for job in snapshot.jobs {
        let previous_jobs = if job.previous_jobs.is_empty() {
            "<span class=\"muted\">None</span>".to_string()
        } else {
            job.previous_jobs
                .iter()
                .map(|previous| {
                    format!(
                        r#"<span class="chip">job-{} / {} ({})</span>"#,
                        previous.job_index + 1,
                        html_escape(&previous.runner_job_name),
                        html_escape(&display_status(&previous.status))
                    )
                })
                .collect::<Vec<_>>()
                .join("")
        };
        let resolved_inputs_json =
            serde_json::to_string_pretty(&job.resolved_inputs).unwrap_or_else(|_| "{}".to_string());
        body.push_str(&format!(
            r#"<article class="job-card"><div class="entity-head"><div><h3>{}</h3><p class="muted">Runner job: <code>{}</code></p></div><div class="badge-row">{}</div></div><div class="meta-grid"><div class="meta-pair"><span>Previous jobs</span><strong>{}</strong></div><div class="meta-pair"><span>Failure category</span><strong>{}</strong></div><div class="meta-pair"><span>Terminal reason</span><strong>{}</strong></div><div class="meta-pair"><span>Exit code</span><strong>{}</strong></div><div class="meta-pair"><span>Duration ms</span><strong>{}</strong></div><div class="meta-pair"><span>Cancel reason</span><strong>{}</strong></div><div class="meta-pair"><span>Cancel requested</span><strong>{}</strong></div><div class="meta-pair"><span>Cancel started</span><strong>{}</strong></div><div class="meta-pair"><span>Cancel retries</span><strong>{}</strong></div><div class="meta-pair"><span>Last cancel retry</span><strong>{}</strong></div><div class="meta-pair"><span>Infra retries</span><strong>{}</strong></div><div class="meta-pair"><span>Last infra retry</span><strong>{}</strong></div><div class="meta-pair"><span>Stdout</span><strong>{}B · truncated={}</strong></div><div class="meta-pair"><span>Stderr</span><strong>{}B · truncated={}</strong></div><div class="meta-pair"><span>Artifacts</span><strong>{} files · {}B</strong></div></div><div class="log-grid"><div class="log-panel"><span class="subsection-title">Resolved inputs</span><pre>{}</pre></div><div class="log-panel"><span class="subsection-title">Stdout</span><pre>{}</pre></div><div class="log-panel"><span class="subsection-title">Stderr</span><pre>{}</pre></div></div></article>"#,
            html_escape(&job.run.display_name()),
            html_escape(&job.run.runner_job_name),
            badge(&display_status(&job.run.status), status_tone(&job.run.status)),
            previous_jobs,
            html_escape(&render_optional(job.run.failure_category.as_deref())),
            html_escape(&render_optional(job.run.terminal_reason.as_deref())),
            html_escape(&render_optional(job.run.exit_code.map(|value| value.to_string()).as_deref())),
            html_escape(&render_optional(job.run.duration_ms.map(|value| value.to_string()).as_deref())),
            html_escape(&job.run.cancel_reason.clone().unwrap_or_default()),
            html_escape(&job.run.cancel_requested_at.clone().unwrap_or_default()),
            html_escape(&job.run.cancel_started_at.clone().unwrap_or_default()),
            job.run.cancel_retry_count,
            html_escape(&job.run.last_cancel_retry_at.clone().unwrap_or_default()),
            job.run.infra_retry_count,
            html_escape(&job.run.last_infra_retry_at.clone().unwrap_or_default()),
            job.run.output_metadata.stdout.bytes,
            job.run.output_metadata.stdout.truncated,
            job.run.output_metadata.stderr.bytes,
            job.run.output_metadata.stderr.truncated,
            job.run.output_metadata.artifacts.count,
            job.run.output_metadata.artifacts.bytes,
            html_escape(&resolved_inputs_json),
            html_escape(&job.stdout),
            html_escape(&job.stderr)
        ));
    }
    body.push_str("</div></section><script>const e=new EventSource('/pipelines/");
    body.push_str(&pipeline.id);
    body.push_str("/events');e.onmessage=(msg)=>console.log(msg.data);</script>");
    Ok(Html(layout("Pipeline", &body)))
}

async fn pipeline_events(
    _: CurrentUser,
    State(state): State<Arc<AppState>>,
    AxumPath(pipeline_id): AxumPath<String>,
) -> Sse<
    impl futures_util::Stream<Item = Result<axum::response::sse::Event, std::convert::Infallible>>,
> {
    let stream = async_stream::stream! {
        loop {
            let payload = match state.db.pipeline_snapshot(&pipeline_id) {
                Ok(Some(snapshot)) => serde_json::to_string(&snapshot).unwrap_or_else(|_| "{}".to_string()),
                Ok(None) => "{}".to_string(),
                Err(error) => json!({ "error": error.to_string() }).to_string(),
            };
            if payload != "{}" { yield Ok(axum::response::sse::Event::default().data(payload)); }
            time::sleep(std::time::Duration::from_secs(2)).await;
        }
    };
    Sse::new(stream)
}

async fn rerun_pipeline(
    CurrentUser(user): CurrentUser,
    State(state): State<Arc<AppState>>,
    AxumPath(pipeline_id): AxumPath<String>,
    Form(form): Form<CsrfOnlyForm>,
) -> Response {
    let result: Result<Redirect, Response> = (|| {
        verify_csrf(&state, &user, &form.csrf_token)?;
        let pipeline = authorized_pipeline(&state, &user, &pipeline_id)?;
        let workflow = state
            .db
            .get_workflow_by_version_id(&pipeline.workflow_version_id)
            .map_err(internal_error)?
            .ok_or_else(|| not_found("workflow"))?;
        let new_pipeline_id = scheduler::enqueue_workflow_run(
            Arc::clone(&state),
            &workflow,
            "rerun",
            pipeline.trigger_ref.as_deref(),
            pipeline.commit_sha.as_deref(),
        )
        .map_err(internal_error)?;
        Ok(Redirect::to(&format!("/pipelines/{new_pipeline_id}")))
    })();
    match result {
        Ok(redirect) => redirect.into_response(),
        Err(response) => response,
    }
}

async fn cancel_pipeline_route(
    CurrentUser(user): CurrentUser,
    State(state): State<Arc<AppState>>,
    AxumPath(pipeline_id): AxumPath<String>,
    Form(form): Form<CsrfOnlyForm>,
) -> Response {
    match verify_csrf(&state, &user, &form.csrf_token)
        .and_then(|_| authorized_pipeline(&state, &user, &pipeline_id))
    {
        Ok(pipeline) => match scheduler::cancel_pipeline(Arc::clone(&state), &pipeline.id).await {
            Ok(()) => Redirect::to(&format!("/pipelines/{}", pipeline.id)).into_response(),
            Err(error) => internal_error(error),
        },
        Err(response) => response,
    }
}

#[derive(Serialize)]
struct SessionInfo {
    user: User,
    csrf_token: String,
}
#[derive(Deserialize)]
struct ApiRepoCreateRequest {
    owner_id: Option<String>,
    name: String,
    default_branch: String,
    csrf_token: String,
}
#[derive(Deserialize)]
struct ApiRunnerCreateRequest {
    name: String,
    base_url: String,
    token: String,
    csrf_token: String,
}
#[derive(Deserialize)]
struct ApiWorkflowRequest {
    csrf_token: String,
    repo_id: String,
    name: String,
    enabled: bool,
    trigger_kind: String,
    branches: Vec<String>,
    jobs: Vec<ApiWorkflowJob>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ApiWorkflowJob {
    runner_id: String,
    runner_job_name: String,
    #[serde(default)]
    inputs: BTreeMap<String, WorkflowInputBinding>,
    #[serde(default)]
    allow_failure: bool,
}

async fn api_me(
    CurrentUser(user): CurrentUser,
    State(state): State<Arc<AppState>>,
) -> Json<SessionInfo> {
    Json(SessionInfo {
        csrf_token: csrf_token(&state, &user),
        user,
    })
}
async fn api_list_repos(
    CurrentUser(user): CurrentUser,
    State(state): State<Arc<AppState>>,
) -> Result<Json<Vec<Repo>>, Response> {
    Ok(Json(visible_repos_for_user(&state, &user)?))
}
async fn api_create_repo(
    CurrentUser(user): CurrentUser,
    State(state): State<Arc<AppState>>,
    Json(request): Json<ApiRepoCreateRequest>,
) -> Result<Json<Repo>, Response> {
    verify_csrf(&state, &user, &request.csrf_token)?;
    let owner_id = request.owner_id.unwrap_or_else(|| user.id.clone());
    if user.role != "admin" && owner_id != user.id {
        return Err(forbidden("developers can only create their own repos"));
    }
    let owner = state
        .db
        .get_user(&owner_id)
        .map_err(internal_error)?
        .ok_or_else(|| bad_request("owner not found"))?;
    let normalized = git::validate_repo_name(&request.name).map_err(bad_request)?;
    validate_branch_name(&request.default_branch)?;
    let bare_path = PathBuf::from(&state.config.repos_dir).join(format!("{}.git", Uuid::now_v7()));
    let repo_id = state
        .db
        .create_repo(
            &owner.id,
            &request.name,
            &normalized,
            &bare_path.display().to_string(),
            &request.default_branch,
        )
        .map_err(internal_error)?;
    let repo = state
        .db
        .get_repo(&repo_id)
        .map_err(internal_error)?
        .ok_or_else(|| internal_error_text("missing repo after create"))?;
    git::init_bare_repo(Path::new(&repo.bare_path)).map_err(internal_error_text)?;
    git::install_post_receive_hook(
        Path::new(&repo.bare_path),
        state.server_bin.as_path(),
        state.config_path.as_path(),
        &repo.id,
    )
    .map_err(internal_error_text)?;
    Ok(Json(repo))
}
async fn api_list_runners(
    _: AdminUser,
    State(state): State<Arc<AppState>>,
) -> Result<Json<Vec<models::Runner>>, Response> {
    Ok(Json(state.db.list_runners().map_err(internal_error)?))
}
async fn api_create_runner(
    _: AdminUser,
    CurrentUser(user): CurrentUser,
    State(state): State<Arc<AppState>>,
    Json(request): Json<ApiRunnerCreateRequest>,
) -> Result<Json<models::Runner>, Response> {
    verify_csrf(&state, &user, &request.csrf_token)?;
    validate_runner_name(&request.name)?;
    validate_base_url(&request.base_url)?;
    let runner_id = state
        .db
        .create_runner(&request.name, &request.base_url, &request.token)
        .map_err(internal_error)?;
    refresh_single_runner(&state, &runner_id)
        .await
        .map_err(internal_error_text)?;
    let runner = state
        .db
        .get_runner(&runner_id)
        .map_err(internal_error)?
        .ok_or_else(|| not_found("runner"))?;
    Ok(Json(runner))
}
async fn api_list_workflows(
    CurrentUser(user): CurrentUser,
    State(state): State<Arc<AppState>>,
) -> Result<Json<Vec<WorkflowApiView>>, Response> {
    let repos = visible_repos_for_user(&state, &user)?;
    let repo_ids = repos.into_iter().map(|repo| repo.id).collect::<Vec<_>>();
    let workflows = state
        .db
        .list_workflows()
        .map_err(internal_error)?
        .into_iter()
        .filter(|workflow| repo_ids.iter().any(|id| id == &workflow.repo_id))
        .map(|workflow| {
            Ok(WorkflowApiView {
                schema_status: workflow_schema_status(&state, &workflow).map_err(internal_error)?,
                workflow,
            })
        })
        .collect::<Result<Vec<_>, Response>>()?;
    Ok(Json(workflows))
}
async fn api_create_workflow(
    CurrentUser(user): CurrentUser,
    State(state): State<Arc<AppState>>,
    Json(request): Json<ApiWorkflowRequest>,
) -> Result<Json<WorkflowApiView>, Response> {
    verify_csrf(&state, &user, &request.csrf_token)?;
    let repo = authorized_repo(&state, &user, &request.repo_id)?;
    let parsed = parse_api_workflow_request(&state, &request)?;
    state
        .db
        .create_workflow(
            &repo.id,
            &request.name,
            request.enabled,
            &parsed.trigger_json,
            &parsed.definition_json,
            &parsed.job_schemas_json,
        )
        .map_err(internal_error)?;
    let workflow = state
        .db
        .workflows_for_repo(&repo.id)
        .map_err(internal_error)?
        .into_iter()
        .find(|workflow| workflow.name == request.name)
        .ok_or_else(|| internal_error_text("workflow missing after create"))?;
    Ok(Json(WorkflowApiView {
        schema_status: workflow_schema_status(&state, &workflow).map_err(internal_error)?,
        workflow,
    }))
}
async fn api_get_workflow(
    CurrentUser(user): CurrentUser,
    State(state): State<Arc<AppState>>,
    AxumPath(workflow_id): AxumPath<String>,
) -> Result<Json<WorkflowApiView>, Response> {
    let workflow = authorized_workflow(&state, &user, &workflow_id)?;
    Ok(Json(WorkflowApiView {
        schema_status: workflow_schema_status(&state, &workflow).map_err(internal_error)?,
        workflow,
    }))
}
async fn api_update_workflow(
    CurrentUser(user): CurrentUser,
    State(state): State<Arc<AppState>>,
    AxumPath(workflow_id): AxumPath<String>,
    Json(request): Json<ApiWorkflowRequest>,
) -> Result<Json<WorkflowApiView>, Response> {
    verify_csrf(&state, &user, &request.csrf_token)?;
    let workflow = authorized_workflow(&state, &user, &workflow_id)?;
    if workflow.repo_id != request.repo_id {
        return Err(bad_request("workflow repo cannot be changed"));
    }
    let parsed = parse_api_workflow_request(&state, &request)?;
    state
        .db
        .update_workflow(
            &workflow.id,
            &request.name,
            request.enabled,
            &parsed.trigger_json,
            &parsed.definition_json,
            &parsed.job_schemas_json,
        )
        .map_err(internal_error)?;
    let workflow = state
        .db
        .get_workflow(&workflow.id)
        .map_err(internal_error)?
        .ok_or_else(|| not_found("workflow"))?;
    Ok(Json(WorkflowApiView {
        schema_status: workflow_schema_status(&state, &workflow).map_err(internal_error)?,
        workflow,
    }))
}
async fn api_list_pipelines(
    CurrentUser(user): CurrentUser,
    State(state): State<Arc<AppState>>,
) -> Result<Json<Vec<PipelineRun>>, Response> {
    let repos = visible_repos_for_user(&state, &user)?;
    let repo_ids = repos.into_iter().map(|repo| repo.id).collect::<Vec<_>>();
    let pipelines = state
        .db
        .list_pipeline_runs()
        .map_err(internal_error)?
        .into_iter()
        .filter(|pipeline| repo_ids.iter().any(|id| id == &pipeline.repo_id))
        .collect::<Vec<_>>();
    Ok(Json(pipelines))
}
async fn api_get_pipeline(
    CurrentUser(user): CurrentUser,
    State(state): State<Arc<AppState>>,
    AxumPath(pipeline_id): AxumPath<String>,
) -> Result<Json<models::PipelineSnapshot>, Response> {
    let pipeline = authorized_pipeline(&state, &user, &pipeline_id)?;
    Ok(Json(
        state
            .db
            .pipeline_snapshot(&pipeline.id)
            .map_err(internal_error)?
            .ok_or_else(|| not_found("pipeline"))?,
    ))
}

async fn refresh_single_runner(state: &Arc<AppState>, runner_id: &str) -> Result<(), String> {
    let runner = state
        .db
        .get_runner(runner_id)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "runner not found".to_string())?;
    let jobs = state
        .runner_client
        .list_jobs(&runner)
        .await
        .map_err(|error| error.to_string())?;
    let jobs = jobs
        .into_iter()
        .map(|job| {
            let name = job.name.clone();
            (
                name,
                serde_json::to_string(&job).unwrap_or_else(|_| "{}".to_string()),
            )
        })
        .collect::<Vec<_>>();
    state
        .db
        .replace_runner_jobs(runner_id, &jobs)
        .map_err(|error| error.to_string())?;
    state
        .db
        .update_runner_health(runner_id, "healthy")
        .map_err(|error| error.to_string())?;
    Ok(())
}

struct ParsedWorkflow {
    trigger_json: String,
    definition_json: String,
    job_schemas_json: String,
}

fn display_status(status: &str) -> String {
    match status {
        "cancel_requested" => "cancel requested".to_string(),
        "canceling" => "stopping".to_string(),
        "failed" => "failed".to_string(),
        _ => status.to_string(),
    }
}

fn render_optional(value: Option<&str>) -> String {
    value.unwrap_or_default().to_string()
}

fn parse_workflow_form(
    state: &Arc<AppState>,
    form: &WorkflowForm,
) -> Result<ParsedWorkflow, Response> {
    if form.name.trim().is_empty() {
        return Err(bad_request("workflow name cannot be empty"));
    }
    let trigger_kind = form.trigger_kind.trim();
    if !matches!(trigger_kind, "push" | "manual") {
        return Err(bad_request("trigger kind must be push or manual"));
    }
    let branch_name = form.branch_name.trim();
    if branch_name.contains(',') {
        return Err(bad_request("branch name must contain only one branch"));
    }
    let trigger = WorkflowTrigger {
        kind: trigger_kind.to_string(),
        branches: if branch_name.is_empty() {
            Vec::new()
        } else {
            vec![branch_name.to_string()]
        },
    };
    let jobs = parse_workflow_form_jobs(&form.jobs_json)?;
    let definition = WorkflowDefinition { jobs };
    definition.validate().map_err(bad_request)?;
    let job_schemas = validate_workflow_runners(state, &definition)?;
    Ok(ParsedWorkflow {
        trigger_json: serde_json::to_string(&trigger).map_err(internal_error_text)?,
        definition_json: serde_json::to_string(&definition).map_err(internal_error_text)?,
        job_schemas_json: serde_json::to_string(&job_schemas).map_err(internal_error_text)?,
    })
}

fn parse_api_workflow_request(
    state: &Arc<AppState>,
    request: &ApiWorkflowRequest,
) -> Result<ParsedWorkflow, Response> {
    if request.name.trim().is_empty() {
        return Err(bad_request("workflow name cannot be empty"));
    }
    if !matches!(request.trigger_kind.as_str(), "push" | "manual") {
        return Err(bad_request("trigger kind must be push or manual"));
    }
    let trigger = WorkflowTrigger {
        kind: request.trigger_kind.clone(),
        branches: request.branches.clone(),
    };
    let jobs = request
        .jobs
        .iter()
        .map(|job| WorkflowJobDefinition {
            runner_id: job.runner_id.clone(),
            runner_job_name: job.runner_job_name.clone(),
            inputs: job.inputs.clone(),
            allow_failure: job.allow_failure,
        })
        .collect::<Vec<_>>();
    let definition = WorkflowDefinition { jobs };
    definition.validate().map_err(bad_request)?;
    let job_schemas = validate_workflow_runners(state, &definition)?;
    Ok(ParsedWorkflow {
        trigger_json: serde_json::to_string(&trigger).map_err(internal_error_text)?,
        definition_json: serde_json::to_string(&definition).map_err(internal_error_text)?,
        job_schemas_json: serde_json::to_string(&job_schemas).map_err(internal_error_text)?,
    })
}

#[derive(Debug, Clone, Deserialize)]
struct SubmittedWorkflowJob {
    runner_id: String,
    runner_job_name: String,
    #[serde(default)]
    inputs: BTreeMap<String, WorkflowInputBinding>,
    #[serde(default)]
    allow_failure: bool,
}

fn parse_workflow_form_jobs(input: &str) -> Result<Vec<WorkflowJobDefinition>, Response> {
    let jobs = serde_json::from_str::<Vec<SubmittedWorkflowJob>>(input)
        .map_err(|error| bad_request(format!("invalid workflow jobs payload: {error}")))?;
    if jobs.is_empty() {
        return Err(bad_request("workflow must contain at least one job"));
    }
    Ok(jobs
        .into_iter()
        .map(|job| WorkflowJobDefinition {
            runner_id: job.runner_id,
            runner_job_name: job.runner_job_name,
            inputs: job.inputs,
            allow_failure: job.allow_failure,
        })
        .collect())
}

fn validate_workflow_runners(
    state: &Arc<AppState>,
    definition: &WorkflowDefinition,
) -> Result<Vec<RunnerJobSchema>, Response> {
    let mut runner_job_defs = BTreeMap::new();
    for (job_index, job) in definition.jobs.iter().enumerate() {
        let runner = state
            .db
            .get_runner(&job.runner_id)
            .map_err(internal_error)?
            .ok_or_else(|| bad_request(format!("unknown runner {}", job.runner_id)))?;
        let jobs = state
            .db
            .list_runner_jobs(&runner.id)
            .map_err(internal_error)?;
        let definition_json = jobs
            .iter()
            .find(|(name, _)| name == &job.runner_job_name)
            .map(|(_, definition_json)| definition_json.clone());
        let Some(definition_json) = definition_json else {
            return Err(bad_request(format!(
                "runner {} does not advertise job {}",
                runner.name, job.runner_job_name
            )));
        };
        let runner_job =
            serde_json::from_str::<RunnerJobSchema>(&definition_json).map_err(|error| {
                internal_error(format!(
                    "failed to parse runner job definition for {}: {error}",
                    job.runner_job_name
                ))
            })?;
        runner_job_defs.insert(job_index, runner_job);
    }

    for (job_index, job) in definition.jobs.iter().enumerate() {
        let runner_job = runner_job_defs
            .get(&job_index)
            .ok_or_else(|| internal_error("missing parsed runner job definition"))?;
        for (input_name, value) in &job.inputs {
            let expected_kind = runner_job
                .inputs
                .get(input_name)
                .map(|entry| entry.kind.as_str())
                .ok_or_else(|| {
                    bad_request(format!(
                        "workflow job {} provides unknown input {} for runner job {}",
                        job_index + 1,
                        input_name,
                        job.runner_job_name
                    ))
                })?;
            match value {
                WorkflowInputBinding::Commit | WorkflowInputBinding::Branch => {
                    if expected_kind != "string" {
                        return Err(bad_request(format!(
                            "workflow input {input_name} expects {expected_kind} but built-in binding is string"
                        )));
                    }
                    continue;
                }
                WorkflowInputBinding::SourceArtifact => {
                    if expected_kind != "artifact" {
                        return Err(bad_request(format!(
                            "workflow input {input_name} expects {expected_kind} but source binding is artifact"
                        )));
                    }
                    continue;
                }
                WorkflowInputBinding::Literal { value } => {
                    if !value_matches_input_kind(value, expected_kind) {
                        return Err(bad_request(format!(
                            "workflow input {input_name} expects {expected_kind} but got {}",
                            describe_json_value_kind(value)
                        )));
                    }
                    continue;
                }
                WorkflowInputBinding::JobOutput { .. } => {}
            }
            if let Some(binding) = parse_job_output_binding(value) {
                if binding.job_index >= definition.jobs.len() {
                    return Err(bad_request(format!(
                        "workflow input {input_name} references unknown job job-{}",
                        binding.job_index + 1
                    )));
                }
                if binding.job_index >= job_index {
                    return Err(bad_request(format!(
                        "workflow input {input_name} references job-{}.{} but only earlier jobs can be referenced",
                        binding.job_index + 1,
                        binding.output_name
                    )));
                }
                let upstream_runner_job = runner_job_defs
                    .get(&binding.job_index)
                    .ok_or_else(|| internal_error("missing upstream runner job definition"))?;
                let output = upstream_runner_job
                    .outputs
                    .get(&binding.output_name)
                    .ok_or_else(|| {
                        bad_request(format!(
                            "workflow input {input_name} references missing output job-{}.{}",
                            binding.job_index + 1,
                            binding.output_name
                        ))
                    })?;
                if output.kind.as_str() != expected_kind {
                    return Err(bad_request(format!(
                        "workflow input {input_name} expects {expected_kind} but job-{}.{} is {}",
                        binding.job_index + 1,
                        binding.output_name,
                        output.kind.as_str()
                    )));
                }
                continue;
            }
        }
    }
    Ok(definition
        .jobs
        .iter()
        .enumerate()
        .filter_map(|(job_index, _)| runner_job_defs.get(&job_index).cloned())
        .collect())
}

fn value_matches_input_kind(value: &Value, expected_kind: &str) -> bool {
    match expected_kind {
        "string" | "artifact" => value.is_string(),
        "integer" => value.as_i64().is_some(),
        "boolean" => value.is_boolean(),
        "json" => !value.is_null(),
        _ => false,
    }
}

fn describe_json_value_kind(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(number) => {
            if number.as_i64().is_some() {
                "integer"
            } else {
                "number"
            }
        }
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

#[derive(Debug, Clone, Serialize)]
struct WorkflowRunnerCatalogEntry {
    id: String,
    name: String,
    jobs: Vec<RunnerJobSchema>,
}

#[derive(Debug, Clone, Serialize)]
struct WorkflowEditorJob {
    runner_id: String,
    runner_job_name: String,
    allow_failure: bool,
    #[serde(default)]
    inputs: BTreeMap<String, WorkflowInputBinding>,
}

fn workflow_runner_catalog(
    state: &Arc<AppState>,
) -> Result<Vec<WorkflowRunnerCatalogEntry>, Response> {
    let runners = state.db.list_runners().map_err(internal_error)?;
    let mut catalog = Vec::with_capacity(runners.len());
    for runner in runners {
        let jobs = state
            .db
            .list_runner_jobs(&runner.id)
            .map_err(internal_error)?
            .into_iter()
            .map(|(job_name, definition_json)| {
                let mut parsed =
                    serde_json::from_str::<RunnerJobSchema>(&definition_json).unwrap_or_default();
                parsed.name = job_name;
                parsed
            })
            .collect::<Vec<_>>();
        catalog.push(WorkflowRunnerCatalogEntry {
            id: runner.id,
            name: runner.name,
            jobs,
        });
    }
    Ok(catalog)
}

fn workflow_schema_status(
    state: &Arc<AppState>,
    workflow: &Workflow,
) -> Result<WorkflowSchemaStatus, Box<dyn std::error::Error>> {
    let definition: WorkflowDefinition = serde_json::from_str(&workflow.definition_json)?;
    let snapshot_json = state.db.workflow_job_schemas_json(&workflow.version_id)?;
    let snapshot: Vec<RunnerJobSchema> = serde_json::from_str(&snapshot_json)?;
    if snapshot.len() != definition.jobs.len() {
        return Ok(WorkflowSchemaStatus::Incompatible);
    }

    let mut status = WorkflowSchemaStatus::Current;
    for (job_index, job) in definition.jobs.iter().enumerate() {
        let Some(saved_schema) = snapshot.get(job_index) else {
            return Ok(WorkflowSchemaStatus::Incompatible);
        };
        let current_schema = state
            .db
            .list_runner_jobs(&job.runner_id)?
            .into_iter()
            .find(|(name, _)| name == &job.runner_job_name)
            .map(|(_, definition_json)| serde_json::from_str::<RunnerJobSchema>(&definition_json))
            .transpose()?;
        let Some(current_schema) = current_schema else {
            return Ok(WorkflowSchemaStatus::Incompatible);
        };
        if &current_schema != saved_schema {
            status = WorkflowSchemaStatus::Stale;
        }
    }

    Ok(status)
}

fn workflow_form_fields(
    workflow: Option<&Workflow>,
    runner_catalog: &[WorkflowRunnerCatalogEntry],
    repo_selector: Option<&str>,
) -> String {
    let name = workflow
        .map(|item| html_escape(&item.name))
        .unwrap_or_default();
    let (trigger_kind, branch_name, jobs_json, repo_id_input, definition) =
        if let Some(item) = workflow {
            let trigger: WorkflowTrigger =
                serde_json::from_str(&item.trigger_json).unwrap_or(WorkflowTrigger {
                    kind: "push".to_string(),
                    branches: vec!["main".to_string()],
                });
            let definition: WorkflowDefinition = serde_json::from_str(&item.definition_json)
                .unwrap_or(WorkflowDefinition { jobs: Vec::new() });
            let jobs_json =
                serde_json::to_string(&definition.jobs).unwrap_or_else(|_| "[]".to_string());
            (
                trigger.kind,
                trigger.branches.first().cloned().unwrap_or_default(),
                jobs_json,
                format!(
                    "<input type=\"hidden\" name=\"repo_id\" value=\"{}\" />",
                    item.repo_id
                ),
                definition,
            )
        } else {
            (
                "push".to_string(),
                "main".to_string(),
                "[]".to_string(),
                String::new(),
                WorkflowDefinition { jobs: Vec::new() },
            )
        };
    let mut catalog = runner_catalog.to_vec();
    for job in &definition.jobs {
        if let Some(runner) = catalog.iter_mut().find(|runner| runner.id == job.runner_id) {
            if !runner
                .jobs
                .iter()
                .any(|item| item.name == job.runner_job_name)
            {
                runner.jobs.push(RunnerJobSchema {
                    name: job.runner_job_name.clone(),
                    ..RunnerJobSchema::default()
                });
                runner
                    .jobs
                    .sort_by(|left, right| left.name.cmp(&right.name));
            }
        } else {
            catalog.push(WorkflowRunnerCatalogEntry {
                id: job.runner_id.clone(),
                name: job.runner_id.clone(),
                jobs: vec![RunnerJobSchema {
                    name: job.runner_job_name.clone(),
                    ..RunnerJobSchema::default()
                }],
            });
        }
    }
    let runner_catalog_json =
        render_script_json(&serde_json::to_string(&catalog).unwrap_or_else(|_| "[]".to_string()));
    let mut initial_jobs = definition
        .jobs
        .iter()
        .map(|job| WorkflowEditorJob {
            runner_id: job.runner_id.clone(),
            runner_job_name: job.runner_job_name.clone(),
            allow_failure: job.allow_failure,
            inputs: job.inputs.clone(),
        })
        .collect::<Vec<_>>();
    if initial_jobs.is_empty() {
        let (runner_id, runner_job_name) = catalog
            .first()
            .map(|runner| {
                (
                    if catalog.len() == 1 {
                        runner.id.clone()
                    } else {
                        String::new()
                    },
                    if catalog.len() == 1 && runner.jobs.len() == 1 {
                        runner
                            .jobs
                            .first()
                            .map(|job| job.name.clone())
                            .unwrap_or_default()
                    } else {
                        String::new()
                    },
                )
            })
            .unwrap_or_default();
        initial_jobs.push(WorkflowEditorJob {
            runner_id,
            runner_job_name,
            allow_failure: false,
            inputs: BTreeMap::new(),
        });
    }
    let initial_jobs_json = render_script_json(
        &serde_json::to_string(&initial_jobs).unwrap_or_else(|_| "[]".to_string()),
    );
    let repo_field = repo_selector.unwrap_or(&repo_id_input);
    format!(
        r#"<div class="form-grid form-grid-2"><label><span>Workflow name</span><input name="name" value="{name}" /></label><label><span>Trigger</span><select name="trigger_kind"><option value="push" {push_selected}>push</option><option value="manual" {manual_selected}>manual</option></select></label></div><div class="form-grid form-grid-2"><label><span>Repository</span>{repo_input}</label><label><span>Branch</span><input name="branch_name" value="{branch_name}" /></label></div><div class="card soft-card"><div class="section-head"><div><div class="eyebrow">Jobs</div><h3>Workflow builder</h3><p class="muted">Jobs run one by one in the order shown below. Inputs are rendered from the selected runner job manifest.</p></div></div><div id="workflow-builder" class="stack-md"><div id="workflow-job-list" class="stack-md"></div><div class="actions"><button type="button" id="workflow-add-job" class="secondary">Add job</button></div></div></div><textarea id="workflow-jobs-json" name="jobs_json" hidden>{jobs_json}</textarea><div class="inline-note">Artifact inputs can point to <code>source.tar.gz</code>. Typed inputs can bind to matching outputs from earlier jobs in the workflow.</div><script type="application/json" id="workflow-runner-catalog">{runner_catalog_json}</script><script type="application/json" id="workflow-initial-jobs">{initial_jobs_json}</script><script>
(() => {{
  const list = document.getElementById('workflow-job-list');
  const addButton = document.getElementById('workflow-add-job');
  const jobsJsonField = document.getElementById('workflow-jobs-json');
  const catalog = JSON.parse(document.getElementById('workflow-runner-catalog').textContent || '[]');
  const initialJobs = JSON.parse(document.getElementById('workflow-initial-jobs').textContent || '[]');
  const catalogById = new Map(catalog.map((runner) => [runner.id, runner]));

  function makeInput(type, value) {{
    const input = document.createElement('input');
    input.type = type;
    if (type === 'checkbox') {{
      input.checked = Boolean(value);
    }} else {{
      input.value = value || '';
    }}
    return input;
  }}

  function makeSelect() {{
    return document.createElement('select');
  }}

  function getRunner(runnerId) {{
    return catalogById.get(runnerId) || null;
  }}

  function getRunnerJobs(runnerId) {{
    const runner = getRunner(runnerId);
    return runner ? runner.jobs : [];
  }}

  function getJobDefinition(runnerId, jobName) {{
    return getRunnerJobs(runnerId).find((job) => job.name === jobName) || null;
  }}

  function fillRunnerOptions(select, selectedRunnerId) {{
    select.replaceChildren();
    const placeholder = document.createElement('option');
    placeholder.value = '';
    placeholder.textContent = catalog.length <= 1 ? 'Select runner' : 'Choose runner';
    placeholder.selected = !selectedRunnerId;
    select.appendChild(placeholder);
    for (const runner of catalog) {{
      const option = document.createElement('option');
      option.value = runner.id;
      option.textContent = runner.name + ' (' + runner.id + ')';
      option.selected = runner.id === selectedRunnerId;
      select.appendChild(option);
    }}
    if (!select.value && catalog.length === 1) {{
      select.value = catalog[0].id;
    }}
  }}

  function takenJobNames(runnerId, currentRow) {{
    const rows = [...list.querySelectorAll('[data-workflow-job-row]')];
    const currentIndex = rows.indexOf(currentRow);
    return new Set(
      rows
        .slice(0, currentIndex === -1 ? rows.length : currentIndex)
        .filter((row) => row !== currentRow)
        .map((row) => {{
          const rowRunnerId = row.querySelector('[data-field="runner_id"]').value;
          const rowJobName = row.querySelector('[data-field="runner_job_name"]').value;
          return rowRunnerId === runnerId ? rowJobName : '';
        }})
        .filter(Boolean)
    );
  }}

  function fillJobOptions(runnerSelect, jobSelect, selectedJobName) {{
    jobSelect.replaceChildren();
    const jobs = getRunnerJobs(runnerSelect.value);
    const currentRow = runnerSelect.closest('[data-workflow-job-row]');
    const taken = takenJobNames(runnerSelect.value, currentRow);
    const availableJobs = jobs.filter((job) => !taken.has(job.name) || job.name === selectedJobName);
    const placeholder = document.createElement('option');
    placeholder.value = '';
    placeholder.textContent = availableJobs.length <= 1 ? 'Select job' : 'Choose job';
    placeholder.selected = !selectedJobName;
    jobSelect.appendChild(placeholder);
    for (const job of availableJobs) {{
      const option = document.createElement('option');
      option.value = job.name;
      option.textContent = job.name;
      option.selected = job.name === selectedJobName;
      jobSelect.appendChild(option);
    }}
    jobSelect.disabled = availableJobs.length === 0;
    if (!jobSelect.value && availableJobs.length === 1) {{
      jobSelect.value = availableJobs[0].name;
    }}
  }}

  function buildDerivedJobs(rows) {{
    return rows.map((row, index) => {{
      const runnerId = row.querySelector('[data-field="runner_id"]').value.trim();
      const runnerJobName = row.querySelector('[data-field="runner_job_name"]').value.trim();
      const runner = getRunner(runnerId);
      return {{
        row,
        runner,
        jobIndex: index,
        runnerId,
        runnerJobName,
        name: runner && runnerJobName ? `${{runner.name}} / ${{runnerJobName}}` : (runnerJobName || `job-${{index + 1}}`)
      }};
    }});
  }}

  function inferBinding(inputName, kind, rawValue) {{
    if (kind === 'artifact') {{
      if (rawValue && rawValue.kind === 'source_artifact') return {{ mode: 'source_artifact', value: 'source.tar.gz' }};
      if (rawValue && typeof rawValue === 'object' && rawValue.kind === 'job_output') {{
        return {{ mode: 'output_artifact', value: JSON.stringify(rawValue) }};
      }}
      if (inputName === 'source') return {{ mode: 'source_artifact', value: 'source.tar.gz' }};
      return {{ mode: 'source_artifact', value: 'source.tar.gz' }};
    }}
    if (kind === 'string') {{
      if (rawValue && rawValue.kind === 'commit') return {{ mode: 'commit', value: '' }};
      if (rawValue && rawValue.kind === 'branch') return {{ mode: 'branch', value: '' }};
      if (rawValue && typeof rawValue === 'object' && rawValue.kind === 'job_output') {{
        return {{ mode: 'output_value', value: JSON.stringify(rawValue) }};
      }}
      if (inputName === 'commit') return {{ mode: 'commit', value: '' }};
      if (inputName === 'branch') return {{ mode: 'branch', value: '' }};
      if (rawValue && rawValue.kind === 'literal') return {{ mode: 'literal', value: typeof rawValue.value === 'string' ? rawValue.value : '' }};
      return {{ mode: 'literal', value: '' }};
    }}
    if ((kind === 'boolean' || kind === 'integer' || kind === 'json')
      && rawValue && typeof rawValue === 'object'
      && rawValue.kind === 'job_output') {{
      return {{ mode: 'output_value', value: JSON.stringify(rawValue) }};
    }}
    if (kind === 'boolean') return {{ mode: 'literal', value: rawValue && rawValue.kind === 'literal' && rawValue.value === true ? 'true' : 'false' }};
    if (kind === 'integer') return {{ mode: 'literal', value: rawValue && rawValue.kind === 'literal' ? String(rawValue.value ?? '') : '' }};
    if (kind === 'json') return {{ mode: 'literal', value: rawValue && rawValue.kind === 'literal' ? JSON.stringify(rawValue.value) : '' }};
    return {{ mode: 'literal', value: rawValue && rawValue.kind === 'literal' ? String(rawValue.value ?? '') : '' }};
  }}

  function readInputBinding(inputRow) {{
    const kind = inputRow.dataset.inputKind;
    const name = inputRow.dataset.inputName;
    const modeSelect = inputRow.querySelector('[data-binding-mode]');
    const valueField = inputRow.querySelector('[data-binding-value]');
    const mode = modeSelect ? modeSelect.value : 'literal';
    if (kind === 'artifact') {{
      if (mode === 'source_artifact') return [name, {{ kind: 'source_artifact' }}];
      return [name, JSON.parse(valueField.value)];
    }}
    if (kind === 'string') {{
      if (mode === 'commit') return [name, {{ kind: 'commit' }}];
      if (mode === 'branch') return [name, {{ kind: 'branch' }}];
      if (mode === 'output_value') return [name, JSON.parse(valueField.value)];
      return [name, {{ kind: 'literal', value: valueField ? valueField.value : '' }}];
    }}
    if (mode === 'output_value') {{
      return [name, JSON.parse(valueField.value)];
    }}
    if (kind === 'boolean') {{
      return [name, {{ kind: 'literal', value: valueField.value === 'true' }}];
    }}
    if (kind === 'integer') {{
      const raw = valueField.value.trim();
      const parsed = Number.parseInt(raw, 10);
      return [name, {{ kind: 'literal', value: Number.isFinite(parsed) ? parsed : raw }}];
    }}
    if (kind === 'json') {{
      const raw = valueField.value.trim();
      if (!raw) return [name, {{ kind: 'literal', value: {{}} }}];
      try {{
        return [name, {{ kind: 'literal', value: JSON.parse(raw) }}];
      }} catch (_error) {{
        return [name, {{ kind: 'literal', value: raw }}];
      }}
    }}
    return [name, {{ kind: 'literal', value: valueField ? valueField.value : '' }}];
  }}

  function syncJobsJson() {{
    const rows = [...list.querySelectorAll('[data-workflow-job-row]')];
    const derivedJobs = buildDerivedJobs(rows);
    const jobs = derivedJobs.map((job) => {{
      const inputsMap = [...job.row.querySelectorAll('[data-input-row]')]
        .map(readInputBinding)
        .reduce((acc, [key, value]) => {{
          acc[key] = value;
          return acc;
        }}, {{}});
      return {{
        runner_id: job.runnerId,
        runner_job_name: job.runnerJobName,
        inputs: inputsMap,
        allow_failure: Boolean(job.row._allowFailure)
      }};
    }}).filter((job) => job.runner_id || job.runner_job_name || Object.keys(job.inputs).length > 0);
    jobsJsonField.value = JSON.stringify(jobs);
    renderOutputBindingOptions(derivedJobs);
    renderOutputTable(derivedJobs);
  }}

  function labelWrap(text, input) {{
    const label = document.createElement('label');
    const caption = document.createElement('span');
    caption.textContent = text;
    label.appendChild(caption);
    label.appendChild(input);
    return label;
  }}

  function outputOptionsFor(currentRow, derivedJobs, expectedKind) {{
    const options = [];
    const currentIndex = derivedJobs.findIndex((job) => job.row === currentRow);
    for (const job of derivedJobs) {{
      if (job.row === currentRow) continue;
      if (currentIndex !== -1 && derivedJobs.indexOf(job) >= currentIndex) continue;
      const definition = getJobDefinition(job.runnerId, job.runnerJobName);
      const outputs = definition ? Object.entries(definition.outputs || {{}}) : [];
      for (const [outputName, outputDef] of outputs) {{
        if ((outputDef.type || '') !== expectedKind) continue;
        options.push({{
          value: JSON.stringify({{ kind: 'job_output', job_index: job.jobIndex, output_name: outputName }}),
          label: `${{job.name}} -> ${{outputName}}`
        }});
      }}
    }}
    return options;
  }}

  function parseOutputBinding(value) {{
    if (!value) return null;
    try {{
      return JSON.parse(value);
    }} catch (_error) {{
      return null;
    }}
  }}

  function findDerivedJobByIndex(derivedJobs, jobIndex) {{
    return derivedJobs.find((job) => job.jobIndex === jobIndex) || null;
  }}

  function literalHintFor(kind, mode) {{
    if (mode !== 'literal') return '';
    if (kind === 'string') return 'Enter a plain string value.';
    if (kind === 'integer') return 'Enter a signed integer like 42.';
    if (kind === 'boolean') return 'Choose true or false.';
    if (kind === 'json') return 'Enter valid non-null JSON.';
    return '';
  }}

  function outputBindingHint(row, inputRow, mode, derivedJobs) {{
    if (mode !== 'output_artifact' && mode !== 'output_value') return '';
    const reference = inputRow.dataset.bindingValue || '';
    const kind = inputRow.dataset.inputKind;
    const expectedKind = mode === 'output_artifact' ? 'artifact' : kind;
    const options = outputOptionsFor(row, derivedJobs, expectedKind);
    if (options.length === 0) {{
      return `No earlier jobs expose matching ${{expectedKind}} outputs yet.`;
    }}
    if (!reference) return `Select a ${{expectedKind}} output from an earlier job.`;
    const binding = parseOutputBinding(reference);
    const sourceJob = binding ? findDerivedJobByIndex(derivedJobs, binding.job_index) : null;
    if (sourceJob) {{
      return `Binding to ${{sourceJob.name}}.`;
    }}
    return '';
  }}

  function renderOutputBindingOptions(derivedJobs) {{
    for (const job of derivedJobs) {{
      for (const inputRow of job.row.querySelectorAll('[data-input-row]')) {{
        const mode = inputRow.querySelector('[data-binding-mode]').value;
        const valueField = inputRow.querySelector('[data-binding-value]');
        const kind = inputRow.dataset.inputKind;
        if (!valueField || valueField.tagName !== 'SELECT') continue;
        if (mode !== 'output_artifact' && mode !== 'output_value') continue;
        const selected = inputRow.dataset.bindingValue || valueField.value;
        valueField.replaceChildren();
        const expectedKind = mode === 'output_artifact' ? 'artifact' : kind;
        const options = outputOptionsFor(job.row, derivedJobs, expectedKind);
        if (options.length === 0) {{
          const option = document.createElement('option');
          option.value = '';
          option.textContent = `No ${{expectedKind}} outputs available`;
          option.selected = true;
          valueField.appendChild(option);
        }}
        for (const optionData of options) {{
          const option = document.createElement('option');
          option.value = optionData.value;
          option.textContent = optionData.label;
          option.selected = optionData.value === selected;
          valueField.appendChild(option);
        }}
        if (!valueField.value && valueField.options.length > 0) {{
          valueField.value = valueField.options[0].value;
        }}
        inputRow.dataset.bindingValue = valueField.value;
      }}
    }}
  }}

  function bindingModesFor(kind) {{
    if (kind === 'artifact') return [
      ['source_artifact', 'Source archive'],
      ['output_artifact', 'Output artifact']
    ];
    if (kind === 'string') return [
      ['literal', 'Literal'],
      ['output_value', 'Job output'],
      ['commit', 'Current commit'],
      ['branch', 'Current branch']
    ];
    if (kind === 'integer' || kind === 'boolean' || kind === 'json') return [
      ['literal', 'Literal'],
      ['output_value', 'Job output']
    ];
    return [['literal', 'Literal']];
  }}

  function buildValueField(kind, binding, row) {{
    if (kind === 'artifact') {{
      const select = makeSelect();
      select.setAttribute('data-binding-value', 'true');
      if (binding.mode === 'source_artifact') {{
        const option = document.createElement('option');
        option.value = 'source.tar.gz';
        option.textContent = 'source.tar.gz';
        select.appendChild(option);
      }}
      row.dataset.bindingValue = binding.value || '';
      return select;
    }}
    if (binding.mode === 'output_value') {{
      const select = makeSelect();
      select.setAttribute('data-binding-value', 'true');
      row.dataset.bindingValue = binding.value || '';
      return select;
    }}
    if (kind === 'string' && binding.mode !== 'literal') {{
      const note = document.createElement('div');
      note.className = 'muted';
      note.textContent = binding.mode === 'commit'
        ? '<commit>'
        : '<branch>';
      return note;
    }}
    if (kind === 'boolean') {{
      const select = makeSelect();
      select.setAttribute('data-binding-value', 'true');
      for (const value of ['true', 'false']) {{
        const option = document.createElement('option');
        option.value = value;
        option.textContent = value;
        option.selected = value === String(binding.value || 'false');
        select.appendChild(option);
      }}
      return select;
    }}
    if (kind === 'json') {{
      const textarea = document.createElement('textarea');
      textarea.rows = 3;
      textarea.value = binding.value || '';
      textarea.setAttribute('data-binding-value', 'true');
      return textarea;
    }}
    const input = makeInput(kind === 'integer' ? 'number' : 'text', binding.value || '');
    input.setAttribute('data-binding-value', 'true');
    return input;
  }}

  function renderInputTable(row) {{
    const derivedJobs = buildDerivedJobs([...list.querySelectorAll('[data-workflow-job-row]')]);
    const wrap = row.querySelector('[data-inputs-wrap]');
    const summary = row.querySelector('[data-input-summary]');
    wrap.replaceChildren();
    const runnerId = row.querySelector('[data-field="runner_id"]').value;
    const runnerJobName = row.querySelector('[data-field="runner_job_name"]').value;
    const definition = getJobDefinition(runnerId, runnerJobName);
    const inputs = definition ? Object.entries(definition.inputs || {{}}) : [];
    if (inputs.length === 0) {{
      const empty = document.createElement('div');
      empty.className = 'muted';
      empty.textContent = 'This runner job does not declare inputs.';
      wrap.appendChild(empty);
      if (summary) summary.textContent = 'No inputs';
      return;
    }}
    if (summary) summary.textContent = `${{inputs.length}} input${{inputs.length === 1 ? '' : 's'}}`;
    const table = document.createElement('table');
    table.className = 'inputs-table';
    table.innerHTML = '<thead><tr><th>Input</th><th>Type</th><th>Binding</th><th>Value</th></tr></thead>';
    const tbody = document.createElement('tbody');
    for (const [inputName, inputDef] of inputs) {{
      const inputRow = document.createElement('tr');
      inputRow.setAttribute('data-input-row', 'true');
      inputRow.dataset.inputName = inputName;
      inputRow.dataset.inputKind = inputDef.type;
      const binding = inferBinding(inputName, inputDef.type, row._inputs[inputName]);
      inputRow.dataset.bindingValue = binding.value || '';

      const nameCell = document.createElement('td');
      nameCell.innerHTML = `<strong>${{inputName}}</strong>${{inputDef.required ? ' <span class="badge badge-warning">required</span>' : ''}}`;
      const typeCell = document.createElement('td');
      typeCell.textContent = inputDef.type;
      const modeCell = document.createElement('td');
      const modeSelect = makeSelect();
      modeSelect.setAttribute('data-binding-mode', 'true');
      for (const [value, label] of bindingModesFor(inputDef.type)) {{
        const option = document.createElement('option');
        option.value = value;
        option.textContent = label;
        option.selected = value === binding.mode;
        modeSelect.appendChild(option);
      }}
      const valueCell = document.createElement('td');
      modeCell.appendChild(modeSelect);

      function paintValueField() {{
        valueCell.replaceChildren();
        const currentBinding = {{
          mode: modeSelect.value,
          value: inputRow.dataset.bindingValue || binding.value || ''
        }};
        const field = buildValueField(inputDef.type, currentBinding, inputRow);
        if ('value' in field) {{
          field.addEventListener('input', () => {{
            inputRow.dataset.bindingValue = field.value || '';
            syncJobsJson();
          }});
          field.addEventListener('change', () => {{
            inputRow.dataset.bindingValue = field.value || '';
            syncJobsJson();
          }});
        }}
        valueCell.appendChild(field);
        const hint = literalHintFor(inputDef.type, modeSelect.value)
          || outputBindingHint(row, inputRow, modeSelect.value, derivedJobs);
        if (hint) {{
          const note = document.createElement('div');
          note.className = 'muted';
          note.textContent = hint;
          valueCell.appendChild(note);
        }}
      }}

      modeSelect.addEventListener('change', () => {{
        inputRow.dataset.bindingValue = '';
        paintValueField();
        syncJobsJson();
      }});
      paintValueField();

      inputRow.append(nameCell, typeCell, modeCell, valueCell);
      tbody.appendChild(inputRow);
    }}
    table.appendChild(tbody);
    wrap.appendChild(table);
  }}

  function renderOutputTable(derivedJobs) {{
    for (const job of derivedJobs) {{
      const wrap = job.row.querySelector('[data-outputs-wrap]');
      const summary = job.row.querySelector('[data-output-summary]');
      if (!wrap) continue;
      wrap.replaceChildren();
      const definition = getJobDefinition(job.runnerId, job.runnerJobName);
      const outputs = definition ? Object.entries(definition.outputs || {{}}) : [];
      if (summary) summary.textContent = outputs.length === 0
        ? 'No outputs'
        : `${{outputs.length}} output${{outputs.length === 1 ? '' : 's'}}`;
      if (outputs.length === 0) {{
        const empty = document.createElement('div');
        empty.className = 'muted';
        empty.textContent = 'This runner job does not declare outputs.';
        wrap.appendChild(empty);
        continue;
      }}
      const table = document.createElement('table');
      table.className = 'inputs-table';
      table.innerHTML = '<thead><tr><th>Output</th><th>Type</th><th>Required</th></tr></thead>';
      const tbody = document.createElement('tbody');
      for (const [outputName, outputDef] of outputs) {{
        const outputRow = document.createElement('tr');
        const nameCell = document.createElement('td');
        nameCell.innerHTML = `<strong>${{outputName}}</strong>`;
        const typeCell = document.createElement('td');
        typeCell.textContent = outputDef.type || 'unknown';
        const requiredCell = document.createElement('td');
        requiredCell.textContent = outputDef.required ? 'required' : 'optional';
        outputRow.append(nameCell, typeCell, requiredCell);
        tbody.appendChild(outputRow);
      }}
      table.appendChild(tbody);
      wrap.appendChild(table);
    }}
  }}

  function addRow(job) {{
    const row = document.createElement('fieldset');
    row.setAttribute('data-workflow-job-row', 'true');
    row.className = 'job-builder-row';
    row._inputs = job.inputs || {{}};
    row._allowFailure = Boolean(job.allow_failure);

    const runnerSelect = makeSelect();
    runnerSelect.setAttribute('data-field', 'runner_id');
    const jobSelect = makeSelect();
    jobSelect.setAttribute('data-field', 'runner_job_name');
    const inputSummary = document.createElement('button');
    inputSummary.type = 'button';
    inputSummary.className = 'input-summary-trigger ghost';
    inputSummary.setAttribute('data-input-summary', 'true');
    const outputSummary = document.createElement('button');
    outputSummary.type = 'button';
    outputSummary.className = 'input-summary-trigger ghost';
    outputSummary.setAttribute('data-output-summary', 'true');
    const inputsDialog = document.createElement('dialog');
    inputsDialog.className = 'inputs-dialog';
    const dialogCard = document.createElement('div');
    dialogCard.className = 'dialog-card';
    const dialogHeader = document.createElement('div');
    dialogHeader.className = 'section-head';
    const dialogHeaderText = document.createElement('div');
    dialogHeaderText.innerHTML = '<div class="eyebrow">Inputs</div><h3>Configure job inputs</h3><p class="muted">Bindings are saved back into the workflow when you submit the form.</p>';
    const dialogCloseTop = document.createElement('button');
    dialogCloseTop.type = 'button';
    dialogCloseTop.textContent = 'Close';
    dialogCloseTop.className = 'ghost';
    dialogHeader.append(dialogHeaderText, dialogCloseTop);
    const inputsWrap = document.createElement('div');
    inputsWrap.className = 'inputs-wrap';
    inputsWrap.setAttribute('data-inputs-wrap', 'true');
    const dialogActions = document.createElement('div');
    dialogActions.className = 'actions';
    const dialogDone = document.createElement('button');
    dialogDone.type = 'button';
    dialogDone.textContent = 'Done';
    dialogActions.appendChild(dialogDone);
    dialogCard.append(dialogHeader, inputsWrap, dialogActions);
    inputsDialog.appendChild(dialogCard);
    const outputsDialog = document.createElement('dialog');
    outputsDialog.className = 'inputs-dialog';
    const outputsDialogCard = document.createElement('div');
    outputsDialogCard.className = 'dialog-card';
    const outputsDialogHeader = document.createElement('div');
    outputsDialogHeader.className = 'section-head';
    const outputsDialogHeaderText = document.createElement('div');
    outputsDialogHeaderText.innerHTML = '<div class="eyebrow">Outputs</div><h3>Declared job outputs</h3><p class="muted">Runner jobs can expose artifact or typed outputs, which downstream jobs can consume.</p>';
    const outputsDialogCloseTop = document.createElement('button');
    outputsDialogCloseTop.type = 'button';
    outputsDialogCloseTop.textContent = 'Close';
    outputsDialogCloseTop.className = 'ghost';
    outputsDialogHeader.append(outputsDialogHeaderText, outputsDialogCloseTop);
    const outputsWrap = document.createElement('div');
    outputsWrap.className = 'inputs-wrap';
    outputsWrap.setAttribute('data-outputs-wrap', 'true');
    const outputsDialogActions = document.createElement('div');
    outputsDialogActions.className = 'actions';
    const outputsDialogDone = document.createElement('button');
    outputsDialogDone.type = 'button';
    outputsDialogDone.textContent = 'Done';
    outputsDialogActions.appendChild(outputsDialogDone);
    outputsDialogCard.append(outputsDialogHeader, outputsWrap, outputsDialogActions);
    outputsDialog.appendChild(outputsDialogCard);
    const removeButton = document.createElement('button');
    removeButton.type = 'button';
    removeButton.textContent = 'Remove job';
    removeButton.className = 'ghost';
    const removeButtonWrap = document.createElement('div');
    removeButtonWrap.className = 'job-row-remove';
    removeButtonWrap.appendChild(removeButton);

    fillRunnerOptions(runnerSelect, job.runner_id);
    fillJobOptions(runnerSelect, jobSelect, job.runner_job_name);

    runnerSelect.addEventListener('change', () => {{
      fillJobOptions(runnerSelect, jobSelect, '');
      row._inputs = {{}};
      renderInputTable(row);
      renderOutputTable(buildDerivedJobs([...list.querySelectorAll('[data-workflow-job-row]')]));
      syncJobsJson();
    }});
    jobSelect.addEventListener('change', () => {{
      row._inputs = {{}};
      renderInputTable(row);
      renderOutputTable(buildDerivedJobs([...list.querySelectorAll('[data-workflow-job-row]')]));
      syncJobsJson();
    }});
    inputSummary.addEventListener('click', () => {{
      if (typeof inputsDialog.showModal === 'function') {{
        inputsDialog.showModal();
      }} else {{
        inputsDialog.setAttribute('open', 'open');
      }}
    }});
    outputSummary.addEventListener('click', () => {{
      if (typeof outputsDialog.showModal === 'function') {{
        outputsDialog.showModal();
      }} else {{
        outputsDialog.setAttribute('open', 'open');
      }}
    }});
    for (const closeButton of [dialogCloseTop, dialogDone]) {{
      closeButton.addEventListener('click', () => inputsDialog.close());
    }}
    for (const closeButton of [outputsDialogCloseTop, outputsDialogDone]) {{
      closeButton.addEventListener('click', () => outputsDialog.close());
    }}
    removeButton.addEventListener('click', () => {{
      inputsDialog.close();
      outputsDialog.close();
      row.remove();
      syncJobsJson();
    }});

    row.append(
      labelWrap('Runner', runnerSelect),
      labelWrap('Job', jobSelect),
      labelWrap('Inputs', inputSummary),
      labelWrap('Outputs', outputSummary),
      inputsDialog,
      outputsDialog,
      removeButtonWrap
    );
    list.appendChild(row);
    renderInputTable(row);
    renderOutputTable(buildDerivedJobs([...list.querySelectorAll('[data-workflow-job-row]')]));
    syncJobsJson();
  }}

  addButton.addEventListener('click', () => addRow({{
    runner_id: catalog.length === 1 ? catalog[0].id : '',
    runner_job_name: '',
    inputs: {{}}
  }}));

  for (const job of initialJobs) {{
    addRow(job);
  }}
}})();
</script><div class="actions"><button type="submit">{submit_label}</button></div>"#,
        repo_input = repo_field,
        name = name,
        push_selected = if trigger_kind == "push" {
            "selected"
        } else {
            ""
        },
        manual_selected = if trigger_kind == "manual" {
            "selected"
        } else {
            ""
        },
        branch_name = html_escape(&branch_name),
        jobs_json = html_escape(&jobs_json),
        runner_catalog_json = runner_catalog_json,
        initial_jobs_json = initial_jobs_json,
        submit_label = if workflow.is_some() {
            "Save workflow"
        } else {
            "Create workflow"
        }
    )
}

fn render_script_json(input: &str) -> String {
    input.replace("</script", "<\\/script")
}

fn visible_repos_for_user(state: &Arc<AppState>, user: &User) -> Result<Vec<Repo>, Response> {
    Ok(state
        .db
        .list_repos()
        .map_err(internal_error)?
        .into_iter()
        .filter(|repo| can_view_repo(user, repo))
        .collect())
}
fn can_view_repo(user: &User, repo: &Repo) -> bool {
    user.role == "admin" || repo.owner_id == user.id
}
fn repo_ids_contains(repos: &[Repo], repo_id: &str) -> bool {
    repos.iter().any(|repo| repo.id == repo_id)
}
fn authorized_repo(state: &Arc<AppState>, user: &User, repo_id: &str) -> Result<Repo, Response> {
    let repo = state
        .db
        .get_repo(repo_id)
        .map_err(internal_error)?
        .ok_or_else(|| not_found("repo"))?;
    if !can_view_repo(user, &repo) {
        return Err(forbidden("repo access denied"));
    }
    Ok(repo)
}
fn authorized_workflow(
    state: &Arc<AppState>,
    user: &User,
    workflow_id: &str,
) -> Result<Workflow, Response> {
    let workflow = state
        .db
        .get_workflow(workflow_id)
        .map_err(internal_error)?
        .ok_or_else(|| not_found("workflow"))?;
    let repo = authorized_repo(state, user, &workflow.repo_id)?;
    if repo.id != workflow.repo_id {
        return Err(forbidden("workflow access denied"));
    }
    Ok(workflow)
}
fn authorized_pipeline(
    state: &Arc<AppState>,
    user: &User,
    pipeline_id: &str,
) -> Result<PipelineRun, Response> {
    let pipeline = state
        .db
        .get_pipeline_run(pipeline_id)
        .map_err(internal_error)?
        .ok_or_else(|| not_found("pipeline"))?;
    let repo = authorized_repo(state, user, &pipeline.repo_id)?;
    if repo.id != pipeline.repo_id {
        return Err(forbidden("pipeline access denied"));
    }
    Ok(pipeline)
}
fn repo_clone_url(state: &Arc<AppState>, repo: &Repo) -> String {
    format!(
        "ssh://git@{}/{}/{}",
        state.config.server.public_base_url.trim_end_matches('/'),
        repo.owner_username,
        repo.name
    )
}
fn validate_username(username: &str) -> Result<(), Response> {
    let trimmed = username.trim();
    if trimmed.len() < 3 {
        return Err(bad_request("username must be at least 3 characters"));
    }
    if !trimmed
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.'))
    {
        return Err(bad_request("username contains invalid characters"));
    }
    Ok(())
}
fn validate_password(password: &str) -> Result<(), Response> {
    if password.len() < 8 {
        Err(bad_request("password must be at least 8 characters"))
    } else {
        Ok(())
    }
}
fn validate_role(role: &str) -> Result<(), Response> {
    if matches!(role, "admin" | "developer") {
        Ok(())
    } else {
        Err(bad_request("role must be admin or developer"))
    }
}
fn validate_branch_name(branch: &str) -> Result<(), Response> {
    if branch.trim().is_empty() || branch.contains(' ') {
        Err(bad_request("default branch is invalid"))
    } else {
        Ok(())
    }
}
fn validate_runner_name(name: &str) -> Result<(), Response> {
    if name.trim().is_empty() {
        Err(bad_request("runner name cannot be empty"))
    } else {
        Ok(())
    }
}
fn validate_base_url(url: &str) -> Result<(), Response> {
    url::Url::parse(url)
        .map(|_| ())
        .map_err(|_| bad_request("base_url must be a valid URL"))
}
pub(super) fn csrf_token(state: &Arc<AppState>, user: &User) -> String {
    let mut mac = HmacSha256::new_from_slice(state.config.auth.session_secret.as_bytes())
        .expect("valid hmac key");
    mac.update(b"csrf:");
    mac.update(user.id.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}
fn verify_csrf(state: &Arc<AppState>, user: &User, token: &str) -> Result<(), Response> {
    if token == csrf_token(state, user) {
        Ok(())
    } else {
        Err(forbidden("csrf validation failed"))
    }
}
fn csrf_input(token: &str) -> String {
    format!(
        "<input type=\"hidden\" name=\"csrf_token\" value=\"{}\" />",
        html_escape(token)
    )
}
fn page_intro(title: &str, subtitle: &str) -> String {
    format!(
        r#"<section class="hero-card"><div class="eyebrow">Strait CI</div><h1>{}</h1><p class="muted">{}</p></section>"#,
        html_escape(title),
        html_escape(subtitle)
    )
}
fn badge(label: &str, tone: &str) -> String {
    format!(
        r#"<span class="badge badge-{}">{}</span>"#,
        html_escape(tone),
        html_escape(label)
    )
}
fn status_tone(status: &str) -> &'static str {
    match status {
        "success" => "success",
        "running" | "pending" => "warning",
        "failed" | "canceled" => "danger",
        "cancel_requested" | "canceling" => "neutral",
        _ => "neutral",
    }
}
fn runner_state_tone(state: &str) -> &'static str {
    match state {
        "healthy" => "success",
        "unknown" => "warning",
        _ => "danger",
    }
}
fn render_workflow_job_chips(definition: &WorkflowDefinition) -> String {
    if definition.jobs.is_empty() {
        return badge("no jobs", "neutral");
    }
    definition
        .jobs
        .iter()
        .enumerate()
        .map(|(index, job)| {
            format!(
                r#"<span class="chip">{}</span>"#,
                html_escape(&job.display_name(index))
            )
        })
        .collect::<Vec<_>>()
        .join("")
}
fn layout(title: &str, body: &str) -> String {
    let app_shell = format!(
        r#"<div class="app-shell"><nav class="topbar"><a class="brand" href="/repos"><span class="brand-mark">S</span><span>Strait CI</span></a><div class="nav-links"><a href="/repos">Repos</a><a href="/runners">Runners</a><a href="/workflows">Workflows</a><a href="/pipelines">Pipelines</a><a href="/users">Users</a></div><form method="post" action="/logout"><button type="submit" class="ghost">Logout</button></form></nav><main class="page-shell">{}</main></div>"#,
        body
    );
    format!(
        "<!doctype html><html><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width, initial-scale=1\"><title>{}</title><style>{}</style></head><body>{}</body></html>",
        title,
        app_styles(),
        app_shell
    )
}
fn layout_public(title: &str, body: &str) -> String {
    format!(
        "<!doctype html><html><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width, initial-scale=1\"><title>{}</title><style>{}</style></head><body><main class=\"public-shell\">{}</main></body></html>",
        title,
        app_styles(),
        body
    )
}
fn app_styles() -> &'static str {
    r#":root{color-scheme:light;--bg:#f5f1e8;--bg-2:#efe8da;--panel:#fffdf8;--panel-soft:#f7f0e4;--ink:#1f241f;--muted:#5e655d;--line:#d7ccb7;--accent:#0f766e;--accent-2:#d97706;--danger:#b42318;--success:#157347;--radius:20px;--shadow:0 18px 40px rgba(72,52,24,.10)}*{box-sizing:border-box}body{margin:0;font-family:ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;background:radial-gradient(circle at top,#fff7ea 0,#f5f1e8 42%,#ece4d2 100%);color:var(--ink)}a{color:inherit}code,pre{font-family:ui-monospace,SFMono-Regular,Menlo,monospace}pre{margin:0;white-space:pre-wrap;word-break:break-word}h1,h2,h3,p{margin:0}button,input,select,textarea{font:inherit}button{border:0;border-radius:999px;padding:.82rem 1.15rem;background:var(--ink);color:#fff;cursor:pointer;font-weight:600}button.secondary{background:var(--accent)}button.ghost{background:transparent;color:var(--ink);border:1px solid var(--line)}label{display:flex;flex-direction:column;gap:.45rem;font-size:.95rem;font-weight:600;color:var(--muted)}label span{font-size:.82rem;text-transform:uppercase;letter-spacing:.08em}input,select,textarea{width:100%;border:1px solid var(--line);border-radius:14px;padding:.9rem 1rem;background:#fff;color:var(--ink)}input:focus,select:focus,textarea:focus{outline:2px solid rgba(15,118,110,.15);border-color:var(--accent)}textarea{min-height:120px;resize:vertical}.app-shell,.public-shell{min-height:100vh}.topbar{display:flex;align-items:center;justify-content:space-between;gap:1rem;padding:1rem 1.4rem;position:sticky;top:0;background:rgba(245,241,232,.86);backdrop-filter:blur(12px);border-bottom:1px solid rgba(215,204,183,.7)}.brand{display:flex;align-items:center;gap:.75rem;text-decoration:none;font-weight:800;letter-spacing:.04em}.brand-mark{display:grid;place-items:center;width:2rem;height:2rem;border-radius:999px;background:linear-gradient(135deg,var(--accent),#115e59);color:#fff}.nav-links{display:flex;flex-wrap:wrap;gap:.5rem}.nav-links a{text-decoration:none;padding:.65rem .9rem;border-radius:999px;color:var(--muted)}.nav-links a:hover{background:rgba(255,255,255,.65);color:var(--ink)}.page-shell,.public-shell{width:min(1180px,calc(100vw - 2rem));margin:0 auto;padding:1.5rem 0 3rem}.hero-card,.card,.entity-card,.job-card,.list-row{background:var(--panel);border:1px solid rgba(255,255,255,.75);box-shadow:var(--shadow)}.hero-card{padding:1.6rem 1.7rem;border-radius:28px;background:linear-gradient(160deg,#fff9ef 0,#f6eddc 100%);margin-bottom:1.25rem}.hero-card h1{font-size:clamp(2rem,3vw,3rem);line-height:1}.hero-card.auth-card{max-width:460px;margin:8vh auto 0}.auth-shell{display:grid;min-height:100vh;place-items:center}.eyebrow{font-size:.78rem;letter-spacing:.14em;text-transform:uppercase;color:var(--accent-2);font-weight:800;margin-bottom:.55rem}.muted{color:var(--muted);line-height:1.55}.card{padding:1.35rem;border-radius:24px;margin-top:1rem}.soft-card{background:var(--panel-soft);box-shadow:none;border-color:var(--line)}.section-head{display:flex;justify-content:space-between;align-items:flex-start;gap:1rem;margin-bottom:1rem}.section-head h2,.section-head h3{font-size:1.3rem}.stack-md{display:flex;flex-direction:column;gap:1rem}.stack-lg{display:flex;flex-direction:column;gap:1.2rem}.form-grid{display:grid;gap:1rem}.form-grid-2{grid-template-columns:repeat(2,minmax(0,1fr))}.form-grid-3{grid-template-columns:repeat(3,minmax(0,1fr))}.actions{display:flex;flex-wrap:wrap;gap:.75rem;align-items:center}.card-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:1rem}.entity-card,.job-card{padding:1.15rem;border-radius:22px}.entity-head{display:flex;justify-content:space-between;align-items:flex-start;gap:1rem;margin-bottom:1rem}.entity-head h3{font-size:1.15rem}.badge-row,.chip-row{display:flex;flex-wrap:wrap;gap:.5rem}.badge,.chip{display:inline-flex;align-items:center;border-radius:999px;padding:.38rem .7rem;font-size:.78rem;font-weight:700;letter-spacing:.03em}.badge-success{background:#dff3e7;color:#14532d}.badge-warning{background:#fff1d6;color:#92400e}.badge-danger{background:#fde7e5;color:#9f1239}.badge-neutral{background:#ece7dc;color:#4b5563}.chip{background:#efe8da;color:#3f453f}.meta-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:.75rem;margin-top:.35rem}.meta-pair{display:flex;flex-direction:column;gap:.25rem;padding:.8rem .9rem;background:rgba(239,232,218,.55);border-radius:16px}.meta-pair span,.subsection-title,.inline-note{font-size:.8rem;text-transform:uppercase;letter-spacing:.08em;color:var(--muted)}.meta-pair strong{font-size:.95rem;color:var(--ink)}.inline-fields{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:.85rem}.inset-panel{padding:1rem;border-radius:18px;background:var(--panel-soft);border:1px solid var(--line)}.table-wrap{overflow:auto}table{width:100%;border-collapse:collapse}th,td{text-align:left;padding:.9rem 1rem;border-bottom:1px solid var(--line)}thead th{font-size:.8rem;text-transform:uppercase;letter-spacing:.08em;color:var(--muted)}.list-row{display:flex;justify-content:space-between;align-items:center;gap:1rem;padding:1rem 1.1rem;border-radius:18px}.list-row-meta{display:flex;gap:.8rem;align-items:center;color:var(--muted);text-align:right}.job-builder-row{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:.85rem;align-items:end;border:1px solid var(--line);border-radius:18px;padding:1rem;background:#fff}.job-builder-row label:last-of-type{align-self:center}.job-builder-summary{grid-column:1/-1;display:flex;justify-content:space-between;gap:1rem;align-items:center}.job-row-remove{display:flex;justify-content:flex-end;align-items:end}.checkbox-field{justify-content:flex-end}.checkbox-field input{width:auto;min-width:1.1rem;align-self:flex-start}.log-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:1rem;margin-top:1rem}.log-panel{padding:1rem;border:1px solid var(--line);border-radius:18px;background:#171717;color:#f8fafc}.log-panel pre{margin-top:.75rem;font-size:.88rem;line-height:1.45}.subsection{margin-top:1rem}.inputs-dialog{padding:0;border:0;background:transparent;max-width:min(860px,calc(100vw - 2rem));width:100%}.inputs-dialog::backdrop{background:rgba(20,19,16,.45);backdrop-filter:blur(4px)}.dialog-card{background:var(--panel);border:1px solid rgba(255,255,255,.85);box-shadow:var(--shadow);border-radius:24px;padding:1.35rem}.inputs-wrap{max-height:60vh;overflow:auto}.public-shell{display:grid;place-items:center;padding:2rem}@media (max-width:900px){.form-grid-2,.form-grid-3,.inline-fields,.log-grid{grid-template-columns:1fr}.topbar{flex-direction:column;align-items:stretch}.nav-links{justify-content:center}.entity-head,.section-head,.list-row,.job-builder-summary{flex-direction:column;align-items:flex-start}.job-row-remove{justify-content:flex-start}.list-row-meta{text-align:left;flex-wrap:wrap}}"#
}
fn html_escape(input: &str) -> String {
    input
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}
fn internal_error(error: impl std::fmt::Display) -> Response {
    (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response()
}
fn internal_error_text(error: impl std::fmt::Display) -> Response {
    internal_error(error)
}
fn bad_request(error: impl std::fmt::Display) -> Response {
    (StatusCode::BAD_REQUEST, error.to_string()).into_response()
}
fn forbidden(error: impl std::fmt::Display) -> Response {
    (StatusCode::FORBIDDEN, error.to_string()).into_response()
}
fn not_found(entity: &str) -> Response {
    (StatusCode::NOT_FOUND, format!("{entity} not found")).into_response()
}
