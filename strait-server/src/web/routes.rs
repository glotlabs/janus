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
    models::{self, PipelineRun, Repo, User, Workflow, WorkflowDefinition, WorkflowJobDefinition, WorkflowTrigger},
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
        .route("/pipelines/{pipeline_id}/cancel", post(cancel_pipeline_route))
        .route("/api/me", get(api_me))
        .route("/api/repos", get(api_list_repos).post(api_create_repo))
        .route("/api/runners", get(api_list_runners).post(api_create_runner))
        .route("/api/workflows", get(api_list_workflows).post(api_create_workflow))
        .route("/api/workflows/{workflow_id}", get(api_get_workflow).put(api_update_workflow))
        .route("/api/pipelines", get(api_list_pipelines))
        .route("/api/pipelines/{pipeline_id}", get(api_get_pipeline))
        .with_state(state)
}

async fn health() -> Html<String> { Html("<pre>ok</pre>".to_string()) }
async fn ready() -> Html<String> { Html("<pre>ready</pre>".to_string()) }
async fn index() -> impl IntoResponse { Redirect::to("/repos") }

async fn login_form() -> Html<String> {
    Html(layout_public(
        "Login",
        r#"<form method="post" action="/login">
<label>Username <input name="username" autocomplete="username" /></label><br/>
<label>Password <input name="password" type="password" autocomplete="current-password" /></label><br/>
<button type="submit">Login</button>
</form>"#,
    ))
}

#[derive(Deserialize)]
struct LoginForm { username: String, password: String }

async fn login(State(state): State<Arc<AppState>>, Form(form): Form<LoginForm>) -> Response {
    if !state.allow_login_attempt(&form.username) {
        return (StatusCode::TOO_MANY_REQUESTS, "too many login attempts, try again later").into_response();
    }
    let Ok(Some((user, hash))) = state.db.get_user_credentials(&form.username) else {
        return crate::auth::unauthorized();
    };
    if !verify_password(&form.password, &hash) {
        return crate::auth::unauthorized();
    }
    let _ = state.db.cleanup_expired_sessions();
    let _ = state.db.delete_sessions_for_user(&user.id);
    let expires_at = (Utc::now() + Duration::days(state.config.auth.session_ttl_days as i64)).to_rfc3339();
    let Ok(session_id) = state.db.create_session(&user.id, &expires_at) else {
        return (StatusCode::INTERNAL_SERVER_ERROR, "failed to create session").into_response();
    };
    let mut response = Redirect::to("/repos").into_response();
    response.headers_mut().append(
        SET_COOKIE,
        session_cookie(&state.config.auth.session_secret, &session_id, state.config.auth.session_cookie_secure)
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
        clear_session_cookie().to_string().parse().expect("cookie header"),
    );
    response
}

#[derive(Deserialize)] struct CsrfOnlyForm { csrf_token: String }
#[derive(Deserialize)] struct CreateUserForm { csrf_token: String, username: String, password: String, role: String }
#[derive(Deserialize)] struct CreateRepoForm { csrf_token: String, owner_id: String, name: String, default_branch: String }
#[derive(Deserialize)] struct CreateRunnerForm { csrf_token: String, name: String, base_url: String, token: String }
#[derive(Deserialize)] struct WorkflowForm { csrf_token: String, repo_id: String, name: String, enabled: Option<String>, trigger_kind: String, branches_csv: String, jobs_spec: String }
#[derive(Deserialize)] struct ManualTriggerForm { csrf_token: String, branch: Option<String>, commit: Option<String> }

async fn users_page(_: AdminUser, CurrentUser(user): CurrentUser, State(state): State<Arc<AppState>>) -> Result<Html<String>, Response> {
    let csrf = csrf_token(&state, &user);
    let users = state.db.list_users().map_err(internal_error)?;
    let mut body = format!(r#"<form method="post" action="/users">{}<label>Username <input name="username" /></label><label>Password <input name="password" type="password" /></label><label>Role <select name="role"><option value="developer">developer</option><option value="admin">admin</option></select></label><button type="submit">Create user</button></form><ul>"#, csrf_input(&csrf));
    for item in users { body.push_str(&format!("<li>{} ({})</li>", html_escape(&item.username), item.role)); }
    body.push_str("</ul>");
    Ok(Html(layout("Users", &body)))
}

async fn create_user(_: AdminUser, CurrentUser(user): CurrentUser, State(state): State<Arc<AppState>>, Form(form): Form<CreateUserForm>) -> Result<Redirect, Response> {
    verify_csrf(&state, &user, &form.csrf_token)?;
    validate_username(&form.username)?;
    validate_password(&form.password)?;
    validate_role(&form.role)?;
    let hash = hash_password(&form.password).map_err(internal_error_text)?;
    state.db.create_user(&form.username, &hash, &form.role).map_err(internal_error)?;
    Ok(Redirect::to("/users"))
}

async fn repos_page(CurrentUser(user): CurrentUser, State(state): State<Arc<AppState>>) -> Result<Html<String>, Response> {
    let repos = state.db.list_repos().map_err(internal_error)?;
    let users = state.db.list_users().map_err(internal_error)?;
    let csrf = csrf_token(&state, &user);
    let mut body = format!(r#"<form method="post" action="/repos">{}<label>Name <input name="name" /></label>"#, csrf_input(&csrf));
    if user.role == "admin" {
        body.push_str(r#"<label>Owner <select name="owner_id">"#);
        for candidate in users {
            let selected = if candidate.id == user.id { " selected" } else { "" };
            body.push_str(&format!("<option value=\"{}\"{}>{}</option>", candidate.id, selected, html_escape(&candidate.username)));
        }
        body.push_str("</select></label>");
    } else {
        body.push_str(&format!("<input type=\"hidden\" name=\"owner_id\" value=\"{}\" /><p>Owner: {}</p>", user.id, html_escape(&user.username)));
    }
    body.push_str(r#"<label>Default branch <input name="default_branch" value="main" /></label><button type="submit">Create repo</button></form><ul>"#);
    for repo in repos.into_iter().filter(|repo| can_view_repo(&user, repo)) {
        let clone_url = repo_clone_url(&state, &repo);
        body.push_str(&format!("<li><strong>{}/{}</strong> clone: <code>{}</code><form method=\"post\" action=\"/repos/{}/trigger\">{}<input name=\"branch\" value=\"refs/heads/{}\" /><input name=\"commit\" value=\"HEAD\" /><button type=\"submit\">Manual trigger</button></form></li>", html_escape(&repo.owner_username), html_escape(&repo.name), html_escape(&clone_url), repo.id, csrf_input(&csrf), html_escape(&repo.default_branch)));
    }
    body.push_str("</ul>");
    Ok(Html(layout("Repos", &body)))
}

async fn create_repo(CurrentUser(user): CurrentUser, State(state): State<Arc<AppState>>, Form(form): Form<CreateRepoForm>) -> Result<Redirect, Response> {
    verify_csrf(&state, &user, &form.csrf_token)?;
    if user.role != "admin" && form.owner_id != user.id { return Err(forbidden("developers can only create their own repos")); }
    let owner = state.db.get_user(&form.owner_id).map_err(internal_error)?.ok_or_else(|| bad_request("owner not found"))?;
    let normalized = git::validate_repo_name(&form.name).map_err(bad_request)?;
    validate_branch_name(&form.default_branch)?;
    let bare_path = PathBuf::from(&state.config.repos_dir).join(format!("{}.git", Uuid::now_v7()));
    let repo_id = state.db.create_repo(&owner.id, &form.name, &normalized, &bare_path.display().to_string(), &form.default_branch).map_err(internal_error)?;
    let repo = state.db.get_repo(&repo_id).map_err(internal_error)?.ok_or_else(|| internal_error_text("missing repo after create"))?;
    git::init_bare_repo(Path::new(&repo.bare_path)).map_err(internal_error_text)?;
    git::install_post_receive_hook(Path::new(&repo.bare_path), state.server_bin.as_path(), state.config_path.as_path(), &repo.id).map_err(internal_error_text)?;
    Ok(Redirect::to("/repos"))
}

async fn trigger_repo(CurrentUser(user): CurrentUser, State(state): State<Arc<AppState>>, AxumPath(repo_id): AxumPath<String>, Form(form): Form<ManualTriggerForm>) -> Result<Redirect, Response> {
    verify_csrf(&state, &user, &form.csrf_token)?;
    let repo = authorized_repo(&state, &user, &repo_id)?;
    let branch = form.branch.unwrap_or_else(|| format!("refs/heads/{}", repo.default_branch));
    let commit = form.commit.unwrap_or_else(|| "HEAD".to_string());
    let refs = vec![models::PushEventRef { old_rev: "0000000000000000000000000000000000000000".to_string(), new_rev: commit, ref_name: branch }];
    let key = git::event_key(&repo_id, &refs);
    state.db.create_push_event(&repo_id, &key, &refs).map_err(internal_error)?;
    Ok(Redirect::to("/pipelines"))
}

async fn runners_page(_: AdminUser, CurrentUser(user): CurrentUser, State(state): State<Arc<AppState>>) -> Result<Html<String>, Response> {
    let runners = state.db.list_runners().map_err(internal_error)?;
    let csrf = csrf_token(&state, &user);
    let mut body = format!(r#"<form method="post" action="/runners">{}<label>Name <input name="name" /></label><label>Base URL <input name="base_url" placeholder="http://127.0.0.1:8080" /></label><label>Token <input name="token" /></label><button type="submit">Add runner</button></form><ul>"#, csrf_input(&csrf));
    for runner in runners {
        body.push_str(&format!("<li><strong>{}</strong> {} [{}]<form method=\"post\" action=\"/runners/{}/test\">{}<button type=\"submit\">Test</button></form><form method=\"post\" action=\"/runners/{}/toggle\">{}<button type=\"submit\">{}</button></form>", html_escape(&runner.name), html_escape(&runner.base_url), html_escape(&runner.last_health_state), runner.id, csrf_input(&csrf), runner.id, csrf_input(&csrf), if runner.enabled { "Disable" } else { "Enable" }));
        let jobs = state.db.list_runner_jobs(&runner.id).map_err(internal_error)?;
        if !jobs.is_empty() {
            body.push_str("<ul>");
            for (job_name, _) in jobs { body.push_str(&format!("<li>{}</li>", html_escape(&job_name))); }
            body.push_str("</ul>");
        }
        body.push_str("</li>");
    }
    body.push_str("</ul>");
    Ok(Html(layout("Runners", &body)))
}

async fn create_runner(_: AdminUser, CurrentUser(user): CurrentUser, State(state): State<Arc<AppState>>, Form(form): Form<CreateRunnerForm>) -> Result<Redirect, Response> {
    verify_csrf(&state, &user, &form.csrf_token)?;
    validate_runner_name(&form.name)?;
    validate_base_url(&form.base_url)?;
    if form.token.trim().is_empty() { return Err(bad_request("runner token cannot be empty")); }
    let runner_id = state.db.create_runner(&form.name, &form.base_url, &form.token).map_err(internal_error)?;
    refresh_single_runner(&state, &runner_id).await.map_err(internal_error_text)?;
    Ok(Redirect::to("/runners"))
}

async fn toggle_runner(_: AdminUser, CurrentUser(user): CurrentUser, State(state): State<Arc<AppState>>, AxumPath(runner_id): AxumPath<String>, Form(form): Form<CsrfOnlyForm>) -> Result<Redirect, Response> {
    verify_csrf(&state, &user, &form.csrf_token)?;
    let runner = state.db.get_runner(&runner_id).map_err(internal_error)?.ok_or_else(|| not_found("runner"))?;
    state.db.set_runner_enabled(&runner_id, !runner.enabled).map_err(internal_error)?;
    Ok(Redirect::to("/runners"))
}

async fn test_runner(_: AdminUser, CurrentUser(user): CurrentUser, State(state): State<Arc<AppState>>, AxumPath(runner_id): AxumPath<String>, Form(form): Form<CsrfOnlyForm>) -> Result<Redirect, Response> {
    verify_csrf(&state, &user, &form.csrf_token)?;
    refresh_single_runner(&state, &runner_id).await.map_err(internal_error_text)?;
    Ok(Redirect::to("/runners"))
}

async fn workflows_page(CurrentUser(user): CurrentUser, State(state): State<Arc<AppState>>) -> Result<Html<String>, Response> {
    let repos = visible_repos_for_user(&state, &user)?;
    let workflows = state.db.list_workflows().map_err(internal_error)?;
    let csrf = csrf_token(&state, &user);
    let mut body = format!(r#"<form method="post" action="/workflows">{}<label>Repo <select name="repo_id">"#, csrf_input(&csrf));
    for repo in &repos {
        body.push_str(&format!("<option value=\"{}\">{}/{}</option>", repo.id, html_escape(&repo.owner_username), html_escape(&repo.name)));
    }
    body.push_str(&workflow_form_fields(None));
    body.push_str("</form><ul>");
    for workflow in workflows.into_iter().filter(|item| repo_ids_contains(&repos, &item.repo_id)) {
        body.push_str(&format!("<li><a href=\"/workflows/{}\">{}</a> repo={} version={} enabled={}</li>", workflow.id, html_escape(&workflow.name), html_escape(&workflow.repo_id), workflow.version, workflow.enabled));
    }
    body.push_str("</ul>");
    Ok(Html(layout("Workflows", &body)))
}

async fn workflow_detail_page(CurrentUser(user): CurrentUser, State(state): State<Arc<AppState>>, AxumPath(workflow_id): AxumPath<String>) -> Result<Html<String>, Response> {
    let workflow = authorized_workflow(&state, &user, &workflow_id)?;
    let csrf = csrf_token(&state, &user);
    let repo = state.db.get_repo(&workflow.repo_id).map_err(internal_error)?.ok_or_else(|| not_found("repo"))?;
    let body = format!("<p>Repo: {}/{}</p><form method=\"post\" action=\"/workflows/{}/update\">{}{}</form>", html_escape(&repo.owner_username), html_escape(&repo.name), workflow.id, csrf_input(&csrf), workflow_form_fields(Some(&workflow)));
    Ok(Html(layout("Workflow", &body)))
}

async fn create_workflow(CurrentUser(user): CurrentUser, State(state): State<Arc<AppState>>, Form(form): Form<WorkflowForm>) -> Result<Redirect, Response> {
    verify_csrf(&state, &user, &form.csrf_token)?;
    let repo = authorized_repo(&state, &user, &form.repo_id)?;
    let parsed = parse_workflow_form(&state, &form)?;
    state.db.create_workflow(&repo.id, &form.name.trim(), form.enabled.is_some(), &parsed.trigger_json, &parsed.definition_json).map_err(internal_error)?;
    Ok(Redirect::to("/workflows"))
}

async fn update_workflow(CurrentUser(user): CurrentUser, State(state): State<Arc<AppState>>, AxumPath(workflow_id): AxumPath<String>, Form(form): Form<WorkflowForm>) -> Result<Redirect, Response> {
    verify_csrf(&state, &user, &form.csrf_token)?;
    let workflow = authorized_workflow(&state, &user, &workflow_id)?;
    if workflow.repo_id != form.repo_id { return Err(bad_request("workflow repo cannot be changed")); }
    let parsed = parse_workflow_form(&state, &form)?;
    state.db.update_workflow(&workflow.id, form.name.trim(), form.enabled.is_some(), &parsed.trigger_json, &parsed.definition_json).map_err(internal_error)?;
    Ok(Redirect::to(&format!("/workflows/{}", workflow.id)))
}

async fn pipelines_page(CurrentUser(user): CurrentUser, State(state): State<Arc<AppState>>) -> Result<Html<String>, Response> {
    let pipelines = state.db.list_pipeline_runs().map_err(internal_error)?;
    let repos = state.db.list_repos().map_err(internal_error)?;
    let repo_by_id = repos.into_iter().map(|repo| (repo.id.clone(), repo)).collect::<BTreeMap<_, _>>();
    let mut body = String::from("<ul>");
    for pipeline in pipelines {
        if let Some(repo) = repo_by_id.get(&pipeline.repo_id) {
            if !can_view_repo(&user, repo) { continue; }
            body.push_str(&format!("<li><a href=\"/pipelines/{}\">{}</a> {} {} {}/{}</li>", pipeline.id, pipeline.id, html_escape(&pipeline.status), html_escape(&pipeline.trigger_ref.clone().unwrap_or_default()), html_escape(&repo.owner_username), html_escape(&repo.name)));
        }
    }
    body.push_str("</ul>");
    Ok(Html(layout("Pipelines", &body)))
}

async fn pipeline_detail(CurrentUser(user): CurrentUser, State(state): State<Arc<AppState>>, AxumPath(pipeline_id): AxumPath<String>) -> Result<Html<String>, Response> {
    let pipeline = authorized_pipeline(&state, &user, &pipeline_id)?;
    let snapshot = state.db.pipeline_snapshot(&pipeline.id).map_err(internal_error)?.ok_or_else(|| not_found("pipeline"))?;
    let csrf = csrf_token(&state, &user);
    let mut body = format!("<p>Status: {}</p><p>Trigger: {:?}</p><form method=\"post\" action=\"/pipelines/{}/rerun\">{}<button type=\"submit\">Rerun</button></form><form method=\"post\" action=\"/pipelines/{}/cancel\">{}<button type=\"submit\">Cancel</button></form><ul>", html_escape(&snapshot.pipeline.status), snapshot.pipeline.trigger_ref, snapshot.pipeline.id, csrf_input(&csrf), snapshot.pipeline.id, csrf_input(&csrf));
    for job in snapshot.jobs {
        body.push_str(&format!("<li><strong>{}</strong> [{}] runner={}<pre>{}</pre><pre>{}</pre></li>", html_escape(&job.run.job_name), html_escape(&job.run.status), html_escape(&job.run.runner_job_name), html_escape(&job.stdout), html_escape(&job.stderr)));
    }
    body.push_str("</ul><script>const e=new EventSource('/pipelines/");
    body.push_str(&pipeline.id);
    body.push_str("/events');e.onmessage=(msg)=>console.log(msg.data);</script>");
    Ok(Html(layout("Pipeline", &body)))
}

async fn pipeline_events(_: CurrentUser, State(state): State<Arc<AppState>>, AxumPath(pipeline_id): AxumPath<String>) -> Sse<impl futures_util::Stream<Item = Result<axum::response::sse::Event, std::convert::Infallible>>> {
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

async fn rerun_pipeline(CurrentUser(user): CurrentUser, State(state): State<Arc<AppState>>, AxumPath(pipeline_id): AxumPath<String>, Form(form): Form<CsrfOnlyForm>) -> Response {
    let result: Result<Redirect, Response> = (|| {
        verify_csrf(&state, &user, &form.csrf_token)?;
        let pipeline = authorized_pipeline(&state, &user, &pipeline_id)?;
        let workflow = state.db.get_workflow_by_version_id(&pipeline.workflow_version_id).map_err(internal_error)?.ok_or_else(|| not_found("workflow"))?;
        let new_pipeline_id = scheduler::enqueue_workflow_run(Arc::clone(&state), &workflow, "rerun", pipeline.trigger_ref.as_deref(), pipeline.commit_sha.as_deref()).map_err(internal_error)?;
        Ok(Redirect::to(&format!("/pipelines/{new_pipeline_id}")))
    })();
    match result { Ok(redirect) => redirect.into_response(), Err(response) => response }
}

async fn cancel_pipeline_route(CurrentUser(user): CurrentUser, State(state): State<Arc<AppState>>, AxumPath(pipeline_id): AxumPath<String>, Form(form): Form<CsrfOnlyForm>) -> Response {
    match verify_csrf(&state, &user, &form.csrf_token).and_then(|_| authorized_pipeline(&state, &user, &pipeline_id)) {
        Ok(pipeline) => match scheduler::cancel_pipeline(Arc::clone(&state), &pipeline.id).await {
            Ok(()) => Redirect::to(&format!("/pipelines/{}", pipeline.id)).into_response(),
            Err(error) => internal_error(error),
        },
        Err(response) => response,
    }
}

#[derive(Serialize)] struct SessionInfo { user: User, csrf_token: String }
#[derive(Deserialize)] struct ApiRepoCreateRequest { owner_id: Option<String>, name: String, default_branch: String, csrf_token: String }
#[derive(Deserialize)] struct ApiRunnerCreateRequest { name: String, base_url: String, token: String, csrf_token: String }
#[derive(Deserialize)] struct ApiWorkflowRequest { csrf_token: String, repo_id: String, name: String, enabled: bool, trigger_kind: String, branches: Vec<String>, jobs: Vec<ApiWorkflowJob> }
#[derive(Debug, Clone, Serialize, Deserialize)] struct ApiWorkflowJob { id: String, name: String, runner_id: String, runner_job_name: String, #[serde(default)] needs: Vec<String>, #[serde(default)] inputs: BTreeMap<String, Value>, #[serde(default)] allow_failure: bool }

async fn api_me(CurrentUser(user): CurrentUser, State(state): State<Arc<AppState>>) -> Json<SessionInfo> { Json(SessionInfo { csrf_token: csrf_token(&state, &user), user }) }
async fn api_list_repos(CurrentUser(user): CurrentUser, State(state): State<Arc<AppState>>) -> Result<Json<Vec<Repo>>, Response> { Ok(Json(visible_repos_for_user(&state, &user)?)) }
async fn api_create_repo(CurrentUser(user): CurrentUser, State(state): State<Arc<AppState>>, Json(request): Json<ApiRepoCreateRequest>) -> Result<Json<Repo>, Response> {
    verify_csrf(&state, &user, &request.csrf_token)?;
    let owner_id = request.owner_id.unwrap_or_else(|| user.id.clone());
    if user.role != "admin" && owner_id != user.id { return Err(forbidden("developers can only create their own repos")); }
    let owner = state.db.get_user(&owner_id).map_err(internal_error)?.ok_or_else(|| bad_request("owner not found"))?;
    let normalized = git::validate_repo_name(&request.name).map_err(bad_request)?;
    validate_branch_name(&request.default_branch)?;
    let bare_path = PathBuf::from(&state.config.repos_dir).join(format!("{}.git", Uuid::now_v7()));
    let repo_id = state.db.create_repo(&owner.id, &request.name, &normalized, &bare_path.display().to_string(), &request.default_branch).map_err(internal_error)?;
    let repo = state.db.get_repo(&repo_id).map_err(internal_error)?.ok_or_else(|| internal_error_text("missing repo after create"))?;
    git::init_bare_repo(Path::new(&repo.bare_path)).map_err(internal_error_text)?;
    git::install_post_receive_hook(Path::new(&repo.bare_path), state.server_bin.as_path(), state.config_path.as_path(), &repo.id).map_err(internal_error_text)?;
    Ok(Json(repo))
}
async fn api_list_runners(_: AdminUser, State(state): State<Arc<AppState>>) -> Result<Json<Vec<models::Runner>>, Response> { Ok(Json(state.db.list_runners().map_err(internal_error)?)) }
async fn api_create_runner(_: AdminUser, CurrentUser(user): CurrentUser, State(state): State<Arc<AppState>>, Json(request): Json<ApiRunnerCreateRequest>) -> Result<Json<models::Runner>, Response> {
    verify_csrf(&state, &user, &request.csrf_token)?;
    validate_runner_name(&request.name)?;
    validate_base_url(&request.base_url)?;
    let runner_id = state.db.create_runner(&request.name, &request.base_url, &request.token).map_err(internal_error)?;
    refresh_single_runner(&state, &runner_id).await.map_err(internal_error_text)?;
    let runner = state.db.get_runner(&runner_id).map_err(internal_error)?.ok_or_else(|| not_found("runner"))?;
    Ok(Json(runner))
}
async fn api_list_workflows(CurrentUser(user): CurrentUser, State(state): State<Arc<AppState>>) -> Result<Json<Vec<Workflow>>, Response> {
    let repos = visible_repos_for_user(&state, &user)?;
    let repo_ids = repos.into_iter().map(|repo| repo.id).collect::<Vec<_>>();
    let workflows = state.db.list_workflows().map_err(internal_error)?.into_iter().filter(|workflow| repo_ids.iter().any(|id| id == &workflow.repo_id)).collect::<Vec<_>>();
    Ok(Json(workflows))
}
async fn api_create_workflow(CurrentUser(user): CurrentUser, State(state): State<Arc<AppState>>, Json(request): Json<ApiWorkflowRequest>) -> Result<Json<Workflow>, Response> {
    verify_csrf(&state, &user, &request.csrf_token)?;
    let repo = authorized_repo(&state, &user, &request.repo_id)?;
    let parsed = parse_api_workflow_request(&state, &request)?;
    state.db.create_workflow(&repo.id, &request.name, request.enabled, &parsed.trigger_json, &parsed.definition_json).map_err(internal_error)?;
    let workflow = state.db.workflows_for_repo(&repo.id).map_err(internal_error)?.into_iter().find(|workflow| workflow.name == request.name).ok_or_else(|| internal_error_text("workflow missing after create"))?;
    Ok(Json(workflow))
}
async fn api_get_workflow(CurrentUser(user): CurrentUser, State(state): State<Arc<AppState>>, AxumPath(workflow_id): AxumPath<String>) -> Result<Json<Workflow>, Response> { Ok(Json(authorized_workflow(&state, &user, &workflow_id)?)) }
async fn api_update_workflow(CurrentUser(user): CurrentUser, State(state): State<Arc<AppState>>, AxumPath(workflow_id): AxumPath<String>, Json(request): Json<ApiWorkflowRequest>) -> Result<Json<Workflow>, Response> {
    verify_csrf(&state, &user, &request.csrf_token)?;
    let workflow = authorized_workflow(&state, &user, &workflow_id)?;
    if workflow.repo_id != request.repo_id { return Err(bad_request("workflow repo cannot be changed")); }
    let parsed = parse_api_workflow_request(&state, &request)?;
    state.db.update_workflow(&workflow.id, &request.name, request.enabled, &parsed.trigger_json, &parsed.definition_json).map_err(internal_error)?;
    Ok(Json(state.db.get_workflow(&workflow.id).map_err(internal_error)?.ok_or_else(|| not_found("workflow"))?))
}
async fn api_list_pipelines(CurrentUser(user): CurrentUser, State(state): State<Arc<AppState>>) -> Result<Json<Vec<PipelineRun>>, Response> {
    let repos = visible_repos_for_user(&state, &user)?;
    let repo_ids = repos.into_iter().map(|repo| repo.id).collect::<Vec<_>>();
    let pipelines = state.db.list_pipeline_runs().map_err(internal_error)?.into_iter().filter(|pipeline| repo_ids.iter().any(|id| id == &pipeline.repo_id)).collect::<Vec<_>>();
    Ok(Json(pipelines))
}
async fn api_get_pipeline(CurrentUser(user): CurrentUser, State(state): State<Arc<AppState>>, AxumPath(pipeline_id): AxumPath<String>) -> Result<Json<models::PipelineSnapshot>, Response> {
    let pipeline = authorized_pipeline(&state, &user, &pipeline_id)?;
    Ok(Json(state.db.pipeline_snapshot(&pipeline.id).map_err(internal_error)?.ok_or_else(|| not_found("pipeline"))?))
}

async fn refresh_single_runner(state: &Arc<AppState>, runner_id: &str) -> Result<(), String> {
    let runner = state.db.get_runner(runner_id).map_err(|error| error.to_string())?.ok_or_else(|| "runner not found".to_string())?;
    let jobs = state.runner_client.list_jobs(&runner).await.map_err(|error| error.to_string())?;
    let jobs = jobs.into_iter().map(|job| (job.name, serde_json::to_string(&job.definition).unwrap_or_else(|_| "{}".to_string()))).collect::<Vec<_>>();
    state.db.replace_runner_jobs(runner_id, &jobs).map_err(|error| error.to_string())?;
    state.db.update_runner_health(runner_id, "healthy").map_err(|error| error.to_string())?;
    Ok(())
}

struct ParsedWorkflow { trigger_json: String, definition_json: String }

fn parse_workflow_form(state: &Arc<AppState>, form: &WorkflowForm) -> Result<ParsedWorkflow, Response> {
    if form.name.trim().is_empty() { return Err(bad_request("workflow name cannot be empty")); }
    let trigger_kind = form.trigger_kind.trim();
    if !matches!(trigger_kind, "push" | "manual") { return Err(bad_request("trigger kind must be push or manual")); }
    let trigger = WorkflowTrigger { kind: trigger_kind.to_string(), branches: parse_csv(&form.branches_csv) };
    let jobs = parse_job_specs(&form.jobs_spec)?;
    let definition = WorkflowDefinition { jobs };
    definition.validate().map_err(bad_request)?;
    validate_workflow_runners(state, &definition)?;
    Ok(ParsedWorkflow { trigger_json: serde_json::to_string(&trigger).map_err(internal_error_text)?, definition_json: serde_json::to_string(&definition).map_err(internal_error_text)? })
}

fn parse_api_workflow_request(state: &Arc<AppState>, request: &ApiWorkflowRequest) -> Result<ParsedWorkflow, Response> {
    if request.name.trim().is_empty() { return Err(bad_request("workflow name cannot be empty")); }
    if !matches!(request.trigger_kind.as_str(), "push" | "manual") { return Err(bad_request("trigger kind must be push or manual")); }
    let trigger = WorkflowTrigger { kind: request.trigger_kind.clone(), branches: request.branches.clone() };
    let jobs = request.jobs.iter().map(|job| WorkflowJobDefinition { id: job.id.clone(), name: job.name.clone(), runner_id: job.runner_id.clone(), runner_job_name: job.runner_job_name.clone(), needs: job.needs.clone(), inputs: job.inputs.clone(), artifacts_from: Vec::new(), allow_failure: job.allow_failure }).collect::<Vec<_>>();
    let definition = WorkflowDefinition { jobs };
    definition.validate().map_err(bad_request)?;
    validate_workflow_runners(state, &definition)?;
    Ok(ParsedWorkflow { trigger_json: serde_json::to_string(&trigger).map_err(internal_error_text)?, definition_json: serde_json::to_string(&definition).map_err(internal_error_text)? })
}

fn parse_job_specs(input: &str) -> Result<Vec<WorkflowJobDefinition>, Response> {
    let mut jobs = Vec::new();
    for (line_no, line) in input.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() { continue; }
        let parts = trimmed.split('|').map(str::trim).collect::<Vec<_>>();
        if parts.len() != 7 { return Err(bad_request(format!("job spec line {} must contain 7 pipe-separated fields", line_no + 1))); }
        let needs = if parts[4].is_empty() { Vec::new() } else { parse_csv(parts[4]) };
        let allow_failure = parse_bool(parts[5]).map_err(bad_request)?;
        let inputs = parse_input_bindings(parts[6])?;
        jobs.push(WorkflowJobDefinition { id: parts[0].to_string(), name: parts[1].to_string(), runner_id: parts[2].to_string(), runner_job_name: parts[3].to_string(), needs, inputs, artifacts_from: Vec::new(), allow_failure });
    }
    if jobs.is_empty() { return Err(bad_request("workflow must contain at least one job spec")); }
    Ok(jobs)
}

fn parse_input_bindings(input: &str) -> Result<BTreeMap<String, Value>, Response> {
    let mut bindings = BTreeMap::new();
    for pair in input.split(',') {
        let trimmed = pair.trim();
        if trimmed.is_empty() { continue; }
        let (key, raw_value) = trimmed.split_once('=').ok_or_else(|| bad_request("input bindings must use key=value pairs"))?;
        let value = if let Some(stripped) = raw_value.strip_prefix("json:") {
            serde_json::from_str::<Value>(stripped).map_err(bad_request)?
        } else if raw_value == "true" || raw_value == "false" {
            json!(raw_value == "true")
        } else if let Ok(integer) = raw_value.parse::<i64>() {
            json!(integer)
        } else {
            json!(raw_value)
        };
        bindings.insert(key.trim().to_string(), value);
    }
    Ok(bindings)
}

fn validate_workflow_runners(state: &Arc<AppState>, definition: &WorkflowDefinition) -> Result<(), Response> {
    for job in &definition.jobs {
        let runner = state.db.get_runner(&job.runner_id).map_err(internal_error)?.ok_or_else(|| bad_request(format!("unknown runner {}", job.runner_id)))?;
        let jobs = state.db.list_runner_jobs(&runner.id).map_err(internal_error)?;
        if !jobs.iter().any(|(name, _)| name == &job.runner_job_name) { return Err(bad_request(format!("runner {} does not advertise job {}", runner.name, job.runner_job_name))); }
    }
    Ok(())
}

fn workflow_form_fields(workflow: Option<&Workflow>) -> String {
    let name = workflow.map(|item| html_escape(&item.name)).unwrap_or_default();
    let enabled = workflow.filter(|item| item.enabled).map(|_| " checked").unwrap_or(" checked");
    let (trigger_kind, branches_csv, jobs_spec, repo_id_input) = if let Some(item) = workflow {
        let trigger: WorkflowTrigger = serde_json::from_str(&item.trigger_json).unwrap_or(WorkflowTrigger { kind: "push".to_string(), branches: vec!["main".to_string()] });
        let definition: WorkflowDefinition = serde_json::from_str(&item.definition_json).unwrap_or(WorkflowDefinition { jobs: Vec::new() });
        (trigger.kind, trigger.branches.join(","), render_job_specs(&definition), format!("<input type=\"hidden\" name=\"repo_id\" value=\"{}\" />", item.repo_id))
    } else {
        ("push".to_string(), "main".to_string(), "build|Build|runner-id|build-app||false|commit=$commit,branch=$branch,source=$source".to_string(), String::new())
    };
    format!(r#"{}<label>Name <input name="name" value="{name}" /></label><label>Enabled <input type="checkbox" name="enabled" value="true"{enabled} /></label><label>Trigger <select name="trigger_kind"><option value="push" {push_selected}>push</option><option value="manual" {manual_selected}>manual</option></select></label><label>Branches CSV <input name="branches_csv" value="{branches_csv}" /></label><label>Job Specs <textarea name="jobs_spec" rows="10" cols="120">{jobs_spec}</textarea></label><p>Format: job_id|Display Name|runner_id|runner_job_name|needs_csv|allow_failure|input1=value,input2=value</p><p>Special values: $commit, $branch, $source, $job.&lt;job_id&gt;.&lt;artifact_name&gt;</p><button type="submit">{submit_label}</button>"#, repo_id_input, name = name, enabled = enabled, push_selected = if trigger_kind == "push" { "selected" } else { "" }, manual_selected = if trigger_kind == "manual" { "selected" } else { "" }, branches_csv = html_escape(&branches_csv), jobs_spec = html_escape(&jobs_spec), submit_label = if workflow.is_some() { "Save workflow" } else { "Create workflow" })
}

fn render_job_specs(definition: &WorkflowDefinition) -> String {
    definition.jobs.iter().map(|job| {
        let inputs = job.inputs.iter().map(|(key, value)| format!("{key}={}", render_input_value(value))).collect::<Vec<_>>().join(",");
        format!("{}|{}|{}|{}|{}|{}|{}", job.id, job.name, job.runner_id, job.runner_job_name, job.needs.join(","), job.allow_failure, inputs)
    }).collect::<Vec<_>>().join("\n")
}

fn render_input_value(value: &Value) -> String {
    match value { Value::String(text) => text.clone(), Value::Bool(boolean) => boolean.to_string(), Value::Number(number) => number.to_string(), other => format!("json:{}", other) }
}

fn visible_repos_for_user(state: &Arc<AppState>, user: &User) -> Result<Vec<Repo>, Response> { Ok(state.db.list_repos().map_err(internal_error)?.into_iter().filter(|repo| can_view_repo(user, repo)).collect()) }
fn can_view_repo(user: &User, repo: &Repo) -> bool { user.role == "admin" || repo.owner_id == user.id }
fn repo_ids_contains(repos: &[Repo], repo_id: &str) -> bool { repos.iter().any(|repo| repo.id == repo_id) }
fn authorized_repo(state: &Arc<AppState>, user: &User, repo_id: &str) -> Result<Repo, Response> {
    let repo = state.db.get_repo(repo_id).map_err(internal_error)?.ok_or_else(|| not_found("repo"))?;
    if !can_view_repo(user, &repo) { return Err(forbidden("repo access denied")); }
    Ok(repo)
}
fn authorized_workflow(state: &Arc<AppState>, user: &User, workflow_id: &str) -> Result<Workflow, Response> {
    let workflow = state.db.get_workflow(workflow_id).map_err(internal_error)?.ok_or_else(|| not_found("workflow"))?;
    let repo = authorized_repo(state, user, &workflow.repo_id)?;
    if repo.id != workflow.repo_id { return Err(forbidden("workflow access denied")); }
    Ok(workflow)
}
fn authorized_pipeline(state: &Arc<AppState>, user: &User, pipeline_id: &str) -> Result<PipelineRun, Response> {
    let pipeline = state.db.get_pipeline_run(pipeline_id).map_err(internal_error)?.ok_or_else(|| not_found("pipeline"))?;
    let repo = authorized_repo(state, user, &pipeline.repo_id)?;
    if repo.id != pipeline.repo_id { return Err(forbidden("pipeline access denied")); }
    Ok(pipeline)
}
fn repo_clone_url(state: &Arc<AppState>, repo: &Repo) -> String { format!("ssh://git@{}/{}/{}", state.config.server.public_base_url.trim_end_matches('/'), repo.owner_username, repo.name) }
fn validate_username(username: &str) -> Result<(), Response> {
    let trimmed = username.trim();
    if trimmed.len() < 3 { return Err(bad_request("username must be at least 3 characters")); }
    if !trimmed.chars().all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.')) { return Err(bad_request("username contains invalid characters")); }
    Ok(())
}
fn validate_password(password: &str) -> Result<(), Response> { if password.len() < 8 { Err(bad_request("password must be at least 8 characters")) } else { Ok(()) } }
fn validate_role(role: &str) -> Result<(), Response> { if matches!(role, "admin" | "developer") { Ok(()) } else { Err(bad_request("role must be admin or developer")) } }
fn validate_branch_name(branch: &str) -> Result<(), Response> { if branch.trim().is_empty() || branch.contains(' ') { Err(bad_request("default branch is invalid")) } else { Ok(()) } }
fn validate_runner_name(name: &str) -> Result<(), Response> { if name.trim().is_empty() { Err(bad_request("runner name cannot be empty")) } else { Ok(()) } }
fn validate_base_url(url: &str) -> Result<(), Response> { url::Url::parse(url).map(|_| ()).map_err(|_| bad_request("base_url must be a valid URL")) }
pub(super) fn csrf_token(state: &Arc<AppState>, user: &User) -> String {
    let mut mac = HmacSha256::new_from_slice(state.config.auth.session_secret.as_bytes()).expect("valid hmac key");
    mac.update(b"csrf:");
    mac.update(user.id.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}
fn verify_csrf(state: &Arc<AppState>, user: &User, token: &str) -> Result<(), Response> { if token == csrf_token(state, user) { Ok(()) } else { Err(forbidden("csrf validation failed")) } }
fn csrf_input(token: &str) -> String { format!("<input type=\"hidden\" name=\"csrf_token\" value=\"{}\" />", html_escape(token)) }
fn parse_csv(input: &str) -> Vec<String> { input.split(',').map(str::trim).filter(|item| !item.is_empty()).map(ToString::to_string).collect() }
fn parse_bool(input: &str) -> Result<bool, String> { match input { "true" => Ok(true), "false" => Ok(false), other => Err(format!("invalid boolean value: {other}")) } }
fn layout(title: &str, body: &str) -> String { format!("<!doctype html><html><head><meta charset=\"utf-8\"><title>{}</title></head><body><nav><a href=\"/repos\">Repos</a> | <a href=\"/runners\">Runners</a> | <a href=\"/workflows\">Workflows</a> | <a href=\"/pipelines\">Pipelines</a> | <a href=\"/users\">Users</a> <form method=\"post\" action=\"/logout\" style=\"display:inline\"><button type=\"submit\">Logout</button></form></nav><main>{}</main></body></html>", title, body) }
fn layout_public(title: &str, body: &str) -> String { format!("<!doctype html><html><head><meta charset=\"utf-8\"><title>{}</title></head><body><main>{}</main></body></html>", title, body) }
fn html_escape(input: &str) -> String { input.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;").replace('"', "&quot;") }
fn internal_error(error: impl std::fmt::Display) -> Response { (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response() }
fn internal_error_text(error: impl std::fmt::Display) -> Response { internal_error(error) }
fn bad_request(error: impl std::fmt::Display) -> Response { (StatusCode::BAD_REQUEST, error.to_string()).into_response() }
fn forbidden(error: impl std::fmt::Display) -> Response { (StatusCode::FORBIDDEN, error.to_string()).into_response() }
fn not_found(entity: &str) -> Response { (StatusCode::NOT_FOUND, format!("{entity} not found")).into_response() }
