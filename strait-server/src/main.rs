mod auth;
mod config;
mod db;
mod git;
mod models;
mod runner;
mod scheduler;

use std::{
    collections::BTreeMap,
    env, fs, io,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
};

use auth::{
    AdminUser, CurrentUser, clear_session_cookie, hash_password, parse_session_cookie,
    session_cookie, verify_password,
};
use axum::{
    Form, Router,
    extract::{Path as AxumPath, State},
    http::{HeaderMap, StatusCode, header::SET_COOKIE},
    response::{Html, IntoResponse, Redirect, Response, Sse},
    routing::{get, post},
};
use chrono::{Duration, Utc};
use config::Config;
use hmac::{Hmac, Mac};
use models::{PipelineRun, Repo, User, Workflow, WorkflowDefinition, WorkflowJobDefinition, WorkflowTrigger};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::Sha256;
use tokio::time;
use tracing::info;
use uuid::Uuid;

use crate::{db::Database, runner::RunnerClient};

type HmacSha256 = Hmac<Sha256>;

#[derive(Clone)]
pub struct AppState {
    config: Arc<Config>,
    db: Database,
    runner_client: RunnerClient,
    config_path: Arc<PathBuf>,
    server_bin: Arc<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_tracing();

    let cli = Cli::from_env()?;
    let state = build_state(cli.config_path.clone(), env::current_exe()?)?;

    match cli.command {
        Command::Serve => serve(state).await,
        Command::HookPostReceive { repo_id } => hook_post_receive(state, &repo_id),
        Command::AdminReconcileHooks => reconcile_hooks(state),
        Command::AdminSeedUser {
            username,
            password,
            role,
        } => seed_user(state, &username, &password, &role),
    }
}

fn build_state(
    config_path: PathBuf,
    server_bin: PathBuf,
) -> Result<Arc<AppState>, Box<dyn std::error::Error>> {
    let config = Arc::new(Config::load_from_path(&config_path)?);
    fs::create_dir_all(&config.data_dir)?;
    fs::create_dir_all(&config.repos_dir)?;
    if let Some(parent) = Path::new(&config.database.path).parent() {
        fs::create_dir_all(parent)?;
    }

    let db = Database::open(&config.database.path)?;
    let admin_hash = hash_password(&config.auth.bootstrap_admin.password)?;
    db.ensure_user(&config.auth.bootstrap_admin.username, &admin_hash, "admin")?;

    Ok(Arc::new(AppState {
        config,
        db,
        runner_client: RunnerClient::new(),
        config_path: Arc::new(config_path),
        server_bin: Arc::new(server_bin),
    }))
}

async fn serve(state: Arc<AppState>) -> Result<(), Box<dyn std::error::Error>> {
    let address: SocketAddr = state.config.server.listen.parse()?;
    scheduler::spawn(Arc::clone(&state));
    let app = build_router(state);
    let listener = tokio::net::TcpListener::bind(address).await?;
    info!(listen = %address, "strait-server listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

fn hook_post_receive(state: Arc<AppState>, repo_id: &str) -> Result<(), Box<dyn std::error::Error>> {
    let refs = git::read_push_refs(&mut io::stdin())?;
    let event_key = git::event_key(repo_id, &refs);
    state.db.create_push_event(repo_id, &event_key, &refs)?;
    Ok(())
}

fn reconcile_hooks(state: Arc<AppState>) -> Result<(), Box<dyn std::error::Error>> {
    for repo in state.db.list_repos()? {
        git::install_post_receive_hook(
            Path::new(&repo.bare_path),
            state.server_bin.as_path(),
            state.config_path.as_path(),
            &repo.id,
        )?;
    }
    Ok(())
}

fn seed_user(
    state: Arc<AppState>,
    username: &str,
    password: &str,
    role: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let password_hash = hash_password(password)?;
    state.db.create_user(username, &password_hash, role)?;
    Ok(())
}

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
        r#"<form method="post" action="/login">
<label>Username <input name="username" autocomplete="username" /></label><br/>
<label>Password <input name="password" type="password" autocomplete="current-password" /></label><br/>
<button type="submit">Login</button>
</form>"#,
    ))
}

#[derive(Deserialize)]
struct LoginForm {
    username: String,
    password: String,
}

async fn login(State(state): State<Arc<AppState>>, Form(form): Form<LoginForm>) -> Response {
    let Ok(Some((user, hash))) = state.db.get_user_credentials(&form.username) else {
        return auth::unauthorized();
    };
    if !verify_password(&form.password, &hash) {
        return auth::unauthorized();
    }

    let expires_at = (Utc::now() + Duration::days(7)).to_rfc3339();
    let Ok(session_id) = state.db.create_session(&user.id, &expires_at) else {
        return (StatusCode::INTERNAL_SERVER_ERROR, "failed to create session").into_response();
    };
    let mut response = Redirect::to("/repos").into_response();
    response.headers_mut().append(
        SET_COOKIE,
        session_cookie(&state.config.auth.session_secret, &session_id)
            .to_string()
            .parse()
            .expect("cookie header"),
    );
    response
}

async fn logout(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Response {
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
    enabled: Option<String>,
    trigger_kind: String,
    branches_csv: String,
    jobs_spec: String,
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
    let mut body = format!(
        r#"<form method="post" action="/users">
{}
<label>Username <input name="username" /></label>
<label>Password <input name="password" type="password" /></label>
<label>Role <select name="role"><option value="developer">developer</option><option value="admin">admin</option></select></label>
<button type="submit">Create user</button>
</form>
<ul>"#,
        csrf_input(&csrf)
    );
    for item in users {
        body.push_str(&format!("<li>{} ({})</li>", html_escape(&item.username), item.role));
    }
    body.push_str("</ul>");
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
    let mut body = format!(
        r#"<form method="post" action="/repos">
{}
<label>Name <input name="name" /></label>"#,
        csrf_input(&csrf)
    );

    if user.role == "admin" {
        body.push_str(r#"<label>Owner <select name="owner_id">"#);
        for candidate in users {
            let selected = if candidate.id == user.id { " selected" } else { "" };
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
            "<input type=\"hidden\" name=\"owner_id\" value=\"{}\" /><p>Owner: {}</p>",
            user.id,
            html_escape(&user.username)
        ));
    }

    body.push_str(
        r#"<label>Default branch <input name="default_branch" value="main" /></label>
<button type="submit">Create repo</button>
</form>
<ul>"#,
    );
    for repo in repos.into_iter().filter(|repo| can_view_repo(&user, repo)) {
        let clone_url = repo_clone_url(&state, &repo);
        body.push_str(&format!(
            "<li><strong>{}/{}</strong> clone: <code>{}</code>
            <form method=\"post\" action=\"/repos/{}/trigger\">{}<input name=\"branch\" value=\"refs/heads/{}\" /><input name=\"commit\" value=\"HEAD\" /><button type=\"submit\">Manual trigger</button></form>
            </li>",
            html_escape(&repo.owner_username),
            html_escape(&repo.name),
            html_escape(&clone_url),
            repo.id,
            csrf_input(&csrf),
            html_escape(&repo.default_branch)
        ));
    }
    body.push_str("</ul>");
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
    let refs = vec![crate::models::PushEventRef {
        old_rev: "0000000000000000000000000000000000000000".to_string(),
        new_rev: commit,
        ref_name: branch,
    }];
    let key = git::event_key(&repo_id, &refs);
    state.db.create_push_event(&repo_id, &key, &refs).map_err(internal_error)?;
    Ok(Redirect::to("/pipelines"))
}

async fn runners_page(
    _: AdminUser,
    CurrentUser(user): CurrentUser,
    State(state): State<Arc<AppState>>,
) -> Result<Html<String>, Response> {
    let runners = state.db.list_runners().map_err(internal_error)?;
    let csrf = csrf_token(&state, &user);
    let mut body = format!(
        r#"<form method="post" action="/runners">
{}
<label>Name <input name="name" /></label>
<label>Base URL <input name="base_url" placeholder="http://127.0.0.1:8080" /></label>
<label>Token <input name="token" /></label>
<button type="submit">Add runner</button>
</form>
<ul>"#,
        csrf_input(&csrf)
    );
    for runner in runners {
        body.push_str(&format!(
            "<li><strong>{}</strong> {} [{}]
            <form method=\"post\" action=\"/runners/{}/test\">{}<button type=\"submit\">Test</button></form>
            <form method=\"post\" action=\"/runners/{}/toggle\">{}<button type=\"submit\">{}</button></form>",
            html_escape(&runner.name),
            html_escape(&runner.base_url),
            html_escape(&runner.last_health_state),
            runner.id,
            csrf_input(&csrf),
            runner.id,
            csrf_input(&csrf),
            if runner.enabled { "Disable" } else { "Enable" }
        ));
        let jobs = state.db.list_runner_jobs(&runner.id).map_err(internal_error)?;
        if !jobs.is_empty() {
            body.push_str("<ul>");
            for (job_name, _) in jobs {
                body.push_str(&format!("<li>{}</li>", html_escape(&job_name)));
            }
            body.push_str("</ul>");
        }
        body.push_str("</li>");
    }
    body.push_str("</ul>");
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
    refresh_single_runner(&state, &runner_id).await.map_err(internal_error_text)?;
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
    refresh_single_runner(&state, &runner_id).await.map_err(internal_error_text)?;
    Ok(Redirect::to("/runners"))
}

async fn workflows_page(
    CurrentUser(user): CurrentUser,
    State(state): State<Arc<AppState>>,
) -> Result<Html<String>, Response> {
    let repos = visible_repos_for_user(&state, &user)?;
    let workflows = state.db.list_workflows().map_err(internal_error)?;
    let csrf = csrf_token(&state, &user);
    let mut body = format!(
        r#"<form method="post" action="/workflows">
{}
<label>Repo <select name="repo_id">"#,
        csrf_input(&csrf)
    );
    for repo in &repos {
        body.push_str(&format!(
            "<option value=\"{}\">{}/{}</option>",
            repo.id,
            html_escape(&repo.owner_username),
            html_escape(&repo.name)
        ));
    }
    body.push_str(&workflow_form_fields(None));
    body.push_str("</form><ul>");
    for workflow in workflows.into_iter().filter(|item| repo_ids_contains(&repos, &item.repo_id)) {
        body.push_str(&format!(
            "<li><a href=\"/workflows/{}\">{}</a> repo={} version={} enabled={}</li>",
            workflow.id,
            html_escape(&workflow.name),
            html_escape(&workflow.repo_id),
            workflow.version,
            workflow.enabled
        ));
    }
    body.push_str("</ul>");
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
    let body = format!(
        "<p>Repo: {}/{}</p><form method=\"post\" action=\"/workflows/{}/update\">{}{}</form>",
        html_escape(&repo.owner_username),
        html_escape(&repo.name),
        workflow.id,
        csrf_input(&csrf),
        workflow_form_fields(Some(&workflow))
    );
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
            form.enabled.is_some(),
            &parsed.trigger_json,
            &parsed.definition_json,
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
            form.enabled.is_some(),
            &parsed.trigger_json,
            &parsed.definition_json,
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
    let mut body = String::from("<ul>");
    for pipeline in pipelines {
        if let Some(repo) = repo_by_id.get(&pipeline.repo_id) {
            if !can_view_repo(&user, repo) {
                continue;
            }
            body.push_str(&format!(
                "<li><a href=\"/pipelines/{}\">{}</a> {} {} {}/{}</li>",
                pipeline.id,
                pipeline.id,
                html_escape(&pipeline.status),
                html_escape(&pipeline.trigger_ref.clone().unwrap_or_default()),
                html_escape(&repo.owner_username),
                html_escape(&repo.name)
            ));
        }
    }
    body.push_str("</ul>");
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
    let mut body = format!(
        "<p>Status: {}</p><p>Trigger: {:?}</p>
        <form method=\"post\" action=\"/pipelines/{}/rerun\">{}<button type=\"submit\">Rerun</button></form>
        <form method=\"post\" action=\"/pipelines/{}/cancel\">{}<button type=\"submit\">Cancel</button></form><ul>",
        html_escape(&snapshot.pipeline.status),
        snapshot.pipeline.trigger_ref,
        snapshot.pipeline.id,
        csrf_input(&csrf),
        snapshot.pipeline.id,
        csrf_input(&csrf)
    );
    for job in snapshot.jobs {
        body.push_str(&format!(
            "<li><strong>{}</strong> [{}] runner={}<pre>{}</pre><pre>{}</pre></li>",
            html_escape(&job.run.job_name),
            html_escape(&job.run.status),
            html_escape(&job.run.runner_job_name),
            html_escape(&job.stdout),
            html_escape(&job.stderr)
        ));
    }
    body.push_str("</ul><script>const e=new EventSource('/pipelines/");
    body.push_str(&pipeline.id);
    body.push_str("/events');e.onmessage=(msg)=>console.log(msg.data);</script>");
    Ok(Html(layout("Pipeline", &body)))
}

async fn pipeline_events(
    _: CurrentUser,
    State(state): State<Arc<AppState>>,
    AxumPath(pipeline_id): AxumPath<String>,
) -> Sse<impl futures_util::Stream<Item = Result<axum::response::sse::Event, std::convert::Infallible>>> {
    let stream = async_stream::stream! {
        loop {
            let payload = match state.db.pipeline_snapshot(&pipeline_id) {
                Ok(Some(snapshot)) => serde_json::to_string(&snapshot).unwrap_or_else(|_| "{}".to_string()),
                Ok(None) => "{}".to_string(),
                Err(error) => json!({ "error": error.to_string() }).to_string(),
            };
            if payload != "{}" {
                yield Ok(axum::response::sse::Event::default().data(payload));
            }
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
            (
                job.name,
                serde_json::to_string(&job.definition).unwrap_or_else(|_| "{}".to_string()),
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
}

fn parse_workflow_form(state: &Arc<AppState>, form: &WorkflowForm) -> Result<ParsedWorkflow, Response> {
    if form.name.trim().is_empty() {
        return Err(bad_request("workflow name cannot be empty"));
    }
    let trigger_kind = form.trigger_kind.trim();
    if !matches!(trigger_kind, "push" | "manual") {
        return Err(bad_request("trigger kind must be push or manual"));
    }
    let branches = parse_csv(&form.branches_csv);
    let trigger = WorkflowTrigger {
        kind: trigger_kind.to_string(),
        branches,
    };
    let jobs = parse_job_specs(&form.jobs_spec)?;
    let definition = WorkflowDefinition { jobs };
    definition.validate().map_err(bad_request)?;
    validate_workflow_runners(state, &definition)?;
    Ok(ParsedWorkflow {
        trigger_json: serde_json::to_string(&trigger).map_err(internal_error_text)?,
        definition_json: serde_json::to_string(&definition).map_err(internal_error_text)?,
    })
}

fn parse_job_specs(input: &str) -> Result<Vec<WorkflowJobDefinition>, Response> {
    let mut jobs = Vec::new();
    for (line_no, line) in input.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let parts = trimmed.split('|').map(str::trim).collect::<Vec<_>>();
        if parts.len() != 7 {
            return Err(bad_request(format!(
                "job spec line {} must contain 7 pipe-separated fields",
                line_no + 1
            )));
        }
        let needs = if parts[4].is_empty() {
            Vec::new()
        } else {
            parse_csv(parts[4])
        };
        let allow_failure = parse_bool(parts[5]).map_err(bad_request)?;
        let inputs = parse_input_bindings(parts[6])?;
        jobs.push(WorkflowJobDefinition {
            id: parts[0].to_string(),
            name: parts[1].to_string(),
            runner_id: parts[2].to_string(),
            runner_job_name: parts[3].to_string(),
            needs,
            inputs,
            artifacts_from: Vec::new(),
            allow_failure,
        });
    }
    if jobs.is_empty() {
        return Err(bad_request("workflow must contain at least one job spec"));
    }
    Ok(jobs)
}

fn parse_input_bindings(input: &str) -> Result<BTreeMap<String, Value>, Response> {
    let mut bindings = BTreeMap::new();
    for pair in input.split(',') {
        let trimmed = pair.trim();
        if trimmed.is_empty() {
            continue;
        }
        let (key, raw_value) = trimmed
            .split_once('=')
            .ok_or_else(|| bad_request("input bindings must use key=value pairs"))?;
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

fn validate_workflow_runners(
    state: &Arc<AppState>,
    definition: &WorkflowDefinition,
) -> Result<(), Response> {
    for job in &definition.jobs {
        let runner = state
            .db
            .get_runner(&job.runner_id)
            .map_err(internal_error)?
            .ok_or_else(|| bad_request(format!("unknown runner {}", job.runner_id)))?;
        let jobs = state.db.list_runner_jobs(&runner.id).map_err(internal_error)?;
        if !jobs.iter().any(|(name, _)| name == &job.runner_job_name) {
            return Err(bad_request(format!(
                "runner {} does not advertise job {}",
                runner.name, job.runner_job_name
            )));
        }
    }
    Ok(())
}

fn workflow_form_fields(workflow: Option<&Workflow>) -> String {
    let name = workflow.map(|item| html_escape(&item.name)).unwrap_or_default();
    let enabled = workflow.filter(|item| item.enabled).map(|_| " checked").unwrap_or(" checked");
    let (trigger_kind, branches_csv, jobs_spec, repo_id_input) = if let Some(item) = workflow {
        let trigger: WorkflowTrigger = serde_json::from_str(&item.trigger_json).unwrap_or(WorkflowTrigger {
            kind: "push".to_string(),
            branches: vec!["main".to_string()],
        });
        let definition: WorkflowDefinition =
            serde_json::from_str(&item.definition_json).unwrap_or(WorkflowDefinition { jobs: Vec::new() });
        (
            trigger.kind,
            trigger.branches.join(","),
            render_job_specs(&definition),
            format!("<input type=\"hidden\" name=\"repo_id\" value=\"{}\" />", item.repo_id),
        )
    } else {
        (
            "push".to_string(),
            "main".to_string(),
            "build|Build|runner-id|build-app||false|commit=$commit,branch=$branch,source=$source".to_string(),
            String::new(),
        )
    };

    format!(
        r#"{}
<label>Name <input name="name" value="{name}" /></label>
<label>Enabled <input type="checkbox" name="enabled" value="true"{enabled} /></label>
<label>Trigger <select name="trigger_kind">
<option value="push" {push_selected}>push</option>
<option value="manual" {manual_selected}>manual</option>
</select></label>
<label>Branches CSV <input name="branches_csv" value="{branches_csv}" /></label>
<label>Job Specs <textarea name="jobs_spec" rows="10" cols="120">{jobs_spec}</textarea></label>
<p>Format: job_id|Display Name|runner_id|runner_job_name|needs_csv|allow_failure|input1=value,input2=value</p>
<p>Special values: $commit, $branch, $source, $job.&lt;job_id&gt;.&lt;artifact_name&gt;</p>
<button type="submit">{submit_label}</button>"#,
        repo_id_input,
        name = name,
        enabled = enabled,
        push_selected = if trigger_kind == "push" { "selected" } else { "" },
        manual_selected = if trigger_kind == "manual" { "selected" } else { "" },
        branches_csv = html_escape(&branches_csv),
        jobs_spec = html_escape(&jobs_spec),
        submit_label = if workflow.is_some() { "Save workflow" } else { "Create workflow" },
    )
}

fn render_job_specs(definition: &WorkflowDefinition) -> String {
    definition
        .jobs
        .iter()
        .map(|job| {
            let inputs = job
                .inputs
                .iter()
                .map(|(key, value)| format!("{key}={}", render_input_value(value)))
                .collect::<Vec<_>>()
                .join(",");
            format!(
                "{}|{}|{}|{}|{}|{}|{}",
                job.id,
                job.name,
                job.runner_id,
                job.runner_job_name,
                job.needs.join(","),
                job.allow_failure,
                inputs
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn render_input_value(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        Value::Bool(boolean) => boolean.to_string(),
        Value::Number(number) => number.to_string(),
        other => format!("json:{}", other),
    }
}

fn visible_repos_for_user(state: &Arc<AppState>, user: &User) -> Result<Vec<Repo>, Response> {
    let repos = state.db.list_repos().map_err(internal_error)?;
    Ok(repos.into_iter().filter(|repo| can_view_repo(user, repo)).collect())
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
        return Err(bad_request("password must be at least 8 characters"));
    }
    Ok(())
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
        return Err(bad_request("default branch is invalid"));
    }
    Ok(())
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

fn csrf_token(state: &Arc<AppState>, user: &User) -> String {
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

fn parse_csv(input: &str) -> Vec<String> {
    input.split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(ToString::to_string)
        .collect()
}

fn parse_bool(input: &str) -> Result<bool, String> {
    match input {
        "true" => Ok(true),
        "false" => Ok(false),
        other => Err(format!("invalid boolean value: {other}")),
    }
}

fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "strait_server=info,axum=info".into()),
        )
        .json()
        .flatten_event(true)
        .init();
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut terminate =
            signal(SignalKind::terminate()).expect("failed to install SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {},
            _ = terminate.recv() => {},
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

fn layout(title: &str, body: &str) -> String {
    format!(
        "<!doctype html><html><head><meta charset=\"utf-8\"><title>{}</title></head><body><nav><a href=\"/repos\">Repos</a> | <a href=\"/runners\">Runners</a> | <a href=\"/workflows\">Workflows</a> | <a href=\"/pipelines\">Pipelines</a> | <a href=\"/users\">Users</a> <form method=\"post\" action=\"/logout\" style=\"display:inline\"><button type=\"submit\">Logout</button></form></nav><main>{}</main></body></html>",
        title, body
    )
}

fn layout_public(title: &str, body: &str) -> String {
    format!(
        "<!doctype html><html><head><meta charset=\"utf-8\"><title>{}</title></head><body><main>{}</main></body></html>",
        title, body
    )
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

struct Cli {
    config_path: PathBuf,
    command: Command,
}

enum Command {
    Serve,
    HookPostReceive { repo_id: String },
    AdminReconcileHooks,
    AdminSeedUser {
        username: String,
        password: String,
        role: String,
    },
}

impl Cli {
    fn from_env() -> Result<Self, Box<dyn std::error::Error>> {
        let args = env::args().skip(1).collect::<Vec<_>>();
        let mut config_path = PathBuf::from("server.toml");
        if args.is_empty() {
            return Ok(Self {
                config_path,
                command: Command::Serve,
            });
        }
        let mut index = 0;
        let command = match args.get(index).map(String::as_str) {
            Some("serve") => Command::Serve,
            Some("hook") if args.get(index + 1).map(String::as_str) == Some("post-receive") => {
                index += 2;
                let mut repo_id = None;
                while index < args.len() {
                    match args[index].as_str() {
                        "--repo-id" => {
                            index += 1;
                            repo_id = args.get(index).cloned();
                        }
                        "--config" => {
                            index += 1;
                            config_path = PathBuf::from(args.get(index).ok_or("missing config path")?);
                        }
                        _ => {}
                    }
                    index += 1;
                }
                Command::HookPostReceive {
                    repo_id: repo_id.ok_or("missing --repo-id")?,
                }
            }
            Some("admin") if args.get(index + 1).map(String::as_str) == Some("reconcile-hooks") => {
                index += 2;
                while index < args.len() {
                    if args[index] == "--config" {
                        index += 1;
                        config_path = PathBuf::from(args.get(index).ok_or("missing config path")?);
                    }
                    index += 1;
                }
                Command::AdminReconcileHooks
            }
            Some("admin") if args.get(index + 1).map(String::as_str) == Some("seed-user") => {
                index += 2;
                let mut username = None;
                let mut password = None;
                let mut role = Some("developer".to_string());
                while index < args.len() {
                    match args[index].as_str() {
                        "--username" => {
                            index += 1;
                            username = args.get(index).cloned();
                        }
                        "--password" => {
                            index += 1;
                            password = args.get(index).cloned();
                        }
                        "--role" => {
                            index += 1;
                            role = args.get(index).cloned();
                        }
                        "--config" => {
                            index += 1;
                            config_path = PathBuf::from(args.get(index).ok_or("missing config path")?);
                        }
                        _ => {}
                    }
                    index += 1;
                }
                Command::AdminSeedUser {
                    username: username.ok_or("missing --username")?,
                    password: password.ok_or("missing --password")?,
                    role: role.unwrap_or_else(|| "developer".to_string()),
                }
            }
            _ => Command::Serve,
        };
        Ok(Self {
            config_path,
            command,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        Json,
        body::{Body, to_bytes},
        http::{Request, StatusCode},
    };
    use serde_json::Value as JsonValue;
    use sha2::Digest;
    use std::{
        fs,
        path::PathBuf,
        sync::{Arc, Mutex},
        time::{SystemTime, UNIX_EPOCH},
    };
    use tower::util::ServiceExt;

    #[tokio::test]
    async fn repo_creation_installs_hook() {
        let fixture = test_fixture().await;
        let user = fixture.user.clone();
        let token = csrf_token(&fixture.state, &user);
        let cookie = session_cookie_value(&fixture.state, &user.id);

        let response = fixture
            .app
            .clone()
            .oneshot(
                Request::post("/repos")
                    .header("content-type", "application/x-www-form-urlencoded")
                    .header("cookie", cookie)
                    .body(Body::from(format!(
                        "csrf_token={}&owner_id={}&name=demo&default_branch=main",
                        token, user.id
                    )))
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::SEE_OTHER);
        let repos = fixture.state.db.list_repos().expect("repos");
        let repo = repos.iter().find(|repo| repo.name == "demo").expect("repo");
        let hook = fs::read_to_string(PathBuf::from(&repo.bare_path).join("hooks/post-receive"))
            .expect("hook");
        assert!(hook.contains("hook post-receive"));
        assert!(hook.contains(&repo.id));
    }

    #[test]
    fn hook_ingestion_is_idempotent() {
        let dir = temp_dir("hook_idempotent");
        let config_path = write_test_config(&dir);
        let state = build_state(config_path, PathBuf::from("/bin/strait-server")).expect("state");
        let hash = hash_password("password123").expect("hash");
        state.db.create_user("alice", &hash, "developer").expect("user");
        let user = state.db.get_user_credentials("alice").expect("user").unwrap().0;
        let repo_id = state.db.create_repo(
            &user.id,
            "demo",
            "demo",
            &dir.join("repos/demo.git").display().to_string(),
            "main",
        ).expect("repo");
        let refs = vec![crate::models::PushEventRef {
            old_rev: "0".repeat(40),
            new_rev: "1".repeat(40),
            ref_name: "refs/heads/main".to_string(),
        }];
        let key = git::event_key(&repo_id, &refs);
        state.db.create_push_event(&repo_id, &key, &refs).expect("push1");
        state.db.create_push_event(&repo_id, &key, &refs).expect("push2");
        let events = state.db.list_unprocessed_push_events().expect("events");
        assert_eq!(events.len(), 1);
    }

    #[tokio::test]
    async fn push_event_creates_pipeline_and_dispatches_job() {
        let mock = spawn_mock_runner().await;
        let fixture = test_fixture_with_runner(&mock.base_url).await;
        let repo = create_repo_direct(&fixture.state, &fixture.user, "demo");
        create_workflow_direct(&fixture.state, &repo.id, &fixture.runner_id);
        fixture.state.db.create_push_event(
            &repo.id,
            "event-1",
            &[crate::models::PushEventRef {
                old_rev: "0".repeat(40),
                new_rev: "1".repeat(40),
                ref_name: "refs/heads/main".to_string(),
            }],
        ).expect("push event");

        scheduler::reconcile_once(Arc::clone(&fixture.state)).await.expect("reconcile");

        let pipelines = fixture.state.db.list_pipeline_runs().expect("pipelines");
        assert_eq!(pipelines.len(), 1);
        let snapshot = fixture.state.db.pipeline_snapshot(&pipelines[0].id).expect("snapshot").unwrap();
        assert_eq!(snapshot.jobs.len(), 1);
        assert_eq!(snapshot.jobs[0].run.status, "running");
    }

    #[tokio::test]
    async fn scheduler_persists_terminal_runner_state() {
        let mock = spawn_mock_runner().await;
        let fixture = test_fixture_with_runner(&mock.base_url).await;
        let repo = create_repo_direct(&fixture.state, &fixture.user, "demo");
        create_workflow_direct(&fixture.state, &repo.id, &fixture.runner_id);
        fixture.state.db.create_push_event(
            &repo.id,
            "event-2",
            &[crate::models::PushEventRef {
                old_rev: "0".repeat(40),
                new_rev: "1".repeat(40),
                ref_name: "refs/heads/main".to_string(),
            }],
        ).expect("push event");

        scheduler::reconcile_once(Arc::clone(&fixture.state)).await.expect("reconcile1");
        scheduler::reconcile_once(Arc::clone(&fixture.state)).await.expect("reconcile2");

        let pipeline = fixture.state.db.list_pipeline_runs().expect("pipelines").remove(0);
        let snapshot = fixture.state.db.pipeline_snapshot(&pipeline.id).expect("snapshot").unwrap();
        assert_eq!(snapshot.pipeline.status, "success");
        assert_eq!(snapshot.jobs[0].run.status, "success");
        assert!(snapshot.jobs[0].stdout.contains("ok"));
    }

    struct TestFixture {
        state: Arc<AppState>,
        app: Router,
        user: User,
        runner_id: String,
    }

    async fn test_fixture() -> TestFixture {
        test_fixture_with_runner("http://127.0.0.1:9").await
    }

    async fn test_fixture_with_runner(base_url: &str) -> TestFixture {
        let dir = temp_dir("fixture");
        let config_path = write_test_config(&dir);
        let state = build_state(config_path, PathBuf::from("/bin/strait-server")).expect("state");
        let hash = hash_password("password123").expect("hash");
        state.db.create_user("alice", &hash, "developer").expect("user");
        let user = state.db.get_user_credentials("alice").expect("creds").unwrap().0;
        let runner_id = state.db.create_runner("runner1", base_url, "token").expect("runner");
        if base_url != "http://127.0.0.1:9" {
            state.db.replace_runner_jobs(
                &runner_id,
                &[(
                    "build-app".to_string(),
                    r#"{"name":"build-app","timeout_seconds":60}"#.to_string(),
                )],
            ).expect("runner jobs");
            state.db.update_runner_health(&runner_id, "healthy").expect("health");
        }
        let session_id = state
            .db
            .create_session(&user.id, &(Utc::now() + Duration::days(1)).to_rfc3339())
            .expect("session");
        let app = build_router(Arc::clone(&state));
        let _cookie = session_cookie(&state.config.auth.session_secret, &session_id);
        TestFixture {
            state,
            app,
            user,
            runner_id,
        }
    }

    fn create_repo_direct(state: &Arc<AppState>, user: &User, name: &str) -> Repo {
        let path = PathBuf::from(&state.config.repos_dir).join(format!("{name}.git"));
        let repo_id = state.db.create_repo(
            &user.id,
            name,
            name,
            &path.display().to_string(),
            "main",
        ).expect("repo");
        state.db.get_repo(&repo_id).expect("repo").unwrap()
    }

    fn create_workflow_direct(state: &Arc<AppState>, repo_id: &str, runner_id: &str) {
        let trigger = serde_json::to_string(&WorkflowTrigger {
            kind: "push".to_string(),
            branches: vec!["main".to_string()],
        }).expect("trigger");
        let definition = serde_json::to_string(&WorkflowDefinition {
            jobs: vec![WorkflowJobDefinition {
                id: "build".to_string(),
                name: "Build".to_string(),
                runner_id: runner_id.to_string(),
                runner_job_name: "build-app".to_string(),
                needs: Vec::new(),
                inputs: BTreeMap::from([
                    ("commit".to_string(), json!("$commit")),
                    ("branch".to_string(), json!("$branch")),
                ]),
                artifacts_from: Vec::new(),
                allow_failure: false,
            }],
        }).expect("definition");
        state.db.create_workflow(repo_id, "wf", true, &trigger, &definition).expect("workflow");
    }

    fn write_test_config(dir: &Path) -> PathBuf {
        let config_path = dir.join("server.toml");
        fs::create_dir_all(dir.join("data")).expect("data dir");
        fs::create_dir_all(dir.join("repos")).expect("repos dir");
        fs::write(
            &config_path,
            format!(
                r#"data_dir = "{}"
repos_dir = "{}"

[database]
path = "{}"

[server]
listen = "127.0.0.1:0"
public_base_url = "ci.test"

[auth]
session_secret = "test-secret"

[auth.bootstrap_admin]
username = "admin"
password = "password123"

[scheduler]
poll_interval_ms = 50

[runners]
healthcheck_interval_seconds = 60
"#,
                dir.join("data").display(),
                dir.join("repos").display(),
                dir.join("data/server.sqlite3").display(),
            ),
        )
        .expect("config");
        config_path
    }

    fn temp_dir(label: &str) -> PathBuf {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("strait-server-{label}-{suffix}"));
        fs::create_dir_all(&dir).expect("temp dir");
        dir
    }

    fn session_cookie_value(state: &Arc<AppState>, user_id: &str) -> String {
        let session_id = state
            .db
            .create_session(user_id, &(Utc::now() + Duration::days(1)).to_rfc3339())
            .expect("session");
        session_cookie(&state.config.auth.session_secret, &session_id).to_string()
    }

    struct MockRunnerState {
        runs: Mutex<BTreeMap<String, usize>>,
    }

    struct MockRunner {
        base_url: String,
    }

    async fn spawn_mock_runner() -> MockRunner {
        let state = Arc::new(MockRunnerState {
            runs: Mutex::new(BTreeMap::new()),
        });
        let app = Router::new()
            .route("/jobs", get(mock_list_jobs))
            .route("/jobs/{name}/runs", post(mock_create_run))
            .route("/runs/{job_id}", get(mock_get_run).delete(mock_cancel_run))
            .route("/runs/{job_id}/logs", get(mock_logs))
            .route("/artifacts", post(mock_artifact_upload))
            .route("/artifacts/{artifact_id}", get(mock_artifact_download))
            .with_state(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve");
        });
        MockRunner {
            base_url: format!("http://{}", addr),
        }
    }

    async fn mock_list_jobs() -> Json<JsonValue> {
        Json(json!([{"name":"build-app","timeout_seconds":60}]))
    }

    async fn mock_create_run(
        State(state): State<Arc<MockRunnerState>>,
        AxumPath(_name): AxumPath<String>,
    ) -> (StatusCode, Json<JsonValue>) {
        let job_id = Uuid::now_v7().to_string();
        state.runs.lock().expect("runs").insert(job_id.clone(), 0);
        (
            StatusCode::CREATED,
            Json(json!({"job_id":job_id,"status":"running","started_at":Utc::now().to_rfc3339()})),
        )
    }

    async fn mock_get_run(
        State(state): State<Arc<MockRunnerState>>,
        AxumPath(job_id): AxumPath<String>,
    ) -> Json<JsonValue> {
        let mut runs = state.runs.lock().expect("runs");
        let attempts = runs.entry(job_id.clone()).or_insert(0);
        *attempts += 1;
        if *attempts >= 2 {
            Json(json!({
                "job_id": job_id,
                "name": "build-app",
                "status": "success",
                "started_at": Utc::now().to_rfc3339(),
                "finished_at": Utc::now().to_rfc3339(),
                "exit_code": 0,
                "outputs": {}
            }))
        } else {
            Json(json!({
                "job_id": job_id,
                "name": "build-app",
                "status": "running",
                "started_at": Utc::now().to_rfc3339(),
                "finished_at": null,
                "exit_code": null,
                "outputs": {}
            }))
        }
    }

    async fn mock_logs() -> Json<JsonValue> {
        Json(json!({"stdout":"ok\n","stderr":""}))
    }

    async fn mock_cancel_run() -> StatusCode {
        StatusCode::ACCEPTED
    }

    async fn mock_artifact_upload(body: Body) -> (StatusCode, Json<JsonValue>) {
        let bytes = to_bytes(body, usize::MAX).await.expect("bytes");
        (
            StatusCode::CREATED,
            Json(json!({
                "artifact_id":"artifact-1",
                "sha256":format!("{:x}", Sha256::digest(&bytes)),
                "size": bytes.len(),
                "expires_at": Utc::now().to_rfc3339()
            })),
        )
    }

    async fn mock_artifact_download() -> Body {
        Body::from("artifact")
    }
}
