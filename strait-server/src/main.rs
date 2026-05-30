mod auth;
mod config;
mod db;
mod git;
mod models;
mod runner;
mod scheduler;

use std::{
    env, fs,
    io,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
};

use auth::{AdminUser, CurrentUser, clear_session_cookie, hash_password, session_cookie, verify_password};
use axum::{
    Form, Router,
    extract::{Path as AxumPath, Query, State},
    http::{HeaderMap, StatusCode, header::SET_COOKIE},
    response::{Html, IntoResponse, Redirect, Response, Sse},
    routing::{get, post},
};
use chrono::{Duration, Utc};
use config::Config;
use models::{WorkflowDefinition, WorkflowTrigger};
use serde::Deserialize;
use serde_json::json;
use tokio::time;
use tracing::info;
use uuid::Uuid;

use crate::{db::Database, runner::RunnerClient};

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
    let config = Arc::new(Config::load_from_path(&cli.config_path)?);
    fs::create_dir_all(&config.data_dir)?;
    fs::create_dir_all(&config.repos_dir)?;
    if let Some(parent) = Path::new(&config.database.path).parent() {
        fs::create_dir_all(parent)?;
    }

    let db = Database::open(&config.database.path)?;
    let admin_hash = hash_password(&config.auth.bootstrap_admin.password)?;
    db.ensure_user(&config.auth.bootstrap_admin.username, &admin_hash, "admin")?;

    let state = Arc::new(AppState {
        config: Arc::clone(&config),
        db,
        runner_client: RunnerClient::new(),
        config_path: Arc::new(cli.config_path.clone()),
        server_bin: Arc::new(env::current_exe()?),
    });

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

fn build_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/login", get(login_form).post(login))
        .route("/logout", post(logout))
        .route("/health", get(health))
        .route("/ready", get(ready))
        .route("/users", get(users_page).post(create_user))
        .route("/repos", get(repos_page).post(create_repo))
        .route("/runners", get(runners_page).post(create_runner))
        .route("/runners/{runner_id}/disable", post(disable_runner))
        .route("/runners/{runner_id}/test", post(test_runner))
        .route("/workflows", get(workflows_page).post(create_workflow))
        .route("/pipelines", get(pipelines_page))
        .route("/pipelines/{pipeline_id}", get(pipeline_detail))
        .route("/pipelines/{pipeline_id}/events", get(pipeline_events))
        .route("/repos/{repo_id}/trigger", post(trigger_repo))
        .with_state(state)
}

async fn health() -> Html<String> {
    Html("<pre>ok</pre>".to_string())
}

async fn ready() -> Html<String> {
    Html("<pre>ready</pre>".to_string())
}

async fn index() -> impl IntoResponse {
    Redirect::to("/repos").into_response()
}

async fn login_form() -> Html<String> {
    Html(layout(
        "Login",
        r#"<form method="post" action="/login">
<label>Username <input name="username" /></label><br/>
<label>Password <input name="password" type="password" /></label><br/>
<button type="submit">Login</button>
</form>"#,
    ))
}

#[derive(Deserialize)]
struct LoginForm {
    username: String,
    password: String,
}

async fn login(
    State(state): State<Arc<AppState>>,
    Form(form): Form<LoginForm>,
) -> Response {
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
    if let Some(cookie) = headers.get("cookie").and_then(|value| value.to_str().ok()) {
        for part in cookie.split(';') {
            let trimmed = part.trim();
            if let Some(session_id) = trimmed.strip_prefix("strait_session=") {
                let _ = state.db.delete_session(session_id);
            }
        }
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

async fn users_page(
    _: AdminUser,
    State(state): State<Arc<AppState>>,
) -> Result<Html<String>, Response> {
    let users = state
        .db
        .list_users()
        .map_err(internal_error)?;
    let mut body = String::from(
        r#"<form method="post" action="/users">
<label>Username <input name="username" /></label>
<label>Password <input name="password" type="password" /></label>
<label>Role <select name="role"><option>developer</option><option>admin</option></select></label>
<button type="submit">Create user</button>
</form>
<ul>"#,
    );
    for user in users {
        body.push_str(&format!("<li>{} ({})</li>", user.username, user.role));
    }
    body.push_str("</ul>");
    Ok(Html(layout("Users", &body)))
}

#[derive(Deserialize)]
struct CreateUserForm {
    username: String,
    password: String,
    role: String,
}

async fn create_user(
    _: AdminUser,
    State(state): State<Arc<AppState>>,
    Form(form): Form<CreateUserForm>,
) -> Result<Redirect, Response> {
    let hash = hash_password(&form.password).map_err(internal_error_text)?;
    state
        .db
        .create_user(&form.username, &hash, &form.role)
        .map_err(internal_error)?;
    Ok(Redirect::to("/users"))
}

async fn repos_page(
    user: CurrentUser,
    State(state): State<Arc<AppState>>,
) -> Result<Html<String>, Response> {
    let repos = state.db.list_repos().map_err(internal_error)?;
    let users = state.db.list_users().map_err(internal_error)?;
    let mut body = String::from(
        r#"<form method="post" action="/repos">
<label>Name <input name="name" /></label>
<label>Owner <select name="owner_id">"#,
    );
    for candidate in users {
        let selected = if candidate.id == user.0.id { " selected" } else { "" };
        body.push_str(&format!(
            "<option value=\"{}\"{}>{}</option>",
            candidate.id, selected, candidate.username
        ));
    }
    body.push_str(
        r#"</select></label>
<label>Default branch <input name="default_branch" value="main" /></label>
<button type="submit">Create repo</button>
</form>
<ul>"#,
    );
    for repo in repos {
        let clone_url = format!(
            "ssh://git@{}/{}/{}",
            state.config.server.public_base_url.trim_end_matches('/'),
            repo.owner_username,
            repo.name
        );
        body.push_str(&format!(
            "<li><strong>{}/{}</strong> clone: <code>{}</code>
            <form method=\"post\" action=\"/repos/{}/trigger\"><button type=\"submit\">Manual trigger</button></form>
            </li>",
            repo.owner_username, repo.name, clone_url, repo.id
        ));
    }
    body.push_str("</ul>");
    Ok(Html(layout("Repos", &body)))
}

#[derive(Deserialize)]
struct CreateRepoForm {
    owner_id: String,
    name: String,
    default_branch: String,
}

async fn create_repo(
    _: CurrentUser,
    State(state): State<Arc<AppState>>,
    Form(form): Form<CreateRepoForm>,
) -> Result<Redirect, Response> {
    let normalized = git::validate_repo_name(&form.name).map_err(bad_request)?;
    let repo_id = state
        .db
        .create_repo(
            &form.owner_id,
            &form.name,
            &normalized,
            &PathBuf::from(&state.config.repos_dir)
                .join(format!("{}.git", Uuid::now_v7()))
                .display()
                .to_string(),
            &form.default_branch,
        )
        .map_err(internal_error)?;
    let repo = state.db.get_repo(&repo_id).map_err(internal_error)?.ok_or_else(|| internal_error_text("missing repo after create"))?;
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

async fn runners_page(
    _: CurrentUser,
    State(state): State<Arc<AppState>>,
) -> Result<Html<String>, Response> {
    let runners = state.db.list_runners().map_err(internal_error)?;
    let mut body = String::from(
        r#"<form method="post" action="/runners">
<label>Name <input name="name" /></label>
<label>Base URL <input name="base_url" placeholder="http://127.0.0.1:8080" /></label>
<label>Token <input name="token" /></label>
<button type="submit">Add runner</button>
</form>
<ul>"#,
    );
    for runner in runners {
        body.push_str(&format!(
            "<li><strong>{}</strong> {} [{}]
            <form method=\"post\" action=\"/runners/{}/test\"><button type=\"submit\">Test</button></form>
            <form method=\"post\" action=\"/runners/{}/disable\"><button type=\"submit\">{}</button></form>
            </li>",
            runner.name,
            runner.base_url,
            runner.last_health_state,
            runner.id,
            runner.id,
            if runner.enabled { "Disable" } else { "Enable" }
        ));
        let jobs = state.db.list_runner_jobs(&runner.id).map_err(internal_error)?;
        if !jobs.is_empty() {
            body.push_str("<ul>");
            for (job_name, _) in jobs {
                body.push_str(&format!("<li>{}</li>", job_name));
            }
            body.push_str("</ul>");
        }
    }
    body.push_str("</ul>");
    Ok(Html(layout("Runners", &body)))
}

#[derive(Deserialize)]
struct CreateRunnerForm {
    name: String,
    base_url: String,
    token: String,
}

async fn create_runner(
    _: CurrentUser,
    State(state): State<Arc<AppState>>,
    Form(form): Form<CreateRunnerForm>,
) -> Result<Redirect, Response> {
    let runner_id = state
        .db
        .create_runner(&form.name, &form.base_url, &form.token)
        .map_err(internal_error)?;
    refresh_single_runner(&state, &runner_id).await.map_err(internal_error_text)?;
    Ok(Redirect::to("/runners"))
}

async fn disable_runner(
    _: CurrentUser,
    State(state): State<Arc<AppState>>,
    AxumPath(runner_id): AxumPath<String>,
) -> Result<Redirect, Response> {
    let runner = state.db.get_runner(&runner_id).map_err(internal_error)?.ok_or_else(|| not_found("runner"))?;
    state
        .db
        .set_runner_enabled(&runner_id, !runner.enabled)
        .map_err(internal_error)?;
    Ok(Redirect::to("/runners"))
}

async fn test_runner(
    _: CurrentUser,
    State(state): State<Arc<AppState>>,
    AxumPath(runner_id): AxumPath<String>,
) -> Result<Redirect, Response> {
    refresh_single_runner(&state, &runner_id).await.map_err(internal_error_text)?;
    Ok(Redirect::to("/runners"))
}

async fn refresh_single_runner(
    state: &Arc<AppState>,
    runner_id: &str,
) -> Result<(), String> {
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

async fn workflows_page(
    _: CurrentUser,
    State(state): State<Arc<AppState>>,
) -> Result<Html<String>, Response> {
    let repos = state.db.list_repos().map_err(internal_error)?;
    let workflows = state.db.list_workflows().map_err(internal_error)?;
    let mut body = String::from(
        r#"<form method="post" action="/workflows">
<label>Repo <select name="repo_id">"#,
    );
    for repo in repos {
        body.push_str(&format!(
            "<option value=\"{}\">{}/{}</option>",
            repo.id, repo.owner_username, repo.name
        ));
    }
    body.push_str(
        r#"</select></label>
<label>Name <input name="name" /></label>
<label>Enabled <input type="checkbox" name="enabled" value="true" checked /></label>
<label>Trigger JSON <textarea name="trigger_json" rows="5" cols="60">{"kind":"push","branches":["main"]}</textarea></label>
<label>Definition JSON <textarea name="definition_json" rows="12" cols="100">{"jobs":[{"id":"build","name":"Build","runner_id":"replace-runner-id","runner_job_name":"build-app","needs":[],"inputs":{"commit":"$commit","branch":"$branch","source":"$source"},"allow_failure":false}]}</textarea></label>
<button type="submit">Create workflow</button>
</form>
<ul>"#,
    );
    for workflow in workflows {
        body.push_str(&format!(
            "<li><strong>{}</strong> repo={} version={} enabled={}</li>",
            workflow.name, workflow.repo_id, workflow.version, workflow.enabled
        ));
    }
    body.push_str("</ul>");
    Ok(Html(layout("Workflows", &body)))
}

#[derive(Deserialize)]
struct CreateWorkflowForm {
    repo_id: String,
    name: String,
    enabled: Option<String>,
    trigger_json: String,
    definition_json: String,
}

async fn create_workflow(
    _: CurrentUser,
    State(state): State<Arc<AppState>>,
    Form(form): Form<CreateWorkflowForm>,
) -> Result<Redirect, Response> {
    let trigger: WorkflowTrigger =
        serde_json::from_str(&form.trigger_json).map_err(internal_error_text)?;
    if !matches!(trigger.kind.as_str(), "push" | "manual") {
        return Err(bad_request("trigger kind must be push or manual"));
    }
    let definition: WorkflowDefinition =
        serde_json::from_str(&form.definition_json).map_err(internal_error_text)?;
    definition.validate().map_err(bad_request)?;
    state
        .db
        .create_workflow(
            &form.repo_id,
            &form.name,
            form.enabled.is_some(),
            &form.trigger_json,
            &form.definition_json,
        )
        .map_err(internal_error)?;
    Ok(Redirect::to("/workflows"))
}

async fn pipelines_page(
    _: CurrentUser,
    State(state): State<Arc<AppState>>,
) -> Result<Html<String>, Response> {
    let pipelines = state.db.list_pipeline_runs().map_err(internal_error)?;
    let mut body = String::from("<ul>");
    for pipeline in pipelines {
        body.push_str(&format!(
            "<li><a href=\"/pipelines/{}\">{}</a> {} {}</li>",
            pipeline.id,
            pipeline.id,
            pipeline.status,
            pipeline.trigger_ref.unwrap_or_default()
        ));
    }
    body.push_str("</ul>");
    Ok(Html(layout("Pipelines", &body)))
}

async fn pipeline_detail(
    _: CurrentUser,
    State(state): State<Arc<AppState>>,
    AxumPath(pipeline_id): AxumPath<String>,
) -> Result<Html<String>, Response> {
    let snapshot = state
        .db
        .pipeline_snapshot(&pipeline_id)
        .map_err(internal_error)?
        .ok_or_else(|| not_found("pipeline"))?;
    let mut body = format!(
        "<p>Status: {}</p><p>Trigger: {:?}</p><ul>",
        snapshot.pipeline.status, snapshot.pipeline.trigger_ref
    );
    for job in snapshot.jobs {
        body.push_str(&format!(
            "<li><strong>{}</strong> [{}]<pre>{}</pre><pre>{}</pre></li>",
            job.run.job_name, job.run.status, html_escape(&job.stdout), html_escape(&job.stderr)
        ));
    }
    body.push_str("</ul><script>const e=new EventSource('/pipelines/");
    body.push_str(&pipeline_id);
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

async fn trigger_repo(
    _: CurrentUser,
    State(state): State<Arc<AppState>>,
    AxumPath(repo_id): AxumPath<String>,
    Query(query): Query<std::collections::BTreeMap<String, String>>,
) -> Result<Redirect, Response> {
    let branch = query
        .get("branch")
        .cloned()
        .unwrap_or_else(|| "refs/heads/main".to_string());
    let commit = query
        .get("commit")
        .cloned()
        .unwrap_or_else(|| "HEAD".to_string());
    let refs = vec![crate::models::PushEventRef {
        old_rev: "0000000000000000000000000000000000000000".to_string(),
        new_rev: commit,
        ref_name: branch,
    }];
    let key = crate::git::event_key(&repo_id, &refs);
    state
        .db
        .create_push_event(&repo_id, &key, &refs)
        .map_err(internal_error)?;
    Ok(Redirect::to("/pipelines"))
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

fn html_escape(input: &str) -> String {
    input
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
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
                            config_path =
                                PathBuf::from(args.get(index).ok_or("missing config path")?);
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
