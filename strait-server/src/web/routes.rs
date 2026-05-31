use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::Arc,
};

use axum::{
    Form, Json, Router,
    extract::{Path as AxumPath, State},
    http::{
        HeaderMap, StatusCode,
        header::{CACHE_CONTROL, CONTENT_TYPE, SET_COOKIE},
    },
    response::{IntoResponse, Redirect, Response, Sse},
    routing::{get, post},
};
use chrono::{Duration, Utc};
use hmac::{Hmac, Mac};
use maud::{Markup, html};
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

use super::views::{
    pipeline as pipeline_view, repo as repo_view, runner as runner_view, user as user_view,
    workflow as workflow_view,
};

type HmacSha256 = Hmac<Sha256>;

pub(crate) fn build_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/login", get(login_form).post(login))
        .route("/logout", post(logout))
        .route("/health", get(health))
        .route("/ready", get(ready))
        .route("/assets/app.css", get(asset_app_css))
        .route(
            "/assets/workflow_builder.js",
            get(asset_workflow_builder_js),
        )
        .route(
            "/assets/workflow_builder_dom.js",
            get(asset_workflow_builder_dom_js),
        )
        .route(
            "/assets/workflow_builder_bindings.js",
            get(asset_workflow_builder_bindings_js),
        )
        .route(
            "/assets/workflow_builder_tables.js",
            get(asset_workflow_builder_tables_js),
        )
        .route(
            "/assets/workflow_builder_rows.js",
            get(asset_workflow_builder_rows_js),
        )
        .route(
            "/assets/workflow_builder_state.js",
            get(asset_workflow_builder_state_js),
        )
        .route("/assets/pipeline_events.js", get(asset_pipeline_events_js))
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

async fn health() -> Markup {
    html! { pre { "ok" } }
}
async fn ready() -> Markup {
    html! { pre { "ready" } }
}
struct EmbeddedAsset {
    content_type: &'static str,
    body: &'static str,
}

impl IntoResponse for EmbeddedAsset {
    fn into_response(self) -> Response {
        (
            [
                (CONTENT_TYPE, self.content_type),
                (CACHE_CONTROL, "public, max-age=300"),
            ],
            self.body,
        )
            .into_response()
    }
}

async fn asset_app_css() -> EmbeddedAsset {
    embedded_asset("text/css; charset=utf-8", include_str!("assets/app.css"))
}
async fn asset_workflow_builder_js() -> EmbeddedAsset {
    embedded_asset(
        "text/javascript; charset=utf-8",
        include_str!("assets/workflow_builder.js"),
    )
}
async fn asset_workflow_builder_dom_js() -> EmbeddedAsset {
    embedded_asset(
        "text/javascript; charset=utf-8",
        include_str!("assets/workflow_builder_dom.js"),
    )
}
async fn asset_workflow_builder_bindings_js() -> EmbeddedAsset {
    embedded_asset(
        "text/javascript; charset=utf-8",
        include_str!("assets/workflow_builder_bindings.js"),
    )
}
async fn asset_workflow_builder_tables_js() -> EmbeddedAsset {
    embedded_asset(
        "text/javascript; charset=utf-8",
        include_str!("assets/workflow_builder_tables.js"),
    )
}
async fn asset_workflow_builder_rows_js() -> EmbeddedAsset {
    embedded_asset(
        "text/javascript; charset=utf-8",
        include_str!("assets/workflow_builder_rows.js"),
    )
}
async fn asset_workflow_builder_state_js() -> EmbeddedAsset {
    embedded_asset(
        "text/javascript; charset=utf-8",
        include_str!("assets/workflow_builder_state.js"),
    )
}
async fn asset_pipeline_events_js() -> EmbeddedAsset {
    embedded_asset(
        "text/javascript; charset=utf-8",
        include_str!("assets/pipeline_events.js"),
    )
}
fn embedded_asset(content_type: &'static str, body: &'static str) -> EmbeddedAsset {
    EmbeddedAsset { content_type, body }
}
async fn index() -> impl IntoResponse {
    Redirect::to("/repos")
}

async fn login_form() -> Markup {
    user_view::login_page()
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
pub(super) enum WorkflowSchemaStatus {
    Current,
    Stale,
    Incompatible,
}

impl WorkflowSchemaStatus {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Current => "current",
            Self::Stale => "stale",
            Self::Incompatible => "incompatible",
        }
    }

    pub(super) fn tone(self) -> &'static str {
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
) -> Result<Markup, Response> {
    let csrf = csrf_token(&state, &user);
    let users = state.db.list_users().map_err(internal_error)?;
    Ok(user_view::users_page(users, &csrf))
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
) -> Result<Markup, Response> {
    let repos = state.db.list_repos().map_err(internal_error)?;
    let users = state.db.list_users().map_err(internal_error)?;
    let csrf = csrf_token(&state, &user);
    let repo_cards = repos
        .into_iter()
        .filter(|repo| can_view_repo(&user, repo))
        .map(|repo| repo_view::RepoCard {
            clone_url: repo_clone_url(&state, &repo),
            repo,
        })
        .collect::<Vec<_>>();
    Ok(repo_view::repos_page(&user, users, repo_cards, &csrf))
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
) -> Result<Markup, Response> {
    let runners = state.db.list_runners().map_err(internal_error)?;
    let csrf = csrf_token(&state, &user);
    let runners_with_jobs = runners
        .into_iter()
        .map(|runner| {
            let jobs = state
                .db
                .list_runner_jobs(&runner.id)
                .map_err(internal_error)?;
            Ok((runner, jobs))
        })
        .collect::<Result<Vec<_>, Response>>()?;
    Ok(runner_view::runners_page(runners_with_jobs, &csrf))
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
) -> Result<Markup, Response> {
    let repos = visible_repos_for_user(&state, &user)?;
    let workflows = state.db.list_workflows().map_err(internal_error)?;
    let runner_catalog = workflow_runner_catalog(&state)?;
    let csrf = csrf_token(&state, &user);
    let repo_select = workflow_view::repo_selector(&repos);
    let form = workflow_form_view(None, &runner_catalog, repo_select);
    let workflow_cards = workflows
        .into_iter()
        .filter(|item| repo_ids_contains(&repos, &item.repo_id))
        .map(|workflow| {
            let schema_status =
                workflow_schema_status(&state, &workflow).map_err(internal_error)?;
            let trigger: WorkflowTrigger =
                serde_json::from_str(&workflow.trigger_json).unwrap_or(WorkflowTrigger {
                    kind: "push".to_string(),
                    branches: Vec::new(),
                });
            let definition: WorkflowDefinition = serde_json::from_str(&workflow.definition_json)
                .unwrap_or(WorkflowDefinition { jobs: Vec::new() });
            Ok(workflow_view::WorkflowCard {
                workflow,
                schema_status,
                trigger,
                definition,
            })
        })
        .collect::<Result<Vec<_>, Response>>()?;
    Ok(workflow_view::workflows_page(form, workflow_cards, &csrf))
}

async fn workflow_detail_page(
    CurrentUser(user): CurrentUser,
    State(state): State<Arc<AppState>>,
    AxumPath(workflow_id): AxumPath<String>,
) -> Result<Markup, Response> {
    let workflow = authorized_workflow(&state, &user, &workflow_id)?;
    let csrf = csrf_token(&state, &user);
    let repo = state
        .db
        .get_repo(&workflow.repo_id)
        .map_err(internal_error)?
        .ok_or_else(|| not_found("repo"))?;
    let runner_catalog = workflow_runner_catalog(&state)?;
    let repo_field = workflow_view::fixed_repo_field(&workflow, &repo);
    let schema_status = workflow_schema_status(&state, &workflow).map_err(internal_error)?;
    let form = workflow_form_view(Some(&workflow), &runner_catalog, repo_field);
    Ok(workflow_view::workflow_detail_page(
        &workflow,
        &repo,
        schema_status,
        form,
        &csrf,
    ))
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
            &parsed.job_schemas,
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
            &parsed.job_schemas,
        )
        .map_err(internal_error)?;
    Ok(Redirect::to(&format!("/workflows/{}", workflow.id)))
}

async fn pipelines_page(
    CurrentUser(user): CurrentUser,
    State(state): State<Arc<AppState>>,
) -> Result<Markup, Response> {
    let pipelines = state.db.list_pipeline_runs().map_err(internal_error)?;
    let repos = state.db.list_repos().map_err(internal_error)?;
    let repo_by_id = repos
        .into_iter()
        .map(|repo| (repo.id.clone(), repo))
        .collect::<BTreeMap<_, _>>();
    let visible_pipelines = pipelines
        .into_iter()
        .filter_map(|pipeline| {
            let repo = repo_by_id.get(&pipeline.repo_id)?;
            can_view_repo(&user, repo).then_some((pipeline, repo.clone()))
        })
        .collect::<Vec<_>>();
    Ok(pipeline_view::pipelines_page(visible_pipelines))
}

async fn pipeline_detail(
    CurrentUser(user): CurrentUser,
    State(state): State<Arc<AppState>>,
    AxumPath(pipeline_id): AxumPath<String>,
) -> Result<Markup, Response> {
    let pipeline = authorized_pipeline(&state, &user, &pipeline_id)?;
    let snapshot = state
        .db
        .pipeline_snapshot(&pipeline.id)
        .map_err(internal_error)?
        .ok_or_else(|| not_found("pipeline"))?;
    let csrf = csrf_token(&state, &user);
    Ok(pipeline_view::pipeline_detail_page(
        &pipeline, &snapshot, &csrf,
    ))
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
            &parsed.job_schemas,
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
            &parsed.job_schemas,
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
    job_schemas: Vec<RunnerJobSchema>,
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
        job_schemas,
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
        job_schemas,
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
        let Some(runner_job) = jobs
            .into_iter()
            .find(|schema| schema.name == job.runner_job_name)
        else {
            return Err(bad_request(format!(
                "runner {} does not advertise job {}",
                runner.name, job.runner_job_name
            )));
        };
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
            .map_err(internal_error)?;
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
    let snapshot = state.db.workflow_job_schemas(&workflow.version_id)?;
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
            .find(|schema| schema.name == job.runner_job_name);
        let Some(current_schema) = current_schema else {
            return Ok(WorkflowSchemaStatus::Incompatible);
        };
        if &current_schema != saved_schema {
            status = WorkflowSchemaStatus::Stale;
        }
    }

    Ok(status)
}

fn workflow_form_view(
    workflow: Option<&Workflow>,
    runner_catalog: &[WorkflowRunnerCatalogEntry],
    repo_field: Markup,
) -> workflow_view::WorkflowFormView {
    let (trigger_kind, branch_name, jobs_json, definition) = if let Some(item) = workflow {
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
            definition,
        )
    } else {
        (
            "push".to_string(),
            "main".to_string(),
            "[]".to_string(),
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

    workflow_view::WorkflowFormView {
        name: workflow.map(|item| item.name.clone()).unwrap_or_default(),
        trigger_kind,
        branch_name,
        jobs_json,
        repo_field,
        runner_catalog_json: workflow_view::script_json(
            &serde_json::to_string(&catalog).unwrap_or_else(|_| "[]".to_string()),
        ),
        initial_jobs_json: workflow_view::script_json(
            &serde_json::to_string(&initial_jobs).unwrap_or_else(|_| "[]".to_string()),
        ),
        is_edit: workflow.is_some(),
    }
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
