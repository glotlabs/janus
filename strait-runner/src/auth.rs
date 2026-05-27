use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    marker::PhantomData,
    sync::Arc,
};

use axum::{
    Json,
    extract::{FromRef, FromRequestParts},
    http::{StatusCode, header, request::Parts},
    response::{IntoResponse, Response},
};
use serde::Serialize;

use crate::config::AuthConfig;

#[derive(Debug, Clone)]
pub struct AuthStore {
    tokens: BTreeMap<String, TokenRecord>,
}

impl AuthStore {
    pub fn load_from_config<F>(config: &AuthConfig, env_lookup: F) -> Result<Self, AuthError>
    where
        F: Fn(&str) -> Option<String>,
    {
        if config.mode != "bearer" {
            return Err(AuthError::UnsupportedMode(config.mode.clone()));
        }

        let mut tokens = BTreeMap::new();
        for token in &config.tokens {
            let value = env_lookup(&token.token_env)
                .ok_or_else(|| AuthError::MissingTokenEnv(token.token_env.clone()))?;
            tokens.insert(
                value,
                TokenRecord {
                    name: token.name.clone(),
                    permissions: token.permissions.iter().cloned().collect(),
                },
            );
        }

        Ok(Self { tokens })
    }

    fn authorize(&self, header_value: Option<&str>, permission: &str) -> Result<(), AuthRejection> {
        let token = parse_bearer_token(header_value)?;
        let record = self.tokens.get(token).ok_or(AuthRejection::InvalidToken)?;

        if !record.permissions.contains(permission) {
            return Err(AuthRejection::MissingPermission {
                token_name: record.name.clone(),
                permission: permission.to_string(),
            });
        }

        Ok(())
    }
}

#[derive(Debug, Clone)]
struct TokenRecord {
    name: String,
    permissions: BTreeSet<String>,
}

#[derive(Debug)]
pub enum AuthError {
    UnsupportedMode(String),
    MissingTokenEnv(String),
}

impl fmt::Display for AuthError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedMode(mode) => write!(f, "unsupported auth mode: {mode}"),
            Self::MissingTokenEnv(name) => write!(f, "missing auth token env var: {name}"),
        }
    }
}

impl std::error::Error for AuthError {}

pub trait RequiredPermission {
    const NAME: &'static str;
}

pub struct Authorized<P>(PhantomData<P>);

impl<P> Clone for Authorized<P> {
    fn clone(&self) -> Self {
        Self(PhantomData)
    }
}

impl<P> Copy for Authorized<P> {}

impl<S, P> FromRequestParts<S> for Authorized<P>
where
    Arc<AuthStore>: FromRef<S>,
    P: RequiredPermission,
    S: Send + Sync,
{
    type Rejection = AuthRejection;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let auth = Arc::<AuthStore>::from_ref(state);
        let header_value = parts
            .headers
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok());

        auth.authorize(header_value, P::NAME)?;

        Ok(Self(PhantomData))
    }
}

impl FromRef<crate::AppState> for Arc<AuthStore> {
    fn from_ref(input: &crate::AppState) -> Self {
        Arc::clone(&input.auth)
    }
}

#[derive(Debug)]
pub enum AuthRejection {
    MissingAuthorization,
    InvalidAuthorizationHeader,
    InvalidToken,
    MissingPermission {
        token_name: String,
        permission: String,
    },
}

impl IntoResponse for AuthRejection {
    fn into_response(self) -> Response {
        let status = match self {
            Self::MissingPermission { .. } => StatusCode::FORBIDDEN,
            Self::MissingAuthorization | Self::InvalidAuthorizationHeader | Self::InvalidToken => {
                StatusCode::UNAUTHORIZED
            }
        };

        let mut response = (status, Json(AuthErrorResponse::from_rejection(&self))).into_response();

        if status == StatusCode::UNAUTHORIZED {
            response.headers_mut().insert(
                header::WWW_AUTHENTICATE,
                header::HeaderValue::from_static("Bearer"),
            );
        }

        response
    }
}

impl fmt::Display for AuthRejection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingAuthorization => write!(f, "missing authorization header"),
            Self::InvalidAuthorizationHeader => write!(f, "invalid authorization header"),
            Self::InvalidToken => write!(f, "invalid bearer token"),
            Self::MissingPermission {
                token_name,
                permission,
            } => write!(
                f,
                "token {token_name} does not have required permission {permission}"
            ),
        }
    }
}

#[derive(Debug, Serialize)]
struct AuthErrorResponse {
    code: &'static str,
    message: String,
}

impl AuthErrorResponse {
    fn from_rejection(rejection: &AuthRejection) -> Self {
        let code = match rejection {
            AuthRejection::MissingAuthorization => "auth_missing_authorization",
            AuthRejection::InvalidAuthorizationHeader => "auth_invalid_authorization_header",
            AuthRejection::InvalidToken => "auth_invalid_token",
            AuthRejection::MissingPermission { .. } => "auth_missing_permission",
        };

        Self {
            code,
            message: rejection.to_string(),
        }
    }
}

fn parse_bearer_token(header_value: Option<&str>) -> Result<&str, AuthRejection> {
    let header_value = header_value.ok_or(AuthRejection::MissingAuthorization)?;
    let (scheme, token) = header_value
        .split_once(' ')
        .ok_or(AuthRejection::InvalidAuthorizationHeader)?;

    if scheme != "Bearer" || token.is_empty() {
        return Err(AuthRejection::InvalidAuthorizationHeader);
    }

    Ok(token)
}

pub struct ArtifactsWrite;
impl RequiredPermission for ArtifactsWrite {
    const NAME: &'static str = "artifacts:write";
}

pub struct ArtifactsRead;
impl RequiredPermission for ArtifactsRead {
    const NAME: &'static str = "artifacts:read";
}

pub struct JobsRun;
impl RequiredPermission for JobsRun {
    const NAME: &'static str = "jobs:run";
}

pub struct JobsRead;
impl RequiredPermission for JobsRead {
    const NAME: &'static str = "jobs:read";
}

pub struct LogsRead;
impl RequiredPermission for LogsRead {
    const NAME: &'static str = "logs:read";
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Arc};

    use axum::{
        Router,
        body::Body,
        http::{Request, StatusCode, header},
        routing::get,
    };
    use tower::util::ServiceExt;

    use super::{ArtifactsRead, AuthStore, Authorized, JobsRun};
    use crate::{
        AppState,
        artifacts::ArtifactStore,
        config::{ArtifactsConfig, AuthConfig, Config, JobsConfig, ServerConfig},
        jobs::JobStore,
        manifest::ManifestStore,
    };

    #[test]
    fn loads_tokens_from_env_lookup() {
        let config = AuthConfig {
            mode: "bearer".to_string(),
            tokens: vec![crate::config::AuthTokenConfig {
                name: "runner".to_string(),
                token_env: "TEST_RUNNER_TOKEN".to_string(),
                permissions: vec!["jobs:run".to_string()],
            }],
        };

        let auth = AuthStore::load_from_config(&config, |name| {
            BTreeMap::from([("TEST_RUNNER_TOKEN", "secret-token")])
                .get(name)
                .map(|value| value.to_string())
        })
        .expect("auth should load");

        assert!(
            auth.authorize(Some("Bearer secret-token"), "jobs:run")
                .is_ok()
        );
    }

    #[tokio::test]
    async fn rejects_missing_bearer_token() {
        let app = Router::new()
            .route("/protected", get(protected_jobs_run))
            .with_state(test_state());

        let response = app
            .oneshot(
                Request::get("/protected")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn rejects_missing_permission() {
        let app = Router::new()
            .route("/protected", get(protected_artifacts_read))
            .with_state(test_state());

        let response = app
            .oneshot(
                Request::get("/protected")
                    .header(header::AUTHORIZATION, "Bearer jobs-run-token")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn accepts_valid_token_with_permission() {
        let app = Router::new()
            .route("/protected", get(protected_jobs_run))
            .with_state(test_state());

        let response = app
            .oneshot(
                Request::get("/protected")
                    .header(header::AUTHORIZATION, "Bearer jobs-run-token")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn rejects_invalid_authorization_header_shape() {
        let app = Router::new()
            .route("/protected", get(protected_jobs_run))
            .with_state(test_state());

        let response = app
            .oneshot(
                Request::get("/protected")
                    .header(header::AUTHORIZATION, "Bearer")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn rejects_unknown_token() {
        let app = Router::new()
            .route("/protected", get(protected_jobs_run))
            .with_state(test_state());

        let response = app
            .oneshot(
                Request::get("/protected")
                    .header(header::AUTHORIZATION, "Bearer wrong-token")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    async fn protected_jobs_run(_: Authorized<JobsRun>) -> &'static str {
        "ok"
    }

    async fn protected_artifacts_read(_: Authorized<ArtifactsRead>) -> &'static str {
        "ok"
    }

    fn test_state() -> AppState {
        let temp = std::env::temp_dir().join("strait-runner-auth-test");
        std::fs::create_dir_all(temp.join("manifests")).expect("manifests dir");
        let config = Config {
            data_dir: temp.display().to_string(),
            manifests_dir: temp.join("manifests").display().to_string(),
            server: ServerConfig {
                listen: "127.0.0.1:0".to_string(),
            },
            auth: AuthConfig {
                mode: "bearer".to_string(),
                tokens: vec![],
            },
            artifacts: ArtifactsConfig {
                max_size_mb: 1,
                ttl_seconds: 3600,
                cleanup_interval_seconds: 600,
                require_checksum_on_upload: true,
            },
            jobs: JobsConfig {
                default_log_limit_mb: 50,
                max_request_body_kb: 64,
                cleanup_successful_workdirs: true,
                keep_failed_workdirs: true,
            },
        };

        AppState {
            config: Arc::new(config.clone()),
            auth: Arc::new(
                AuthStore::load_from_config(
                    &AuthConfig {
                        mode: "bearer".to_string(),
                        tokens: vec![
                            crate::config::AuthTokenConfig {
                                name: "runner".to_string(),
                                token_env: "TOKEN_JOBS_RUN".to_string(),
                                permissions: vec!["jobs:run".to_string()],
                            },
                            crate::config::AuthTokenConfig {
                                name: "reader".to_string(),
                                token_env: "TOKEN_ARTIFACTS_READ".to_string(),
                                permissions: vec!["artifacts:read".to_string()],
                            },
                        ],
                    },
                    |name| match name {
                        "TOKEN_JOBS_RUN" => Some("jobs-run-token".to_string()),
                        "TOKEN_ARTIFACTS_READ" => Some("artifacts-read-token".to_string()),
                        _ => None,
                    },
                )
                .expect("auth should load"),
            ),
            manifests: Arc::new(
                ManifestStore::load_from_dir(&config.manifests_dir).expect("manifests should load"),
            ),
            artifacts: Arc::new(
                ArtifactStore::new(
                    &config.data_dir,
                    config.artifacts.ttl_seconds,
                    config.artifacts.max_size_mb,
                    config.artifacts.require_checksum_on_upload,
                )
                .expect("artifact store should init"),
            ),
            jobs: Arc::new(JobStore::new(&config.data_dir).expect("job store should init")),
            runtime_status: Arc::new(crate::RuntimeStatus::new(0, 0)),
        }
    }
}
