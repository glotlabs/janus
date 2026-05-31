use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    marker::PhantomData,
    sync::{Arc, Mutex},
};

use axum::{
    Json,
    body::{Body, Bytes, to_bytes},
    extract::{FromRef, FromRequestParts, State},
    http::{Request, StatusCode, request::Parts},
    middleware::Next,
    response::{IntoResponse, Response},
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use chrono::{DateTime, Utc};
use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use serde::Serialize;
use strait_lib::{
    HEADER_SIGNATURE, HEADER_SIGNATURE_CONTENT_SHA256, HEADER_SIGNATURE_KEY_ID,
    HEADER_SIGNATURE_NONCE, HEADER_SIGNATURE_TIMESTAMP, SIGNATURE_ALGORITHM_ED25519,
    canonical_signed_request, sha256_hex,
};
use tracing::{debug, warn};

use crate::config::AuthConfig;

const SIGNATURE_TOLERANCE_SECONDS: i64 = 300;
const NONCE_RETENTION_SECONDS: i64 = SIGNATURE_TOLERANCE_SECONDS * 2;

#[derive(Debug)]
pub struct AuthStore {
    keys: BTreeMap<String, PublicKeyRecord>,
    seen_nonces: Mutex<BTreeMap<String, DateTime<Utc>>>,
    #[cfg(test)]
    test_servers: BTreeMap<String, AuthContext>,
}

impl AuthStore {
    pub fn load_from_config(config: &AuthConfig) -> Result<Self, AuthError> {
        if config.mode != "signed" {
            return Err(AuthError::UnsupportedMode(config.mode.clone()));
        }
        if config.servers.is_empty() {
            return Err(AuthError::NoTrustedServers);
        }

        let mut keys = BTreeMap::new();
        for server in &config.servers {
            let decoded = STANDARD
                .decode(server.public_key.trim())
                .map_err(|_| AuthError::InvalidPublicKey(server.key_id.clone()))?;
            let public_key_bytes: [u8; 32] = decoded
                .try_into()
                .map_err(|_| AuthError::InvalidPublicKey(server.key_id.clone()))?;
            let verifying_key = VerifyingKey::from_bytes(&public_key_bytes)
                .map_err(|_| AuthError::InvalidPublicKey(server.key_id.clone()))?;

            keys.insert(
                server.key_id.clone(),
                PublicKeyRecord {
                    name: server.name.clone(),
                    verifying_key,
                    permissions: server.permissions.iter().cloned().collect(),
                },
            );
        }

        Ok(Self {
            keys,
            seen_nonces: Mutex::new(BTreeMap::new()),
            #[cfg(test)]
            test_servers: BTreeMap::new(),
        })
    }

    #[cfg(test)]
    pub(crate) fn test_with_signed_servers(records: &[(&str, &str, &[&str])]) -> Self {
        let test_servers = records
            .iter()
            .map(|(key_id, name, permissions)| {
                (
                    (*key_id).to_string(),
                    AuthContext {
                        key_id: (*key_id).to_string(),
                        server_name: (*name).to_string(),
                        permissions: permissions
                            .iter()
                            .map(|value| (*value).to_string())
                            .collect(),
                    },
                )
            })
            .collect();

        Self {
            keys: BTreeMap::new(),
            seen_nonces: Mutex::new(BTreeMap::new()),
            test_servers,
        }
    }

    fn verify_request(
        &self,
        method: &str,
        path_and_query: &str,
        headers: &axum::http::HeaderMap,
        body: &[u8],
    ) -> Result<AuthContext, AuthRejection> {
        let key_id = required_header(headers, HEADER_SIGNATURE_KEY_ID)?;
        let timestamp = required_header(headers, HEADER_SIGNATURE_TIMESTAMP)?;
        let nonce = required_header(headers, HEADER_SIGNATURE_NONCE)?;
        let content_sha256 = required_header(headers, HEADER_SIGNATURE_CONTENT_SHA256)?;
        let signature = required_header(headers, HEADER_SIGNATURE)?;

        let record = self.keys.get(key_id).ok_or(AuthRejection::InvalidKeyId)?;
        verify_timestamp(timestamp)?;
        self.verify_nonce(key_id, nonce)?;

        let actual_sha256 = sha256_hex(body);
        if !content_sha256.eq_ignore_ascii_case(&actual_sha256) {
            return Err(AuthRejection::ContentShaMismatch);
        }

        let signature = parse_signature(signature)?;
        let canonical =
            canonical_signed_request(method, path_and_query, content_sha256, timestamp, nonce);
        record
            .verifying_key
            .verify(canonical.as_bytes(), &signature)
            .map_err(|_| AuthRejection::InvalidSignature)?;

        Ok(AuthContext {
            key_id: key_id.to_string(),
            server_name: record.name.clone(),
            permissions: record.permissions.clone(),
        })
    }

    fn verify_nonce(&self, key_id: &str, nonce: &str) -> Result<(), AuthRejection> {
        if nonce.trim().is_empty() || nonce.len() > 128 {
            return Err(AuthRejection::InvalidNonce);
        }

        let now = Utc::now();
        let cutoff = now - chrono::Duration::seconds(NONCE_RETENTION_SECONDS);
        let replay_key = format!("{key_id}:{nonce}");
        let mut seen = self.seen_nonces.lock().expect("nonce mutex poisoned");
        seen.retain(|_, seen_at| *seen_at >= cutoff);
        if seen.contains_key(&replay_key) {
            return Err(AuthRejection::Replay);
        }
        seen.insert(replay_key, now);
        Ok(())
    }

    #[cfg(test)]
    fn test_signed_context(
        &self,
        headers: &axum::http::HeaderMap,
    ) -> Result<Option<AuthContext>, AuthRejection> {
        let Some(value) = headers
            .get(HEADER_SIGNATURE_KEY_ID)
            .and_then(|value| value.to_str().ok())
        else {
            return Ok(None);
        };
        let context = self
            .test_servers
            .get(value)
            .cloned()
            .ok_or(AuthRejection::InvalidKeyId)?;
        Ok(Some(context))
    }
}

#[derive(Debug)]
struct PublicKeyRecord {
    name: String,
    verifying_key: VerifyingKey,
    permissions: BTreeSet<String>,
}

#[derive(Debug)]
pub enum AuthError {
    UnsupportedMode(String),
    NoTrustedServers,
    InvalidPublicKey(String),
}

impl fmt::Display for AuthError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedMode(mode) => write!(f, "unsupported auth mode: {mode}"),
            Self::NoTrustedServers => write!(f, "at least one trusted server key is required"),
            Self::InvalidPublicKey(key_id) => write!(f, "invalid public key for {key_id}"),
        }
    }
}

impl std::error::Error for AuthError {}

#[derive(Debug, Clone)]
struct AuthContext {
    key_id: String,
    server_name: String,
    permissions: BTreeSet<String>,
}

pub async fn verify_signed_request(
    State(state): State<crate::AppState>,
    mut request: Request<Body>,
    next: Next,
) -> Result<Response, AuthRejection> {
    let method = request.method().as_str().to_string();
    let path_and_query = request
        .uri()
        .path_and_query()
        .map(|value| value.as_str().to_string())
        .unwrap_or_else(|| request.uri().path().to_string());
    let headers = request.headers().clone();
    let max_bytes = max_signed_request_bytes(&state)?;
    let body = std::mem::replace(request.body_mut(), Body::empty());
    let bytes: Bytes = to_bytes(body, max_bytes)
        .await
        .map_err(|_| AuthRejection::RequestTooLarge)?;

    #[cfg(test)]
    let context = if let Some(context) = state.auth.test_signed_context(&headers)? {
        context
    } else {
        match state
            .auth
            .verify_request(&method, &path_and_query, &headers, &bytes)
        {
            Ok(context) => context,
            Err(error) => {
                log_auth_failure(&headers, &error);
                return Err(error);
            }
        }
    };

    #[cfg(not(test))]
    let context = match state
        .auth
        .verify_request(&method, &path_and_query, &headers, &bytes)
    {
        Ok(context) => context,
        Err(error) => {
            log_auth_failure(&headers, &error);
            return Err(error);
        }
    };
    debug!(
        key_id = %context.key_id,
        server_name = %context.server_name,
        method = %method,
        path = %path_and_query,
        "signed auth accepted"
    );
    request.extensions_mut().insert(context);
    *request.body_mut() = Body::from(bytes);

    Ok(next.run(request).await)
}

fn max_signed_request_bytes(state: &crate::AppState) -> Result<usize, AuthRejection> {
    let job_bytes = state
        .config
        .jobs
        .max_request_body_kb
        .checked_mul(1024)
        .and_then(|value| usize::try_from(value).ok())
        .ok_or(AuthRejection::InvalidBodyLimit)?;
    Ok(job_bytes.max(state.artifacts.max_upload_bytes()))
}

pub trait RequiredPermission {
    const NAME: &'static str;
}

pub struct Authorized<P> {
    server_name: String,
    _marker: PhantomData<P>,
}

impl<P> Clone for Authorized<P> {
    fn clone(&self) -> Self {
        Self {
            server_name: self.server_name.clone(),
            _marker: PhantomData,
        }
    }
}

impl<P> Authorized<P> {
    pub fn token_name(&self) -> &str {
        &self.server_name
    }
}

impl<S, P> FromRequestParts<S> for Authorized<P>
where
    P: RequiredPermission,
    Arc<AuthStore>: FromRef<S>,
    S: Send + Sync,
{
    type Rejection = AuthRejection;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let context = if let Some(context) = parts.extensions.get::<AuthContext>() {
            context.clone()
        } else {
            #[cfg(test)]
            {
                let auth = Arc::<AuthStore>::from_ref(state);
                if let Some(context) = auth.test_signed_context(&parts.headers)? {
                    context
                } else {
                    return Err(AuthRejection::MissingAuthorization);
                }
            }
            #[cfg(not(test))]
            {
                return Err(AuthRejection::MissingAuthorization);
            }
        };

        if !context.permissions.contains(P::NAME) {
            return Err(AuthRejection::MissingPermission {
                server_name: context.server_name.clone(),
                permission: P::NAME.to_string(),
            });
        }

        Ok(Self {
            server_name: context.server_name.clone(),
            _marker: PhantomData,
        })
    }
}

impl FromRef<crate::AppState> for Arc<AuthStore> {
    fn from_ref(input: &crate::AppState) -> Self {
        Arc::clone(&input.auth)
    }
}

fn log_auth_failure(headers: &axum::http::HeaderMap, error: &AuthRejection) {
    let key_id = headers
        .get(HEADER_SIGNATURE_KEY_ID)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("<missing>");
    warn!(
        key_id = %key_id,
        code = %error.code(),
        "signed auth request rejected"
    );
}

#[derive(Debug)]
pub enum AuthRejection {
    MissingAuthorization,
    InvalidAuthorizationHeader,
    InvalidKeyId,
    InvalidSignature,
    InvalidTimestamp,
    InvalidNonce,
    Replay,
    ContentShaMismatch,
    RequestTooLarge,
    InvalidBodyLimit,
    MissingPermission {
        server_name: String,
        permission: String,
    },
}

impl IntoResponse for AuthRejection {
    fn into_response(self) -> Response {
        match &self {
            Self::MissingAuthorization => {
                warn!(reason = "missing_authorization", "auth request rejected");
            }
            Self::InvalidAuthorizationHeader => {
                warn!(
                    reason = "invalid_authorization_header",
                    "auth request rejected"
                );
            }
            Self::InvalidKeyId => {
                warn!(reason = "invalid_key_id", "auth request rejected");
            }
            Self::InvalidSignature => {
                warn!(reason = "invalid_signature", "auth request rejected");
            }
            Self::InvalidTimestamp => {
                warn!(reason = "invalid_timestamp", "auth request rejected");
            }
            Self::InvalidNonce => {
                warn!(reason = "invalid_nonce", "auth request rejected");
            }
            Self::Replay => {
                warn!(reason = "replay", "auth request rejected");
            }
            Self::ContentShaMismatch => {
                warn!(reason = "content_sha_mismatch", "auth request rejected");
            }
            Self::RequestTooLarge => {
                warn!(reason = "request_too_large", "auth request rejected");
            }
            Self::InvalidBodyLimit => {
                warn!(reason = "invalid_body_limit", "auth request rejected");
            }
            Self::MissingPermission {
                server_name,
                permission,
            } => {
                warn!(
                    server_name = %server_name,
                    permission = %permission,
                    reason = "missing_permission",
                    "auth request rejected"
                );
            }
        }

        let status = match self {
            Self::MissingPermission { .. } => StatusCode::FORBIDDEN,
            Self::RequestTooLarge => StatusCode::PAYLOAD_TOO_LARGE,
            _ => StatusCode::UNAUTHORIZED,
        };

        (status, Json(AuthErrorResponse::from_rejection(&self))).into_response()
    }
}

impl fmt::Display for AuthRejection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingAuthorization => write!(f, "missing signed request authentication"),
            Self::InvalidAuthorizationHeader => write!(f, "invalid signed request authentication"),
            Self::InvalidKeyId => write!(f, "unknown signing key id"),
            Self::InvalidSignature => write!(f, "invalid request signature"),
            Self::InvalidTimestamp => write!(f, "invalid or expired request timestamp"),
            Self::InvalidNonce => write!(f, "invalid request nonce"),
            Self::Replay => write!(f, "request nonce was already used"),
            Self::ContentShaMismatch => write!(f, "request body checksum does not match"),
            Self::RequestTooLarge => write!(f, "signed request body is too large"),
            Self::InvalidBodyLimit => write!(f, "invalid signed request body limit"),
            Self::MissingPermission {
                server_name,
                permission,
            } => write!(
                f,
                "server {server_name} does not have required permission {permission}"
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
        Self {
            code: rejection.code(),
            message: rejection.to_string(),
        }
    }
}

impl AuthRejection {
    fn code(&self) -> &'static str {
        match self {
            AuthRejection::MissingAuthorization => "auth_missing_authorization",
            AuthRejection::InvalidAuthorizationHeader => "auth_invalid_authorization_header",
            AuthRejection::InvalidKeyId => "auth_invalid_key_id",
            AuthRejection::InvalidSignature => "auth_invalid_signature",
            AuthRejection::InvalidTimestamp => "auth_invalid_timestamp",
            AuthRejection::InvalidNonce => "auth_invalid_nonce",
            AuthRejection::Replay => "auth_replay",
            AuthRejection::ContentShaMismatch => "auth_content_sha_mismatch",
            AuthRejection::RequestTooLarge => "auth_request_too_large",
            AuthRejection::InvalidBodyLimit => "auth_invalid_body_limit",
            AuthRejection::MissingPermission { .. } => "auth_missing_permission",
        }
    }
}

fn required_header<'a>(
    headers: &'a axum::http::HeaderMap,
    name: &'static str,
) -> Result<&'a str, AuthRejection> {
    headers
        .get(name)
        .ok_or(AuthRejection::MissingAuthorization)?
        .to_str()
        .map_err(|_| AuthRejection::InvalidAuthorizationHeader)
}

fn verify_timestamp(value: &str) -> Result<(), AuthRejection> {
    let timestamp = DateTime::parse_from_rfc3339(value)
        .map_err(|_| AuthRejection::InvalidTimestamp)?
        .with_timezone(&Utc);
    let now = Utc::now();
    let age = now.signed_duration_since(timestamp).num_seconds().abs();
    if age > SIGNATURE_TOLERANCE_SECONDS {
        return Err(AuthRejection::InvalidTimestamp);
    }
    Ok(())
}

fn parse_signature(value: &str) -> Result<Signature, AuthRejection> {
    let Some(encoded) = value
        .strip_prefix(SIGNATURE_ALGORITHM_ED25519)
        .and_then(|rest| rest.strip_prefix(':'))
    else {
        return Err(AuthRejection::InvalidAuthorizationHeader);
    };
    let decoded = STANDARD
        .decode(encoded)
        .map_err(|_| AuthRejection::InvalidAuthorizationHeader)?;
    let bytes: [u8; 64] = decoded
        .try_into()
        .map_err(|_| AuthRejection::InvalidAuthorizationHeader)?;
    Ok(Signature::from_bytes(&bytes))
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
    use super::*;
    use axum::http::HeaderMap;
    use ed25519_dalek::{Signer, SigningKey};
    use strait_lib::{
        HEADER_SIGNATURE, HEADER_SIGNATURE_CONTENT_SHA256, HEADER_SIGNATURE_KEY_ID,
        HEADER_SIGNATURE_NONCE, HEADER_SIGNATURE_TIMESTAMP, canonical_signed_request, sha256_hex,
    };

    #[test]
    fn verifies_signed_request() {
        let signing_key = SigningKey::from_bytes(&[7_u8; 32]);
        let auth = auth_store_for_key(&signing_key);
        let body = br#"{"commit":"abc123"}"#;
        let headers = signed_headers(
            &signing_key,
            "test-key",
            "POST",
            "/jobs/build/runs",
            body,
            "nonce-1",
        );

        let context = auth
            .verify_request("POST", "/jobs/build/runs", &headers, body)
            .expect("signature should verify");

        assert_eq!(context.server_name, "test-server");
        assert!(context.permissions.contains("jobs:run"));
    }

    #[test]
    fn rejects_replayed_nonce() {
        let signing_key = SigningKey::from_bytes(&[8_u8; 32]);
        let auth = auth_store_for_key(&signing_key);
        let body = b"{}";
        let headers = signed_headers(
            &signing_key,
            "test-key",
            "POST",
            "/jobs/build/runs",
            body,
            "nonce-1",
        );

        auth.verify_request("POST", "/jobs/build/runs", &headers, body)
            .expect("first use should verify");
        let error = auth
            .verify_request("POST", "/jobs/build/runs", &headers, body)
            .expect_err("second use should be rejected");

        assert!(matches!(error, AuthRejection::Replay));
    }

    #[test]
    fn rejects_body_mismatch() {
        let signing_key = SigningKey::from_bytes(&[9_u8; 32]);
        let auth = auth_store_for_key(&signing_key);
        let headers = signed_headers(
            &signing_key,
            "test-key",
            "POST",
            "/jobs/build/runs",
            b"{}",
            "nonce-1",
        );
        let error = auth
            .verify_request("POST", "/jobs/build/runs", &headers, br#"{"changed":true}"#)
            .expect_err("changed body should be rejected");

        assert!(matches!(error, AuthRejection::ContentShaMismatch));
    }

    fn auth_store_for_key(signing_key: &SigningKey) -> AuthStore {
        AuthStore::load_from_config(&crate::config::AuthConfig {
            mode: "signed".to_string(),
            servers: vec![crate::config::AuthServerConfig {
                name: "test-server".to_string(),
                key_id: "test-key".to_string(),
                public_key: STANDARD.encode(signing_key.verifying_key().to_bytes()),
                permissions: vec!["jobs:run".to_string()],
            }],
        })
        .expect("auth should load")
    }

    fn signed_headers(
        signing_key: &SigningKey,
        key_id: &str,
        method: &str,
        path_and_query: &str,
        body: &[u8],
        nonce: &str,
    ) -> HeaderMap {
        let timestamp = Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        let content_sha256 = sha256_hex(body);
        let canonical =
            canonical_signed_request(method, path_and_query, &content_sha256, &timestamp, nonce);
        let signature = signing_key.sign(canonical.as_bytes());
        let mut headers = HeaderMap::new();
        headers.insert(HEADER_SIGNATURE_KEY_ID, key_id.parse().expect("key id"));
        headers.insert(HEADER_SIGNATURE_TIMESTAMP, timestamp.parse().expect("time"));
        headers.insert(HEADER_SIGNATURE_NONCE, nonce.parse().expect("nonce"));
        headers.insert(
            HEADER_SIGNATURE_CONTENT_SHA256,
            content_sha256.parse().expect("sha"),
        );
        headers.insert(
            HEADER_SIGNATURE,
            format!(
                "{SIGNATURE_ALGORITHM_ED25519}:{}",
                STANDARD.encode(signature.to_bytes())
            )
            .parse()
            .expect("signature"),
        );
        headers
    }
}
