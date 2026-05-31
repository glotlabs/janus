use std::{
    fmt, fs,
    path::{Path, PathBuf},
    time::{Duration, SystemTime},
};

use axum::{
    Json,
    body::{Body, Bytes},
    extract::{Path as AxumPath, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::time::Duration as StdDuration;
use strait_lib::{ArtifactUploadResponse, HEADER_SHA256};
use tracing::warn;
use uuid::Uuid;

use crate::{
    auth::{ArtifactsRead, ArtifactsWrite, Authorized},
    storage::atomic_write,
};

#[derive(Debug, Clone)]
pub struct ArtifactStore {
    root_dir: PathBuf,
    ttl_seconds: u64,
    max_size_bytes: usize,
    require_checksum_on_upload: bool,
}

impl ArtifactStore {
    pub fn new(
        data_dir: impl AsRef<Path>,
        ttl_seconds: u64,
        max_size_mb: u64,
        require_checksum_on_upload: bool,
    ) -> Result<Self, ArtifactError> {
        let root_dir = data_dir.as_ref().join("artifacts");
        fs::create_dir_all(&root_dir).map_err(|source| ArtifactError::CreateDir {
            path: root_dir.display().to_string(),
            source,
        })?;

        let bytes_per_mb = 1024_u64 * 1024_u64;
        let max_size_bytes = max_size_mb
            .checked_mul(bytes_per_mb)
            .and_then(|value| usize::try_from(value).ok())
            .ok_or(ArtifactError::InvalidMaxSize { max_size_mb })?;

        Ok(Self {
            root_dir,
            ttl_seconds,
            max_size_bytes,
            require_checksum_on_upload,
        })
    }

    pub fn max_upload_bytes(&self) -> usize {
        self.max_size_bytes
    }

    pub fn require_checksum_on_upload(&self) -> bool {
        self.require_checksum_on_upload
    }

    pub fn store_bytes(
        &self,
        bytes: &[u8],
        checksum_header: Option<&str>,
    ) -> Result<ArtifactMetadata, ArtifactError> {
        if self.require_checksum_on_upload && checksum_header.is_none() {
            return Err(ArtifactError::MissingChecksum);
        }

        if bytes.len() > self.max_size_bytes {
            return Err(ArtifactError::TooLarge {
                size: bytes.len(),
                max: self.max_size_bytes,
            });
        }

        let sha256 = hex::encode(Sha256::digest(bytes));

        if let Some(expected) = checksum_header
            && !expected.eq_ignore_ascii_case(&sha256)
        {
            return Err(ArtifactError::ChecksumMismatch {
                expected: expected.to_string(),
                actual: sha256,
            });
        }

        let artifact_id = format!("art_{}", Uuid::now_v7().simple());
        let expires_at = iso8601_now_plus(self.ttl_seconds)?;
        let metadata = ArtifactMetadata {
            artifact_id: artifact_id.clone(),
            sha256,
            size: bytes.len() as u64,
            expires_at,
        };

        let artifact_dir = self.root_dir.join(&artifact_id);
        fs::create_dir_all(&artifact_dir).map_err(|source| ArtifactError::CreateDir {
            path: artifact_dir.display().to_string(),
            source,
        })?;

        let blob_path = artifact_dir.join("blob");
        atomic_write(&blob_path, bytes).map_err(|source| ArtifactError::WriteFile {
            path: blob_path.display().to_string(),
            source,
        })?;

        let metadata_json = serde_json::to_vec_pretty(&metadata)
            .map_err(|source| ArtifactError::SerializeMetadata { source })?;
        let metadata_path = artifact_dir.join("metadata.json");
        atomic_write(&metadata_path, &metadata_json).map_err(|source| {
            ArtifactError::WriteFile {
                path: metadata_path.display().to_string(),
                source,
            }
        })?;

        Ok(metadata)
    }

    pub fn store_file(&self, path: &Path) -> Result<ArtifactMetadata, ArtifactError> {
        let bytes = fs::read(path).map_err(|source| ArtifactError::ReadFile {
            path: path.display().to_string(),
            source,
        })?;
        let checksum = hex::encode(Sha256::digest(&bytes));

        self.store_bytes(&bytes, Some(&checksum))
    }

    pub fn load(&self, artifact_id: &str) -> Result<StoredArtifact, ArtifactError> {
        validate_artifact_id(artifact_id)?;
        let artifact_dir = self.root_dir.join(artifact_id);
        let blob_path = artifact_dir.join("blob");

        let metadata = self.load_metadata(artifact_id)?;
        if metadata.is_expired()? {
            return Err(ArtifactError::NotFound(artifact_id.to_string()));
        }
        let bytes =
            fs::read(&blob_path).map_err(|_| ArtifactError::NotFound(artifact_id.to_string()))?;

        Ok(StoredArtifact { metadata, bytes })
    }

    pub fn load_metadata(&self, artifact_id: &str) -> Result<ArtifactMetadata, ArtifactError> {
        validate_artifact_id(artifact_id)?;
        let artifact_dir = self.root_dir.join(artifact_id);
        let metadata_path = artifact_dir.join("metadata.json");
        let metadata_raw = fs::read(&metadata_path)
            .map_err(|_| ArtifactError::NotFound(artifact_id.to_string()))?;

        serde_json::from_slice(&metadata_raw).map_err(|source| ArtifactError::ParseMetadata {
            path: metadata_path.display().to_string(),
            source,
        })
    }

    pub fn artifact_blob_path(&self, artifact_id: &str) -> PathBuf {
        self.root_dir.join(artifact_id).join("blob")
    }

    pub fn recover_incomplete_artifacts(&self) -> Result<usize, ArtifactError> {
        let mut removed = 0;
        let entries = fs::read_dir(&self.root_dir).map_err(|source| ArtifactError::ReadDir {
            path: self.root_dir.display().to_string(),
            source,
        })?;

        for entry in entries {
            let entry = entry.map_err(|source| ArtifactError::ReadDir {
                path: self.root_dir.display().to_string(),
                source,
            })?;
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }

            if self.artifact_dir_is_recoverable(&path)? {
                fs::remove_dir_all(&path).map_err(|source| ArtifactError::RemoveDir {
                    path: path.display().to_string(),
                    source,
                })?;
                removed += 1;
            }
        }

        Ok(removed)
    }

    pub fn cleanup_expired(&self) -> Result<usize, ArtifactError> {
        let mut removed = 0;
        let entries = fs::read_dir(&self.root_dir).map_err(|source| ArtifactError::ReadDir {
            path: self.root_dir.display().to_string(),
            source,
        })?;

        for entry in entries {
            let entry = entry.map_err(|source| ArtifactError::ReadDir {
                path: self.root_dir.display().to_string(),
                source,
            })?;
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }

            let artifact_id = entry.file_name().to_string_lossy().to_string();
            let metadata = match self.load_metadata(&artifact_id) {
                Ok(metadata) => metadata,
                Err(ArtifactError::NotFound(_)) => continue,
                Err(error) => return Err(error),
            };

            if metadata.is_expired()? {
                fs::remove_dir_all(&path).map_err(|source| ArtifactError::RemoveDir {
                    path: path.display().to_string(),
                    source,
                })?;
                removed += 1;
            }
        }

        Ok(removed)
    }
    fn artifact_dir_is_recoverable(&self, path: &Path) -> Result<bool, ArtifactError> {
        let artifact_id = path
            .file_name()
            .expect("artifact dir should have a name")
            .to_string_lossy()
            .to_string();
        let metadata_path = path.join("metadata.json");
        let blob_path = path.join("blob");

        let metadata_bytes = match fs::read(&metadata_path) {
            Ok(bytes) => bytes,
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(true),
            Err(source) => {
                return Err(ArtifactError::ReadFile {
                    path: metadata_path.display().to_string(),
                    source,
                });
            }
        };
        let metadata = match serde_json::from_slice::<ArtifactMetadata>(&metadata_bytes) {
            Ok(metadata) => metadata,
            Err(_) => return Ok(true),
        };

        if metadata.artifact_id != artifact_id {
            return Ok(true);
        }

        let blob = match fs::read(&blob_path) {
            Ok(bytes) => bytes,
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(true),
            Err(source) => {
                return Err(ArtifactError::ReadFile {
                    path: blob_path.display().to_string(),
                    source,
                });
            }
        };

        let actual_sha256 = hex::encode(Sha256::digest(&blob));
        Ok(metadata.size != blob.len() as u64 || metadata.sha256 != actual_sha256)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArtifactMetadata {
    pub artifact_id: String,
    pub sha256: String,
    pub size: u64,
    pub expires_at: String,
}

impl ArtifactMetadata {
    pub fn is_expired(&self) -> Result<bool, ArtifactError> {
        let expires_at = DateTime::parse_from_rfc3339(&self.expires_at).map_err(|source| {
            ArtifactError::ParseExpiry {
                expires_at: self.expires_at.clone(),
                source,
            }
        })?;

        Ok(expires_at.with_timezone(&Utc) <= Utc::now())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredArtifact {
    pub metadata: ArtifactMetadata,
    pub bytes: Vec<u8>,
}

#[derive(Debug)]
pub enum ArtifactError {
    InvalidMaxSize {
        max_size_mb: u64,
    },
    CreateDir {
        path: String,
        source: std::io::Error,
    },
    ReadDir {
        path: String,
        source: std::io::Error,
    },
    RemoveDir {
        path: String,
        source: std::io::Error,
    },
    WriteFile {
        path: String,
        source: std::io::Error,
    },
    ReadFile {
        path: String,
        source: std::io::Error,
    },
    SerializeMetadata {
        source: serde_json::Error,
    },
    ParseMetadata {
        path: String,
        source: serde_json::Error,
    },
    ParseExpiry {
        expires_at: String,
        source: chrono::ParseError,
    },
    MissingChecksum,
    ChecksumMismatch {
        expected: String,
        actual: String,
    },
    TooLarge {
        size: usize,
        max: usize,
    },
    RateLimitExceeded {
        token_name: String,
        limit: u32,
        window_seconds: u64,
    },
    NotFound(String),
    InvalidArtifactId(String),
    InvalidBody(axum::Error),
    InvalidHeader {
        name: &'static str,
        message: String,
    },
    TimeOverflow,
}

impl fmt::Display for ArtifactError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidMaxSize { max_size_mb } => {
                write!(f, "invalid artifact max size in mb: {max_size_mb}")
            }
            Self::CreateDir { path, source } => {
                write!(f, "failed to create artifact directory {path}: {source}")
            }
            Self::ReadDir { path, source } => {
                write!(f, "failed to read artifact directory {path}: {source}")
            }
            Self::RemoveDir { path, source } => {
                write!(f, "failed to remove artifact directory {path}: {source}")
            }
            Self::WriteFile { path, source } => {
                write!(f, "failed to write artifact file {path}: {source}")
            }
            Self::ReadFile { path, source } => {
                write!(f, "failed to read artifact file {path}: {source}")
            }
            Self::SerializeMetadata { source } => {
                write!(f, "failed to serialize artifact metadata: {source}")
            }
            Self::ParseMetadata { path, source } => {
                write!(f, "failed to parse artifact metadata {path}: {source}")
            }
            Self::ParseExpiry { expires_at, source } => {
                write!(f, "failed to parse artifact expiry {expires_at}: {source}")
            }
            Self::MissingChecksum => write!(f, "missing required {HEADER_SHA256} header"),
            Self::ChecksumMismatch { expected, actual } => {
                write!(
                    f,
                    "artifact checksum mismatch: expected {expected}, got {actual}"
                )
            }
            Self::TooLarge { size, max } => {
                write!(
                    f,
                    "artifact exceeds size limit: got {size} bytes, max {max} bytes"
                )
            }
            Self::RateLimitExceeded {
                token_name,
                limit,
                window_seconds,
            } => {
                write!(
                    f,
                    "token {token_name} exceeded artifact upload rate limit of {limit} requests per {window_seconds} seconds"
                )
            }
            Self::NotFound(artifact_id) => write!(f, "artifact not found: {artifact_id}"),
            Self::InvalidArtifactId(artifact_id) => write!(f, "invalid artifact id: {artifact_id}"),
            Self::InvalidBody(source) => write!(f, "failed to read request body: {source}"),
            Self::InvalidHeader { name, message } => {
                write!(f, "invalid header {name}: {message}")
            }
            Self::TimeOverflow => write!(f, "artifact expiration time overflow"),
        }
    }
}

impl std::error::Error for ArtifactError {}

impl IntoResponse for ArtifactError {
    fn into_response(self) -> Response {
        let retry_after = match &self {
            Self::RateLimitExceeded {
                token_name,
                limit,
                window_seconds,
            } => {
                warn!(
                    token_name = %token_name,
                    limit,
                    window_seconds,
                    reason = "artifact_rate_limit_exceeded",
                    "request rejected"
                );
                Some(*window_seconds)
            }
            _ => None,
        };

        let status = match self {
            Self::MissingChecksum
            | Self::ChecksumMismatch { .. }
            | Self::TooLarge { .. }
            | Self::InvalidArtifactId(_)
            | Self::InvalidBody(_)
            | Self::InvalidHeader { .. } => StatusCode::BAD_REQUEST,
            Self::RateLimitExceeded { .. } => StatusCode::TOO_MANY_REQUESTS,
            Self::NotFound(_) => StatusCode::NOT_FOUND,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };

        let payload = Json(ErrorResponse {
            code: artifact_error_code(&self),
            message: self.to_string(),
        });

        let mut response = (status, payload).into_response();
        if let Some(retry_after) = retry_after {
            response.headers_mut().insert(
                header::RETRY_AFTER,
                HeaderValue::from_str(&retry_after.to_string())
                    .expect("retry-after header should be valid"),
            );
        }

        response
    }
}

#[derive(Serialize)]
struct ErrorResponse {
    code: &'static str,
    message: String,
}

fn artifact_error_code(error: &ArtifactError) -> &'static str {
    match error {
        ArtifactError::InvalidMaxSize { .. } => "artifact_invalid_max_size",
        ArtifactError::CreateDir { .. } => "artifact_create_dir_failed",
        ArtifactError::ReadDir { .. } => "artifact_read_dir_failed",
        ArtifactError::RemoveDir { .. } => "artifact_remove_dir_failed",
        ArtifactError::WriteFile { .. } => "artifact_write_failed",
        ArtifactError::ReadFile { .. } => "artifact_read_failed",
        ArtifactError::SerializeMetadata { .. } => "artifact_metadata_serialize_failed",
        ArtifactError::ParseMetadata { .. } => "artifact_metadata_parse_failed",
        ArtifactError::ParseExpiry { .. } => "artifact_expiry_parse_failed",
        ArtifactError::MissingChecksum => "artifact_missing_checksum",
        ArtifactError::ChecksumMismatch { .. } => "artifact_checksum_mismatch",
        ArtifactError::TooLarge { .. } => "artifact_too_large",
        ArtifactError::RateLimitExceeded { .. } => "artifact_rate_limit_exceeded",
        ArtifactError::NotFound(_) => "artifact_not_found",
        ArtifactError::InvalidArtifactId(_) => "artifact_invalid_id",
        ArtifactError::InvalidBody(_) => "artifact_invalid_body",
        ArtifactError::InvalidHeader { .. } => "artifact_invalid_header",
        ArtifactError::TimeOverflow => "artifact_time_overflow",
    }
}

pub async fn upload_artifact(
    auth: Authorized<ArtifactsWrite>,
    State(state): State<crate::AppState>,
    headers: HeaderMap,
    body: Body,
) -> Result<(StatusCode, Json<ArtifactUploadResponse>), ArtifactError> {
    state
        .rate_limiter
        .check(
            "artifacts_write",
            auth.token_name(),
            state.config.artifacts.max_upload_requests_per_minute,
            StdDuration::from_secs(60),
        )
        .map_err(|error| ArtifactError::RateLimitExceeded {
            token_name: auth.token_name().to_string(),
            limit: error.limit,
            window_seconds: error.window_seconds,
        })?;
    let checksum = checksum_from_headers(&headers, state.artifacts.require_checksum_on_upload())?;
    let limit = state.artifacts.max_upload_bytes();
    let bytes: Bytes = axum::body::to_bytes(body, limit)
        .await
        .map_err(ArtifactError::InvalidBody)?;

    let metadata = state
        .artifacts
        .store_bytes(bytes.as_ref(), checksum.as_deref())?;

    Ok((
        StatusCode::CREATED,
        Json(ArtifactUploadResponse {
            artifact_id: metadata.artifact_id,
            sha256: metadata.sha256,
            size: metadata.size,
            expires_at: metadata.expires_at,
        }),
    ))
}

pub async fn download_artifact(
    _: Authorized<ArtifactsRead>,
    State(state): State<crate::AppState>,
    AxumPath(artifact_id): AxumPath<String>,
) -> Result<Response, ArtifactError> {
    let artifact = state.artifacts.load(&artifact_id)?;

    let mut response = Response::new(Body::from(artifact.bytes));
    *response.status_mut() = StatusCode::OK;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    response.headers_mut().insert(
        header::HeaderName::from_static(HEADER_SHA256),
        HeaderValue::from_str(&artifact.metadata.sha256).map_err(|error| {
            ArtifactError::InvalidHeader {
                name: HEADER_SHA256,
                message: error.to_string(),
            }
        })?,
    );

    Ok(response)
}

fn checksum_from_headers(
    headers: &HeaderMap,
    checksum_required: bool,
) -> Result<Option<String>, ArtifactError> {
    match headers.get(HEADER_SHA256) {
        Some(value) => {
            let value = value
                .to_str()
                .map_err(|error| ArtifactError::InvalidHeader {
                    name: HEADER_SHA256,
                    message: error.to_string(),
                })?;
            validate_sha256_header(value)?;
            Ok(Some(value.to_string()))
        }
        None if checksum_required => Err(ArtifactError::MissingChecksum),
        None => Ok(None),
    }
}

fn validate_sha256_header(value: &str) -> Result<(), ArtifactError> {
    if value.len() != 64 {
        return Err(ArtifactError::InvalidHeader {
            name: HEADER_SHA256,
            message: "must be exactly 64 hexadecimal characters".to_string(),
        });
    }

    if !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(ArtifactError::InvalidHeader {
            name: HEADER_SHA256,
            message: "must contain only hexadecimal characters".to_string(),
        });
    }

    Ok(())
}

fn validate_artifact_id(artifact_id: &str) -> Result<(), ArtifactError> {
    validate_prefixed_hex_id(artifact_id, "art_")
        .then_some(())
        .ok_or_else(|| ArtifactError::InvalidArtifactId(artifact_id.to_string()))
}

fn validate_prefixed_hex_id(value: &str, prefix: &str) -> bool {
    let Some(suffix) = value.strip_prefix(prefix) else {
        return false;
    };

    suffix.len() == 32 && suffix.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn iso8601_now_plus(ttl_seconds: u64) -> Result<String, ArtifactError> {
    let expires_at = SystemTime::now()
        .checked_add(Duration::from_secs(ttl_seconds))
        .ok_or(ArtifactError::TimeOverflow)?;
    let timestamp: DateTime<Utc> = expires_at.into();

    Ok(timestamp.to_rfc3339_opts(SecondsFormat::Secs, true))
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::{Path, PathBuf},
        time::{SystemTime, UNIX_EPOCH},
    };

    use axum::{
        Router,
        body::Body,
        http::{Request, StatusCode},
        routing::{get, post},
    };
    use sha2::{Digest, Sha256};
    use strait_lib::{
        ArtifactUploadResponse, HEADER_SHA256, ROUTE_RUNNER_ARTIFACT_BY_ID, ROUTE_RUNNER_ARTIFACTS,
        runner_artifact_path,
    };
    use tower::util::ServiceExt;

    use super::{ArtifactStore, download_artifact, upload_artifact};
    use crate::{
        AppState, auth::AuthStore, config::Config, jobs::JobStore, manifest::ManifestStore,
    };

    #[tokio::test]
    async fn stores_and_reads_artifact() {
        let temp = temp_dir("artifact_store");
        let store = ArtifactStore::new(&temp, 3600, 1, true).expect("store should init");
        let payload = b"hello world";
        let checksum = hex::encode(Sha256::digest(payload));

        let metadata = store
            .store_bytes(payload, Some(&checksum))
            .expect("artifact should store");
        let stored = store
            .load(&metadata.artifact_id)
            .expect("artifact should load");

        assert_eq!(stored.metadata, metadata);
        assert_eq!(stored.bytes, payload);
    }

    #[tokio::test]
    async fn rejects_checksum_mismatch() {
        let temp = temp_dir("artifact_checksum");
        let store = ArtifactStore::new(&temp, 3600, 1, true).expect("store should init");

        let error = store
            .store_bytes(b"hello world", Some("deadbeef"))
            .expect_err("checksum mismatch must fail");

        assert!(matches!(
            error,
            super::ArtifactError::ChecksumMismatch { .. }
        ));
    }

    #[tokio::test]
    async fn rejects_artifacts_over_limit() {
        let temp = temp_dir("artifact_too_large");
        let store = ArtifactStore::new(&temp, 3600, 1, false).expect("store should init");
        let payload = vec![b'a'; (1024 * 1024) + 1];

        let error = store
            .store_bytes(&payload, None)
            .expect_err("oversized artifact must fail");

        assert!(matches!(error, super::ArtifactError::TooLarge { .. }));
    }

    #[tokio::test]
    async fn uploads_and_downloads_through_http() {
        let temp = temp_dir("artifact_http");
        let state = test_state(&temp);
        let app = Router::new()
            .route(ROUTE_RUNNER_ARTIFACTS, post(upload_artifact))
            .route(ROUTE_RUNNER_ARTIFACT_BY_ID, get(download_artifact))
            .with_state(state);
        let payload = b"artifact-body";
        let checksum = hex::encode(Sha256::digest(payload));

        let upload = app
            .clone()
            .oneshot(
                Request::post(ROUTE_RUNNER_ARTIFACTS)
                    .header("authorization", "Bearer artifacts-write-token")
                    .header("content-type", "application/octet-stream")
                    .header(HEADER_SHA256, checksum.as_str())
                    .body(Body::from(payload.to_vec()))
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");

        assert_eq!(upload.status(), StatusCode::CREATED);
        let body = axum::body::to_bytes(upload.into_body(), usize::MAX)
            .await
            .expect("upload body");
        let metadata: ArtifactUploadResponse =
            serde_json::from_slice(&body).expect("shared upload response should deserialize");

        let download = app
            .oneshot(
                Request::get(runner_artifact_path(&metadata.artifact_id))
                    .header("authorization", "Bearer artifacts-read-token")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");

        assert_eq!(download.status(), StatusCode::OK);
        let body = axum::body::to_bytes(download.into_body(), usize::MAX)
            .await
            .expect("download body");
        assert_eq!(body.as_ref(), payload);
    }

    #[tokio::test]
    async fn rejects_download_of_expired_artifact() {
        let temp = temp_dir("artifact_http_expired_download");
        let state = test_state(&temp);
        let app = Router::new()
            .route("/artifacts", post(upload_artifact))
            .route("/artifacts/{artifact_id}", get(download_artifact))
            .with_state(state.clone());
        let payload = b"artifact-body";
        let checksum = hex::encode(Sha256::digest(payload));

        let upload = app
            .clone()
            .oneshot(
                Request::post("/artifacts")
                    .header("authorization", "Bearer artifacts-write-token")
                    .header("content-type", "application/octet-stream")
                    .header("x-sha256", checksum.as_str())
                    .body(Body::from(payload.to_vec()))
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");

        assert_eq!(upload.status(), StatusCode::CREATED);
        let body = axum::body::to_bytes(upload.into_body(), usize::MAX)
            .await
            .expect("upload body");
        let mut metadata: super::ArtifactMetadata =
            serde_json::from_slice(&body).expect("metadata should deserialize");
        metadata.expires_at = "1970-01-01T00:00:00Z".to_string();
        let metadata_path = temp
            .join("artifacts")
            .join(&metadata.artifact_id)
            .join("metadata.json");
        let metadata_json = serde_json::to_vec_pretty(&metadata).expect("metadata json");
        crate::storage::atomic_write(&metadata_path, &metadata_json).expect("metadata rewrite");

        let download = app
            .oneshot(
                Request::get(format!("/artifacts/{}", metadata.artifact_id))
                    .header("authorization", "Bearer artifacts-read-token")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");

        assert_eq!(download.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn rejects_checksum_header_with_invalid_length() {
        let temp = temp_dir("artifact_http_bad_checksum_length");
        let state = test_state(&temp);
        let app = Router::new()
            .route("/artifacts", post(upload_artifact))
            .with_state(state);

        let response = app
            .oneshot(
                Request::post("/artifacts")
                    .header("authorization", "Bearer artifacts-write-token")
                    .header("content-type", "application/octet-stream")
                    .header("x-sha256", "deadbeef")
                    .body(Body::from(b"artifact-body".to_vec()))
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn rejects_checksum_header_with_non_hex_characters() {
        let temp = temp_dir("artifact_http_bad_checksum_chars");
        let state = test_state(&temp);
        let app = Router::new()
            .route("/artifacts", post(upload_artifact))
            .with_state(state);

        let response = app
            .oneshot(
                Request::post("/artifacts")
                    .header("authorization", "Bearer artifacts-write-token")
                    .header("content-type", "application/octet-stream")
                    .header(
                        "x-sha256",
                        "gggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggg",
                    )
                    .body(Body::from(b"artifact-body".to_vec()))
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn rate_limits_artifact_uploads_per_token() {
        let temp = temp_dir("artifact_http_rate_limit");
        let state = test_state_with_upload_rate_limit(&temp, 1);
        let app = Router::new()
            .route("/artifacts", post(upload_artifact))
            .with_state(state);
        let payload = b"artifact-body";
        let checksum = hex::encode(Sha256::digest(payload));

        let first = app
            .clone()
            .oneshot(
                Request::post("/artifacts")
                    .header("authorization", "Bearer artifacts-write-token")
                    .header("content-type", "application/octet-stream")
                    .header("x-sha256", checksum.as_str())
                    .body(Body::from(payload.to_vec()))
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");

        let second = app
            .oneshot(
                Request::post("/artifacts")
                    .header("authorization", "Bearer artifacts-write-token")
                    .header("content-type", "application/octet-stream")
                    .header("x-sha256", checksum.as_str())
                    .body(Body::from(payload.to_vec()))
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");

        assert_eq!(first.status(), StatusCode::CREATED);
        assert_eq!(second.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(
            second
                .headers()
                .get("retry-after")
                .and_then(|value| value.to_str().ok()),
            Some("60")
        );
    }

    #[tokio::test]
    async fn rejects_invalid_artifact_id_over_http() {
        let temp = temp_dir("artifact_http_invalid_id");
        let state = test_state(&temp);
        let app = Router::new()
            .route("/artifacts", post(upload_artifact))
            .route("/artifacts/{artifact_id}", get(download_artifact))
            .with_state(state);

        let response = app
            .oneshot(
                Request::get("/artifacts/not-an-artifact-id")
                    .header("authorization", "Bearer artifacts-read-token")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn cleanup_removes_expired_artifacts() {
        let temp = temp_dir("artifact_cleanup_expired");
        let store = ArtifactStore::new(&temp, 0, 1, false).expect("store should init");
        let metadata = store
            .store_bytes(b"expired", None)
            .expect("artifact should store");

        let removed = store.cleanup_expired().expect("cleanup should succeed");

        assert_eq!(removed, 1);
        assert!(!temp.join("artifacts").join(metadata.artifact_id).exists());
    }

    #[tokio::test]
    async fn cleanup_keeps_unexpired_artifacts() {
        let temp = temp_dir("artifact_cleanup_keep");
        let store = ArtifactStore::new(&temp, 3600, 1, false).expect("store should init");
        let metadata = store
            .store_bytes(b"alive", None)
            .expect("artifact should store");

        let removed = store.cleanup_expired().expect("cleanup should succeed");

        assert_eq!(removed, 0);
        assert!(temp.join("artifacts").join(metadata.artifact_id).exists());
    }

    #[test]
    fn recovers_artifact_dir_missing_metadata() {
        let temp = temp_dir("artifact_recovery_missing_metadata");
        let artifact_dir = temp.join("artifacts").join("art_incomplete");
        fs::create_dir_all(&artifact_dir).expect("artifact dir should exist");
        fs::write(artifact_dir.join("blob"), b"partial").expect("blob should exist");

        let store = ArtifactStore::new(&temp, 3600, 1, false).expect("store should init");
        let removed = store
            .recover_incomplete_artifacts()
            .expect("recovery should succeed");

        assert_eq!(removed, 1);
        assert!(!artifact_dir.exists());
    }

    #[test]
    fn recovers_artifact_dir_with_mismatched_blob() {
        let temp = temp_dir("artifact_recovery_mismatched_blob");
        let artifact_dir = temp.join("artifacts").join("art_broken");
        fs::create_dir_all(&artifact_dir).expect("artifact dir should exist");
        fs::write(artifact_dir.join("blob"), b"wrong-bytes").expect("blob should exist");
        fs::write(
            artifact_dir.join("metadata.json"),
            serde_json::to_vec_pretty(&super::ArtifactMetadata {
                artifact_id: "art_broken".to_string(),
                sha256: hex::encode(Sha256::digest(b"expected-bytes")),
                size: "expected-bytes".len() as u64,
                expires_at: "2030-01-01T00:00:00Z".to_string(),
            })
            .expect("metadata should serialize"),
        )
        .expect("metadata should exist");

        let store = ArtifactStore::new(&temp, 3600, 1, false).expect("store should init");
        let removed = store
            .recover_incomplete_artifacts()
            .expect("recovery should succeed");

        assert_eq!(removed, 1);
        assert!(!artifact_dir.exists());
    }

    fn test_state(temp: &Path) -> AppState {
        let manifests_dir = temp.join("manifests");
        fs::create_dir_all(&manifests_dir).expect("manifests dir should be created");
        let config = Config {
            data_dir: temp.display().to_string(),
            manifests_dir: manifests_dir.display().to_string(),
            server: crate::config::ServerConfig {
                listen: "127.0.0.1:0".to_string(),
            },
            auth: crate::config::AuthConfig {
                mode: "bearer".to_string(),
                tokens: Vec::new(),
            },
            artifacts: crate::config::ArtifactsConfig {
                max_size_mb: 1,
                ttl_seconds: 3600,
                cleanup_interval_seconds: 600,
                require_checksum_on_upload: true,
                max_upload_requests_per_minute: 60,
            },
            jobs: crate::config::JobsConfig {
                default_log_limit_mb: 50,
                max_request_body_kb: 64,
                max_run_requests_per_minute: 60,
                cleanup_successful_workdirs: true,
                keep_failed_workdirs: true,
            },
        };

        AppState {
            config: std::sync::Arc::new(config.clone()),
            auth: std::sync::Arc::new(
                AuthStore::load_from_config(
                    &crate::config::AuthConfig {
                        mode: "bearer".to_string(),
                        tokens: vec![
                            crate::config::AuthTokenConfig {
                                name: "writer".to_string(),
                                token_env: "TOKEN_ARTIFACTS_WRITE".to_string(),
                                permissions: vec!["artifacts:write".to_string()],
                            },
                            crate::config::AuthTokenConfig {
                                name: "reader".to_string(),
                                token_env: "TOKEN_ARTIFACTS_READ".to_string(),
                                permissions: vec!["artifacts:read".to_string()],
                            },
                        ],
                    },
                    |name| match name {
                        "TOKEN_ARTIFACTS_WRITE" => Some("artifacts-write-token".to_string()),
                        "TOKEN_ARTIFACTS_READ" => Some("artifacts-read-token".to_string()),
                        _ => None,
                    },
                )
                .expect("auth should load"),
            ),
            manifests: std::sync::Arc::new(
                ManifestStore::load_from_dir(&config.manifests_dir).expect("manifests should load"),
            ),
            artifacts: std::sync::Arc::new(
                ArtifactStore::new(
                    &config.data_dir,
                    config.artifacts.ttl_seconds,
                    config.artifacts.max_size_mb,
                    config.artifacts.require_checksum_on_upload,
                )
                .expect("artifact store should init"),
            ),
            jobs: std::sync::Arc::new(
                JobStore::new(&config.data_dir).expect("job store should init"),
            ),
            rate_limiter: std::sync::Arc::new(crate::rate_limit::RateLimiter::new()),
            runtime_status: std::sync::Arc::new(crate::RuntimeStatus::new(0, 0)),
        }
    }

    fn test_state_with_upload_rate_limit(
        temp: &Path,
        max_upload_requests_per_minute: u32,
    ) -> AppState {
        let manifests_dir = temp.join("manifests");
        fs::create_dir_all(&manifests_dir).expect("manifests dir should be created");
        let config = Config {
            data_dir: temp.display().to_string(),
            manifests_dir: manifests_dir.display().to_string(),
            server: crate::config::ServerConfig {
                listen: "127.0.0.1:0".to_string(),
            },
            auth: crate::config::AuthConfig {
                mode: "bearer".to_string(),
                tokens: Vec::new(),
            },
            artifacts: crate::config::ArtifactsConfig {
                max_size_mb: 1,
                ttl_seconds: 3600,
                cleanup_interval_seconds: 600,
                require_checksum_on_upload: true,
                max_upload_requests_per_minute,
            },
            jobs: crate::config::JobsConfig {
                default_log_limit_mb: 50,
                max_request_body_kb: 64,
                max_run_requests_per_minute: 60,
                cleanup_successful_workdirs: true,
                keep_failed_workdirs: true,
            },
        };

        AppState {
            config: std::sync::Arc::new(config.clone()),
            auth: std::sync::Arc::new(
                AuthStore::load_from_config(
                    &crate::config::AuthConfig {
                        mode: "bearer".to_string(),
                        tokens: vec![
                            crate::config::AuthTokenConfig {
                                name: "writer".to_string(),
                                token_env: "TOKEN_ARTIFACTS_WRITE".to_string(),
                                permissions: vec!["artifacts:write".to_string()],
                            },
                            crate::config::AuthTokenConfig {
                                name: "reader".to_string(),
                                token_env: "TOKEN_ARTIFACTS_READ".to_string(),
                                permissions: vec!["artifacts:read".to_string()],
                            },
                        ],
                    },
                    |name| match name {
                        "TOKEN_ARTIFACTS_WRITE" => Some("artifacts-write-token".to_string()),
                        "TOKEN_ARTIFACTS_READ" => Some("artifacts-read-token".to_string()),
                        _ => None,
                    },
                )
                .expect("auth should load"),
            ),
            manifests: std::sync::Arc::new(
                ManifestStore::load_from_dir(&config.manifests_dir).expect("manifests should load"),
            ),
            artifacts: std::sync::Arc::new(
                ArtifactStore::new(
                    &config.data_dir,
                    config.artifacts.ttl_seconds,
                    config.artifacts.max_size_mb,
                    config.artifacts.require_checksum_on_upload,
                )
                .expect("artifact store should init"),
            ),
            jobs: std::sync::Arc::new(
                JobStore::new(&config.data_dir).expect("job store should init"),
            ),
            rate_limiter: std::sync::Arc::new(crate::rate_limit::RateLimiter::new()),
            runtime_status: std::sync::Arc::new(crate::RuntimeStatus::new(0, 0)),
        }
    }

    fn temp_dir(label: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time should work")
            .as_nanos();
        let path = std::env::temp_dir().join(format!("strait-runner-{label}-{unique}"));
        fs::create_dir_all(&path).expect("temp dir should be created");
        path
    }
}
