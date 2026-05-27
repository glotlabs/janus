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
use uuid::Uuid;

const ARTIFACT_HEADER_SHA256: &str = "x-sha256";

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

        fs::write(artifact_dir.join("blob"), bytes).map_err(|source| ArtifactError::WriteFile {
            path: artifact_dir.join("blob").display().to_string(),
            source,
        })?;

        let metadata_json = serde_json::to_vec_pretty(&metadata)
            .map_err(|source| ArtifactError::SerializeMetadata { source })?;
        fs::write(artifact_dir.join("metadata.json"), metadata_json).map_err(|source| {
            ArtifactError::WriteFile {
                path: artifact_dir.join("metadata.json").display().to_string(),
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
        let artifact_dir = self.root_dir.join(artifact_id);
        let blob_path = artifact_dir.join("blob");

        let metadata = self.load_metadata(artifact_id)?;
        let bytes =
            fs::read(&blob_path).map_err(|_| ArtifactError::NotFound(artifact_id.to_string()))?;

        Ok(StoredArtifact { metadata, bytes })
    }

    pub fn load_metadata(&self, artifact_id: &str) -> Result<ArtifactMetadata, ArtifactError> {
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
    NotFound(String),
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
            Self::MissingChecksum => write!(f, "missing required x-sha256 header"),
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
            Self::NotFound(artifact_id) => write!(f, "artifact not found: {artifact_id}"),
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
        let status = match self {
            Self::MissingChecksum
            | Self::ChecksumMismatch { .. }
            | Self::TooLarge { .. }
            | Self::InvalidBody(_)
            | Self::InvalidHeader { .. } => StatusCode::BAD_REQUEST,
            Self::NotFound(_) => StatusCode::NOT_FOUND,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };

        let payload = Json(ErrorResponse {
            error: self.to_string(),
        });

        (status, payload).into_response()
    }
}

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
}

pub async fn upload_artifact(
    State(state): State<crate::AppState>,
    headers: HeaderMap,
    body: Body,
) -> Result<(StatusCode, Json<ArtifactMetadata>), ArtifactError> {
    let checksum = checksum_from_headers(&headers, state.artifacts.require_checksum_on_upload())?;
    let limit = state.artifacts.max_upload_bytes();
    let bytes: Bytes = axum::body::to_bytes(body, limit)
        .await
        .map_err(ArtifactError::InvalidBody)?;

    let metadata = state
        .artifacts
        .store_bytes(bytes.as_ref(), checksum.as_deref())?;

    Ok((StatusCode::CREATED, Json(metadata)))
}

pub async fn download_artifact(
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
        header::HeaderName::from_static(ARTIFACT_HEADER_SHA256),
        HeaderValue::from_str(&artifact.metadata.sha256).map_err(|error| {
            ArtifactError::InvalidHeader {
                name: ARTIFACT_HEADER_SHA256,
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
    match headers.get(ARTIFACT_HEADER_SHA256) {
        Some(value) => value
            .to_str()
            .map(|value| Some(value.to_string()))
            .map_err(|error| ArtifactError::InvalidHeader {
                name: ARTIFACT_HEADER_SHA256,
                message: error.to_string(),
            }),
        None if checksum_required => Err(ArtifactError::MissingChecksum),
        None => Ok(None),
    }
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
    use tower::util::ServiceExt;

    use super::{ArtifactStore, download_artifact, upload_artifact};
    use crate::{AppState, config::Config, jobs::JobStore, manifest::ManifestStore};

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
            .route("/artifacts", post(upload_artifact))
            .route("/artifacts/{artifact_id}", get(download_artifact))
            .with_state(state);
        let payload = b"artifact-body";
        let checksum = hex::encode(Sha256::digest(payload));

        let upload = app
            .clone()
            .oneshot(
                Request::post("/artifacts")
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
        let metadata: super::ArtifactMetadata =
            serde_json::from_slice(&body).expect("metadata should deserialize");

        let download = app
            .oneshot(
                Request::get(format!("/artifacts/{}", metadata.artifact_id))
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
            },
            jobs: crate::config::JobsConfig {
                default_log_limit_mb: 50,
                cleanup_successful_workdirs: true,
                keep_failed_workdirs: true,
            },
        };

        AppState {
            config: std::sync::Arc::new(config.clone()),
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
