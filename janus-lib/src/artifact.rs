use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArtifactUploadResponse {
    pub artifact_id: String,
    pub sha256: String,
    pub size: u64,
    pub expires_at: String,
}
