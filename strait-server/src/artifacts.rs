use std::{
    fs,
    path::{Path, PathBuf},
};

use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::models::ServerArtifact;

#[derive(Debug, Clone)]
pub struct ArtifactStore {
    root: PathBuf,
}

impl ArtifactStore {
    pub fn new(data_dir: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let root = PathBuf::from(data_dir).join("server-artifacts");
        fs::create_dir_all(&root)?;
        Ok(Self { root })
    }

    pub fn store_bytes(
        &self,
        scope_type: &str,
        scope_id: &str,
        artifact_name: &str,
        bytes: &[u8],
    ) -> Result<PendingArtifact, Box<dyn std::error::Error>> {
        let artifact_id = format!("srvart_{}", Uuid::now_v7().simple());
        let storage_path = self.root.join(format!("{artifact_id}.bin"));
        fs::write(&storage_path, bytes)?;
        Ok(PendingArtifact {
            id: artifact_id,
            scope_type: scope_type.to_string(),
            scope_id: scope_id.to_string(),
            artifact_name: artifact_name.to_string(),
            sha256: format!("{:x}", Sha256::digest(bytes)),
            size_bytes: i64::try_from(bytes.len())?,
            storage_path: storage_path.display().to_string(),
        })
    }

    pub fn store_file(
        &self,
        scope_type: &str,
        scope_id: &str,
        artifact_name: &str,
        source_path: &Path,
    ) -> Result<PendingArtifact, Box<dyn std::error::Error>> {
        let bytes = fs::read(source_path)?;
        self.store_bytes(scope_type, scope_id, artifact_name, &bytes)
    }

    pub fn read_bytes(
        &self,
        artifact: &ServerArtifact,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        Ok(fs::read(&artifact.storage_path)?)
    }
}

#[derive(Debug, Clone)]
pub struct PendingArtifact {
    pub id: String,
    pub scope_type: String,
    pub scope_id: String,
    pub artifact_name: String,
    pub sha256: String,
    pub size_bytes: i64,
    pub storage_path: String,
}
