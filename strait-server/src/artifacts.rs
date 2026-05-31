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
    max_artifact_bytes: usize,
}

impl ArtifactStore {
    pub fn new(
        data_dir: &str,
        max_artifact_bytes: usize,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let root = PathBuf::from(data_dir).join("server-artifacts");
        fs::create_dir_all(&root)?;
        let root = root.canonicalize()?;
        Ok(Self {
            root,
            max_artifact_bytes,
        })
    }

    pub fn store_bytes(
        &self,
        scope_type: &str,
        scope_id: &str,
        artifact_name: &str,
        bytes: &[u8],
    ) -> Result<PendingArtifact, Box<dyn std::error::Error>> {
        if bytes.len() > self.max_artifact_bytes {
            return Err(format!(
                "artifact exceeds maximum size of {} bytes",
                self.max_artifact_bytes
            )
            .into());
        }
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
        let path = PathBuf::from(&artifact.storage_path).canonicalize()?;
        if !path.starts_with(&self.root) {
            return Err("artifact storage path escapes artifact root".into());
        }
        Ok(fs::read(path)?)
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

#[cfg(test)]
mod tests {
    use std::{fs, path::PathBuf};

    use uuid::Uuid;

    use super::*;

    #[test]
    fn read_rejects_paths_outside_artifact_root() {
        let temp = std::env::temp_dir().join(format!("strait-artifacts-{}", Uuid::now_v7()));
        let data_dir = temp.join("data");
        fs::create_dir_all(&data_dir).expect("data dir");
        let outside = temp.join("outside.bin");
        fs::write(&outside, b"secret").expect("outside file");
        let store = ArtifactStore::new(data_dir.to_str().expect("data dir"), 1024).expect("store");

        let artifact = ServerArtifact {
            id: "srvart_test".to_string(),
            scope_type: "test".to_string(),
            scope_id: "scope".to_string(),
            artifact_name: "artifact".to_string(),
            sha256: String::new(),
            size_bytes: 6,
            storage_path: outside.display().to_string(),
            created_at: String::new(),
        };

        let error = store.read_bytes(&artifact).expect_err("escape rejected");
        assert!(
            error
                .to_string()
                .contains("artifact storage path escapes artifact root")
        );
        let _ = fs::remove_dir_all(PathBuf::from(temp));
    }
}
