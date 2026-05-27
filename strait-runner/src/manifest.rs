use std::{
    collections::BTreeMap,
    fmt, fs,
    path::{Path, PathBuf},
};

use serde::Deserialize;

const RESERVED_PARAM_NAMES: &[&str] = &["id", "name", "workdir", "output_dir", "metadata_path"];

#[derive(Debug, Clone)]
pub struct ManifestStore {
    manifests: BTreeMap<String, JobManifest>,
}

impl ManifestStore {
    pub fn load_from_dir(path: impl AsRef<Path>) -> Result<Self, ManifestError> {
        let path = path.as_ref();
        let metadata = fs::metadata(path).map_err(|source| ManifestError::ReadDir {
            path: path.display().to_string(),
            source,
        })?;

        if !metadata.is_dir() {
            return Err(ManifestError::NotDirectory {
                path: path.display().to_string(),
            });
        }

        let mut manifests = BTreeMap::new();
        let entries = fs::read_dir(path).map_err(|source| ManifestError::ReadDir {
            path: path.display().to_string(),
            source,
        })?;

        for entry in entries {
            let entry = entry.map_err(|source| ManifestError::ReadDirEntry {
                path: path.display().to_string(),
                source,
            })?;
            let entry_path = entry.path();

            if !is_toml_file(&entry_path) {
                continue;
            }

            let manifest = JobManifest::load_from_path(&entry_path)?;

            if manifests
                .insert(manifest.name.clone(), manifest.clone())
                .is_some()
            {
                return Err(ManifestError::DuplicateJobName {
                    name: manifest.name,
                    path: entry_path.display().to_string(),
                });
            }
        }

        Ok(Self { manifests })
    }

    pub fn len(&self) -> usize {
        self.manifests.len()
    }

    #[allow(dead_code)]
    pub fn get(&self, name: &str) -> Option<&JobManifest> {
        self.manifests.get(name)
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct JobManifest {
    pub name: String,
    pub script: String,
    pub timeout_seconds: u64,
    pub concurrency: Concurrency,
    #[serde(default)]
    pub params: BTreeMap<String, ParamSpec>,
    #[serde(default)]
    pub outputs: BTreeMap<String, OutputSpec>,
}

impl JobManifest {
    pub fn load_from_path(path: impl AsRef<Path>) -> Result<Self, ManifestError> {
        let path = path.as_ref();
        let raw = fs::read_to_string(path).map_err(|source| ManifestError::ReadFile {
            path: path.display().to_string(),
            source,
        })?;

        let manifest: JobManifest =
            toml::from_str(&raw).map_err(|source| ManifestError::ParseFile {
                path: path.display().to_string(),
                source,
            })?;

        manifest.validate(path)?;

        Ok(manifest)
    }

    fn validate(&self, path: &Path) -> Result<(), ManifestError> {
        validate_job_name(&self.name, path)?;
        validate_script_path(&self.script, path)?;

        if self.timeout_seconds == 0 {
            return Err(ManifestError::InvalidTimeout {
                path: path.display().to_string(),
            });
        }

        for param_name in self.params.keys() {
            validate_param_name(param_name, path)?;
        }

        for output_name in self.outputs.keys() {
            validate_output_name(output_name, path)?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ParamType {
    String,
    Integer,
    Boolean,
    Artifact,
    Json,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct ParamSpec {
    #[serde(rename = "type")]
    pub kind: ParamType,
    pub required: bool,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct OutputSpec {
    pub path: String,
    pub required: bool,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Concurrency {
    Parallel,
    JobExclusive,
    GlobalExclusive,
}

#[derive(Debug)]
pub enum ManifestError {
    ReadDir {
        path: String,
        source: std::io::Error,
    },
    ReadDirEntry {
        path: String,
        source: std::io::Error,
    },
    NotDirectory {
        path: String,
    },
    ReadFile {
        path: String,
        source: std::io::Error,
    },
    ParseFile {
        path: String,
        source: toml::de::Error,
    },
    DuplicateJobName {
        name: String,
        path: String,
    },
    InvalidJobName {
        name: String,
        path: String,
    },
    InvalidParamName {
        name: String,
        path: String,
    },
    InvalidOutputName {
        name: String,
        path: String,
    },
    InvalidTimeout {
        path: String,
    },
    ScriptNotFound {
        script: String,
        path: String,
    },
    ScriptNotExecutable {
        script: String,
        path: String,
    },
}

impl fmt::Display for ManifestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ReadDir { path, source } => {
                write!(f, "failed to read manifests directory {path}: {source}")
            }
            Self::ReadDirEntry { path, source } => {
                write!(
                    f,
                    "failed to read entry in manifests directory {path}: {source}"
                )
            }
            Self::NotDirectory { path } => write!(f, "manifests path is not a directory: {path}"),
            Self::ReadFile { path, source } => {
                write!(f, "failed to read manifest {path}: {source}")
            }
            Self::ParseFile { path, source } => {
                write!(f, "failed to parse manifest {path}: {source}")
            }
            Self::DuplicateJobName { name, path } => {
                write!(f, "duplicate job name {name} found in manifest {path}")
            }
            Self::InvalidJobName { name, path } => {
                write!(f, "invalid job name {name} in manifest {path}")
            }
            Self::InvalidParamName { name, path } => {
                write!(f, "invalid param name {name} in manifest {path}")
            }
            Self::InvalidOutputName { name, path } => {
                write!(f, "invalid output name {name} in manifest {path}")
            }
            Self::InvalidTimeout { path } => {
                write!(f, "timeout_seconds must be positive in manifest {path}")
            }
            Self::ScriptNotFound { script, path } => {
                write!(f, "script {script} does not exist for manifest {path}")
            }
            Self::ScriptNotExecutable { script, path } => {
                write!(f, "script {script} is not executable for manifest {path}")
            }
        }
    }
}

impl std::error::Error for ManifestError {}

fn is_toml_file(path: &Path) -> bool {
    matches!(path.extension().and_then(|ext| ext.to_str()), Some("toml"))
}

fn validate_job_name(name: &str, path: &Path) -> Result<(), ManifestError> {
    if !is_safe_name(name) || name.contains('/') {
        return Err(ManifestError::InvalidJobName {
            name: name.to_string(),
            path: path.display().to_string(),
        });
    }

    Ok(())
}

fn validate_param_name(name: &str, path: &Path) -> Result<(), ManifestError> {
    if !is_safe_name(name) || RESERVED_PARAM_NAMES.contains(&name) {
        return Err(ManifestError::InvalidParamName {
            name: name.to_string(),
            path: path.display().to_string(),
        });
    }

    Ok(())
}

fn validate_output_name(name: &str, path: &Path) -> Result<(), ManifestError> {
    if !is_safe_name(name) {
        return Err(ManifestError::InvalidOutputName {
            name: name.to_string(),
            path: path.display().to_string(),
        });
    }

    Ok(())
}

fn is_safe_name(name: &str) -> bool {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return false;
    };

    if !first.is_ascii_alphabetic() {
        return false;
    }

    chars.all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-'))
}

fn validate_script_path(script: &str, path: &Path) -> Result<(), ManifestError> {
    let script_path = PathBuf::from(script);
    let metadata = fs::metadata(&script_path).map_err(|source| {
        if source.kind() == std::io::ErrorKind::NotFound {
            ManifestError::ScriptNotFound {
                script: script.to_string(),
                path: path.display().to_string(),
            }
        } else {
            ManifestError::ReadFile {
                path: script_path.display().to_string(),
                source,
            }
        }
    })?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        if metadata.permissions().mode() & 0o111 == 0 {
            return Err(ManifestError::ScriptNotExecutable {
                script: script.to_string(),
                path: path.display().to_string(),
            });
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::{Path, PathBuf},
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::{Concurrency, JobManifest, ManifestError, ManifestStore, ParamType};

    #[test]
    fn parses_manifest_file() {
        let temp = temp_dir("parse_manifest");
        let script = write_executable_script(&temp, "build.sh");
        let manifest_path = temp.join("build-app.toml");

        fs::write(
            &manifest_path,
            format!(
                r#"
name = "build-app"
script = "{}"
timeout_seconds = 600
concurrency = "job_exclusive"

[params.commit]
type = "string"
required = true

[outputs.app]
path = "app.tar.gz"
required = true
"#,
                script.display()
            ),
        )
        .expect("manifest should be written");

        let manifest = JobManifest::load_from_path(&manifest_path).expect("manifest should load");

        assert_eq!(manifest.name, "build-app");
        assert_eq!(manifest.timeout_seconds, 600);
        assert_eq!(manifest.concurrency, Concurrency::JobExclusive);
        assert_eq!(manifest.params["commit"].kind, ParamType::String);
        assert!(manifest.outputs["app"].required);
    }

    #[test]
    fn rejects_duplicate_job_names() {
        let temp = temp_dir("duplicate_names");
        let script_a = write_executable_script(&temp, "build-a.sh");
        let script_b = write_executable_script(&temp, "build-b.sh");

        write_manifest(
            &temp.join("a.toml"),
            "build-app",
            &script_a,
            "parallel",
            "commit",
        );
        write_manifest(
            &temp.join("b.toml"),
            "build-app",
            &script_b,
            "job_exclusive",
            "branch",
        );

        let error = ManifestStore::load_from_dir(&temp).expect_err("duplicate names must fail");

        assert!(matches!(error, ManifestError::DuplicateJobName { .. }));
    }

    #[test]
    fn rejects_invalid_job_name() {
        let temp = temp_dir("invalid_job_name");
        let script = write_executable_script(&temp, "build.sh");
        let manifest_path = temp.join("bad.toml");

        write_manifest(&manifest_path, "build/app", &script, "parallel", "commit");

        let error = JobManifest::load_from_path(&manifest_path).expect_err("bad name must fail");

        assert!(matches!(error, ManifestError::InvalidJobName { .. }));
    }

    #[test]
    fn rejects_invalid_param_name() {
        let temp = temp_dir("invalid_param_name");
        let script = write_executable_script(&temp, "build.sh");
        let manifest_path = temp.join("bad.toml");

        fs::write(
            &manifest_path,
            format!(
                r#"
name = "build-app"
script = "{}"
timeout_seconds = 600
concurrency = "parallel"

[params."bad.param"]
type = "string"
required = true

[outputs.app]
path = "app.tar.gz"
required = true
"#,
                script.display()
            ),
        )
        .expect("manifest should be written");

        let error = JobManifest::load_from_path(&manifest_path).expect_err("bad param must fail");

        assert!(matches!(error, ManifestError::InvalidParamName { .. }));
    }

    #[test]
    fn rejects_reserved_param_name() {
        let temp = temp_dir("reserved_param_name");
        let script = write_executable_script(&temp, "build.sh");
        let manifest_path = temp.join("bad.toml");

        write_manifest(
            &manifest_path,
            "build-app",
            &script,
            "parallel",
            "output_dir",
        );

        let error =
            JobManifest::load_from_path(&manifest_path).expect_err("reserved param must fail");

        assert!(matches!(error, ManifestError::InvalidParamName { .. }));
    }

    #[test]
    fn rejects_non_executable_script() {
        let temp = temp_dir("non_executable_script");
        let script = temp.join("build.sh");
        fs::write(&script, "#!/bin/sh\nexit 0\n").expect("script should be written");
        let manifest_path = temp.join("bad.toml");

        write_manifest(&manifest_path, "build-app", &script, "parallel", "commit");

        let error =
            JobManifest::load_from_path(&manifest_path).expect_err("non-executable script fails");

        assert!(matches!(error, ManifestError::ScriptNotExecutable { .. }));
    }

    fn write_manifest(path: &Path, name: &str, script: &Path, concurrency: &str, param_name: &str) {
        fs::write(
            path,
            format!(
                r#"
name = "{name}"
script = "{}"
timeout_seconds = 600
concurrency = "{concurrency}"

[params.{param_name}]
type = "string"
required = true

[outputs.app]
path = "app.tar.gz"
required = true
"#,
                script.display()
            ),
        )
        .expect("manifest should be written");
    }

    fn write_executable_script(dir: &Path, name: &str) -> PathBuf {
        let path = dir.join(name);
        fs::write(&path, "#!/bin/sh\nexit 0\n").expect("script should be written");

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            let mut permissions = fs::metadata(&path).expect("metadata").permissions();
            permissions.set_mode(0o755);
            fs::set_permissions(&path, permissions).expect("permissions should be set");
        }

        path
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
