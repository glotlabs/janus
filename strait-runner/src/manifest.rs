use std::{
    collections::BTreeMap,
    fmt, fs,
    path::{Component, Path, PathBuf},
};

use regex::Regex;
use serde::{Deserialize, Serialize};

const RESERVED_INPUT_NAMES: &[&str] = &["id", "name", "workdir", "output_dir", "metadata_path"];

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

    pub fn all(&self) -> impl Iterator<Item = &JobManifest> {
        self.manifests.values()
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct JobManifest {
    pub name: String,
    pub script: String,
    pub timeout_seconds: u64,
    pub concurrency: Concurrency,
    #[serde(default)]
    pub inputs: BTreeMap<String, InputSpec>,
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

        for input_name in self.inputs.keys() {
            validate_input_name(input_name, path)?;
        }

        for (input_name, input) in &self.inputs {
            validate_input_constraints(input_name, input, path)?;
        }

        for output_name in self.outputs.keys() {
            validate_output_name(output_name, path)?;
        }

        for output in self.outputs.values() {
            validate_output_path(&output.path, path)?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum InputType {
    String,
    Integer,
    Boolean,
    Artifact,
    Json,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct InputSpec {
    #[serde(rename = "type")]
    pub kind: InputType,
    pub required: bool,
    #[serde(default)]
    pub sensitive: bool,
    pub max_length: Option<usize>,
    pub pattern: Option<String>,
    pub max_json_bytes: Option<usize>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct OutputSpec {
    pub path: String,
    pub required: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
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
    InvalidInputName {
        name: String,
        path: String,
    },
    InvalidInputConstraint {
        name: String,
        path: String,
        reason: String,
    },
    InvalidOutputName {
        name: String,
        path: String,
    },
    InvalidOutputPath {
        output: String,
        path: String,
    },
    InvalidTimeout {
        path: String,
    },
    ScriptNotAbsolute {
        script: String,
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
            Self::InvalidInputName { name, path } => {
                write!(f, "invalid input name {name} in manifest {path}")
            }
            Self::InvalidInputConstraint { name, path, reason } => {
                write!(
                    f,
                    "invalid input constraint for {name} in manifest {path}: {reason}"
                )
            }
            Self::InvalidOutputName { name, path } => {
                write!(f, "invalid output name {name} in manifest {path}")
            }
            Self::InvalidOutputPath { output, path } => {
                write!(f, "invalid output path {output} in manifest {path}")
            }
            Self::InvalidTimeout { path } => {
                write!(f, "timeout_seconds must be positive in manifest {path}")
            }
            Self::ScriptNotAbsolute { script, path } => {
                write!(
                    f,
                    "script {script} must be an absolute path in manifest {path}"
                )
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

fn validate_input_name(name: &str, path: &Path) -> Result<(), ManifestError> {
    if !is_safe_name(name) || RESERVED_INPUT_NAMES.contains(&name) {
        return Err(ManifestError::InvalidInputName {
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

fn validate_input_constraints(
    name: &str,
    spec: &InputSpec,
    path: &Path,
) -> Result<(), ManifestError> {
    if let Some(max_length) = spec.max_length {
        if max_length == 0 {
            return Err(ManifestError::InvalidInputConstraint {
                name: name.to_string(),
                path: path.display().to_string(),
                reason: "max_length must be positive".to_string(),
            });
        }

        if !matches!(spec.kind, InputType::String) {
            return Err(ManifestError::InvalidInputConstraint {
                name: name.to_string(),
                path: path.display().to_string(),
                reason: "max_length is only supported for string inputs".to_string(),
            });
        }
    }

    if let Some(pattern) = &spec.pattern {
        if !matches!(spec.kind, InputType::String) {
            return Err(ManifestError::InvalidInputConstraint {
                name: name.to_string(),
                path: path.display().to_string(),
                reason: "pattern is only supported for string inputs".to_string(),
            });
        }

        Regex::new(pattern).map_err(|error| ManifestError::InvalidInputConstraint {
            name: name.to_string(),
            path: path.display().to_string(),
            reason: format!("invalid regex pattern: {error}"),
        })?;
    }

    if let Some(max_json_bytes) = spec.max_json_bytes {
        if max_json_bytes == 0 {
            return Err(ManifestError::InvalidInputConstraint {
                name: name.to_string(),
                path: path.display().to_string(),
                reason: "max_json_bytes must be positive".to_string(),
            });
        }

        if !matches!(spec.kind, InputType::Json) {
            return Err(ManifestError::InvalidInputConstraint {
                name: name.to_string(),
                path: path.display().to_string(),
                reason: "max_json_bytes is only supported for json inputs".to_string(),
            });
        }
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
    if !script_path.is_absolute() {
        return Err(ManifestError::ScriptNotAbsolute {
            script: script.to_string(),
            path: path.display().to_string(),
        });
    }
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

fn validate_output_path(output: &str, path: &Path) -> Result<(), ManifestError> {
    let output_path = Path::new(output);
    if output.is_empty() || output_path.is_absolute() {
        return Err(ManifestError::InvalidOutputPath {
            output: output.to_string(),
            path: path.display().to_string(),
        });
    }

    if output_path.components().any(|component| {
        matches!(
            component,
            Component::ParentDir | Component::RootDir | Component::Prefix(_)
        )
    }) {
        return Err(ManifestError::InvalidOutputPath {
            output: output.to_string(),
            path: path.display().to_string(),
        });
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

    use super::{Concurrency, JobManifest, ManifestError, ManifestStore, InputType};

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

[inputs.commit]
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
        assert_eq!(manifest.inputs["commit"].kind, InputType::String);
        assert!(!manifest.inputs["commit"].sensitive);
        assert!(manifest.outputs["app"].required);
    }

    #[test]
    fn parses_sensitive_input_flag() {
        let temp = temp_dir("parse_sensitive_input");
        let script = write_executable_script(&temp, "build.sh");
        let manifest_path = temp.join("build-app.toml");

        fs::write(
            &manifest_path,
            format!(
                r#"
name = "build-app"
script = "{}"
timeout_seconds = 600
concurrency = "parallel"

[inputs.token]
type = "string"
required = true
sensitive = true
"#,
                script.display()
            ),
        )
        .expect("manifest should be written");

        let manifest = JobManifest::load_from_path(&manifest_path).expect("manifest should load");

        assert!(manifest.inputs["token"].sensitive);
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
    fn rejects_invalid_input_name() {
        let temp = temp_dir("invalid_input_name");
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

[inputs."bad.input"]
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

        let error = JobManifest::load_from_path(&manifest_path).expect_err("bad input must fail");

        assert!(matches!(error, ManifestError::InvalidInputName { .. }));
    }

    #[test]
    fn rejects_reserved_input_name() {
        let temp = temp_dir("reserved_input_name");
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
            JobManifest::load_from_path(&manifest_path).expect_err("reserved input must fail");

        assert!(matches!(error, ManifestError::InvalidInputName { .. }));
    }

    #[test]
    fn rejects_invalid_string_input_pattern() {
        let temp = temp_dir("invalid_input_pattern");
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

[inputs.commit]
type = "string"
required = true
pattern = "[unterminated"
"#,
                script.display()
            ),
        )
        .expect("manifest should be written");

        let error =
            JobManifest::load_from_path(&manifest_path).expect_err("invalid regex must fail");

        assert!(matches!(
            error,
            ManifestError::InvalidInputConstraint { .. }
        ));
    }

    #[test]
    fn rejects_string_constraints_on_non_string_input() {
        let temp = temp_dir("invalid_string_constraint_kind");
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

[inputs.count]
type = "integer"
required = true
max_length = 12
"#,
                script.display()
            ),
        )
        .expect("manifest should be written");

        let error = JobManifest::load_from_path(&manifest_path)
            .expect_err("string constraint on integer must fail");

        assert!(matches!(
            error,
            ManifestError::InvalidInputConstraint { .. }
        ));
    }

    #[test]
    fn rejects_json_constraints_on_non_json_input() {
        let temp = temp_dir("invalid_json_constraint_kind");
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

[inputs.commit]
type = "string"
required = true
max_json_bytes = 32
"#,
                script.display()
            ),
        )
        .expect("manifest should be written");

        let error = JobManifest::load_from_path(&manifest_path)
            .expect_err("json constraint on string must fail");

        assert!(matches!(
            error,
            ManifestError::InvalidInputConstraint { .. }
        ));
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

    #[test]
    fn rejects_relative_script_path() {
        let temp = temp_dir("relative_script_path");
        let manifest_path = temp.join("bad.toml");

        fs::write(
            &manifest_path,
            r#"
name = "build-app"
script = "build.sh"
timeout_seconds = 600
concurrency = "parallel"

[inputs.commit]
type = "string"
required = true
"#,
        )
        .expect("manifest should be written");

        let error =
            JobManifest::load_from_path(&manifest_path).expect_err("relative script path fails");

        assert!(matches!(error, ManifestError::ScriptNotAbsolute { .. }));
    }

    #[test]
    fn rejects_output_path_traversal() {
        let temp = temp_dir("invalid_output_path");
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

[inputs.commit]
type = "string"
required = true

[outputs.app]
path = "../escape.txt"
required = true
"#,
                script.display()
            ),
        )
        .expect("manifest should be written");

        let error = JobManifest::load_from_path(&manifest_path).expect_err("bad output path fails");

        assert!(matches!(error, ManifestError::InvalidOutputPath { .. }));
    }

    fn write_manifest(path: &Path, name: &str, script: &Path, concurrency: &str, input_name: &str) {
        fs::write(
            path,
            format!(
                r#"
name = "{name}"
script = "{}"
timeout_seconds = 600
concurrency = "{concurrency}"

[inputs.{input_name}]
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
