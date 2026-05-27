use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use chrono::{SecondsFormat, Utc};
use serde_json::{Map, Value};
use tokio::sync::watch;
use uuid::Uuid;

use crate::{
    artifacts::ArtifactStore,
    manifest::{Concurrency, JobManifest, ManifestStore, ParamType},
    storage::atomic_write,
};

use super::{
    JobCreatedResponse, JobError, JobMetadata, JobStatus,
    models::{JobCreated, JobExecution, JobLogs},
};

#[derive(Debug)]
pub struct JobStore {
    pub(super) root_dir: PathBuf,
    pub(super) running_jobs: Mutex<BTreeMap<String, RunningJob>>,
}

#[derive(Debug, Clone)]
pub(super) struct RunningJob {
    pub(super) job_id: String,
    pub(super) name: String,
    pub(super) concurrency: Concurrency,
    pub(super) cancel_tx: watch::Sender<bool>,
}

impl JobStore {
    pub fn new(data_dir: impl AsRef<Path>) -> Result<Self, JobError> {
        let root_dir = data_dir.as_ref().join("jobs");
        fs::create_dir_all(&root_dir).map_err(|source| JobError::CreateDir {
            path: root_dir.display().to_string(),
            source,
        })?;

        Ok(Self {
            root_dir,
            running_jobs: Mutex::new(BTreeMap::new()),
        })
    }

    pub fn create_job(
        self: &Arc<Self>,
        name: &str,
        params: Map<String, Value>,
        manifests: &ManifestStore,
        artifacts: &ArtifactStore,
        default_log_limit_mb: u64,
        cleanup_successful_workdirs: bool,
        keep_failed_workdirs: bool,
    ) -> Result<JobCreatedResponse, JobError> {
        let manifest = manifests
            .get(name)
            .cloned()
            .ok_or_else(|| JobError::UnknownJob(name.to_string()))?;

        let resolved = validate_params(&manifest, &params, artifacts)?;
        let log_limit_bytes = default_log_limit_mb
            .checked_mul(1024_u64 * 1024_u64)
            .ok_or(JobError::InvalidLogLimit {
                max_size_mb: default_log_limit_mb,
            })?;

        let job_id = format!("job_{}", Uuid::now_v7().simple());
        let started_at = now_rfc3339();
        let job = JobMetadata {
            job_id: job_id.clone(),
            name: manifest.name.clone(),
            status: JobStatus::Running,
            started_at: started_at.clone(),
            finished_at: None,
            exit_code: None,
            params,
            resolved_artifacts: resolved,
            outputs: BTreeMap::new(),
        };

        let (cancel_tx, cancel_rx) = watch::channel(false);

        {
            let mut running_jobs = self.running_jobs.lock().expect("job mutex poisoned");
            enforce_concurrency(&manifest, &running_jobs)?;
            running_jobs.insert(
                job_id.clone(),
                RunningJob {
                    job_id: job_id.clone(),
                    name: manifest.name.clone(),
                    concurrency: manifest.concurrency.clone(),
                    cancel_tx,
                },
            );
        }

        let setup_result: Result<PathBuf, JobError> = (|| {
            let job_dir = self.root_dir.join(&job_id);
            fs::create_dir_all(job_dir.join("work")).map_err(|source| JobError::CreateDir {
                path: job_dir.join("work").display().to_string(),
                source,
            })?;
            fs::create_dir_all(job_dir.join("output")).map_err(|source| JobError::CreateDir {
                path: job_dir.join("output").display().to_string(),
                source,
            })?;
            fs::write(job_dir.join("stdout.log"), []).map_err(|source| JobError::WriteFile {
                path: job_dir.join("stdout.log").display().to_string(),
                source,
            })?;
            fs::write(job_dir.join("stderr.log"), []).map_err(|source| JobError::WriteFile {
                path: job_dir.join("stderr.log").display().to_string(),
                source,
            })?;
            self.persist_metadata(job_dir.join("metadata.json").as_path(), &job)?;

            Ok(job_dir)
        })();

        let job_dir = match setup_result {
            Ok(job_dir) => job_dir,
            Err(error) => {
                let mut running_jobs = self.running_jobs.lock().expect("job mutex poisoned");
                running_jobs.remove(&job_id);
                return Err(error);
            }
        };

        let execution = JobExecution {
            artifacts: Arc::new(artifacts.clone()),
            manifest,
            job_id: job_id.clone(),
            metadata_path: job_dir.join("metadata.json"),
            work_dir: job_dir.join("work"),
            output_dir: job_dir.join("output"),
            stdout_path: job_dir.join("stdout.log"),
            stderr_path: job_dir.join("stderr.log"),
            log_limit_bytes,
            cleanup_successful_workdirs,
            keep_failed_workdirs,
            metadata: job,
            cancel_rx,
        };

        let store = Arc::clone(self);
        tokio::spawn(async move {
            store.run_job(execution).await;
        });

        Ok(JobCreatedResponse::from(JobCreated {
            job_id,
            status: JobStatus::Running,
            started_at,
        }))
    }

    pub fn read_job(&self, job_id: &str) -> Result<JobMetadata, JobError> {
        let metadata_path = self.root_dir.join(job_id).join("metadata.json");
        let bytes = fs::read(&metadata_path).map_err(|source| {
            if source.kind() == std::io::ErrorKind::NotFound {
                JobError::JobNotFound(job_id.to_string())
            } else {
                JobError::ReadFile {
                    path: metadata_path.display().to_string(),
                    source,
                }
            }
        })?;

        serde_json::from_slice(&bytes).map_err(|source| JobError::ParseMetadata {
            path: metadata_path.display().to_string(),
            source,
        })
    }

    pub fn read_logs(&self, job_id: &str) -> Result<JobLogs, JobError> {
        let job_dir = self.root_dir.join(job_id);
        let stdout_path = job_dir.join("stdout.log");
        let stderr_path = job_dir.join("stderr.log");

        let stdout = fs::read_to_string(&stdout_path).map_err(|source| {
            if source.kind() == std::io::ErrorKind::NotFound {
                JobError::JobNotFound(job_id.to_string())
            } else {
                JobError::ReadFile {
                    path: stdout_path.display().to_string(),
                    source,
                }
            }
        })?;
        let stderr = fs::read_to_string(&stderr_path).map_err(|source| {
            if source.kind() == std::io::ErrorKind::NotFound {
                JobError::JobNotFound(job_id.to_string())
            } else {
                JobError::ReadFile {
                    path: stderr_path.display().to_string(),
                    source,
                }
            }
        })?;

        Ok(JobLogs { stdout, stderr })
    }

    pub fn cancel_job(&self, job_id: &str) -> Result<(), JobError> {
        let running_jobs = self.running_jobs.lock().expect("job mutex poisoned");
        let running_job = running_jobs
            .get(job_id)
            .ok_or_else(|| match self.read_job(job_id) {
                Ok(_) => JobError::JobNotRunning(job_id.to_string()),
                Err(JobError::JobNotFound(_)) => JobError::JobNotFound(job_id.to_string()),
                Err(error) => error,
            })?;

        running_job
            .cancel_tx
            .send(true)
            .map_err(|_| JobError::JobNotRunning(job_id.to_string()))?;
        Ok(())
    }

    pub fn recover_interrupted_jobs(&self) -> Result<usize, JobError> {
        let mut recovered = 0;
        let entries = fs::read_dir(&self.root_dir).map_err(|source| JobError::ReadFile {
            path: self.root_dir.display().to_string(),
            source,
        })?;

        for entry in entries {
            let entry = entry.map_err(|source| JobError::ReadFile {
                path: self.root_dir.display().to_string(),
                source,
            })?;
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }

            let metadata_path = path.join("metadata.json");
            let stderr_path = path.join("stderr.log");
            let mut metadata = match fs::read(&metadata_path) {
                Ok(bytes) => match serde_json::from_slice::<JobMetadata>(&bytes).map_err(|source| {
                    JobError::ParseMetadata {
                        path: metadata_path.display().to_string(),
                        source,
                    }
                }) {
                    Ok(metadata) => metadata,
                    Err(JobError::ParseMetadata { .. }) => {
                        fs::remove_dir_all(&path).map_err(|source| JobError::CreateDir {
                            path: path.display().to_string(),
                            source,
                        })?;
                        recovered += 1;
                        continue;
                    }
                    Err(error) => return Err(error),
                },
                Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
                    fs::remove_dir_all(&path).map_err(|source| JobError::CreateDir {
                        path: path.display().to_string(),
                        source,
                    })?;
                    recovered += 1;
                    continue;
                }
                Err(source) => {
                    return Err(JobError::ReadFile {
                        path: metadata_path.display().to_string(),
                        source,
                    });
                }
            };

            if metadata.status == JobStatus::Running {
                metadata.status = JobStatus::Failed;
                metadata.finished_at = Some(now_rfc3339());
                metadata.exit_code = None;
                self.persist_metadata(&metadata_path, &metadata)?;
                append_recovery_message(&stderr_path)?;
                recovered += 1;
            }
        }

        Ok(recovered)
    }

    pub(super) fn persist_metadata(
        &self,
        path: &Path,
        metadata: &JobMetadata,
    ) -> Result<(), JobError> {
        let metadata_json = serde_json::to_vec_pretty(metadata)
            .map_err(|source| JobError::SerializeMetadata { source })?;
        atomic_write(path, &metadata_json).map_err(|source| JobError::WriteFile {
            path: path.display().to_string(),
            source,
        })
    }
}

fn validate_params(
    manifest: &JobManifest,
    params: &Map<String, Value>,
    artifacts: &ArtifactStore,
) -> Result<BTreeMap<String, String>, JobError> {
    for (param_name, spec) in &manifest.params {
        if spec.required && !params.contains_key(param_name) {
            return Err(JobError::MissingParam(param_name.clone()));
        }
    }

    for name in params.keys() {
        if !manifest.params.contains_key(name) {
            return Err(JobError::UnknownParam(name.clone()));
        }
    }

    let mut resolved = BTreeMap::new();

    for (name, value) in params {
        let spec = &manifest.params[name];

        match spec.kind {
            ParamType::String => {
                if !value.is_string() {
                    return Err(JobError::InvalidParamType {
                        name: name.clone(),
                        expected: "string",
                    });
                }
            }
            ParamType::Integer => {
                if value.as_i64().is_none() {
                    return Err(JobError::InvalidParamType {
                        name: name.clone(),
                        expected: "integer",
                    });
                }
            }
            ParamType::Boolean => {
                if !value.is_boolean() {
                    return Err(JobError::InvalidParamType {
                        name: name.clone(),
                        expected: "boolean",
                    });
                }
            }
            ParamType::Artifact => {
                let artifact_id = value.as_str().ok_or_else(|| JobError::InvalidParamType {
                    name: name.clone(),
                    expected: "artifact id string",
                })?;
                let metadata = artifacts.load_metadata(artifact_id)?;

                if metadata.is_expired()? {
                    return Err(JobError::ExpiredArtifact {
                        name: name.clone(),
                        artifact_id: artifact_id.to_string(),
                    });
                }

                resolved.insert(
                    name.clone(),
                    artifacts
                        .artifact_blob_path(artifact_id)
                        .display()
                        .to_string(),
                );
            }
            ParamType::Json => {
                if value.is_null() {
                    return Err(JobError::InvalidParamType {
                        name: name.clone(),
                        expected: "json",
                    });
                }
            }
        }
    }

    Ok(resolved)
}

fn enforce_concurrency(
    manifest: &JobManifest,
    running_jobs: &BTreeMap<String, RunningJob>,
) -> Result<(), JobError> {
    match manifest.concurrency {
        Concurrency::Parallel => {
            if running_jobs
                .values()
                .any(|job| matches!(job.concurrency, Concurrency::GlobalExclusive))
            {
                return Err(JobError::ConcurrencyConflict {
                    reason: "cannot start job while a global_exclusive job is running".to_string(),
                });
            }
        }
        Concurrency::JobExclusive => {
            if let Some(job) = running_jobs.values().find(|job| {
                matches!(job.concurrency, Concurrency::GlobalExclusive) || job.name == manifest.name
            }) {
                let reason = if matches!(job.concurrency, Concurrency::GlobalExclusive) {
                    "cannot start job while a global_exclusive job is running".to_string()
                } else {
                    format!(
                        "cannot start job {} while another instance is running ({})",
                        manifest.name, job.job_id
                    )
                };
                return Err(JobError::ConcurrencyConflict { reason });
            }
        }
        Concurrency::GlobalExclusive => {
            if !running_jobs.is_empty() {
                return Err(JobError::ConcurrencyConflict {
                    reason: "cannot start global_exclusive job while another job is running"
                        .to_string(),
                });
            }
        }
    }

    Ok(())
}

pub(super) fn now_rfc3339() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true)
}

fn append_recovery_message(path: &Path) -> Result<(), JobError> {
    let message = "runner restarted before job completion; marking job as failed";
    let mut stderr = fs::read_to_string(path).unwrap_or_default();
    if !stderr.is_empty() && !stderr.ends_with('\n') {
        stderr.push('\n');
    }
    stderr.push_str(message);
    stderr.push('\n');

    fs::write(path, stderr).map_err(|source| JobError::WriteFile {
        path: path.display().to_string(),
        source,
    })
}
