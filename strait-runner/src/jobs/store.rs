use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

use chrono::{SecondsFormat, Utc};
use regex::Regex;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use tokio::sync::watch;
use tracing::{Instrument, error, info, info_span, warn};

use crate::{
    artifacts::ArtifactStore,
    manifest::{Concurrency, InputType, JobManifest, ManifestStore},
    storage::atomic_write,
};

use super::{
    JobCreatedResponse, JobError, JobMetadata, JobStatus,
    models::{
        FailureCategory, JobCreated, JobExecution, JobLogs, JobOutputMetadata, TerminalReason,
    },
};

const REDACTED_INPUT_VALUE: &str = "[REDACTED]";
const RECOVERY_RUNNING_MESSAGE: &str =
    "runner restarted before job completion; marking job as failed";
const RECOVERY_CANCEL_MESSAGE: &str =
    "runner restarted while canceling the job; marking job as canceled";

#[derive(Debug)]
pub struct JobStore {
    pub(super) root_dir: PathBuf,
    pub(super) create_lock: Mutex<()>,
    pub(super) running_jobs: Mutex<BTreeMap<String, RunningJob>>,
    pub(super) shutting_down: AtomicBool,
}

#[derive(Debug, Clone)]
pub(super) struct RunningJob {
    pub(super) job_id: String,
    pub(super) name: String,
    pub(super) metadata_path: PathBuf,
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
            create_lock: Mutex::new(()),
            running_jobs: Mutex::new(BTreeMap::new()),
            shutting_down: AtomicBool::new(false),
        })
    }

    pub fn create_job(
        self: &Arc<Self>,
        name: &str,
        idempotency_key: &str,
        request_body: &[u8],
        inputs: Map<String, Value>,
        manifests: &ManifestStore,
        artifacts: &ArtifactStore,
        default_log_limit_mb: u64,
        cleanup_successful_workdirs: bool,
        keep_failed_workdirs: bool,
    ) -> Result<JobCreatedResponse, JobError> {
        if self.shutting_down.load(Ordering::SeqCst) {
            return Err(JobError::ShuttingDown);
        }
        validate_idempotency_key(idempotency_key)?;

        let _create_lock = self.create_lock.lock().expect("job creation mutex poisoned");

        let manifest = manifests
            .get(name)
            .cloned()
            .ok_or_else(|| JobError::UnknownJob(name.to_string()))?;

        let resolved = validate_inputs(&manifest, &inputs, artifacts)?;
        let metadata_inputs = redact_sensitive_inputs(&manifest, &inputs);
        let log_limit_bytes = default_log_limit_mb
            .checked_mul(1024_u64 * 1024_u64)
            .ok_or(JobError::InvalidLogLimit {
                max_size_mb: default_log_limit_mb,
            })?;
        let request_hash = hash_request(name, request_body);
        let job_id = job_id_for_idempotency_key(idempotency_key);
        let job_dir = self.root_dir.join(&job_id);

        if job_dir.exists() {
            return self.load_existing_job_for_request(
                &job_dir,
                &job_id,
                name,
                idempotency_key,
                &request_hash,
            );
        }

        let started_at = now_rfc3339();
        let job = JobMetadata {
            job_id: job_id.clone(),
            name: manifest.name.clone(),
            idempotency_key: idempotency_key.to_string(),
            request_hash,
            status: JobStatus::Running,
            started_at: started_at.clone(),
            finished_at: None,
            duration_ms: None,
            exit_code: None,
            terminal_reason: None,
            failure_category: None,
            inputs: metadata_inputs,
            resolved_artifacts: resolved,
            outputs: BTreeMap::new(),
            output_metadata: JobOutputMetadata::default(),
        };

        info!(
            job_id = %job_id,
            job_name = %manifest.name,
            concurrency = ?manifest.concurrency,
            timeout_seconds = manifest.timeout_seconds,
            "job accepted"
        );

        let (cancel_tx, cancel_rx) = watch::channel(false);

        {
            let mut running_jobs = self.running_jobs.lock().expect("job mutex poisoned");
            enforce_concurrency(&manifest, &running_jobs)?;
            running_jobs.insert(
                job_id.clone(),
                RunningJob {
                    job_id: job_id.clone(),
                    name: manifest.name.clone(),
                    metadata_path: self.root_dir.join(&job_id).join("metadata.json"),
                    concurrency: manifest.concurrency.clone(),
                    cancel_tx,
                },
            );
        }

        let setup_result: Result<PathBuf, JobError> = (|| {
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
                error!(
                    job_id = %job_id,
                    job_name = %job.name,
                    error = %error,
                    "job setup failed"
                );
                let mut running_jobs = self.running_jobs.lock().expect("job mutex poisoned");
                running_jobs.remove(&job_id);
                return Err(error);
            }
        };
        let job_name = job.name.clone();

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
            raw_inputs: inputs,
            cancel_rx,
        };

        let store = Arc::clone(self);
        let span = info_span!("job_execution", job_id = %job_id, job_name = %job_name);
        tokio::spawn(
            async move {
                store.run_job(execution).await;
            }
            .instrument(span),
        );

        Ok(JobCreatedResponse::from(JobCreated {
            job_id,
            status: JobStatus::Running,
            started_at,
        }))
    }

    pub fn read_job(&self, job_id: &str) -> Result<JobMetadata, JobError> {
        validate_job_id(job_id)?;
        info!(job_id = %job_id, "reading job metadata");
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
        validate_job_id(job_id)?;
        info!(job_id = %job_id, "reading job logs");
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
        validate_job_id(job_id)?;
        let running_job = self
            .running_jobs
            .lock()
            .expect("job mutex poisoned")
            .get(job_id)
            .cloned();
        let Some(running_job) = running_job else {
            return match self.read_job(job_id) {
                Ok(metadata) if matches!(
                    metadata.status,
                    JobStatus::CancelRequested | JobStatus::Canceling | JobStatus::Canceled
                ) =>
                {
                    Ok(())
                }
                Ok(_) => Err(JobError::JobNotRunning(job_id.to_string())),
                Err(JobError::JobNotFound(_)) => Err(JobError::JobNotFound(job_id.to_string())),
                Err(error) => Err(error),
            };
        };

        self.transition_metadata_status(
            &running_job.metadata_path,
            Some(JobStatus::Running),
            JobStatus::CancelRequested,
        )?;

        running_job
            .cancel_tx
            .send(true)
            .map_err(|_| JobError::JobNotRunning(job_id.to_string()))?;
        warn!(job_id = %job_id, job_name = %running_job.name, "job cancel signal sent");
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
                        warn!(path = %path.display(), "removing job directory with invalid metadata");
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
                    warn!(path = %path.display(), "removing job directory missing metadata");
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

            match metadata.status {
                JobStatus::Running => {
                    metadata.status = JobStatus::Failed;
                    metadata.finished_at = Some(now_rfc3339());
                    metadata.duration_ms =
                        calculate_duration_ms(&metadata.started_at, metadata.finished_at.as_deref());
                    metadata.exit_code = None;
                    metadata.terminal_reason = Some(TerminalReason::Shutdown);
                    metadata.failure_category = Some(FailureCategory::Infra);
                    self.persist_metadata(&metadata_path, &metadata)?;
                    append_recovery_message(&stderr_path, RECOVERY_RUNNING_MESSAGE)?;
                    warn!(
                        job_id = %metadata.job_id,
                        job_name = %metadata.name,
                        "recovered interrupted running job as failed"
                    );
                    recovered += 1;
                }
                JobStatus::CancelRequested | JobStatus::Canceling => {
                    metadata.status = JobStatus::Canceled;
                    metadata.finished_at = Some(now_rfc3339());
                    metadata.duration_ms =
                        calculate_duration_ms(&metadata.started_at, metadata.finished_at.as_deref());
                    metadata.exit_code = None;
                    metadata.terminal_reason = Some(TerminalReason::Shutdown);
                    metadata.failure_category = Some(FailureCategory::Canceled);
                    self.persist_metadata(&metadata_path, &metadata)?;
                    append_recovery_message(&stderr_path, RECOVERY_CANCEL_MESSAGE)?;
                    warn!(
                        job_id = %metadata.job_id,
                        job_name = %metadata.name,
                        "recovered interrupted canceling job as canceled"
                    );
                    recovered += 1;
                }
                _ => {}
            }
        }

        Ok(recovered)
    }

    pub fn begin_shutdown(&self) -> usize {
        self.shutting_down.store(true, Ordering::SeqCst);
        let running_jobs = self.running_jobs.lock().expect("job mutex poisoned");
        for running_job in running_jobs.values() {
            let _ = self.transition_metadata_status(
                &running_job.metadata_path,
                Some(JobStatus::Running),
                JobStatus::CancelRequested,
            );
            let _ = running_job.cancel_tx.send(true);
            warn!(
                job_id = %running_job.job_id,
                job_name = %running_job.name,
                "shutdown cancel signal sent"
            );
        }
        running_jobs.len()
    }

    pub async fn wait_for_drain(&self, timeout: std::time::Duration) -> bool {
        let deadline = tokio::time::Instant::now() + timeout;

        loop {
            if self
                .running_jobs
                .lock()
                .expect("job mutex poisoned")
                .is_empty()
            {
                return true;
            }

            if tokio::time::Instant::now() >= deadline {
                return false;
            }

            tokio::time::sleep(tokio::time::Duration::from_millis(25)).await;
        }
    }

    pub fn is_shutting_down(&self) -> bool {
        self.shutting_down.load(Ordering::SeqCst)
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

    pub(super) fn transition_metadata_status(
        &self,
        path: &Path,
        expected_current: Option<JobStatus>,
        next: JobStatus,
    ) -> Result<(), JobError> {
        let mut metadata = self.read_metadata_from_path(path)?;
        if metadata.status.is_terminal() {
            return Ok(());
        }
        if let Some(expected_current) = expected_current
            && metadata.status != expected_current
        {
            return Ok(());
        }
        if metadata.status == next {
            return Ok(());
        }
        metadata.status = next;
        self.persist_metadata(path, &metadata)
    }

    pub(super) fn read_metadata_from_path(&self, path: &Path) -> Result<JobMetadata, JobError> {
        let bytes = fs::read(path).map_err(|source| JobError::ReadFile {
            path: path.display().to_string(),
            source,
        })?;
        serde_json::from_slice(&bytes).map_err(|source| JobError::ParseMetadata {
            path: path.display().to_string(),
            source,
        })
    }

    fn load_existing_job_for_request(
        &self,
        job_dir: &Path,
        job_id: &str,
        name: &str,
        idempotency_key: &str,
        request_hash: &str,
    ) -> Result<JobCreatedResponse, JobError> {
        let metadata_path = job_dir.join("metadata.json");
        let bytes = fs::read(&metadata_path).map_err(|source| JobError::ReadFile {
            path: metadata_path.display().to_string(),
            source,
        })?;
        let metadata: JobMetadata =
            serde_json::from_slice(&bytes).map_err(|source| JobError::ParseMetadata {
                path: metadata_path.display().to_string(),
                source,
            })?;

        if metadata.job_id != job_id
            || metadata.name != name
            || metadata.idempotency_key != idempotency_key
            || metadata.request_hash != request_hash
        {
            return Err(JobError::IdempotencyConflict {
                key: idempotency_key.to_string(),
            });
        }

        Ok(JobCreatedResponse {
            job_id: metadata.job_id,
            status: metadata.status,
            started_at: metadata.started_at,
        })
    }
}

fn validate_inputs(
    manifest: &JobManifest,
    inputs: &Map<String, Value>,
    artifacts: &ArtifactStore,
) -> Result<BTreeMap<String, String>, JobError> {
    for (input_name, spec) in &manifest.inputs {
        if spec.required && !inputs.contains_key(input_name) {
            return Err(JobError::MissingInput(input_name.clone()));
        }
    }

    for name in inputs.keys() {
        if !manifest.inputs.contains_key(name) {
            return Err(JobError::UnknownInput(name.clone()));
        }
    }

    let mut resolved = BTreeMap::new();

    for (name, value) in inputs {
        let spec = &manifest.inputs[name];

        match spec.kind {
            InputType::String => {
                let string_value = value.as_str().ok_or_else(|| JobError::InvalidInputType {
                    name: name.clone(),
                    expected: "string",
                })?;

                if let Some(max_length) = spec.max_length
                    && string_value.chars().count() > max_length
                {
                    return Err(JobError::InvalidInputValue {
                        name: name.clone(),
                        reason: format!("must be at most {max_length} characters"),
                    });
                }

                if let Some(pattern) = &spec.pattern
                    && !Regex::new(pattern)
                        .expect("manifest pattern should have been validated")
                        .is_match(string_value)
                {
                    return Err(JobError::InvalidInputValue {
                        name: name.clone(),
                        reason: format!("must match pattern {pattern}"),
                    });
                }
            }
            InputType::Integer => {
                if value.as_i64().is_none() {
                    return Err(JobError::InvalidInputType {
                        name: name.clone(),
                        expected: "integer",
                    });
                }
            }
            InputType::Boolean => {
                if !value.is_boolean() {
                    return Err(JobError::InvalidInputType {
                        name: name.clone(),
                        expected: "boolean",
                    });
                }
            }
            InputType::Artifact => {
                let artifact_id = value.as_str().ok_or_else(|| JobError::InvalidInputType {
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
            InputType::Json => {
                if value.is_null() {
                    return Err(JobError::InvalidInputType {
                        name: name.clone(),
                        expected: "json",
                    });
                }

                if let Some(max_json_bytes) = spec.max_json_bytes
                    && value.to_string().len() > max_json_bytes
                {
                    return Err(JobError::InvalidInputValue {
                        name: name.clone(),
                        reason: format!("must be at most {max_json_bytes} JSON bytes"),
                    });
                }
            }
        }
    }

    Ok(resolved)
}

fn redact_sensitive_inputs(
    manifest: &JobManifest,
    inputs: &Map<String, Value>,
) -> Map<String, Value> {
    inputs
        .iter()
        .map(|(name, value)| {
            let value = if manifest.inputs[name].sensitive {
                Value::String(REDACTED_INPUT_VALUE.to_string())
            } else {
                value.clone()
            };
            (name.clone(), value)
        })
        .collect()
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

fn calculate_duration_ms(started_at: &str, finished_at: Option<&str>) -> Option<u64> {
    let finished_at = finished_at?;
    let started = chrono::DateTime::parse_from_rfc3339(started_at).ok()?;
    let finished = chrono::DateTime::parse_from_rfc3339(finished_at).ok()?;
    let millis = finished.signed_duration_since(started).num_milliseconds();
    u64::try_from(millis).ok()
}

fn hash_request(name: &str, request_body: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(name.as_bytes());
    hasher.update([0]);
    hasher.update(request_body);
    hex::encode(hasher.finalize())
}

fn job_id_for_idempotency_key(idempotency_key: &str) -> String {
    format!("job_{}", &hash_request("job", idempotency_key.as_bytes())[..32])
}

fn validate_job_id(job_id: &str) -> Result<(), JobError> {
    validate_prefixed_hex_id(job_id, "job_")
        .then_some(())
        .ok_or_else(|| JobError::InvalidJobId(job_id.to_string()))
}

fn validate_idempotency_key(idempotency_key: &str) -> Result<(), JobError> {
    if idempotency_key.is_empty() || idempotency_key.len() > 128 {
        return Err(JobError::InvalidIdempotencyKey(idempotency_key.to_string()));
    }
    if !idempotency_key
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(JobError::InvalidIdempotencyKey(idempotency_key.to_string()));
    }
    Ok(())
}

fn validate_prefixed_hex_id(value: &str, prefix: &str) -> bool {
    let Some(suffix) = value.strip_prefix(prefix) else {
        return false;
    };

    suffix.len() == 32 && suffix.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn append_recovery_message(path: &Path, message: &str) -> Result<(), JobError> {
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
