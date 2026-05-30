use std::{
    collections::BTreeMap,
    fs,
    io::Write,
    path::{Component, Path, PathBuf},
    process::Stdio,
    sync::Arc,
};

use tokio::{
    io::AsyncReadExt,
    process::{Child, Command},
    sync::mpsc,
    task::JoinHandle,
    time::{Duration, Instant},
};
use tracing::{error, info, warn};

use crate::{artifacts::ArtifactStore, manifest::OutputSpec};

use super::{
    JobError, JobMetadata, JobStatus, JobStore,
    models::{ExecutionOutcome, JobExecution, JobOutputArtifact},
    store::now_rfc3339,
};

const DEFAULT_JOB_PATH: &str = "/usr/local/bin:/usr/bin:/bin";

impl JobStore {
    pub(super) async fn run_job(self: Arc<Self>, execution: JobExecution) {
        info!(
            job_id = %execution.job_id,
            job_name = %execution.metadata.name,
            script = %execution.manifest.script,
            "job execution started"
        );
        let outcome = self.execute_process(&execution).await;
        self.finish_job(execution, outcome);
    }

    async fn execute_process(
        &self,
        execution: &JobExecution,
    ) -> Result<ExecutionOutcome, JobError> {
        let mut command = Command::new(&execution.manifest.script);
        command
            .current_dir(&execution.work_dir)
            .env_clear()
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        configure_process_group(&mut command);
        command.env("PATH", DEFAULT_JOB_PATH);

        for (key, value) in build_job_env(execution) {
            command.env(key, value);
        }

        let mut child = command.spawn().map_err(|source| JobError::SpawnProcess {
            script: execution.manifest.script.clone(),
            source,
        })?;
        info!(
            job_id = %execution.job_id,
            job_name = %execution.metadata.name,
            "job process spawned"
        );

        let stdout = child.stdout.take().ok_or_else(|| JobError::SpawnProcess {
            script: execution.manifest.script.clone(),
            source: std::io::Error::other("stdout pipe missing"),
        })?;
        let stderr = child.stderr.take().ok_or_else(|| JobError::SpawnProcess {
            script: execution.manifest.script.clone(),
            source: std::io::Error::other("stderr pipe missing"),
        })?;
        let (log_limit_tx, mut log_limit_rx) = mpsc::unbounded_channel();

        let mut stdout_task = tokio::spawn(capture_output(
            stdout,
            execution.stdout_path.clone(),
            execution.log_limit_bytes,
            "stdout",
            log_limit_tx.clone(),
        ));
        let mut stderr_task = tokio::spawn(capture_output(
            stderr,
            execution.stderr_path.clone(),
            execution.log_limit_bytes,
            "stderr",
            log_limit_tx,
        ));

        let cancel_wait = async {
            let mut cancel_rx = execution.cancel_rx.clone();
            if *cancel_rx.borrow() {
                Ok(())
            } else {
                cancel_rx.changed().await.map_err(|_| ())
            }
        };
        tokio::pin!(cancel_wait);
        let deadline = Instant::now() + Duration::from_secs(execution.manifest.timeout_seconds);

        loop {
            if Instant::now() >= deadline {
                warn!(
                    job_id = %execution.job_id,
                    job_name = %execution.metadata.name,
                    timeout_seconds = execution.manifest.timeout_seconds,
                    "job timed out"
                );
                terminate_job(&mut child).await;
                abort_capture_tasks(&mut stdout_task, &mut stderr_task).await;
                return Ok(ExecutionOutcome {
                    status: JobStatus::TimedOut,
                    exit_code: None,
                    message: None,
                });
            }

            tokio::select! {
                _ = &mut cancel_wait => {
                    warn!(
                        job_id = %execution.job_id,
                        job_name = %execution.metadata.name,
                        "job canceled"
                    );
                    terminate_job(&mut child).await;
                    abort_capture_tasks(&mut stdout_task, &mut stderr_task).await;
                    return Ok(ExecutionOutcome {
                        status: JobStatus::Canceled,
                        exit_code: None,
                        message: None,
                    });
                },
                Some(message) = log_limit_rx.recv() => {
                    warn!(
                        job_id = %execution.job_id,
                        job_name = %execution.metadata.name,
                        message = %message,
                        "job log limit reached"
                    );
                    terminate_job(&mut child).await;
                    abort_capture_tasks(&mut stdout_task, &mut stderr_task).await;
                    return Ok(ExecutionOutcome {
                        status: JobStatus::Failed,
                        exit_code: None,
                        message: Some(message),
                    });
                },
                _ = tokio::time::sleep(Duration::from_millis(25)) => {}
            }

            match child.try_wait().map_err(|source| JobError::WaitProcess {
                script: execution.manifest.script.clone(),
                source,
            })? {
                Some(status) => {
                    await_capture_result(&mut stdout_task).await?;
                    await_capture_result(&mut stderr_task).await?;
                    info!(
                        job_id = %execution.job_id,
                        job_name = %execution.metadata.name,
                        exit_code = status.code(),
                        success = status.success(),
                        "job process exited"
                    );

                    return Ok(ExecutionOutcome {
                        status: if status.success() {
                            JobStatus::Success
                        } else {
                            JobStatus::Failed
                        },
                        exit_code: status.code(),
                        message: None,
                    });
                }
                None => {}
            }
        }
    }

    fn finish_job(&self, execution: JobExecution, outcome: Result<ExecutionOutcome, JobError>) {
        let mut metadata = execution.metadata;
        let finished_at = now_rfc3339();

        match outcome {
            Ok(outcome) => {
                if let Some(message) = &outcome.message {
                    let _ = append_runtime_message(&execution.stderr_path, message);
                }

                let final_status = if matches!(outcome.status, JobStatus::Success) {
                    match register_outputs(
                        &execution.artifacts,
                        &execution.manifest.outputs,
                        &execution.output_dir,
                        &mut metadata,
                    ) {
                        Ok(()) => {
                            info!(
                                job_id = %execution.job_id,
                                job_name = %metadata.name,
                                output_count = metadata.outputs.len(),
                                "job outputs registered"
                            );
                            JobStatus::Success
                        }
                        Err(error) => {
                            error!(
                                job_id = %execution.job_id,
                                job_name = %metadata.name,
                                error = %error,
                                "job output registration failed"
                            );
                            let _ = append_error(&execution.stderr_path, &error);
                            JobStatus::Failed
                        }
                    }
                } else {
                    outcome.status
                };

                metadata.status = final_status.clone();
                metadata.exit_code = outcome.exit_code;
                metadata.finished_at = Some(finished_at);

                let _ = self.persist_metadata(&execution.metadata_path, &metadata);
                info!(
                    job_id = %execution.job_id,
                    job_name = %metadata.name,
                    status = ?metadata.status,
                    exit_code = metadata.exit_code,
                    "job execution finished"
                );

                match final_status {
                    JobStatus::Success if execution.cleanup_successful_workdirs => {
                        match fs::remove_dir_all(&execution.work_dir) {
                            Ok(()) => info!(
                                job_id = %execution.job_id,
                                job_name = %metadata.name,
                                path = %execution.work_dir.display(),
                                "job workdir removed after success"
                            ),
                            Err(error) => warn!(
                                job_id = %execution.job_id,
                                job_name = %metadata.name,
                                path = %execution.work_dir.display(),
                                error = %error,
                                "failed to remove successful job workdir"
                            ),
                        }
                    }
                    JobStatus::Failed | JobStatus::TimedOut if !execution.keep_failed_workdirs => {
                        match fs::remove_dir_all(&execution.work_dir) {
                            Ok(()) => info!(
                                job_id = %execution.job_id,
                                job_name = %metadata.name,
                                path = %execution.work_dir.display(),
                                "job workdir removed after terminal failure"
                            ),
                            Err(error) => warn!(
                                job_id = %execution.job_id,
                                job_name = %metadata.name,
                                path = %execution.work_dir.display(),
                                error = %error,
                                "failed to remove failed job workdir"
                            ),
                        }
                    }
                    _ => {}
                }
            }
            Err(error) => {
                metadata.status = JobStatus::Failed;
                metadata.exit_code = None;
                metadata.finished_at = Some(finished_at);
                let _ = self.persist_metadata(&execution.metadata_path, &metadata);
                let _ = fs::write(&execution.stderr_path, format!("{error}\n"));
                error!(
                    job_id = %execution.job_id,
                    job_name = %metadata.name,
                    error = %error,
                    "job execution failed"
                );
            }
        }

        let mut running_jobs = self.running_jobs.lock().expect("job mutex poisoned");
        running_jobs.remove(&execution.job_id);
    }
}

fn build_job_env(execution: &JobExecution) -> BTreeMap<String, String> {
    let metadata = &execution.metadata;
    let mut env = BTreeMap::from([
        ("JOB_ID".to_string(), metadata.job_id.clone()),
        ("JOB_NAME".to_string(), metadata.name.clone()),
        (
            "JOB_WORKDIR".to_string(),
            execution.work_dir.display().to_string(),
        ),
        (
            "JOB_OUTPUT_DIR".to_string(),
            execution.output_dir.display().to_string(),
        ),
        (
            "JOB_METADATA_PATH".to_string(),
            execution.metadata_path.display().to_string(),
        ),
    ]);

    for (name, value) in &execution.raw_inputs {
        let env_name = normalize_input_env(name);
        let env_value = if let Some(path) = metadata.resolved_artifacts.get(name) {
            path.clone()
        } else if let Some(raw) = value.as_str() {
            raw.to_string()
        } else {
            value.to_string()
        };
        env.insert(env_name, env_value);
    }

    env
}

fn normalize_input_env(name: &str) -> String {
    format!("JOB_{}", name.replace('-', "_").to_ascii_uppercase())
}

fn register_outputs(
    artifacts: &ArtifactStore,
    specs: &BTreeMap<String, OutputSpec>,
    output_dir: &Path,
    metadata: &mut JobMetadata,
) -> Result<(), JobError> {
    for (name, spec) in specs {
        let path =
            safe_output_path(output_dir, &spec.path).ok_or_else(|| JobError::MissingOutput {
                name: name.clone(),
                path: spec.path.clone(),
            })?;

        match fs::metadata(&path) {
            Ok(file_type) if file_type.is_file() => {}
            Ok(_) => {
                return Err(JobError::MissingOutput {
                    name: name.clone(),
                    path: path.display().to_string(),
                });
            }
            Err(source) if source.kind() == std::io::ErrorKind::NotFound && spec.required => {
                return Err(JobError::MissingOutput {
                    name: name.clone(),
                    path: path.display().to_string(),
                });
            }
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => continue,
            Err(source) => {
                return Err(JobError::ReadFile {
                    path: path.display().to_string(),
                    source,
                });
            }
        }

        let stored = artifacts.store_file(&path)?;
        metadata.outputs.insert(
            name.clone(),
            JobOutputArtifact {
                artifact_id: stored.artifact_id,
                sha256: stored.sha256,
                size: stored.size,
            },
        );
    }

    Ok(())
}

fn safe_output_path(output_dir: &Path, relative_path: &str) -> Option<PathBuf> {
    let path = Path::new(relative_path);
    if relative_path.is_empty() || path.is_absolute() {
        return None;
    }

    if path.components().any(|component| {
        matches!(
            component,
            Component::ParentDir | Component::RootDir | Component::Prefix(_)
        )
    }) {
        return None;
    }

    Some(output_dir.join(path))
}

async fn capture_output<R: tokio::io::AsyncRead + Unpin>(
    mut reader: R,
    path: std::path::PathBuf,
    limit_bytes: u64,
    stream_name: &'static str,
    log_limit_tx: mpsc::UnboundedSender<String>,
) -> Result<(), JobError> {
    let mut file = fs::File::create(&path).map_err(|source| JobError::WriteFile {
        path: path.display().to_string(),
        source,
    })?;
    let mut buffer = [0_u8; 8192];
    let mut written = 0_u64;

    loop {
        let read = reader
            .read(&mut buffer)
            .await
            .map_err(|source| JobError::ReadFile {
                path: path.display().to_string(),
                source,
            })?;
        if read == 0 {
            return Ok(());
        }

        let remaining = limit_bytes.saturating_sub(written) as usize;
        if remaining == 0 {
            let _ = log_limit_tx.send(log_limit_message(stream_name, limit_bytes));
            return Ok(());
        }

        let to_write = remaining.min(read);
        file.write_all(&buffer[..to_write])
            .map_err(|source| JobError::WriteFile {
                path: path.display().to_string(),
                source,
            })?;
        written += to_write as u64;

        if to_write < read {
            let _ = log_limit_tx.send(log_limit_message(stream_name, limit_bytes));
            return Ok(());
        }
    }
}

async fn await_capture_result(task: &mut JoinHandle<Result<(), JobError>>) -> Result<(), JobError> {
    match task.await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => Err(error),
        Err(error) => Err(JobError::ReadFile {
            path: "job output capture task".to_string(),
            source: std::io::Error::other(error.to_string()),
        }),
    }
}

async fn abort_capture_tasks(
    first: &mut JoinHandle<Result<(), JobError>>,
    second: &mut JoinHandle<Result<(), JobError>>,
) {
    first.abort();
    second.abort();
    let _ = first.await;
    let _ = second.await;
}

fn log_limit_message(stream_name: &str, limit_bytes: u64) -> String {
    format!("job {stream_name} log exceeded configured limit of {limit_bytes} bytes")
}

async fn terminate_job(child: &mut Child) {
    #[cfg(unix)]
    if let Some(pid) = child.id() {
        if unix_kill_process_group(pid as i32).is_ok() {
            let _ = child.wait().await;
            return;
        }
    }

    let _ = child.kill().await;
    let _ = child.wait().await;
}

#[cfg(unix)]
fn configure_process_group(command: &mut Command) {
    command.process_group(0);
}

#[cfg(not(unix))]
fn configure_process_group(_command: &mut Command) {}

#[cfg(unix)]
fn unix_kill_process_group(pid: i32) -> std::io::Result<()> {
    unsafe extern "C" {
        fn kill(pid: i32, sig: i32) -> i32;
    }

    const SIGKILL: i32 = 9;

    let result = unsafe { kill(-pid, SIGKILL) };
    if result == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

fn append_error(path: &Path, error: &JobError) -> Result<(), JobError> {
    append_runtime_message(path, &error.to_string())
}

fn append_runtime_message(path: &Path, message: &str) -> Result<(), JobError> {
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
