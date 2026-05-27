use std::{
    collections::BTreeMap,
    fs,
    path::Path,
    process::Stdio,
    sync::Arc,
};

use tokio::{
    process::Command,
    time::{Duration, timeout},
};

use crate::{artifacts::ArtifactStore, manifest::OutputSpec};

use super::{
    JobError, JobMetadata, JobStatus, JobStore,
    models::{ExecutionOutcome, JobExecution, JobOutputArtifact},
    store::now_rfc3339,
};

impl JobStore {
    pub(super) async fn run_job(self: Arc<Self>, execution: JobExecution) {
        let outcome = self.execute_process(&execution).await;
        self.finish_job(execution, outcome);
    }

    async fn execute_process(&self, execution: &JobExecution) -> Result<ExecutionOutcome, JobError> {
        let stdout = fs::File::create(&execution.stdout_path).map_err(|source| JobError::WriteFile {
            path: execution.stdout_path.display().to_string(),
            source,
        })?;
        let stderr = fs::File::create(&execution.stderr_path).map_err(|source| JobError::WriteFile {
            path: execution.stderr_path.display().to_string(),
            source,
        })?;

        let mut command = Command::new(&execution.manifest.script);
        command
            .current_dir(&execution.work_dir)
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr));

        for (key, value) in build_job_env(&execution.metadata, execution) {
            command.env(key, value);
        }

        let mut child = command.spawn().map_err(|source| JobError::SpawnProcess {
            script: execution.manifest.script.clone(),
            source,
        })?;

        let cancel_wait = async {
            let mut cancel_rx = execution.cancel_rx.clone();
            if *cancel_rx.borrow() {
                Ok(())
            } else {
                cancel_rx.changed().await.map_err(|_| ())
            }
        };

        tokio::select! {
            result = timeout(
                Duration::from_secs(execution.manifest.timeout_seconds),
                child.wait(),
            ) => match result {
                Ok(Ok(status)) => {
                    let code = status.code();
                    Ok(ExecutionOutcome {
                        status: if status.success() {
                            JobStatus::Success
                        } else {
                            JobStatus::Failed
                        },
                        exit_code: code,
                    })
                }
                Ok(Err(source)) => Err(JobError::WaitProcess {
                    script: execution.manifest.script.clone(),
                    source,
                }),
                Err(_) => {
                    let _ = child.kill().await;
                    let _ = child.wait().await;
                    Ok(ExecutionOutcome {
                        status: JobStatus::TimedOut,
                        exit_code: None,
                    })
                }
            },
            _ = cancel_wait => {
                let _ = child.kill().await;
                let _ = child.wait().await;
                Ok(ExecutionOutcome {
                    status: JobStatus::Canceled,
                    exit_code: None,
                })
            }
        }
    }

    fn finish_job(&self, execution: JobExecution, outcome: Result<ExecutionOutcome, JobError>) {
        let mut metadata = execution.metadata;
        let finished_at = now_rfc3339();

        match outcome {
            Ok(outcome) => {
                let final_status = if matches!(outcome.status, JobStatus::Success) {
                    match register_outputs(
                        &execution.artifacts,
                        &execution.manifest.outputs,
                        &execution.output_dir,
                        &mut metadata,
                    ) {
                        Ok(()) => JobStatus::Success,
                        Err(error) => {
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

                match final_status {
                    JobStatus::Success if execution.cleanup_successful_workdirs => {
                        let _ = fs::remove_dir_all(&execution.work_dir);
                    }
                    JobStatus::Failed | JobStatus::TimedOut if !execution.keep_failed_workdirs => {
                        let _ = fs::remove_dir_all(&execution.work_dir);
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
            }
        }

        let mut running_jobs = self.running_jobs.lock().expect("job mutex poisoned");
        running_jobs.remove(&execution.job_id);
    }
}

fn build_job_env(metadata: &JobMetadata, execution: &JobExecution) -> BTreeMap<String, String> {
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

    for (name, value) in &metadata.params {
        let env_name = normalize_param_env(name);
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

fn normalize_param_env(name: &str) -> String {
    format!("JOB_{}", name.replace('-', "_").to_ascii_uppercase())
}

fn register_outputs(
    artifacts: &ArtifactStore,
    specs: &BTreeMap<String, OutputSpec>,
    output_dir: &Path,
    metadata: &mut JobMetadata,
) -> Result<(), JobError> {
    for (name, spec) in specs {
        let path = output_dir.join(&spec.path);

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

fn append_error(path: &Path, error: &JobError) -> Result<(), JobError> {
    let mut stderr = fs::read_to_string(path).unwrap_or_default();
    if !stderr.is_empty() && !stderr.ends_with('\n') {
        stderr.push('\n');
    }
    stderr.push_str(&error.to_string());
    stderr.push('\n');

    fs::write(path, stderr).map_err(|source| JobError::WriteFile {
        path: path.display().to_string(),
        source,
    })
}
