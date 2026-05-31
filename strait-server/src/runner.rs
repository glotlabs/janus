use reqwest::{Client, StatusCode};
use serde_json::Value;
pub use strait_lib::{
    ArtifactUploadResponse, HEADER_IDEMPOTENCY_KEY, HEADER_SHA256, JobCreatedResponse,
    JobLogsResponse, JobOutputMetadata, JobStatusResponse, RunnerRoute,
};

use crate::models::{Runner, RunnerJobDefinition};

#[derive(Debug, Clone)]
pub struct RunnerClient {
    http: Client,
}

impl RunnerClient {
    pub fn new() -> Self {
        Self {
            http: Client::new(),
        }
    }

    pub async fn list_jobs(
        &self,
        runner: &Runner,
    ) -> Result<Vec<RunnerJobDefinition>, Box<dyn std::error::Error + Send + Sync>> {
        let response = self
            .http
            .get(runner_url(runner, RunnerRoute::Jobs))
            .bearer_auth(&runner.token)
            .send()
            .await?;
        if response.status() != StatusCode::OK {
            return Err(format!("runner returned {}", response.status()).into());
        }
        Ok(response.json().await?)
    }

    pub async fn upload_artifact(
        &self,
        runner: &Runner,
        bytes: Vec<u8>,
        sha256: &str,
    ) -> Result<ArtifactUploadResponse, Box<dyn std::error::Error + Send + Sync>> {
        let response = self
            .http
            .post(runner_url(runner, RunnerRoute::Artifacts))
            .bearer_auth(&runner.token)
            .header(HEADER_SHA256, sha256)
            .body(bytes)
            .send()
            .await?;
        if response.status() != StatusCode::CREATED {
            return Err(runner_http_error("runner artifact upload failed", response).await);
        }
        Ok(response.json().await?)
    }

    pub async fn create_job_run(
        &self,
        runner: &Runner,
        runner_job_name: &str,
        idempotency_key: &str,
        body: Value,
    ) -> Result<JobCreatedResponse, Box<dyn std::error::Error + Send + Sync>> {
        let response = self
            .http
            .post(runner_url(
                runner,
                RunnerRoute::JobRuns {
                    job_name: runner_job_name,
                },
            ))
            .bearer_auth(&runner.token)
            .header(HEADER_IDEMPOTENCY_KEY, idempotency_key)
            .json(&body)
            .send()
            .await?;
        if !matches!(response.status(), StatusCode::CREATED | StatusCode::OK) {
            return Err(runner_http_error("runner create job failed", response).await);
        }
        Ok(response.json().await?)
    }

    pub async fn get_job_run(
        &self,
        runner: &Runner,
        runner_run_id: &str,
    ) -> Result<JobStatusResponse, Box<dyn std::error::Error + Send + Sync>> {
        let response = self
            .http
            .get(runner_url(
                runner,
                RunnerRoute::Run {
                    job_id: runner_run_id,
                },
            ))
            .bearer_auth(&runner.token)
            .send()
            .await?;
        if response.status() != StatusCode::OK {
            return Err(format!("runner get run failed with {}", response.status()).into());
        }
        Ok(response.json().await?)
    }

    pub async fn get_job_logs(
        &self,
        runner: &Runner,
        runner_run_id: &str,
    ) -> Result<JobLogsResponse, Box<dyn std::error::Error + Send + Sync>> {
        let response = self
            .http
            .get(runner_url(
                runner,
                RunnerRoute::RunLogs {
                    job_id: runner_run_id,
                },
            ))
            .bearer_auth(&runner.token)
            .send()
            .await?;
        if response.status() != StatusCode::OK {
            return Err(format!("runner logs failed with {}", response.status()).into());
        }
        Ok(response.json().await?)
    }

    pub async fn cancel_job_run(
        &self,
        runner: &Runner,
        runner_run_id: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let response = self
            .http
            .delete(runner_url(
                runner,
                RunnerRoute::Run {
                    job_id: runner_run_id,
                },
            ))
            .bearer_auth(&runner.token)
            .send()
            .await?;
        if response.status() != StatusCode::ACCEPTED {
            return Err(format!("runner cancel failed with {}", response.status()).into());
        }
        Ok(())
    }

    pub async fn download_artifact(
        &self,
        runner: &Runner,
        artifact_id: &str,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
        let response = self
            .http
            .get(runner_url(runner, RunnerRoute::Artifact { artifact_id }))
            .bearer_auth(&runner.token)
            .send()
            .await?;
        if response.status() != StatusCode::OK {
            return Err(runner_http_error("runner artifact download failed", response).await);
        }
        Ok(response.bytes().await?.to_vec())
    }
}

fn runner_url(runner: &Runner, route: RunnerRoute<'_>) -> String {
    format!("{}{}", runner.base_url.trim_end_matches('/'), route.path())
}

async fn runner_http_error(
    context: &'static str,
    response: reqwest::Response,
) -> Box<dyn std::error::Error + Send + Sync> {
    let status = response.status();
    let retry_after = response
        .headers()
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    let body = response.text().await.unwrap_or_default();
    let detail = serde_json::from_str::<Value>(&body)
        .ok()
        .and_then(|value| {
            let code = value.get("code").and_then(Value::as_str);
            let message = value.get("message").and_then(Value::as_str);
            match (code, message) {
                (Some(code), Some(message)) => Some(format!("{code}: {message}")),
                (Some(code), None) => Some(code.to_string()),
                (None, Some(message)) => Some(message.to_string()),
                (None, None) => None,
            }
        })
        .or_else(|| (!body.trim().is_empty()).then(|| body.trim().to_string()));
    let retry_after = retry_after
        .map(|value| format!(" retry_after={value}s"))
        .unwrap_or_default();
    match detail {
        Some(detail) => format!("{context} with {status}: {detail}{retry_after}").into(),
        None => format!("{context} with {status}{retry_after}").into(),
    }
}
