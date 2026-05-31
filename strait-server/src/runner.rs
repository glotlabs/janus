use std::time::Duration;

use reqwest::{Client, Method, StatusCode};
use serde_json::Value;
pub use strait_lib::{
    ArtifactUploadResponse, HEADER_IDEMPOTENCY_KEY, HEADER_SHA256, JobCreatedResponse,
    JobLogsResponse, JobOutputMetadata, JobStatusResponse, RunnerCapabilitiesResponse, RunnerRoute,
};

const RUNNER_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const RUNNER_REQUEST_TIMEOUT: Duration = Duration::from_secs(120);

use crate::{
    models::{Runner, RunnerJobDefinition},
    runner_auth::RunnerSigner,
};

#[derive(Debug, Clone)]
pub struct RunnerClient {
    http: Client,
    signer: RunnerSigner,
}

impl RunnerClient {
    pub(crate) fn new(signer: RunnerSigner) -> Self {
        let http = Client::builder()
            .connect_timeout(RUNNER_CONNECT_TIMEOUT)
            .timeout(RUNNER_REQUEST_TIMEOUT)
            .build()
            .expect("runner http client");
        Self { http, signer }
    }

    pub async fn capabilities(
        &self,
        runner: &Runner,
    ) -> Result<RunnerCapabilitiesResponse, Box<dyn std::error::Error + Send + Sync>> {
        let response = self
            .signed_request(runner, Method::GET, RunnerRoute::Capabilities, Vec::new())
            .send()
            .await?;
        if response.status() != StatusCode::OK {
            return Err(format!("runner capabilities returned {}", response.status()).into());
        }
        Ok(response.json().await?)
    }

    pub async fn list_jobs(
        &self,
        runner: &Runner,
    ) -> Result<Vec<RunnerJobDefinition>, Box<dyn std::error::Error + Send + Sync>> {
        let response = self
            .signed_request(runner, Method::GET, RunnerRoute::Jobs, Vec::new())
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
            .signed_request(runner, Method::POST, RunnerRoute::Artifacts, bytes)
            .header(HEADER_SHA256, sha256)
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
        let bytes = serde_json::to_vec(&body)?;
        let response = self
            .signed_request(
                runner,
                Method::POST,
                RunnerRoute::JobRuns {
                    job_name: runner_job_name,
                },
                bytes,
            )
            .header(HEADER_IDEMPOTENCY_KEY, idempotency_key)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
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
            .signed_request(
                runner,
                Method::GET,
                RunnerRoute::Run {
                    job_id: runner_run_id,
                },
                Vec::new(),
            )
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
            .signed_request(
                runner,
                Method::GET,
                RunnerRoute::RunLogs {
                    job_id: runner_run_id,
                },
                Vec::new(),
            )
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
            .signed_request(
                runner,
                Method::DELETE,
                RunnerRoute::Run {
                    job_id: runner_run_id,
                },
                Vec::new(),
            )
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
            .signed_request(
                runner,
                Method::GET,
                RunnerRoute::Artifact { artifact_id },
                Vec::new(),
            )
            .send()
            .await?;
        if response.status() != StatusCode::OK {
            return Err(runner_http_error("runner artifact download failed", response).await);
        }
        Ok(response.bytes().await?.to_vec())
    }

    fn signed_request(
        &self,
        runner: &Runner,
        method: Method,
        route: RunnerRoute<'_>,
        body: Vec<u8>,
    ) -> reqwest::RequestBuilder {
        let path = route.path();
        let headers = self.signer.sign(method.as_str(), &path, &body);
        headers.apply(
            self.http
                .request(method, runner_url_from_path(runner, &path))
                .body(body),
        )
    }
}

fn runner_url_from_path(runner: &Runner, path: &str) -> String {
    format!("{}{}", runner.base_url.trim_end_matches('/'), path)
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
