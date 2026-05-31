use std::time::Duration;

use reqwest::{Client, Method, StatusCode};
use serde::de::DeserializeOwned;
use serde_json::Value;
pub use strait_lib::{
    ArtifactUploadResponse, HEADER_IDEMPOTENCY_KEY, HEADER_SHA256, JobCreatedResponse,
    JobLogsResponse, JobOutputMetadata, JobStatusResponse, RunnerCapabilitiesResponse, RunnerRoute,
};

use crate::{
    config::{LimitsConfig, RunnersConfig},
    models::{Runner, RunnerJobDefinition},
    runner_auth::RunnerSigner,
};

#[derive(Debug, Clone)]
pub struct RunnerClient {
    http: Client,
    signer: RunnerSigner,
    limits: LimitsConfig,
}

impl RunnerClient {
    pub(crate) fn new(signer: RunnerSigner, limits: LimitsConfig, runners: RunnersConfig) -> Self {
        let http = Client::builder()
            .connect_timeout(Duration::from_secs(runners.connect_timeout_seconds))
            .timeout(Duration::from_secs(runners.request_timeout_seconds))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .expect("runner http client");
        Self {
            http,
            signer,
            limits,
        }
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
        read_json_response(response, self.limits.runner_json_bytes).await
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
        read_json_response(response, self.limits.runner_json_bytes).await
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
            return Err(runner_http_error(
                "runner artifact upload failed",
                response,
                self.limits.runner_error_bytes,
            )
            .await);
        }
        read_json_response(response, self.limits.runner_json_bytes).await
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
            return Err(runner_http_error(
                "runner create job failed",
                response,
                self.limits.runner_error_bytes,
            )
            .await);
        }
        read_json_response(response, self.limits.runner_json_bytes).await
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
        read_json_response(response, self.limits.runner_json_bytes).await
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
        read_json_response(response, self.limits.runner_logs_bytes).await
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
            return Err(runner_http_error(
                "runner artifact download failed",
                response,
                self.limits.runner_error_bytes,
            )
            .await);
        }
        read_response_bytes(response, self.limits.runner_artifact_bytes).await
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
    max_error_bytes: usize,
) -> Box<dyn std::error::Error + Send + Sync> {
    let status = response.status();
    let retry_after = response
        .headers()
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    let body = read_response_bytes(response, max_error_bytes)
        .await
        .map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
        .unwrap_or_default();
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

async fn read_json_response<T: DeserializeOwned>(
    response: reqwest::Response,
    max_bytes: usize,
) -> Result<T, Box<dyn std::error::Error + Send + Sync>> {
    let bytes = read_response_bytes(response, max_bytes).await?;
    Ok(serde_json::from_slice(&bytes)?)
}

async fn read_response_bytes(
    mut response: reqwest::Response,
    max_bytes: usize,
) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
    if let Some(length) = response.content_length()
        && length > max_bytes as u64
    {
        return Err(format!("runner response exceeded {max_bytes} bytes").into());
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        if bytes.len().saturating_add(chunk.len()) > max_bytes {
            return Err(format!("runner response exceeded {max_bytes} bytes").into());
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}
