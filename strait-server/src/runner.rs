use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::models::Runner;

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
            .get(format!("{}/jobs", runner.base_url.trim_end_matches('/')))
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
            .post(format!(
                "{}/artifacts",
                runner.base_url.trim_end_matches('/')
            ))
            .bearer_auth(&runner.token)
            .header("x-sha256", sha256)
            .body(bytes)
            .send()
            .await?;
        if response.status() != StatusCode::CREATED {
            return Err(format!("runner artifact upload failed with {}", response.status()).into());
        }
        Ok(response.json().await?)
    }

    pub async fn create_job_run(
        &self,
        runner: &Runner,
        runner_job_name: &str,
        body: Value,
    ) -> Result<JobCreatedResponse, Box<dyn std::error::Error + Send + Sync>> {
        let response = self
            .http
            .post(format!(
                "{}/jobs/{}/runs",
                runner.base_url.trim_end_matches('/'),
                runner_job_name
            ))
            .bearer_auth(&runner.token)
            .json(&body)
            .send()
            .await?;
        if response.status() != StatusCode::CREATED {
            return Err(format!("runner create job failed with {}", response.status()).into());
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
            .get(format!(
                "{}/runs/{}",
                runner.base_url.trim_end_matches('/'),
                runner_run_id
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
            .get(format!(
                "{}/runs/{}/logs",
                runner.base_url.trim_end_matches('/'),
                runner_run_id
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
            .delete(format!(
                "{}/runs/{}",
                runner.base_url.trim_end_matches('/'),
                runner_run_id
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
            .get(format!(
                "{}/artifacts/{}",
                runner.base_url.trim_end_matches('/'),
                artifact_id
            ))
            .bearer_auth(&runner.token)
            .send()
            .await?;
        if response.status() != StatusCode::OK {
            return Err(format!("runner artifact download failed with {}", response.status()).into());
        }
        Ok(response.bytes().await?.to_vec())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunnerJobDefinition {
    pub name: String,
    #[serde(flatten)]
    pub definition: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArtifactUploadResponse {
    pub artifact_id: String,
    pub sha256: String,
    pub size: u64,
    pub expires_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobCreatedResponse {
    pub job_id: String,
    pub status: String,
    pub started_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobStatusResponse {
    pub job_id: String,
    pub name: String,
    pub status: String,
    pub started_at: String,
    pub finished_at: Option<String>,
    pub exit_code: Option<i32>,
    #[serde(default)]
    pub outputs: std::collections::BTreeMap<String, JobOutputResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobOutputResponse {
    pub artifact_id: String,
    pub sha256: String,
    pub size: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobLogsResponse {
    pub stdout: String,
    pub stderr: String,
}
