pub const HEADER_IDEMPOTENCY_KEY: &str = "x-idempotency-key";
pub const HEADER_SHA256: &str = "x-sha256";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunnerRouteTemplate {
    Capabilities,
    Jobs,
    Artifacts,
    Artifact,
    JobRuns,
    Run,
    RunLogs,
}

impl RunnerRouteTemplate {
    pub fn path(self) -> &'static str {
        match self {
            Self::Capabilities => "/capabilities",
            Self::Jobs => "/jobs",
            Self::Artifacts => "/artifacts",
            Self::Artifact => "/artifacts/{artifact_id}",
            Self::JobRuns => "/jobs/{name}/runs",
            Self::Run => "/runs/{job_id}",
            Self::RunLogs => "/runs/{job_id}/logs",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunnerRoute<'a> {
    Capabilities,
    Jobs,
    Artifacts,
    Artifact { artifact_id: &'a str },
    JobRuns { job_name: &'a str },
    Run { job_id: &'a str },
    RunLogs { job_id: &'a str },
}

impl RunnerRoute<'_> {
    pub fn path(self) -> String {
        match self {
            Self::Capabilities => RunnerRouteTemplate::Capabilities.path().to_string(),
            Self::Jobs => RunnerRouteTemplate::Jobs.path().to_string(),
            Self::Artifacts => RunnerRouteTemplate::Artifacts.path().to_string(),
            Self::Artifact { artifact_id } => format!("/artifacts/{artifact_id}"),
            Self::JobRuns { job_name } => format!("/jobs/{job_name}/runs"),
            Self::Run { job_id } => format!("/runs/{job_id}"),
            Self::RunLogs { job_id } => format!("/runs/{job_id}/logs"),
        }
    }

    pub fn template(self) -> RunnerRouteTemplate {
        match self {
            Self::Capabilities => RunnerRouteTemplate::Capabilities,
            Self::Jobs => RunnerRouteTemplate::Jobs,
            Self::Artifacts => RunnerRouteTemplate::Artifacts,
            Self::Artifact { .. } => RunnerRouteTemplate::Artifact,
            Self::JobRuns { .. } => RunnerRouteTemplate::JobRuns,
            Self::Run { .. } => RunnerRouteTemplate::Run,
            Self::RunLogs { .. } => RunnerRouteTemplate::RunLogs,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runner_paths_match_route_templates() {
        assert_eq!(RunnerRoute::Capabilities.path(), "/capabilities");
        assert_eq!(RunnerRoute::Jobs.path(), "/jobs");
        assert_eq!(RunnerRoute::Artifacts.path(), "/artifacts");
        assert_eq!(
            RunnerRoute::JobRuns {
                job_name: "build-app"
            }
            .path(),
            "/jobs/build-app/runs"
        );
        assert_eq!(
            RunnerRoute::Run { job_id: "job_123" }.path(),
            "/runs/job_123"
        );
        assert_eq!(
            RunnerRoute::RunLogs { job_id: "job_123" }.path(),
            "/runs/job_123/logs"
        );
        assert_eq!(
            RunnerRoute::Artifact {
                artifact_id: "art_123"
            }
            .path(),
            "/artifacts/art_123"
        );
    }

    #[test]
    fn runner_route_templates_match_axum_paths() {
        assert_eq!(RunnerRouteTemplate::Capabilities.path(), "/capabilities");
        assert_eq!(RunnerRouteTemplate::Jobs.path(), "/jobs");
        assert_eq!(RunnerRouteTemplate::Artifacts.path(), "/artifacts");
        assert_eq!(
            RunnerRouteTemplate::Artifact.path(),
            "/artifacts/{artifact_id}"
        );
        assert_eq!(RunnerRouteTemplate::JobRuns.path(), "/jobs/{name}/runs");
        assert_eq!(RunnerRouteTemplate::Run.path(), "/runs/{job_id}");
        assert_eq!(RunnerRouteTemplate::RunLogs.path(), "/runs/{job_id}/logs");
        assert_eq!(
            RunnerRoute::RunLogs { job_id: "job_123" }.template(),
            RunnerRouteTemplate::RunLogs
        );
    }
}
