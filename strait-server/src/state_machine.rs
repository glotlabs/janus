#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobStatus {
    Pending,
    Running,
    CancelRequested,
    Canceling,
    Success,
    Failed,
    Canceled,
    Blocked,
    Skipped,
}

impl JobStatus {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "pending" => Some(Self::Pending),
            "running" => Some(Self::Running),
            "cancel_requested" => Some(Self::CancelRequested),
            "canceling" => Some(Self::Canceling),
            "success" => Some(Self::Success),
            "failed" => Some(Self::Failed),
            "canceled" => Some(Self::Canceled),
            "blocked" => Some(Self::Blocked),
            "skipped" => Some(Self::Skipped),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::CancelRequested => "cancel_requested",
            Self::Canceling => "canceling",
            Self::Success => "success",
            Self::Failed => "failed",
            Self::Canceled => "canceled",
            Self::Blocked => "blocked",
            Self::Skipped => "skipped",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PipelineStatus {
    Running,
    CancelRequested,
    Canceling,
    Success,
    Failed,
    Canceled,
    Blocked,
}

impl PipelineStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::CancelRequested => "cancel_requested",
            Self::Canceling => "canceling",
            Self::Success => "success",
            Self::Failed => "failed",
            Self::Canceled => "canceled",
            Self::Blocked => "blocked",
        }
    }
}

pub fn next_ready_job_status(
    dependency_statuses: impl Iterator<Item = (JobStatus, bool)>,
) -> Option<JobStatus> {
    let statuses = dependency_statuses.collect::<Vec<_>>();
    if statuses
        .iter()
        .any(|(status, allow_failure)| *status == JobStatus::Failed && !allow_failure)
    {
        return Some(JobStatus::Blocked);
    }
    if statuses.iter().any(|(status, _)| {
        matches!(
            status,
            JobStatus::Pending
                | JobStatus::Running
                | JobStatus::CancelRequested
                | JobStatus::Canceling
        )
    }) {
        return None;
    }
    if statuses
        .iter()
        .any(|(status, _)| *status == JobStatus::Canceled)
    {
        return Some(JobStatus::Blocked);
    }
    Some(JobStatus::Pending)
}

pub fn terminal_pipeline_status(
    statuses: impl Iterator<Item = (JobStatus, bool)>,
) -> PipelineStatus {
    let statuses = statuses.collect::<Vec<_>>();
    if !statuses.is_empty()
        && statuses
            .iter()
            .all(|(status, _)| matches!(status, JobStatus::Success | JobStatus::Skipped))
    {
        return PipelineStatus::Success;
    }
    if statuses
        .iter()
        .any(|(status, allow_failure)| *status == JobStatus::Failed && !allow_failure)
    {
        return PipelineStatus::Failed;
    }
    if statuses
        .iter()
        .any(|(status, _)| *status == JobStatus::Blocked)
    {
        return PipelineStatus::Blocked;
    }
    if statuses
        .iter()
        .all(|(status, _)| *status == JobStatus::Canceled)
        && !statuses.is_empty()
    {
        return PipelineStatus::Canceled;
    }
    if statuses
        .iter()
        .any(|(status, _)| *status == JobStatus::Canceling)
    {
        return PipelineStatus::Canceling;
    }
    if statuses
        .iter()
        .any(|(status, _)| *status == JobStatus::CancelRequested)
    {
        return PipelineStatus::CancelRequested;
    }
    PipelineStatus::Running
}
