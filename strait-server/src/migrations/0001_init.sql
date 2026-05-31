CREATE TABLE IF NOT EXISTS users (
    id TEXT PRIMARY KEY,
    username TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    role TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sessions (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS repos (
    id TEXT PRIMARY KEY,
    owner_id TEXT NOT NULL,
    name TEXT NOT NULL,
    normalized_name TEXT NOT NULL,
    bare_path TEXT NOT NULL,
    default_branch TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(owner_id, normalized_name),
    FOREIGN KEY(owner_id) REFERENCES users(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS runners (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    base_url TEXT NOT NULL,
    enabled INTEGER NOT NULL,
    last_health_state TEXT NOT NULL,
    last_seen_at TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS runner_jobs (
    id TEXT PRIMARY KEY,
    runner_id TEXT NOT NULL,
    job_name TEXT NOT NULL,
    concurrency TEXT NOT NULL DEFAULT 'parallel',
    timeout_seconds INTEGER NOT NULL DEFAULT 0,
    last_refreshed_at TEXT NOT NULL,
    UNIQUE(runner_id, job_name),
    FOREIGN KEY(runner_id) REFERENCES runners(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS runner_job_inputs (
    id TEXT PRIMARY KEY,
    runner_job_id TEXT NOT NULL,
    input_name TEXT NOT NULL,
    input_type TEXT NOT NULL,
    required INTEGER NOT NULL,
    sensitive INTEGER NOT NULL DEFAULT 0,
    max_length INTEGER,
    pattern TEXT,
    max_json_bytes INTEGER,
    UNIQUE(runner_job_id, input_name),
    FOREIGN KEY(runner_job_id) REFERENCES runner_jobs(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS runner_job_outputs (
    id TEXT PRIMARY KEY,
    runner_job_id TEXT NOT NULL,
    output_name TEXT NOT NULL,
    output_type TEXT NOT NULL,
    required INTEGER NOT NULL,
    output_path TEXT NOT NULL DEFAULT '',
    UNIQUE(runner_job_id, output_name),
    FOREIGN KEY(runner_job_id) REFERENCES runner_jobs(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS workflows (
    id TEXT PRIMARY KEY,
    repo_id TEXT NOT NULL,
    name TEXT NOT NULL,
    enabled INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY(repo_id) REFERENCES repos(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS workflow_versions (
    id TEXT PRIMARY KEY,
    workflow_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    trigger_json TEXT NOT NULL,
    definition_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(workflow_id, version),
    FOREIGN KEY(workflow_id) REFERENCES workflows(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS workflow_version_job_schemas (
    id TEXT PRIMARY KEY,
    workflow_version_id TEXT NOT NULL,
    job_index INTEGER NOT NULL,
    job_name TEXT NOT NULL,
    concurrency TEXT NOT NULL DEFAULT 'parallel',
    timeout_seconds INTEGER NOT NULL DEFAULT 0,
    UNIQUE(workflow_version_id, job_index),
    FOREIGN KEY(workflow_version_id) REFERENCES workflow_versions(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS workflow_version_job_schema_inputs (
    id TEXT PRIMARY KEY,
    workflow_version_job_schema_id TEXT NOT NULL,
    input_name TEXT NOT NULL,
    input_type TEXT NOT NULL,
    required INTEGER NOT NULL,
    sensitive INTEGER NOT NULL DEFAULT 0,
    max_length INTEGER,
    pattern TEXT,
    max_json_bytes INTEGER,
    UNIQUE(workflow_version_job_schema_id, input_name),
    FOREIGN KEY(workflow_version_job_schema_id) REFERENCES workflow_version_job_schemas(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS workflow_version_job_schema_outputs (
    id TEXT PRIMARY KEY,
    workflow_version_job_schema_id TEXT NOT NULL,
    output_name TEXT NOT NULL,
    output_type TEXT NOT NULL,
    required INTEGER NOT NULL,
    output_path TEXT NOT NULL DEFAULT '',
    UNIQUE(workflow_version_job_schema_id, output_name),
    FOREIGN KEY(workflow_version_job_schema_id) REFERENCES workflow_version_job_schemas(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS push_events (
    id TEXT PRIMARY KEY,
    repo_id TEXT NOT NULL,
    received_at TEXT NOT NULL,
    event_key TEXT NOT NULL UNIQUE,
    processed_at TEXT,
    FOREIGN KEY(repo_id) REFERENCES repos(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS push_event_refs (
    id TEXT PRIMARY KEY,
    push_event_id TEXT NOT NULL,
    old_rev TEXT NOT NULL,
    new_rev TEXT NOT NULL,
    ref_name TEXT NOT NULL,
    FOREIGN KEY(push_event_id) REFERENCES push_events(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    id TEXT PRIMARY KEY,
    repo_id TEXT NOT NULL,
    workflow_id TEXT NOT NULL,
    workflow_version_id TEXT NOT NULL,
    trigger_type TEXT NOT NULL,
    trigger_ref TEXT,
    commit_sha TEXT,
    status TEXT NOT NULL,
    started_at TEXT NOT NULL,
    cancel_reason TEXT,
    cancel_requested_at TEXT,
    cancel_started_at TEXT,
    finished_at TEXT,
    FOREIGN KEY(repo_id) REFERENCES repos(id) ON DELETE CASCADE,
    FOREIGN KEY(workflow_id) REFERENCES workflows(id) ON DELETE CASCADE,
    FOREIGN KEY(workflow_version_id) REFERENCES workflow_versions(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS job_runs (
    id TEXT PRIMARY KEY,
    pipeline_run_id TEXT NOT NULL,
    job_index INTEGER NOT NULL,
    runner_id TEXT NOT NULL,
    runner_job_name TEXT NOT NULL,
    dispatch_idempotency_key TEXT NOT NULL,
    runner_run_id TEXT,
    status TEXT NOT NULL,
    outcome_policy TEXT NOT NULL,
    started_at TEXT,
    duration_ms INTEGER,
    exit_code INTEGER,
    terminal_reason TEXT,
    failure_category TEXT,
    cancel_reason TEXT,
    cancel_requested_at TEXT,
    cancel_started_at TEXT,
    cancel_retry_count INTEGER NOT NULL DEFAULT 0,
    last_cancel_retry_at TEXT,
    infra_retry_count INTEGER NOT NULL DEFAULT 0,
    last_infra_retry_at TEXT,
    finished_at TEXT,
    output_metadata_json TEXT,
    input_payload_json TEXT,
    FOREIGN KEY(pipeline_run_id) REFERENCES pipeline_runs(id) ON DELETE CASCADE,
    FOREIGN KEY(runner_id) REFERENCES runners(id) ON DELETE CASCADE
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_job_runs_dispatch_idempotency_key
    ON job_runs(dispatch_idempotency_key);

CREATE TABLE IF NOT EXISTS job_run_previous (
    job_run_id TEXT NOT NULL,
    previous_job_run_id TEXT NOT NULL,
    PRIMARY KEY(job_run_id, previous_job_run_id),
    FOREIGN KEY(job_run_id) REFERENCES job_runs(id) ON DELETE CASCADE,
    FOREIGN KEY(previous_job_run_id) REFERENCES job_runs(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS job_run_artifacts (
    id TEXT PRIMARY KEY,
    job_run_id TEXT NOT NULL,
    artifact_name TEXT NOT NULL,
    artifact_role TEXT NOT NULL,
    output_type TEXT NOT NULL,
    runner_artifact_id TEXT,
    server_artifact_id TEXT,
    value_json TEXT,
    sha256 TEXT,
    size_bytes INTEGER,
    FOREIGN KEY(job_run_id) REFERENCES job_runs(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS job_run_logs (
    job_run_id TEXT PRIMARY KEY,
    stdout TEXT NOT NULL,
    stderr TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(job_run_id) REFERENCES job_runs(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS server_artifacts (
    id TEXT PRIMARY KEY,
    scope_type TEXT NOT NULL,
    scope_id TEXT NOT NULL,
    artifact_name TEXT NOT NULL,
    sha256 TEXT NOT NULL,
    size_bytes INTEGER NOT NULL,
    storage_path TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(scope_type, scope_id, artifact_name)
);
