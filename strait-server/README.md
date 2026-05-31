# strait-server

`strait-server` is the orchestration layer for managed repositories, workflows, and pipeline runs.

Current implementation:

- SQLite-backed user, session, repo, runner, workflow, push-event, pipeline, and job-run storage
- Server-rendered admin UI for login, users, repos, runners, workflows, and pipeline detail
- Bare repository provisioning with deterministic `post-receive` hook installation
- CLI hook ingestion via `strait-server hook post-receive --repo-id <repo_id>`
- Background scheduler that turns push events into pipeline runs and dispatches runnable jobs to `strait-runner`
- Runner health refresh and cached runner-job metadata
- Signed session cookies and local username/password auth

Commands:

```bash
cargo run -p strait-server -- serve
cargo run -p strait-server -- admin seed-user --username alice --password secret --role developer
cargo run -p strait-server -- admin reconcile-hooks
```

JavaScript asset tests use Node's built-in test runner and do not require a package manager:

```bash
./test_js.sh
```

Configuration:

- Start from [`server.example.toml`](/Users/petter/dev/Projects/strait-ci/strait-server/server.example.toml)
- Default config path is `server.toml`
- `hook post-receive` also accepts `--config <path>`

Workflow JSON shape:

```json
{
  "jobs": [
    {
      "id": "build",
      "name": "Build",
      "runner_id": "runner-uuid",
      "runner_job_name": "build-app",
      "needs": [],
      "inputs": {
        "commit": "$commit",
        "branch": "$branch",
        "source": "$source"
      },
      "allow_failure": false
    }
  ]
}
```

Supported input bindings:

- `$commit`
- `$branch`
- `$source`
- `$job.<job_id>.<artifact_name>`
