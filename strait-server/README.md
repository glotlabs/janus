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
cargo run -p strait-server -- admin runner-key init --config server.toml
cargo run -p strait-server -- admin runner-key show --config server.toml
cargo run -p strait-server -- admin runner-key show --format toml --config server.toml
cargo run -p strait-server -- admin runner-key rotate --config server.toml
```

Initialize runner signing keys once before first `serve`; this generates `[runner_auth]`, creates the Ed25519 keypair, and writes the generated key ID into the server config.

Runner signing key rotation generates a new key ID, writes a new Ed25519 keypair, updates `[runner_auth]` in the server config, and leaves existing public key files in place for rollout. Add the new public key to every runner as another `[[auth.servers]]` entry before restarting the server with the new key; remove the old runner entry after old in-flight requests have drained.

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
      "outcome_policy": "required"
    }
  ]
}
```

Supported input bindings:

- `$commit`
- `$branch`
- `$source`
- `$job.<job_id>.<artifact_name>`
