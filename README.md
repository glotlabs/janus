# Janus

Janus is a lightweight CI-style orchestration system.

The project is split into a server that owns repositories, workflows, and
pipeline scheduling; a runner that executes predefined jobs on worker hosts; and
a shared library that keeps the server/runner protocol types in sync.

## Workspace Layout

- [`janus-server`](janus-server/README.md) - orchestration service, admin UI,
  repository hooks, pipeline scheduling, and runner dispatch.
- [`janus-runner`](janus-runner/README.md) - HTTP worker service for running
  named jobs from local TOML manifests.
- [`janus-lib`](janus-lib/README.md) - shared wire types, schema types, protocol
  constants, and compatibility tests used by both services.

## Development

Build the whole workspace:

```bash
cargo build
```

Run the Rust test suite:

```bash
cargo test
```

Run JavaScript asset tests for the server UI:

```bash
./test_js.sh
```

For setup, configuration, and service-specific commands, start with the detailed
component documentation:

- [`janus-server/README.md`](janus-server/README.md)
- [`janus-runner/README.md`](janus-runner/README.md)
- [`janus-lib/README.md`](janus-lib/README.md)
