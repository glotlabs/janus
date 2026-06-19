# janus-lib

`janus-lib` contains shared wire and schema types used by `janus-server` and
`janus-runner`.

The purpose of this crate is to keep protocol-level contracts in sync. If a
runner response, runner job schema, or serialized enum changes, both sides
should compile against the same Rust type instead of drifting independently.

## Module Layout

- `protocol`: shared HTTP header names and runner route templates/path builders.
- `capabilities`: runner protocol version constants and compatibility response.
- `artifact`: artifact DTOs that cross the server/runner boundary.
- `job`: job run DTOs, outputs, statuses, terminal reasons, and output metadata.
- `schema`: runner job definition DTOs and schema enums.

The crate root re-exports the public protocol surface so callers can use
`janus_lib::TypeName`. Keep implementation details inside the owning module and
add compatibility tests near the type or helper they protect.

## What Belongs Here

- Runner API DTOs shared across the server/runner HTTP boundary.
- Runner job schema types, including input/output kinds and concurrency values.
- Serialized enum values whose string representation is part of the protocol.
- Protocol constants such as shared HTTP header names and runner API route
  templates.
- Small pure helpers on shared types, such as `as_str`, `parse`, and
  `is_terminal`.
- Compatibility tests that lock JSON shape and enum casing.

## What Does Not Belong Here

- HTTP server or client code such as axum extractors, reqwest clients, routes,
  middleware, or auth handling.
- Database models, SQL mapping, migrations, or persistence helpers.
- Runtime execution logic, process management, scheduling, or cancellation
  orchestration.
- Server-owned workflow and pipeline state machines.
- Runner manifest loading from disk, filesystem validation, or script
  executability checks.
- Configuration structs, application state, logging setup, or tokio tasks.
- Heavy dependencies needed by only one binary.

When in doubt, keep `janus-lib` limited to types that are serialized,
deserialized, or compared by both `janus-server` and `janus-runner`.
