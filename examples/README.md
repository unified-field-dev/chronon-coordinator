# chronon-coordinator examples

Canonical teaching path for inventory-backed default jobs — no database or Chronon
daemon required.

| Example | Role |
|---------|------|
| `register_default_jobs_embedded` | Manual `DefaultJobDescriptor` + register |
| `script_with_default_job` | `#[chronon_coordinator_macros::script]` + `default_job` |

## 1. Inventory descriptor — `register_default_jobs_embedded`

```bash
export CARGO_BUILD_JOBS=1
cargo run -p chronon-coordinator --example register_default_jobs_embedded
```

Success: stdout prints `registered welcome-email for script send_welcome_email`.

## 2. Script macro — `script_with_default_job`

When to use: product scripts that own both the handler and the embedded default job.

```bash
export CARGO_BUILD_JOBS=1
cargo run -p chronon-coordinator --example script_with_default_job
```

Success: stdout prints `script_with_default_job: OK — registered example-cleanup`.

API path: `#[chronon_coordinator_macros::script(..., default_job(...))]` → inventory
`DefaultJobDescriptor` → `register_default_jobs_embedded` → job listed on the local
backend. The handler recovers Valence via `chronon_valence_identity::valence_from_context`.

Look next at `chronon-coordinator-macros` for the attribute surface; hosts replace
`LocalBackend` with a real coordinator backend.
