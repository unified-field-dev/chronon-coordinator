# chronon-coordinator examples

Canonical teaching path for inventory-backed default jobs — no database or Chronon daemon
is required.

## `register_default_jobs_embedded` — inventory default job registration

Run when you want to see how `DefaultJobDescriptor` + `register_default_jobs_embedded` upsert
a job through `ChrononCoordinatorBackend` with Valence actor snapshotting.

```bash
export CARGO_BUILD_JOBS=1
cargo run -p chronon-coordinator --example register_default_jobs_embedded
```

Success: stdout prints `registered welcome-email for script send_welcome_email`.

See `examples/register_default_jobs_embedded.rs` for the `LocalBackend` trait
implementation, the `chronon_coordinator::inventory::submit!` block, and
`JobBuilder::with_valence` + `upsert_job_with_valence` flow. Hosts replace `LocalBackend`
with their real coordinator backend and call `register_default_jobs_embedded` at boot.
