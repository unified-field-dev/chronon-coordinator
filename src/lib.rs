//! # Chronon coordinator
//!
//! Coordinator API for hosts integrating [Chronon]. An object-safe
//! [`ChrononCoordinatorBackend`] trait abstracts job/run/revision **admin** (upsert, run-now,
//! list, pause) behind one `dyn` interface. Fluent **schedule construction** lives in Chronon
//! ([`chronon_scheduler::JobBuilder`]); this crate's [`JobBuilder`] wraps that API with Valence
//! actor snapshots. Typed [`ScriptScheduler`] / [`TypedJobRef`] helpers and default-job bootstrap
//! from link-time inventory sit on top. Swapping how jobs are persisted and run later is a
//! backend swap, not a rewrite.
//!
//! [Chronon]: https://github.com/unified-field-dev/chronon
//!
//! This crate defines [`ChrononCoordinatorBackend`] and the typed helpers ([`JobBuilder`],
//! [`Scheduler`], default-job bootstrap). Runtime tick and worker loops live in upstream
//! `chronon-runtime`. Jobs are persisted and run through a host-supplied backend that implements
//! the trait.
//!
//! ## Features
//!
//! - **Object-safe backend trait** — [`ChrononCoordinatorBackend`] is `dyn`-compatible, so hosts
//!   hold `Arc<dyn ChrononCoordinatorBackend>` without caring how jobs, runs, and revisions are
//!   persisted or executed
//! - **Valence job builder** — [`JobBuilder`] delegates cron / run-once / manual construction to
//!   Chronon ([`chronon_scheduler::JobBuilder`]) and snapshots Valence into `actor_json`
//! - **Typed scheduling helpers** — [`ScriptScheduler`] wraps [`JobBuilder`] with a bound backend
//!   for a one-line `.add()`; [`typed_job_ref_for_script`] / [`TypedJobRef`] resolve an existing
//!   job by name and support typed one-off parameter override on [`TypedJobRef::run_now`]
//! - **Script discovery handle** — [`Scheduler`] exposes the upstream [`ScriptRegistry`] for script
//!   discovery in host / UI server functions, independent of any running Chronon runtime
//! - **Default-job bootstrap** — [`ensure_default_jobs_embedded`] (and the
//!   [`register_default_jobs_embedded`] wrapper below) upsert every job discovered via
//!   `#[chronon_coordinator_macros::script(..., default_job(...))]` inventory, so default jobs stay
//!   in sync as scripts are added, removed, or have their schedule attributes changed
//!
//! *One coordinator trait — the same product code administers and inspects Chronon jobs whether
//! they run in this process or behind a split runtime. Schedule fluent API stays in Chronon.*
//!
//! # Getting started
//!
//! Most hosts hold a single `Arc<dyn ChrononCoordinatorBackend>` (built by a host-specific
//! adapter crate wrapping upstream `chronon-runtime` or a remote API) and use the helpers below
//! on top of it. Product code depends on the trait, not the concrete backend, so it stays
//! portable if the backend implementation changes later.
//!
//! ## Discover scripts
//!
//! [`Scheduler`] wraps the upstream [`ScriptRegistry`] and works from link-time inventory alone —
//! inventory alone is enough:
//!
//! ```rust,ignore
//! use chronon_coordinator::Scheduler;
//!
//! let scheduler = Scheduler::from_inventory();
//! let names = scheduler.list_scripts();
//! # let _ = names;
//! ```
//!
//! ## Build and schedule a job
//!
//! Use [`JobBuilder`] when you hold a [`Valence`](valence::Valence) context, or
//! [`ScriptScheduler`] to build and persist in one chain. Schedule fluent API lives in Chronon;
//! this crate adds Valence lineage:
//!
//! ```rust,ignore
//! # async fn wire(
//! #     backend: &dyn chronon_coordinator::ChrononCoordinatorBackend,
//! #     handle: chronon_core::ScriptHandle<()>,
//! #     valence: valence::Valence,
//! # ) -> chronon_coordinator::Result<()> {
//! use chronon_coordinator::JobBuilder;
//!
//! let job = JobBuilder::new(&handle)
//!     .with_valence(valence)
//!     .name("nightly-report")
//!     .cron("0 0 * * * *")?
//!     .build()?;
//! backend.upsert_job_with_valence(&valence, job).await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Run a job now
//!
//! ```rust,ignore
//! # async fn run(backend: &dyn chronon_coordinator::ChrononCoordinatorBackend) -> chronon_coordinator::Result<()> {
//! let run_id = backend.run_now("job-id").await?;
//! # let _ = run_id;
//! # Ok(())
//! # }
//! ```
//!
//! ## Bootstrap default jobs at boot
//!
//! Safe to call on every boot: existing rows are updated in place when a default job's schedule
//! drifts from its `#[chronon_coordinator_macros::script(..., default_job(...))]` attributes.
//!
//! ```rust,ignore
//! # async fn boot(
//! #     backend: std::sync::Arc<dyn chronon_coordinator::ChrononCoordinatorBackend>,
//! #     factory: std::sync::Arc<dyn valence::ValenceFactory>,
//! # ) {
//! chronon_coordinator::register_default_jobs_embedded(backend, factory).await;
//! # }
//! ```
//!
//! ## Concern → API
//!
//! | Concern | API |
//! |---------|-----|
//! | Backend-agnostic job/run/revision admin | [`ChrononCoordinatorBackend`] — the trait every host adapter implements |
//! | Build a job with Valence (Chronon schedule + identity) | [`JobBuilder`] — Valence wrapper over [`chronon_scheduler::JobBuilder`] |
//! | Build-and-persist in one chain | [`ScriptScheduler`] — [`JobBuilder`] bound to a backend |
//! | Look up / run-now an existing job by name | [`typed_job_ref_for_script`] / [`TypedJobRef`] |
//! | Discover scripts without a running backend | [`Scheduler`] — handle over the upstream [`ScriptRegistry`] |
//! | Seed jobs at boot from `#[chronon_coordinator_macros::script]` inventory | [`ensure_default_jobs_embedded`] / [`register_default_jobs_embedded`] |
//!
//! Older import path: [`models`].
//!
//! Runnable examples: see `examples/README.md` in this crate.
//!
//! Jobs are persisted and run through a host-supplied [`ChrononCoordinatorBackend`] (for example
//! an in-process adapter over `chronon-runtime`, or a remote HTTP client).

mod coordinator_trait;
mod default_job;
mod job_builder;
mod scheduler;
mod typed;

pub use chronon_core::{
    ChrononError, Job, JobRevision, MisfirePolicy, Result, RetryPolicy, Run, RunStatus,
    ScheduleKind, Script, ScriptHandle,
};
/// Upstream script registry re-export, shared by [`Scheduler`] and [`ScriptHandle`] discovery.
pub use chronon_executor::ScriptRegistry;
/// Parsed cron expression re-export, used by [`JobBuilder::cron`].
pub use chronon_scheduler::CronExpr;
pub use coordinator_trait::ChrononCoordinatorBackend;
pub use default_job::{
    default_job_schedule_equivalent, ensure_default_jobs_embedded,
    ensure_default_jobs_embedded_with_skip, merge_default_job_schedule_fields,
    DefaultJobDescriptor, DefaultJobEnsureFn, DefaultJobRegistry,
};
pub use job_builder::{
    snapshot_actor_json, snapshot_job_actor_from_valence, validate_external_job_actor_json,
    JobBuilder,
};
pub use quark::inventory;
pub use scheduler::Scheduler;
pub use typed::{typed_job_ref_for_script, ScriptScheduler, TypedJobRef};

/// Re-exports model types under [`models`] for the `chronon_coordinator::models::*` import path.
pub mod models {
    pub use chronon_core::{
        Job, JobRevision, MisfirePolicy, RetryPolicy, Run, RunStatus, ScheduleKind, Script,
    };
}

/// Register default jobs discovered via link-time inventory using a host Valence factory.
///
/// Builds a fresh `Actor::System` [`Valence`](valence::Valence) context per job via `factory`,
/// then delegates to [`ensure_default_jobs_embedded`]. Errors are logged (target
/// `"chronon_default_jobs"`) rather than propagated, since this is typically called from
/// fire-and-forget boot code; use [`ensure_default_jobs_embedded`] directly if you need to
/// handle failures.
///
/// # Examples
///
/// ```rust,ignore
/// # async fn boot(
/// #     backend: std::sync::Arc<dyn chronon_coordinator::ChrononCoordinatorBackend>,
/// #     factory: std::sync::Arc<dyn valence::ValenceFactory>,
/// # ) {
/// chronon_coordinator::register_default_jobs_embedded(backend, factory).await;
/// # }
/// ```
pub async fn register_default_jobs_embedded(
    backend: std::sync::Arc<dyn ChrononCoordinatorBackend>,
    factory: std::sync::Arc<dyn valence::ValenceFactory>,
) {
    register_default_jobs_embedded_with_skip(backend, factory, &[]).await;
}

/// Like [`register_default_jobs_embedded`], but skips job names in `skip`.
///
/// Use this when a host manages some default jobs' schedule by hand (e.g. an admin UI has
/// already customized it) and does not want this bootstrap to overwrite it.
pub async fn register_default_jobs_embedded_with_skip(
    backend: std::sync::Arc<dyn ChrononCoordinatorBackend>,
    factory: std::sync::Arc<dyn valence::ValenceFactory>,
    skip: &[&str],
) {
    use valence::Actor;

    let actor = Actor::System {
        operation: "chronon_default_jobs".into(),
    };
    let actor_json = match serde_json::to_value(&actor) {
        Ok(json) => json,
        Err(err) => {
            log::error!(
                target: "chronon_default_jobs",
                "failed to serialize system actor for default jobs: {err}"
            );
            return;
        }
    };
    if let Err(err) = factory.build(&actor_json) {
        log::error!(
            target: "chronon_default_jobs",
            "valence build for default jobs: {err}"
        );
        return;
    }
    let factory = std::sync::Arc::clone(&factory);
    let build_valence = move || {
        factory.build(&actor_json).map_err(|err| {
            anyhow::anyhow!("valence build for default jobs after successful probe: {err}")
        })
    };
    match ensure_default_jobs_embedded_with_skip(backend, build_valence, skip).await {
        Ok(()) => log::info!(target: "chronon_default_jobs", "default Chronon jobs registered"),
        Err(e) => log::error!(
            target: "chronon_default_jobs",
            "default Chronon jobs failed: {e}"
        ),
    }
}
