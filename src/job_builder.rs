//! Fluent builder for creating and scheduling jobs with Valence identity.
//!
//! Schedule construction (cron/run-once/manual, params, policies) is delegated to
//! [`chronon_scheduler::JobBuilder`]. This wrapper adds Valence actor snapshotting required
//! before [`JobBuilder::build`].

use chrono::{DateTime, Utc};
use chronon_core::{ChrononError, Job, MisfirePolicy, Result, RetryPolicy, ScriptHandle};
use serde::Serialize;
use serde_json::Value;
use valence::Valence;

/// Serializes [`Valence::actor`](valence::Valence::actor) for persisted job lineage
/// (`Job::actor_json`, revision `changed_by_actor_json`).
///
/// Callers should prefer subsystem APIs such as
/// [`ChrononCoordinatorBackend::upsert_job_with_valence`](crate::ChrononCoordinatorBackend::upsert_job_with_valence)
/// over duplicating this at the app boundary.
///
/// # Examples
///
/// ```rust,ignore
/// use chronon_coordinator::snapshot_actor_json;
///
/// # fn snapshot(valence: &valence::Valence) -> chronon_coordinator::Result<()> {
/// let actor_json = snapshot_actor_json(valence)?;
/// # let _ = actor_json;
/// # Ok(())
/// # }
/// ```
///
/// # Errors
///
/// Returns [`ChrononError::ParamError`] when the actor fails to serialize. Messages do not
/// embed the actor body.
pub fn snapshot_actor_json(valence: &Valence) -> Result<Value> {
    serde_json::to_value(valence.actor()).map_err(Into::into)
}

/// True when `actor_json` uses the well-known System object key (case-sensitive).
fn is_system_shaped_actor(actor_json: &Value) -> bool {
    actor_json.get("System").is_some()
}

/// Rejects forged System-shaped actors on untrusted
/// [`ChrononCoordinatorBackend::upsert_job`](crate::ChrononCoordinatorBackend::upsert_job) paths.
///
/// Backend implementations should call this before persisting caller-supplied
/// [`Job::actor_json`]. Trusted in-process callers with a live [`Valence`] should use
/// [`ChrononCoordinatorBackend::upsert_job_with_valence`](crate::ChrononCoordinatorBackend::upsert_job_with_valence)
/// (which snapshots via [`snapshot_job_actor_from_valence`]) instead.
///
/// # Errors
///
/// Returns [`ChrononError::ParamError`] with a stable message when `actor_json` is System-shaped.
pub fn validate_external_job_actor_json(actor_json: &Value) -> Result<()> {
    if is_system_shaped_actor(actor_json) {
        return Err(ChrononError::ParamError(
            "external upsert cannot use System-shaped actor_json".into(),
        ));
    }
    Ok(())
}

/// Overwrites [`Job::actor_json`] from `valence`, matching [`JobBuilder::build`].
///
/// Use from [`ChrononCoordinatorBackend::upsert_job_with_valence`](crate::ChrononCoordinatorBackend::upsert_job_with_valence)
/// before persisting; do not re-run [`validate_external_job_actor_json`] on the snapshot.
///
/// # Errors
///
/// Returns [`ChrononError::ParamError`] when the actor fails to serialize.
pub fn snapshot_job_actor_from_valence(job: &mut Job, valence: &Valence) -> Result<()> {
    job.actor_json = snapshot_actor_json(valence)?;
    Ok(())
}

/// Builder for creating a scheduled job with Valence identity capture.
///
/// Schedule fields are built by [`chronon_scheduler::JobBuilder`] (preferred Chronon construction
/// API). This type requires [`JobBuilder::with_valence`] and [`JobBuilder::name`] before
/// [`JobBuilder::build`].
///
/// # Examples
///
/// ```rust,ignore
/// # fn build(handle: chronon_core::ScriptHandle<()>, valence: valence::Valence) -> chronon_coordinator::Result<()> {
/// use chronon_coordinator::JobBuilder;
///
/// let job = JobBuilder::new(&handle)
///     .with_valence(valence)
///     .name("nightly-report")
///     .cron("0 0 * * * *")?
///     .timezone("UTC")
///     .build()?;
/// # let _ = job;
/// # Ok(())
/// # }
/// ```
#[must_use = "build the Job with JobBuilder::build"]
pub struct JobBuilder<P> {
    inner: chronon_scheduler::JobBuilder<P>,
    valence: Option<Valence>,
}

impl<P> JobBuilder<P>
where
    P: Serialize,
{
    /// Create a new job builder for the given script.
    pub fn new(handle: &ScriptHandle<P>) -> Self {
        Self {
            inner: chronon_scheduler::JobBuilder::new(handle),
            valence: None,
        }
    }

    /// Set the Valence context for identity capture.
    pub fn with_valence(mut self, valence: Valence) -> Self {
        self.valence = Some(valence);
        self
    }

    /// Borrow the configured Valence (required before [`Self::build`]).
    pub fn valence(&self) -> Option<&Valence> {
        self.valence.as_ref()
    }

    /// Set the job name (unique per deployment).
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.inner = self.inner.name(name);
        self
    }

    /// Set the cron schedule.
    ///
    /// # Errors
    ///
    /// Returns [`ChrononError::InvalidCron`] when `expr` is not a valid cron expression.
    pub fn cron(mut self, expr: &str) -> Result<Self> {
        self.inner = self.inner.cron(expr)?;
        Ok(self)
    }

    /// Set the timezone for cron evaluation.
    pub fn timezone(mut self, tz: impl Into<String>) -> Self {
        self.inner = self.inner.timezone(tz);
        self
    }

    /// Schedule a one-time execution at the specified time.
    pub fn run_once_at(mut self, at: DateTime<Utc>) -> Self {
        self.inner = self.inner.run_once_at(at);
        self
    }

    /// Set the job to manual-only (no automatic scheduling).
    pub fn manual(mut self) -> Self {
        self.inner = self.inner.manual();
        self
    }

    /// Set the script parameters.
    pub fn params(mut self, params: P) -> Self {
        self.inner = self.inner.params(params);
        self
    }

    /// Set the execution pool (for distributed mode).
    pub fn pool(mut self, pool: impl Into<String>) -> Self {
        self.inner = self.inner.pool(pool);
        self
    }

    /// Set the target region (for distributed mode).
    pub fn region(mut self, region: impl Into<String>) -> Self {
        self.inner = self.inner.region(region);
        self
    }

    /// Set maximum concurrent runs.
    pub fn concurrency(mut self, max: i32) -> Self {
        self.inner = self.inner.concurrency(max);
        self
    }

    /// Set execution timeout in milliseconds.
    pub fn timeout_ms(mut self, ms: i64) -> Self {
        self.inner = self.inner.timeout_ms(ms);
        self
    }

    /// Set the retry policy for failed runs.
    pub fn retry_policy(mut self, policy: RetryPolicy) -> Self {
        self.inner = self.inner.retry_policy(policy);
        self
    }

    /// Set the misfire policy for missed runs.
    pub fn misfire_policy(mut self, policy: MisfirePolicy) -> Self {
        self.inner = self.inner.misfire_policy(policy);
        self
    }

    /// Disable the job (it won't run until enabled).
    pub fn disabled(mut self) -> Self {
        self.inner = self.inner.disabled();
        self
    }

    /// Build the final `Job` payload, snapshotting Valence into `actor_json`.
    ///
    /// # Errors
    ///
    /// - [`ChrononError::ParamError`] when name or valence is missing
    /// - [`ChrononError::InvalidCron`] / [`ChrononError::InvalidTimezone`] from schedule construction
    /// - [`ChrononError::ParamError`] when params or actor snapshot fail to serialize
    pub fn build(self) -> Result<Job> {
        let valence = self
            .valence
            .ok_or_else(|| ChrononError::ParamError("valence context is required".to_string()))?;

        let mut job = self.inner.build()?;
        snapshot_job_actor_from_valence(&mut job, &valence)?;
        Ok(job)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use valence::{Actor, InMemoryBackend};

    fn test_valence() -> Valence {
        Valence::builder()
            .add_backend("default", Arc::new(InMemoryBackend::new()))
            .with_actor(Actor::System {
                operation: "job_builder_test".into(),
            })
            .build()
            .expect("valence")
    }

    #[test]
    fn validate_external_job_actor_json_rejects_forged_system() {
        let forged = serde_json::json!({"System": {"operation": "evil"}});
        let err = validate_external_job_actor_json(&forged).unwrap_err();
        assert!(err.to_string().contains("System"));
    }

    #[test]
    fn validate_external_job_actor_json_allows_service_actor() {
        let service = serde_json::json!({"Service": {"name": "chronon_api"}});
        validate_external_job_actor_json(&service).expect("service actor");
    }

    #[test]
    fn snapshot_job_actor_from_valence_matches_build() {
        let valence = test_valence();
        let built = JobBuilder::new(&ScriptHandle::<()>::new("probe"))
            .with_valence(valence.clone())
            .name("probe")
            .manual()
            .build()
            .expect("build");
        let mut job = Job::new("probe", "probe");
        snapshot_job_actor_from_valence(&mut job, &valence).expect("snapshot");
        assert_eq!(job.actor_json, built.actor_json);
    }

    #[test]
    fn build_missing_valence_is_param_error() {
        let err = JobBuilder::new(&ScriptHandle::<()>::new("probe"))
            .name("probe")
            .manual()
            .build()
            .unwrap_err();
        match err {
            ChrononError::ParamError(msg) => assert!(msg.contains("valence")),
            other => panic!("expected ParamError, got {other}"),
        }
    }

    #[test]
    fn build_missing_name_is_param_error() {
        let err = JobBuilder::new(&ScriptHandle::<()>::new("probe"))
            .with_valence(test_valence())
            .manual()
            .build()
            .unwrap_err();
        match err {
            ChrononError::ParamError(msg) => assert!(msg.contains("job name")),
            other => panic!("expected ParamError, got {other}"),
        }
    }
}
