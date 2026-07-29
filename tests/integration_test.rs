//! Integration tests: `Scheduler`, `JobBuilder`, and typed scheduling helpers.
//!
//! Test-only fixtures below are intentionally undocumented; this binary target is exempt from
//! the library's `missing_docs = "deny"` lint (see `Cargo.toml`).
#![allow(missing_docs)]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use chronon_coordinator::{
    snapshot_actor_json, snapshot_job_actor_from_valence, typed_job_ref_for_script,
    validate_external_job_actor_json, ChrononCoordinatorBackend, ChrononError, Job, JobBuilder,
    JobRevision, Result, Run, ScheduleKind, Scheduler, ScriptHandle, ScriptScheduler,
};
use serde::Serialize;
use valence::{Actor, InMemoryBackend, Valence};

fn test_valence() -> Valence {
    Valence::builder()
        .add_backend("default", Arc::new(InMemoryBackend::new()))
        .with_actor(Actor::System {
            operation: "chronon_coordinator_test".into(),
        })
        .build()
        .expect("valence")
}

const fn demo_handle() -> ScriptHandle<()> {
    ScriptHandle::<()>::new("demo_script")
}

/// Minimal in-memory backend for `ScriptScheduler` / `TypedJobRef` coverage.
struct MemBackend {
    by_id: Mutex<HashMap<String, Job>>,
    by_name: Mutex<HashMap<String, String>>,
    runs: Mutex<Vec<String>>,
    last_params_override: Mutex<Option<Option<serde_json::Value>>>,
}

impl MemBackend {
    fn new() -> Self {
        Self {
            by_id: Mutex::new(HashMap::new()),
            by_name: Mutex::new(HashMap::new()),
            runs: Mutex::new(Vec::new()),
            last_params_override: Mutex::new(None),
        }
    }

    fn store_job(&self, job: Job) {
        self.by_name
            .lock()
            .unwrap()
            .insert(job.job_name.clone(), job.job_id.clone());
        self.by_id.lock().unwrap().insert(job.job_id.clone(), job);
    }
}

#[async_trait]
impl ChrononCoordinatorBackend for MemBackend {
    async fn load_jobs_from_db(&self) -> Result<()> {
        Ok(())
    }

    async fn upsert_job(&self, job: Job) -> Result<()> {
        validate_external_job_actor_json(&job.actor_json)?;
        self.store_job(job);
        Ok(())
    }

    async fn upsert_job_with_valence(&self, valence: &Valence, mut job: Job) -> Result<()> {
        snapshot_job_actor_from_valence(&mut job, valence)?;
        self.store_job(job);
        Ok(())
    }

    async fn get_job(&self, job_id: &str) -> Option<Job> {
        self.by_id.lock().unwrap().get(job_id).cloned()
    }

    async fn get_job_by_name(&self, job_name: &str) -> Option<Job> {
        let id = self.by_name.lock().unwrap().get(job_name).cloned()?;
        self.get_job(&id).await
    }

    async fn list_jobs(&self) -> Vec<Job> {
        self.by_id.lock().unwrap().values().cloned().collect()
    }

    async fn list_runs(
        &self,
        _job_id: Option<&str>,
        _status: Option<&str>,
        _offset: usize,
        _limit: usize,
    ) -> Result<Vec<Run>> {
        Ok(vec![])
    }

    async fn get_run(&self, _run_id: &str) -> Result<Option<Run>> {
        Ok(None)
    }

    async fn pause_job(&self, _job_id: &str) -> Result<()> {
        Ok(())
    }

    async fn resume_job(&self, _job_id: &str) -> Result<()> {
        Ok(())
    }

    async fn list_revisions(&self, _job_id_or_name: &str) -> Result<Vec<JobRevision>> {
        Ok(vec![])
    }

    async fn update_job_config(&self, _job_id: &str, updated: Job) -> Result<()> {
        self.upsert_job(updated).await
    }

    async fn update_job_config_with_valence(
        &self,
        valence: &Valence,
        job_id: &str,
        updated: Job,
    ) -> Result<()> {
        let _ = job_id;
        self.upsert_job_with_valence(valence, updated).await
    }

    async fn run_now(&self, job_id: &str) -> Result<String> {
        self.run_now_with_params(job_id, None).await
    }

    async fn run_now_with_params(
        &self,
        job_id: &str,
        params_override: Option<serde_json::Value>,
    ) -> Result<String> {
        if self.get_job(job_id).await.is_none() {
            return Err(ChrononError::JobNotFound(job_id.to_string()));
        }
        *self.last_params_override.lock().unwrap() = Some(params_override);
        let run_id = format!("run-{job_id}");
        self.runs.lock().unwrap().push(run_id.clone());
        Ok(run_id)
    }
}

#[derive(Serialize)]
struct DemoParams {
    n: u32,
}

#[test]
fn scheduler_from_inventory_lists_scripts() {
    let scheduler = Scheduler::from_inventory();
    let names = scheduler.list_scripts();
    // Inventory may be empty in this crate alone; the call must still succeed.
    assert!(names.iter().all(|n| !n.is_empty()) || names.is_empty());
    let _ = scheduler.registry();
}

#[test]
fn job_builder_cron_happy_path() {
    let scheduler = Scheduler::from_inventory();
    let job = scheduler
        .script(&demo_handle())
        .with_valence(test_valence())
        .name("nightly-demo")
        .cron("0 0 * * * *")
        .expect("cron")
        .build()
        .expect("build");
    assert_eq!(job.job_name, "nightly-demo");
    assert_eq!(job.script_name, "demo_script");
    assert_eq!(job.schedule_kind, ScheduleKind::Cron);
    assert_eq!(job.cron_expr.as_deref(), Some("0 0 * * * *"));
    assert!(job.next_run_at.is_some());
}

#[test]
fn job_builder_invalid_cron_fails() {
    match JobBuilder::new(&demo_handle())
        .with_valence(test_valence())
        .name("bad-cron")
        .cron("not-a-cron")
    {
        Ok(_) => panic!("expected invalid cron"),
        Err(err) => assert!(matches!(err, ChrononError::InvalidCron(_))),
    }
}

#[test]
fn job_builder_validates_timezone_set_after_cron() {
    let result = JobBuilder::new(&demo_handle())
        .with_valence(test_valence())
        .name("bad-timezone")
        .cron("0 0 * * *")
        .expect("cron")
        .timezone("not-a-timezone")
        .build();

    assert!(matches!(result, Err(ChrononError::InvalidTimezone(_))));
}

#[test]
fn job_builder_missing_name_fails() {
    match JobBuilder::new(&demo_handle())
        .with_valence(test_valence())
        .build()
    {
        Ok(_) => panic!("expected missing name"),
        Err(ChrononError::ParamError(msg)) => assert!(msg.contains("job name")),
        Err(other) => panic!("expected ParamError, got {other:?}"),
    }
}

#[test]
fn job_builder_missing_valence_fails() {
    match JobBuilder::new(&demo_handle()).name("no-valence").build() {
        Ok(_) => panic!("expected missing valence"),
        Err(ChrononError::ParamError(msg)) => assert!(msg.contains("valence")),
        Err(other) => panic!("expected ParamError, got {other:?}"),
    }
}

#[test]
fn job_builder_manual_schedule() {
    let job = JobBuilder::new(&demo_handle())
        .with_valence(test_valence())
        .name("manual-demo")
        .manual()
        .build()
        .expect("build");
    assert_eq!(job.schedule_kind, ScheduleKind::Manual);
    assert!(job.next_run_at.is_none());
}

#[test]
fn job_builder_run_once_at_sets_schedule() {
    let at = Utc.with_ymd_and_hms(2031, 6, 1, 12, 0, 0).unwrap();
    let job = JobBuilder::new(&demo_handle())
        .with_valence(test_valence())
        .name("once-demo")
        .run_once_at(at)
        .build()
        .expect("build");
    assert_eq!(job.schedule_kind, ScheduleKind::RunOnce);
    assert_eq!(job.run_once_at, Some(at));
    assert_eq!(job.next_run_at, Some(at));
}

#[test]
fn snapshot_actor_json_roundtrip() {
    let valence = test_valence();
    let json = snapshot_actor_json(&valence).expect("snapshot");
    assert!(json.get("System").is_some() || json.to_string().contains("System"));
}

#[tokio::test]
async fn script_scheduler_add_happy_path() {
    let backend = MemBackend::new();
    let job = ScriptScheduler::new(&backend, &demo_handle(), test_valence())
        .name("scheduled-demo")
        .cron("0 */5 * * * *")
        .expect("cron")
        .add()
        .await
        .expect("add");
    assert_eq!(job.job_name, "scheduled-demo");
    let stored = backend
        .get_job_by_name("scheduled-demo")
        .await
        .expect("stored");
    assert_eq!(stored.script_name, "demo_script");
}

#[tokio::test]
async fn script_scheduler_invalid_cron_fails_before_upsert() {
    let backend = MemBackend::new();
    match ScriptScheduler::new(&backend, &demo_handle(), test_valence())
        .name("bad")
        .cron("%%%")
    {
        Ok(_) => panic!("expected invalid cron"),
        Err(err) => assert!(matches!(err, ChrononError::InvalidCron(_))),
    }
    assert!(backend.list_jobs().await.is_empty());
}

#[tokio::test]
async fn typed_job_ref_run_now_happy_path() {
    let backend = MemBackend::new();
    let job = ScriptScheduler::new(&backend, &demo_handle(), test_valence())
        .name("run-now-demo")
        .manual()
        .add()
        .await
        .expect("add");
    let job_ref = typed_job_ref_for_script::<()>(&backend, "run-now-demo", "demo_script")
        .await
        .expect("typed ref");
    assert_eq!(job_ref.job().job_id, job.job_id);
    let run_id = job_ref.run_now().await.expect("run_now");
    assert!(run_id.starts_with("run-"));
}

#[tokio::test]
async fn typed_job_ref_missing_job_fails() {
    let backend = MemBackend::new();
    match typed_job_ref_for_script::<()>(&backend, "missing", "demo_script").await {
        Ok(_) => panic!("expected missing job"),
        Err(err) => assert!(matches!(err, ChrononError::JobNotFound(_))),
    }
}

#[tokio::test]
async fn typed_job_ref_script_mismatch_fails() {
    let backend = MemBackend::new();
    ScriptScheduler::new(&backend, &demo_handle(), test_valence())
        .name("mismatch-demo")
        .manual()
        .add()
        .await
        .expect("add");
    match typed_job_ref_for_script::<()>(&backend, "mismatch-demo", "other_script").await {
        Ok(_) => panic!("expected script mismatch"),
        Err(err) => assert!(matches!(err, ChrononError::ScriptMismatch { .. })),
    }
}

#[tokio::test]
async fn typed_job_ref_run_now_with_params_override() {
    let backend = MemBackend::new();
    ScriptScheduler::new(&backend, &demo_handle(), test_valence())
        .name("params-demo")
        .manual()
        .add()
        .await
        .expect("add");
    let job_ref = typed_job_ref_for_script::<DemoParams>(&backend, "params-demo", "demo_script")
        .await
        .expect("typed ref");
    let run_id = job_ref
        .params(DemoParams { n: 7 })
        .run_now()
        .await
        .expect("run_now");
    assert!(run_id.starts_with("run-"));
    let override_seen = backend
        .last_params_override
        .lock()
        .unwrap()
        .clone()
        .expect("params captured");
    let params = override_seen.expect("Some override");
    assert_eq!(params["n"], 7);
}

/// Backend that rejects every upsert (sad path for `ScriptScheduler::add`).
struct FailUpsert;

#[async_trait]
impl ChrononCoordinatorBackend for FailUpsert {
    async fn load_jobs_from_db(&self) -> Result<()> {
        Ok(())
    }

    async fn upsert_job(&self, _job: Job) -> Result<()> {
        Err(ChrononError::Internal("upsert denied".into()))
    }

    async fn upsert_job_with_valence(&self, _valence: &Valence, _job: Job) -> Result<()> {
        Err(ChrononError::Internal("upsert denied".into()))
    }

    async fn get_job(&self, _job_id: &str) -> Option<Job> {
        None
    }

    async fn get_job_by_name(&self, _job_name: &str) -> Option<Job> {
        None
    }

    async fn list_jobs(&self) -> Vec<Job> {
        vec![]
    }

    async fn list_runs(
        &self,
        _job_id: Option<&str>,
        _status: Option<&str>,
        _offset: usize,
        _limit: usize,
    ) -> Result<Vec<Run>> {
        Ok(vec![])
    }

    async fn get_run(&self, _run_id: &str) -> Result<Option<Run>> {
        Ok(None)
    }

    async fn pause_job(&self, _job_id: &str) -> Result<()> {
        Ok(())
    }

    async fn resume_job(&self, _job_id: &str) -> Result<()> {
        Ok(())
    }

    async fn list_revisions(&self, _job_id_or_name: &str) -> Result<Vec<JobRevision>> {
        Ok(vec![])
    }

    async fn update_job_config(&self, _job_id: &str, _updated: Job) -> Result<()> {
        Ok(())
    }

    async fn update_job_config_with_valence(
        &self,
        _valence: &Valence,
        _job_id: &str,
        _updated: Job,
    ) -> Result<()> {
        Ok(())
    }

    async fn run_now(&self, _job_id: &str) -> Result<String> {
        Err(ChrononError::Internal("unused".into()))
    }

    async fn run_now_with_params(
        &self,
        _job_id: &str,
        _params_override: Option<serde_json::Value>,
    ) -> Result<String> {
        Err(ChrononError::Internal("unused".into()))
    }
}

#[tokio::test]
async fn script_scheduler_add_propagates_upsert_failure() {
    match ScriptScheduler::new(&FailUpsert, &demo_handle(), test_valence())
        .name("fail-add")
        .manual()
        .add()
        .await
    {
        Ok(_) => panic!("expected upsert failure"),
        Err(ChrononError::Internal(msg)) => assert!(msg.contains("upsert denied")),
        Err(other) => panic!("expected Internal, got {other:?}"),
    }
}
