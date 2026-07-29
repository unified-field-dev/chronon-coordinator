//! Default-job bootstrap contract: `ensure_*` / `register_*` over inventory fixtures.
//!
//! Link-time `DefaultJobDescriptor` submissions apply to this integration binary only.
#![allow(missing_docs)]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use chronon_coordinator::{
    ensure_default_jobs_embedded, ensure_default_jobs_embedded_with_skip,
    register_default_jobs_embedded, register_default_jobs_embedded_with_skip,
    snapshot_job_actor_from_valence, validate_external_job_actor_json, ChrononCoordinatorBackend,
    ChrononError, DefaultJobDescriptor, DefaultJobRegistry, Job, JobBuilder, JobRevision, Result,
    Run, ScriptHandle,
};
use valence::{
    Actor, InMemoryBackend, RouterValenceFactory, RouterValenceFactoryConfig, Valence,
    ValenceFactory, DEFAULT_IN_MEMORY_ROUTER_KEY,
};

const PROBE_JOB: &str = "coord-probe-job";

fn probe_handle() -> ScriptHandle<()> {
    ScriptHandle::<()>::new("coord_probe_script")
}

fn test_valence() -> Valence {
    Valence::builder()
        .add_backend("default", Arc::new(InMemoryBackend::new()))
        .with_actor(Actor::System {
            operation: "chronon_coordinator_default_jobs_test".into(),
        })
        .build()
        .expect("valence")
}

fn mem_factory() -> Arc<dyn ValenceFactory> {
    let mut router = valence::DatabaseRouter::new();
    router.register(
        DEFAULT_IN_MEMORY_ROUTER_KEY.to_string(),
        Arc::new(InMemoryBackend::new()),
    );
    RouterValenceFactory::arc(
        Arc::new(router),
        RouterValenceFactoryConfig::new(DEFAULT_IN_MEMORY_ROUTER_KEY),
    )
}

struct FailValenceFactory;

impl ValenceFactory for FailValenceFactory {
    fn build(&self, _actor_json: &serde_json::Value) -> valence::Result<Valence> {
        Err(valence::Error::Identity(
            "default-jobs factory build failed".into(),
        ))
    }
}

/// Succeeds once (probe), then fails on subsequent builds (ensure rebuild).
struct ProbeThenFailFactory {
    calls: std::sync::atomic::AtomicUsize,
}

impl ProbeThenFailFactory {
    fn new() -> Self {
        Self {
            calls: std::sync::atomic::AtomicUsize::new(0),
        }
    }
}

impl ValenceFactory for ProbeThenFailFactory {
    fn build(&self, actor_json: &serde_json::Value) -> valence::Result<Valence> {
        let n = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if n == 0 {
            mem_factory().build(actor_json)
        } else {
            Err(valence::Error::Identity(
                "rebuild after successful probe failed".into(),
            ))
        }
    }
}

/// In-memory backend that records upsert job names (and can deny upserts).
struct RecordingBackend {
    by_id: Mutex<HashMap<String, Job>>,
    by_name: Mutex<HashMap<String, String>>,
    upserted: Mutex<Vec<String>>,
    deny_upsert: bool,
}

impl RecordingBackend {
    fn new() -> Self {
        Self {
            by_id: Mutex::new(HashMap::new()),
            by_name: Mutex::new(HashMap::new()),
            upserted: Mutex::new(Vec::new()),
            deny_upsert: false,
        }
    }

    fn denying() -> Self {
        Self {
            deny_upsert: true,
            ..Self::new()
        }
    }

    fn store_job(&self, job: Job) -> Result<()> {
        if self.deny_upsert {
            return Err(ChrononError::Internal("upsert denied".into()));
        }
        self.upserted.lock().unwrap().push(job.job_name.clone());
        self.by_name
            .lock()
            .unwrap()
            .insert(job.job_name.clone(), job.job_id.clone());
        self.by_id.lock().unwrap().insert(job.job_id.clone(), job);
        Ok(())
    }
}

#[async_trait]
impl ChrononCoordinatorBackend for RecordingBackend {
    async fn load_jobs_from_db(&self) -> Result<()> {
        Ok(())
    }

    async fn upsert_job(&self, job: Job) -> Result<()> {
        validate_external_job_actor_json(&job.actor_json)?;
        self.store_job(job)
    }

    async fn upsert_job_with_valence(&self, valence: &Valence, mut job: Job) -> Result<()> {
        snapshot_job_actor_from_valence(&mut job, valence)?;
        self.store_job(job)
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
        _params_override: Option<serde_json::Value>,
    ) -> Result<String> {
        if self.get_job(job_id).await.is_none() {
            return Err(ChrononError::JobNotFound(job_id.to_string()));
        }
        Ok(format!("run-{job_id}"))
    }
}

fn ensure_coord_probe_embedded(
    backend: Arc<dyn ChrononCoordinatorBackend>,
    valence: Valence,
) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>> {
    Box::pin(async move {
        let valence_for_upsert = valence.clone();
        let job = JobBuilder::new(&probe_handle())
            .with_valence(valence)
            .name(PROBE_JOB)
            .manual()
            .build()
            .map_err(|e| anyhow::anyhow!("build {PROBE_JOB}: {e}"))?;
        backend
            .upsert_job_with_valence(&valence_for_upsert, job)
            .await
            .map_err(|e| anyhow::anyhow!("ensure Chronon default job `{PROBE_JOB}`: {e}"))?;
        Ok(())
    })
}

chronon_coordinator::inventory::submit! {
    DefaultJobDescriptor {
        job_name: "coord-probe-job",
        ensure: ensure_coord_probe_embedded,
    }
}

#[test]
fn default_job_registry_discovers_probe() {
    let registry = DefaultJobRegistry::auto_discover();
    let names = registry.sorted_job_names();
    assert!(
        names.contains(&PROBE_JOB),
        "expected {PROBE_JOB} in {names:?}"
    );
    assert!(registry.get(PROBE_JOB).is_some());
}

#[tokio::test]
async fn ensure_default_jobs_embedded_upserts_registered_job() {
    let concrete = Arc::new(RecordingBackend::new());
    let backend: Arc<dyn ChrononCoordinatorBackend> = Arc::clone(&concrete) as _;
    ensure_default_jobs_embedded(backend, || Ok(test_valence()))
        .await
        .expect("bootstrap");
    let stored = concrete
        .get_job_by_name(PROBE_JOB)
        .await
        .expect("probe job upserted");
    assert_eq!(stored.script_name, "coord_probe_script");
    let upserted = concrete.upserted.lock().unwrap().clone();
    assert!(
        upserted.iter().any(|n| n == PROBE_JOB),
        "expected upsert of {PROBE_JOB}, got {upserted:?}"
    );
}

#[tokio::test]
async fn ensure_default_jobs_embedded_skip_omits_listed_jobs() {
    let concrete = Arc::new(RecordingBackend::new());
    let backend: Arc<dyn ChrononCoordinatorBackend> = Arc::clone(&concrete) as _;
    ensure_default_jobs_embedded_with_skip(backend, || Ok(test_valence()), &[PROBE_JOB])
        .await
        .expect("bootstrap with skip");
    let upserted = concrete.upserted.lock().unwrap().clone();
    assert!(
        !upserted.iter().any(|n| n == PROBE_JOB),
        "{PROBE_JOB} should be skipped, got {upserted:?}"
    );
    assert!(concrete.get_job_by_name(PROBE_JOB).await.is_none());
}

#[tokio::test]
async fn ensure_default_jobs_embedded_maps_upsert_failure() {
    let backend: Arc<dyn ChrononCoordinatorBackend> = Arc::new(RecordingBackend::denying());
    let err = ensure_default_jobs_embedded(backend, || Ok(test_valence()))
        .await
        .expect_err("upsert must fail");
    let msg = err.to_string();
    assert!(
        msg.contains(PROBE_JOB) && msg.contains("upsert denied"),
        "unexpected error: {msg}"
    );
}

#[tokio::test]
async fn register_default_jobs_embedded_happy_path() {
    let concrete = Arc::new(RecordingBackend::new());
    let backend: Arc<dyn ChrononCoordinatorBackend> = Arc::clone(&concrete) as _;
    register_default_jobs_embedded(backend, mem_factory()).await;
    let stored = concrete
        .get_job_by_name(PROBE_JOB)
        .await
        .expect("register upserted probe");
    assert_eq!(stored.job_name, PROBE_JOB);
}

#[tokio::test]
async fn register_default_jobs_embedded_with_skip_omits_listed() {
    let concrete = Arc::new(RecordingBackend::new());
    let backend: Arc<dyn ChrononCoordinatorBackend> = Arc::clone(&concrete) as _;
    register_default_jobs_embedded_with_skip(backend, mem_factory(), &[PROBE_JOB]).await;
    assert!(concrete.get_job_by_name(PROBE_JOB).await.is_none());
}

#[tokio::test]
async fn register_default_jobs_embedded_swallows_factory_failure() {
    let concrete = Arc::new(RecordingBackend::new());
    let backend: Arc<dyn ChrononCoordinatorBackend> = Arc::clone(&concrete) as _;
    // Fire-and-forget boot helper logs and returns — must not panic.
    register_default_jobs_embedded(backend, Arc::new(FailValenceFactory)).await;
    assert!(
        concrete.list_jobs().await.is_empty(),
        "factory failure must not upsert"
    );
}

#[tokio::test]
async fn register_default_jobs_embedded_swallows_rebuild_after_probe() {
    let concrete = Arc::new(RecordingBackend::new());
    let backend: Arc<dyn ChrononCoordinatorBackend> = Arc::clone(&concrete) as _;
    // Probe succeeds; ensure rebuild fails — must log and return, not panic.
    register_default_jobs_embedded(backend, Arc::new(ProbeThenFailFactory::new())).await;
    assert!(
        concrete.list_jobs().await.is_empty(),
        "rebuild failure after probe must not upsert"
    );
}

#[tokio::test]
async fn register_default_jobs_embedded_swallows_ensure_failure() {
    let concrete = Arc::new(RecordingBackend::denying());
    let backend: Arc<dyn ChrononCoordinatorBackend> = Arc::clone(&concrete) as _;
    register_default_jobs_embedded(backend, mem_factory()).await;
    assert!(concrete.list_jobs().await.is_empty());
}
