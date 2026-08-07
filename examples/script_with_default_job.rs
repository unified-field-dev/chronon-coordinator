//! `#[chronon_coordinator_macros::script]` + `default_job` → inventory → embedded register.
//!
//! ```bash
//! CARGO_BUILD_JOBS=1 cargo run -p chronon-coordinator --example script_with_default_job
//! ```
//!
//! Success: stdout prints `script_with_default_job: OK — registered example-cleanup`.

#![allow(missing_docs)]
#![allow(clippy::expect_used, clippy::print_stdout, clippy::unused_async)]

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use chronon_coordinator::{
    snapshot_job_actor_from_valence, validate_external_job_actor_json, ChrononCoordinatorBackend,
    Job, JobRevision, Result, Run,
};
use chronon_core::ScriptContext;
use chronon_valence_identity::valence_from_context;
use valence::{
    DatabaseRouter, InMemoryBackend, RouterValenceFactory, RouterValenceFactoryConfig,
    ValenceFactory, DEFAULT_IN_MEMORY_ROUTER_KEY,
};

const JOB_NAME: &str = "example-cleanup";

#[derive(Default)]
struct LocalBackend {
    jobs: Mutex<Vec<Job>>,
}

impl LocalBackend {
    fn store_job(&self, job: Job) {
        let mut jobs = self.jobs.lock().expect("local backend lock");
        if let Some(existing) = jobs
            .iter_mut()
            .find(|existing| existing.job_id == job.job_id)
        {
            *existing = job;
        } else {
            jobs.push(job);
        }
    }
}

#[async_trait]
impl ChrononCoordinatorBackend for LocalBackend {
    async fn load_jobs_from_db(&self) -> Result<()> {
        Ok(())
    }

    async fn upsert_job(&self, job: Job) -> Result<()> {
        validate_external_job_actor_json(&job.actor_json)?;
        self.store_job(job);
        Ok(())
    }

    async fn upsert_job_with_valence(
        &self,
        valence: &valence::Valence,
        mut job: Job,
    ) -> Result<()> {
        snapshot_job_actor_from_valence(&mut job, valence)?;
        self.store_job(job);
        Ok(())
    }

    async fn get_job(&self, job_id: &str) -> Option<Job> {
        self.jobs
            .lock()
            .expect("local backend lock")
            .iter()
            .find(|job| job.job_id == job_id)
            .cloned()
    }

    async fn get_job_by_name(&self, job_name: &str) -> Option<Job> {
        self.jobs
            .lock()
            .expect("local backend lock")
            .iter()
            .find(|job| job.job_name == job_name)
            .cloned()
    }

    async fn list_jobs(&self) -> Vec<Job> {
        self.jobs.lock().expect("local backend lock").clone()
    }

    async fn list_runs(
        &self,
        _job_id: Option<&str>,
        _status: Option<&str>,
        _offset: usize,
        _limit: usize,
    ) -> Result<Vec<Run>> {
        Ok(Vec::new())
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
        Ok(Vec::new())
    }

    async fn update_job_config(&self, _job_id: &str, updated: Job) -> Result<()> {
        self.upsert_job(updated).await
    }

    async fn update_job_config_with_valence(
        &self,
        valence: &valence::Valence,
        job_id: &str,
        updated: Job,
    ) -> Result<()> {
        let _ = job_id;
        self.upsert_job_with_valence(valence, updated).await
    }

    async fn run_now(&self, job_id: &str) -> Result<String> {
        Ok(format!("local-run-{job_id}"))
    }

    async fn run_now_with_params(
        &self,
        job_id: &str,
        _params_override: Option<serde_json::Value>,
    ) -> Result<String> {
        self.run_now(job_id).await
    }
}

#[chronon_coordinator_macros::script(
    name = "example_cleanup",
    default_job(job = "example-cleanup", manual)
)]
pub async fn example_cleanup(ctx: Box<dyn ScriptContext>) -> anyhow::Result<()> {
    let valence = valence_from_context(&*ctx)?;
    let _ = valence;
    Ok(())
}

fn in_memory_factory() -> Arc<dyn ValenceFactory> {
    let mut router = DatabaseRouter::new();
    router.register(
        DEFAULT_IN_MEMORY_ROUTER_KEY.to_string(),
        Arc::new(InMemoryBackend::new()),
    );
    RouterValenceFactory::arc(
        Arc::new(router),
        RouterValenceFactoryConfig::new(DEFAULT_IN_MEMORY_ROUTER_KEY),
    )
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    assert_eq!(ExampleCleanupScript::NAME, "example_cleanup");
    let _handle = ExampleCleanupScript::handle();

    let local = Arc::new(LocalBackend::default());
    let backend: Arc<dyn ChrononCoordinatorBackend> = Arc::clone(&local) as _;
    chronon_coordinator::register_default_jobs_embedded(backend, in_memory_factory()).await;

    let names: Vec<String> = local
        .list_jobs()
        .await
        .into_iter()
        .map(|j| j.job_name)
        .collect();
    anyhow::ensure!(
        names.iter().any(|n| n == JOB_NAME),
        "expected job {JOB_NAME} in {names:?}"
    );

    println!("script_with_default_job: OK — registered {JOB_NAME}");
    Ok(())
}
