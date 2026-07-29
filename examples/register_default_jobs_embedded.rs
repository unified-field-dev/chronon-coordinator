//! Registers an inventory-backed default job with a local, in-memory host backend.
//!
//! Run with:
//! `CARGO_BUILD_JOBS=1 cargo run --example register_default_jobs_embedded`
#![allow(missing_docs)]
#![allow(clippy::expect_used, clippy::print_stdout)]

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use chronon_coordinator::{
    snapshot_job_actor_from_valence, validate_external_job_actor_json, ChrononCoordinatorBackend,
    DefaultJobDescriptor, Job, JobBuilder, JobRevision, Result, Run, ScriptHandle,
};
use valence::{
    DatabaseRouter, InMemoryBackend, RouterValenceFactory, RouterValenceFactoryConfig, Valence,
    ValenceFactory, DEFAULT_IN_MEMORY_ROUTER_KEY,
};

const WELCOME_JOB: &str = "welcome-email";

/// A host-owned backend suitable only for this local example.
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

    async fn upsert_job_with_valence(&self, valence: &Valence, mut job: Job) -> Result<()> {
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
        valence: &Valence,
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

fn welcome_script() -> ScriptHandle<()> {
    ScriptHandle::new("send_welcome_email")
}

fn ensure_welcome_job(
    backend: Arc<dyn ChrononCoordinatorBackend>,
    valence: Valence,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<()>> + Send>> {
    Box::pin(async move {
        let valence_for_upsert = valence.clone();
        let job = JobBuilder::new(&welcome_script())
            .with_valence(valence)
            .name(WELCOME_JOB)
            .manual()
            .build()?;
        backend
            .upsert_job_with_valence(&valence_for_upsert, job)
            .await?;
        Ok(())
    })
}

chronon_coordinator::inventory::submit! {
    DefaultJobDescriptor {
        job_name: WELCOME_JOB,
        ensure: ensure_welcome_job,
    }
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
async fn main() {
    let local = Arc::new(LocalBackend::default());
    let backend: Arc<dyn ChrononCoordinatorBackend> = Arc::clone(&local) as _;

    chronon_coordinator::register_default_jobs_embedded(backend, in_memory_factory()).await;

    for job in local.list_jobs().await {
        println!("registered {} for script {}", job.job_name, job.script_name);
    }
}
