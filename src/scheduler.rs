//! Script-registry handle for Chronon discovery (no runtime loop).

use std::sync::Arc;

use chronon_core::ScriptHandle;
use chronon_executor::ScriptRegistry;

use crate::JobBuilder;

/// Chronon scheduler handle over the upstream [`ScriptRegistry`].
///
/// Holds the upstream [`ScriptRegistry`] for discovery in host / UI server functions.
/// Runtime tick/worker loops live in upstream `chronon-runtime`; this type exposes
/// inventory and registry discovery only.
///
/// # Examples
///
/// ```rust,ignore
/// use chronon_coordinator::Scheduler;
///
/// let scheduler = Scheduler::from_inventory();
/// let names = scheduler.list_scripts();
/// # let _ = names;
/// ```
pub struct Scheduler {
    registry: Arc<ScriptRegistry>,
}

impl Scheduler {
    /// Wrap an existing upstream registry (typically from a built `Chronon` runtime).
    pub const fn from_registry(registry: Arc<ScriptRegistry>) -> Self {
        Self { registry }
    }

    /// Discover scripts from link-time inventory (no runtime required).
    pub fn from_inventory() -> Self {
        Self {
            registry: Arc::new(ScriptRegistry::from_inventory()),
        }
    }

    /// Shared registry handle.
    pub fn registry(&self) -> &ScriptRegistry {
        &self.registry
    }

    /// Shared registry as `Arc`.
    pub fn registry_arc(&self) -> Arc<ScriptRegistry> {
        Arc::clone(&self.registry)
    }

    /// List all registered script names.
    pub fn list_scripts(&self) -> Vec<String> {
        self.registry
            .list()
            .into_iter()
            .map(|d| d.name.to_string())
            .collect()
    }

    /// Start building a job for a registered script handle.
    pub fn script<P>(&self, handle: &ScriptHandle<P>) -> JobBuilder<P>
    where
        P: serde::Serialize,
    {
        JobBuilder::new(handle)
    }
}
