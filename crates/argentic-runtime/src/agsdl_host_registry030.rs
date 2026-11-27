//! Host-owned registrations. Each supervisor builds one executor per used harness.
use crate::{
    host::HostContext,
    persistence::{failure, Result},
};
use futures::future::BoxFuture;
use serde_json::Value;
use std::{collections::BTreeMap, sync::Arc};

pub use crate::agsdl_host_action_approval030::{
    CommandApprovals030, Respond030, RespondArgentic030,
};
pub use crate::agsdl_host_argentic030::{ArgenticAdapter030, ArgenticConfig030};
pub use crate::agsdl_host_flow030::operator030::AgentOperator030;
pub use crate::agsdl_host_flow030::FlowWorkAttempt030;
pub use crate::agsdl_host_runtime030::{AgentExecutor030, AgentWork030, WorkEventSink030};
pub use crate::agsdl_host_steer030::{SteerDelivery030, SteerInbox030};
pub use crate::agsdl_host_work_admission030::{
    AttestedWorkPolicy030, NativePlanLock030, PlanEffectPermit030, PreparedEffectiveAgentWork030,
};

/// Installed harness and model catalogue, observed for this start.
pub struct HarnessObservation030 {
    pub version: String,
    /// Registry model entries with name, efforts and defaultEffort.
    pub models: Vec<Value>,
    pub default_model: Option<String>,
    /// Registry capability entries with name, support and evidenceJson.
    pub capabilities: Vec<Value>,
    pub evidence: Value,
    pub version_receipt: String,
    /// Host observation included when deriving the registry version.
    pub fingerprint: Value,
}

pub struct AgentExecutorContext030<'a> {
    pub host: &'a HostContext,
    pub document: &'a Value,
    pub approvals: Arc<dyn CommandApprovals030>,
}

pub trait HarnessAdapter030: Send + Sync {
    fn id(&self) -> &str;
    fn adapter_id(&self) -> &str;
    fn module_json(&self) -> &str;

    /// Local validation of an authored agent. Called before installation observation.
    fn qualify_agent(&self, _agent: &Value, _pointer: &str) -> Result<()> {
        Ok(())
    }

    fn validate_document(&self, _document: &Value, _location: Option<&url::Url>) -> Result<()> {
        Ok(())
    }

    fn observe<'a>(
        &'a self,
        host: &'a HostContext,
        scope: &'a Value,
        read_only: bool,
    ) -> BoxFuture<'a, Result<HarnessObservation030>>;

    fn validate_observation(
        &self,
        _document: &Value,
        _scope: &Value,
        _observed: &HarnessObservation030,
    ) -> Result<()> {
        Ok(())
    }

    /// Create one executor for the run. Provider work begins in AgentExecutor030::run.
    fn executor(&self, context: AgentExecutorContext030<'_>) -> Result<Arc<dyn AgentExecutor030>>;

    fn supports_host_approval(&self) -> bool {
        true
    }
    fn supports_mcp_equipment(&self) -> bool {
        false
    }

    fn opt_in_error(&self) -> crate::persistence::dto::EngineFailure {
        failure(
            "HARNESS_OPT_IN_REQUIRED",
            "source",
            &format!(
                "Explicit {} harness opt-in is required for preflight or Start",
                self.id()
            ),
        )
    }
}

#[derive(Clone, Default)]
pub struct HarnessRegistry030(BTreeMap<String, Arc<dyn HarnessAdapter030>>);

impl HarnessRegistry030 {
    pub fn with_codex() -> Self {
        static CODEX: std::sync::OnceLock<Arc<dyn HarnessAdapter030>> = std::sync::OnceLock::new();
        let adapter = CODEX.get_or_init(|| {
            Arc::new(crate::agsdl_host_codex030::registration::CodexAdapter030::new())
        });
        Self(BTreeMap::from([(adapter.id().to_owned(), adapter.clone())]))
    }

    pub fn register(&mut self, adapter: Arc<dyn HarnessAdapter030>) -> Result<()> {
        use std::collections::btree_map::Entry;
        let module: Value = serde_json::from_str(adapter.module_json())?;
        let id = adapter.id();
        if id.is_empty() || adapter.adapter_id().is_empty() || module["name"] != id {
            return Err(failure(
                "HARNESS_REGISTRATION",
                "harness",
                "Harness id and module name must match and the adapter id must be present",
            ));
        }
        match self.0.entry(id.to_owned()) {
            Entry::Vacant(entry) => {
                entry.insert(adapter);
                Ok(())
            }
            Entry::Occupied(_) => Err(failure(
                "HARNESS_REGISTRATION",
                "harness",
                &format!("Harness {id} is already registered"),
            )),
        }
    }

    pub fn get(&self, id: &str) -> Option<&dyn HarnessAdapter030> {
        self.0.get(id).map(AsRef::as_ref)
    }

    pub fn iter(&self) -> impl Iterator<Item = &dyn HarnessAdapter030> {
        self.0.values().map(AsRef::as_ref)
    }

    pub(crate) fn available(&self) -> argentic_core::calculation::AvailableHarnesses {
        self.iter()
            .map(|adapter| (adapter.id().to_owned(), adapter.adapter_id().to_owned()))
            .collect()
    }
}

pub(crate) fn missing_executor(
    agent: &str,
    harness: &str,
) -> crate::persistence::dto::EngineFailure {
    let agent = crate::agsdl_host_work_admission030::pointer_token030(agent);
    failure(
        "UNSUPPORTED_030_FEATURE",
        &format!("/agents/{agent}/harness"),
        &format!("No executor is registered for harness {harness}."),
    )
}
