//! Live operator controls. A committed hold reserves its machine clock change
//! before the supervisor asks the adapter to interrupt the turn.
use super::*;
use crate::{
    agsdl_host_projection030::RuntimeProjectionScope030, persistence::flow030::OperatorEvent030,
};
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum OperatorAction {
    InterruptWork { activation_id: String },
    ResumeWork { activation_id: String },
    PauseRun,
    ResumeRun,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct OperatorRequest {
    pub scope: RuntimeProjectionScope030,
    pub request_id: String,
    pub action: OperatorAction,
}

pub(crate) fn conflict(message: &str) -> repository::dto::EngineFailure {
    failure("OPERATOR_TRANSITION", "operator", message)
}

pub(crate) async fn control(
    host: &HostState,
    pool: &SqlitePool,
    request: OperatorRequest,
) -> Result<FlowReceipt030> {
    if request.request_id.is_empty() || request.request_id.len() > 128 {
        return Err(failure(
            "INPUT_CONSTRAINT",
            "requestId",
            "Invalid operator request ID",
        ));
    }
    let system = &request.scope.system_start_id;
    let lease = host.start_lease().await?;
    let owner = lease.owner(system)?;
    let runtime = host.flow030();
    let (dispatch, supports_hold) = runtime
        .supervisors
        .dispatcher(system)
        .ok_or_else(|| conflict("The run has no live supervisor"))?;
    let _dispatch = dispatch.lock().await;
    let inspection = inspect_run(runtime, host, pool, &owner).await?;
    let capture = &inspection.capture;
    if capture.source != request.scope.source
        || capture.identity.snapshot_id != request.scope.snapshot_id
    {
        return Err(failure(
            "CORRELATION",
            "scope",
            "Operator target differs from the retained run",
        ));
    }
    if capture.versions.contract != argentic_core::AGSDL_040 {
        return Err(failure(
            "HOST_CAPABILITY",
            "operator",
            "Operator controls require AgSDL 0.4.0",
        ));
    }
    let broker = runtime.humans.broker(host, pool, &capture.identity).await?;
    let _clock = broker.operation.lock().await;
    // Ordered with approval submissions and any reserved clock retirement.
    if !inspection.flow.state.stopping && inspection.flow.state.verdict.is_none() {
        broker.pump(host, &CancellationToken::new()).await?;
    }
    let _admission = host.admission_lock().lock_owned().await;
    let authority = Authority {
        runtime,
        lease: &lease,
        owner,
    };
    let mut db = pool.acquire().await?;
    sqlx::query("PRAGMA synchronous=FULL")
        .execute(&mut *db)
        .await?;
    let mut tx = db.begin_with("BEGIN IMMEDIATE").await?;
    let id = &capture.identity;
    let event_id = format!(
        "operator:{:x}",
        Sha256::digest(request.request_id.as_bytes())
    );
    let previous = sqlx::query("SELECT event_json,receipt_json,diagnostic_json FROM engine_flow030_events WHERE system_id=? AND invocation_id=? AND event_id=?")
        .bind(system).bind(&id.flow_invocation_id).bind(&event_id).fetch_optional(&mut *tx).await?;
    let view = flow_store030::read_tx(&mut tx, &authority, id)
        .await
        .map_err(store_error)?;
    let state = &view.state;
    if let Some(previous) = previous {
        let value: Value = serde_json::from_str(previous.get("event_json"))?;
        let recorded = &value["kind"]["Operator"];
        let matches = match request.action {
            OperatorAction::InterruptWork { activation_id } => {
                recorded == &json!({"Hold":{"activation_id":activation_id}})
            }
            OperatorAction::ResumeWork { activation_id } => {
                recorded == &json!({"ResumeWork":{"activation_id":activation_id}})
            }
            OperatorAction::PauseRun => recorded.get("Pause").is_some(),
            OperatorAction::ResumeRun => recorded == "ResumeRun",
        };
        if !matches {
            return Err(failure(
                "CONFLICT",
                "requestId",
                "Operator request ID has another action",
            ));
        }
        if let Some(marker) = runtime.pending_event_commit(id, &event_id)? {
            return operator_receipt(
                resolve_native_event_commit_tx(runtime, &lease, tx, &marker)
                    .await?
                    .ok_or_else(uncertain)?,
            );
        }
        let receipt = previous.get::<Option<&str>, _>("receipt_json");
        return match receipt {
            Some(receipt) => Ok(serde_json::from_str(receipt)?),
            None => operator_receipt(Apply030::Diagnostic(serde_json::from_str(
                previous.get("diagnostic_json"),
            )?)),
        };
    }
    if state.stopping || state.verdict.is_some() {
        return Err(conflict("The run is stopping or terminal"));
    }
    let active = sqlx::query_scalar::<_, String>("SELECT effect_id FROM engine_flow030_handoffs WHERE system_id=? AND invocation_id=? AND receipt_json ->> '$.kind'='nativeOwnedHandoff030'")
        .bind(system).bind(&id.flow_invocation_id).fetch_all(&mut *tx).await?;
    let admitted = |a: &repository::flow030::Activation030| {
        a.phase == "running"
            && a.physical
            && a.stop_effect_id.is_none()
            && a.start_effect_id
                .as_ref()
                .is_some_and(|id| active.contains(id))
    };
    let check_work = |activation_id: &str| {
        if state.paused {
            return Err(conflict("Resume the run to release its held work"));
        }
        if !supports_hold {
            return Err(failure(
                "HOST_CAPABILITY",
                "operator",
                "This agent executor cannot hold work",
            ));
        }
        if !state
            .activations
            .iter()
            .any(|a| a.id == activation_id && a.agent.is_some() && admitted(a))
        {
            return Err(conflict("The target is not an admitted, active agent work"));
        }
        Ok(())
    };
    let kind = match request.action {
        OperatorAction::InterruptWork { activation_id } => {
            check_work(&activation_id)?;
            OperatorEvent030::Hold { activation_id }
        }
        OperatorAction::ResumeWork { activation_id } => {
            check_work(&activation_id)?;
            match state.operator_holds.get(&activation_id) {
                Some(hold) if hold.ready => (),
                Some(_) => return Err(conflict("The turn interruption is still pending")),
                None => return Err(conflict("The work is not held")),
            }
            OperatorEvent030::ResumeWork { activation_id }
        }
        OperatorAction::PauseRun => {
            let plan: repository::flow030::FlowPlan030 =
                serde_json::from_str(&capture.compiled_plan_json)?;
            if state.activations.iter().filter(|a| admitted(a)).any(|a| {
                a.step.as_ref().is_some_and(|address| {
                    plan.steps
                        .iter()
                        .any(|s| &s.address == address && s.r#type == "call")
                })
            }) {
                return Err(failure(
                    "HOST_CAPABILITY",
                    "operator",
                    "An admitted host call cannot be suspended",
                ));
            }
            let activation_ids: Vec<_> = state
                .activations
                .iter()
                .filter(|a| a.agent.is_some() && admitted(a))
                .map(|a| a.id.clone())
                .collect();
            if !activation_ids.is_empty() && !supports_hold {
                return Err(failure(
                    "HOST_CAPABILITY",
                    "operator",
                    "This agent executor cannot hold work",
                ));
            }
            OperatorEvent030::Pause { activation_ids }
        }
        OperatorAction::ResumeRun => {
            if !state.paused {
                return Err(conflict("The run is not paused"));
            }
            if state.operator_holds.values().any(|hold| !hold.ready) {
                return Err(conflict("A turn interruption is still pending"));
            }
            OperatorEvent030::ResumeRun
        }
    };
    let event = FlowEvent030 {
        identity: id.clone(),
        event_id,
        expected_revision: state.revision.clone(),
        kind: FlowEventKind030::Operator(kind),
    };
    let applied =
        apply_native_event_transaction030(runtime, &lease, tx, &event, Some((&broker, state)))
            .await?;
    runtime.supervisors.wake(system);
    operator_receipt(applied)
}

fn operator_receipt(applied: Apply030) -> Result<FlowReceipt030> {
    match applied {
        Apply030::Applied(receipt) | Apply030::Replayed(receipt) => Ok(receipt),
        Apply030::Diagnostic(_) => Err(conflict("The run has a retained execution diagnostic")),
    }
}

/// The executor acknowledges only after the native turn has ended. The
/// activation, attempt and agent instance continue to belong to the run.
pub(crate) struct AgentOperator030 {
    host: HostState,
    pool: SqlitePool,
    attempt: FlowWorkAttempt030,
    commands: watch::Receiver<Option<String>>,
    parked: Arc<AtomicBool>,
}

impl AgentOperator030 {
    pub(crate) fn new(
        host: HostState,
        pool: SqlitePool,
        attempt: FlowWorkAttempt030,
        commands: watch::Receiver<Option<String>>,
        parked: Arc<AtomicBool>,
    ) -> Self {
        Self {
            host,
            pool,
            attempt,
            commands,
            parked,
        }
    }

    /// Serialize the provider turn write with Stop and operator transitions.
    /// The guard is released before waiting for the server's response.
    pub(crate) async fn admit_turn(&self) -> Result<Option<OwnedMutexGuard<()>>> {
        let lease = self.host.start_lease().await?;
        let guard = self.host.admission_lock().lock_owned().await;
        lease.ensure_live()?;
        let raw: String = sqlx::query_scalar("SELECT r.state_json FROM engine_flow030_runs r JOIN engine_flow030_owners o USING(system_id,invocation_id) WHERE r.system_id=? AND r.invocation_id=? AND o.generation=? AND o.owner_epoch=? AND o.status='live'")
            .bind(&self.attempt.identity().system_start_id).bind(&self.attempt.identity().flow_invocation_id)
            .bind(self.attempt.generation()).bind(&self.attempt.owner().owner_epoch)
            .fetch_one(&self.pool).await?;
        let state: FlowState030 = serde_json::from_str(&raw)?;
        let id = &self.attempt.work().activation_id;
        if state.paused
            || state.stopping
            || state.verdict.is_some()
            || state.operator_holds.contains_key(id)
            || !state
                .activations
                .iter()
                .any(|a| a.id == *id && a.phase == "running" && a.stop_effect_id.is_none())
        {
            return Ok(None);
        }
        Ok(Some(guard))
    }

    pub(crate) async fn requested(&mut self) -> String {
        loop {
            if let Some(id) = self.commands.borrow_and_update().clone() {
                return id;
            }
            if self.commands.changed().await.is_err() {
                std::future::pending::<()>().await;
            }
        }
    }

    pub(crate) async fn park(&mut self, mut hold_id: String) -> Result<()> {
        self.parked.store(true, Ordering::Release);
        loop {
            self.confirm(&hold_id).await?;
            loop {
                let next = self.commands.borrow_and_update().clone();
                match next {
                    None => {
                        self.parked.store(false, Ordering::Release);
                        self.host
                            .flow030()
                            .supervisors
                            .wake(&self.attempt.identity().system_start_id);
                        return Ok(());
                    }
                    Some(id) if id != hold_id => {
                        hold_id = id;
                        break;
                    }
                    _ => (),
                }
                self.commands
                    .changed()
                    .await
                    .map_err(|_| failure("INTERRUPTED", "operator", "Hold lost its supervisor"))?;
            }
        }
    }

    async fn confirm(&self, hold_id: &str) -> Result<()> {
        let host = &self.host;
        let runtime = host.flow030();
        let lease = host.start_lease().await?;
        let _admission = host.admission_lock().lock_owned().await;
        let authority = Authority {
            runtime,
            lease: &lease,
            owner: self.attempt.owner().clone(),
        };
        let mut db = self.pool.acquire().await?;
        sqlx::query("PRAGMA synchronous=FULL")
            .execute(&mut *db)
            .await?;
        let mut tx = db.begin_with("BEGIN IMMEDIATE").await?;
        let identity = self.attempt.identity();
        let view = flow_store030::read_tx(&mut tx, &authority, identity)
            .await
            .map_err(store_error)?;
        let activation_id = &self.attempt.work().activation_id;
        if !view
            .state
            .operator_holds
            .get(activation_id)
            .is_some_and(|hold| hold.id == hold_id && !hold.ready)
        {
            return Ok(());
        }
        let event = FlowEvent030 {
            identity: identity.clone(),
            event_id: format!(
                "operator-held:{:x}",
                Sha256::digest(serde_json::to_vec(&(hold_id, activation_id))?)
            ),
            expected_revision: view.state.revision,
            kind: FlowEventKind030::Operator(OperatorEvent030::Held {
                activation_id: activation_id.clone(),
                hold_id: hold_id.into(),
            }),
        };
        apply_native_event_transaction030(runtime, &lease, tx, &event, None).await?;
        runtime.supervisors.wake(&identity.system_start_id);
        Ok(())
    }
}
