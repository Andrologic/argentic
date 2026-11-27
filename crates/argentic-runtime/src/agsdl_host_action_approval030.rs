//! Existing native approval/resolution/delivery, owned entirely by runtime.
use crate::{
    agsdl_host::HostState,
    agsdl_host_diagnostics030::{self as diagnostics, Stage},
    agsdl_host_flow030 as flow,
    host::ActionPolicySource,
    persistence::{failure, Result},
};
use futures::future::BoxFuture;
use sqlx::SqlitePool;
use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;
pub(crate) struct AgentActionHumans030 {
    pub host: HostState,
    pub pool: SqlitePool,
    pub policy: Arc<dyn ActionPolicySource>,
}
pub type Respond030<'a> = Box<dyn FnOnce(bool) -> BoxFuture<'a, Result<()>> + Send + 'a>;

/// The existing human broker owns identity, deadlines and the final send gate.
pub trait CommandApprovals030: Send + Sync {
    /// Use the same human clock as action approvals. Without a person port,
    /// resolve the plan as a refusal without comment.
    fn answer_plan<'a>(
        &'a self,
        _work: &'a flow::FlowWorkAttempt030,
        payload: serde_json::Value,
        _lock: &'a crate::harness::NativePlanLock030,
        events: &'a crate::harness::WorkEventSink030,
        cancel: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        Box::pin(async move {
            let receipt = events.emit(
                serde_json::json!({"kind":"planResolved","planId":payload["planId"],"decision":"reject"}),
                cancel,
            ).await?;
            Ok(serde_json::from_str(&receipt.receipt_json)?)
        })
    }

    fn answer_model<'a>(
        &'a self,
        _work: &'a flow::FlowWorkAttempt030,
        _payload: serde_json::Value,
        _operator: &'a crate::harness::AgentOperator030,
        _cancel: &'a CancellationToken,
        respond: Respond030<'a>,
    ) -> BoxFuture<'a, Result<()>> {
        respond(false)
    }

    fn answer<'a>(
        &'a self,
        work: &'a flow::FlowWorkAttempt030,
        payload: serde_json::Value,
        cancel: &'a CancellationToken,
        respond: Respond030<'a>,
    ) -> BoxFuture<'a, Result<()>>;
}

impl CommandApprovals030 for AgentActionHumans030 {
    fn answer_model<'a>(
        &'a self,
        attempt: &'a flow::FlowWorkAttempt030,
        payload: serde_json::Value,
        operator: &'a crate::harness::AgentOperator030,
        cancel: &'a CancellationToken,
        respond: Respond030<'a>,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let view =
                flow::inspect_run(self.host.flow030(), &self.host, &self.pool, attempt.owner())
                    .await?;
            let policy = self.policy.lock_current(&view.capture, &payload).await?;
            let admission = operator.admit_turn().await?;
            let allowed = policy.allows() && admission.is_some() && !cancel.is_cancelled();
            respond(allowed).await
        })
    }

    fn answer_plan<'a>(
        &'a self,
        attempt: &'a flow::FlowWorkAttempt030,
        payload: serde_json::Value,
        lock: &'a crate::harness::NativePlanLock030,
        events: &'a crate::harness::WorkEventSink030,
        cancel: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        Box::pin(async move {
            let host = &self.host;
            let broker = host
                .flow030()
                .human_runtime()
                .broker(host, &self.pool, attempt.identity())
                .await?;
            let ticket = {
                let _operation = broker.operation.lock().await;
                let work = broker.admit_agent(host, attempt).await?;
                broker.pump(host, cancel).await?;
                let ticket = broker.open(host, &work, "plan", payload).await?;
                broker.pump(host, cancel).await?;
                ticket
            };
            host.flow030()
                .supervisor_registry()
                .wake(&attempt.identity().system_start_id);
            while !broker.resolution_ready(&ticket).await? {
                tokio::select! {
                    _ = cancel.cancelled() => return Err(failure("STOP_PENDING", "plan", "Plan review was stopped")),
                    _ = tokio::time::sleep(Duration::from_millis(25)) => {}
                }
            }
            let decision = broker.take_resolution(host, &ticket).await?;
            let receipt = events.emit(decision.plan_event()?, cancel).await?;
            lock.resolve(attempt, &decision).await?;
            let _operation = broker.operation.lock().await;
            broker.acknowledge(host, &decision).await?;
            broker.pump(host, cancel).await?;
            Ok(serde_json::from_str(&receipt.receipt_json)?)
        })
    }

    fn answer<'a>(
        &'a self,
        attempt: &'a crate::agsdl_host_flow030::FlowWorkAttempt030,
        mut payload: serde_json::Value,
        cancel: &'a CancellationToken,
        respond: Respond030<'a>,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let host = &self.host;
            let broker = diagnostics::at(
                attempt,
                Stage::BrokerAttach,
                host.flow030()
                    .human_runtime()
                    .broker(host, &self.pool, attempt.identity())
                    .await,
            )?;
            if payload.get("title").is_none() {
                // Legacy RPC payloads carry the title in the native reason field.
                payload["title"] = payload["nativeRequest"]["params"]["reason"].clone();
            }
            let (work, ticket) = {
                let _operation = broker.operation.lock().await;
                let work = diagnostics::at(
                    attempt,
                    Stage::BrokerAdmit,
                    broker.admit_agent(host, attempt).await,
                )?;
                if !work.belongs_to(attempt) {
                    return diagnostics::at(
                        attempt,
                        Stage::BrokerOwner,
                        Err(failure(
                            "CORRELATION",
                            "approval",
                            "Native action owner changed",
                        )),
                    );
                }
                // A first ask can admit its own work. Settle that clock
                // event before open reserves the next one.
                diagnostics::at(attempt, Stage::BrokerPump, broker.pump(host, cancel).await)?;
                let ticket = diagnostics::at(
                    attempt,
                    Stage::BrokerOpen,
                    broker.open(host, &work, "ask", payload.clone()).await,
                )?;
                diagnostics::at(attempt, Stage::BrokerPump, broker.pump(host, cancel).await)?;
                (work, ticket)
            };
            host.flow030()
                .supervisor_registry()
                .wake(&attempt.identity().system_start_id);
            while !diagnostics::at(
                attempt,
                Stage::BrokerWait,
                broker.resolution_ready(&ticket).await,
            )? {
                tokio::select! {
                    _ = cancel.cancelled() => return diagnostics::at(attempt, Stage::BrokerWait, Err(failure("STOP_PENDING", "approval", "Native action was stopped"))),
                    _ = tokio::time::sleep(Duration::from_millis(25)) => {}
                }
            }
            let decision = diagnostics::at(
                attempt,
                Stage::BrokerResolution,
                broker.action_resolution(host, &ticket).await,
            )?;
            if decision.resolution()["operatorCancelled"] == true {
                return Err(failure(
                    "OPERATOR_HOLD",
                    "approval",
                    "The turn is being interrupted",
                ));
            }
            #[cfg(test)]
            if payload["nativeRequest"]["method"] == "mcp/tool/requestApproval" {
                host.flow030()
                    .operator_test_yield("mcp.approval.resolved")
                    .await;
            }
            let view = diagnostics::at(
                attempt,
                Stage::BrokerInspect,
                flow::inspect_run(host.flow030(), host, &self.pool, attempt.owner()).await,
            )?;
            // The host's real source/configuration guards remain owned until
            // native delivery and its correlated acknowledgement have returned.
            let policy = diagnostics::at(
                attempt,
                Stage::BrokerPolicy,
                self.policy.lock_current(&view.capture, &payload).await,
            )?;
            #[cfg(test)]
            if payload["nativeRequest"]["method"] == "mcp/tool/requestApproval" {
                host.flow030()
                    .operator_test_yield("mcp.approval.policy")
                    .await;
            }
            let allowed = policy.allows();
            let _operation = broker.operation.lock().await;
            diagnostics::at(
                attempt,
                Stage::BrokerDeliver,
                broker
                    .deliver_action(host, &work, &decision, &payload, |yes| {
                        let response = respond(yes && allowed && !cancel.is_cancelled());
                        async move { diagnostics::at(attempt, Stage::BrokerReply, response.await) }
                    })
                    .await,
            )?;
            diagnostics::at(attempt, Stage::BrokerPump, broker.pump(host, cancel).await)?;
            drop(policy);
            Ok(())
        })
    }
}
