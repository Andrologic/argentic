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
    fn answer<'a>(
        &'a self,
        work: &'a flow::FlowWorkAttempt030,
        payload: serde_json::Value,
        cancel: &'a CancellationToken,
        respond: Respond030<'a>,
    ) -> BoxFuture<'a, Result<()>>;
}

impl CommandApprovals030 for AgentActionHumans030 {
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
            let withdraw_on_cancel = payload
                .pointer("/nativeRequest/method")
                .is_some_and(|method| method == "argentic/tool/requestApproval")
                && payload
                    .pointer("/nativeRequest/params/actions/0/action")
                    .is_some_and(|action| action == "network");
            let stopped = || failure("STOP_PENDING", "approval", "Native action was stopped");
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
                if cancel.is_cancelled() {
                    return Err(stopped());
                }
                let ticket = diagnostics::at(
                    attempt,
                    Stage::BrokerOpen,
                    broker.open(host, &work, "ask", payload.clone()).await,
                )?;
                let publication = CancellationToken::new();
                let publication = if withdraw_on_cancel {
                    &publication
                } else {
                    cancel
                };
                diagnostics::at(
                    attempt,
                    Stage::BrokerPump,
                    broker.pump(host, publication).await,
                )?;
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
                    _ = cancel.cancelled() => {
                        if withdraw_on_cancel {
                            diagnostics::at(
                                attempt, Stage::BrokerWait,
                                broker.withdraw_ask(host, &work, &ticket).await,
                            )?;
                        }
                        return diagnostics::at(attempt, Stage::BrokerWait, Err(stopped()));
                    },
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
