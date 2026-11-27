//! Durable flow-owned human arbitration. The resident only computes a pure
//! projection; native handoffs, timestamps, answers and timeout causes stay here.
use crate::{
    agsdl_host::{HostStartLease, HostState},
    agsdl_host_clock030::{Clock030, ClockBroker030, HostClockInput030, HostClockOutput030},
    agsdl_host_flow030::FlowWorkAttempt030,
    persistence::{
        dto::EngineOwner,
        failure,
        flow030::{
            FlowEffect030, FlowEffectKind030, FlowIdentity030, FlowPlan030, FlowState030,
            WorkRef030,
        },
        public030::HumanScope030,
        start030::StartCapture030,
        validate_json, Result,
    },
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::{Connection, Row, SqliteConnection, SqlitePool};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
use tokio_util::sync::CancellationToken;

fn require(ok: bool, message: &str) -> Result<()> {
    if ok {
        Ok(())
    } else {
        Err(failure("CORRELATION", "humanClock", message))
    }
}
#[path = "agsdl_host_human030_validation.rs"]
mod validation;
pub(crate) use validation::machine_remaining_ms030;

fn uuid() -> String {
    uuid::Uuid::new_v4().to_string()
}

/// Public IPC input contains neither time, owner, message nor native receipt.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct HumanSubmission030 {
    pub scope: HumanScope030,
    pub request_id: String,
    pub expected_revision: String,
    pub answer: HumanAnswer030,
}
pub(crate) use crate::persistence::human_answer030::human_comment_is_empty030;
pub use crate::persistence::human_answer030::HumanAnswer030;
#[derive(Clone, Copy)]
pub(crate) enum HumanOrigin030 {
    Person,
    #[cfg(test)]
    SyntheticPerson,
}
impl HumanOrigin030 {
    fn name(self) -> &'static str {
        match self {
            Self::Person => "person",
            #[cfg(test)]
            Self::SyntheticPerson => "synthetic-person",
        }
    }
}
/// Constructed only after a durable handoff has been authenticated. Not IPC.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct OwnedHumanWork030 {
    identity: FlowIdentity030,
    owner: EngineOwner,
    generation: String,
    reference: WorkRef030,
    kind: String,
    provider_attempt_id: Option<String>,
}
impl OwnedHumanWork030 {
    pub(crate) fn belongs_to(&self, attempt: &FlowWorkAttempt030) -> bool {
        self.identity == *attempt.identity()
            && self.owner == *attempt.owner()
            && self.generation == attempt.generation()
            && self.reference.activation_id == attempt.work().activation_id
            && self.reference.start_effect_id == attempt.start_effect_id()
            && self.provider_attempt_id.as_deref() == Some(attempt.provider_attempt_id())
    }
    pub(crate) fn work_id(&self) -> &str {
        &self.reference.activation_id
    }
    #[cfg(test)]
    pub(crate) fn reference(&self) -> &WorkRef030 {
        &self.reference
    }
}
/// A delivery reservation is deliberately separate from the resolution. Re-read
/// uncertain reservations; never send a second physical answer on restart.
pub(crate) struct ResolvedHumanDecision030 {
    identity: FlowIdentity030,
    owner: EngineOwner,
    generation: String,
    request_id: String,
    work_ids: Vec<String>,
    kind: String,
    payload: Value,
    resolution: Value,
}
impl ResolvedHumanDecision030 {
    pub(crate) fn resolution(&self) -> &Value {
        &self.resolution
    }
    #[cfg(test)]
    pub(crate) fn payload(&self) -> &Value {
        &self.payload
    }
    #[cfg(test)]
    pub(crate) fn kind(&self) -> &str {
        &self.kind
    }
    pub(crate) fn verifies_plan(&self, work: &FlowWorkAttempt030, plan_id: &str) -> bool {
        self.kind == "plan"
            && self.identity == *work.identity()
            && self.owner == *work.owner()
            && self.generation == work.generation()
            && self.work_ids == [work.work().activation_id.clone()]
            && self.payload.get("planId").and_then(Value::as_str) == Some(plan_id)
    }
    /// Build a controller event, not a flow WorkResult. A rejected ask continues.
    pub(crate) fn plan_event(&self) -> Result<Value> {
        require(self.kind == "plan", "Not a plan decision")?;
        let answer = &self.resolution["answer"];
        Ok(
            json!({"kind":"planResolved", "planId":self.payload["planId"],
            "decision": if answer["decision"] == "yes" {"accept"} else {"reject"},
            "comment":answer.get("comment").and_then(Value::as_str).unwrap_or("")}),
        )
    }
    /// Approval output is built from the message retained at native admission.
    pub(crate) fn approval_output(&self) -> Result<Value> {
        require(
            self.kind == "approval" && self.resolution["expired"] == false,
            "Approval is not a normal decision",
        )?;
        Ok(json!({"choice":self.resolution["answer"]["choice"],
            "comment":self.resolution["answer"]["comment"],"message":self.payload["message"]}))
    }
}
pub(crate) struct TimeoutControl030 {
    work_id: String,
}
impl TimeoutControl030 {
    pub(crate) fn work_id(&self) -> &str {
        &self.work_id
    }
}

/// A native Call port retains the attested protocol/schema validator. This
/// capability is never part of IPC; a caller cannot attest its own answer JSON.
pub(crate) trait QuestionValidator030: Send + Sync {
    fn validate(&self, captured_payload: &Value, exact_answer_json: &str) -> Result<()>;
}

pub(crate) struct HumanBroker030 {
    pub(crate) operation: tokio::sync::Mutex<()>,
    pool: SqlitePool,
    identity: FlowIdentity030,
    owner: EngineOwner,
    generation: String,
    clock: Arc<dyn Clock030>,
    jobs: Arc<ClockBroker030>,
    question_validators: Mutex<BTreeMap<String, Arc<dyn QuestionValidator030>>>,
}
impl HumanBroker030 {
    /// Attach within a live generation. Existing journals are read, never replayed
    /// as effects. A different owner/generation fails closed, preserving history.
    pub(crate) async fn attach(
        host: &HostState,
        pool: SqlitePool,
        identity: FlowIdentity030,
        clock: Arc<dyn Clock030>,
        jobs: Arc<ClockBroker030>,
    ) -> Result<Self> {
        let lease = host.start_lease().await?;
        let _gate = host.admission_lock().lock_owned().await;
        let owner = lease.owner(&identity.system_start_id)?;
        let broker = Self {
            operation: Default::default(),
            pool,
            identity,
            owner,
            generation: lease.generation().into(),
            clock,
            jobs,
            question_validators: Mutex::new(BTreeMap::new()),
        };
        let mut connection = broker.durable_connection().await?;
        let mut tx = connection.begin_with("BEGIN IMMEDIATE").await?;
        broker.guard(&mut tx, &lease).await?;
        sqlx::query("INSERT INTO engine_flow030_clock(system_id,invocation_id,owner_epoch,generation,observed_ms) VALUES(?,?,?,?,?) ON CONFLICT DO NOTHING")
            .bind(&broker.identity.system_start_id).bind(&broker.identity.flow_invocation_id)
            .bind(&broker.owner.owner_epoch).bind(&broker.generation).bind(broker.clock.now_ms().to_string()).execute(&mut *tx).await?;
        let row = broker.clock_row(&mut tx).await?;
        require(
            row.try_get::<&str, _>("owner_epoch")? == broker.owner.owner_epoch
                && row.try_get::<&str, _>("generation")? == broker.generation,
            "Clock generation was lost; no automatic replay",
        )?;
        tx.commit().await?;
        Ok(broker)
    }
    async fn guard(
        &self,
        db: &mut SqliteConnection,
        lease: &HostStartLease,
    ) -> Result<StartCapture030> {
        lease.ensure_live()?;
        require(
            lease.generation() == self.generation
                && lease.owner(&self.identity.system_start_id)? == self.owner,
            "Host generation changed",
        )?;
        let row = sqlx::query("SELECT invocation_id,snapshot_id,capture_epoch,owner_epoch,generation,status,capture_sha256 FROM engine_flow030_owners WHERE system_id=?")
            .bind(&self.identity.system_start_id).fetch_one(&mut *db).await?;
        require(
            row.try_get::<&str, _>("invocation_id")? == self.identity.flow_invocation_id
                && row.try_get::<&str, _>("snapshot_id")? == self.identity.snapshot_id
                && row.try_get::<&str, _>("capture_epoch")? == self.identity.epoch
                && row.try_get::<&str, _>("owner_epoch")? == self.owner.owner_epoch
                && row.try_get::<&str, _>("generation")? == self.generation
                && row.try_get::<&str, _>("status")? == "live",
            "Foreign or revoked flow owner",
        )?;
        let raw: String = sqlx::query_scalar("SELECT capture_json FROM engine_start030_requests WHERE system_id=? AND invocation_id=?")
            .bind(&self.identity.system_start_id).bind(&self.identity.flow_invocation_id).fetch_one(&mut *db).await?;
        let capture: StartCapture030 = serde_json::from_str(&raw)?;
        require(
            capture.identity == self.identity
                && format!("{:x}", Sha256::digest(raw.as_bytes()))
                    == row.try_get::<&str, _>("capture_sha256")?,
            "Capture changed",
        )?;
        let raw: String = sqlx::query_scalar(
            "SELECT state_json FROM engine_flow030_runs WHERE system_id=? AND invocation_id=?",
        )
        .bind(&self.identity.system_start_id)
        .bind(&self.identity.flow_invocation_id)
        .fetch_one(&mut *db)
        .await?;
        let state: FlowState030 = serde_json::from_str(&raw)?;
        require(
            state.identity == self.identity && !state.stopping,
            "Flow is stopped or terminal",
        )?;
        Ok(capture)
    }
    async fn durable_connection(&self) -> Result<sqlx::pool::PoolConnection<sqlx::Sqlite>> {
        let mut db = self.pool.acquire().await?;
        sqlx::query("PRAGMA synchronous=FULL")
            .execute(&mut *db)
            .await?;
        Ok(db)
    }

    async fn clock_row(&self, db: &mut SqliteConnection) -> Result<sqlx::sqlite::SqliteRow> {
        Ok(
            sqlx::query("SELECT * FROM engine_flow030_clock WHERE system_id=? AND invocation_id=?")
                .bind(&self.identity.system_start_id)
                .bind(&self.identity.flow_invocation_id)
                .fetch_one(db)
                .await?,
        )
    }
    /// Agent identity comes from its actual native handoff, never a JS callback.
    pub(crate) async fn admit_agent(
        &self,
        host: &HostState,
        attempt: &FlowWorkAttempt030,
    ) -> Result<OwnedHumanWork030> {
        require(
            attempt.identity() == &self.identity
                && attempt.owner() == &self.owner
                && attempt.generation() == self.generation,
            "Foreign agent attempt",
        )?;
        self.admit(host, attempt.start_effect_id(), Some(attempt), None)
            .await
    }
    /// For Call/Approval this authenticates the owned host handoff and capture.
    /// Call must additionally keep its native protocol call-id in request payload.
    pub(crate) async fn admit_host_work(
        &self,
        host: &HostState,
        start_effect_id: &str,
    ) -> Result<OwnedHumanWork030> {
        self.admit(
            host,
            start_effect_id,
            None,
            Some(crate::host::HostContext::DEFAULT_REMOTE_MCP_TOOL_BUDGET_MS),
        )
        .await
    }
    pub(crate) async fn admit_call(
        &self,
        host: &HostState,
        start_effect_id: &str,
        remote_mcp_budget_ms: std::num::NonZeroU64,
    ) -> Result<OwnedHumanWork030> {
        self.admit(host, start_effect_id, None, Some(remote_mcp_budget_ms))
            .await
    }
    async fn admit(
        &self,
        host: &HostState,
        effect_id: &str,
        attempt: Option<&FlowWorkAttempt030>,
        remote_mcp_budget_ms: Option<std::num::NonZeroU64>,
    ) -> Result<OwnedHumanWork030> {
        self.settle_pending_flow_clock(host).await?;
        let lease = host.start_lease().await?;
        let _gate = host.admission_lock().lock_owned().await;
        let mut connection = self.durable_connection().await?;
        let mut tx = connection.begin_with("BEGIN IMMEDIATE").await?;
        let capture = self.guard(&mut tx, &lease).await?;
        let raw: String = sqlx::query_scalar("SELECT effect_json FROM engine_flow030_outbox WHERE system_id=? AND invocation_id=? AND effect_id=? AND status IN ('uncertain','settled')")
            .bind(&self.identity.system_start_id).bind(&self.identity.flow_invocation_id).bind(effect_id).fetch_one(&mut *tx).await?;
        let effect: FlowEffect030 = serde_json::from_str(&raw)?;
        let FlowEffectKind030::Start {
            work,
            step,
            agent,
            message_json,
            ..
        } = &effect.kind
        else {
            return Err(failure("CORRELATION", "humanClock", "Not a Start"));
        };
        require(
            effect.identity == self.identity && work.start_effect_id == effect_id,
            "Start differs",
        )?;
        let raw_handoff: String = sqlx::query_scalar("SELECT receipt_json FROM engine_flow030_handoffs WHERE system_id=? AND invocation_id=? AND effect_id=? AND owner_epoch=? AND generation=?")
            .bind(&self.identity.system_start_id).bind(&self.identity.flow_invocation_id).bind(effect_id).bind(&self.owner.owner_epoch).bind(&self.generation).fetch_one(&mut *tx).await?;
        let handoff: Value = serde_json::from_str(&raw_handoff)?;
        require(
            handoff["kind"] == "nativeOwnedHandoff030"
                && handoff["requestId"]
                    == format!("flow030:{}:{}", self.identity.system_start_id, effect_id)
                && handoff["identity"] == serde_json::to_value(&self.identity)?
                && handoff["effectId"] == effect_id
                && handoff["ownerEpoch"] == self.owner.owner_epoch
                && handoff["generation"] == self.generation,
            "Handoff differs",
        )?;
        let plan: FlowPlan030 = serde_json::from_str(&capture.compiled_plan_json)?;
        let decl = plan.steps.iter().find(|s| &Some(s.address.clone()) == step);
        let kind = if agent.is_some() {
            "agent"
        } else {
            decl.map(|s| s.r#type.as_str()).unwrap_or("")
        };
        require(
            ["agent", "call", "approval"].contains(&kind),
            "Unsupported host work",
        )?;
        if let Some(attempt) = attempt {
            require(
                work.activation_id == attempt.work().activation_id
                    && work.agent_instance_id == attempt.work().agent_instance_id
                    && handoff["workAttempt"] == serde_json::to_value(attempt)?,
                "Agent handoff differs",
            )?;
            let exists: i64 = sqlx::query_scalar("SELECT count(*) FROM engine_flow030_work_admissions WHERE system_id=? AND invocation_id=? AND activation_id=? AND start_effect_id=? AND owner_epoch=? AND generation=?")
                .bind(&self.identity.system_start_id).bind(&self.identity.flow_invocation_id).bind(&work.activation_id).bind(effect_id).bind(&self.owner.owner_epoch).bind(&self.generation).fetch_one(&mut *tx).await?;
            require(exists == 1, "Agent admission absent")?;
        } else {
            require(agent.is_none(), "Agent attempt required")?;
        }
        let raw_state: String = sqlx::query_scalar(
            "SELECT state_json FROM engine_flow030_runs WHERE system_id=? AND invocation_id=?",
        )
        .bind(&self.identity.system_start_id)
        .bind(&self.identity.flow_invocation_id)
        .fetch_one(&mut *tx)
        .await?;
        let state: FlowState030 = serde_json::from_str(&raw_state)?;
        let activation = state
            .activations
            .iter()
            .find(|a| a.id == work.activation_id)
            .ok_or_else(|| failure("CORRELATION", "humanClock", "Activation absent"))?;
        require(
            !state.stopping
                && state.verdict.is_none()
                && activation.phase == "running"
                && activation.result.is_none()
                && activation.stop_effect_id.is_none()
                && activation.start_effect_id.as_deref() == Some(effect_id)
                && activation.instance_id == work.agent_instance_id,
            "Wrong or terminal visit",
        )?;
        let owned = OwnedHumanWork030 {
            identity: self.identity.clone(),
            owner: self.owner.clone(),
            generation: self.generation.clone(),
            reference: work.clone(),
            kind: kind.into(),
            provider_attempt_id: attempt.map(|a| a.provider_attempt_id().into()),
        };
        let timeout = decl.and_then(|s| s.timeout_ms_json.clone());
        let document: Value = serde_json::from_str(&capture.authored_source)?;
        let declaration: Value = decl
            .map(|s| serde_json::from_str(&s.declaration_json))
            .transpose()?
            .unwrap_or(Value::Null);
        let timeout = timeout.or_else(|| {
            remote_mcp_budget_ms
                .filter(|_| {
                    crate::persistence::call_protocol030::is_remote_mcp_call(
                        &document,
                        &declaration,
                    )
                })
                .map(|budget| budget.to_string())
        });
        let answer_timeout = if let Some(agent) = agent {
            document["agents"][agent]
                .get("answerTimeoutMs")
                .map(Value::to_string)
        } else {
            declaration.get("answerTimeoutMs").map(Value::to_string)
        };
        let now = self.clock.now_ms().to_string();
        let started: String = sqlx::query_scalar("SELECT started_ms FROM engine_flow030_handoff_clock WHERE system_id=? AND invocation_id=? AND start_effect_id=? AND owner_epoch=? AND generation=?")
            .bind(&self.identity.system_start_id).bind(&self.identity.flow_invocation_id).bind(effect_id)
            .bind(&self.owner.owner_epoch).bind(&self.generation).fetch_optional(&mut *tx).await?
            .ok_or_else(|| failure("CORRELATION", "clock", "Native handoff clock origin is absent; no budget backfill"))?;
        require(
            validation::ms(&started)? <= validation::ms(&now)?,
            "Handoff time exceeds admission",
        )?;
        let raw_owned = serde_json::to_string(&owned)?;
        let previous: Option<String> = sqlx::query_scalar("SELECT work_json FROM engine_flow030_clock_works WHERE system_id=? AND invocation_id=? AND activation_id=?")
            .bind(&self.identity.system_start_id).bind(&self.identity.flow_invocation_id).bind(owned.work_id()).fetch_optional(&mut *tx).await?;
        if let Some(previous) = previous {
            require(previous == raw_owned, "Conflicting clock admission")?;
            tx.commit().await?;
            return Ok(owned);
        }
        sqlx::query("INSERT INTO engine_flow030_clock_works(system_id,invocation_id,activation_id,start_effect_id,owner_epoch,generation,kind,work_json,admitted_ms,timeout_json,answer_timeout_json) VALUES(?,?,?,?,?,?,?,?,?,?,?)")
            .bind(&self.identity.system_start_id).bind(&self.identity.flow_invocation_id).bind(owned.work_id()).bind(effect_id)
            .bind(&self.owner.owner_epoch).bind(&self.generation).bind(kind).bind(&raw_owned).bind(&started).bind(&timeout).bind(&answer_timeout).execute(&mut *tx).await?;
        let event = if kind == "approval" {
            let choices = declaration
                .get("next")
                .and_then(Value::as_object)
                .map(|m| m.keys().cloned().collect::<Vec<_>>())
                .unwrap_or_default();
            require(!choices.is_empty(), "Approval choices absent")?;
            let payload = json!({"choices":choices,"message":message_json.as_ref().map(|s| serde_json::from_str::<Value>(s)).transpose()?.unwrap_or(json!({})),"title":declaration["title"],"description":declaration["description"],"step":step,"work":work});
            let request = self
                .insert_request(
                    &mut tx,
                    "approval",
                    &[owned.work_id().into()],
                    payload,
                    json!({"openedAtMs":started,"timeoutMs":timeout}),
                    &started,
                )
                .await?;
            json!({"kind":"approval","requestId":request,"workId":owned.work_id(),"choices":choices,"timeoutMs":timeout,"startedAtMs":started})
        } else {
            json!({"kind":"admit","workId":owned.work_id(),"timeoutMs":timeout,"answerTimeoutMs":answer_timeout,"startedAtMs":started})
        };
        self.reserve(&mut tx, event, &now).await?;
        #[cfg(test)]
        host.flow030()
            .operator_test_yield("approval.clock.reserve")
            .await;
        tx.commit().await?;
        #[cfg(test)]
        host.flow030()
            .operator_test_yield("approval.clock.ack")
            .await;
        Ok(owned)
    }
    async fn verify_work(&self, db: &mut SqliteConnection, work: &OwnedHumanWork030) -> Result<()> {
        require(
            work.identity == self.identity
                && work.owner == self.owner
                && work.generation == self.generation,
            "Foreign work owner",
        )?;
        let raw: Option<String> = sqlx::query_scalar("SELECT work_json FROM engine_flow030_clock_works WHERE system_id=? AND invocation_id=? AND activation_id=? AND retired=0")
            .bind(&self.identity.system_start_id).bind(&self.identity.flow_invocation_id).bind(work.work_id()).fetch_optional(&mut *db).await?;
        require(
            raw.as_deref() == Some(serde_json::to_string(work)?.as_str()),
            "Work is absent or retired",
        )?;
        let raw: String = sqlx::query_scalar(
            "SELECT state_json FROM engine_flow030_runs WHERE system_id=? AND invocation_id=?",
        )
        .bind(&self.identity.system_start_id)
        .bind(&self.identity.flow_invocation_id)
        .fetch_one(db)
        .await?;
        let state: FlowState030 = serde_json::from_str(&raw)?;
        require(
            state.activations.iter().any(|a| {
                a.id == work.work_id()
                    && a.phase == "running"
                    && a.result.is_none()
                    && a.stop_effect_id.is_none()
                    && a.start_effect_id.as_deref() == Some(work.reference.start_effect_id.as_str())
            }),
            "Work visit is no longer active",
        )
    }
    async fn insert_request(
        &self,
        db: &mut SqliteConnection,
        kind: &str,
        works: &[String],
        payload: Value,
        deadline: Value,
        now: &str,
    ) -> Result<String> {
        let id = uuid();
        sqlx::query("INSERT INTO engine_flow030_human_requests(system_id,invocation_id,request_id,kind,work_ids_json,payload_json,opened_ms,deadline_json) VALUES(?,?,?,?,?,?,?,?)")
            .bind(&self.identity.system_start_id).bind(&self.identity.flow_invocation_id).bind(&id).bind(kind).bind(json!(works).to_string()).bind(payload.to_string()).bind(now).bind(deadline.to_string()).execute(db).await?;
        Ok(id)
    }
    /// The owned executor supplies an attested payload (planId/text or native
    /// call-id/schema). It must not turn provider JSON into an origin=person.
    pub(crate) async fn open(
        &self,
        host: &HostState,
        work: &OwnedHumanWork030,
        kind: &str,
        payload: Value,
    ) -> Result<String> {
        require(
            ["ask", "plan", "question"].contains(&kind) && work.kind != "approval",
            "Invalid request category",
        )?;
        if kind == "plan" {
            require(
                payload["planId"].as_str().is_some_and(|s| !s.is_empty())
                    && payload["text"]
                        .as_str()
                        .is_some_and(|s| !human_comment_is_empty030(s)),
                "Plan content absent",
            )?;
        }
        self.settle_pending_flow_clock(host).await?;
        let lease = host.start_lease().await?;
        let _gate = host.admission_lock().lock_owned().await;
        let mut connection = self.durable_connection().await?;
        let mut tx = connection.begin_with("BEGIN IMMEDIATE").await?;
        self.guard(&mut tx, &lease).await?;
        self.verify_work(&mut tx, work).await?;
        if kind == "ask" && self.operator_held(&mut tx, work.work_id()).await? {
            return Err(failure(
                "OPERATOR_HOLD",
                "approval",
                "The turn is being interrupted",
            ));
        }
        let timeout: Option<String> = sqlx::query_scalar("SELECT answer_timeout_json FROM engine_flow030_clock_works WHERE system_id=? AND invocation_id=? AND activation_id=?")
            .bind(&self.identity.system_start_id).bind(&self.identity.flow_invocation_id).bind(work.work_id()).fetch_one(&mut *tx).await?;
        let now = self.clock.now_ms().to_string();
        let request_id = self
            .insert_request(
                &mut tx,
                kind,
                &[work.work_id().into()],
                payload,
                json!({"openedAtMs":now,"timeoutMs":timeout}),
                &now,
            )
            .await?;
        self.reserve(&mut tx, json!({"kind":"open","requestId":request_id,"requestKind":kind,"reviewer":"person","workIds":[work.work_id()]}), &now).await?;
        tx.commit().await?;
        Ok(request_id)
    }
    /// Bind a product protocol question to its native receipt and validator.
    /// Keep the validator in the owned Call executor's lifetime; after a loss it
    /// must be reattested rather than reconstructed from public question JSON.
    pub(crate) async fn open_question(
        &self,
        host: &HostState,
        work: &OwnedHumanWork030,
        payload: Value,
        validator: Arc<dyn QuestionValidator030>,
    ) -> Result<String> {
        require(
            payload["nativeCallId"]
                .as_str()
                .is_some_and(|s| !s.is_empty())
                && payload["protocolReceiptId"]
                    .as_str()
                    .is_some_and(|s| !s.is_empty()),
            "Native protocol receipt absent",
        )?;
        let request = self.open(host, work, "question", payload).await?;
        self.question_validators
            .lock()
            .map_err(|_| {
                failure(
                    "RUNTIME_UNAVAILABLE",
                    "question",
                    "Protocol validators unavailable",
                )
            })?
            .insert(request.clone(), validator);
        Ok(request)
    }

    /// Retire the clock after joining the work, including terminal flow cleanup.
    /// Preserve its final budget and historical requests while removing pause links.
    pub(crate) async fn retire(&self, host: &HostState, work: &OwnedHumanWork030) -> Result<()> {
        require(
            work.identity == self.identity
                && work.owner == self.owner
                && work.generation == self.generation
                && work.kind != "approval",
            "Foreign clock retirement",
        )?;
        self.event(host, json!({"kind":"retire","workId":work.work_id()}))
            .await
    }
    /// Same entry point for product IPC and the gated QA driver. Native receipt
    /// is allocated only after scope/revision authentication. Capture the
    /// broker's arrival time before evaluating any earlier flow clock ticket.
    pub(crate) async fn submit(
        &self,
        host: &HostState,
        input: HumanSubmission030,
        origin: HumanOrigin030,
    ) -> Result<String> {
        // Bound user input before any durable ticket can be reserved.
        let answer_json = serde_json::to_string(&input.answer)?;
        if answer_json.len() > 65_536 {
            return Err(failure(
                "INPUT_LIMIT",
                "answer",
                "Human answer exceeds 65536 bytes",
            ));
        }
        validate_json(&answer_json)?;
        let received_ms = self.clock.now_ms().to_string();
        // Inspect and validate the answer before taking the admission lock.
        // The transaction below rechecks the revision and visit before reserving it.
        if let HumanAnswer030::Question { answer_json } = &input.answer {
            validate_json(answer_json)?;
            if origin.name() != "synthetic-person" {
                let row = {
                    let mut db = self.pool.acquire().await?;
                    self.request_row(&mut db, &input.request_id).await?
                };
                require(
                    row.try_get::<&str, _>("kind")? == "question",
                    "Not a protocol question",
                )?;
                require(
                    row.try_get::<&str, _>("revision")? == input.expected_revision,
                    "Stale human revision",
                )?;
                let payload: Value = serde_json::from_str(row.try_get("payload_json")?)?;
                let validator = self
                    .question_validators
                    .lock()
                    .map_err(|_| {
                        failure(
                            "RUNTIME_UNAVAILABLE",
                            "question",
                            "Protocol validators unavailable",
                        )
                    })?
                    .get(&input.request_id)
                    .cloned()
                    .ok_or_else(|| {
                        failure(
                            "CORRELATION",
                            "question",
                            "Product question requires protocol schema attestation",
                        )
                    })?;
                validator.validate(&payload, answer_json)?;
            }
        }
        self.settle_pending_flow_clock(host).await?;
        let lease = host.start_lease().await?;
        let _gate = host.admission_lock().lock_owned().await;
        let mut connection = self.durable_connection().await?;
        let mut tx = connection.begin_with("BEGIN IMMEDIATE").await?;
        let capture = self.guard(&mut tx, &lease).await?;
        require(
            input.scope.system_start_id == self.identity.system_start_id
                && input.scope.source == capture.source,
            "Public scope differs",
        )?;
        let row = self.request_row(&mut tx, &input.request_id).await?;
        require(
            row.try_get::<&str, _>("revision")? == input.expected_revision,
            "Stale human revision",
        )?;
        let work_ids: Vec<String> = serde_json::from_str(row.try_get("work_ids_json")?)?;
        let raw_state: String = sqlx::query_scalar(
            "SELECT state_json FROM engine_flow030_runs WHERE system_id=? AND invocation_id=?",
        )
        .bind(&self.identity.system_start_id)
        .bind(&self.identity.flow_invocation_id)
        .fetch_one(&mut *tx)
        .await?;
        let state: FlowState030 = serde_json::from_str(&raw_state)?;
        require(
            !state.stopping
                && state.verdict.is_none()
                && work_ids.iter().all(|id| {
                    !state.operator_holds.contains_key(id)
                        && state.activations.iter().any(|a| {
                            &a.id == id
                                && a.phase == "running"
                                && a.result.is_none()
                                && a.stop_effect_id.is_none()
                        })
                }),
            "Human answer targets a stale visit",
        )?;
        let kind: &str = row.try_get("kind")?;
        let payload: Value = serde_json::from_str(row.try_get("payload_json")?)?;
        let receipt_id = uuid();
        let event = match &input.answer {
            HumanAnswer030::Approval { choice, comment } => {
                require(
                    kind == "approval"
                        && payload["choices"]
                            .as_array()
                            .is_some_and(|c| c.contains(&json!(choice))),
                    "Unknown approval choice",
                )?;
                json!({"kind":"approvalAnswer","requestId":input.request_id,"nativeReceiptId":receipt_id,"choice":choice,"comment":comment})
            }
            HumanAnswer030::Decision { decision, comment } => {
                require(
                    ["ask", "plan"].contains(&kind) && ["yes", "no"].contains(&decision.as_str()),
                    "Invalid binary decision",
                )?;
                json!({"kind":"answer","requestId":input.request_id,"nativeReceiptId":receipt_id,"answer":{"decision":decision,"comment":comment}})
            }
            HumanAnswer030::Question { .. } => {
                require(kind == "question", "Not a protocol question")?;
                json!({"kind":"answer","requestId":input.request_id,"nativeReceiptId":receipt_id,"answer":null})
            }
        };
        sqlx::query("INSERT INTO engine_flow030_human_submissions VALUES(?,?,?,?,?,?,?,NULL)")
            .bind(&self.identity.system_start_id)
            .bind(&self.identity.flow_invocation_id)
            .bind(&receipt_id)
            .bind(&input.request_id)
            .bind(&received_ms)
            .bind(origin.name())
            .bind(&answer_json)
            .execute(&mut *tx)
            .await?;
        self.reserve(&mut tx, event, &received_ms).await?;
        tx.commit().await?;
        Ok(receipt_id)
    }
    async fn request_row(
        &self,
        db: &mut SqliteConnection,
        id: &str,
    ) -> Result<sqlx::sqlite::SqliteRow> {
        Ok(sqlx::query("SELECT * FROM engine_flow030_human_requests WHERE system_id=? AND invocation_id=? AND request_id=?")
            .bind(&self.identity.system_start_id).bind(&self.identity.flow_invocation_id).bind(id).fetch_one(db).await?)
    }
    /// Exactly one unresolved clock ticket per flow. An uncertain commit blocks
    /// new admissions/answers until pump/readback reconciles this exact ticket.
    async fn reserve(&self, db: &mut SqliteConnection, event: Value, now: &str) -> Result<()> {
        let row = self.clock_row(db).await?;
        require(
            now.parse::<u64>().ok() >= row.try_get::<&str, _>("observed_ms")?.parse::<u64>().ok(),
            "Native time moved backwards",
        )?;
        let event_id = uuid();
        let input = HostClockInput030 {
            identity: self.identity.clone(),
            owner: self.owner.clone(),
            event_id: event_id.clone(),
            expected_revision: row.try_get("revision")?,
            state_json: row.try_get("state_json")?,
            event_json: json!({"owner":self.owner,"eventId":event_id,"nowMs":now,"event":event})
                .to_string(),
            options_json: json!({"maxSliceMs":2147483647}).to_string(),
        };
        sqlx::query("INSERT INTO engine_flow030_clock_events(system_id,invocation_id,event_id,ticket_json) VALUES(?,?,?,?)")
            .bind(&self.identity.system_start_id).bind(&self.identity.flow_invocation_id).bind(event_id).bind(serde_json::to_string(&input)?).execute(db).await?;
        Ok(())
    }
    pub(crate) async fn reserve_operator_clock(
        &self,
        db: &mut SqliteConnection,
        before: &FlowState030,
        event_id: &str,
    ) -> Result<()> {
        let raw: String = sqlx::query_scalar(
            "SELECT state_json FROM engine_flow030_runs WHERE system_id=? AND invocation_id=?",
        )
        .bind(&self.identity.system_start_id)
        .bind(&self.identity.flow_invocation_id)
        .fetch_one(&mut *db)
        .await?;
        let after: FlowState030 = serde_json::from_str(&raw)?;
        let active: Vec<String> = sqlx::query_scalar("SELECT activation_id FROM engine_flow030_clock_works WHERE system_id=? AND invocation_id=? AND kind='agent' AND retired=0 ORDER BY rowid")
            .bind(&self.identity.system_start_id).bind(&self.identity.flow_invocation_id)
            .fetch_all(&mut *db).await?;
        let mut held = Vec::new();
        let mut released = Vec::new();
        for id in active {
            match (
                before.operator_holds.contains_key(&id),
                after.operator_holds.contains_key(&id),
            ) {
                (false, true) => held.push(id),
                (true, false) => released.push(id),
                _ => (),
            }
        }
        if held.is_empty() && released.is_empty() {
            return Ok(());
        }
        let requests = sqlx::query("SELECT request_id,work_ids_json,resolution_json FROM engine_flow030_human_requests WHERE system_id=? AND invocation_id=? AND kind='ask' AND delivery='pending' ORDER BY rowid")
            .bind(&self.identity.system_start_id).bind(&self.identity.flow_invocation_id)
            .fetch_all(&mut *db).await?;
        let mut cancel_asks = Vec::new();
        let mut close_asks = Vec::new();
        for request in requests {
            let works: Vec<String> = serde_json::from_str(request.try_get("work_ids_json")?)?;
            if works.iter().any(|id| held.contains(id)) {
                let request_id: String = request.try_get("request_id")?;
                if request
                    .try_get::<Option<&str>, _>("resolution_json")?
                    .is_some()
                {
                    self.reserve_resolution(db, &request_id).await?;
                } else {
                    cancel_asks.push(request_id.clone());
                }
                close_asks.push(request_id);
            }
        }
        let mut event =
            json!({"kind":"operatorHold","held":held,"released":released,"cancelAsks":cancel_asks});
        if !close_asks.is_empty() {
            event["closedAsks"] = json!({"operatorEventId":event_id,"requestIds":close_asks});
        }
        self.reserve(db, event, &self.clock.now_ms().to_string())
            .await
    }

    async fn operator_held(&self, db: &mut SqliteConnection, id: &str) -> Result<bool> {
        Ok(sqlx::query_scalar(
            "SELECT EXISTS (
            SELECT 1 FROM engine_flow030_runs r, json_each(r.state_json, '$.operatorHolds') h
            WHERE r.system_id=? AND r.invocation_id=? AND h.key=?)",
        )
        .bind(&self.identity.system_start_id)
        .bind(&self.identity.flow_invocation_id)
        .bind(id)
        .fetch_one(db)
        .await?)
    }
    /// The caller holds the broker operation lock through any dependent Stop.
    pub(crate) async fn observe_deadlines(&self, host: &HostState) -> Result<()> {
        let cancel = CancellationToken::new();
        self.pump(host, &cancel).await?;
        if self.next_wake_ms().await? == Some(0) {
            self.tick(host).await?;
            self.pump(host, &cancel).await?;
        }
        Ok(())
    }

    pub(crate) async fn tick(&self, host: &HostState) -> Result<()> {
        self.event(host, json!({"kind":"tick"})).await
    }
    // Flow transactions can reserve retirements and operator clock changes.
    // Clock writers hold the broker operation lock and apply that exact
    // reservation before appending their own event.
    async fn settle_pending_flow_clock(&self, host: &HostState) -> Result<()> {
        let pending: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM engine_flow030_clock_events
              WHERE system_id=? AND invocation_id=? AND output_json IS NULL
                AND json_extract(ticket_json ->> '$.eventJson', '$.event.kind') IN ('retire','operatorHold'))",
        )
        .bind(&self.identity.system_start_id)
        .bind(&self.identity.flow_invocation_id)
        .fetch_one(&self.pool)
        .await?;
        if pending {
            self.pump(host, &CancellationToken::new()).await?;
        }
        Ok(())
    }

    async fn event(&self, host: &HostState, event: Value) -> Result<()> {
        self.settle_pending_flow_clock(host).await?;
        let lease = host.start_lease().await?;
        let _gate = host.admission_lock().lock_owned().await;
        let mut connection = self.durable_connection().await?;
        let mut tx = connection.begin_with("BEGIN IMMEDIATE").await?;
        self.guard(&mut tx, &lease).await?;
        self.reserve(&mut tx, event, &self.clock.now_ms().to_string())
            .await?;
        tx.commit().await?;
        Ok(())
    }
    /// Execute the distinct pure resident job OUTSIDE the admission/SQL locks.
    /// Calling pump again after a lost commit acknowledgment reads the journal;
    /// it cannot replay a delivery or allocate a new machine budget.
    pub(crate) async fn pump(
        &self,
        host: &HostState,
        cancelled: &CancellationToken,
    ) -> Result<Option<HostClockOutput030>> {
        let input = {
            let lease = host.start_lease().await?;
            let _gate = host.admission_lock().lock_owned().await;
            let mut connection = self.durable_connection().await?;
            let mut tx = connection.begin_with("BEGIN IMMEDIATE").await?;
            self.guard(&mut tx, &lease).await?;
            let raw: Option<String> = sqlx::query_scalar("SELECT ticket_json FROM engine_flow030_clock_events WHERE system_id=? AND invocation_id=? AND output_json IS NULL")
                .bind(&self.identity.system_start_id).bind(&self.identity.flow_invocation_id).fetch_optional(&mut *tx).await?;
            tx.commit().await?;
            raw.map(|s| serde_json::from_str::<HostClockInput030>(&s))
                .transpose()?
        };
        let Some(input) = input else {
            return Ok(None);
        };
        #[cfg(test)]
        host.flow030().operator_test_yield("clock.reserved").await;
        let output = self
            .jobs
            .evaluate(&self.generation, input.clone(), cancelled)
            .await?;
        #[cfg(test)]
        host.flow030().operator_test_yield("clock.evaluated").await;
        let lease = host.start_lease().await?;
        let _gate = host.admission_lock().lock_owned().await;
        let mut connection = self.durable_connection().await?;
        let mut tx = connection.begin_with("BEGIN IMMEDIATE").await?;
        let capture = self.guard(&mut tx, &lease).await?;
        #[cfg(test)]
        host.flow030().operator_test_yield("clock.commit").await;
        self.commit(&mut tx, &input, &output, &capture.versions.contract)
            .await?;
        lease.ensure_live()?;
        tx.commit().await?;
        #[cfg(test)]
        host.flow030().operator_test_yield("clock.ack").await;
        Ok(Some(output))
    }
    async fn commit(
        &self,
        db: &mut SqliteConnection,
        input: &HostClockInput030,
        output: &HostClockOutput030,
        contract: &str,
    ) -> Result<()> {
        let stored: Option<String> = sqlx::query_scalar("SELECT output_json FROM engine_flow030_clock_events WHERE system_id=? AND invocation_id=? AND event_id=? AND ticket_json=?")
            .bind(&self.identity.system_start_id).bind(&self.identity.flow_invocation_id).bind(&input.event_id).bind(serde_json::to_string(input)?).fetch_one(&mut *db).await?;
        if let Some(stored) = stored {
            return require(
                stored == serde_json::to_string(output)?,
                "Conflicting clock completion",
            );
        }
        validate_json(&output.state_json)?;
        validate_json(&output.receipt_json)?;
        let journal: Value = serde_json::from_str(&output.state_json)?;
        let old: Value = if input.state_json.is_empty() {
            json!({"entries":[]})
        } else {
            serde_json::from_str(&input.state_json)?
        };
        let entries = journal["entries"]
            .as_array()
            .ok_or_else(|| failure("CORRELATION", "clock", "Journal entries absent"))?;
        let previous = old["entries"]
            .as_array()
            .ok_or_else(|| failure("CORRELATION", "clock", "Stored journal invalid"))?;
        require(
            journal["format"] == "macro-agsdl-human-clock-journal/1"
                && journal["owner"] == serde_json::to_value(&self.owner)?
                && journal["options"] == serde_json::from_str::<Value>(&input.options_json)?
                && entries.len() == previous.len() + 1
                && entries[..previous.len()] == previous[..]
                && entries.last()
                    == Some(
                        &json!({"eventJson":input.event_json,"receiptJson":output.receipt_json}),
                    ),
            "Pure clock journal attribution differs",
        )?;
        let event: Value = serde_json::from_str(&input.event_json)?;
        let receipt: Value = serde_json::from_str(&output.receipt_json)?;
        validation::validate(self, db, &event, &receipt).await?;
        let revision = (previous.len() + 1).to_string();
        let changed = sqlx::query("UPDATE engine_flow030_clock SET revision=?,state_json=?,receipt_json=?,observed_ms=? WHERE system_id=? AND invocation_id=? AND revision=? AND owner_epoch=? AND generation=?")
            .bind(&revision).bind(&output.state_json).bind(&output.receipt_json).bind(event["nowMs"].as_str())
            .bind(&self.identity.system_start_id).bind(&self.identity.flow_invocation_id).bind(&input.expected_revision).bind(&self.owner.owner_epoch).bind(&self.generation).execute(&mut *db).await?;
        require(changed.rows_affected() == 1, "Clock CAS conflict")?;
        for request in receipt["requests"]
            .as_array()
            .ok_or_else(|| failure("CORRELATION", "clock", "Missing request snapshots"))?
        {
            sqlx::query("UPDATE engine_flow030_human_requests SET work_ids_json=?,deadline_json=? WHERE system_id=? AND invocation_id=? AND request_id=?")
                .bind(request["workIds"].to_string()).bind(json!({"openedAtMs":request["openedAtMs"],"timeoutMs":request["answerTimeoutMs"]}).to_string())
                .bind(&self.identity.system_start_id).bind(&self.identity.flow_invocation_id).bind(request["requestId"].as_str()).execute(&mut *db).await?;
        }
        if event["event"]["kind"] == "retire" {
            sqlx::query("UPDATE engine_flow030_clock_works SET retired=1 WHERE system_id=? AND invocation_id=? AND activation_id=?")
                .bind(&self.identity.system_start_id).bind(&self.identity.flow_invocation_id).bind(event["event"]["workId"].as_str()).execute(&mut *db).await?;
        }
        for resolution in receipt["pendingResolutions"]
            .as_array()
            .ok_or_else(|| failure("CORRELATION", "clock", "Missing resolutions"))?
        {
            let request = resolution["requestId"]
                .as_str()
                .ok_or_else(|| failure("CORRELATION", "clock", "Missing request identity"))?;
            let mut durable_resolution = resolution.clone();
            durable_resolution["reason"] = json!(if resolution["expired"] != true {
                "answer"
            } else if resolution["failure"] == "APPROVAL_TIMEOUT" {
                "approvalTimeout"
            } else {
                "answerTimeout"
            });
            sqlx::query("UPDATE engine_flow030_human_requests SET resolution_json=?,revision='1' WHERE system_id=? AND invocation_id=? AND request_id=? AND resolution_json IS NULL")
                .bind(durable_resolution.to_string()).bind(&self.identity.system_start_id).bind(&self.identity.flow_invocation_id).bind(request).execute(&mut *db).await?;
            // The deadline can win inside the hold projection. Keep the timeout
            // resolution, but close its delivery to the turn that is ending.
            if resolution["expired"] == true
                && event
                    .pointer("/event/cancelAsks")
                    .and_then(Value::as_array)
                    .is_some_and(|ids| ids.contains(&json!(request)))
                && event
                    .pointer("/event/closedAsks/requestIds")
                    .and_then(Value::as_array)
                    .is_some_and(|ids| ids.contains(&json!(request)))
            {
                self.reserve_resolution(db, request).await?;
            }
            sqlx::query("INSERT INTO engine_flow030_human_resolution_proofs(system_id,invocation_id,request_id,event_id,resolution_json) VALUES(?,?,?,?,?) ON CONFLICT DO NOTHING")
                .bind(&self.identity.system_start_id).bind(&self.identity.flow_invocation_id).bind(request)
                .bind(&input.event_id).bind(durable_resolution.to_string()).execute(&mut *db).await?;
        }
        for answer in receipt["nativeAnswers"]
            .as_array()
            .ok_or_else(|| failure("CORRELATION", "clock", "Missing answers"))?
        {
            sqlx::query("UPDATE engine_flow030_human_submissions SET accepted=? WHERE system_id=? AND invocation_id=? AND receipt_id=? AND request_id=? AND accepted IS NULL")
                .bind(answer["accepted"].as_bool()).bind(&self.identity.system_start_id).bind(&self.identity.flow_invocation_id).bind(answer["nativeReceiptId"].as_str()).bind(answer["requestId"].as_str()).execute(&mut *db).await?;
        }
        for key in ["clocks", "retiredClocks"] {
            for clock in receipt[key]
                .as_array()
                .ok_or_else(|| failure("CORRELATION", "clock", "Missing work clocks"))?
            {
                sqlx::query("UPDATE engine_flow030_clock_works SET snapshot_json=? WHERE system_id=? AND invocation_id=? AND activation_id=?")
                    .bind(clock["snapshot"].to_string()).bind(&self.identity.system_start_id).bind(&self.identity.flow_invocation_id).bind(clock["workId"].as_str()).execute(&mut *db).await?;
            }
        }
        for work_id in receipt["expiredWorkIds"]
            .as_array()
            .ok_or_else(|| failure("CORRELATION", "clock", "Missing expirations"))?
        {
            if contract == argentic_core::AGSDL_040 {
                // A committed acceptance ends the agent's work budget. The
                // registry may still be joining its process and reading proof
                // of a steer. Both decisions arbitrate under the admission lock.
                let finished: bool = sqlx::query_scalar(
                    "SELECT EXISTS (
                        SELECT 1 FROM engine_flow030_work_events
                        WHERE system_id=? AND invocation_id=? AND activation_id=?
                          AND json_extract(decision_json ->> '$.receiptJson', '$.decision.kind')
                              IN ('accepted', 'stopping', 'finished')
                    )",
                )
                .bind(&self.identity.system_start_id)
                .bind(&self.identity.flow_invocation_id)
                .bind(work_id.as_str())
                .fetch_one(&mut *db)
                .await?;
                if finished {
                    continue;
                }
            }
            sqlx::query("INSERT INTO engine_flow030_clock_controls(system_id,invocation_id,activation_id,receipt_id,reason,observed_ms) VALUES(?,?,?,?,'TIMEOUT',?) ON CONFLICT DO NOTHING")
                .bind(&self.identity.system_start_id).bind(&self.identity.flow_invocation_id).bind(work_id.as_str()).bind(uuid()).bind(event["nowMs"].as_str()).execute(&mut *db).await?;
        }
        sqlx::query("UPDATE engine_flow030_clock_events SET output_json=? WHERE system_id=? AND invocation_id=? AND event_id=? AND output_json IS NULL")
            .bind(serde_json::to_string(output)?).bind(&self.identity.system_start_id).bind(&self.identity.flow_invocation_id).bind(&input.event_id).execute(db).await?;
        Ok(())
    }
    /// The QA driver supplies a pinned flow stabilizer that drains decisions,
    /// joined terminations and routes. A large Elapse is split at every actual
    /// wake boundary; newly admitted continuations receive only the remainder.
    #[cfg(all(test, feature = "agsdl-engine-qa", debug_assertions))]
    pub(crate) async fn elapse_qa<F, Fut>(
        &self,
        host: &HostState,
        clock: &Arc<crate::agsdl_host_clock030::VirtualClock030>,
        ms: u64,
        cancelled: &CancellationToken,
        mut stabilize: F,
    ) -> Result<()>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<()>>,
    {
        let source: Arc<dyn Clock030> = clock.clone();
        require(
            Arc::ptr_eq(&source, &self.clock),
            "QA clock does not own this flow",
        )?;
        let end = clock.now_ms().checked_add(ms).ok_or_else(|| {
            failure(
                "INPUT_LIMIT",
                "clock",
                "Elapse exceeds exact native milliseconds",
            )
        })?;
        loop {
            stabilize().await?;
            if !self.flow_running().await? {
                clock.advance_to(end)?;
                return Ok(());
            }
            self.tick(host).await?;
            self.pump(host, cancelled).await?;
            stabilize().await?;
            if !self.flow_running().await? {
                clock.advance_to(end)?;
                return Ok(());
            }
            let now = clock.now_ms();
            if now == end {
                return Ok(());
            }
            let wake = self.next_wake_ms().await?.unwrap_or(end - now);
            require(wake != 0, "Due control was not stabilized or retired")?;
            clock.advance_to(end.min(now.checked_add(wake).unwrap_or(end)))?;
        }
    }

    #[cfg(all(test, feature = "agsdl-engine-qa", debug_assertions))]
    async fn flow_running(&self) -> Result<bool> {
        let raw: String = sqlx::query_scalar(
            "SELECT state_json FROM engine_flow030_runs WHERE system_id=? AND invocation_id=?",
        )
        .bind(&self.identity.system_start_id)
        .bind(&self.identity.flow_invocation_id)
        .fetch_one(&self.pool)
        .await?;
        let state: FlowState030 = serde_json::from_str(&raw)?;
        Ok(!state.stopping && state.verdict.is_none())
    }

    /// Native deadline slice; wakeups are hints, tick/pump arbitrates expiration.
    pub(crate) async fn next_wake_ms(&self) -> Result<Option<u64>> {
        let mut db = self.pool.acquire().await?;
        let row = self.clock_row(&mut db).await?;
        let receipt: Value = serde_json::from_str(row.try_get("receipt_json")?)?;
        let observed = validation::ms(row.try_get("observed_ms")?)?;
        let now = self.clock.now_ms();
        require(now >= observed, "Native wake time moved backwards")?;
        // The pure delay is relative to its captured observation, not the
        // later moment at which evaluation/commit/IO finishes.
        Ok(receipt["nextWakeMs"]
            .as_u64()
            .map(|delay| delay.saturating_sub(now - observed)))
    }
    pub(crate) async fn owned_work(&self, id: &str) -> Result<OwnedHumanWork030> {
        let raw: String = sqlx::query_scalar("SELECT work_json FROM engine_flow030_clock_works WHERE system_id=? AND invocation_id=? AND activation_id=?")
            .bind(&self.identity.system_start_id).bind(&self.identity.flow_invocation_id).bind(id).fetch_one(&self.pool).await?;
        Ok(serde_json::from_str(&raw)?)
    }
    /// Close the request when its native shell client disappears. A person
    /// may have answered just before the disconnect. Consume that decision
    /// without delivering it to the abandoned operation.
    pub(crate) async fn withdraw_ask(
        &self,
        host: &HostState,
        work: &OwnedHumanWork030,
        request_id: &str,
    ) -> Result<()> {
        let cancel = CancellationToken::new();
        let _operation = self.operation.lock().await;
        self.settle_pending_flow_clock(host).await?;
        {
            let lease = host.start_lease().await?;
            let _gate = host.admission_lock().lock_owned().await;
            let mut connection = self.durable_connection().await?;
            let mut tx = connection.begin_with("BEGIN IMMEDIATE").await?;
            self.guard(&mut tx, &lease).await?;
            self.verify_work(&mut tx, work).await?;
            let row = self.request_row(&mut tx, request_id).await?;
            let works: Vec<String> = serde_json::from_str(row.try_get("work_ids_json")?)?;
            require(
                row.try_get::<&str, _>("kind")? == "ask" && works == [work.work_id()],
                "Foreign action withdrawal",
            )?;
            if row.try_get::<&str, _>("delivery")? != "pending" {
                return Ok(());
            }
            if row.try_get::<Option<&str>, _>("resolution_json")?.is_none() {
                let event =
                    json!({"kind":"withdrawAsk","requestId":request_id,"workId":work.work_id()});
                self.reserve(&mut tx, event, &self.clock.now_ms().to_string())
                    .await?;
            }
            tx.commit().await?;
        }
        self.pump(host, &cancel).await?;
        let decision = self.take_resolution(host, request_id).await?;
        self.acknowledge(host, &decision).await?;
        self.pump(host, &cancel).await?;
        Ok(())
    }

    pub(crate) async fn resolution_ready(&self, request: &str) -> Result<bool> {
        Ok(sqlx::query_scalar("SELECT resolution_json IS NOT NULL FROM engine_flow030_human_requests WHERE system_id=? AND invocation_id=? AND request_id=?")
            .bind(&self.identity.system_start_id).bind(&self.identity.flow_invocation_id).bind(request).fetch_one(&self.pool).await?)
    }
    pub(crate) async fn request_kind(&self, request: &str) -> Result<String> {
        Ok(sqlx::query_scalar("SELECT kind FROM engine_flow030_human_requests WHERE system_id=? AND invocation_id=? AND request_id=?")
            .bind(&self.identity.system_start_id).bind(&self.identity.flow_invocation_id).bind(request).fetch_one(&self.pool).await?)
    }
    #[cfg(test)]
    pub(crate) async fn oldest_open(&self, work: &OwnedHumanWork030) -> Result<Option<String>> {
        let rows = sqlx::query("SELECT request_id,work_ids_json FROM engine_flow030_human_requests WHERE system_id=? AND invocation_id=? AND resolution_json IS NULL ORDER BY rowid")
            .bind(&self.identity.system_start_id).bind(&self.identity.flow_invocation_id).fetch_all(&self.pool).await?;
        require(
            work.identity == self.identity
                && work.owner == self.owner
                && work.generation == self.generation,
            "Foreign request lookup",
        )?;
        for row in rows {
            let works: Vec<String> = serde_json::from_str(row.try_get("work_ids_json")?)?;
            if works.contains(&work.work_id().into()) {
                return Ok(Some(row.try_get("request_id")?));
            }
        }
        Ok(None)
    }
    /// Reserve before delivery. A reservation left by a crash stays visible and
    /// cannot be delivered again automatically, even in the same generation.
    pub(crate) async fn take_resolution(
        &self,
        host: &HostState,
        request_id: &str,
    ) -> Result<ResolvedHumanDecision030> {
        self.read_resolution(host, request_id, true).await
    }

    // Action policy lookup can wait. Leave the answer undelivered until the
    // actual provider write takes admission and reserves it.
    pub(crate) async fn action_resolution(
        &self,
        host: &HostState,
        request_id: &str,
    ) -> Result<ResolvedHumanDecision030> {
        self.read_resolution(host, request_id, false).await
    }

    async fn read_resolution(
        &self,
        host: &HostState,
        request_id: &str,
        reserve: bool,
    ) -> Result<ResolvedHumanDecision030> {
        let lease = host.start_lease().await?;
        let _gate = host.admission_lock().lock_owned().await;
        let mut connection = self.durable_connection().await?;
        let mut tx = connection.begin_with("BEGIN IMMEDIATE").await?;
        self.guard(&mut tx, &lease).await?;
        let row = self.request_row(&mut tx, request_id).await?;
        if !reserve {
            require(
                row.try_get::<&str, _>("kind")? == "ask",
                "Expected action request",
            )?;
            let work_ids: Vec<String> = serde_json::from_str(row.try_get("work_ids_json")?)?;
            if let [id] = work_ids.as_slice() {
                if self.operator_held(&mut tx, id).await? {
                    return Err(failure(
                        "OPERATOR_HOLD",
                        "approval",
                        "The turn is being interrupted",
                    ));
                }
            }
        }
        let raw: Option<String> = row.try_get("resolution_json")?;
        require(
            raw.is_some() && row.try_get::<&str, _>("delivery")? == "pending",
            "Resolution absent or already reserved; inspect instead of replay",
        )?;
        validation::validate_stored(self, &mut tx, request_id).await?;
        let decision = ResolvedHumanDecision030 {
            identity: self.identity.clone(),
            owner: self.owner.clone(),
            generation: self.generation.clone(),
            request_id: request_id.into(),
            work_ids: serde_json::from_str(row.try_get("work_ids_json")?)?,
            kind: row.try_get("kind")?,
            payload: serde_json::from_str(row.try_get("payload_json")?)?,
            resolution: serde_json::from_str(raw.as_deref().unwrap())?,
        };
        if reserve {
            self.reserve_resolution(&mut tx, request_id).await?;
        }
        tx.commit().await?;
        Ok(decision)
    }

    async fn reserve_resolution(&self, db: &mut SqliteConnection, request_id: &str) -> Result<()> {
        let update = sqlx::query("UPDATE engine_flow030_human_requests SET delivery='reserved' WHERE system_id=? AND invocation_id=? AND request_id=? AND delivery='pending'")
            .bind(&self.identity.system_start_id).bind(&self.identity.flow_invocation_id).bind(request_id).execute(db).await?;
        require(
            update.rows_affected() == 1,
            "Resolution is no longer pending",
        )
    }
    /// Exact protocol answer bytes come from the accepted native submission,
    /// never from the pure closure marker used to resume a question's clocks.
    pub(crate) async fn protocol_answer(
        &self,
        decision: &ResolvedHumanDecision030,
    ) -> Result<Option<String>> {
        require(
            decision.identity == self.identity
                && decision.owner == self.owner
                && decision.generation == self.generation
                && decision.kind == "question",
            "Foreign protocol decision",
        )?;
        if decision.resolution["expired"] == true {
            return Ok(None);
        }
        let receipt_id = decision.resolution["answer"]["nativeReceiptId"]
            .as_str()
            .ok_or_else(|| failure("CORRELATION", "question", "Native answer receipt absent"))?;
        let raw: String = sqlx::query_scalar("SELECT answer_json FROM engine_flow030_human_submissions WHERE system_id=? AND invocation_id=? AND receipt_id=? AND request_id=? AND accepted=1")
            .bind(&self.identity.system_start_id).bind(&self.identity.flow_invocation_id).bind(receipt_id).bind(&decision.request_id).fetch_one(&self.pool).await?;
        match serde_json::from_str::<HumanAnswer030>(&raw)? {
            HumanAnswer030::Question { answer_json } => Ok(Some(answer_json)),
            _ => Err(failure(
                "CORRELATION",
                "question",
                "Answer is not a protocol question",
            )),
        }
    }

    pub(crate) async fn acknowledge(
        &self,
        host: &HostState,
        decision: &ResolvedHumanDecision030,
    ) -> Result<()> {
        require(
            decision.identity == self.identity
                && decision.owner == self.owner
                && decision.generation == self.generation,
            "Foreign delivery acknowledgment",
        )?;
        self.settle_pending_flow_clock(host).await?;
        let lease = host.start_lease().await?;
        let _gate = host.admission_lock().lock_owned().await;
        let mut connection = self.durable_connection().await?;
        let mut tx = connection.begin_with("BEGIN IMMEDIATE").await?;
        self.guard(&mut tx, &lease).await?;
        self.reserve(
            &mut tx,
            json!({"kind":"ack","requestId":decision.request_id}),
            &self.clock.now_ms().to_string(),
        )
        .await?;
        sqlx::query("UPDATE engine_flow030_human_requests SET delivery='acknowledged' WHERE system_id=? AND invocation_id=? AND request_id=? AND resolution_json=?")
            .bind(&self.identity.system_start_id).bind(&self.identity.flow_invocation_id).bind(&decision.request_id).bind(decision.resolution.to_string()).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }
    /// Send an action answer under the same admission gate as Stop/revocation.
    /// IO happens after the read transaction ends, with native ownership live.
    pub(crate) async fn deliver_action<F, Fut>(
        &self,
        host: &HostState,
        work: &OwnedHumanWork030,
        decision: &ResolvedHumanDecision030,
        payload: &Value,
        deliver: F,
    ) -> Result<()>
    where
        F: FnOnce(bool) -> Fut,
        Fut: std::future::Future<Output = Result<()>>,
    {
        let lease = host.start_lease().await?;
        let gate = host.admission_lock().lock_owned().await;
        let mut connection = self.durable_connection().await?;
        let mut tx = connection.begin().await?;
        self.guard(&mut tx, &lease).await?;
        self.verify_work(&mut tx, work).await?;
        require(
            decision.identity == self.identity
                && decision.owner == self.owner
                && decision.generation == self.generation
                && decision.kind == "ask"
                && decision.work_ids == [work.work_id()]
                && decision.payload == *payload,
            "Action decision belongs to another native request",
        )?;
        let row = self.request_row(&mut tx, &decision.request_id).await?;
        require(
            row.try_get::<Option<String>, _>("resolution_json")?
                .as_deref()
                == Some(decision.resolution.to_string().as_str()),
            "Action decision is stale",
        )?;
        if self.operator_held(&mut tx, work.work_id()).await? {
            return Err(failure(
                "OPERATOR_HOLD",
                "approval",
                "The turn is being interrupted",
            ));
        }
        self.reserve_resolution(&mut tx, &decision.request_id)
            .await?;
        tx.commit().await?;
        lease.ensure_live()?;
        #[cfg(test)]
        let gate = host.flow030().track_mcp_gate(gate);
        deliver(
            decision.resolution["expired"] != true
                && decision.resolution["answer"]["decision"] == "yes",
        )
        .await?;
        drop(gate);
        drop(lease);
        self.acknowledge(host, decision).await
    }
    pub(crate) async fn retire_finished(&self, host: &HostState, id: &str) -> Result<()> {
        let result = async {
            self.settle_pending_flow_clock(host).await?;
            let row = sqlx::query("SELECT work_json,retired FROM engine_flow030_clock_works WHERE system_id=? AND invocation_id=? AND activation_id=?")
                .bind(&self.identity.system_start_id).bind(&self.identity.flow_invocation_id).bind(id).fetch_one(&self.pool).await?;
            if row.try_get::<bool, _>("retired")? {
                return Ok(());
            }
            let work: OwnedHumanWork030 = serde_json::from_str(row.try_get("work_json")?)?;
            self.retire(host, &work).await?;
            self.pump(host, &CancellationToken::new()).await?;
            Ok(())
        }.await;
        self.ignore_if_stopping(result).await
    }

    async fn ignore_if_stopping(&self, result: Result<()>) -> Result<()> {
        if result.is_err() {
            let stopping: bool = sqlx::query_scalar(
                "SELECT json_extract(state_json,'$.stopping') FROM engine_flow030_runs WHERE system_id=? AND invocation_id=?",
            ).bind(&self.identity.system_start_id).bind(&self.identity.flow_invocation_id)
                .fetch_one(&self.pool).await?;
            if stopping {
                return Ok(());
            }
        }
        result
    }

    pub(crate) async fn pump_call_retirement(&self, host: &HostState) -> Result<()> {
        self.ignore_if_stopping(self.settle_pending_flow_clock(host).await)
            .await
    }

    pub(crate) async fn lock_call_settlement(
        &self,
        host: &HostState,
        cancel: &CancellationToken,
    ) -> Result<tokio::sync::MutexGuard<'_, ()>> {
        let operation = self.operation.lock().await;
        self.ignore_if_stopping(self.pump(host, cancel).await.map(|_| ()))
            .await?;
        Ok(operation)
    }

    pub(crate) async fn reserve_call_retirement_tx(
        &self,
        db: &mut SqliteConnection,
        id: &str,
    ) -> Result<()> {
        self.reserve_finished_retirement_tx(db, id, "call").await
    }

    pub(crate) async fn reserve_agent_retirement_tx(
        &self,
        db: &mut SqliteConnection,
        id: &str,
    ) -> Result<()> {
        let timed_out: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM engine_flow030_clock_controls
              WHERE system_id=? AND invocation_id=? AND activation_id=? AND joined=0)",
        )
        .bind(&self.identity.system_start_id)
        .bind(&self.identity.flow_invocation_id)
        .bind(id)
        .fetch_one(&mut *db)
        .await?;
        if timed_out {
            return Err(failure(
                "TIMEOUT",
                "clock",
                "The work deadline already committed",
            ));
        }
        self.reserve_finished_retirement_tx(db, id, "agent").await
    }

    async fn reserve_finished_retirement_tx(
        &self,
        db: &mut SqliteConnection,
        id: &str,
        kind: &str,
    ) -> Result<()> {
        let active: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM engine_flow030_clock_works
              WHERE system_id=? AND invocation_id=? AND activation_id=?
                AND kind=? AND retired=0)",
        )
        .bind(&self.identity.system_start_id)
        .bind(&self.identity.flow_invocation_id)
        .bind(id)
        .bind(kind)
        .fetch_one(&mut *db)
        .await?;
        if active {
            self.reserve(
                db,
                json!({"kind":"retire","workId":id}),
                &self.clock.now_ms().to_string(),
            )
            .await?;
        }
        Ok(())
    }
    /// TIMEOUT was already persisted by pump, under the shared admission fence.
    /// Return only its native control. Cancel AND join the actual owned future
    /// and descendants after this method returns, with no gate held. The work
    /// controller must receive stop TIMEOUT before any provider terminal.
    pub(crate) async fn timeout_controls(&self) -> Result<Vec<TimeoutControl030>> {
        let rows = sqlx::query("SELECT activation_id,receipt_id FROM engine_flow030_clock_controls WHERE system_id=? AND invocation_id=? AND joined=0")
            .bind(&self.identity.system_start_id).bind(&self.identity.flow_invocation_id).fetch_all(&self.pool).await?;
        rows.iter()
            .map(|r| {
                Ok(TimeoutControl030 {
                    work_id: r.try_get("activation_id")?,
                })
            })
            .collect()
    }
}

#[cfg(test)]
#[path = "agsdl_host_human030_tests.rs"]
mod tests;

#[path = "agsdl_host_human030_runtime.rs"]
mod runtime;
pub(crate) use runtime::HumanRuntime030;

#[cfg(any(test, feature = "dev-support"))]
#[path = "agsdl_host_human030_fixture.rs"]
mod fixture;
