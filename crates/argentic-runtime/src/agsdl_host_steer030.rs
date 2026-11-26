//! In-process delivery to an exact owned agent attempt. Receiving a message
//! outside that parent cannot mint a consumption acknowledgment.
use crate::{
    agsdl_host_flow030::FlowWorkAttempt030,
    persistence::{
        failure,
        flow030::{
            accepts_steer030, steer_effect_eligible030, FlowEffect030, FlowEffectKind030,
            FlowState030,
        },
        Result,
    },
    tasks::StreamTasks,
};
use std::sync::{Arc, OnceLock};
use tokio::sync::{mpsc, oneshot, Mutex, OwnedMutexGuard};

#[derive(Clone)]
pub(crate) struct SteerSender030(mpsc::Sender<QueuedSteer030>);
pub struct SteerInbox030(mpsc::Receiver<QueuedSteer030>);

pub(crate) fn channel030() -> (SteerSender030, SteerInbox030) {
    let (send, recv) = mpsc::channel(1);
    (SteerSender030(send), SteerInbox030(recv))
}

pub(crate) fn asynchronous_contract(contract: &serde_json::Value) -> bool {
    contract == argentic_core::AGSDL_040
}

fn refused() -> crate::persistence::dto::EngineFailure {
    failure(
        "CORRELATION",
        "steer",
        "Steer recipient differs from the owned work",
    )
}
fn uncertain() -> crate::persistence::dto::EngineFailure {
    failure(
        "UNCERTAIN",
        "steer",
        "Steer consumption was not acknowledged",
    )
}

fn matches_attempt(effect: &FlowEffect030, attempt: &FlowWorkAttempt030) -> bool {
    matches!(&effect.kind, FlowEffectKind030::Steer { work, .. }
        if effect.identity == *attempt.identity()
        && work.activation_id == attempt.work().activation_id
        && work.start_effect_id == attempt.start_effect_id()
        && work.agent_instance_id == attempt.work().agent_instance_id)
}

pub(crate) fn wire_eligible030(state: &FlowState030, effect: &FlowEffect030) -> bool {
    let FlowEffectKind030::Steer {
        work,
        delivery_activation_id,
        ..
    } = &effect.kind
    else {
        return false;
    };
    accepts_steer030(state, work)
        && steer_effect_eligible030(state, effect)
        && !state.activations.iter().any(|activation| {
            activation.id == *delivery_activation_id && activation.phase == "withdrawingSteer"
        })
}

#[derive(Clone)]
pub(crate) struct SteerGate030 {
    pub admission: Arc<Mutex<()>>,
    pub pool: sqlx::SqlitePool,
    pub deadline: tokio::time::Instant,
    wire_checkpoint: Arc<OnceLock<String>>,
}

impl SteerGate030 {
    pub(crate) fn new(
        admission: Arc<Mutex<()>>,
        pool: sqlx::SqlitePool,
        deadline: tokio::time::Instant,
    ) -> Self {
        Self {
            admission,
            pool,
            deadline,
            wire_checkpoint: Arc::new(OnceLock::new()),
        }
    }

    pub(crate) fn wire_checkpoint(&self) -> Option<&str> {
        self.wire_checkpoint.get().map(String::as_str)
    }

    fn lost_evidence(&self) -> crate::persistence::dto::EngineFailure {
        if self.wire_checkpoint().is_some() {
            uncertain()
        } else {
            failure(
                "STEER_NOT_SENT",
                "steer",
                "The offer did not reach provider admission",
            )
        }
    }
}

pub struct SteerDelivery030 {
    effect: FlowEffect030,
    attempt: FlowWorkAttempt030,
    ack: oneshot::Sender<Result<ConsumedSteer030>>,
    gate: Option<SteerGate030>,
}

/// Sealed evidence from the owned consumer, never deserialized from IPC.
pub(crate) struct ConsumedSteer030 {
    effect: FlowEffect030,
    attempt: FlowWorkAttempt030,
    consumed: bool,
}
impl ConsumedSteer030 {
    pub(crate) fn consumed(&self) -> bool {
        self.consumed
    }
    pub(crate) fn verifies(&self, effect: &FlowEffect030, attempt: &FlowWorkAttempt030) -> bool {
        self.effect == *effect
            && self.attempt.provider_attempt_id() == attempt.provider_attempt_id()
            && self.attempt.owner() == attempt.owner()
            && self.attempt.generation() == attempt.generation()
            && matches_attempt(effect, attempt)
    }
}
impl SteerSender030 {
    pub(crate) fn is_closed(&self) -> bool {
        self.0.is_closed()
    }
    #[cfg(test)]
    pub(crate) async fn deliver(
        &self,
        effect: &FlowEffect030,
        attempt: &FlowWorkAttempt030,
    ) -> Result<ConsumedSteer030> {
        self.deliver_with_gate(effect, attempt, None).await
    }

    pub(crate) async fn deliver_with_gate(
        &self,
        effect: &FlowEffect030,
        attempt: &FlowWorkAttempt030,
        gate: Option<SteerGate030>,
    ) -> Result<ConsumedSteer030> {
        if !matches_attempt(effect, attempt) {
            return Err(refused());
        }
        let (ack, mut result) = oneshot::channel();
        let offer = self.0.send(QueuedSteer030(Some(SteerDelivery030 {
            effect: effect.clone(),
            attempt: attempt.clone(),
            ack,
            gate: gate.clone(),
        })));
        let offered = match &gate {
            Some(gate) => tokio::time::timeout_at(gate.deadline, offer)
                .await
                .map_err(|_| gate.lost_evidence())?,
            None => offer.await,
        };
        if offered.is_err() {
            return Ok(ConsumedSteer030 {
                effect: effect.clone(),
                attempt: attempt.clone(),
                consumed: false,
            });
        }
        if let Some(gate) = gate {
            match tokio::time::timeout_at(gate.deadline, &mut result).await {
                Ok(receipt) => receipt.map_err(|_| gate.lost_evidence())?,
                Err(_) => {
                    // Do not close the receiver while admission can authorize
                    // a send. A write already admitted holds the same guard.
                    let _admission = gate.admission.clone().lock_owned().await;
                    let receipt = result.try_recv().map_err(|_| gate.lost_evidence());
                    drop(result);
                    receipt?
                }
            }
        } else {
            result.await.map_err(|_| uncertain())?
        }
    }
}
impl SteerInbox030 {
    pub fn close(&mut self) {
        self.0.close();
    }
    pub(crate) fn try_receive(&mut self) -> Option<SteerDelivery030> {
        self.0.try_recv().ok().and_then(|mut offer| offer.0.take())
    }
    pub async fn receive(&mut self) -> Option<SteerDelivery030> {
        self.0.recv().await.and_then(|mut offer| offer.0.take())
    }
}
/// The mailbox can prove non-receipt even if preparation fails before
/// the executor starts polling its inbox.
struct QueuedSteer030(Option<SteerDelivery030>);

impl Drop for QueuedSteer030 {
    fn drop(&mut self) {
        if let Some(offer) = self.0.take().filter(|offer| offer.gate.is_some()) {
            let _ = offer.ack.send(Ok(ConsumedSteer030 {
                effect: offer.effect,
                attempt: offer.attempt,
                consumed: false,
            }));
        }
    }
}

impl SteerDelivery030 {
    pub fn effect(&self) -> &FlowEffect030 {
        &self.effect
    }

    async fn current_state(
        &self,
    ) -> Result<
        Option<(
            OwnedMutexGuard<()>,
            crate::persistence::flow030::FlowState030,
        )>,
    > {
        let Some(gate) = &self.gate else {
            return Ok(None);
        };
        let guard = gate.admission.clone().lock_owned().await;
        let raw: String = sqlx::query_scalar(
            "SELECT state_json FROM engine_flow030_runs WHERE system_id=? AND invocation_id=?",
        )
        .bind(&self.effect.identity.system_start_id)
        .bind(&self.effect.identity.flow_invocation_id)
        .fetch_one(&gate.pool)
        .await?;
        Ok(Some((guard, serde_json::from_str(&raw)?)))
    }

    /// Serialize the actual provider send with Stop and recipient completion.
    /// The adapter releases the guard before awaiting a provider response.
    pub async fn wire_admission(&self) -> Result<Option<OwnedMutexGuard<()>>> {
        let Some((guard, state)) = self.current_state().await? else {
            return Ok(None);
        };
        if self.ack.is_closed()
            || self
                .deadline()
                .is_some_and(|deadline| tokio::time::Instant::now() >= deadline)
        {
            return Err(uncertain());
        }
        if !wire_eligible030(&state, &self.effect) {
            return Err(failure(
                "STALE_REFERENCE",
                "steer",
                "The targeted work can no longer receive a steer",
            ));
        }
        let gate = self.gate.as_ref().unwrap();
        let checkpoint: String = sqlx::query_scalar(crate::persistence::flow_store030::RUN_HEAD030)
            .bind(&self.effect.identity.system_start_id)
            .bind(&self.effect.identity.flow_invocation_id)
            .fetch_one(&gate.pool)
            .await?;
        gate.wire_checkpoint
            .set(checkpoint)
            .map_err(|_| refused())?;
        Ok(Some(guard))
    }

    pub async fn observe_turn_end_while_counted(&self) -> Result<bool> {
        let Some((_guard, state)) = self.current_state().await? else {
            return Ok(false);
        };
        let FlowEffectKind030::Steer { work, .. } = &self.effect.kind else {
            return Err(refused());
        };
        Ok(crate::persistence::flow030::accepts_steer030(&state, work))
    }

    pub fn deadline(&self) -> Option<tokio::time::Instant> {
        self.gate.as_ref().map(|gate| gate.deadline)
    }

    pub fn unavailable_content(self) {
        let _ = self.ack.send(Err(failure(
            "CONTENT_UNAVAILABLE",
            "steer",
            "The steer message content could not be prepared",
        )));
    }

    pub(crate) fn wrong_recipient(self) {
        let _ = self.ack.send(Err(refused()));
    }

    /// A provider receipt still has to precede a committed work exclusion.
    pub(crate) async fn acknowledge_from_provider(
        delivery: &mut Option<Self>,
        tasks: &StreamTasks,
        attempt: &FlowWorkAttempt030,
        turn_ended_while_counted: bool,
    ) -> Result<()> {
        let Some(offer) = delivery.as_ref() else {
            return Ok(());
        };
        if offer.gate.is_some() && offer.ack.is_closed() {
            delivery.take();
            return Ok(());
        }
        // Keep the proof and offer in their owner until the cancellable read
        // returns. Stop can continue this same observation if it wins the race.
        let observation = match (turn_ended_while_counted, offer.deadline()) {
            (true, _) => None,
            (false, Some(deadline)) => {
                match tokio::time::timeout_at(deadline, offer.current_state()).await {
                    Ok(state) => state?,
                    Err(_) => {
                        delivery.take();
                        return Ok(());
                    }
                }
            }
            (false, None) => offer.current_state().await?,
        };
        let offer = delivery.take().unwrap();
        if let Some((_guard, state)) = &observation {
            let FlowEffectKind030::Steer { work, .. } = &offer.effect.kind else {
                return Err(refused());
            };
            if !crate::persistence::flow030::accepts_steer030(state, work) {
                return Ok(());
            }
        }
        if offer
            .deadline()
            .is_some_and(|deadline| tokio::time::Instant::now() >= deadline)
        {
            return Ok(());
        }
        offer.resolve(tasks, attempt, true)
    }

    /// Let the recipient return after submitting its result. Its registry
    /// still joins the observer and the provider process before routing.
    pub(crate) fn handoff_observation<F>(
        mut self,
        tasks: &StreamTasks,
        attempt: &FlowWorkAttempt030,
        observation: F,
    ) -> Result<()>
    where
        F: std::future::Future<Output = Result<Option<bool>>> + Send + 'static,
    {
        let request = self.verify_consumer(tasks, attempt)?;
        let cleanup_deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
        let deadline = self
            .deadline()
            .map_or(cleanup_deadline, |deadline| deadline.min(cleanup_deadline));
        let cleanup = tokio::spawn(async move {
            let observed = tokio::select! {
                result = tokio::time::timeout_at(deadline, observation) => result,
                _ = self.ack.closed() => return Ok(()),
            };
            let result = observed
                .map_err(|_| uncertain())
                .and_then(|result| result)
                .and_then(|receipt| receipt.ok_or_else(uncertain))
                .map(|consumed| ConsumedSteer030 {
                    effect: self.effect,
                    attempt: self.attempt,
                    consumed,
                });
            let _ = self.ack.send(result);
            Ok(())
        });
        tasks
            .attach_cleanup030(&request, cleanup)
            .map_err(|_| refused())
    }

    pub(crate) fn report_uncertain(self) {
        let _ = self.ack.send(Err(uncertain()));
    }

    /// Call only after the transport has consumed the message and sender.
    pub async fn acknowledge(
        self,
        tasks: &StreamTasks,
        attempt: &FlowWorkAttempt030,
    ) -> Result<()> {
        Self::acknowledge_from_provider(&mut Some(self), tasks, attempt, false).await
    }

    /// Only before any provider send, or after an explicit provider rejection.
    pub fn not_sent(self, tasks: &StreamTasks, attempt: &FlowWorkAttempt030) -> Result<()> {
        self.resolve(tasks, attempt, false)
    }
    fn resolve(
        self,
        tasks: &StreamTasks,
        attempt: &FlowWorkAttempt030,
        consumed: bool,
    ) -> Result<()> {
        if consumed
            && self
                .gate
                .as_ref()
                .is_some_and(|gate| gate.wire_checkpoint().is_none())
        {
            return Err(refused());
        }
        let asynchronous = self.gate.is_some();
        match self.evidence(tasks, attempt, consumed)?() {
            Ok(()) => Ok(()),
            // Argentic already settled the sender's attempt. A late report
            // cannot affect the recipient's work.
            Err(()) if asynchronous => Ok(()),
            Err(()) => Err(uncertain()),
        }
    }

    #[cfg(test)]
    pub(crate) fn simulation(
        self,
        tasks: &StreamTasks,
        attempt: &FlowWorkAttempt030,
        consumed: bool,
    ) -> Result<impl FnOnce() -> std::result::Result<(), ()> + Send> {
        self.evidence(tasks, attempt, consumed)
    }

    fn evidence(
        self,
        tasks: &StreamTasks,
        attempt: &FlowWorkAttempt030,
        consumed: bool,
    ) -> Result<impl FnOnce() -> std::result::Result<(), ()> + Send> {
        self.verify_consumer(tasks, attempt)?;
        Ok(move || {
            self.ack
                .send(Ok(ConsumedSteer030 {
                    effect: self.effect,
                    attempt: self.attempt,
                    consumed,
                }))
                .map_err(|_| ())
        })
    }

    fn verify_consumer(&self, tasks: &StreamTasks, attempt: &FlowWorkAttempt030) -> Result<String> {
        if !matches_attempt(&self.effect, attempt)
            || self.attempt.provider_attempt_id() != attempt.provider_attempt_id()
            || self.attempt.owner() != attempt.owner()
            || self.attempt.generation() != attempt.generation()
        {
            return Err(refused());
        }
        let request = format!(
            "flow030:{}:{}",
            attempt.identity().system_start_id,
            attempt.start_effect_id()
        );
        tasks
            .verify_agent_scope030(&request, attempt)
            .map_err(|_| refused())?;
        Ok(request)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::flow030::WorkRef030;

    fn effect(attempt: &FlowWorkAttempt030) -> FlowEffect030 {
        FlowEffect030 {
            id: "steer-effect".into(),
            identity: attempt.identity().clone(),
            revision: "2".into(),
            kind: FlowEffectKind030::Steer {
                work: WorkRef030 {
                    activation_id: attempt.work().activation_id.clone(),
                    start_effect_id: attempt.start_effect_id().into(),
                    agent_instance_id: attempt.work().agent_instance_id.clone(),
                },
                delivery_activation_id: "nudge".into(),
                message_json: "{\"prompt\":\"Update the changelog.\"}".into(),
                occurrence: None,
                sender: None,
            },
        }
    }

    #[tokio::test]
    async fn recipient_returns_before_observation_but_joins_it_before_routing() {
        let attempt = FlowWorkAttempt030::registry_fixture030("attempt");
        let target = attempt.clone();
        let fx = effect(&attempt);
        let tasks = StreamTasks::default();
        let registry = tasks.clone();
        let (send, mut inbox) = channel030();
        let (finished, ended) = oneshot::channel();
        let (confirm, receipt) = oneshot::channel();
        tasks
            .start_agent030(
                format!(
                    "flow030:{}:{}",
                    attempt.identity().system_start_id,
                    attempt.start_effect_id()
                ),
                &attempt,
                async move {
                    inbox
                        .receive()
                        .await
                        .unwrap()
                        .handoff_observation(&registry, &target, async {
                            Ok(receipt.await.ok().map(|()| true))
                        })
                        .unwrap();
                    finished.send(()).unwrap();
                },
            )
            .unwrap();
        let mut pending = Box::pin(send.deliver(&fx, &attempt));
        tokio::select! {
            _ = &mut pending => panic!("Steer settled before the receipt"),
            result = ended => result.unwrap(),
        }
        let mut joined = Box::pin(tasks.join_agent030(&attempt, false));
        tokio::select! {
            biased;
            _ = &mut joined => panic!("Work joined before its observer ended"),
            _ = tokio::task::yield_now() => (),
        }
        confirm.send(()).unwrap();
        assert!(pending.await.unwrap().consumed());
        joined.await.unwrap();
    }

    #[tokio::test]
    async fn dropping_an_unpolled_040_mailbox_proves_non_receipt() {
        let attempt = FlowWorkAttempt030::registry_fixture030("attempt");
        let fx = effect(&attempt);
        let (send, inbox) = channel030();
        let mut result = Box::pin(send.deliver_with_gate(
            &fx,
            &attempt,
            Some(SteerGate030::new(
                Arc::new(Mutex::new(())),
                crate::sqlite::memory().await.unwrap(),
                tokio::time::Instant::now() + std::time::Duration::from_secs(2),
            )),
        ));
        tokio::select! {
            biased;
            _ = &mut result => panic!("Mail was settled before closing"),
            _ = tokio::task::yield_now() => (),
        }
        assert_eq!(send.0.capacity(), 0);
        drop(inbox);
        assert!(!result.await.unwrap().consumed());
    }

    #[tokio::test]
    async fn expiration_while_waiting_for_admission_prevents_provider_send() {
        let attempt = FlowWorkAttempt030::registry_fixture030("attempt");
        let fx = effect(&attempt);
        let id = attempt.identity();
        let pool = crate::sqlite::memory().await.unwrap();
        sqlx::query("INSERT INTO engine_flow030_runs VALUES(?,?,?,?,'{}','{}',?)")
            .bind(&id.system_start_id)
            .bind(&id.flow_invocation_id)
            .bind(&id.snapshot_id)
            .bind(&id.epoch)
            .bind(
                serde_json::to_string(&crate::persistence::flow030::FlowState030::new(id.clone()))
                    .unwrap(),
            )
            .execute(&pool)
            .await
            .unwrap();
        let gate = SteerGate030::new(
            Arc::new(Mutex::new(())),
            pool,
            tokio::time::Instant::now() + std::time::Duration::from_millis(250),
        );
        let hold = gate.admission.clone().lock_owned().await;
        let (send, mut inbox) = channel030();
        let (receipt, ()) = tokio::join!(
            send.deliver_with_gate(&fx, &attempt, Some(gate.clone())),
            async {
                let offer = inbox.receive().await.unwrap();
                let (guard, ()) = tokio::join!(offer.wire_admission(), async {
                    tokio::time::sleep_until(gate.deadline).await;
                    drop(hold);
                });
                assert_eq!(guard.unwrap_err().code, "UNCERTAIN");
            }
        );
        assert_eq!(receipt.err().unwrap().code, "STEER_NOT_SENT");
    }

    #[tokio::test]
    async fn expired_040_confirmation_does_not_fail_the_recipient() {
        let attempt = FlowWorkAttempt030::registry_fixture030("attempt");
        let (send, mut inbox) = channel030();
        let tasks = StreamTasks::default();
        let owned_tasks = tasks.clone();
        let target = attempt.clone();
        let (received, receipt) = oneshot::channel();
        let (expired, wait) = oneshot::channel();
        let (completed, done) = oneshot::channel();
        tasks
            .start_agent030(
                format!(
                    "flow030:{}:{}",
                    attempt.identity().system_start_id,
                    attempt.start_effect_id()
                ),
                &attempt,
                async move {
                    let delivery = inbox.receive().await.unwrap();
                    received.send(()).unwrap();
                    wait.await.unwrap();
                    completed
                        .send(delivery.acknowledge(&owned_tasks, &target).await)
                        .unwrap();
                },
            )
            .unwrap();
        let fx = effect(&attempt);
        let pool = crate::sqlite::memory().await.unwrap();
        let mut delivery = Box::pin(send.deliver_with_gate(
            &fx,
            &attempt,
            Some(SteerGate030::new(
                Arc::new(Mutex::new(())),
                pool,
                tokio::time::Instant::now() + std::time::Duration::from_secs(30),
            )),
        ));
        tokio::select! {
            result = &mut delivery => panic!("Delivery resolved before expiry: {}", result.err().unwrap().code),
            result = receipt => result.unwrap(),
        }
        drop(delivery);
        expired.send(()).unwrap();
        done.await.unwrap().unwrap();
        tasks.join_agent030(&attempt, false).await.unwrap();
    }

    #[tokio::test]
    async fn only_exact_owned_consumer_can_acknowledge_delivery() {
        for mode in 0..3 {
            let attempt = FlowWorkAttempt030::registry_fixture030("attempt");
            let fx = effect(&attempt);
            let (send, mut inbox) = channel030();
            let tasks = StreamTasks::default();
            let target = attempt.clone();
            let registry = tasks.clone();
            let request = format!(
                "flow030:{}:{}",
                attempt.identity().system_start_id,
                attempt.start_effect_id()
            );
            let (done, consumed) = oneshot::channel();
            tasks
                .start_agent030(request.clone(), &attempt, async move {
                    let delivery = inbox.receive().await.unwrap();
                    assert!(matches!(&delivery.effect().kind,
                    FlowEffectKind030::Steer { message_json, delivery_activation_id, .. }
                    if message_json.contains("changelog") && delivery_activation_id == "nudge"));
                    if mode == 2 {
                        drop(delivery);
                        let _ = done.send(());
                        return;
                    }
                    let foreign = FlowWorkAttempt030::registry_fixture030("foreign");
                    let result = delivery
                        .acknowledge(&registry, if mode == 0 { &target } else { &foreign })
                        .await;
                    assert_eq!(result.is_ok(), mode == 0);
                    let _ = done.send(());
                })
                .unwrap();
            let result = send.deliver(&fx, &attempt).await;
            assert_eq!(result.is_ok(), mode == 0);
            if let Ok(receipt) = result {
                assert!(receipt.verifies(&fx, &attempt));
            }
            consumed.await.unwrap();
            tasks.join_agent030(&attempt, false).await.unwrap();
        }
    }

    #[tokio::test]
    async fn receiving_outside_owned_scope_never_proves_consumption() {
        let attempt = FlowWorkAttempt030::registry_fixture030("attempt");
        let fx = effect(&attempt);
        let (send, mut inbox) = channel030();
        let (sent, received) = tokio::join!(send.deliver(&fx, &attempt), async {
            inbox
                .receive()
                .await
                .unwrap()
                .acknowledge(&StreamTasks::default(), &attempt)
                .await
        });
        assert!(sent.is_err());
        assert!(received.is_err());
    }

    #[tokio::test]
    async fn foreign_work_and_closed_recipient_are_refused_without_acknowledgment() {
        let attempt = FlowWorkAttempt030::registry_fixture030("attempt");
        let mut fx = effect(&attempt);
        let (send, inbox) = channel030();
        fx.identity.epoch = "foreign".into();
        assert_eq!(
            send.deliver(&fx, &attempt).await.err().unwrap().code,
            "CORRELATION"
        );
        drop(inbox);
        assert!(!send
            .deliver(&effect(&attempt), &attempt)
            .await
            .unwrap()
            .consumed());
    }
}
