//! Versioned, content-free projection of retained runs, including after restart.
//! A continuation binds the run, reducer revision and external projection facts.
use crate::agsdl_host::HostState;
use crate::persistence::{
    dto::EngineSourceRevision,
    failure,
    flow030::FlowPlan030,
    runtime_projection030::{
        self, RuntimeActivation030, RuntimeGroup030, RuntimeInstance030, RuntimePath030,
        RuntimeProjection030,
    },
    Result,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::{Row, SqlitePool};
use std::collections::BTreeSet;

const PAGE_SIZE: usize = 100;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RuntimeProjectionScope030 {
    pub system_start_id: String,
    pub source: EngineSourceRevision,
    pub snapshot_id: String,
}

/// The first page obtains the snapshot identity from the authenticated capture.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RuntimeProjectionQuery030 {
    pub system_start_id: String,
    pub source: EngineSourceRevision,
    pub snapshot_id: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RuntimeProjectionContinuation030 {
    pub scope: RuntimeProjectionScope030,
    pub revision: String,
    pub offset: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_version: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
#[allow(
    clippy::large_enum_variant,
    reason = "Boxing activation data changes the public projection item type"
)]
pub enum RuntimeProjectionItem030 {
    Instance { value: RuntimeInstance030 },
    Activation { value: RuntimeActivation030 },
    Group { value: RuntimeGroup030 },
    Path { value: RuntimePath030 },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
pub enum OperatorWorkState {
    Holding,
    Held,
    Interrupted { code: String },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RuntimeProjectionPage030 {
    pub scope: RuntimeProjectionScope030,
    pub flow_invocation_id: String,
    pub revision: String,
    pub stopping: bool,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub paused: bool,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub interrupted_while_paused: bool,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub operator_works: std::collections::BTreeMap<String, OperatorWorkState>,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub model_overrides: std::collections::BTreeMap<String, crate::capture::ModelOverride>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub termination_uncertain: bool,
    pub verdict: Option<String>,
    pub total: String,
    pub items: Vec<RuntimeProjectionItem030>,
    pub next_page: Option<RuntimeProjectionContinuation030>,
}

fn page(
    scope: RuntimeProjectionScope030,
    continuation: Option<RuntimeProjectionContinuation030>,
    view: RuntimeProjection030,
    snapshot_version: Option<String>,
) -> Result<RuntimeProjectionPage030> {
    if scope.system_start_id != view.system_start_id || scope.snapshot_id != view.snapshot_id {
        return Err(failure(
            "CORRELATION",
            "scope",
            "Projection scope differs from captured flow",
        ));
    }
    let offset = match continuation {
        Some(cursor) => {
            if cursor.scope != scope
                || cursor.revision != view.revision
                || cursor.snapshot_version != snapshot_version
            {
                return Err(failure(
                    "CONFLICT",
                    "continuation",
                    "Projection changed since the previous page",
                ));
            }
            cursor.offset as usize
        }
        None => 0,
    };
    // The pure projector sorts section IDs and preserves branch/path/FIFO order.
    let mut items: Vec<RuntimeProjectionItem030> = view
        .instances
        .into_iter()
        .map(|value| RuntimeProjectionItem030::Instance { value })
        .collect();
    items.extend(
        view.activations
            .into_iter()
            .map(|value| RuntimeProjectionItem030::Activation { value }),
    );
    items.extend(
        view.groups
            .into_iter()
            .map(|value| RuntimeProjectionItem030::Group { value }),
    );
    items.extend(
        view.paths
            .into_iter()
            .map(|value| RuntimeProjectionItem030::Path { value }),
    );
    if offset > items.len() {
        return Err(failure(
            "CONFLICT",
            "continuation",
            "Projection offset is invalid",
        ));
    }
    let total = items.len();
    let end = offset.saturating_add(PAGE_SIZE).min(total);
    let next_page = if end < total {
        Some(RuntimeProjectionContinuation030 {
            scope: scope.clone(),
            revision: view.revision.clone(),
            snapshot_version,
            offset: u32::try_from(end)
                .map_err(|_| failure("INPUT_LIMIT", "continuation", "Projection is too large"))?,
        })
    } else {
        None
    };
    let items = items.into_iter().skip(offset).take(end - offset).collect();
    Ok(RuntimeProjectionPage030 {
        scope,
        flow_invocation_id: view.flow_invocation_id,
        revision: view.revision,
        stopping: view.stopping,
        paused: false,
        interrupted_while_paused: false,
        operator_works: Default::default(),
        model_overrides: Default::default(),
        termination_uncertain: false,
        verdict: view.verdict,
        total: total.to_string(),
        items,
        next_page,
    })
}

fn snapshot_version(
    handoffs: &BTreeSet<String>,
    interrupted: bool,
    revoked: bool,
    mcp_uncertain: bool,
) -> Result<String> {
    let facts = serde_json::to_vec(&(handoffs, interrupted, revoked, mcp_uncertain))?;
    Ok(format!("{:x}", Sha256::digest(facts)))
}

pub(crate) async fn read(
    host: &HostState,
    pool: &SqlitePool,
    query: RuntimeProjectionQuery030,
    continuation: Option<RuntimeProjectionContinuation030>,
) -> Result<RuntimeProjectionPage030> {
    // Observation does not require a live executor or grant authority to resume.
    // Read capture and state together so a page describes one committed revision.
    let row = sqlx::query("SELECT s.capture_json,r.plan_json,r.state_json,r.source_json,r.snapshot_id,r.epoch,
        (SELECT json_group_array(h.effect_id) FROM engine_flow030_handoffs h
          WHERE h.system_id=r.system_id AND h.invocation_id=r.invocation_id
          AND h.receipt_json ->> '$.kind'='nativeOwnedHandoff030') AS handoffs,
        EXISTS(SELECT 1 FROM engine_flow030_interruptions i WHERE i.system_id=r.system_id) AS interrupted,
        EXISTS(SELECT 1 FROM engine_flow030_owners o WHERE o.system_id=r.system_id AND o.status='revoked') AS revoked FROM engine_start030_requests s JOIN engine_flow030_runs r ON r.system_id=s.system_id AND r.invocation_id=s.invocation_id WHERE s.system_id=?")
        .bind(&query.system_start_id).fetch_optional(pool).await?
        .ok_or_else(|| failure("NOT_FOUND", "systemStartId", "Native flow capture is absent"))?;
    let capture: crate::persistence::start030::StartCapture030 =
        serde_json::from_str(row.try_get("capture_json")?)?;
    let state: crate::persistence::flow030::FlowState030 =
        serde_json::from_str(row.try_get("state_json")?)?;
    let source: EngineSourceRevision = serde_json::from_str(row.try_get("source_json")?)?;
    if capture.source != query.source
        || capture.identity.system_start_id != query.system_start_id
        || state.identity != capture.identity
        || source != capture.source
        || row.try_get::<String, _>("snapshot_id")? != capture.identity.snapshot_id
        || row.try_get::<String, _>("epoch")? != capture.identity.epoch
        || row.try_get::<String, _>("plan_json")? != capture.compiled_plan_json
        || query
            .snapshot_id
            .as_ref()
            .is_some_and(|id| *id != capture.identity.snapshot_id)
    {
        return Err(failure(
            "CORRELATION",
            "scope",
            "Projection source or snapshot differs",
        ));
    }
    let scope = RuntimeProjectionScope030 {
        system_start_id: query.system_start_id,
        source: query.source,
        snapshot_id: capture.identity.snapshot_id,
    };
    let plan: FlowPlan030 = serde_json::from_str(&capture.compiled_plan_json)?;
    let handoffs = serde_json::from_str(row.try_get("handoffs")?)?;
    let projection =
        runtime_projection030::project_runtime030(&plan, &state, &handoffs).map_err(|_| {
            failure(
                "CORRELATION",
                "projection",
                "Captured flow projection is invalid",
            )
        })?;
    let current = plan.versions.contract == argentic_core::AGSDL_040;
    let interrupted: bool = row.try_get("interrupted")?;
    let revoked: bool = row.try_get("revoked")?;
    let mcp_uncertain = current
        && host
            .flow030()
            .supervisor_registry()
            .outcome(&scope.system_start_id)
            .is_some_and(|outcome| {
                outcome.is_err_and(|error| error.code == "MCP_TERMINATION_UNCERTAIN")
            });
    let snapshot_version = current
        .then(|| snapshot_version(&handoffs, interrupted, revoked, mcp_uncertain))
        .transpose()?;
    let mut page = page(scope, continuation, projection, snapshot_version)?;
    page.model_overrides = crate::capture::verify_model_overrides(
        &plan,
        &serde_json::from_str(&capture.registry_json)?,
    )?;
    if current {
        if interrupted {
            page.verdict = Some("interrupted".into());
            page.stopping = false;
        }
        page.stopping &= page.verdict.is_none();
        page.paused = state.paused && page.verdict.is_none();
        page.interrupted_while_paused = state.paused && interrupted;
        page.operator_works = state
            .operator_holds
            .iter()
            .map(|(id, hold)| {
                let status = if interrupted {
                    OperatorWorkState::Interrupted {
                        code: "INTERRUPTED".into(),
                    }
                } else if hold.ready {
                    OperatorWorkState::Held
                } else {
                    OperatorWorkState::Holding
                };
                (id.clone(), status)
            })
            .collect();
        page.termination_uncertain = (interrupted
            || ((revoked || mcp_uncertain) && page.verdict.is_none()))
            && state
                .activations
                .iter()
                .any(|activation| activation.physical);
    }
    Ok(page)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::dto::EngineSourceOwner;

    #[test]
    fn continuation_is_bound_to_scope_revision_and_external_facts() {
        let scope = RuntimeProjectionScope030 {
            system_start_id: "system".into(),
            snapshot_id: "snapshot".into(),
            source: EngineSourceRevision {
                owner: EngineSourceOwner {
                    project_id: "project".into(),
                    plan_id: "plan".into(),
                    branch: "develop".into(),
                },
                revision: "1".into(),
                source_sha256: "a".repeat(64),
            },
        };
        let instance = |id: usize| RuntimeInstance030 {
            id: format!("{id:03}"),
            agent: "worker".into(),
            selector_kind: "shared".into(),
            selector_name: None,
            current_activation_id: None,
            queue_activation_ids: vec![],
            discarded: false,
        };
        let view = RuntimeProjection030 {
            system_start_id: "system".into(),
            flow_invocation_id: "flow".into(),
            snapshot_id: "snapshot".into(),
            revision: "7".into(),
            stopping: false,
            verdict: None,
            instances: (0..101).map(instance).collect(),
            activations: vec![],
            groups: vec![],
            paths: vec![],
        };
        let handoffs = BTreeSet::new();
        let version = Some(snapshot_version(&handoffs, false, false, false).unwrap());
        let first = page(scope.clone(), None, view.clone(), version.clone()).unwrap();
        assert_eq!(first.items.len(), 100);
        let legacy = serde_json::to_value(&first).unwrap();
        assert!(legacy.get("terminationUncertain").is_none());
        assert_eq!(
            serde_json::from_value::<RuntimeProjectionPage030>(legacy).unwrap(),
            first
        );
        let mut uncertain = first.clone();
        uncertain.termination_uncertain = true;
        assert_eq!(
            serde_json::to_value(uncertain).unwrap()["terminationUncertain"],
            true
        );
        let next = first.next_page.unwrap();
        assert_eq!(
            page(
                scope.clone(),
                Some(next.clone()),
                view.clone(),
                version.clone()
            )
            .unwrap()
            .items
            .len(),
            1
        );
        let admitted = BTreeSet::from(["effect".into()]);
        for changed_version in [
            snapshot_version(&admitted, false, false, false).unwrap(),
            snapshot_version(&handoffs, true, true, false).unwrap(),
            snapshot_version(&handoffs, false, false, true).unwrap(),
        ] {
            assert_eq!(
                page(
                    scope.clone(),
                    Some(next.clone()),
                    view.clone(),
                    Some(changed_version)
                )
                .unwrap_err()
                .code,
                "CONFLICT"
            );
        }
        let mut changed = view.clone();
        changed.revision = "8".into();
        assert_eq!(
            page(scope.clone(), Some(next.clone()), changed, version.clone())
                .unwrap_err()
                .code,
            "CONFLICT"
        );
        let mut other = next;
        other.scope.snapshot_id = "other".into();
        assert_eq!(
            page(scope, Some(other), view, version).unwrap_err().code,
            "CONFLICT"
        );
    }
}
