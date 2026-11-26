use super::{admission, approvals, CodexExecutor030};
use crate::{
    api::{self, CodexObservation},
    harness::{
        AgentExecutor030, AgentExecutorContext030, HarnessAdapter030, HarnessObservation030,
    },
    host::HostContext,
    persistence::{failure, Result},
};
use futures::future::BoxFuture;
use serde_json::{json, Value};
use std::{path::Path, sync::Arc};

pub(crate) struct CodexAdapter030(String);

impl CodexAdapter030 {
    pub(crate) fn new() -> Self {
        Self(approvals::module030())
    }
}

impl HarnessAdapter030 for CodexAdapter030 {
    fn id(&self) -> &str {
        "codex"
    }
    fn adapter_id(&self) -> &str {
        api::CODEX_ADAPTER
    }
    fn module_json(&self) -> &str {
        &self.0
    }

    fn qualify_agent(&self, agent: &Value, pointer: &str) -> Result<()> {
        admission::codex_permissions030(&agent["permissions"], &format!("{pointer}/permissions"))?;
        if agent["planFirst"] == "ask" {
            return Err(failure(
                "HARNESS_CAPABILITY",
                &format!("{pointer}/planFirst"),
                "agsdl.execution.codexPlanAsk030",
            ));
        }
        Ok(())
    }

    fn validate_document(&self, document: &Value, location: Option<&url::Url>) -> Result<()> {
        admission::validate_codex_document_at030(document, location)
    }

    fn observe<'a>(
        &'a self,
        host: &'a HostContext,
        scope: &'a Value,
        read_only: bool,
    ) -> BoxFuture<'a, Result<HarnessObservation030>> {
        Box::pin(async move {
            if let Some(observed) = api::observe_codex(host, scope, read_only).await? {
                return Ok(observation(&observed));
            }
            let status = api::codex_status(host).await;
            Err(failure(&status.code, "engine", "Official private Codex profile is not ready; consult the documented login procedure"))
        })
    }

    fn validate_observation(
        &self,
        document: &Value,
        scope: &Value,
        observed: &HarnessObservation030,
    ) -> Result<()> {
        validate_captured_observation(document, scope, &observed.version, &observed.models)
    }

    fn executor(&self, context: AgentExecutorContext030<'_>) -> Result<Arc<dyn AgentExecutor030>> {
        Ok(Arc::new(
            CodexExecutor030::new(context.host.clone())
                .with_mcp_equipment(context.document)
                .with_approvals(context.approvals),
        ))
    }

    fn supports_host_approval(&self) -> bool {
        cfg!(all(target_os = "macos", target_arch = "aarch64"))
    }
    fn supports_mcp_equipment(&self) -> bool {
        true
    }

    fn opt_in_error(&self) -> crate::persistence::dto::EngineFailure {
        failure(
            "CODEX_OPT_IN_REQUIRED",
            "source",
            "Explicit Codex opt-in is required for preflight or Start",
        )
    }
}

pub(crate) fn validate_observation(
    document: &Value,
    scope: &Value,
    observed: &CodexObservation,
) -> Result<()> {
    let document_location = crate::capture::document_location(scope)?;
    crate::agsdl_host_codex030::admission::validate_codex_document_at030(
        document,
        document_location.as_ref(),
    )?;
    validate_captured_observation(document, scope, &observed.version, &observed.models)
}

fn validate_captured_observation(
    document: &Value,
    scope: &Value,
    version: &str,
    models: &[Value],
) -> Result<()> {
    let document_location = crate::capture::document_location(scope)?;
    if document["agents"].as_object().is_some_and(|agents| {
        agents
            .values()
            .filter(|agent| agent["harness"] == "codex")
            .any(|agent| {
                agent["skills"]
                    .as_array()
                    .is_some_and(|skills| !skills.is_empty())
            })
    }) {
        let workspace = scope["root"]
            .as_str()
            .ok_or_else(|| failure("CORRELATION", "skills", "Native project scope is absent"))?;
        let snapshot_root = if scope.get("projectSnapshot").is_some() {
            Some(crate::capture::captured_project_root(
                document_location.as_ref(),
            )?)
        } else {
            None
        };
        let mut codex_document = document.clone();
        if let Some(agents) = codex_document["agents"].as_object_mut() {
            agents.retain(|_, agent| agent["harness"] == "codex");
        }
        crate::agsdl_host_content030::qualify_skill_folders(
            &codex_document,
            snapshot_root
                .as_deref()
                .unwrap_or_else(|| Path::new(workspace)),
            document_location.as_ref(),
        )?;
    }
    if !crate::api::qualified_codex_version(version) || models.is_empty() {
        return Err(failure(
            "CODEX_VERSION_UNQUALIFIED",
            "/agents",
            "Codex observation is outside the qualified version or catalogue",
        ));
    }
    Ok(())
}

pub(crate) fn observation(observed: &CodexObservation) -> HarnessObservation030 {
    let adapter = crate::agsdl_host_codex::ADAPTER;
    let evidence = json!({"adapterId":adapter,"version":observed.version,"profileDigest":observed.profile.digest,"accountDigest":observed.account_digest,"kind":"native-codex"});
    let evidence_json = evidence.to_string();
    let capabilities = ["result-submission", "continuity", "permission-enforcement", "contained-stop", "response-attribution", "instructions", "resources", "model.name", "model.effort", "instance.new", "delivery.queue", "delivery.steer", "answer-timeout", "reviewer.person", "planFirst.auto", "completion-check", "skills", "mcpServers", "harness-settings"]
        .into_iter().filter(|name| match *name {
            "completion-check" => cfg!(all(target_os = "macos", target_arch = "aarch64")),
            "skills" => observed.version == "0.167.0" && crate::content_io::file_helper::qualified_platform(),
            _ => true,
        }).map(|name| json!({"name":name,"support":if matches!(name, "resources" | "planFirst.auto") {"approximate"} else {"native"},"evidenceJson":evidence_json})).collect::<Vec<_>>();
    let default = observed
        .models
        .iter()
        .find(|model| model["isDefault"] == true)
        .and_then(|model| model["name"].as_str());
    let models = observed.models.iter().map(|model| json!({"name":model["name"],"efforts":model["efforts"],"defaultEffort":model["defaultEffort"]})).collect::<Vec<_>>();
    HarnessObservation030 {
        version: observed.version.clone(),
        models,
        default_model: default.map(str::to_owned),
        capabilities,
        evidence,
        version_receipt: observed.profile.digest.clone(),
        fingerprint: json!({"profile":observed.profile.identity(),"accountDigest":observed.account_digest,"models":observed.models,"version":observed.version}),
    }
}
