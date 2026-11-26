//! Codex qualification and native sandbox restrictions.
use crate::{
    agsdl_host_work_admission030::{
        pointer_token030, qualified_observation030, AttestedWorkPolicy030,
        PreparedEffectiveAgentWork030,
    },
    persistence::{failure, Result},
};
use serde_json::Value;

pub(crate) fn prepare(prepared: PreparedEffectiveAgentWork030) -> Result<AttestedWorkPolicy030> {
    let registry = serde_json::from_str(prepared.registry_json())?;
    let observed = qualified_observation030(
        &registry,
        prepared.agent(),
        "codex",
        crate::agsdl_host_codex::ADAPTER,
    )?;
    if prepared.harness() != "codex"
        || !observed["version"]
            .as_str()
            .is_some_and(crate::agsdl_host_codex::qualified_version030)
        || observed["models"].as_array().is_none_or(Vec::is_empty)
    {
        return Err(failure(
            "HARNESS_REFUSED",
            &format!("/agents/{}/harness", pointer_token030(prepared.agent())),
            "The captured Codex adapter is not qualified",
        ));
    }
    validate_codex_document_at030(prepared.document(), prepared.document_location())?;
    codex_read_only030(prepared.policy())?;
    let contract: Value = serde_json::from_str(prepared.contract_json())?;
    if !contract["planFirst"].is_null() && contract["planFirst"] != "auto" {
        return Err(failure(
            "HARNESS_CAPABILITY",
            &format!("/agents/{}/planFirst", pointer_token030(prepared.agent())),
            "agsdl.execution.codexPlanAsk030",
        ));
    }
    if let Some(command) = contract
        .pointer("/completionCheck/command")
        .and_then(Value::as_str)
    {
        crate::agsdl_host_completion030::grant(prepared.policy(), command)?;
    }
    AttestedWorkPolicy030::new(
        prepared,
        crate::agsdl_host_codex::ADAPTER,
        30_000.try_into().unwrap(),
        65_536.try_into().unwrap(),
        false,
    )
}

pub(crate) fn codex_permissions030(value: &serde_json::Value, pointer: &str) -> Result<()> {
    use crate::permissions::{Action, Effect, Outside, Preset, RequestedPermissions, Reviewer};
    let parsed = serde_json::from_value::<RequestedPermissions>(value.clone()).map_err(|_| {
        failure(
            "HARNESS_CAPABILITY",
            pointer,
            "Codex permissions are invalid",
        )
    })?;
    if value.get("reviewer").and_then(serde_json::Value::as_str) == Some("model") {
        return Err(failure(
            "HARNESS_CAPABILITY",
            &format!("{pointer}/reviewer"),
            "agsdl.execution.codexReviewer030",
        ));
    }
    let supported = match parsed {
        RequestedPermissions::Preset(Preset::ReadOnly | Preset::Workspace) => true,
        RequestedPermissions::Object(p) => {
            matches!(p.preset, Some(Preset::ReadOnly | Preset::Workspace))
                && p.directories.is_empty()
                && p.outside.is_none_or(|o| o == Outside::Deny)
                && p.reviewer != Some(Reviewer::Model)
                && p.rules.iter().all(|r| {
                    r.action == Action::Mcp
                        || (r.action == Action::Write
                            && r.effect == Effect::Ask
                            && r.pattern.is_none())
                })
        }
        _ => false,
    };
    if supported {
        return Ok(());
    }
    if let Some(rules) = value.get("rules").and_then(serde_json::Value::as_array) {
        for (index, rule) in rules.iter().enumerate() {
            if rule["effect"] == "ask" {
                return Err(failure(
                    "HARNESS_CAPABILITY",
                    &format!("{pointer}/rules/{index}/effect"),
                    "agsdl.execution.codexAsk030",
                ));
            }
        }
    }
    if !matches!(value.as_str(), Some("read-only" | "workspace")) {
        return Err(failure(
            "HARNESS_CAPABILITY",
            pointer,
            "agsdl.execution.codexPermissions030",
        ));
    }
    Ok(())
}

/// All ceilings must be expressible by the qualified named native profiles.
/// A restrictive custom rule is refused rather than silently dropped.
pub(crate) fn codex_read_only030(policy: &crate::permissions::EffectivePolicy) -> Result<bool> {
    crate::agsdl_host_completion030::workspace_sandbox_read_only030(
        policy,
        "The effective Codex permission ceiling cannot be represented by this slice",
    )
}

/// Shared native preflight/admission restriction. Inline values are payloads,
/// so data containing keys such as uri or timeoutMs is never treated as code.
pub(crate) fn validate_codex_document030(document: &serde_json::Value) -> Result<()> {
    validate_codex_document_at030(document, None)
}

pub(crate) fn validate_codex_document_at030(
    document: &serde_json::Value,
    document_location: Option<&url::Url>,
) -> Result<()> {
    use serde_json::Value;
    let agents = document.get("agents").and_then(Value::as_object);
    if !agents.is_some_and(|agents| agents.values().any(|agent| agent["harness"] == "codex")) {
        return Ok(());
    }
    if let Some(agents) = agents {
        for (name, agent) in agents
            .iter()
            .filter(|(_, agent)| agent["harness"] == "codex")
        {
            let pointer = format!("/agents/{}", pointer_token030(name));
            codex_permissions030(&agent["permissions"], &format!("{pointer}/permissions"))?;
            if agent.get("completionCheck").is_some()
                && !cfg!(all(target_os = "macos", target_arch = "aarch64"))
            {
                return Err(failure(
                    "HARNESS_CAPABILITY",
                    &format!("{pointer}/completionCheck"),
                    "The confined completion-check executor is qualified only on macOS ARM64",
                ));
            }
            if agent.get("completionCheck").is_some()
                && (agent["permissions"] == "read-only"
                    || agent["permissions"]["preset"] == "read-only")
            {
                return Err(failure(
                    "HARNESS_CAPABILITY",
                    &format!("{pointer}/completionCheck"),
                    "Completion checks require shell permission; the read-only preset denies shell",
                ));
            }
            for key in ["planFirst"] {
                if agent.get(key).is_some() && !(key == "planFirst" && agent[key] == "auto") {
                    return Err(failure(
                        "HARNESS_CAPABILITY",
                        &format!("{pointer}/{key}"),
                        if key == "planFirst" {
                            "agsdl.execution.codexPlanAsk030"
                        } else {
                            "This Codex option is not connected"
                        },
                    ));
                }
            }
            if let Some(settings) = agent["harnessSettings"].as_object() {
                for (key, value) in settings {
                    if key != "webSearch" || value != "disabled" {
                        return Err(failure(
                            "HARNESS_CAPABILITY",
                            &format!("{pointer}/harnessSettings/{}", pointer_token030(key)),
                            "Only the isolated profile's disabled web search setting is supported",
                        ));
                    }
                }
            }
            if agent["plan"] == true {
                return Err(failure(
                    "HARNESS_CAPABILITY",
                    &format!("{pointer}/plan"),
                    "Codex plans are not connected",
                ));
            }
        }
    }
    crate::agsdl_host_content030::validate_text_sources(document, document_location)
}

#[cfg(test)]
mod codex030_tests {
    use super::*;
    use serde_json::{json, Value};
    fn document() -> Value {
        json!({"agents":{"worker":{"harness":"codex","permissions":"workspace"}},"flow":{"entry":"work","steps":{"work":{"type":"agent","agent":"worker","next":[]}}}})
    }
    #[test]
    fn codex_documents_accept_host_calls_but_keep_unsupported_call_features_closed() {
        let mut doc = document();
        doc["mcpServers"] = json!({"local":{"command":["/bin/sh","server.sh"]}});
        doc["flow"]["steps"]["call"] = json!({"type":"call","command":"true","parse":"json","next":{"passed":[],"failed":"work"}});
        doc["flow"]["steps"]["tool"] = json!({"type":"call","mcp":{"server":"local","tool":"check"},"arguments":{"data":{"value":{"type":"approval","timeoutMs":1,"uri":"literal"}}}});
        assert!(validate_codex_document030(&doc).is_ok());
        doc["flow"]["steps"]["call"]["timeoutMs"] = json!(1);
        assert!(validate_codex_document030(&doc).is_ok());
        doc["flow"]["steps"]["call"]
            .as_object_mut()
            .unwrap()
            .remove("timeoutMs");
        doc["flow"]["steps"]["call"]["next"] = json!({"step":"work","each":["data"]});
        assert!(validate_codex_document030(&doc).is_ok());
    }

    #[test]
    fn preflight_and_admission_share_precise_slice_refusals() {
        let cases = [
            (
                "/agents/worker/planFirst",
                json!("ask"),
                "HARNESS_CAPABILITY",
                "/agents/worker/planFirst",
            ),
            (
                "/agents/worker/permissions",
                json!({"rules":[{"action":"write","effect":"ask"}]}),
                "HARNESS_CAPABILITY",
                "/agents/worker/permissions/rules/0/effect",
            ),
        ];
        for (pointer, value, code, location) in cases {
            let mut doc = document();
            let (parent, key) = pointer.rsplit_once('/').unwrap();
            doc.pointer_mut(parent)
                .unwrap()
                .as_object_mut()
                .unwrap()
                .insert(key.into(), value);
            let failure = validate_codex_document030(&doc).unwrap_err();
            assert_eq!(failure.code, code, "{pointer}");
            assert_eq!(failure.pointer, location, "{pointer}");
        }
        let mut doc = document();
        doc["content"] = json!({"remote":{"uri":"https://example.invalid/source"}});
        let failure = validate_codex_document030(&doc).unwrap_err();
        assert_eq!(failure.code, "CONTENT_SOURCE_UNSUPPORTED");
        assert_eq!(failure.pointer, "/content/remote/uri");
    }
    #[test]
    fn named_entries_do_not_hide_deadlines_or_external_sources() {
        for name in ["value", "results"] {
            let mut doc = document();
            doc["flow"]["steps"] = json!({name: {"type":"agent", "agent":"worker", "timeoutMs":1}});
            assert!(validate_codex_document030(&doc).is_ok());
            let mut doc = document();
            doc["content"] = json!({name: {"uri":"https://example.invalid/source"}});
            let error = validate_codex_document030(&doc).unwrap_err();
            assert_eq!(error.pointer, format!("/content/{name}/uri"));
            let mut doc = document();
            doc["agents"]["worker"]["resources"] =
                json!({name: {"uri":"https://example.invalid/source"}});
            let error = validate_codex_document030(&doc).unwrap_err();
            assert_eq!(
                error.pointer,
                format!("/agents/worker/resources/{name}/uri")
            );
        }
    }
    #[test]
    fn inline_payloads_and_other_harnesses_are_not_mistaken_for_executable_options() {
        let mut doc = document();
        doc["content"] = json!({"inline":{"value":{"uri":"data","timeoutMs":1,"type":"call"}}});
        doc["content"]["timeoutMs"] = json!({"value":"Ordinary named content"});
        doc["agents"]["worker"]["resources"] = json!({"sample":{"ref":"inline"}});
        assert!(validate_codex_document030(&doc).is_ok());
        doc["agents"]["worker"]["harness"] = json!("example");
        doc["agents"]["worker"]["planFirst"] = json!("ask");
        assert!(validate_codex_document030(&doc).is_ok());
    }
    #[test]
    fn native_profile_respects_the_most_restrictive_supported_ceiling() {
        use crate::permissions::{normalize_requested, EffectivePolicy, PathContext};
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let context = PathContext {
            workspace: root.clone(),
            home: root,
        };
        let normalize = |value| {
            normalize_requested(serde_json::from_value(value).unwrap(), context.clone()).unwrap()
        };
        let workspace = normalize(json!("workspace"));
        let readonly = normalize(json!("read-only"));
        assert!(!codex_read_only030(&EffectivePolicy::new(
            workspace.clone(),
            workspace.clone(),
            workspace.clone()
        ))
        .unwrap());
        assert!(codex_read_only030(&EffectivePolicy::new(
            workspace.clone(),
            readonly.clone(),
            readonly
        ))
        .unwrap());
        let custom =
            normalize(json!({"preset":"workspace","rules":[{"action":"shell","effect":"deny"}]}));
        assert_eq!(
            codex_read_only030(&EffectivePolicy::new(
                workspace.clone(),
                custom.clone(),
                custom
            ))
            .unwrap_err()
            .code,
            "HARNESS_CAPABILITY"
        );
        assert!(codex_permissions030(
            &json!({"preset":"workspace","rules":[{"action":"network","effect":"ask"}]}),
            "/agsdlHostPermissions"
        )
        .is_err());
    }
}
