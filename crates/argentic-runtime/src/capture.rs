//! Observed Codex registry preparation shared by native hosts.
use crate::persistence::{failure, Result};
use semver::{Version, VersionReq};
use serde_json::{json, Value};
use std::{collections::BTreeSet, path::Path, sync::OnceLock};

pub(crate) fn document_location(scope: &Value) -> Result<Option<url::Url>> {
    scope
        .get("documentLocation")
        .map(|location| {
            let invalid = || failure("CORRELATION", "scope", "Invalid captured document location");
            let uri = location.as_str().ok_or_else(invalid)?;
            crate::content::local_file_path(uri, None).map_err(|_| invalid())?;
            url::Url::parse(uri).map_err(|_| invalid())
        })
        .transpose()
}

pub(crate) fn captured_document_location(
    capture: &crate::persistence::start030::StartCapture030,
) -> Result<Option<url::Url>> {
    let private: Value = serde_json::from_str(&capture.private_capture_json)?;
    private
        .get("callScopeJson")
        .and_then(Value::as_str)
        .map(|raw_scope| document_location(&serde_json::from_str(raw_scope)?))
        .transpose()
        .map(Option::flatten)
}

fn version_requirement(range: &str) -> std::result::Result<VersionReq, semver::Error> {
    static SEPARATORS: OnceLock<regex::Regex> = OnceLock::new();
    range.parse().or_else(|error| {
        if range.contains(',') {
            return Err(error);
        }
        // AgSDL examples separate comparison operators with spaces. The
        // SemVer parser accepts commas between those same comparisons.
        SEPARATORS
            .get_or_init(|| regex::Regex::new(r"[ \t]+([<>=~^])").unwrap())
            .replace_all(range.trim(), ", $1")
            .parse()
    })
}

/// Describe only the installed adapter observed through the native profile.
/// This prepares compiler inputs and never authorizes execution.
pub fn codex_registry(
    document: &Value,
    source_sha256: &str,
    scope: &Value,
    observed: &crate::api::CodexObservation,
) -> Result<(Value, Vec<Value>)> {
    let document_location = document_location(scope)?;
    crate::agsdl_host_work_admission030::validate_codex_document_at030(
        document,
        document_location.as_ref(),
    )?;
    if document["agents"].as_object().is_some_and(|agents| {
        agents.values().any(|agent| {
            agent["skills"]
                .as_array()
                .is_some_and(|skills| !skills.is_empty())
        })
    }) {
        crate::agsdl_host_codex030::content::qualify_skill_folders(
            document,
            Path::new(scope["root"].as_str().ok_or_else(|| {
                failure("CORRELATION", "skills", "Native project scope is absent")
            })?),
            document_location.as_ref(),
        )?;
    }
    if !crate::api::qualified_codex_version(&observed.version) || observed.models.is_empty() {
        return Err(failure(
            "CODEX_VERSION_UNQUALIFIED",
            "/agents",
            "Codex observation is outside the qualified version or catalogue",
        ));
    }
    let adapter = crate::agsdl_host_codex::ADAPTER;
    let evidence = json!({"adapterId":adapter,"version":observed.version,"profileDigest":observed.profile.digest,"accountDigest":observed.account_digest,"kind":"native-codex"}).to_string();
    let mut capabilities = ["result-submission", "continuity", "permission-enforcement", "contained-stop", "response-attribution", "instructions", "resources", "model.name", "model.effort", "instance.new", "delivery.queue", "delivery.steer", "answer-timeout", "reviewer.person", "planFirst.auto", "completion-check", "skills"]
        .into_iter().filter(|name| match *name {
            "completion-check" => cfg!(all(target_os = "macos", target_arch = "aarch64")),
            "skills" => observed.version == "0.167.0" && crate::content_io::file_helper::qualified_platform(),
            _ => true,
        }).map(|name| json!({"name":name,"support":if matches!(name, "resources" | "planFirst.auto") {"approximate"} else {"native"},"evidenceJson":evidence})).collect::<Vec<_>>();
    let mut ranges = BTreeSet::new();
    for (name, agent) in document["agents"].as_object().into_iter().flatten() {
        if agent["harness"] != "codex" {
            continue;
        }
        let Some(range) = agent["harnessVersion"].as_str() else {
            continue;
        };
        if !ranges.insert(range) {
            continue;
        }
        let pointer = format!(
            "/agents/{}/harnessVersion",
            name.replace('~', "~0").replace('/', "~1")
        );
        let requirement = version_requirement(range)
            .map_err(|error| failure("HARNESS_VERSION_INVALID", &pointer, &error.to_string()))?;
        let installed = Version::parse(&observed.version)
            .map_err(|error| failure("CODEX_VERSION_UNQUALIFIED", &pointer, &error.to_string()))?;
        if !requirement.matches(&installed) {
            return Err(failure(
                "HARNESS_VERSION",
                &pointer,
                &format!("Codex {installed} does not satisfy {range}."),
            ));
        }
        let receipt = json!({
            "kind": "harness-version-range",
            "harness": "codex",
            "version": observed.version,
            "range": range,
            "satisfied": true,
            "receipt": observed.profile.digest,
        });
        capabilities.push(json!({
            "name":format!("harness-version-range-{}", ranges.len()),
            "support":"native",
            "evidenceJson":receipt.to_string(),
        }));
    }
    let default = observed
        .models
        .iter()
        .find(|model| model["isDefault"] == true)
        .and_then(|model| model["name"].as_str());
    let models = observed.models.iter().map(|model| json!({"name":model["name"],"efforts":model["efforts"],"defaultEffort":model["defaultEffort"]})).collect::<Vec<_>>();
    let agents = document["agents"].as_object().into_iter().flatten().filter(|(_, agent)| agent["harness"] == "codex").map(|(name, agent)| {
        json!({"agentName":name,"agentDeclarationJson":agent.to_string(),"settingsJson":"{}",
            "permissionsJson":json!({"requested":agent["permissions"],"authority":"native-03-at-handoff"}).to_string(),
            "scopeJson":scope.to_string(),"evidenceJson":evidence,"provenanceJson":json!({"sourceSha256":source_sha256,"agent":name}).to_string()})
    }).collect();
    Ok((
        json!({"harness":"codex","version":observed.version,"installation":"available","adapterId":adapter,"qualified":true,"capabilities":capabilities,"models":models,"defaultModel":default}),
        agents,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};

    #[test]
    fn codex_version_requirement_is_checked_against_native_observation() {
        for (range, expected_error) in [
            (None, None),
            (Some(">=0.167.0 <0.168.0"), None),
            (Some(">= 0.167.0  < 0.168.0"), None),
            (Some(">=0.168.0"), Some("HARNESS_VERSION")),
            (Some(">="), Some("HARNESS_VERSION_INVALID")),
        ] {
            let mut document = json!({"contract":"agsdl-0.3.0","id":"audit","agents":{
                "worker":{"harness":"codex","permissions":"workspace"}
            }});
            if let Some(range) = range {
                document["agents"]["worker"]["harnessVersion"] = json!(range);
            }
            let authored = document.to_string();
            let source = json!({"owner":{"projectId":"project","planId":"source","branch":"develop"},
                "revision":"1","sourceSha256":format!("{:x}",Sha256::digest(authored.as_bytes()))});
            let tmp = tempfile::tempdir().unwrap();
            let root = tmp.path().canonicalize().unwrap();
            let scope = json!({"projectId":"project","root":root,
                "rootIdentity":crate::content_io::admitted::workspace_root_identity(&root).unwrap().observation_key().unwrap(),
                "sourceOwner":source["owner"]});
            let observed = crate::agsdl_host_codex::CodexObservation030 {
                profile: crate::agsdl_host_codex::profile::Profile {
                    executable: root.join("bin/codex"),
                    directory: root.join("profile"),
                    home: root.join("profile/home"),
                    bootstrap: root.join("bootstrap"),
                    digest: "synthetic-profile".into(),
                },
                version: "0.167.0".into(),
                models: vec![
                    json!({"name":"fixture","efforts":["medium"],"defaultEffort":"medium","isDefault":true}),
                ],
                account_digest: "synthetic-account".into(),
            };
            let captured = codex_registry(
                &document,
                source["sourceSha256"].as_str().unwrap(),
                &scope,
                &observed.into(),
            );
            if let Some(expected) = expected_error {
                let error = captured.unwrap_err();
                assert_eq!(error.code, expected);
                assert_eq!(error.pointer, "/agents/worker/harnessVersion");
                continue;
            }
            let (harness, agents) = captured.unwrap();
            let registry = json!({"format":"macro-agsdl-registry/030","version":"audit",
                "modules":[{"harness":"codex","moduleJson":crate::api::codex_module()}],
                "harnesses":[harness],"agents":agents,"hostCapabilities":[]});
            let compiled: Value = serde_json::from_str(
                &crate::calculation::compile(&authored, &source.to_string(), &registry.to_string())
                    .unwrap(),
            )
            .unwrap();
            assert_eq!(compiled.pointer("/static/valid"), Some(&json!(true)));
            assert_eq!(compiled.pointer("/runtime/supported"), Some(&json!(true)));
        }
    }
}
