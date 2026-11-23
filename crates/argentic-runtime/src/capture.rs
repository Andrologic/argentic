//! Observed Codex registry preparation shared by native hosts.
use crate::persistence::{failure, Result};
use semver::{Version, VersionReq};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    sync::OnceLock,
};

/// A host-selected model and optional effort for one agent in a run.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ModelOverride {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effort: Option<String>,
}

/// Add a run's model choices to its native observations before versioning
/// the registry and compiling. Keep all authored agent declarations intact.
pub fn apply_model_overrides(
    registry: &mut Value,
    overrides: &BTreeMap<String, ModelOverride>,
) -> Result<()> {
    if overrides.is_empty() {
        return Ok(());
    }
    let agents = registry["agents"]
        .as_array_mut()
        .ok_or_else(model_correlation)?;
    let mut entries = Vec::with_capacity(overrides.len());
    let mut seen = BTreeSet::new();
    for (index, agent) in agents.iter().enumerate() {
        let Some(name) = agent["agentName"].as_str() else {
            continue;
        };
        let Some(model) = overrides.get(name) else {
            continue;
        };
        if !seen.insert(name) || agent.get("modelOverride").is_some() {
            return Err(model_correlation());
        }
        entries.push((index, serde_json::to_value(model)?));
    }
    if entries.len() != overrides.len() {
        return Err(failure(
            "MODEL_OVERRIDE",
            "modelOverrides",
            "No native observation exists for the agent",
        ));
    }
    for (index, model) in entries {
        agents[index]["modelOverride"] = model;
    }
    Ok(())
}

fn model_correlation() -> crate::persistence::dto::EngineFailure {
    failure(
        "CORRELATION",
        "modelOverrides",
        "Retained model selection differs from the native registry",
    )
}

/// Match the frozen choice to the native request and recorded catalogue.
/// Old captures omit the optional field on both sides.
pub(crate) fn verify_model_overrides(
    plan: &crate::persistence::flow030::FlowPlan030,
    registry: &Value,
) -> Result<BTreeMap<String, ModelOverride>> {
    let observed: BTreeMap<_, _> = registry["agents"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|agent| Some((agent["agentName"].as_str()?, agent.get("modelOverride")?)))
        .collect();
    let mut effective = BTreeMap::new();
    for agent in &plan.agents {
        let name = agent["agentName"].as_str().unwrap_or_default();
        let replacement = agent.get("modelOverride");
        if replacement != observed.get(name).copied() {
            return Err(model_correlation());
        }
        let Some(replacement) = replacement else {
            continue;
        };
        let requested: ModelOverride =
            serde_json::from_value(replacement.clone()).map_err(|_| model_correlation())?;
        let harness = registry["harnesses"]
            .as_array()
            .into_iter()
            .flatten()
            .find(|candidate| {
                candidate["harness"] == agent["harness"]
                    && candidate["version"] == agent["harnessVersion"]
                    && candidate["adapterId"] == agent["adapterId"]
            })
            .ok_or_else(model_correlation)?;
        let model = harness["models"]
            .as_array()
            .into_iter()
            .flatten()
            .find(|model| model["name"] == requested.name)
            .ok_or_else(model_correlation)?;
        let effort = requested
            .effort
            .as_deref()
            .or_else(|| model["defaultEffort"].as_str());
        if agent["modelName"] != requested.name
            || agent["modelEffort"].as_str() != effort
            || (effort.is_none() && !agent["modelEffort"].is_null())
            || effort.is_some_and(|effort| {
                !model["efforts"]
                    .as_array()
                    .is_some_and(|levels| levels.iter().any(|level| level == effort))
            })
            || effective
                .insert(
                    name.to_owned(),
                    ModelOverride {
                        name: requested.name,
                        effort: effort.map(str::to_owned),
                    },
                )
                .is_some()
        {
            return Err(model_correlation());
        }
    }
    if effective.len() != observed.len() {
        return Err(model_correlation());
    }
    Ok(effective)
}

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

/// The retained project root in the same form `canonicalize` gives a skill
/// path, including Windows' verbatim prefix. A missing snapshot is refused.
fn captured_project_root(document_location: Option<&url::Url>) -> Result<std::path::PathBuf> {
    document_location
        .and_then(|uri| uri.to_file_path().ok())
        .and_then(|path| path.parent().map(Path::to_owned))
        .and_then(|root| root.canonicalize().ok())
        .ok_or_else(|| {
            failure(
                "CONTENT_UNAVAILABLE",
                "projectSnapshot",
                "The captured project snapshot is missing or has changed",
            )
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
        let workspace = scope["root"]
            .as_str()
            .ok_or_else(|| failure("CORRELATION", "skills", "Native project scope is absent"))?;
        let snapshot_root = if scope.get("projectSnapshot").is_some() {
            Some(captured_project_root(document_location.as_ref())?)
        } else {
            None
        };
        crate::agsdl_host_codex030::content::qualify_skill_folders(
            document,
            snapshot_root
                .as_deref()
                .unwrap_or_else(|| Path::new(workspace)),
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

    /// The comparison in `qualify_skill_folders` uses canonical paths. A
    /// document URL that differs only in spelling still selects its skill.
    #[cfg(unix)]
    #[test]
    fn captured_project_root_matches_canonical_skill_paths() {
        let temp = tempfile::tempdir().unwrap();
        let real = temp.path().canonicalize().unwrap().join("project");
        std::fs::create_dir_all(real.join("skills/review")).unwrap();
        std::fs::write(real.join("skills/review/SKILL.md"), "Review.\n").unwrap();
        let alias = temp.path().join("alias");
        std::os::unix::fs::symlink(&real, &alias).unwrap();
        let location = url::Url::from_file_path(alias.join("agsdl.json")).unwrap();
        let root = captured_project_root(Some(&location)).unwrap();
        assert_eq!(root, real);
        let document = json!({"agents":{"worker":{"skills":["review"]}},
            "skills":{"review":{"uri":"skills/review/SKILL.md"}}});
        crate::agsdl_host_codex030::content::qualify_skill_folders(
            &document,
            &root,
            Some(&location),
        )
        .unwrap();
        std::fs::remove_dir_all(&real).unwrap();
        assert_eq!(
            captured_project_root(Some(&location)).unwrap_err().code,
            "CONTENT_UNAVAILABLE"
        );
    }

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
