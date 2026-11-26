use crate::{
    agsdl_host_qualification030::declared_steps030,
    agsdl_host_work_admission030::pointer_token030,
    permissions::{Action, Effect, EffectivePolicy, Outside, Preset, RequestedPermissions},
    persistence::{failure, Result},
};
use argentic_harness::policy::{Effect as ReadEffect, Preset as ReadPreset, ReadPolicy, ReadRule};
use serde_json::Value;

pub(super) fn unsupported(pointer: &str, feature: &str) -> crate::persistence::dto::EngineFailure {
    failure(
        "HARNESS_CAPABILITY",
        pointer,
        &format!("Argentic does not support {feature}"),
    )
}

pub(super) fn qualify(agent: &Value, pointer: &str) -> Result<()> {
    for property in [
        "resources",
        "skills",
        "mcpServers",
        "plan",
        "planFirst",
        "completionCheck",
        "answerTimeoutMs",
    ] {
        if agent.get(property).is_some_and(|value| {
            value != false
                && !value.as_array().is_some_and(Vec::is_empty)
                && !value.as_object().is_some_and(serde_json::Map::is_empty)
        }) {
            return Err(unsupported(&format!("{pointer}/{property}"), property));
        }
    }
    let at = format!("{pointer}/permissions");
    let permissions: RequestedPermissions = serde_json::from_value(agent["permissions"].clone())
        .map_err(|_| unsupported(&at, "these permissions"))?;
    match permissions {
        RequestedPermissions::Preset(Preset::ReadOnly) => {}
        RequestedPermissions::Preset(_) => {
            return Err(unsupported(&at, "presets that grant non-read permissions"))
        }
        RequestedPermissions::Object(permissions) => {
            if permissions
                .preset
                .is_some_and(|preset| preset != Preset::ReadOnly)
            {
                return Err(unsupported(
                    &format!("{at}/preset"),
                    "presets that grant non-read permissions",
                ));
            }
            if !permissions.directories.is_empty() {
                return Err(unsupported(
                    &format!("{at}/directories"),
                    "additional read roots",
                ));
            }
            if permissions.outside == Some(Outside::Read) {
                return Err(unsupported(
                    &format!("{at}/outside"),
                    "reads outside the workspace",
                ));
            }
            if permissions.reviewer.is_some() {
                return Err(unsupported(
                    &format!("{at}/reviewer"),
                    "permission reviewers",
                ));
            }
            for (index, rule) in permissions.rules.iter().enumerate() {
                let at = format!("{at}/rules/{index}");
                if rule.action != Action::Read && rule.effect != Effect::Deny {
                    return Err(unsupported(
                        &format!("{at}/effect"),
                        "non-deny rules for actions other than read",
                    ));
                }
                if rule.action == Action::Read && rule.effect == Effect::Ask {
                    return Err(unsupported(&format!("{at}/effect"), "read approval"));
                }
            }
        }
    }
    Ok(())
}

pub(super) fn validate_document(document: &Value, location: Option<&url::Url>) -> Result<()> {
    if document["contract"] != argentic_core::AGSDL_040 {
        return Err(unsupported("/contract", "contracts other than AgSDL 0.4.0"));
    }
    for (pointer, step) in declared_steps030(document) {
        if step["agent"]
            .as_str()
            .is_some_and(|agent| document["agents"][agent]["harness"] == "argentic")
            && step["resources"]
                .as_object()
                .is_some_and(|resources| !resources.is_empty())
        {
            return Err(unsupported(&format!("{pointer}/resources"), "resources"));
        }
    }
    crate::agsdl_host_content030::validate_text_sources(document, location)
}

pub(super) fn compile_reads(policy: &EffectivePolicy) -> Result<Vec<ReadPolicy>> {
    policy
        .policies()
        .map(|layer| {
            let paths = layer.context();
            let rules = layer
                .rules()
                .iter()
                .filter(|rule| rule.action == Action::Read)
                .flat_map(|rule| {
                    let effect = match rule.effect {
                        Effect::Allow => ReadEffect::Allow,
                        Effect::Ask => ReadEffect::Deny,
                        Effect::Deny => ReadEffect::Deny,
                    };
                    let logical = rule.logical_pattern();
                    let physical = rule.physical_pattern();
                    // The registry rejects aliases. Allows on an alias therefore
                    // cannot grant access, while restrictive rules cover both names.
                    let patterns = if effect == ReadEffect::Allow && logical != physical {
                        vec![]
                    } else if logical == physical {
                        vec![logical]
                    } else {
                        vec![logical, physical]
                    };
                    patterns.into_iter().map(move |pattern| ReadRule {
                        pattern: pattern.map(|pattern| {
                            if cfg!(windows) {
                                pattern.replace('\\', "/")
                            } else {
                                pattern.to_owned()
                            }
                        }),
                        effect,
                    })
                })
                .collect::<Vec<_>>();
            let preset = layer.preset().map(|_| ReadPreset::ReadOnly);
            ReadPolicy::compile(
                &paths.workspace,
                Some(&paths.home),
                preset,
                Some(false),
                &rules,
            )
            .map_err(|_| {
                failure(
                    "PERMISSION",
                    "/permissions",
                    "Argentic could not compile a read ceiling",
                )
            })
        })
        .collect()
}

pub(super) fn agent_pointer(name: &str) -> String {
    format!("/agents/{}", pointer_token030(name))
}
