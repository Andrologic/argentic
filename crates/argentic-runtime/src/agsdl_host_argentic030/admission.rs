use crate::{
    agsdl_host_qualification030::declared_steps030,
    agsdl_host_work_admission030::pointer_token030,
    permissions::{
        Action, DirectoryAccess, Effect, EffectivePolicy, Outside, Preset, RequestedPermissions,
        Reviewer,
    },
    persistence::{failure, Result},
};
use argentic_harness::policy::{
    Action as NativeAction, Effect as NativeEffect, Policy, Preset as NativePreset, Rule, Zone,
};
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
    if let RequestedPermissions::Object(permissions) = &permissions {
        if !permissions.directories.is_empty() {
            return Err(unsupported(
                &format!("{at}/directories"),
                "additional workspace roots",
            ));
        }
        if permissions.reviewer == Some(Reviewer::Model) {
            return Err(unsupported(
                &format!("{at}/reviewer"),
                "model permission reviewers",
            ));
        }
        for (index, rule) in permissions.rules.iter().enumerate() {
            if matches!(rule.action, Action::Mcp | Action::Delegate) && rule.effect != Effect::Deny
            {
                return Err(unsupported(
                    &format!("{at}/rules/{index}/effect"),
                    "MCP or delegation permissions",
                ));
            }
        }
    }
    if shell_requested(&permissions) {
        argentic_harness::tools::shell_preflight()
            .map_err(|diagnostic| failure("HARNESS_CAPABILITY", &at, &diagnostic))?;
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

pub(super) fn shell_requested(permissions: &RequestedPermissions) -> bool {
    match permissions {
        RequestedPermissions::Preset(preset) => *preset != Preset::ReadOnly,
        RequestedPermissions::Object(permissions) => {
            let mut allowed = matches!(permissions.preset, Some(Preset::Workspace | Preset::Full));
            for rule in permissions
                .rules
                .iter()
                .filter(|rule| rule.action == Action::Shell)
            {
                let global = rule.pattern.as_ref().is_none_or(|pattern| {
                    !pattern.is_empty() && pattern.bytes().all(|byte| byte == b'*')
                });
                if global
                    && (rule.effect == Effect::Deny
                        || rule.effect == Effect::Ask
                            && permissions.reviewer != Some(Reviewer::Person))
                {
                    return false;
                }
                allowed |= rule.effect == Effect::Allow
                    || rule.effect == Effect::Ask && permissions.reviewer == Some(Reviewer::Person);
            }
            allowed
        }
    }
}

pub(super) fn compile_policy(policy: &EffectivePolicy) -> Result<Vec<Policy>> {
    policy
        .policies()
        .map(|layer| {
            let paths = layer.context();
            let rules = layer
                .rules()
                .iter()
                .flat_map(|rule| {
                    let effect = match rule.effect {
                        Effect::Allow => NativeEffect::Allow,
                        Effect::Ask => NativeEffect::Ask,
                        Effect::Deny => NativeEffect::Deny,
                    };
                    let action = match rule.action {
                        Action::Read => NativeAction::Read,
                        Action::Write => NativeAction::Write,
                        Action::Shell => NativeAction::Shell,
                        Action::Network => NativeAction::Network,
                        Action::Mcp => NativeAction::Mcp,
                        Action::Delegate => NativeAction::Delegate,
                    };
                    let patterns = if matches!(rule.action, Action::Read | Action::Write) {
                        let logical = rule.logical_pattern();
                        let physical = rule.physical_pattern();
                        // Tools reject aliases, so an alias cannot grant access.
                        // Restrictive rules still cover both captured names.
                        if logical == physical {
                            vec![logical]
                        } else if effect == NativeEffect::Allow {
                            vec![]
                        } else {
                            vec![logical, physical]
                        }
                    } else {
                        vec![rule.pattern.as_deref()]
                    };
                    patterns.into_iter().map(move |pattern| Rule {
                        action,
                        pattern: pattern.map(str::to_owned),
                        effect,
                    })
                })
                .collect::<Vec<_>>();
            let preset = layer.preset().map(|preset| match preset {
                Preset::ReadOnly => NativePreset::ReadOnly,
                Preset::Workspace => NativePreset::Workspace,
                Preset::Full => NativePreset::Full,
            });
            Policy::compile_rules(
                &paths.workspace,
                Some(&paths.home),
                preset,
                Some(layer.outside() == Outside::Read),
                &rules,
            )
            .map(|policy| {
                policy.with_directories(
                    layer
                        .directories()
                        .iter()
                        .map(|directory| {
                            let zone = match directory.access {
                                DirectoryAccess::Read => Zone::ReadDirectory,
                                DirectoryAccess::Write => Zone::WriteDirectory,
                            };
                            (directory.canonical.clone(), zone)
                        })
                        .collect(),
                )
            })
            .map_err(|error| failure("PERMISSION", "/permissions", &error))
        })
        .collect()
}

pub(super) fn agent_pointer(name: &str) -> String {
    format!("/agents/{}", pointer_token030(name))
}
