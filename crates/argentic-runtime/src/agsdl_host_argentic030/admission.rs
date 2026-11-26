use crate::{
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
use argentic_harness::tools::{shell_network_allowed, shell_read_conflict, ShellReadConflict};
use serde_json::Value;
use std::path::{Path, PathBuf};

pub(super) fn unsupported(pointer: &str, feature: &str) -> crate::persistence::dto::EngineFailure {
    failure(
        "HARNESS_CAPABILITY",
        pointer,
        &format!("Argentic does not support {feature}"),
    )
}

pub(super) fn qualify(agent: &Value, pointer: &str) -> Result<()> {
    for property in ["plan", "planFirst", "completionCheck"] {
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
        if permissions.directories.len() > 8 {
            return Err(failure(
                "HARNESS_CAPABILITY",
                &format!("{at}/directories"),
                "Argentic supports at most eight additional roots per agent",
            ));
        }
        if permissions.reviewer == Some(Reviewer::Model) {
            return Err(unsupported(
                &format!("{at}/reviewer"),
                "model permission reviewers",
            ));
        }
        for (index, rule) in permissions.rules.iter().enumerate() {
            if rule.action == Action::Delegate && rule.effect != Effect::Deny {
                return Err(unsupported(
                    &format!("{at}/rules/{index}/effect"),
                    "delegation permissions",
                ));
            }
        }
    }
    if shell_requested(&permissions) {
        #[cfg(any(target_os = "linux", all(target_os = "macos", target_arch = "aarch64")))]
        if let RequestedPermissions::Object(object) = &permissions {
            if object.outside == Some(Outside::Deny) {
                return Err(read_conflict(
                    &format!("{at}/outside"),
                    ShellReadConflict::Outside,
                ));
            }
            for (index, rule) in object.rules.iter().enumerate() {
                if rule.action != Action::Read
                    || rule.effect == Effect::Allow
                    || rule
                        .pattern
                        .as_deref()
                        .is_some_and(|pattern| !pattern.starts_with('/'))
                {
                    continue;
                }
                let policy = Policy::compile_rules(
                    std::path::Path::new("/"),
                    None,
                    None,
                    None,
                    &[Rule {
                        action: NativeAction::Read,
                        effect: NativeEffect::Deny,
                        pattern: rule.pattern.clone(),
                    }],
                )
                .map_err(|error| failure("PERMISSION", &format!("{at}/rules/{index}"), &error))?;
                if let Some(conflict) = shell_read_conflict(&policy, false) {
                    let field = if rule.pattern.is_some() {
                        "match"
                    } else {
                        "effect"
                    };
                    return Err(read_conflict(
                        &format!("{at}/rules/{index}/{field}"),
                        conflict,
                    ));
                }
            }
        }
        argentic_harness::tools::shell_preflight()
            .map_err(|diagnostic| failure("HARNESS_CAPABILITY", &at, &diagnostic))?;
    }
    Ok(())
}

pub(super) fn validate_document(document: &Value, location: Option<&url::Url>) -> Result<()> {
    if document["contract"] != argentic_core::AGSDL_040 {
        return Err(unsupported("/contract", "contracts other than AgSDL 0.4.0"));
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
                if global && rule.effect == Effect::Deny {
                    return false;
                }
                allowed |= rule.effect != Effect::Deny;
            }
            allowed
        }
    }
}

fn read_conflict(
    pointer: &str,
    conflict: ShellReadConflict,
) -> crate::persistence::dto::EngineFailure {
    failure("HARNESS_UNSUPPORTED", pointer, &conflict.to_string())
}

pub(super) fn admit_shell(policies: &[Policy], person: bool, pointer: &str) -> Result<bool> {
    if !policies.iter().all(|policy| policy.may_run_shell(person)) {
        return Ok(false);
    }
    let network = shell_network_allowed(policies);
    for policy in policies {
        if let Some(conflict) = shell_read_conflict(policy, network) {
            let at = match conflict {
                ShellReadConflict::Outside => format!("{pointer}/permissions/outside"),
                ShellReadConflict::Rule { .. } => format!("{pointer}/permissions/rules"),
            };
            return Err(read_conflict(&at, conflict));
        }
    }
    Ok(true)
}

pub(super) fn extra_roots(
    policy: &EffectivePolicy,
    private: &Path,
    shell: bool,
    pointer: &str,
) -> Result<Vec<PathBuf>> {
    let requested = policy
        .policies()
        .next()
        .ok_or_else(|| unsupported(pointer, "missing agent policy"))?;
    let workspace = &requested.context().workspace;
    let at = format!("{pointer}/permissions/directories");
    let mut roots = Vec::<PathBuf>::new();
    let overlaps = |left: &Path, right: &Path| left.starts_with(right) || right.starts_with(left);
    for directory in requested.directories() {
        let path = &directory.canonical;
        if overlaps(private, path)
            || path
                .ancestors()
                .last()
                .is_some_and(|base| argentic_harness::tools::private_project_path(base, path))
        {
            return Err(failure(
                "PERMISSION",
                &at,
                "An additional root overlaps private host data",
            ));
        }
        if path == workspace || roots.contains(path) {
            return Err(failure(
                "HARNESS_CAPABILITY",
                &at,
                "Additional roots must have distinct canonical paths",
            ));
        }
        if shell && (overlaps(workspace, path) || roots.iter().any(|root| overlaps(root, path))) {
            return Err(failure(
                "HARNESS_CAPABILITY",
                &at,
                "Overlapping roots cannot be staged independently for shell access",
            ));
        }
        roots.push(path.clone());
    }
    Ok(roots)
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
                if layer.explicitly_denies_outside_reads() {
                    Some(false)
                } else if layer.outside() == Outside::Read {
                    Some(true)
                } else {
                    None
                },
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
