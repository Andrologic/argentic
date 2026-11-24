//! Existing native runtime capability refusals, independent of source capture.
use crate::{
    permissions::{requested_may_equip_mcp, RequestedPermissions},
    persistence::{failure, Result},
};
use argentic_core::durable_json as exact_json;
use serde::Serialize;
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize)]
pub struct Diagnostic {
    pub code: String,
    pub pointer: String,
    pub message: String,
}

#[derive(Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct McpServerQualification {
    /// Authored JSON pointers to call steps, including composition definitions.
    pub direct_call_steps: Vec<String>,
    /// Names of Codex agents eligible to discover this server.
    pub agent_equipment: Vec<String>,
    /// Unsupported HTTP endpoint, if any.
    pub diagnostic: Option<Diagnostic>,
}

impl McpServerQualification {
    pub fn is_used(&self) -> bool {
        !self.direct_call_steps.is_empty() || !self.agent_equipment.is_empty()
    }

    pub fn blocking_diagnostic(&self) -> Option<&Diagnostic> {
        self.diagnostic.as_ref().filter(|_| self.is_used())
    }
}

#[derive(Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProductQualification {
    /// The first blocking product capability issue outside MCP.
    pub product_diagnostic: Option<Diagnostic>,
    pub mcp_servers: BTreeMap<String, McpServerQualification>,
}

impl ProductQualification {
    pub fn blocking_diagnostic(&self) -> Option<&Diagnostic> {
        self.mcp_servers
            .values()
            .find_map(McpServerQualification::blocking_diagnostic)
            .or(self.product_diagnostic.as_ref())
    }
}

fn supported_contract(document: &Value) -> bool {
    document["contract"]
        .as_str()
        .is_some_and(argentic_core::supports_contract)
}

pub(crate) fn declared_steps030(document: &Value) -> impl Iterator<Item = (String, &Value)> {
    let flow = document
        .pointer("/flow/steps")
        .and_then(Value::as_object)
        .into_iter()
        .flatten()
        .map(|(name, step)| {
            (
                format!("/flow/steps/{}", name.replace('~', "~0").replace('/', "~1")),
                step,
            )
        });
    let bodies = document
        .get("compositions")
        .and_then(Value::as_object)
        .into_iter()
        .flatten()
        .flat_map(|(composition, body)| {
            body.get("steps")
                .and_then(Value::as_object)
                .into_iter()
                .flatten()
                .map(move |(name, step)| {
                    (
                        format!(
                            "/compositions/{}/steps/{}",
                            composition.replace('~', "~0").replace('/', "~1"),
                            name.replace('~', "~0").replace('/', "~1")
                        ),
                        step,
                    )
                })
        });
    flow.chain(bodies)
}

pub(crate) fn mcp_endpoint_diagnostic(name: &str, server: &Value) -> Option<Diagnostic> {
    let error = crate::agsdl_host_mcp_http::endpoint(server["url"].as_str()?).err()?;
    let name = name.replace('~', "~0").replace('/', "~1");
    Some(Diagnostic {
        code: error.code,
        pointer: format!("/mcpServers/{name}/url"),
        message: error.detail,
    })
}

pub(crate) fn qualify_product030(
    document: &Value,
    compiled: Option<&str>,
    host_permissions: Option<&RequestedPermissions>,
) -> Result<ProductQualification> {
    if !supported_contract(document) {
        return Ok(ProductQualification::default());
    }
    let mut mcp_servers: BTreeMap<_, _> = document["mcpServers"]
        .as_object()
        .into_iter()
        .flatten()
        .map(|(name, declaration)| {
            (
                name.clone(),
                McpServerQualification {
                    diagnostic: mcp_endpoint_diagnostic(name, declaration),
                    ..Default::default()
                },
            )
        })
        .collect();
    for (pointer, step) in declared_steps030(document) {
        if step["type"] == "call" {
            if let Some(report) = step["mcp"]["server"]
                .as_str()
                .and_then(|name| mcp_servers.get_mut(name))
            {
                report.direct_call_steps.push(pointer);
            }
        }
    }
    for (agent_name, agent) in document["agents"].as_object().into_iter().flatten() {
        let Some(servers) = agent["mcpServers"].as_array() else {
            continue;
        };
        if agent["harness"] != "codex" || servers.is_empty() {
            continue;
        }
        let pointer = format!(
            "/agents/{}/permissions",
            agent_name.replace('~', "~0").replace('/', "~1")
        );
        let invalid = || {
            failure(
                "HARNESS_CAPABILITY",
                &pointer,
                "Codex MCP permissions are invalid",
            )
        };
        let requested = match serde_json::from_value(agent["permissions"].clone()) {
            Ok(requested) => requested,
            // The product diagnostic below reports unsupported agent permissions.
            Err(_) => continue,
        };
        for name in servers.iter().filter_map(Value::as_str) {
            let Some(report) = mcp_servers.get_mut(name) else {
                continue;
            };
            if !requested_may_equip_mcp(&requested, name).map_err(|_| invalid())? {
                continue;
            }
            if let Some(host) = host_permissions {
                if !requested_may_equip_mcp(host, name).map_err(|_| invalid())? {
                    continue;
                }
            }
            report.agent_equipment.push(agent_name.clone());
        }
    }
    Ok(ProductQualification {
        product_diagnostic: product_diagnostic030(document, compiled),
        mcp_servers,
    })
}

pub(crate) fn product_slice030(authored: &str, compiled: Option<&str>) -> Option<Diagnostic> {
    let document = exact_json::value(authored).ok()?;
    match qualify_product030(&document, compiled, None) {
        Ok(report) => report.blocking_diagnostic().cloned(),
        Err(error) => Some(Diagnostic {
            code: error.code,
            pointer: error.pointer,
            message: error.message,
        }),
    }
}

pub(crate) fn product_diagnostic030(
    document: &Value,
    compiled: Option<&str>,
) -> Option<Diagnostic> {
    if !supported_contract(document) {
        return None;
    }
    let issue = |pointer: String, message: &str| {
        Some(Diagnostic {
            code: "UNSUPPORTED_030_FEATURE".into(),
            pointer,
            message: message.into(),
        })
    };
    for (name, agent) in document.get("agents")?.as_object()? {
        let pointer = format!("/agents/{}", name.replace('~', "~0").replace('/', "~1"));
        if agent.get("harness").and_then(Value::as_str) != Some("codex") {
            return issue(
                format!("{pointer}/harness"),
                "Only the Codex harness is supported for this run.",
            );
        }
        if let Err(refusal) = crate::agsdl_host_work_admission030::codex_permissions030(
            &agent["permissions"],
            &format!("{pointer}/permissions"),
        ) {
            return issue(refusal.pointer, &refusal.message);
        }
        if agent.get("planFirst").and_then(Value::as_str) == Some("ask") {
            return issue(
                format!("{pointer}/planFirst"),
                "agsdl.execution.codexPlanAsk030",
            );
        }
    }
    for (pointer, step) in declared_steps030(document) {
        if !matches!(
            step.get("type").and_then(Value::as_str),
            Some("agent" | "call" | "approval" | "join" | "compose")
        ) {
            return issue(pointer, "agsdl.execution.codexStepType030");
        }
        if let Some(limit) = step.get("maxVisits") {
            if exact_json::positive_integer(&limit.to_string()).is_none() {
                return issue(
                    format!("{pointer}/maxVisits"),
                    "agsdl.execution.codexVisitBounds030",
                );
            }
        }
    }
    let plan = exact_json::value(compiled?).ok()?;
    for step in plan.get("steps")?.as_array()? {
        let pointer = format!(
            "/flow/steps/{}",
            step.get("address")?
                .as_array()?
                .last()?
                .as_str()?
                .replace('~', "~0")
                .replace('/', "~1")
        );
        if !matches!(
            step.get("type").and_then(Value::as_str),
            Some("agent" | "call" | "approval" | "join" | "compose")
        ) {
            return issue(
                pointer,
                "Only agent, call, approval and join steps are supported for this run.",
            );
        }
    }
    None
}
