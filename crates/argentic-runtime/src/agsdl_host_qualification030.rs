//! Existing native runtime capability refusals, independent of source capture.
use argentic_core::durable_json as exact_json;
use serde_json::Value;
pub(crate) struct PreflightDiagnostic {
    pub code: String,
    pub pointer: String,
    pub message: String,
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

pub(crate) fn product_slice030(
    authored: &str,
    compiled: Option<&str>,
) -> Option<PreflightDiagnostic> {
    let document = exact_json::value(authored).ok()?;
    if !document
        .get("contract")
        .and_then(Value::as_str)
        .is_some_and(argentic_core::supports_contract)
    {
        return None;
    }
    let issue = |pointer: String, message: &str| {
        Some(PreflightDiagnostic {
            code: "UNSUPPORTED_030_FEATURE".into(),
            pointer,
            message: message.into(),
        })
    };
    for (name, server) in document
        .get("mcpServers")
        .and_then(Value::as_object)
        .into_iter()
        .flatten()
    {
        if let Some(url) = server["url"].as_str() {
            if let Err(error) = crate::agsdl_host_mcp_http::endpoint(url) {
                let name = name.replace('~', "~0").replace('/', "~1");
                return Some(PreflightDiagnostic {
                    code: error.code,
                    pointer: format!("/mcpServers/{name}/url"),
                    message: error.detail,
                });
            }
        }
    }
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
    for (pointer, step) in declared_steps030(&document) {
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
