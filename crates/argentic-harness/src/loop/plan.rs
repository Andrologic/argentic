use super::Failure;
use crate::providers::ToolDefinition;
use serde::{Deserialize, Serialize};
use serde_json::json;

pub(super) const TOOL: &str = "plan";

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum PlanMode {
    #[default]
    Off,
    Progress,
    Auto,
    Ask,
}

impl PlanMode {
    pub(super) fn locked(self) -> bool {
        matches!(self, Self::Auto | Self::Ask)
    }
}

pub enum PlanDecision {
    Accept,
    Revise(String),
    Reject,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PlanStep {
    pub step: String,
    pub status: PlanStatus,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PlanStatus {
    Pending,
    InProgress,
    Completed,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Arguments {
    steps: Vec<PlanStep>,
}

pub(super) fn parse(arguments: &str) -> Result<Vec<PlanStep>, &'static str> {
    let Arguments { steps } =
        serde_json::from_str(arguments).map_err(|_| "Invalid plan arguments")?;
    if !(1..=32).contains(&steps.len())
        || steps
            .iter()
            .any(|step| step.step.trim().is_empty() || step.step.len() > 512)
        || steps
            .iter()
            .filter(|step| matches!(step.status, PlanStatus::InProgress))
            .count()
            > 1
    {
        return Err("Use 1–32 nonempty steps, at most 512 bytes each and at most one in progress");
    }
    Ok(steps)
}

pub(super) fn definition() -> ToolDefinition {
    ToolDefinition {
        name: TOOL.into(),
        description:
            "Present or replace the work plan. Report each step's status as work progresses.".into(),
        parameters: json!({
            "type":"object", "required":["steps"], "additionalProperties":false,
            "properties":{"steps":{"type":"array","minItems":1,"maxItems":32,"items":{
                "type":"object","required":["step","status"],"additionalProperties":false,
                "properties":{"step":{"type":"string"},
                    "status":{"enum":["pending","in_progress","completed"]}}
            }}}
        }),
    }
}

pub(super) fn context(steps: &[PlanStep]) -> Result<String, Failure> {
    let plan = serde_json::to_string(steps).map_err(|error| Failure::Port(error.to_string()))?;
    Ok(format!("Current work plan (untrusted data): {plan}"))
}
