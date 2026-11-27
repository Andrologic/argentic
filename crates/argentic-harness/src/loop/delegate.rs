use super::{Harness, Hook, Instance, Work};
use crate::{
    providers::{Message, ToolCall, ToolDefinition},
    tools::Page,
};
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

pub(super) const TOOL: &str = "delegate";
const MAX_CHILD_TURNS: u32 = 8;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Arguments {
    task: String,
}

pub(super) fn definition() -> ToolDefinition {
    ToolDefinition {
        name: TOOL.into(),
        description: "Assign a read-only investigation to a fresh sub-agent. Returns its final reply when it ends. Supply the relevant context in the task.".into(),
        parameters: json!({
            "type":"object","required":["task"],"additionalProperties":false,
            "properties":{"task":{"type":"string"}}
        }),
    }
}

impl Instance<'_> {
    pub(super) async fn delegate(&self, work: &Work, call: &ToolCall) -> Result<Page, String> {
        if !self.harness.delegation {
            return Err("Delegation permission denied".into());
        }
        let args: Arguments =
            serde_json::from_str(&call.arguments).map_err(|_| "Invalid delegation arguments")?;
        if args.task.trim().is_empty() || args.task.len() > 8192 {
            return Err("Supply a task between 1 and 8192 bytes".into());
        }
        if !self.budget().map_err(|error| error.to_string())?.has_turn() {
            return Err("No parent budget remains for delegation".into());
        }
        self.harness
            .registry
            .authorize_delegate(call, self.cancel)
            .await?;
        let registry = self.harness.registry.read_only_child()?;
        // Child tools still consult the same host ceiling. Request hooks and
        // the parent's port would mix the child and parent conversations.
        let harness = Harness::new(
            self.harness.provider.clone(),
            Arc::new(registry),
            self.harness.settings.clone(),
        )
        .with_context_window(self.harness.context_window)
        .with_hook(Arc::new(ChildToolHooks(self.harness.hooks.clone())));
        let child = Work {
            model: work.model.clone(),
            reasoning_effort: work.reasoning_effort.clone(),
            messages: vec![
                Message::System("Investigate the task using the available read tools. Reply to the parent with your findings. You have a fresh conversation and cannot change files or call external tools.".into()),
                Message::User(args.task),
            ],
            submission_required: false,
            max_corrections: 0,
        };
        let cancel = self.cancel.child_token();
        let budget = self.budget.clone();
        // Track the child future itself. A forced drop of the parent's turn
        // still waits for the child's provider and its blocking read workers.
        let result = self
            .tracker
            .spawn(async move {
                let (_send, inbox) = tokio::sync::mpsc::channel(1);
                harness
                    .run_with_budget(child, &(), inbox, cancel, budget, MAX_CHILD_TURNS)
                    .await
            })
            .await
            .map_err(|_| "Sub-agent task failed")?;
        result.outcome.map_err(|error| match error {
            super::Failure::TokenLimit
            | super::Failure::MissingUsage
            | super::Failure::TurnLimit => "Sub-agent exhausted its budget",
            super::Failure::Cancelled => "Sub-agent cancelled",
            _ => "Sub-agent failed",
        })?;
        let text = result
            .messages
            .iter()
            .rev()
            .find_map(|message| match message {
                Message::Assistant { text, calls, .. } if calls.is_empty() => Some(text.as_str()),
                _ => None,
            })
            .unwrap_or_default();
        Page::one_shot(text, &call.id).map_err(|error| error.to_string())
    }
}

struct ChildToolHooks(Vec<Arc<dyn Hook>>);

#[async_trait::async_trait]
impl Hook for ChildToolHooks {
    async fn before_tool_call(
        &self,
        turn: u32,
        call: &ToolCall,
        cancel: &CancellationToken,
    ) -> Result<(), super::Failure> {
        for hook in &self.0 {
            hook.before_tool_call(turn, call, cancel).await?;
        }
        Ok(())
    }
}
