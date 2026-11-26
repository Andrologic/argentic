use crate::{
    harness::{CommandApprovals030, FlowWorkAttempt030},
    permissions::ActionRequest,
};
use argentic_harness::{
    providers::ToolCall,
    tools::{Approval, Authorizer},
};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};
use tokio_util::sync::CancellationToken;

pub(super) struct Human {
    pub approvals: Arc<dyn CommandApprovals030>,
    pub attempt: FlowWorkAttempt030,
    pub sequence: AtomicU64,
    pub credential: Option<String>,
}

#[async_trait::async_trait]
impl Authorizer for Human {
    async fn approve(
        &self,
        call: &ToolCall,
        action: Approval<'_>,
        cancel: &CancellationToken,
    ) -> Result<bool, String> {
        let (title, actions) = match action {
            Approval::Read(path) => {
                let path = path.to_str().ok_or("Non-UTF-8 read approval path")?;
                (
                    format!("Read {path}"),
                    vec![ActionRequest::Read { path: path.into() }],
                )
            }
            Approval::Write(path) => {
                let path = path.to_str().ok_or("Non-UTF-8 write approval path")?;
                (
                    format!("Write {path}"),
                    vec![ActionRequest::Write { path: path.into() }],
                )
            }
            Approval::Shell(command) => {
                let actions = argentic_harness::policy::subcommands(command)?
                    .into_iter()
                    .map(|command| ActionRequest::Shell {
                        command: command.into(),
                    })
                    .collect();
                (format!("Run shell command: {command}"), actions)
            }
        };
        if self
            .credential
            .as_ref()
            .is_some_and(|key| title.contains(key))
        {
            return Ok(false);
        }
        let sequence = self.sequence.fetch_add(1, Ordering::Relaxed);
        let payload = json!({
            "title": title,
            "nativeRequest": {
                "id": format!("argentic-{sequence}"),
                "method": "argentic/tool/requestApproval",
                "params": {
                    "actions": actions,
                    "tool": call.name,
                    "argumentsDigest": format!("{:x}", Sha256::digest(call.arguments.as_bytes())),
                }
            }
        });
        let permit = AtomicBool::new(false);
        self.approvals
            .answer(
                &self.attempt,
                payload,
                cancel,
                Box::new(|yes| {
                    let permit = &permit;
                    Box::pin(async move {
                        permit.store(yes, Ordering::Relaxed);
                        Ok(())
                    })
                }),
            )
            .await
            .map_err(|error| error.code)?;
        Ok(permit.load(Ordering::Relaxed) && !cancel.is_cancelled())
    }
}
