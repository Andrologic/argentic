use crate::{
    agsdl_host_action_approval030::permits_model_review,
    harness::{CommandApprovals030, FlowWorkAttempt030},
    permissions::{native_request_path, ActionRequest},
};
use argentic_harness::{
    providers::ToolCall,
    tools::{Approval, Authorizer},
};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::path::Path;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};
use tokio_util::sync::CancellationToken;

pub(super) struct ToolApprovals {
    pub approvals: Arc<dyn CommandApprovals030>,
    pub person: Option<Arc<dyn CommandApprovals030>>,
    pub attempt: FlowWorkAttempt030,
    pub sequence: AtomicU64,
    pub credential: Option<String>,
    pub model: bool,
}

fn file_request(path: &Path, write: bool) -> Result<(String, Vec<ActionRequest>), String> {
    let path = native_request_path(path).ok_or("Unsupported file approval path")?;
    let (verb, action) = if write {
        ("Write", ActionRequest::Write { path: path.clone() })
    } else {
        ("Read", ActionRequest::Read { path: path.clone() })
    };
    Ok((format!("{verb} {path}"), vec![action]))
}

#[async_trait::async_trait]
impl Authorizer for ToolApprovals {
    async fn approve(
        &self,
        call: &ToolCall,
        action: Approval<'_>,
        cancel: &CancellationToken,
    ) -> Result<bool, String> {
        // Proxy connections run outside the agent task that owns reviewer
        // journal events. The human broker owns the connection's approval.
        let approvals = if matches!(action, Approval::Network(_)) {
            let Some(person) = &self.person else {
                return Ok(false);
            };
            person
        } else {
            &self.approvals
        };
        let (title, actions) = match action {
            Approval::Read(path) => file_request(path, false)?,
            Approval::Write(path) => file_request(path, true)?,
            Approval::Delegate => {
                let args: serde_json::Value = serde_json::from_str(&call.arguments)
                    .map_err(|_| "Invalid delegation arguments")?;
                let task = args["task"].as_str().ok_or("Missing delegation task")?;
                (
                    format!("Delegate read-only task: {task}"),
                    vec![ActionRequest::Delegate {}],
                )
            }
            Approval::Network(host) => (
                format!("Connect to {host}"),
                vec![ActionRequest::Network { host: host.into() }],
            ),
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
        let mut payload = json!({
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
        if self.model && permits_model_review(&payload["nativeRequest"]) {
            payload["nativeRequest"]["params"]["reviewArguments"] = json!(call.arguments);
        }
        let permit = AtomicBool::new(false);
        approvals
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

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_spelling(path: &Path, expected: &str) {
        for (write, action) in [(false, "read"), (true, "write")] {
            let (_, requests) = file_request(path, write).unwrap();
            assert_eq!(requests.len(), 1);
            assert_eq!(
                serde_json::to_value(&requests[0]).unwrap(),
                json!({
                    "action": action,
                    "path": expected
                })
            );
        }
    }

    #[test]
    fn file_approval_requests_use_policy_paths() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().canonicalize().unwrap().join("memo.txt");
        let expected = path.to_str().unwrap();
        let expected = if cfg!(windows) {
            expected.trim_start_matches(r"\\?\").replace('\\', "/")
        } else {
            expected.to_owned()
        };
        assert_spelling(&path, &expected);
    }

    #[cfg(windows)]
    #[test]
    fn windows_file_approval_requests_accept_dos_and_verbatim_paths() {
        for path in [r"C:\work\memo.txt", r"\\?\C:\work\memo.txt"] {
            assert_spelling(Path::new(path), "C:/work/memo.txt");
        }
    }
}
