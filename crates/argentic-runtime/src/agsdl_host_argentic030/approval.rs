use crate::{
    agsdl_host_action_approval030::permits_model_review,
    harness::{CommandApprovals030, FlowWorkAttempt030},
    permissions::{native_request_path, ActionRequest},
};
use argentic_harness::{
    providers::{contains_credential, ToolCall},
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
    pub live_policy: Arc<dyn CommandApprovals030>,
    pub person: Option<Arc<dyn CommandApprovals030>>,
    pub attempt: FlowWorkAttempt030,
    pub sequence: AtomicU64,
    pub credential: Option<String>,
    pub model: bool,
}

fn file_request(path: &Path, write: bool) -> Result<(String, ActionRequest), String> {
    let path = native_request_path(path).ok_or("Unsupported file approval path")?;
    let (verb, action) = if write {
        ("Write", ActionRequest::Write { path: path.clone() })
    } else {
        ("Read", ActionRequest::Read { path: path.clone() })
    };
    Ok((format!("{verb} {path}"), action))
}

impl ToolApprovals {
    fn payload(
        &self,
        call: &ToolCall,
        title: String,
        actions: Vec<ActionRequest>,
    ) -> serde_json::Value {
        let sequence = self.sequence.fetch_add(1, Ordering::Relaxed);
        json!({
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
        })
    }
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
            Approval::Read(path) | Approval::Write(path) => {
                let (title, action) = file_request(path, matches!(action, Approval::Write(_)))?;
                (title, vec![action])
            }
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
        if self.credential.as_deref().is_some_and(|key| {
            contains_credential(&title, key) || contains_credential(&call.arguments, key)
        }) {
            return Ok(false);
        }
        let mut payload = self.payload(call, title, actions);
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

    async fn revalidate_files(
        &self,
        call: &ToolCall,
        actions: &[Approval<'_>],
    ) -> Result<Box<dyn Send + Sync>, String> {
        let actions = actions
            .iter()
            .map(|action| match action {
                Approval::Read(path) | Approval::Write(path) => {
                    file_request(path, matches!(action, Approval::Write(_)))
                        .map(|(_, action)| action)
                }
                _ => Err("Expected a file action".into()),
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut payload = self.payload(call, "Revalidate file access".into(), actions);
        if self.model {
            payload["reviewer"] = json!("model");
        }
        let lease = self
            .live_policy
            .lock_argentic_files(&self.attempt, &payload)
            .await
            .map_err(|error| error.code)?
            .ok_or("File permission denied by current policy")?;
        Ok(Box::new(lease))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_spelling(path: &Path, expected: &str) {
        for (write, action) in [(false, "read"), (true, "write")] {
            let (_, request) = file_request(path, write).unwrap();
            assert_eq!(
                serde_json::to_value(request).unwrap(),
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
