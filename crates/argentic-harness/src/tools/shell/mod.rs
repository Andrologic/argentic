#[cfg(all(
    target_os = "linux",
    any(target_arch = "aarch64", target_arch = "x86_64")
))]
mod linux;
#[cfg(all(
    target_os = "linux",
    any(target_arch = "aarch64", target_arch = "x86_64")
))]
mod workspace;

use super::{output, Page, Registry};
use crate::{
    policy::{Action, Effect, Zone},
    providers::ToolCall,
};
use serde::Deserialize;
use std::sync::Arc;
use tokio_util::{sync::CancellationToken, task::TaskTracker};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Arguments {
    command: String,
    #[serde(default = "default_timeout")]
    timeout_ms: u64,
}

fn default_timeout() -> u64 {
    30_000
}

pub fn preflight() -> Result<(), String> {
    #[cfg(all(
        target_os = "linux",
        any(target_arch = "aarch64", target_arch = "x86_64")
    ))]
    return linux::preflight();
    #[cfg(not(all(
        target_os = "linux",
        any(target_arch = "aarch64", target_arch = "x86_64")
    )))]
    Err("Argentic shell requires Linux on x86-64 or ARM64 with system Bubblewrap".into())
}

impl Registry {
    pub(super) async fn shell(
        self: &Arc<Self>,
        call: ToolCall,
        cancel: CancellationToken,
        tracker: &TaskTracker,
    ) -> Result<Page, String> {
        if !self.shell {
            return Err("Shell tool is disabled".into());
        }
        let args: Arguments = serde_json::from_str(&call.arguments).map_err(|e| e.to_string())?;
        if !(1..=300_000).contains(&args.timeout_ms) {
            return Err("Shell timeout_ms must be between 1 and 300000".into());
        }
        let commands = crate::policy::subcommands(&args.command)?;
        let effect = self
            .policy
            .iter()
            .flat_map(|policy| {
                commands
                    .iter()
                    .map(move |command| policy.decide(Action::Shell, command, Zone::Workspace))
            })
            .max()
            .unwrap_or(Effect::Deny);
        self.authorize(
            effect,
            &call,
            super::Approval::Shell(&args.command),
            &cancel,
        )
        .await?;
        let read = self
            .policy
            .iter()
            .all(|policy| policy.blanket_allows(Action::Read, Zone::Workspace));
        let write = read
            && self.writes
            && self
                .policy
                .iter()
                .all(|policy| policy.blanket_allows(Action::Write, Zone::Workspace));
        let network = self
            .policy
            .iter()
            .all(|policy| policy.blanket_allows(Action::Network, Zone::Workspace));
        let registry = self.clone();
        let guard = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err("Shell command cancelled".into()),
            guard = self.workspace_gate.clone().write_owned() => guard,
        };
        tracker
            .spawn_blocking(move || {
                let _guard = guard;
                let text = run(&registry, &args, write, network, &cancel)?;
                // Shell output starts with its status and is retained once. Replaying
                // a shell call to retrieve a cursor would repeat its effects.
                let mut page =
                    output::paginate(&text, &call.id, None, None).map_err(|e| e.to_string())?;
                page.next_cursor = None;
                Ok(page)
            })
            .await
            .map_err(|e| e.to_string())?
    }
}

fn run(
    registry: &Registry,
    args: &Arguments,
    write: bool,
    network: bool,
    cancel: &CancellationToken,
) -> Result<String, String> {
    #[cfg(all(
        target_os = "linux",
        any(target_arch = "aarch64", target_arch = "x86_64")
    ))]
    {
        let root = &registry.roots[0];
        let workspace = workspace::Snapshot::new(root, cancel, |path| {
            registry.path_effect(root, Action::Read, &root.path.join(path)) == Effect::Allow
        })
        .map_err(|e| e.to_string())?;
        let outcome = linux::run(
            &args.command,
            workspace.path(),
            write,
            network,
            args.timeout_ms,
            cancel,
        )?;
        if write {
            workspace
                .publish(root, cancel, |path| {
                    let absolute = root.path.join(path);
                    [Action::Read, Action::Write].into_iter().all(|action| {
                        registry.path_effect(root, action, &absolute) == Effect::Allow
                    })
                })
                .map_err(|e| format!("Shell workspace publication: {e}"))?;
        }
        Ok(outcome)
    }
    #[cfg(not(all(
        target_os = "linux",
        any(target_arch = "aarch64", target_arch = "x86_64")
    )))]
    {
        let _ = (registry, args, write, network, cancel);
        preflight()?;
        unreachable!()
    }
}
