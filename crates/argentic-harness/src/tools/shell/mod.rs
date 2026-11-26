#[cfg(all(
    target_os = "linux",
    any(target_arch = "aarch64", target_arch = "x86_64")
))]
mod linux;
#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
mod macos;
#[cfg(any(
    all(
        target_os = "linux",
        any(target_arch = "aarch64", target_arch = "x86_64")
    ),
    all(target_os = "macos", target_arch = "aarch64")
))]
mod process;
#[cfg(any(
    all(
        target_os = "linux",
        any(target_arch = "aarch64", target_arch = "x86_64")
    ),
    all(target_os = "macos", target_arch = "aarch64")
))]
mod proxy;
#[cfg(any(
    all(
        target_os = "linux",
        any(target_arch = "aarch64", target_arch = "x86_64")
    ),
    all(target_os = "macos", target_arch = "aarch64")
))]
mod workspace;

use super::{output, Page, Registry};
use crate::{
    policy::{Action, Effect, Policy},
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

// Keep the launcher and its permission check on the same mount list.
#[cfg(not(target_os = "macos"))]
pub(super) const RUNTIME_DIRS: &[&str] = &[
    "/bin",
    "/usr/bin",
    "/lib",
    "/lib64",
    "/usr/lib",
    "/usr/lib64",
];
#[cfg(target_os = "linux")]
pub(super) const LOADER_CACHE: &str = "/etc/ld.so.cache";
#[cfg(not(target_os = "macos"))]
const BASE_RUNTIME_FILES: &[(&str, bool)] =
    &[("/dev", true), ("/proc", true), ("/etc/ld.so.cache", false)];
#[cfg(not(target_os = "macos"))]
pub(super) const NETWORK_MOUNTS: &[(&str, bool)] = &[("/etc/ssl/certs", true)];
#[cfg(target_os = "macos")]
pub(super) const RUNTIME_DIRS: &[&str] = &["/bin", "/usr/bin", "/usr/lib", "/System/Library"];
#[cfg(target_os = "macos")]
pub(super) const BASE_RUNTIME_FILES: &[(&str, bool)] =
    &[("/dev/null", false), ("/dev/urandom", false)];
#[cfg(target_os = "macos")]
pub(super) const NETWORK_MOUNTS: &[(&str, bool)] = &[("/private/etc/ssl", true)];

#[cfg(any(
    all(
        target_os = "linux",
        any(target_arch = "aarch64", target_arch = "x86_64")
    ),
    all(target_os = "macos", target_arch = "aarch64")
))]
pub(super) const BASH_ARGS: &[&str] = &[
    "/bin/bash", "--noprofile", "--norc", "-c",
    "if /bin/bash --noprofile --norc -n -c \"$1\"; then exec /bin/bash --noprofile --norc -c \"$1\"; else exit 2; fi",
    "argentic-shell",
];

#[derive(Debug, PartialEq, Eq)]
pub enum ShellReadConflict {
    Outside,
    Rule { index: usize, mount: &'static str },
}

impl std::fmt::Display for ShellReadConflict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Outside => f.write_str(
                "Explicit outside read denial conflicts with required shell runtime mounts",
            ),
            Self::Rule { mount, .. } => write!(
                f,
                "Read restriction overlaps required shell runtime mount {mount}"
            ),
        }
    }
}

pub fn shell_read_conflict(policy: &Policy, network: bool) -> Option<ShellReadConflict> {
    if policy.explicitly_denies_outside_reads() {
        return Some(ShellReadConflict::Outside);
    }
    RUNTIME_DIRS
        .iter()
        .map(|&path| (path, true))
        .chain(BASE_RUNTIME_FILES.iter().copied())
        .chain(NETWORK_MOUNTS.iter().copied().filter(|_| network))
        .filter_map(|(mount, tree)| {
            let index = policy.restricted_read_rule(mount, tree).or_else(|| {
                let source = std::fs::canonicalize(mount).ok()?;
                policy.restricted_read_rule(source.to_str()?, tree)
            })?;
            Some(ShellReadConflict::Rule { index, mount })
        })
        .min_by_key(|conflict| match conflict {
            ShellReadConflict::Rule { index, .. } => *index,
            ShellReadConflict::Outside => unreachable!(),
        })
}

pub fn shell_network_allowed(policies: &[Policy]) -> bool {
    !policies.is_empty() && policies.iter().all(|policy| policy.may_use_network())
}

pub fn preflight() -> Result<(), String> {
    #[cfg(all(
        target_os = "linux",
        any(target_arch = "aarch64", target_arch = "x86_64")
    ))]
    return linux::preflight();
    #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
    return macos::preflight();
    #[cfg(not(any(
    all(target_os = "linux", any(target_arch = "aarch64", target_arch = "x86_64")),
    all(target_os = "macos", target_arch = "aarch64")
)))]
    Err("Argentic shell requires Linux x86-64 or ARM64 with Bubblewrap, or macOS ARM64 with Seatbelt".into())
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
        let parsed = crate::policy::parse_shell(&args.command)?;
        let effect = self
            .policy
            .iter()
            .map(|policy| policy.parsed_shell_effect(&parsed))
            .max()
            .unwrap_or(Effect::Deny);
        let network = shell_network_allowed(&self.policy);
        if effect != Effect::Deny {
            for policy in &self.policy {
                if let Some(conflict) = shell_read_conflict(policy, network) {
                    return Err(conflict.to_string());
                }
            }
        }
        self.authorize(
            effect,
            &call,
            super::Approval::Shell(&args.command),
            &cancel,
        )
        .await?;
        // Take every root's gate in path order so overlapping calls cannot
        // deadlock. Identical roots share a gate.
        let mut roots: Vec<_> = self.roots.iter().zip(&self.root_gates).collect();
        roots.sort_unstable_by(|(left, _), (right, _)| left.path.cmp(&right.path));
        roots.dedup_by(|(left, _), (right, _)| left.path == right.path);
        let mut guards = Vec::with_capacity(roots.len());
        for (_, gate) in roots {
            guards.push(tokio::select! {
                biased;
                _ = cancel.cancelled() => return Err("Shell command cancelled".into()),
                guard = gate.clone().write_owned() => guard,
            });
        }
        let registry = self.clone();
        tracker
            .spawn_blocking(move || {
                let _guards = guards;
                let text = run(registry, &args, network, &call, &cancel)?;
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
    registry: Arc<Registry>,
    args: &Arguments,
    network: bool,
    call: &ToolCall,
    cancel: &CancellationToken,
) -> Result<String, String> {
    #[cfg(any(
        all(
            target_os = "linux",
            any(target_arch = "aarch64", target_arch = "x86_64")
        ),
        all(target_os = "macos", target_arch = "aarch64")
    ))]
    {
        let mut snapshots = Vec::with_capacity(registry.roots.len());
        for root in &registry.roots {
            let write = registry.writes
                && registry.policy.iter().all(|policy| {
                    let zone = registry.base_zone(&root.path);
                    [Action::Read, Action::Write]
                        .into_iter()
                        .all(|action| policy.blanket_allows_mount(action, &root.path, zone))
                });
            let snapshot = workspace::Snapshot::new(
                root,
                registry.shell_staging_directory.as_deref(),
                cancel,
                |path| {
                    registry.path_effect(root, Action::Read, &root.path.join(path)) == Effect::Allow
                },
            )
            .map_err(|error| error.to_string())?;
            snapshots.push((snapshot, write));
        }
        let mounts = snapshots
            .iter()
            .map(|(snapshot, write)| (snapshot.path(), *write))
            .collect::<Vec<_>>();
        let proxy = network
            .then(|| proxy::Proxy::start(registry.clone(), call.clone(), cancel))
            .transpose()
            .map_err(|error| format!("Argentic proxy is unavailable: {error}"))?;
        #[cfg(target_os = "linux")]
        let run = linux::run;
        #[cfg(target_os = "macos")]
        let run = macos::run;
        let outcome = run(
            &args.command,
            &mounts,
            proxy.as_ref(),
            args.timeout_ms,
            cancel,
        )?;
        drop(proxy);
        let mut pending = Vec::new();
        for (root, (snapshot, write)) in registry.roots.iter().zip(snapshots) {
            if write {
                let changes = snapshot
                    .changes(root, cancel, |path| {
                        let absolute = root.path.join(path);
                        [Action::Read, Action::Write].into_iter().all(|action| {
                            registry.path_effect(root, action, &absolute) == Effect::Allow
                        })
                    })
                    .map_err(|error| format!("Shell workspace publication: {error}"))?;
                pending.extend(changes.into_iter().map(|change| (root, change)));
            }
        }
        let total = pending.len();
        for (index, (root, change)) in pending.into_iter().enumerate() {
            change.apply(root, cancel).map_err(|error| {
                format!("Shell workspace publication: Published {index} of {total} file changes: {error}")
            })?;
        }
        Ok(outcome)
    }
    #[cfg(not(any(
        all(
            target_os = "linux",
            any(target_arch = "aarch64", target_arch = "x86_64")
        ),
        all(target_os = "macos", target_arch = "aarch64")
    )))]
    {
        let _ = (registry, args, network, call, cancel);
        preflight()?;
        unreachable!()
    }
}
