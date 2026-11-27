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

use super::{Approval, ApprovalSource, DispatchAction, DispatchPolicy, Page, Registry};
use crate::{
    policy::{Action, Effect, Policy},
    providers::ToolCall,
};
use serde::{Deserialize, Serialize};
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
// dyld reads the root directory entry during startup. Keep this a literal
// so opening any directory below it still needs its own grant.
#[cfg(target_os = "macos")]
pub(super) const BASE_RUNTIME_FILES: &[(&str, bool)] =
    &[("/", false), ("/dev/null", false), ("/dev/urandom", false)];
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

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ShellNetwork {
    #[default]
    Blanket,
    Intercept,
}

pub const SHELL_NETWORK_PATTERN: &str =
    "Shell network host patterns require the Argentic host setting shellNetwork: \"intercept\"";

pub fn shell_network_allowed(policies: &[Policy], mode: ShellNetwork) -> bool {
    let allows = match mode {
        ShellNetwork::Blanket => Policy::blanket_network,
        ShellNetwork::Intercept => Policy::may_use_network,
    };
    !policies.is_empty() && policies.iter().all(allows)
}

pub fn preflight() -> Result<(), String> {
    preflight_network(false)
}

pub fn preflight_network(network: bool) -> Result<(), String> {
    let _ = network;
    #[cfg(all(
        target_os = "linux",
        any(target_arch = "aarch64", target_arch = "x86_64")
    ))]
    return linux::preflight(network);
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
        credential: Option<&str>,
    ) -> Result<Page, String> {
        if !self.toolbox.shell {
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
        if effect != Effect::Deny {
            self.shell_network_for(None)?;
        }
        let approved_by = self
            .authorize_action(effect, &call, Approval::Shell(&args.command), &cancel)
            .await?
            .approved_by;
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
        let admission = self
            .lock_dispatch(
                &call,
                &[DispatchAction {
                    action: Approval::Shell(&args.command),
                    approved_by,
                }],
                &cancel,
            )
            .await?;
        let registry = self.clone();
        let credential = credential.map(str::to_owned);
        tracker
            .spawn_blocking(move || {
                let _guards = guards;
                let text = run(registry, &args, &call, approved_by, admission, &cancel)?;
                // Shell output starts with its status and is retained once. Replaying
                // a shell call to retrieve a cursor would repeat its effects.
                Page::one_shot_screened(&text, &call.id, credential.as_deref())
                    .map_err(|error| error.to_string())
            })
            .await
            .map_err(|e| e.to_string())?
    }

    fn shell_network_for(&self, live: Option<&Policy>) -> Result<bool, String> {
        let network = shell_network_allowed(&self.policy, self.shell_network)
            && live.is_none_or(|policy| {
                shell_network_allowed(std::slice::from_ref(policy), self.shell_network)
            });
        for policy in self.policy.iter().chain(live) {
            if self.shell_network == ShellNetwork::Blanket
                && policy.first_network_pattern().is_some()
            {
                return Err(SHELL_NETWORK_PATTERN.into());
            }
            if let Some(conflict) = shell_read_conflict(policy, network) {
                return Err(conflict.to_string());
            }
        }
        Ok(network)
    }
}

#[cfg(any(
    all(
        target_os = "linux",
        any(target_arch = "aarch64", target_arch = "x86_64")
    ),
    all(target_os = "macos", target_arch = "aarch64")
))]
fn recheck_shell(
    policy: &DispatchPolicy,
    command: &str,
    approved_by: Option<ApprovalSource>,
) -> Result<(), String> {
    match policy
        .policy
        .as_ref()
        .map(|policy| policy.shell_effect(command))
    {
        Some(Effect::Deny) => Err("Shell permission denied by current policy".into()),
        Some(Effect::Ask) if approved_by != Some(ApprovalSource::Person) => {
            Err("Shell permission denied by current policy".into())
        }
        _ => Ok(()),
    }
}

fn run(
    registry: Arc<Registry>,
    args: &Arguments,
    call: &ToolCall,
    approved_by: Option<ApprovalSource>,
    admission: DispatchPolicy,
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
        recheck_shell(&admission, &args.command, approved_by)?;
        let network = registry.shell_network_for(admission.policy.as_ref())?;
        let mut snapshots = Vec::with_capacity(registry.roots.len());
        for root in &registry.roots {
            let write = registry.toolbox.writes
                && !admission.read_only
                && registry
                    .policy
                    .iter()
                    .chain(admission.policy.iter())
                    .all(|policy| {
                        let zone = registry.base_zone(&root.path);
                        [Action::Read, Action::Write]
                            .into_iter()
                            .all(|action| policy.blanket_allows_mount(action, &root.path, zone))
                    });
            let snapshot = workspace::Snapshot::new(
                root,
                registry.shell_staging_directory.as_deref(),
                cancel,
                write,
                |path| {
                    registry.live_path_effect(&admission, root, Action::Read, &root.path.join(path))
                        == Effect::Allow
                },
            )
            .map_err(|error| error.to_string())?;
            snapshots.push((snapshot, write));
        }
        let mounts = snapshots
            .iter()
            .map(|(snapshot, write)| (snapshot.path(), *write))
            .collect::<Vec<_>>();
        let live = admission.policy.clone();
        drop(admission);
        let proxy = network
            .then(|| proxy::Proxy::start(registry.clone(), live, call.clone(), cancel))
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
        let publication = tokio::runtime::Handle::current().block_on(registry.lock_dispatch(
            call,
            &[DispatchAction {
                action: Approval::Shell(&args.command),
                approved_by,
            }],
            cancel,
        ))?;
        recheck_shell(&publication, &args.command, approved_by)?;
        let mut pending = Vec::new();
        for (root, (snapshot, write)) in registry.roots.iter().zip(snapshots) {
            if write {
                let changes = snapshot
                    .changes(root, cancel, |path| {
                        let absolute = root.path.join(path);
                        [Action::Read, Action::Write].into_iter().all(|action| {
                            registry.live_path_effect(&publication, root, action, &absolute)
                                == Effect::Allow
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
        let _ = (registry, args, call, approved_by, admission, cancel);
        preflight()?;
        unreachable!()
    }
}

#[cfg(all(
    test,
    target_os = "linux",
    any(target_arch = "aarch64", target_arch = "x86_64")
))]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod dispatch_tests {
    use super::*;
    use crate::{
        policy::{Preset, Rule},
        tools::Authorizer,
    };
    use std::time::Duration;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
        sync::{Notify, RwLock},
    };

    struct LivePolicy {
        approved: Notify,
        allowed: Arc<RwLock<bool>>,
    }

    #[async_trait::async_trait]
    impl Authorizer for LivePolicy {
        async fn approve(
            &self,
            _: &ToolCall,
            action: Approval<'_>,
            _: &CancellationToken,
        ) -> Result<bool, String> {
            assert!(matches!(action, Approval::Shell(_)));
            self.approved.notify_one();
            Ok(true)
        }

        async fn revalidate_dispatch(
            &self,
            _: &ToolCall,
            actions: &[DispatchAction<'_>],
        ) -> Result<DispatchPolicy, String> {
            assert!(matches!(
                actions,
                [DispatchAction {
                    action: Approval::Shell(_),
                    approved_by: Some(ApprovalSource::Person),
                }]
            ));
            let guard = self.allowed.clone().read_owned().await;
            if !*guard {
                return Err("Shell permission denied by current policy".into());
            }
            Ok(DispatchPolicy::new(guard))
        }
    }

    fn setup(path: &std::path::Path, network: bool) -> (Arc<Registry>, Arc<LivePolicy>) {
        let mut rules = vec![Rule {
            action: Action::Shell,
            pattern: None,
            effect: Effect::Ask,
        }];
        if network {
            rules.push(Rule {
                action: Action::Network,
                pattern: Some("127.0.0.1".into()),
                effect: Effect::Allow,
            });
        }
        let policy =
            Policy::compile_rules(path, None, Some(Preset::Workspace), None, &rules).unwrap();
        let live = Arc::new(LivePolicy {
            approved: Notify::new(),
            allowed: Arc::new(RwLock::new(true)),
        });
        let registry = Registry::new(&[path.to_owned()], policy, serde_json::json!(true))
            .unwrap()
            .with_writes()
            .with_shell()
            .with_shell_network(ShellNetwork::Intercept)
            .with_authorizer(live.clone());
        (Arc::new(registry), live)
    }

    async fn shell_call(registry: Arc<Registry>, command: String) -> Result<Page, String> {
        registry
            .read_tool(
                ToolCall {
                    id: "shell-test".into(),
                    name: "shell".into(),
                    arguments: serde_json::json!({"command":command,"timeout_ms":10000})
                        .to_string(),
                },
                CancellationToken::new(),
            )
            .await
    }

    #[tokio::test]
    async fn shell_rechecks_approval_after_waiting_for_root() {
        let dir = tempfile::tempdir().unwrap();
        let (registry, live) = setup(dir.path(), false);
        let gate = registry.root_gates[0].clone().write_owned().await;
        let command = tokio::spawn(shell_call(registry, "printf changed > memo".into()));
        live.approved.notified().await;
        *live.allowed.write().await = false;
        drop(gate);
        let error = command.await.unwrap().unwrap_err();
        assert!(
            error.contains("permission denied by current policy"),
            "{error}"
        );
        assert!(!dir.path().join("memo").exists());
    }

    #[tokio::test]
    async fn shell_releases_its_lease_during_bash_and_rechecks_before_publication() {
        if let Err(error) = preflight_network(true) {
            assert_ne!(
                std::env::var("ARGENTIC_REQUIRE_BWRAP").as_deref(),
                Ok("1"),
                "{error}"
            );
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let (registry, live) = setup(dir.path(), true);
        let server = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = server.local_addr().unwrap().port();
        let command = tokio::spawn(shell_call(
            registry,
            format!("curl -fsS http://127.0.0.1:{port}/ && printf changed > memo"),
        ));
        let (mut socket, _) = tokio::time::timeout(Duration::from_secs(5), server.accept())
            .await
            .unwrap()
            .unwrap();
        let mut request = [0; 1024];
        assert!(socket.read(&mut request).await.unwrap() > 0);
        let mut policy = tokio::time::timeout(Duration::from_secs(1), live.allowed.write())
            .await
            .expect("Bash must not hold the policy lease");
        *policy = false;
        drop(policy);
        socket
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
            .await
            .unwrap();
        drop(socket);
        let error = command.await.unwrap().unwrap_err();
        assert!(
            error.contains("permission denied by current policy"),
            "{error}"
        );
        assert!(!dir.path().join("memo").exists());
    }
}
