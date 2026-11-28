//! Host-owned calls use the existing confined process and captured MCP transports.
use crate::{
    agsdl_host::HostState,
    agsdl_host_call_authority030::NativeCallAuthority030,
    agsdl_host_calls::{self as transport, raw_mcp, CommandGrant, Control, PhysicalEvidence},
    agsdl_host_calls030::{
        self as calls, CallAttempt030, CallEvent030, JoinedCall030, PreparedCall030,
    },
    agsdl_host_confinement::{FileAccess, OsCeiling, PathScope, SandboxPolicy},
    agsdl_host_flow030::{self as flow, NativeFlowInspection030},
    agsdl_host_mcp_http::{self as http, RemoteMcp},
    agsdl_host_runtime030::HostExecutor030,
    host::{HostContext, HostPolicyLease, ProjectPolicyLease},
    permissions::{Preset, RequestedPermissions},
    persistence::{
        dto::EngineOwner, failure, flow030::FlowEffect030, start030::StartCapture030, Result,
    },
    source::{McpDeclarations, Snapshot},
    tasks::StreamTasks,
};
use futures::future::BoxFuture;
use serde_json::Value;
use sqlx::SqlitePool;
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};
use tokio_util::sync::CancellationToken;

pub(crate) struct CallExecutor030 {
    pub config: HostContext,
    pub pool: SqlitePool,
    pub tasks: StreamTasks,
    pub owner: EngineOwner,
    pub transport: std::sync::Arc<dyn CallTransport030>,
}
pub(crate) struct StartedCall030 {
    pub attempt: CallAttempt030,
    pub cancel: CancellationToken,
    pub timeout: CancellationToken,
    pub request_id: String,
    pub result: tokio::sync::oneshot::Receiver<Result<Option<JoinedCall030>>>,
}
impl HostExecutor030 for CallExecutor030 {
    fn start(
        &self,
        app: HostState,
        inspection: NativeFlowInspection030,
        effect: FlowEffect030,
        cancel: CancellationToken,
    ) -> BoxFuture<'static, Result<Option<StartedCall030>>> {
        let transport = self.transport.clone();
        let (config, pool, tasks, owner) = (
            self.config.clone(),
            self.pool.clone(),
            self.tasks.clone(),
            self.owner.clone(),
        );
        Box::pin(async move {
            let host = app.clone();
            let authority = NativeCallAuthority030::capture(&host, &owner)
                .await?
                .with_harnesses(config.harnesses());
            let broker = host.start_lease().await?.call_data030()?;
            let attempt = calls::reserve_call030(&pool, &authority, &effect).await?;
            #[cfg(test)]
            host.flow030().operator_test_yield("call.reserved").await;
            match calls::prepare_call030(&pool, &authority, &broker, &attempt, &cancel).await? {
                calls::CallPreparation030::Ready => (),
                calls::CallPreparation030::Failed { .. } => return Ok(None),
                calls::CallPreparation030::Diagnostic { .. } => {
                    return Err(failure(
                        "UNCERTAIN",
                        "call",
                        "Call preparation has a diagnostic",
                    ))
                }
            }
            drop(authority);
            #[cfg(test)]
            host.flow030().operator_test_yield("call.claim").await;
            let claim =
                flow::claim_expected(host.flow030(), &host, &pool, &effect.identity, &effect.id)
                    .await?
                    .ok_or_else(|| failure("STALE_REFERENCE", "call", "Call Start changed"))?;
            if claim.effect() != &effect {
                return Err(failure("CORRELATION", "call", "Claimed Call Start differs"));
            }
            let request_id = claim.request_id();
            let (send, result) = tokio::sync::oneshot::channel();
            let timeout = CancellationToken::new();
            let machine_timeout = timeout.clone();
            let (owned_attempt, owned_cancel) = (attempt.clone(), cancel.clone());
            let (clock_ready, wait_clock) = tokio::sync::oneshot::channel();
            let remote_mcp_budget_ms = config.remote_mcp_tool_budget_ms();
            let worker_app = app.clone();
            let clock_pool = pool.clone();
            let clock_owner = owner.clone();
            claim
                .handoff_host(&tasks, async move {
                    let host = worker_app.clone();
                    let result = async {
                        wait_clock.await.map_err(|_| {
                            failure("INTERRUPTED", "clock", "Clock admission failed")
                        })?;
                        let authority = NativeCallAuthority030::capture(&host, &owner)
                            .await?
                            .with_harnesses(config.harnesses());
                        let control = Control {
                            stop: owned_cancel,
                            machine_timeout,
                        };
                        let prepared = prepared(&pool, &owned_attempt).await?;
                        // Keep approved configuration leases until the synchronous spawn.
                        let project_id = &inspection.capture.source.owner.project_id;
                        let (global, project) = lock_call_policy(&config, project_id).await?;
                        let grant = require_current_call_policy(&global, &project)
                            .and_then(|()| grant(config.root(), &inspection.capture, &prepared));
                        #[cfg(test)]
                        host.flow030()
                            .operator_test_yield("call.before_launch")
                            .await;
                        let started = calls::launch_native030(
                            &pool,
                            &authority,
                            &owned_attempt,
                            &control,
                            || {
                                control
                                    .check()
                                    .and_then(|_| {
                                        grant.map_err(|e| {
                                            transport::CallError::new(&e.code, e.message)
                                        })
                                    })
                                    .and_then(|grant| {
                                        transport.start(
                                            grant,
                                            prepared.clone(),
                                            inspection.capture.clone(),
                                            owned_attempt.clone(),
                                            control.clone(),
                                            remote_tool_admission(
                                                host.clone(),
                                                pool.clone(),
                                                owner.clone(),
                                                owned_attempt.clone(),
                                                config.clone(),
                                                project_id.clone(),
                                            ),
                                        )
                                    })
                            },
                        )
                        .await;
                        drop(project);
                        drop(global);
                        // A Stop can win the preparation/launch race. It authorizes no IO.
                        let end = match started {
                            Ok(Some(Ok(started))) => started.await?,
                            Ok(None) => crate::persistence::call_protocol030::CallEnd030::TimedOut,
                            Ok(Some(Err(_))) => {
                                crate::persistence::call_protocol030::CallEnd030::Returned {
                                    event: CallEvent030::StartFailed,
                                }
                            }
                            Err(error) => {
                                if control.stop.is_cancelled()
                                    || flow::inspect_run(host.flow030(), &host, &pool, &owner)
                                        .await?
                                        .flow
                                        .state
                                        .stopping
                                {
                                    calls::stop_before_launch030(&pool, &authority, &owned_attempt)
                                        .await?;
                                    return Ok(None);
                                }
                                return Err(error);
                            }
                        };
                        let joined = JoinedCall030::from_native(owned_attempt, end, control);
                        Ok(Some(joined))
                    }
                    .await;
                    host.flow030().supervisors.wake(&owner.system_start_id);
                    let _ = send.send(result);
                })
                .await?;
            #[cfg(test)]
            host.flow030()
                .operator_test_yield("call.clock_admission")
                .await;
            let admitted = async {
                let human = host
                    .flow030()
                    .humans
                    .broker(&host, &clock_pool, &effect.identity)
                    .await?;
                let _operation = human.operation.lock().await;
                human
                    .admit_call(&host, &effect.id, remote_mcp_budget_ms)
                    .await?;
                human.pump(&host, &CancellationToken::new()).await?;
                Ok::<_, crate::persistence::dto::EngineFailure>(())
            }
            .await;
            if let Err(error) = admitted {
                // The handoff is durable but the worker never had permission to launch.
                drop(clock_ready);
                if !matches!(tasks.cancel_owned(&request_id).await, Ok(true)) {
                    return Err(failure(
                        "UNCERTAIN",
                        "call",
                        "Call ownership cleanup did not complete",
                    ));
                }
                if flow::inspect_run(host.flow030(), &host, &clock_pool, &clock_owner)
                    .await?
                    .flow
                    .state
                    .stopping
                {
                    let authority = NativeCallAuthority030::capture(&host, &clock_owner).await?;
                    match calls::stop_before_launch030(&clock_pool, &authority, &attempt).await? {
                        calls::CallSettlement030::Terminal { .. } => return Ok(None),
                        _ => {
                            return Err(failure(
                                "UNCERTAIN",
                                "call",
                                "Call completion has a diagnostic",
                            ))
                        }
                    }
                }
                return Err(error);
            }
            let _ = clock_ready.send(());
            Ok(Some(StartedCall030 {
                attempt,
                cancel,
                timeout,
                request_id,
                result,
            }))
        })
    }
}

async fn prepared(pool: &SqlitePool, attempt: &CallAttempt030) -> Result<PreparedCall030> {
    let raw: String = sqlx::query_scalar(
        "SELECT prepared_json FROM engine_flow030_calls WHERE call_id=? AND status='prepared'",
    )
    .bind(attempt.call_id())
    .fetch_one(pool)
    .await?;
    Ok(serde_json::from_str(&raw)?)
}

fn require_current_call_policy(
    global: &HostPolicyLease,
    project: &ProjectPolicyLease,
) -> Result<()> {
    if !matches!(
        global.permissions(),
        RequestedPermissions::Preset(Preset::Workspace)
    ) || !matches!(
        project.agsdl_host_permissions,
        RequestedPermissions::Preset(Preset::Workspace)
    ) || project.tools.get("readOnly").and_then(Value::as_bool) == Some(true)
    {
        return Err(failure(
            "PERMISSION",
            "call",
            "Calls require current host and project workspace permissions",
        ));
    }
    Ok(())
}

async fn lock_call_policy(
    config: &HostContext,
    project_id: &str,
) -> Result<(HostPolicyLease, ProjectPolicyLease)> {
    config
        .lock_ceiling_and_project(project_id)
        .await
        .map_err(|_| {
            failure(
                "PERMISSION",
                "call",
                "Host or project permissions unavailable",
            )
        })
}

/// Calls have no agent permissions to borrow. The captured host ceiling must
/// explicitly allow workspace access.
fn grant(
    data: &Path,
    capture: &StartCapture030,
    prepared: &PreparedCall030,
) -> Result<CommandGrant> {
    let private: Value = serde_json::from_str(&capture.private_capture_json)?;
    let scope: Value = serde_json::from_str(
        private["callScopeJson"]
            .as_str()
            .ok_or_else(|| failure("PERMISSION", "call", "Call scope missing"))?,
    )?;
    let captured: Value = serde_json::from_str(
        private["callPermissionsJson"]
            .as_str()
            .ok_or_else(|| failure("PERMISSION", "call", "Call permissions missing"))?,
    )?;
    if captured["agsdlHostPermissions"] != "workspace"
        || crate::tool_policy::agsdl_native_read_only(&captured)
    {
        return Err(failure(
            "PERMISSION",
            "call",
            "Calls require approved workspace permissions and a writable project",
        ));
    }
    let mcp = match prepared {
        PreparedCall030::Mcp { server, .. } => Some(server.as_str()),
        PreparedCall030::Command { .. } => None,
    };
    workspace_grant(data, capture, &scope, mcp, true)
}

pub(crate) fn agent_mcp_grant(
    config: &HostContext,
    capture: &StartCapture030,
    scope: &Value,
    server: &str,
    read_only: bool,
) -> Result<CommandGrant> {
    workspace_grant(config.root(), capture, scope, Some(server), !read_only)
}

fn workspace_grant(
    data: &Path,
    capture: &StartCapture030,
    scope: &Value,
    mcp: Option<&str>,
    writable: bool,
) -> Result<CommandGrant> {
    let root = PathBuf::from(
        scope["root"]
            .as_str()
            .ok_or_else(|| failure("PERMISSION", "call", "Workspace missing"))?,
    );
    let canonical = root
        .canonicalize()
        .map_err(|_| failure("PERMISSION", "call", "Workspace unavailable"))?;
    let identity = crate::content_io::admitted::workspace_root_identity(&canonical)
        .map_err(|_| failure("PERMISSION", "call", "Workspace identity unavailable"))?
        .observation_key()
        .ok_or_else(|| failure("PERMISSION", "call", "Workspace identity unavailable"))?;
    if canonical != root || scope["rootIdentity"] != identity {
        return Err(failure(
            "PERMISSION",
            "call",
            "Workspace changed since capture",
        ));
    }
    let mut read = vec![PathScope::Subtree(root.clone())];
    let mut write = Vec::new();
    if writable {
        write.push(PathScope::Subtree(root.clone()));
    }
    let mut block = Vec::new();
    if let Some(server) = mcp {
        if let Some(project) = RetainedMcp::open(data, capture, scope, server)? {
            if root.starts_with(&project.snapshot.root) || project.snapshot.root.starts_with(&root)
            {
                return Err(failure(
                    "PERMISSION",
                    "call",
                    "Workspace overlaps the retained project",
                ));
            }
            for (name, path) in project.files {
                project.snapshot.verify_member(&name)?;
                read.push(PathScope::Literal(path.clone()));
                read.extend(
                    path.ancestors()
                        .skip(1)
                        .take_while(|path| path.starts_with(&project.snapshot.root))
                        .map(|path| PathScope::Literal(path.to_owned())),
                );
            }
            block.push(PathScope::Subtree(project.snapshot.root));
        }
    }
    // Explicit native runtime dependencies. User home and application profiles
    // are outside these roots; the environment inherits no credentials.
    if cfg!(unix) {
        read.insert(0, PathScope::Literal(PathBuf::from("/")));
        for path in [
            "/bin",
            "/usr/bin",
            "/usr/lib",
            "/System/Library",
            "/private/var/select",
        ] {
            if Path::new(path).exists() {
                read.push(PathScope::Subtree(Path::new(path).canonicalize().map_err(
                    |_| failure("PERMISSION", "call", "Runtime directory unavailable"),
                )?));
            }
        }
        read.push(PathScope::Literal(PathBuf::from("/dev/null")));
        write.push(PathScope::Literal(PathBuf::from("/dev/null")));
    }
    let sandbox = SandboxPolicy::from_authorized_ceilings(
        capture.source.source_sha256.clone(),
        &[OsCeiling {
            read: FileAccess {
                allow: read,
                block: vec![],
            },
            write: FileAccess {
                allow: write,
                block,
            },
            network: false,
        }],
    )
    .map_err(|message| failure("HOST_CAPABILITY", "call", &message))?;
    Ok(CommandGrant {
        workspace: root,
        environment: BTreeMap::from([("PATH".into(), "/usr/bin:/bin".into())]),
        sandbox,
    })
}

fn remote_tool_admission(
    host: HostState,
    pool: SqlitePool,
    owner: EngineOwner,
    attempt: CallAttempt030,
    config: HostContext,
    project_id: String,
) -> raw_mcp::ToolAdmission {
    Box::new(move || {
        let (host, pool, owner, attempt, config, project_id) = (
            host.clone(),
            pool.clone(),
            owner.clone(),
            attempt.clone(),
            config.clone(),
            project_id.clone(),
        );
        Box::pin(async move {
            let admit = async {
                let authority = NativeCallAuthority030::capture(&host, &owner).await?;
                let policy = lock_call_policy(&config, &project_id).await?;
                require_current_call_policy(&policy.0, &policy.1)?;
                let gate = host.admission_lock().lock_owned().await;
                let mut tx = pool.begin().await?;
                crate::persistence::call_protocol030::admit_remote_tool_tx(
                    &mut tx, &authority, &attempt,
                )
                .await?;
                tx.commit().await?;
                let guard: Box<dyn Send> = Box::new((gate, policy));
                Ok::<_, crate::persistence::dto::EngineFailure>(guard)
            };
            admit
                .await
                .map_err(|e| transport::CallError::new(&e.code, e.message))
        })
    })
}

/// start is synchronous under native admission. The future owns every resource
/// it starts and returns only after physical cleanup, even on cancellation.
pub(crate) trait CallTransport030: Send + Sync {
    fn start(
        &self,
        grant: CommandGrant,
        prepared: PreparedCall030,
        capture: StartCapture030,
        attempt: CallAttempt030,
        control: Control,
        admit_tool: raw_mcp::ToolAdmission,
    ) -> transport::Result<
        BoxFuture<'static, Result<crate::persistence::call_protocol030::CallEnd030>>,
    >;
}
pub(crate) struct NativeCallTransport030 {
    pub config: HostContext,
    pub humans: Option<std::sync::Arc<dyn transport::HostPort>>,
}
impl CallTransport030 for NativeCallTransport030 {
    fn start(
        &self,
        grant: CommandGrant,
        prepared: PreparedCall030,
        capture: StartCapture030,
        attempt: CallAttempt030,
        control: Control,
        admit_tool: raw_mcp::ToolAdmission,
    ) -> transport::Result<
        BoxFuture<'static, Result<crate::persistence::call_protocol030::CallEnd030>>,
    > {
        let started = start_transport(&self.config, grant, &prepared, &capture)?;
        let humans = self
            .humans
            .clone()
            .unwrap_or_else(|| std::sync::Arc::new(NoHuman));
        Ok(Box::pin(async move {
            run_transport(started, &prepared, &attempt, &control, humans, admit_tool).await
        }))
    }
}

pub(crate) enum StartedTransport {
    Command(
        crate::process::ContainedBackgroundProcess,
        Box<SandboxPolicy>,
    ),
    Mcp(Box<raw_mcp::Session>),
    RemoteMcp(Box<RemoteMcp>),
}
fn start_transport(
    config: &HostContext,
    grant: CommandGrant,
    prepared: &PreparedCall030,
    capture: &StartCapture030,
) -> transport::Result<StartedTransport> {
    match prepared {
        PreparedCall030::Command { command, .. } => {
            let child = grant
                .sandbox
                .spawn(
                    Path::new("/bin/sh"),
                    &["-c".into(), command.clone()],
                    &grant.workspace,
                    &grant.environment,
                )
                .map_err(transport::CallError::transport)?;
            Ok(StartedTransport::Command(child, Box::new(grant.sandbox)))
        }
        PreparedCall030::Mcp { server, .. } => {
            let document: Value = serde_json::from_str(&capture.authored_source)
                .map_err(|_| transport::CallError::transport("MCP document is invalid"))?;
            if let Some(url) = document["mcpServers"][server]["url"].as_str() {
                let remote = remote_mcp(config, capture, server, url)?;
                Ok(StartedTransport::RemoteMcp(Box::new(remote)))
            } else {
                raw_mcp::require_stdio()?;
                raw_mcp::start(mcp_server(config, grant, capture, server)?)
                    .map(|session| StartedTransport::Mcp(Box::new(session)))
            }
        }
    }
}

struct RetainedMcp {
    snapshot: Snapshot,
    files: BTreeMap<String, PathBuf>,
    names: Vec<String>,
}

impl RetainedMcp {
    fn open(
        data: &Path,
        capture: &StartCapture030,
        scope: &Value,
        server: &str,
    ) -> Result<Option<Self>> {
        let Some(snapshot) = Snapshot::from_capture(data, capture)? else {
            return Ok(None);
        };
        let mut declarations: McpDeclarations = serde_json::from_value(scope["projectMcp"].clone())
            .map_err(|_| {
                failure(
                    "CORRELATION",
                    "projectSnapshot",
                    "Retain and bind the project MCP declarations before Start",
                )
            })?;
        let mut files = BTreeMap::new();
        for name in declarations.files.remove(server).unwrap_or_default() {
            files.insert(name.clone(), snapshot.root.join(name));
        }
        Ok(Some(Self {
            snapshot,
            files,
            names: declarations.secrets.remove(server).unwrap_or_default(),
        }))
    }

    fn file(&self, argument: &str) -> Option<&Path> {
        if argument.contains('\\') {
            return None;
        }
        let mut parts = Vec::new();
        for part in Path::new(argument).components() {
            match part {
                std::path::Component::Normal(part) => parts.push(part.to_str()?),
                std::path::Component::CurDir => (),
                std::path::Component::ParentDir => {
                    parts.pop()?;
                }
                _ => return None,
            }
        }
        self.files.get(&parts.join("/")).map(PathBuf::as_path)
    }
}

fn retained_mcp(
    config: &HostContext,
    capture: &StartCapture030,
    server: &str,
) -> transport::Result<Option<RetainedMcp>> {
    let private: Value = serde_json::from_str(&capture.private_capture_json)
        .map_err(|_| transport::CallError::transport("MCP capture is invalid"))?;
    let scope: Value = serde_json::from_str(private["callScopeJson"].as_str().unwrap_or(""))
        .map_err(|_| transport::CallError::transport("MCP scope is invalid"))?;
    RetainedMcp::open(config.root(), capture, &scope, server)
        .map_err(|error| transport::CallError::new(&error.code, error.message))
}

pub(crate) fn remote_mcp(
    config: &HostContext,
    capture: &StartCapture030,
    server: &str,
    url: &str,
) -> transport::Result<RemoteMcp> {
    Ok(RemoteMcp {
        config: config.clone(),
        source: capture.source.clone(),
        server: server.to_owned(),
        url: http::endpoint(url)?,
        names: retained_mcp(config, capture, server)?
            .map(|project| project.names)
            .unwrap_or_default(),
    })
}

pub(crate) fn server_secrets(
    config: &HostContext,
    source: &crate::persistence::dto::EngineSourceRevision,
    server: &str,
    names: &[String],
) -> transport::Result<BTreeMap<String, String>> {
    if names.is_empty() {
        return Ok(BTreeMap::new());
    }
    if names
        .iter()
        .any(|name| crate::source::mcp_secret_is_reserved(name))
    {
        return Err(transport::CallError::new(
            "MCP_SECRET_RESERVED",
            "MCP secret name is reserved by the launcher",
        ));
    }
    let unavailable = || {
        transport::CallError::new(
            "MCP_SECRETS_UNAVAILABLE",
            "MCP server secrets are unavailable",
        )
    };
    let secrets = config
        .mcp_secrets(source, server, names)
        .ok_or_else(unavailable)?;
    if secrets.len() != names.len()
        || names.iter().any(|name| !secrets.contains_key(name))
        || secrets.values().any(|value| value.as_bytes().contains(&0))
    {
        return Err(unavailable());
    }
    Ok(secrets)
}

pub(crate) fn mcp_server(
    config: &HostContext,
    mut grant: CommandGrant,
    capture: &StartCapture030,
    server: &str,
) -> transport::Result<raw_mcp::CapturedServer> {
    let project = retained_mcp(config, capture, server)?;
    let document: Value = serde_json::from_str(&capture.authored_source)
        .map_err(|e| transport::CallError::transport(e.to_string()))?;
    let command = document["mcpServers"][server]["command"]
        .as_array()
        .ok_or_else(|| {
            transport::CallError::transport("Only local stdio MCP servers are supported")
        })?;
    let program = command
        .first()
        .and_then(Value::as_str)
        .ok_or_else(|| transport::CallError::transport("MCP program missing"))?;
    let program = if let Some(project) = &project {
        if let Some(path) = project.file(program) {
            if !project.snapshot.executable(path) {
                return Err(transport::CallError::transport(
                    "Retained MCP program is not executable",
                ));
            }
            if !project
                .snapshot
                .allows_shebang(path)
                .map_err(|error| transport::CallError::new(&error.code, error.message))?
            {
                return Err(transport::CallError::transport(
                    "Retained MCP program requires an absolute shebang interpreter",
                ));
            }
            path.to_owned()
        } else if matches!(program, "sh" | "/bin/sh")
            && command
                .get(1)
                .and_then(Value::as_str)
                .and_then(|script| project.file(script))
                .is_some()
        {
            // The system shell may interpret only a declared retained script.
            PathBuf::from("/bin/sh")
        } else {
            return Err(transport::CallError::transport(
                "Project MCP requires a retained executable or /bin/sh with a declared script",
            ));
        }
    } else if Path::new(program).is_absolute() {
        PathBuf::from(program)
    } else if program.contains('/') {
        grant.workspace.join(program)
    } else {
        ["/usr/bin", "/bin"]
            .iter()
            .map(|dir| Path::new(dir).join(program))
            .find(|path| path.is_file())
            .ok_or_else(|| {
                transport::CallError::transport("MCP executable not found in the confined PATH")
            })?
    };
    let arguments = command
        .iter()
        .skip(1)
        .map(|v| {
            v.as_str()
                .ok_or_else(|| transport::CallError::transport("MCP argument is not text"))
                .and_then(|argument| {
                    match project.as_ref().and_then(|project| project.file(argument)) {
                        Some(path) => path.to_str().map(str::to_owned).ok_or_else(|| {
                            transport::CallError::transport("MCP asset path is not UTF-8")
                        }),
                        None => Ok(argument.to_owned()),
                    }
                })
        })
        .collect::<transport::Result<Vec<_>>>()?;
    let mut secret_values = Vec::new();
    if let Some(project) = project.filter(|project| !project.names.is_empty()) {
        if project
            .names
            .iter()
            .any(|name| grant.environment.contains_key(name))
        {
            return Err(transport::CallError::new(
                "MCP_SECRET_RESERVED",
                "MCP secret name is reserved by the launcher",
            ));
        }
        let secrets = server_secrets(config, &capture.source, server, &project.names)?;
        secret_values.extend(secrets.values().filter(|value| !value.is_empty()).cloned());
        grant.environment.extend(secrets);
    }
    Ok(raw_mcp::CapturedServer {
        document_name: server.to_owned(),
        capture_digest: capture.source.source_sha256.clone(),
        protocol: raw_mcp::Protocol::Legacy,
        transport: raw_mcp::ServerTransport::Stdio(raw_mcp::StdioServer {
            program,
            arguments,
            grant,
            secret_values,
        }),
    })
}
pub(crate) async fn run_transport(
    started: StartedTransport,
    prepared: &PreparedCall030,
    attempt: &CallAttempt030,
    control: &Control,
    humans: std::sync::Arc<dyn transport::HostPort>,
    admit_tool: raw_mcp::ToolAdmission,
) -> Result<crate::persistence::call_protocol030::CallEnd030> {
    use crate::persistence::call_protocol030::CallEnd030;
    let started = match started {
        StartedTransport::RemoteMcp(remote) => match remote.start(control).await {
            Ok(mut session) => {
                session.set_tool_admission(admit_tool);
                StartedTransport::Mcp(Box::new(session))
            }
            Err(_) if control.stop.is_cancelled() => return Ok(CallEnd030::Stopped),
            Err(_) => {
                return Ok(CallEnd030::Returned {
                    event: CallEvent030::StartFailed,
                })
            }
        },
        other => other,
    };
    let (event, call_failure, evidence) = match started {
        StartedTransport::RemoteMcp(_) => unreachable!(),
        StartedTransport::Command(child, policy) => {
            let (receipt, failure, evidence) =
                transport::run_command(child, &policy, control, None).await;
            let event = receipt.map(|r| CallEvent030::CommandReceipt {
                stdout: r.stdout,
                stderr: r.stderr,
                exit_code: r.exit_code_json.parse().unwrap_or(1),
            });
            (event, failure, evidence)
        }
        StartedTransport::Mcp(mut session) => {
            let PreparedCall030::Mcp {
                tool,
                arguments_json,
                ..
            } = prepared
            else {
                unreachable!()
            };
            let identity = transport::CallIdentity {
                system_id: attempt.identity().system_start_id.clone(),
                run_id: attempt.identity().flow_invocation_id.clone(),
                group_id: String::new(),
                work_id: attempt.work().activation_id.clone(),
                epoch: attempt.generation().into(),
                effect_id: attempt.effect().id.clone(),
                provider_attempt_id: None,
                native_call_id: attempt.call_id().into(),
            };
            let result = session
                .call(tool, arguments_json, humans, &identity, control)
                .await;
            let tool_in_flight = session.tool_in_flight();
            let (mut evidence, cleanup) = session.shutdown(result.is_ok()).await;
            if let PhysicalEvidence::Remote {
                termination_proven, ..
            } = &mut evidence
            {
                *termination_proven = !tool_in_flight && cleanup.is_none();
            }
            match result {
                Ok(r) => (
                    Some(CallEvent030::ToolReturned {
                        is_error: r.is_error,
                        text: r.text,
                        structured_content_json: r.structured_content_json,
                    }),
                    cleanup,
                    evidence,
                ),
                Err(error) => (None, cleanup.or(Some(error)), evidence),
            }
        }
    };
    if !matches!(
        evidence,
        PhysicalEvidence::Local {
            leader_reaped: true,
            tree_termination_succeeded: true,
            streams_joined: true,
            ..
        } | PhysicalEvidence::Remote {
            response_received: true,
            ..
        } | PhysicalEvidence::Remote {
            termination_proven: true,
            ..
        }
    ) {
        let code = if matches!(evidence, PhysicalEvidence::Remote { .. }) {
            "MCP_TERMINATION_UNCERTAIN"
        } else {
            "UNCERTAIN"
        };
        return Err(failure(
            code,
            "call",
            "Call termination could not be confirmed",
        ));
    }
    if control.stop.is_cancelled() {
        return Ok(CallEnd030::Stopped);
    }
    Ok(CallEnd030::Returned {
        event: if call_failure
            .as_ref()
            .is_some_and(|error| error.code == "OUTPUT_INVALID")
            && matches!(prepared, PreparedCall030::Command { .. })
        {
            CallEvent030::OutputInvalid
        } else if call_failure.is_some() {
            CallEvent030::StartFailed
        } else {
            event.unwrap_or(CallEvent030::StartFailed)
        },
    })
}

// The transport's human callbacks fail explicitly until lot 3.2. No implicit
// approval/decline, fixed watchdog or historical work protocol is introduced.
pub(crate) struct NoHuman;
#[async_trait::async_trait]
impl transport::HostPort for NoHuman {
    fn supports_human_requests(&self) -> bool {
        false
    }
    #[cfg(test)]
    async fn claim(&self, _: &transport::CallRequest) -> transport::Result<transport::ClaimedCall> {
        Err(no_human())
    }
    #[cfg(test)]
    fn launch_gate(&self) -> std::sync::Arc<tokio::sync::Mutex<()>> {
        std::sync::Arc::new(tokio::sync::Mutex::new(()))
    }
    #[cfg(test)]
    async fn authorize_launch(
        &self,
        _: &transport::CallRequest,
        _: &str,
    ) -> transport::Result<transport::LaunchGrant> {
        Err(no_human())
    }
    #[cfg(test)]
    async fn finish(&self, _: &str, _: &transport::Completion) -> transport::Result<()> {
        Err(no_human())
    }
    async fn human_open(
        &self,
        _: &transport::CallIdentity,
        _: &str,
        _: &str,
    ) -> transport::Result<transport::HumanTicket> {
        Err(no_human())
    }
    async fn human_answer(&self, _: &str) -> transport::Result<transport::HumanReply> {
        Err(no_human())
    }
    async fn human_close(&self, _: &str, _: &transport::Result<String>) -> transport::Result<()> {
        Err(no_human())
    }
}
fn no_human() -> transport::CallError {
    transport::CallError::new(
        "HOST_CAPABILITY",
        "MCP human requests are not supported in this run",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn remote_handoff_policy_requires_both_current_workspace_grants() {
        let root = tempfile::tempdir().unwrap();
        let (config, policy) = crate::test_context::context_with_policy(root.path().to_owned());
        policy
            .set_project(RequestedPermissions::Preset(Preset::Workspace))
            .await;
        let (global, project) = lock_call_policy(&config, "project").await.unwrap();
        assert!(require_current_call_policy(&global, &project).is_err());
        drop((global, project));

        policy.workspace().await;
        let (global, project) = lock_call_policy(&config, "project").await.unwrap();
        assert!(require_current_call_policy(&global, &project).is_ok());
        let mut revoke =
            std::pin::pin!(policy.set_project(RequestedPermissions::Preset(Preset::ReadOnly)));
        assert!(futures::poll!(&mut revoke).is_pending());
        drop((global, project));
        revoke.await;

        let (global, project) = lock_call_policy(&config, "project").await.unwrap();
        assert!(require_current_call_policy(&global, &project).is_err());
    }
}
