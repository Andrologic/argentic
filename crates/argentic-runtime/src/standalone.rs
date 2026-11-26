//! Single-process host using the production Start transaction and supervisor.
//! Loading inputs never starts a provider. Only explicit preflight/Start may
//! observe a registered harness with the caller's explicit opt-in.
use crate::{
    api::{self, HumanSubmission, StartSession, StreamTasks},
    host::{
        ActionPolicyLease, ActionPolicySource, HostContext, HostPolicyLease, PolicyFuture,
        PolicySource, ProjectPolicyLease,
    },
    persistence::{
        self,
        dto::*,
        failure,
        flow030::{FlowEffect030, FlowIdentity030},
        flow_store030::{Authority030, CheckFuture, StoreError030},
        start030::{self, RetainedStart030, StartAuthority030, StartCapture030},
        Result,
    },
    source::{OpenedSource, RetainedSource},
    Engine,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::{SqliteConnection, SqlitePool};
use std::{
    collections::BTreeSet,
    fs::File,
    io::{self, Read},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};
use tokio::sync::{OwnedRwLockReadGuard, RwLock};

/// The exact policy file is approved by its SHA-256 at construction. It cannot
/// change during this process. Agent requests are still intersected with it.
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Policy {
    pub owner: EngineSourceOwner,
    pub source_revision: String,
    pub permissions: crate::permissions::RequestedPermissions,
    pub read_only: bool,
}
fn digest(bytes: impl AsRef<[u8]>) -> String {
    format!("{:x}", Sha256::digest(bytes.as_ref()))
}
fn io_error(_: std::io::Error) -> EngineFailure {
    failure(
        "SOURCE_UNAVAILABLE",
        "host",
        "Standalone input or storage is unavailable",
    )
}
fn read(path: &Path) -> Result<Vec<u8>> {
    // A source is a regular file, never a device, directory or unbounded stream.
    let meta = std::fs::symlink_metadata(path).map_err(io_error)?;
    if !meta.file_type().is_file() || meta.len() > 8 * 1024 * 1024 {
        return Err(failure(
            "SOURCE_UNAVAILABLE",
            "host",
            "Expected a regular input file of at most 8 MiB",
        ));
    }
    let mut bytes = Vec::new();
    File::open(path)
        .map_err(io_error)?
        .take(8 * 1024 * 1024 + 1)
        .read_to_end(&mut bytes)
        .map_err(io_error)?;
    if bytes.len() > 8 * 1024 * 1024 {
        return Err(failure("INPUT_LIMIT", "host", "Input grew beyond 8 MiB"));
    }
    Ok(bytes)
}
struct Inputs {
    workspace: PathBuf,
    scope: Value,
    source_path: PathBuf,
    opened: OpenedSource,
    retained: RetainedSource,
    source: EngineSourceRevision,
    policy_path: PathBuf,
    policy_bytes: Vec<u8>,
    policy: Policy,
    policy_digest: String,
}
impl Inputs {
    fn verify(&self) -> Result<()> {
        if (self.opened.is_document()
            && (read(&self.source_path)? != self.opened.authored_json().as_bytes()
                || self.source_path.canonicalize().map_err(io_error)?
                    != self.retained.document_path()))
            || read(&self.policy_path)? != self.policy_bytes
            || scope(&self.workspace, &self.policy.owner, &self.retained)? != self.scope
        {
            return Err(failure(
                "CAPTURE_CHANGED",
                "source",
                "Source, approved policy or workspace identity changed",
            ));
        }
        Ok(())
    }
    fn permissions(&self) -> Value {
        // No Macro tools are offered. Native Codex uses its captured sandbox,
        // the approved ceiling and the engine-owned human approval protocol.
        json!({"mode":"Implement", "readOnly":self.policy.read_only,
            "allowedToolIds":[], "riskLevel":"strict", "agsdlHostPermissions":self.policy.permissions})
    }
}
fn scope(root: &Path, owner: &EngineSourceOwner, source: &RetainedSource) -> Result<Value> {
    let root = root.canonicalize().map_err(io_error)?;
    let identity = crate::content_io::admitted::workspace_root_identity(&root)
        .map_err(|_| {
            failure(
                "SOURCE_UNAVAILABLE",
                "scope",
                "Workspace identity is unavailable",
            )
        })?
        .observation_key()
        .ok_or_else(|| failure("UNSUPPORTED", "scope", "Workspace identity is unqualified"))?;
    let mut scope = json!({"projectId":owner.project_id,"root":root,"rootIdentity":identity,
        "sourceOwner":owner});
    source.bind_scope(&mut scope)?;
    Ok(scope)
}
/// Compare the project snapshot recorded by a Start with the current scope.
/// Captures without a scope or snapshot belong to a JSON document.
fn snapshot_matches(private_capture_json: &str, scope: &Value) -> Result<()> {
    let private: Value = serde_json::from_str(private_capture_json)?;
    let captured = match private["callScopeJson"].as_str() {
        Some(scope) => serde_json::from_str::<Value>(scope)?["projectSnapshot"].take(),
        None => Value::Null,
    };
    if captured != scope["projectSnapshot"] {
        return Err(failure(
            "CAPTURE_CHANGED",
            "source",
            "Start belongs to a different project snapshot",
        ));
    }
    Ok(())
}
#[derive(Clone)]
struct Policies(Arc<RwLock<Inputs>>);
impl Policies {
    async fn acquire(&self) -> Result<OwnedRwLockReadGuard<Inputs>> {
        let guard = self.0.clone().read_owned().await;
        guard.verify()?;
        Ok(guard)
    }
}
impl PolicySource for Policies {
    fn lock_project<'a>(&'a self, project: &'a str) -> PolicyFuture<'a, ProjectPolicyLease> {
        Box::pin(async move {
            let guard = self.acquire().await?;
            if project != guard.policy.owner.project_id {
                return Err(failure(
                    "PERMISSION",
                    "project",
                    "Project is outside the approved policy",
                ));
            }
            Ok(ProjectPolicyLease::new(
                json!({"riskLevel":"strict"}),
                guard.policy.permissions.clone(),
                guard,
            ))
        })
    }
    fn lock_ceiling(&self) -> PolicyFuture<'_, HostPolicyLease> {
        Box::pin(async move {
            let guard = self.acquire().await?;
            Ok(HostPolicyLease::new(
                guard.policy.permissions.clone(),
                guard.policy_digest.clone(),
                guard,
            ))
        })
    }
}
impl ActionPolicySource for Policies {
    fn lock_current<'a>(
        &'a self,
        capture: &'a StartCapture030,
        payload: &'a Value,
    ) -> PolicyFuture<'a, ActionPolicyLease> {
        Box::pin(async move {
            use crate::permissions::*;
            let guard = self.acquire().await?;
            let private: Value = serde_json::from_str(&capture.private_capture_json)?;
            let captured_scope: Value = serde_json::from_str(
                private["callScopeJson"]
                    .as_str()
                    .ok_or_else(|| failure("CORRELATION", "scope", "Captured scope is absent"))?,
            )?;
            if capture.source != guard.source || captured_scope != guard.scope {
                return Err(failure(
                    "CAPTURE_CHANGED",
                    "approval",
                    "Current source or scope differs from the capture",
                ));
            }
            let policy = normalize_requested(
                guard.policy.permissions.clone(),
                PathContext {
                    workspace: guard.workspace.clone(),
                    home: std::env::var_os("HOME").map(PathBuf::from).ok_or_else(|| {
                        failure("PERMISSION", "approval", "Native home is absent")
                    })?,
                },
            )
            .map_err(|_| {
                failure(
                    "PERMISSION",
                    "approval",
                    "Approved policy cannot be normalized",
                )
            })?;
            let native = &payload["nativeRequest"];
            if native["method"] == "mcp/tool/requestApproval" {
                let params = &native["params"];
                let allowed = params["server"]
                    .as_str()
                    .zip(params["tool"].as_str())
                    .is_some_and(|(server, tool)| {
                        evaluate_requested(
                            &policy,
                            &ActionRequest::Mcp {
                                server: server.into(),
                                tool: Some(tool.into()),
                            },
                        )
                        .is_ok_and(|effect| effect != Effect::Deny)
                    });
                return Ok(ActionPolicyLease::new(allowed, guard));
            }
            let path = payload
                .pointer("/nativeRequest/params/additionalPermissions/fileSystem/write/0")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    failure("CORRELATION", "approval", "Native write scope is absent")
                })?;
            let allowed = !guard.policy.read_only
                && evaluate_requested(&policy, &ActionRequest::Write { path: path.into() })
                    .is_ok_and(|e| e != Effect::Deny)
                && evaluate_requested(
                    &policy,
                    &ActionRequest::Shell {
                        command: payload["nativeRequest"]["params"]["command"]
                            .as_str()
                            .unwrap_or("")
                            .into(),
                    },
                )
                .is_ok_and(|e| e == Effect::Allow);
            Ok(ActionPolicyLease::new(allowed, guard))
        })
    }
}

// flock follows the open file description on Unix. Unlock explicitly when
// ownership ends so a descriptor inherited by a concurrent fork cannot keep it.
struct DatabaseLock(Option<File>);

impl DatabaseLock {
    fn release(&mut self) -> io::Result<()> {
        if let Some(file) = &self.0 {
            file.unlock()?;
            self.0 = None;
        }
        Ok(())
    }
}

impl Drop for DatabaseLock {
    fn drop(&mut self) {
        let _ = self.release();
    }
}

/// Owns one database lock, engine incarnation and all execution resources.
/// Keep it alive until `shutdown` succeeds, then call `close`. Dropping a value
/// is not a physical Stop receipt.
pub struct Host {
    engine: Engine,
    pool: SqlitePool,
    tasks: StreamTasks,
    policies: Policies,
    context: HostContext,
    database_lock: Mutex<DatabaseLock>,
    shutdown_complete: std::sync::atomic::AtomicBool,
}
impl Host {
    /// Native host configuration, before preflight/Start. Never a document input.
    pub fn with_harnesses(mut self, harnesses: crate::harness::HarnessRegistry030) -> Self {
        self.context = self.context.with_harnesses(harnesses);
        self
    }
    /// Configure the read-only Argentic adapter. Preflight and Start still
    /// require an explicit "argentic" entry in the allowed harness set.
    pub fn with_argentic(mut self, config: crate::harness::ArgenticConfig030) -> Result<Self> {
        let mut registry = self.context.harnesses().clone();
        registry.register(Arc::new(crate::harness::ArgenticAdapter030::new(config)?))?;
        self.context = self.context.with_harnesses(registry);
        Ok(self)
    }

    pub fn with_codex_auth_file(mut self, path: PathBuf) -> Self {
        self.context = self.context.with_codex_auth_file(path);
        self
    }
    pub fn with_mcp_secrets(mut self, provider: Arc<crate::host::McpSecretProvider>) -> Self {
        self.context = self.context.with_mcp_secrets(provider);
        self
    }
    pub fn with_mcp_http(mut self, authorize: Arc<crate::host::McpHttpAuthorizer>) -> Self {
        self.context = self.context.with_mcp_http(authorize);
        self
    }
    /// Opens only this runtime's standalone schema. The data directory must be
    /// outside the workspace. One process owns it; a second open is refused.
    /// Fences previous runs and reconciles any persisted whole-run stop.
    pub async fn open(
        data: &Path,
        workspace: &Path,
        source: &Path,
        policy: &Path,
        approved_sha256: &str,
    ) -> Result<Self> {
        let workspace = workspace.canonicalize().map_err(io_error)?;
        std::fs::create_dir_all(data).map_err(io_error)?;
        let data = data.canonicalize().map_err(io_error)?;
        if data.starts_with(&workspace) || workspace.starts_with(&data) {
            return Err(failure(
                "PERMISSION",
                "data",
                "Private host data and workspace must not overlap",
            ));
        }
        let policy_bytes = read(policy)?;
        let policy_digest = digest(&policy_bytes);
        if approved_sha256 != policy_digest {
            return Err(failure(
                "PERMISSION",
                "policy",
                "Approve the exact policy SHA-256 before opening the host",
            ));
        }
        let policy_value: Policy = serde_json::from_slice(&policy_bytes)?;
        for id in [
            &policy_value.owner.project_id,
            &policy_value.owner.plan_id,
            &policy_value.owner.branch,
        ] {
            persistence::opaque(id)?;
        }
        persistence::decimal(&policy_value.source_revision)?;
        let source_path = std::path::absolute(source).map_err(io_error)?;
        let opened = OpenedSource::open_path(&source_path)?;
        let document: Value = serde_json::from_str(opened.authored_json())?;
        let argentic_only = document["agents"].as_object().is_some_and(|agents| {
            !agents.is_empty() && agents.values().all(|agent| agent["harness"] == "argentic")
        });
        if !argentic_only {
            api::validate_codex_permissions(
                &serde_json::to_value(&policy_value.permissions)?,
                "policy.permissions",
            )?;
        }
        let lock = File::options()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(data.join("engine.lock"))
            .map_err(io_error)?;
        loop {
            match lock.try_lock() {
                Ok(()) => break,
                Err(std::fs::TryLockError::WouldBlock) => {
                    return Err(failure(
                        "RUNTIME_UNAVAILABLE",
                        "data",
                        "Standalone database is already owned",
                    ));
                }
                Err(std::fs::TryLockError::Error(error))
                    if error.kind() == io::ErrorKind::Interrupted => {}
                Err(std::fs::TryLockError::Error(error)) => return Err(io_error(error)),
            }
        }
        let lock = DatabaseLock(Some(lock));
        let retained = opened.retain(&data)?;
        let inputs = Inputs {
            scope: scope(&workspace, &policy_value.owner, &retained)?,
            workspace,
            source_path,
            source: EngineSourceRevision {
                owner: policy_value.owner.clone(),
                revision: policy_value.source_revision.clone(),
                source_sha256: opened.source_sha256(),
            },
            opened: opened.without_files(),
            retained,
            policy_path: policy.to_owned(),
            policy_bytes,
            policy: policy_value,
            policy_digest,
        };
        inputs.verify()?;
        let database = data.join("engine.sqlite");
        if std::fs::symlink_metadata(&database).is_ok_and(|m| !m.file_type().is_file()) {
            return Err(failure(
                "STORAGE",
                "data",
                "Database must be a regular file",
            ));
        }
        let pool = if database.exists() {
            crate::sqlite::open_file(&database).await?
        } else {
            crate::sqlite::create_file(&database).await?
        };
        api::interrupt_previous_runs(&pool).await?;
        let engine = Engine::new();
        crate::agsdl_host_flow030::recover_pending_stops030(&engine, &pool).await?;
        let policies = Policies(Arc::new(RwLock::new(inputs)));
        let context = HostContext::new(data, Arc::new(policies.clone()));
        Ok(Self {
            engine,
            pool,
            tasks: StreamTasks::default(),
            policies,
            context,
            database_lock: Mutex::new(lock),
            shutdown_complete: std::sync::atomic::AtomicBool::new(false),
        })
    }
    /// Returns the source revision authenticated at open, including exact bytes' digest.
    pub async fn source(&self) -> EngineSourceRevision {
        self.policies.0.read().await.source.clone()
    }
    async fn prepare(
        &self,
        entry: Option<&EngineEntry>,
        allowed_harnesses: &BTreeSet<String>,
    ) -> Result<Prepared> {
        let inputs = self.policies.acquire().await?;
        let doc: Value = serde_json::from_str(inputs.opened.authored_json())?;
        let registered = self.context.harnesses();
        let qualification = api::qualify_product_with_harnesses(
            &doc,
            None,
            Some(&inputs.policy.permissions),
            registered,
        )
        .map_err(|error| inputs.opened.locate_error(error))?;
        if let Some(d) = qualification.blocking_diagnostic() {
            return Err(inputs
                .opened
                .locate_error(failure(&d.code, &d.pointer, &d.message)));
        }
        if !doc["contract"]
            .as_str()
            .is_some_and(argentic_core::supports_contract)
        {
            return Err(failure(
                "UNSUPPORTED_EDITION",
                "/contract",
                "Standalone host requires AgSDL 0.3.0 or 0.4.0",
            ));
        }
        if api::declared_steps(&doc).any(|(_, step)| step["type"] == "call") {
            return Err(failure(
                "UNSUPPORTED",
                "source",
                "Standalone host does not support host calls",
            ));
        }
        let used: BTreeSet<_> = doc["agents"]
            .as_object()
            .into_iter()
            .flatten()
            .filter_map(|(_, agent)| agent["harness"].as_str())
            .collect();
        let mut harnesses = Vec::new();
        let mut agents = Vec::new();
        let mut observations = serde_json::Map::new();
        let mut supports_approval = true;
        for id in &used {
            let adapter = registered.get(id).expect("harness qualified above");
            if !allowed_harnesses.contains(*id) {
                return Err(adapter.opt_in_error());
            }
            supports_approval &= adapter.supports_host_approval();
        }
        let location = crate::capture::document_location(&inputs.scope)?;
        for id in used {
            let adapter = registered.get(id).expect("harness qualified above");
            adapter
                .validate_document(&doc, location.as_ref())
                .map_err(|error| inputs.opened.locate_error(error))?;
            let observation = adapter
                .observe(&self.context, &inputs.scope, inputs.policy.read_only)
                .await?;
            adapter
                .validate_observation(&doc, &inputs.scope, &observation)
                .map_err(|error| inputs.opened.locate_error(error))?;
            let (entry, entries) = crate::capture::observed_registry(
                &doc,
                &inputs.source.source_sha256,
                &inputs.scope,
                id,
                adapter.adapter_id(),
                &observation,
            )
            .map_err(|error| inputs.opened.locate_error(error))?;
            harnesses.push(entry);
            agents.extend(entries);
            observations.insert(id.to_owned(), observation.fingerprint);
        }
        let observation = match observations.len() {
            0 => Value::Null,
            1 => observations.into_iter().next().unwrap().1,
            _ => Value::Object(observations),
        };
        let modules = registered
            .iter()
            .map(|adapter| json!({"harness":adapter.id(),"moduleJson":adapter.module_json()}))
            .collect::<Vec<_>>();
        let mut registry = json!({"format":"macro-agsdl-registry/030", "modules":modules,
            "harnesses":harnesses,"agents":agents,"hostCapabilities": if supports_approval {
                json!([{"name":"approval","support":"native","evidenceJson":"{\"runtime\":\"flow-human-clock\"}"}])
            } else { json!([]) }});
        registry["version"] = json!(format!(
            "sha256:{}",
            digest(serde_json::to_vec(&(
                &registry,
                &observation,
                &inputs.scope,
                inputs.permissions()
            ))?)
        ));
        registry["startEntry"] = serde_json::to_value(entry)?;
        let compiled = self
            .engine
            .compile_document_with_harnesses(
                inputs.source.clone(),
                inputs.opened.authored_json().to_owned(),
                registry.to_string(),
                registered,
            )
            .await?;
        let mut result: Value = serde_json::from_str(&compiled.result_json)?;
        if result.pointer("/static/valid") != Some(&json!(true))
            || result.pointer("/runtime/supported") != Some(&json!(true))
        {
            if !inputs.opened.is_document() {
                if let Some(diagnostic) = ["/static/diagnostics", "/runtime/diagnostics"]
                    .into_iter()
                    .filter_map(|pointer| result.pointer(pointer).and_then(Value::as_array))
                    .flatten()
                    .find(|diagnostic| diagnostic["severity"] == "error")
                {
                    let pointer = diagnostic["pointer"]
                        .as_str()
                        .or_else(|| diagnostic["path"].as_str())
                        .unwrap_or("");
                    // Keep the JSON input's code. The message locates the first
                    // error in its component file, then carries the full
                    // compile report that a JSON source receives.
                    let message = format!(
                        "{}: {}. Compile report: {result}",
                        diagnostic["code"].as_str().unwrap_or("UNSUPPORTED"),
                        diagnostic["message"]
                            .as_str()
                            .unwrap_or("Source is unsupported")
                            .trim_end_matches('.')
                    );
                    return Err(inputs.opened.locate_error(failure(
                        "UNSUPPORTED",
                        pointer,
                        &message,
                    )));
                }
            }
            return Err(failure("UNSUPPORTED", "source", &result.to_string()));
        }
        result["mcpQualification"] = serde_json::to_value(qualification.mcp_servers)?;
        Ok(Prepared {
            result,
            observation,
            scope: inputs.scope.clone(),
            permissions: inputs.permissions(),
            generation: compiled.generation,
        })
    }
    /// Explicitly observes Codex when needed; does not admit or start any work.
    pub async fn preflight(&self, entry: Option<&EngineEntry>, allow_codex: bool) -> Result<Value> {
        self.preflight_with_harnesses(entry, &codex_opt_in(allow_codex))
            .await
    }

    pub async fn preflight_with_harnesses(
        &self,
        entry: Option<&EngineEntry>,
        allowed_harnesses: &BTreeSet<String>,
    ) -> Result<Value> {
        Ok(self.prepare(entry, allowed_harnesses).await?.result)
    }
    /// Idempotent native Start. The private adapter retains real source/policy
    /// guards and delegates ownership and commit acknowledgement to the session.
    pub async fn start(
        &self,
        request_id: String,
        entry: Option<EngineEntry>,
        allow_codex: bool,
    ) -> Result<RetainedStart030> {
        self.start_with_harnesses(request_id, entry, &codex_opt_in(allow_codex))
            .await
    }

    pub async fn start_with_harnesses(
        &self,
        request_id: String,
        entry: Option<EngineEntry>,
        allowed_harnesses: &BTreeSet<String>,
    ) -> Result<RetainedStart030> {
        let session = self.engine.begin_start().await?;
        let request = EngineStartRequest {
            source: self.source().await,
            profile: "macro-agsdl-030-v1".into(),
            associations: EngineAssociations::default(),
            request_id,
            entry,
        };
        let adapter = Start {
            host: self,
            session: &session,
            prepared: Mutex::new(None),
            capture: Mutex::new(None),
            allowed_harnesses,
        };
        let retained = match start030::start(&self.pool, &request, &adapter).await {
            Ok(r) => r,
            Err(e) => {
                if let Some(c) = adapter
                    .capture
                    .lock()
                    .map_err(|_| failure("RUNTIME_UNAVAILABLE", "start", "Capture unavailable"))?
                    .as_ref()
                {
                    session.fail_reservation(&c.identity.system_start_id, e.clone());
                }
                return Err(e);
            }
        };
        // A replay must name the snapshot this host retained, or none for a
        // JSON document. A fresh start that fails here is released.
        let matched = snapshot_matches(
            &retained.capture.private_capture_json,
            &self.policies.0.read().await.scope,
        );
        let fresh = adapter
            .capture
            .lock()
            .map_err(|_| failure("RUNTIME_UNAVAILABLE", "start", "Capture unavailable"))?
            .is_some();
        if let Err(e) = matched {
            if fresh {
                // Release the reservation even if interruption fails.
                let id = &retained.receipt.identity.system_start_id;
                let interrupted = session.interrupt_run(&self.pool, id).await;
                session.fail_reservation(id, e.clone());
                interrupted?;
            }
            return Err(e);
        }
        if fresh {
            if let Err(e) = session
                .attach_committed_start(
                    &self.pool,
                    self.context.clone(),
                    self.tasks.clone(),
                    &retained.receipt.identity,
                    &retained.capture,
                    Arc::new(self.policies.clone()),
                )
                .await
            {
                let id = &retained.receipt.identity.system_start_id;
                let interrupted = session.interrupt_run(&self.pool, id).await;
                session.fail_reservation(id, e.clone());
                interrupted?;
                return Err(e);
            }
        }
        Ok(retained)
    }
    /// Captured source identities and receipts, including interrupted earlier runs.
    pub async fn runs(&self) -> Result<Vec<Value>> {
        let rows: Vec<(String, String, bool)> = sqlx::query_as("SELECT s.receipt_json,s.capture_json,EXISTS(SELECT 1 FROM engine_flow030_interruptions i WHERE i.system_id=s.system_id) FROM engine_start030_requests s ORDER BY s.request_id")
            .fetch_all(&self.pool).await?;
        rows.into_iter().map(|(receipt,capture,interrupted)| {
            let capture:StartCapture030=serde_json::from_str(&capture)?;
            Ok(json!({"receipt":serde_json::from_str::<Value>(&receipt)?,"source":capture.source,"interrupted":interrupted}))
        }).collect()
    }
    pub async fn projection(
        &self,
        query: api::RuntimeProjectionQuery030,
        next: Option<api::RuntimeProjectionContinuation030>,
    ) -> Result<api::RuntimeProjectionPage030> {
        self.engine
            .runtime_projection(&self.pool, query, next)
            .await
    }
    pub async fn humans(
        &self,
        scope: persistence::public030::HumanScope030,
        next: Option<persistence::public030::HumanContinuation030>,
    ) -> Result<persistence::public030::HumanPage030> {
        self.engine.human_page(&self.pool, scope, next).await
    }
    pub async fn submit_person(&self, answer: HumanSubmission) -> Result<String> {
        self.engine.submit_person(&self.pool, answer).await
    }
    pub async fn control_run(
        &self,
        input: api::OperatorRequest,
    ) -> Result<persistence::flow030::FlowReceipt030> {
        self.engine.control_run(&self.pool, input).await
    }
    pub async fn deliver(&self, input: api::DeliveryRequest030) -> Result<api::DeliveryReceipt030> {
        self.engine.deliver_message(&self.pool, input).await
    }
    pub async fn stop(
        &self,
        request: EngineStopRequest,
    ) -> Result<persistence::public::PublicStopState> {
        self.engine
            .request_stop(&self.pool, &self.tasks, &request)
            .await
    }
    pub async fn active_runs(&self) -> Result<Vec<String>> {
        self.engine.shutdown_active_runs(&self.pool).await
    }
    pub fn shutdown_started(&self) -> bool {
        self.engine.shutdown_started()
    }
    /// Consent only limits retirement. A timeout or error does not reopen Starts.
    pub async fn shutdown(&self, approved: &[String]) -> Result<()> {
        self.engine
            .prepare_shutdown(&self.pool, &self.tasks, approved)
            .await?;
        self.shutdown_complete
            .store(true, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
    /// Releases storage only after real retirement. Failure retains the host so
    /// the caller can inspect and retry instead of discarding physical owners.
    pub async fn close(&self) -> Result<()> {
        if !self
            .shutdown_complete
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            return Err(failure(
                "SHUTDOWN_PENDING",
                "host",
                "Complete native shutdown before closing storage",
            ));
        }
        self.pool.close().await;
        self.database_lock
            .lock()
            .expect("database ownership lock")
            .release()
            .map_err(io_error)
    }
}
struct Prepared {
    result: Value,
    observation: Value,
    scope: Value,
    permissions: Value,
    generation: String,
}
fn codex_opt_in(allowed: bool) -> BTreeSet<String> {
    allowed.then(|| "codex".into()).into_iter().collect()
}

struct Start<'a> {
    host: &'a Host,
    session: &'a StartSession<'a>,
    prepared: Mutex<Option<Prepared>>,
    capture: Mutex<Option<StartCapture030>>,
    allowed_harnesses: &'a BTreeSet<String>,
}
impl Authority030 for Start<'_> {
    fn owner<'a>(
        &'a self,
        db: &'a mut SqliteConnection,
        id: &'a FlowIdentity030,
    ) -> CheckFuture<'a> {
        Box::pin(async move {
            let capture = self
                .capture
                .lock()
                .map_err(|_| StoreError030::Authority)?
                .clone();
            self.session.check_owner_tx(db, id, capture.as_ref()).await
        })
    }
    fn effect<'a>(&'a self, _: &'a mut SqliteConnection, _: &'a FlowEffect030) -> CheckFuture<'a> {
        Box::pin(async { Err(StoreError030::Authority) })
    }
    fn settlement<'a>(
        &'a self,
        _: &'a mut SqliteConnection,
        _: &'a FlowEffect030,
        _: &'a Value,
    ) -> CheckFuture<'a> {
        Box::pin(async { Err(StoreError030::Authority) })
    }
    fn owner_epoch(&self) -> &str {
        "0"
    }
}
impl StartAuthority030 for Start<'_> {
    type SourceGuard = (
        OwnedRwLockReadGuard<Inputs>,
        tokio::sync::OwnedMutexGuard<()>,
    );
    fn require_clear(&self, r: &EngineStartRequest) -> Result<()> {
        self.session.require_clear(r)
    }
    fn begin_commit(&self, r: &EngineStartRequest, id: &FlowIdentity030) -> Result<()> {
        self.session.begin_commit(r, id, true)
    }
    fn commit_acknowledged(&self, r: &EngineStartRequest, id: &FlowIdentity030) {
        self.session.commit_acknowledged(r, id)
    }
    async fn capture(&self, r: &EngineStartRequest) -> Result<StartCapture030> {
        let p = self
            .host
            .prepare(r.entry.as_ref(), self.allowed_harnesses)
            .await?;
        let inputs = self.host.policies.acquire().await?;
        if r.source != inputs.source || p.generation != self.session.generation() {
            return Err(failure(
                "CAPTURE_CHANGED",
                "source",
                "Source or engine changed",
            ));
        }
        let plan = p.result["compiledPlanJson"]
            .as_str()
            .ok_or_else(|| failure("CORRELATION", "plan", "Compiled plan absent"))?;
        let plan_value: Value = serde_json::from_str(plan)?;
        let capture=StartCapture030 {
            identity:FlowIdentity030 {system_start_id:uuid::Uuid::new_v4().to_string(),flow_invocation_id:uuid::Uuid::new_v4().to_string(),snapshot_id:uuid::Uuid::new_v4().to_string(),epoch:uuid::Uuid::new_v4().to_string()},
            source:inputs.source.clone(),versions:serde_json::from_value(plan_value["versions"].clone())?,authored_source:inputs.opened.authored_json().to_owned(),compiled_plan_json:plan.into(),
            registry_json:p.result["capturedRegistryJson"].as_str().ok_or_else(||failure("CORRELATION","registry","Captured registry absent"))?.into(),
            private_capture_json:json!({"format":"macro-native-flow-capture/030","generation":p.generation,
                "callScopeJson":p.scope.to_string(),"callPermissionsJson":p.permissions.to_string(),"nativeCatalogueJson":p.observation.to_string(),
                "displayCardsJson":"[]","associations":r.associations,"entryUnassessed":p.result["entryValidation"]["unassessed"]}).to_string(),
        };
        start030::validate_capture(r, &capture)?;
        *self
            .capture
            .lock()
            .map_err(|_| failure("RUNTIME_UNAVAILABLE", "start", "Capture unavailable"))? =
            Some(capture.clone());
        *self
            .prepared
            .lock()
            .map_err(|_| failure("RUNTIME_UNAVAILABLE", "start", "Preparation unavailable"))? =
            Some(p);
        Ok(capture)
    }
    async fn lock_source(&self, r: &EngineStartRequest) -> Result<Self::SourceGuard> {
        let current = self
            .host
            .prepare(r.entry.as_ref(), self.allowed_harnesses)
            .await?;
        let guard = self.host.policies.acquire().await?;
        {
            let old = self
                .prepared
                .lock()
                .map_err(|_| failure("RUNTIME_UNAVAILABLE", "start", "Preparation unavailable"))?;
            let old = old
                .as_ref()
                .ok_or_else(|| failure("CORRELATION", "start", "Preparation absent"))?;
            if current.result != old.result
                || current.observation != old.observation
                || current.scope != old.scope
                || current.permissions != old.permissions
                || current.generation != old.generation
            {
                return Err(failure(
                    "CAPTURE_CHANGED",
                    "source",
                    "Native capture changed before commit",
                ));
            }
        }
        let admission = self.session.lock_admission().await;
        self.session.require_clear(r)?;
        Ok((guard, admission))
    }
    async fn bind_capture_tx(
        &self,
        db: &mut SqliteConnection,
        _: &EngineStartRequest,
        c: &StartCapture030,
    ) -> Result<()> {
        self.session.bind_capture_tx(db, c).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replay_needs_the_snapshot_of_the_current_host() {
        let private = |scope: Option<Value>| {
            match scope {
                Some(scope) => json!({"callScopeJson":scope.to_string()}),
                None => json!({}),
            }
            .to_string()
        };
        let project = json!({"root":"/w","projectSnapshot":"a"});
        let document = json!({"root":"/w"});
        snapshot_matches(&private(Some(project.clone())), &project).unwrap();
        snapshot_matches(&private(Some(document.clone())), &document).unwrap();
        // Captures from before project support carry no snapshot.
        snapshot_matches(&private(None), &document).unwrap();
        for (captured, current) in [
            (Some(project.clone()), document.clone()),
            (Some(document), project.clone()),
            (
                Some(json!({"root":"/w","projectSnapshot":"b"})),
                project.clone(),
            ),
            (None, project),
        ] {
            assert_eq!(
                snapshot_matches(&private(captured), &current)
                    .unwrap_err()
                    .code,
                "CAPTURE_CHANGED"
            );
        }
    }
    struct Fixture {
        _temp: tempfile::TempDir,
        data: PathBuf,
        workspace: PathBuf,
        source: PathBuf,
        policy: PathBuf,
        digest: String,
    }
    impl Fixture {
        fn new(source: &str) -> Self {
            let temp = tempfile::tempdir().unwrap();
            let root = temp.path();
            let workspace = root.join("workspace");
            std::fs::create_dir(&workspace).unwrap();
            let source_path = root.join("source.json");
            std::fs::write(&source_path, source).unwrap();
            let policy = root.join("policy.json");
            let bytes=br#"{"owner":{"projectId":"project","planId":"plan","branch":"standalone"},"sourceRevision":"1","permissions":"workspace","readOnly":false}"#;
            std::fs::write(&policy, bytes).unwrap();
            let data = root.join("data");
            Self {
                _temp: temp,
                data,
                workspace,
                source: source_path,
                policy,
                digest: digest(bytes),
            }
        }
        async fn open(&self) -> Result<Host> {
            Host::open(
                &self.data,
                &self.workspace,
                &self.source,
                &self.policy,
                &self.digest,
            )
            .await
        }
    }
    mod argentic_tests;

    const WAIT: &str = r#"{"contract":"agsdl-0.3.0","id":"standalone","agents":{},"flow":{"entry":"wait","steps":{"wait":{"type":"approval","title":"Continue?","timeoutMs":86400000,"next":{"yes":[],"no":[]}}}}}"#;
    #[tokio::test]
    async fn captures_the_source_location_independently_of_the_workspace() {
        let mut f = Fixture::new(WAIT);
        let directory = f.workspace.join("plans");
        std::fs::create_dir(&directory).unwrap();
        let source = directory.join("source with spaces.json");
        let mission = directory.join("mission.md");
        std::fs::rename(&f.source, &source).unwrap();
        std::fs::write(&mission, "Mission").unwrap();
        f.source = source;
        let host = f.open().await.unwrap();
        {
            let guard = host.policies.acquire().await.unwrap();
            let location = crate::capture::document_location(&guard.scope)
                .unwrap()
                .unwrap();
            assert_eq!(
                location.to_file_path().unwrap().canonicalize().unwrap(),
                f.source.canonicalize().unwrap()
            );
            assert_eq!(
                crate::content::local_file_path("mission.md", Some(&location))
                    .unwrap()
                    .canonicalize()
                    .unwrap(),
                mission.canonicalize().unwrap()
            );
        }
        host.shutdown(&[]).await.unwrap();
        host.close().await.unwrap();
    }

    fn entry() -> Option<EngineEntry> {
        Some(EngineEntry {
            delivery_id: "entry".into(),
            message_json: "{}".into(),
        })
    }
    #[test]
    fn operational_write_review_fixture_stays_inside_the_existing_codex_slice() {
        let raw = include_str!("../examples/fixtures/write-review.json");
        let doc: Value = serde_json::from_str(raw).unwrap();
        api::validate_codex_document(&doc).unwrap();
        assert!(api::product_slice(raw, None).is_none());
        let _: Policy =
            serde_json::from_str(include_str!("../examples/fixtures/policy.json")).unwrap();
    }
    #[tokio::test]
    async fn approved_inputs_lock_storage_and_refuse_mutation_before_any_provider() {
        let f = Fixture::new(WAIT);
        assert_eq!(
            Host::open(&f.data, &f.workspace, &f.source, &f.policy, "unapproved")
                .await
                .err()
                .unwrap()
                .code,
            "PERMISSION"
        );
        assert!(!f.data.join("engine.sqlite").exists());
        let host = f.open().await.unwrap();
        assert_eq!(f.open().await.err().unwrap().code, "RUNTIME_UNAVAILABLE");
        assert!(host.close().await.is_err());
        std::fs::write(&f.source, format!("{WAIT} ")).unwrap();
        assert_eq!(
            host.preflight(entry().as_ref(), false)
                .await
                .unwrap_err()
                .code,
            "CAPTURE_CHANGED"
        );
        std::fs::write(&f.source, WAIT).unwrap();
        std::fs::write(&f.policy, "{}").unwrap();
        assert_eq!(
            host.start("changed".into(), entry(), false)
                .await
                .err()
                .unwrap()
                .code,
            "CAPTURE_CHANGED"
        );
        host.shutdown(&[]).await.unwrap();
        host.close().await.unwrap();
    }
    #[tokio::test]
    async fn codex_requires_explicit_opt_in_and_other_harnesses_and_calls_stay_refused() {
        for (source, expected) in [
            (
                r#"{"contract":"agsdl-0.3.0","id":"optin","agents":{"worker":{"harness":"codex","permissions":"workspace"}},"flow":{"entry":"work","steps":{"work":{"type":"agent","agent":"worker","next":[]}}}}"#,
                "CODEX_OPT_IN_REQUIRED",
            ),
            (
                r#"{"contract":"agsdl-0.3.0","id":"other","agents":{"worker":{"harness":"pi"}},"flow":{"entry":"work","steps":{"work":{"type":"agent","agent":"worker"}}}}"#,
                "UNSUPPORTED_030_FEATURE",
            ),
            (
                r#"{"contract":"agsdl-0.3.0","id":"call","agents":{},"flow":{"entry":"call","steps":{"call":{"type":"call","command":"true"}}}}"#,
                "UNSUPPORTED",
            ),
        ] {
            let f = Fixture::new(source);
            let host = f.open().await.unwrap();
            assert_eq!(
                host.preflight(entry().as_ref(), false)
                    .await
                    .unwrap_err()
                    .code,
                expected
            );
            assert!(host.runs().await.unwrap().is_empty());
            host.shutdown(&[]).await.unwrap();
            host.close().await.unwrap();
        }
    }
    #[derive(Clone)]
    struct RoutingAdapter {
        id: &'static str,
        adapter_id: &'static str,
        module: String,
        runs: Arc<Mutex<Vec<(String, String)>>>,
    }

    impl crate::harness::HarnessAdapter030 for RoutingAdapter {
        fn id(&self) -> &str {
            self.id
        }
        fn adapter_id(&self) -> &str {
            self.adapter_id
        }
        fn module_json(&self) -> &str {
            &self.module
        }

        fn observe<'a>(
            &'a self,
            _: &'a HostContext,
            _: &'a Value,
            _: bool,
        ) -> futures::future::BoxFuture<'a, Result<crate::harness::HarnessObservation030>> {
            Box::pin(async move {
                let mut observed = crate::agsdl_host_codex030::registration::observation(
                    &api::synthetic_codex_observation(),
                );
                if self.id == "argentic" {
                    observed.version = "0.1.0".into();
                    observed.evidence = json!({"kind":"routing-test"});
                    for capability in &mut observed.capabilities {
                        capability["evidenceJson"] = json!(observed.evidence.to_string());
                    }
                }
                Ok(observed)
            })
        }

        fn executor(
            &self,
            _: crate::harness::AgentExecutorContext030<'_>,
        ) -> Result<Arc<dyn crate::harness::AgentExecutor030>> {
            Ok(Arc::new(self.clone()))
        }
    }

    impl crate::harness::AgentExecutor030 for RoutingAdapter {
        fn prepare(
            &self,
            work: crate::harness::PreparedEffectiveAgentWork030,
        ) -> Result<crate::harness::AttestedWorkPolicy030> {
            assert_eq!(work.harness(), self.id);
            if self.id == "codex" {
                return crate::agsdl_host_codex030::admission::prepare(work);
            }
            crate::harness::AttestedWorkPolicy030::new(
                work,
                self.adapter_id,
                1000.try_into().unwrap(),
                1024.try_into().unwrap(),
                false,
            )
        }

        fn run(
            &self,
            work: crate::harness::AgentWork030,
            events: crate::harness::WorkEventSink030,
            cancel: tokio_util::sync::CancellationToken,
        ) -> futures::future::BoxFuture<'static, Result<()>> {
            self.runs
                .lock()
                .unwrap()
                .push((self.id.into(), work.attempt.agent().into()));
            Box::pin(async move {
                events
                    .emit(
                        json!({"kind":"submit","callId":"result","argumentsJson":"{}"}),
                        &cancel,
                    )
                    .await?;
                Ok(())
            })
        }
    }

    #[tokio::test]
    async fn standalone_routes_mixed_harnesses_and_requires_each_opt_in() {
        let fixture = Fixture::new(
            r#"{
            "contract":"agsdl-0.4.0","id":"mixed",
            "agents":{
                "writer":{"harness":"argentic","permissions":"workspace"},
                "reviewer":{"harness":"codex","permissions":"workspace"}
            },
            "flow":{"entry":"draft","steps":{
                "draft":{"type":"agent","agent":"writer","next":"review"},
                "review":{"type":"agent","agent":"reviewer","next":[]}
            }}
        }"#,
        );
        let host = fixture.open().await.unwrap();
        let mut allowed = BTreeSet::from(["codex".into(), "argentic".into()]);
        let error = host
            .preflight_with_harnesses(entry().as_ref(), &allowed)
            .await
            .unwrap_err();
        assert_eq!(error.code, "UNSUPPORTED_030_FEATURE");
        assert_eq!(error.pointer, "/agents/writer/harness");
        assert!(error
            .message
            .contains("No executor is registered for harness argentic"));

        let mut registry = crate::harness::HarnessRegistry030::default();
        let runs = Arc::new(Mutex::new(Vec::new()));
        for (id, adapter_id) in [("codex", api::CODEX_ADAPTER), ("argentic", "routing-test")] {
            let module = if id == "codex" {
                api::codex_module()
            } else {
                let mut module: Value =
                    serde_json::from_str(argentic_core::harness_module(id).unwrap()).unwrap();
                // This test executor has no external effects. Describe the
                // restrictions it enforces locally, in its own test module.
                module["permissions"]["write"]["deny"] = json!("harness");
                module["permissions"]["network"]["deny"] = json!("harness");
                module["permissions"]["read"]["outside"] = json!("harness");
                module.to_string()
            };
            registry
                .register(Arc::new(RoutingAdapter {
                    id,
                    adapter_id,
                    module,
                    runs: runs.clone(),
                }))
                .unwrap();
        }
        let host = host.with_harnesses(registry);
        allowed.remove("argentic");
        let error = host
            .preflight_with_harnesses(entry().as_ref(), &allowed)
            .await
            .unwrap_err();
        assert_eq!(error.code, "HARNESS_OPT_IN_REQUIRED");
        assert!(error.message.contains("argentic"));
        assert!(runs.lock().unwrap().is_empty());

        allowed.insert("argentic".into());
        let preflight = host
            .preflight_with_harnesses(entry().as_ref(), &allowed)
            .await
            .unwrap();
        assert_eq!(
            preflight.pointer("/productRuntimeAvailable"),
            Some(&json!(true))
        );
        let started = host
            .start_with_harnesses("mixed".into(), entry(), &allowed)
            .await
            .unwrap();
        tokio::time::timeout(
            std::time::Duration::from_secs(10),
            host.engine
                .flow030()
                .supervisors
                .wait(&started.receipt.identity.system_start_id),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            *runs.lock().unwrap(),
            [
                ("argentic".into(), "writer".into()),
                ("codex".into(), "reviewer".into())
            ]
        );
        host.shutdown(&[]).await.unwrap();
        host.close().await.unwrap();
    }

    #[tokio::test]
    async fn standalone_real_start_human_submission_stop_and_reopen() {
        let mut document: Value = serde_json::from_str(WAIT).unwrap();
        document["mcpServers"] = json!({"unused":{"url":"http://example.invalid/mcp"}});
        let f = Fixture::new(&document.to_string());
        let host = f.open().await.unwrap();
        let result = host.preflight(entry().as_ref(), false).await.unwrap();
        assert_eq!(result["runtime"]["supported"], true);
        let unused = &result["mcpQualification"]["unused"];
        assert_eq!(unused["diagnostic"]["code"], "MCP_ENDPOINT_UNSUPPORTED");
        assert_eq!(unused["directCallSteps"], json!([]));
        assert_eq!(unused["agentEquipment"], json!([]));
        let started = host.start("start".into(), entry(), false).await.unwrap();
        let replay = host.start("start".into(), entry(), false).await.unwrap();
        assert_eq!(started.receipt, replay.receipt);
        let id = started.receipt.identity.system_start_id.clone();
        let scope = persistence::public030::HumanScope030 {
            system_start_id: id.clone(),
            source: started.capture.source.clone(),
        };
        let page = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let page = host.humans(scope.clone(), None).await.unwrap();
                if !page.requests.is_empty() {
                    break page;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let request = &page.requests[0];
        let stale = HumanSubmission {
            scope: scope.clone(),
            request_id: request.id.clone(),
            expected_revision: "foreign".into(),
            answer: api::HumanAnswer030::Approval {
                choice: "yes".into(),
                comment: String::new(),
            },
        };
        assert!(host.submit_person(stale).await.is_err());
        host.submit_person(HumanSubmission {
            scope: scope.clone(),
            request_id: request.id.clone(),
            expected_revision: request.revision.clone(),
            answer: api::HumanAnswer030::Approval {
                choice: "yes".into(),
                comment: "Reviewed".into(),
            },
        })
        .await
        .unwrap();
        host.stop(EngineStopRequest {
            system_start_id: id.clone(),
            request_id: "stop".into(),
            activation_id: None,
        })
        .await
        .unwrap();
        let query = api::RuntimeProjectionQuery030 {
            system_start_id: id,
            source: scope.source,
            snapshot_id: None,
        };
        assert!(host
            .projection(query.clone(), None)
            .await
            .unwrap()
            .verdict
            .is_some());
        let approved = host.active_runs().await.unwrap();
        host.shutdown(&approved).await.unwrap();
        assert_eq!(
            host.start("later".into(), entry(), false)
                .await
                .err()
                .unwrap()
                .code,
            "RUNTIME_UNAVAILABLE"
        );
        host.close().await.unwrap();
        drop(host);
        let reopened = f.open().await.unwrap();
        assert_eq!(reopened.runs().await.unwrap().len(), 1);
        assert!(reopened
            .projection(query, None)
            .await
            .unwrap()
            .verdict
            .is_some());
        assert!(reopened.active_runs().await.unwrap().is_empty());
        reopened.shutdown(&[]).await.unwrap();
        reopened.close().await.unwrap();
    }
    #[tokio::test]
    async fn standalone_040_stop_waits_for_physical_end_and_reopens() {
        let f = Fixture::new(&WAIT.replace("agsdl-0.3.0", argentic_core::AGSDL_040));
        let host = f.open().await.unwrap();
        let started = host
            .start("start-040".into(), entry(), false)
            .await
            .unwrap();
        assert_eq!(started.capture.versions.contract, argentic_core::AGSDL_040);
        let id = started.receipt.identity.system_start_id;
        let source = started.capture.source;
        let scope = persistence::public030::HumanScope030 {
            system_start_id: id.clone(),
            source: source.clone(),
        };
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while host
                .humans(scope.clone(), None)
                .await
                .unwrap()
                .requests
                .is_empty()
            {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let stop = host
            .stop(EngineStopRequest {
                request_id: "stop-040".into(),
                system_start_id: id.clone(),
                activation_id: None,
            })
            .await
            .unwrap();
        assert!(matches!(
            stop,
            persistence::public::PublicStopState::Requested { .. }
        ));
        let query = api::RuntimeProjectionQuery030 {
            system_start_id: id,
            source,
            snapshot_id: None,
        };
        let cancelled = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let page = host.projection(query.clone(), None).await.unwrap();
                if let Some(verdict) = page.verdict {
                    break verdict;
                }
                assert!(page.stopping);
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(cancelled, "cancelled");
        host.shutdown(&[]).await.unwrap();
        host.close().await.unwrap();
        drop(host);
        let host = f.open().await.unwrap();
        assert_eq!(
            host.projection(query, None)
                .await
                .unwrap()
                .verdict
                .as_deref(),
            Some("cancelled")
        );
        host.shutdown(&[]).await.unwrap();
        host.close().await.unwrap();
    }

    #[tokio::test]
    async fn reopening_finishes_a_committed_stop_of_a_local_approval() {
        const CHILD: &str = "ARGENTIC_TEST_STOPPED_APPROVAL_ROOT";
        if let Some(root) = std::env::var_os(CHILD) {
            let root = PathBuf::from(root);
            let policy = root.join("policy.json");
            let host = Host::open(
                &root.join("data"),
                &root.join("workspace"),
                &root.join("source.json"),
                &policy,
                &digest(std::fs::read(&policy).unwrap()),
            )
            .await
            .unwrap();
            // Stop can commit after the approval ticket is reserved while
            // evaluation of its clock output is still in flight.
            let lease = host.engine.start_lease().await.unwrap();
            lease
                .clock_jobs030()
                .approval_check_probe(Arc::new(|job, _| {
                    let job: Value = serde_json::from_str(job).unwrap();
                    let input: Value =
                        serde_json::from_str(job["inputJson"].as_str().unwrap()).unwrap();
                    if input["operation"] != "humanClock" {
                        return;
                    }
                    let event: Value =
                        serde_json::from_str(input["input"]["eventJson"].as_str().unwrap())
                            .unwrap();
                    if event["event"]["kind"] == "approval" {
                        loop {
                            std::thread::park();
                        }
                    }
                }));
            drop(lease);
            let started = host.start("start".into(), entry(), false).await.unwrap();
            let scope = persistence::public030::HumanScope030 {
                system_start_id: started.receipt.identity.system_start_id,
                source: started.capture.source,
            };
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                while host
                    .humans(scope.clone(), None)
                    .await
                    .unwrap()
                    .requests
                    .is_empty()
                {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
            let hook = host
                .engine
                .flow030()
                .install_system_stop_commit_hook("stop");
            tokio::select! {
                _ = hook.reached() => std::process::exit(0),
                result = host.stop(EngineStopRequest {
                    request_id: "stop".into(),
                    system_start_id: scope.system_start_id,
                    activation_id: None,
                }) => panic!("Stop reached its reply before the commit hook: {result:?}"),
            }
        }
        let f = Fixture::new(&WAIT.replace("agsdl-0.3.0", argentic_core::AGSDL_040));
        let child = tokio::time::timeout(
            std::time::Duration::from_secs(15),
            tokio::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "standalone::tests::reopening_finishes_a_committed_stop_of_a_local_approval",
                ])
                .env(CHILD, f._temp.path())
                .kill_on_drop(true)
                .output(),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(child.status.success(), "{child:?}");
        for reopening in 0..2 {
            let host = f.open().await.unwrap();
            let runs = host.runs().await.unwrap();
            let query = api::RuntimeProjectionQuery030 {
                system_start_id: runs[0]["receipt"]["identity"]["systemStartId"]
                    .as_str()
                    .unwrap()
                    .into(),
                source: host.source().await,
                snapshot_id: None,
            };
            let page = host.projection(query.clone(), None).await.unwrap();
            assert_eq!(page.verdict.as_deref(), Some("cancelled"));
            assert!(!page.stopping && !page.termination_uncertain);
            let pending: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM engine_flow030_clock_events WHERE output_json IS NULL",
            )
            .fetch_one(&host.pool)
            .await
            .unwrap();
            assert_eq!(pending, 1);
            host.shutdown(&[]).await.unwrap();
            host.close().await.unwrap();
            let replay = persistence::replay_verify030::verify_path030(
                &f.data.join("engine.sqlite"),
                &query.system_start_id,
                &page.flow_invocation_id,
            )
            .await;
            assert!(replay.ok, "{replay:?}");
            if reopening == 1 {
                for (index, (mutation, expected)) in [
                    ("UPDATE engine_flow030_outbox
                      SET settlement_json=json_set(settlement_json,'$.kind','nativeJoinedGroupStop030')
                      WHERE settlement_json ->> '$.kind'='nativeReopenedApprovalStop030'",
                     "joinedStop.workAttempt"),
                    ("UPDATE engine_flow030_clock_events SET ticket_json=json_remove(ticket_json,'$.optionsJson')",
                     "human.readback"),
                    ("UPDATE engine_flow030_clock_events SET ticket_json=json_set(ticket_json,'$.eventJson',
                       json_set(ticket_json ->> '$.eventJson','$.event.choices',json('[\"other\"]')) || '')",
                     "human.readback"),
                    ("UPDATE engine_flow030_clock SET receipt_json='{\"unexpected\":true}'",
                     "human.readback"),
                    ("UPDATE engine_flow030_clock SET observed_ms=(SELECT
                       CAST(json_extract(ticket_json ->> '$.eventJson','$.nowMs') AS INTEGER) + 1
                       FROM engine_flow030_clock_events)",
                     "human.readback"),
                    ("UPDATE engine_flow030_clock SET observed_ms=X'FF'",
                     "human.readback"),
                ].into_iter().enumerate() {
                    let copy = f.data.join(format!("wrong-replay-{index}.sqlite"));
                    std::fs::copy(f.data.join("engine.sqlite"), &copy).unwrap();
                    let pool = crate::sqlite::open_file(&copy).await.unwrap();
                    let mut tx = pool.begin().await.unwrap();
                    let edits_ticket = mutation.starts_with("UPDATE engine_flow030_clock_events");
                    if edits_ticket {
                        sqlx::query("DROP TRIGGER engine_flow030_clock_ticket_immutable")
                            .execute(&mut *tx).await.unwrap();
                    }
                    let changed = sqlx::query(mutation).execute(&mut *tx).await.unwrap().rows_affected();
                    assert_eq!(changed, 1);
                    if edits_ticket {
                        sqlx::query(
                            "CREATE TRIGGER engine_flow030_clock_ticket_immutable BEFORE UPDATE OF ticket_json ON engine_flow030_clock_events
                             BEGIN SELECT RAISE(ABORT,'immutable clock ticket'); END",
                        ).execute(&mut *tx).await.unwrap();
                    }
                    tx.commit().await.unwrap();
                    pool.close().await;
                    let replay = persistence::replay_verify030::verify_path030(
                        &copy, &query.system_start_id, &page.flow_invocation_id,
                    ).await;
                    assert_eq!(replay.divergence.unwrap().field, expected, "{mutation}");
                }
            }
        }
    }

    #[tokio::test]
    async fn close_releases_database_ownership_with_a_duplicated_descriptor() {
        let f = Fixture::new(WAIT);
        let host = f.open().await.unwrap();
        // Duplicated descriptors share an open file description on Unix.
        // A child can briefly keep one between fork and exec.
        let duplicate = host
            .database_lock
            .lock()
            .unwrap()
            .0
            .as_ref()
            .unwrap()
            .try_clone()
            .unwrap();
        assert_eq!(f.open().await.err().unwrap().code, "RUNTIME_UNAVAILABLE");
        assert!(host.close().await.is_err());
        assert_eq!(f.open().await.err().unwrap().code, "RUNTIME_UNAVAILABLE");
        host.shutdown(&[]).await.unwrap();
        host.close().await.unwrap();
        let reopened = f.open().await.unwrap();
        host.close().await.unwrap();
        assert_eq!(f.open().await.err().unwrap().code, "RUNTIME_UNAVAILABLE");
        drop(duplicate);
        reopened.shutdown(&[]).await.unwrap();
        reopened.close().await.unwrap();
    }

    #[tokio::test]
    async fn restart_marks_unfinished_run_interrupted_without_adoption() {
        const CHILD: &str = "ARGENTIC_SYNTHETIC_CRASH_ROOT";
        if let Some(root) = std::env::var_os(CHILD) {
            let root = PathBuf::from(root);
            let bytes = std::fs::read(root.join("policy.json")).unwrap();
            let host = Host::open(
                &root.join("data"),
                &root.join("workspace"),
                &root.join("source.json"),
                &root.join("policy.json"),
                &digest(bytes),
            )
            .await
            .unwrap();
            host.start("before-crash".into(), entry(), false)
                .await
                .unwrap();
            std::process::exit(0);
        }
        for contract in ["agsdl-0.3.0", argentic_core::AGSDL_040] {
            let f = Fixture::new(&WAIT.replace("agsdl-0.3.0", contract));
            let child = tokio::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "standalone::tests::restart_marks_unfinished_run_interrupted_without_adoption",
                    "--nocapture",
                ])
                .env(CHILD, f._temp.path())
                .output()
                .await
                .unwrap();
            assert!(
                child.status.success(),
                "{}",
                String::from_utf8_lossy(&child.stderr)
            );
            let host = f.open().await.unwrap();
            let runs = host.runs().await.unwrap();
            assert_eq!(runs.len(), 1);
            assert_eq!(runs[0]["interrupted"], true);
            if contract == argentic_core::AGSDL_040 {
                let page = host
                    .projection(
                        api::RuntimeProjectionQuery030 {
                            system_start_id: runs[0]["receipt"]["identity"]["systemStartId"]
                                .as_str()
                                .unwrap()
                                .into(),
                            source: host.source().await,
                            snapshot_id: None,
                        },
                        None,
                    )
                    .await
                    .unwrap();
                assert_eq!(page.verdict.as_deref(), Some("interrupted"));
            }
            assert!(host.active_runs().await.unwrap().is_empty());
            assert!(host
                .start("before-crash".into(), entry(), false)
                .await
                .is_err());
            assert_eq!(host.runs().await.unwrap().len(), 1);
            host.shutdown(&[]).await.unwrap();
            host.close().await.unwrap();
        }
    }

    #[tokio::test]
    async fn failed_retirement_keeps_host_inspectable_but_cannot_close_or_restart() {
        let f = Fixture::new(WAIT);
        let host = f.open().await.unwrap();
        let started = host.start("start".into(), entry(), false).await.unwrap();
        let human_scope = persistence::public030::HumanScope030 {
            system_start_id: started.receipt.identity.system_start_id.clone(),
            source: started.capture.source.clone(),
        };
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if !host
                    .humans(human_scope.clone(), None)
                    .await
                    .unwrap()
                    .requests
                    .is_empty()
                {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        sqlx::query("CREATE TRIGGER reject_standalone_stop BEFORE INSERT ON engine_flow030_events WHEN NEW.event_id LIKE 'system-stop:retire:%' BEGIN SELECT RAISE(ABORT, 'synthetic storage failure'); END")
            .execute(&host.pool).await.unwrap();
        let active = host.active_runs().await.unwrap();
        assert!(host.shutdown(&active).await.is_err());
        assert!(host.shutdown_started());
        assert!(host.close().await.is_err());
        assert_eq!(host.runs().await.unwrap().len(), 1);
        assert_eq!(
            host.start("new".into(), entry(), false)
                .await
                .err()
                .unwrap()
                .code,
            "SHUTDOWN_PENDING"
        );
        sqlx::query("DROP TRIGGER reject_standalone_stop")
            .execute(&host.pool)
            .await
            .unwrap();
        host.shutdown(&active).await.unwrap();
        host.close().await.unwrap();
    }
    #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
    #[tokio::test]
    async fn current_action_policy_retains_real_input_guard_and_refuses_foreign_capture() {
        let f = Fixture::new(WAIT);
        let host = f.open().await.unwrap();
        let started = host.start("start".into(), entry(), false).await.unwrap();
        let file = f.workspace.join("existing.txt");
        std::fs::write(&file, "before").unwrap();
        let payload = json!({"nativeRequest":{"params":{"command":"printf after > existing.txt","additionalPermissions":{"fileSystem":{"write":[file]}}}}});
        let lease = host
            .policies
            .lock_current(&started.capture, &payload)
            .await
            .unwrap();
        assert!(lease.allows());
        assert!(host.policies.0.try_write().is_err());
        drop(lease);
        assert!(host.policies.0.try_write().is_ok());
        let mut outside = payload.clone();
        outside["nativeRequest"]["params"]["additionalPermissions"]["fileSystem"]["write"] =
            json!([f._temp.path().join("outside.txt")]);
        assert!(!host
            .policies
            .lock_current(&started.capture, &outside)
            .await
            .unwrap()
            .allows());
        let mut foreign = started.capture.clone();
        foreign.source.owner.project_id = "foreign".into();
        assert!(host
            .policies
            .lock_current(&foreign, &payload)
            .await
            .is_err());
        let active = host.active_runs().await.unwrap();
        host.shutdown(&active).await.unwrap();
        host.close().await.unwrap();
    }

    #[tokio::test]
    async fn standalone_retry_retains_stop_and_requires_consent() {
        let f = Fixture::new(WAIT);
        let host = f.open().await.unwrap();
        host.start("start".into(), entry(), false).await.unwrap();
        assert_eq!(
            host.shutdown(&[]).await.unwrap_err().code,
            "SHUTDOWN_ACTIVITY_CHANGED"
        );
        assert!(!host.shutdown_started());
        let approved = host.active_runs().await.unwrap();
        host.shutdown(&approved).await.unwrap();
        assert!(host.shutdown_started());
        host.shutdown(&approved).await.unwrap();
        host.close().await.unwrap();
    }
}
