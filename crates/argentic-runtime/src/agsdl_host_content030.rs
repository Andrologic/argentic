//! Recipient-scoped native content. Nothing here grants authority to resources.
use crate::{host::HostContext, persistence::{failure, Result as HostResult}};
use serde_json::{json, Value};
use std::{path::Path, sync::Arc};
use tokio_util::sync::CancellationToken;

pub(crate) fn content_error(error: crate::content::ContentError) -> crate::persistence::dto::EngineFailure {
    failure(&error.code, &error.pointer, &error.message)
}

fn missing(field: &str) -> crate::persistence::dto::EngineFailure {
    failure("CORRELATION", field, "Native content correlation differs")
}
use crate::content::{
    resolve_content, ContentContext, ContentPayload, ContentUsage, ContentUse,
    ResolvedContentReceipt, SkillDescriptor,
};
use crate::permissions::{normalize_requested, ExecutionPurpose};

pub(crate) fn validate_text_sources(document: &Value, document_location: Option<&url::Url>) -> HostResult<()> {
    use crate::agsdl_host_work_admission030::pointer_token030;
    fn visit(value: &Value, pointer: &str, document_location: Option<&url::Url>) -> HostResult<()> {
        if let Some(object) = value.as_object() {
            if object.get("uri").is_some_and(Value::is_string)
                && object
                    .keys()
                    .all(|key| matches!(key.as_str(), "uri" | "mediaType"))
            {
                let uri = object["uri"].as_str().unwrap();
                crate::content::local_file_path(uri, document_location).map_err(|error| {
                    failure(
                        "CONTENT_SOURCE_UNSUPPORTED",
                        &format!("{pointer}/uri"),
                        &error.message,
                    )
                })?;
                if !crate::content::file_helper::qualified_platform() {
                    return Err(failure(
                        "CONTENT_SOURCE_UNSUPPORTED",
                        &format!("{pointer}/uri"),
                        "Local file content is unavailable on this platform",
                    ));
                }
            }
            if (object.contains_key("uri") || object.contains_key("value"))
                && object
                    .keys()
                    .all(|key| matches!(key.as_str(), "uri" | "value" | "mediaType"))
            {
                if let Some(media) = object.get("mediaType").and_then(Value::as_str) {
                    let mime = media
                        .split(';')
                        .next()
                        .unwrap_or("")
                        .trim()
                        .to_ascii_lowercase();
                    if !mime.starts_with("text/")
                        && mime != "application/json"
                        && !mime.ends_with("+json")
                    {
                        return Err(failure("CONTENT_SOURCE_UNSUPPORTED",&format!("{pointer}/mediaType"),"This adapter supports text and JSON context. The declared media type is unavailable"));
                    }
                }
            }
            for (key, child) in object {
                let segments: Vec<&str> = pointer.split('/').skip(1).collect();
                let agent_definition = segments.len() == 2 && segments[0] == "agents";
                let source_position = segments.len() >= 2
                    && (matches!(segments[segments.len() - 2], "content" | "resources")
                        || segments
                            .last()
                            .is_some_and(|part| part.parse::<usize>().is_ok()));
                let inline_source = source_position
                    && object.contains_key("value")
                    && object
                        .keys()
                        .all(|name| matches!(name.as_str(), "value" | "mediaType"));
                let literal_operand = segments.len() >= 2
                    && segments[segments.len() - 2] == "arguments"
                    && object.len() == 1
                    && object.contains_key("value");
                if (key == "value" && (inline_source || literal_operand))
                    || (agent_definition
                        && ["results", "harnessSettings", "permissions"].contains(&key.as_str()))
                {
                    continue;
                }
                let location = format!("{pointer}/{}", pointer_token030(key));
                visit(child, &location, document_location)?;
            }
        } else if let Some(array) = value.as_array() {
            for (index, child) in array.iter().enumerate() {
                visit(child, &format!("{pointer}/{index}"), document_location)?;
            }
        }
        Ok(())
    }
    visit(document, "", document_location)
}

const CONTENT_SCOPE_REFUSAL: &str =
    "Content is outside the approved data scope or inside private host metadata";

fn approved_data_path(root: &Path, private: &Path, path: &Path) -> bool {
    path.is_absolute()
        && path.starts_with(root)
        && !private_project_path(root, path)
        && !path.starts_with(private)
}

pub(crate) struct Sources030 {
    context: ContentContext,
    document: Value,
    root: std::path::PathBuf,
    private: std::path::PathBuf,
    // Keep the actual bytes and provenance for this work's lifetime. D7 never
    // reconstructs these snapshots by replaying a read after interruption.
    pub receipts: Vec<ResolvedContentReceipt>,
    config: Option<HostContext>,
    ceiling_digest: String,
}

impl Sources030 {
    pub async fn check_ceiling(&self) -> HostResult<()> {
        if let Some(config) = &self.config {
            let ceiling = config
                .lock_agsdl_host_permissions()
                .await
                .map_err(|_| missing("contentPolicy"))?;
            if ceiling.fingerprint() != self.ceiling_digest {
                return Err(failure(
                    "CAPTURE_CHANGED",
                    "skills",
                    "Content permission ceiling changed",
                ));
            }
        }
        Ok(())
    }
    pub async fn new(
        config: &HostContext,
        capture: &crate::persistence::start030::StartCapture030,
        agent: &str,
        policy: &crate::permissions::EffectivePolicy,
        cancel: &CancellationToken,
        tasks: &crate::tasks::StreamTasks,
        attempt: &crate::agsdl_host_flow030::FlowWorkAttempt030,
    ) -> HostResult<Self> {
        let ceiling = config
            .lock_agsdl_host_permissions()
            .await
            .map_err(|_| missing("contentPolicy"))?;
        let paths = policy
            .policies()
            .next()
            .ok_or_else(|| missing("contentPolicy"))?
            .context()
            .clone();
        let root = paths.workspace.clone();
        let private = config
            .root()
            .canonicalize()
            .map_err(|_| missing("contentPrivateRoot"))?;
        let current = normalize_requested(ceiling.permissions().clone(), paths)
            .map_err(|_| missing("contentPolicy"))?;
        let ceiling_digest = ceiling.fingerprint().to_owned();
        let location = crate::capture::captured_document_location(capture)?;
        let context = ContentContext::capture(
            capture.identity.snapshot_id.clone(),
            capture.source.source_sha256.clone(),
            capture.authored_source.clone(),
            location.as_ref().map(url::Url::as_str),
            agent.into(),
            policy.with_current(current),
            ExecutionPurpose::Agent,
            cancel.clone(),
            Arc::new(|_| {
                Box::pin(async {
                    Err(crate::content::ContentError {
                        code: "HARNESS_CAPABILITY".into(),
                        pointer: String::new(),
                        message: "Content read approvals are not represented".into(),
                    })
                })
            }),
        )
        .map_err(content_error)?;
        let parent = format!(
            "flow030:{}:{}",
            attempt.identity().system_start_id,
            attempt.start_effect_id()
        );
        let tasks = tasks.clone();
        let context = context
            .with_network(config.network())
            .with_resource_owner(Arc::new(move |cleanup| {
                tasks.attach_cleanup030(&parent, cleanup)
            }));
        let context = context
            .with_captured_source(config.root(), capture)
            .map_err(content_error)?;
        let mut sources = Self::from_context(
            context,
            serde_json::from_str(&capture.authored_source)?,
            root,
            private,
        )?;
        sources.config = Some(config.clone());
        sources.ceiling_digest = ceiling_digest;
        Ok(sources)
    }

    pub(crate) fn from_context(
        context: ContentContext,
        document: Value,
        root: std::path::PathBuf,
        private: std::path::PathBuf,
    ) -> HostResult<Self> {
        let scope_root = root.clone();
        let scope_private = private.clone();
        let snapshot = context.project_root().map(Path::to_path_buf);
        let context = context.with_read_target_scope(Arc::new(move |path, retained| {
            // The resolver supplies a snapshot path only after it finds the
            // member in the captured inventory. Ordinary files keep the
            // private-root exclusion, including inside managed workspaces.
            let retained = retained
                .zip(snapshot.as_ref())
                .is_some_and(|(target, root)| {
                    target.starts_with(root) && !private_project_path(root, target)
                });
            if retained || approved_data_path(&scope_root, &scope_private, path) {
                Ok(())
            } else {
                Err(crate::content::ContentError {
                    code: "PERMISSION".into(),
                    pointer: String::new(),
                    message: CONTENT_SCOPE_REFUSAL.into(),
                })
            }
        }));
        Ok(Self {
            context,
            document,
            root,
            private,
            receipts: Vec::new(),
            config: None,
            ceiling_digest: String::new(),
        })
    }

    async fn source(
        &mut self,
        source: &mut Value,
        usage: ContentUsage,
        pointer: &str,
    ) -> HostResult<()> {
        let config = self.config.clone();
        let _ceiling = if let Some(config) = &config {
            let ceiling = config
                .lock_agsdl_host_permissions()
                .await
                .map_err(|_| missing("contentPolicy"))?;
            if ceiling.fingerprint() != self.ceiling_digest {
                return Err(failure(
                    "CAPTURE_CHANGED",
                    pointer,
                    "Content permission ceiling changed",
                ));
            }
            Some(ceiling)
        } else {
            None
        };
        let expanded = source
            .get("ref")
            .and_then(Value::as_str)
            .map(|name| &self.document["content"][name])
            .unwrap_or(source);
        self.allowed_file(expanded, pointer)?;
        let receipt = resolve_content(
            &self.context,
            ContentUse {
                source_json: source.to_string(),
                usage,
                pointer: pointer.into(),
            },
        )
        .await
        .map_err(content_error)?;
        let previous: u64 = self
            .receipts
            .iter()
            .filter_map(|receipt| receipt.provenance.size_bytes.parse::<u64>().ok())
            .sum();
        if previous.saturating_add(
            receipt
                .provenance
                .size_bytes
                .parse::<u64>()
                .unwrap_or(u64::MAX),
        ) > 1_000_000
            || receipt
                .provenance
                .size_bytes
                .parse::<usize>()
                .ok()
                .is_none_or(|n| n > 262_144)
        {
            return Err(failure(
                "INPUT_LIMIT",
                pointer,
                "Content exceeds the native harness input bound",
            ));
        }
        match &receipt.payload {
            ContentPayload::Text { text } => {
                *source = json!({"value":text,"mediaType":receipt.provenance.media_type})
            }
            ContentPayload::Json { json: raw } => {
                // The adapter transports resources as text context. Keep the
                // JSON bytes inside that text rather than reserializing them.
                *source = json!({"value":raw,"mediaType":receipt.provenance.media_type});
            }
            ContentPayload::Media { .. } => {
                return Err(failure(
                    "CONTENT_SOURCE_UNSUPPORTED",
                    pointer,
                    "This Codex adapter cannot represent this media type as native context",
                ))
            }
        }
        self.receipts.push(receipt);
        Ok(())
    }

    pub async fn prompt(
        &mut self,
        value: &mut Value,
        usage: ContentUsage,
        pointer: &str,
    ) -> HostResult<()> {
        if let Some(sources) = value.as_array_mut() {
            for (index, source) in sources.iter_mut().enumerate() {
                self.source(source, usage, &format!("{pointer}/{index}"))
                    .await?;
            }
        }
        Ok(())
    }

    pub async fn resources(&mut self, value: &mut Value, pointer: &str) -> HostResult<()> {
        if let Some(resources) = value.as_object_mut() {
            for (name, source) in resources {
                self.source(
                    source,
                    ContentUsage::Resource,
                    &format!("{pointer}/{}", name.replace('~', "~0").replace('/', "~1")),
                )
                .await?;
            }
        }
        Ok(())
    }

    pub async fn message(&mut self, value: &mut Value, pointer: &str) -> HostResult<()> {
        if let Some(message) = value.as_object_mut() {
            if let Some(prompt) = message.get_mut("prompt") {
                self.prompt(prompt, ContentUsage::Prompt, &format!("{pointer}/prompt"))
                    .await?;
            }
            if let Some(resources) = message.get_mut("resources") {
                self.resources(resources, &format!("{pointer}/resources"))
                    .await?;
            }
        }
        Ok(())
    }

    pub async fn skills(&mut self) -> HostResult<Vec<SkillDescriptor>> {
        let config = self.config.clone();
        let _ceiling = if let Some(config) = &config {
            let ceiling = config
                .lock_agsdl_host_permissions()
                .await
                .map_err(|_| missing("contentPolicy"))?;
            if ceiling.fingerprint() != self.ceiling_digest {
                return Err(failure(
                    "CAPTURE_CHANGED",
                    "skills",
                    "Skill permission ceiling changed",
                ));
            }
            Some(ceiling)
        } else {
            None
        };
        qualify_skill_folders(
            &self.document,
            self.context.project_root().unwrap_or(&self.root),
            self.context.document_location(),
        )?;
        for key in self.document["agents"][self.context.binding().map_err(content_error)?.agent]
            ["skills"]
            .as_array()
            .into_iter()
            .flatten()
        {
            let name = key.as_str().ok_or_else(|| missing("skills"))?;
            self.allowed_file(&self.document["skills"][name], &format!("/skills/{name}"))?;
        }
        let selected = crate::content::discover_skills(&self.context)
            .await
            .map_err(content_error)?;
        let previous: u64 = self
            .receipts
            .iter()
            .filter_map(|receipt| receipt.provenance.size_bytes.parse::<u64>().ok())
            .sum();
        let skill_bytes: u64 = selected
            .iter()
            .filter_map(|skill| skill.source.provenance.size_bytes.parse::<u64>().ok())
            .sum();
        if previous.saturating_add(skill_bytes) > 1_000_000 {
            return Err(failure(
                "INPUT_LIMIT",
                "skills",
                "Selected skills exceed the work's content bound",
            ));
        }
        for skill in &selected {
            let root = Path::new(&skill.path)
                .parent()
                .ok_or_else(|| missing("skillRoot"))?;
            for entry in std::fs::read_dir(root).map_err(|_| missing("skillRoot"))? {
                let entry = entry.map_err(|_| missing("skillRoot"))?;
                if entry.path() != Path::new(&skill.path)
                    || !entry
                        .file_type()
                        .map_err(|_| missing("skillRoot"))?
                        .is_file()
                {
                    return Err(failure("CONTENT_SOURCE_UNSUPPORTED","skills","Only single-file skills are represented; selected folders with resources, scripts, other files or links are refused"));
                }
            }
        }
        self.receipts
            .extend(selected.iter().map(|skill| skill.source.clone()));
        Ok(selected)
    }

    fn allowed_file(&self, source: &Value, pointer: &str) -> HostResult<()> {
        if let Some(uri) = source.get("uri").and_then(Value::as_str) {
            let path = crate::content::local_file_path(uri, self.context.document_location())
                .map_err(|error| failure("CONTENT_SOURCE_UNSUPPORTED", pointer, &error.message))?;
            let path = path.canonicalize().map_err(|_| {
                failure(
                    "CONTENT_UNAVAILABLE",
                    pointer,
                    "Native content file is unavailable",
                )
            })?;
            // Project files are read from the retained snapshot. Any other
            // file keeps the workspace rule of an equivalent JSON document.
            let allowed = match self.context.project_root() {
                Some(root) if path.starts_with(root) => !private_project_path(root, &path),
                _ => approved_data_path(&self.root, &self.private, &path),
            };
            if !allowed {
                return Err(failure("PERMISSION", pointer, CONTENT_SCOPE_REFUSAL));
            }
        }
        Ok(())
    }
}

fn private_project_path(root: &Path, path: &Path) -> bool {
    path.strip_prefix(root).is_ok_and(|relative| {
        relative.components().any(|part| {
            part.as_os_str().to_str().is_none_or(|name| {
                matches!(
                    name.to_ascii_lowercase().as_str(),
                    ".macro" | ".git" | ".aws" | ".codex" | "auth.json" | "credentials"
                )
            })
        })
    })
}

/// Only inspect selected folders within the native project scope. Other skill
/// shapes remain explicit refusals before launch as well as at work admission.
pub(crate) fn qualify_skill_folders(
    document: &Value,
    root: &Path,
    document_location: Option<&url::Url>,
) -> HostResult<()> {
    for (agent_name, agent) in document["agents"].as_object().into_iter().flatten() {
        if agent["skills"]
            .as_array()
            .is_some_and(|skills| skills.len() > 32)
        {
            return Err(failure(
                "INPUT_LIMIT",
                &format!("/agents/{agent_name}/skills"),
                "Too many selected skills for the native discovery bound",
            ));
        }
        for name in agent["skills"].as_array().into_iter().flatten() {
            let name = name.as_str().ok_or_else(|| missing("skills"))?;
            let pointer = format!("/agents/{agent_name}/skills");
            let uri = document["skills"][name]["uri"]
                .as_str()
                .ok_or_else(|| missing("skills"))?;
            let path = crate::content::local_file_path(uri, document_location)
                .ok()
                .and_then(|path| path.canonicalize().ok())
                .ok_or_else(|| {
                    failure(
                        "CONTENT_SOURCE_UNSUPPORTED",
                        &pointer,
                        "Selected skill requires an available local SKILL.md file",
                    )
                })?;
            if !path.starts_with(root)
                || private_project_path(root, &path)
                || path.file_name().is_none_or(|name| name != "SKILL.md")
            {
                return Err(failure(
                    "PERMISSION",
                    &pointer,
                    "Selected skill is outside the approved project data scope",
                ));
            }
            let directory = path.parent().ok_or_else(|| missing("skills"))?;
            for entry in std::fs::read_dir(directory).map_err(|_| missing("skills"))? {
                let entry = entry.map_err(|_| missing("skills"))?;
                if entry.path() != path
                    || !entry.file_type().map_err(|_| missing("skills"))?.is_file()
                {
                    return Err(failure("CONTENT_SOURCE_UNSUPPORTED",&pointer,"Only single-file skills are represented; folders with resources, scripts, other files or links are refused"));
                }
            }
        }
    }
    Ok(())
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use crate::permissions::{EffectivePolicy, PathContext, RequestedPermissions};
    use sha2::{Digest, Sha256};

    /// A run from a project keeps the message rules of an equivalent JSON
    /// document. Only paths inside the retained copy use its inventory.
    fn project_read_rules(managed: bool) {
        let temp = tempfile::tempdir().unwrap();
        let base = temp.path().canonicalize().unwrap();
        let data = base.join("data");
        let workspace = if managed {
            data.join("managed/run")
        } else {
            base.join("workspace")
        };
        std::fs::create_dir_all(workspace.join(".git")).unwrap();
        std::fs::create_dir_all(&data).unwrap();
        std::fs::write(workspace.join("notes.md"), "Workspace notes.\n").unwrap();
        std::fs::write(workspace.join(".git/config"), "Private.\n").unwrap();
        let opened = crate::source::OpenedSource::open_path(
            &Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/agsdl-project/review"),
        )
        .unwrap();
        let retained = opened.retain(&data).unwrap();
        let mut scope = json!({"root":workspace});
        retained.bind_scope(&mut scope).unwrap();
        let authored = opened.authored_json().to_owned();
        let capture = crate::persistence::start030::StartCapture030 {
            identity: serde_json::from_value(json!({"systemStartId":"system",
                "flowInvocationId":"flow","snapshotId":"snapshot","epoch":"1"}))
            .unwrap(),
            source: serde_json::from_value(json!({
                "owner":{"projectId":"project","planId":"review","branch":"test"},
                "revision":"1","sourceSha256":opened.source_sha256()}))
            .unwrap(),
            versions: serde_json::from_value(json!({"snapshot":"2","contract":"agsdl-0.4.0",
                "compiler":"compiler","runtime":"runtime","registry":"registry",
                "profile":"macro-agsdl-030-v1"}))
            .unwrap(),
            authored_source: authored.clone(),
            compiled_plan_json: "null".into(),
            registry_json: String::new(),
            private_capture_json: json!({"callScopeJson":scope.to_string()}).to_string(),
        };
        let policy = normalize_requested(
            RequestedPermissions::Preset(crate::permissions::Preset::ReadOnly),
            PathContext {
                workspace: workspace.clone(),
                home: workspace.clone(),
            },
        )
        .unwrap();
        let context = ContentContext::capture(
            "snapshot".into(),
            opened.source_sha256(),
            authored.clone(),
            scope["documentLocation"].as_str(),
            "reviewer".into(),
            EffectivePolicy::new(policy.clone(), policy.clone(), policy),
            ExecutionPurpose::Agent,
            CancellationToken::new(),
            Arc::new(|_| Box::pin(async { panic!("unexpected content approval") })),
        )
        .unwrap()
        .with_captured_source(&data, &capture)
        .unwrap();
        let document: Value = serde_json::from_str(&authored).unwrap();
        let review = document["agents"]["reviewer"]["resources"]["review"].clone();
        let sources = Sources030::from_context(context, document, workspace.clone(), data).unwrap();
        let uri = |path: &Path| json!({"uri":url::Url::from_file_path(path).unwrap().as_str()});
        sources.allowed_file(&review, "/agents/reviewer").unwrap();
        let target = crate::content::local_file_path(
            review["uri"].as_str().unwrap(),
            sources.context.document_location(),
        )
        .unwrap()
        .canonicalize()
        .unwrap();
        let project_root = sources.context.project_root().unwrap();
        let logical_target = workspace.join(target.strip_prefix(project_root).unwrap());
        let read = |path: &Path| crate::permissions::ActionRequest::Read {
            path: crate::permissions::native_request_path(path).unwrap(),
        };
        assert_eq!(
            sources
                .context
                .permission(&read(&logical_target), Some(&target))
                .unwrap()
                .effect,
            crate::permissions::Effect::Allow
        );
        let notes = workspace.join("notes.md");
        let admitted = sources.context.permission(&read(&notes), None);
        if managed {
            assert_eq!(admitted.unwrap_err().code, "PERMISSION");
            assert_eq!(
                sources
                    .allowed_file(&uri(&notes), "/message/resources/0")
                    .unwrap_err()
                    .code,
                "PERMISSION"
            );
        } else {
            assert_eq!(admitted.unwrap().effect, crate::permissions::Effect::Allow);
            sources
                .allowed_file(&uri(&notes), "/message/resources/0")
                .unwrap();
        }
        assert_eq!(
            sources
                .allowed_file(&uri(&workspace.join(".git/config")), "/message/resources/0")
                .unwrap_err()
                .code,
            "PERMISSION"
        );
        // Host data outside the snapshot, including its index, stays private.
        std::fs::write(base.join("data/engine.lock"), "").unwrap();
        let index = std::fs::read_dir(base.join("data/agsdl-snapshots"))
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path()
            .join("files.json");
        for private in [base.join("data/engine.lock"), index] {
            assert_eq!(
                sources
                    .allowed_file(&uri(&private), "/message/resources/0")
                    .unwrap_err()
                    .code,
                "PERMISSION",
                "{}",
                private.display()
            );
        }
    }

    #[test]
    fn project_runs_keep_workspace_rules_outside_the_snapshot() {
        project_read_rules(false);
    }

    #[test]
    fn managed_workspace_admits_only_captured_project_members() {
        project_read_rules(true);
    }

    #[tokio::test]
    async fn recipient_read_scope_is_rechecked_at_content_admission() {
        for folder in [".macro", "private", "../outside"] {
            let temp = tempfile::tempdir().unwrap();
            let root = temp.path().canonicalize().unwrap().join("workspace");
            std::fs::create_dir(&root).unwrap();
            let file = root.join("data.txt");
            std::fs::write(&file, "Public fixture").unwrap();
            let private = root.join(folder);
            std::fs::create_dir(&private).unwrap();
            let destination = private.join("fixture.txt");
            std::fs::write(&destination, "Private fixture").unwrap();
            let document = json!({"agents":{"worker":{}}});
            let document_json = document.to_string();
            let origin = url::Url::from_file_path(root.join("document.json")).unwrap();
            let policy = normalize_requested(
                RequestedPermissions::Preset(crate::permissions::Preset::ReadOnly),
                PathContext {
                    workspace: root.clone(),
                    home: root.clone(),
                },
            )
            .unwrap();
            let context = ContentContext::capture(
                "capture".into(),
                format!("{:x}", Sha256::digest(document_json.as_bytes())),
                document_json,
                Some(origin.as_str()),
                "worker".into(),
                EffectivePolicy::new(policy.clone(), policy.clone(), policy),
                ExecutionPurpose::Agent,
                CancellationToken::new(),
                Arc::new(|_| {
                    Box::pin(async { panic!("read-only content must not request approval") })
                }),
            )
            .unwrap();
            let sources =
                Sources030::from_context(context, document, root.clone(), root.join("private"))
                    .unwrap();
            let absolute = url::Url::from_file_path(&file).unwrap();
            let source = json!({"uri":"data.txt","mediaType":"text/plain"});
            sources
                .allowed_file(&json!({"uri":absolute.as_str()}), "/resource")
                .unwrap();
            sources.allowed_file(&source, "/resource").unwrap();
            std::fs::remove_file(&file).unwrap();
            std::os::unix::fs::symlink(&destination, &file).unwrap();
            assert_eq!(
                sources.allowed_file(&source, "/resource").unwrap_err().code,
                "PERMISSION"
            );
            // Continue through the same native resolver used after preflight.
            // The recipient scope must be enforced before starting its reader.
            let result = resolve_content(
                &sources.context,
                ContentUse {
                    source_json: source.to_string(),
                    usage: ContentUsage::Resource,
                    pointer: "/resource".into(),
                },
            )
            .await;
            assert_eq!(
                result.unwrap_err().code,
                "PERMISSION",
                "scope changed to {folder}"
            );
        }
    }
}
