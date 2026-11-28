use super::*;
use std::{ffi::OsStr, path::Path};
use url::Url;

/// Native-only handle, retained by the host and never deserialized from a tool.
#[derive(Clone)]
pub struct SkillHandle {
    capture_id: String,
    agent: String,
    name: String,
    declared_uri: String,
    final_uri: String,
    sha256: String,
}

#[derive(Clone)]
pub struct SkillDescriptor {
    pub key: String,
    pub name: String,
    pub description: String,
    pub path: String,
    pub source: ResolvedContentReceipt,
    pub handle: SkillHandle,
}

impl SkillDescriptor {
    pub(crate) fn resource_uri(&self, relative_path: &str) -> ContentResult<Url> {
        skill_resource_uri(&self.handle.root_uri()?, relative_path)
    }
}

impl SkillHandle {
    fn root_uri(&self) -> ContentResult<Url> {
        resolve::absolute_uri(&self.final_uri)?
            .join(".")
            .map_err(|e| error("CONTENT_URI", e))
    }
}

pub struct LoadedSkill {
    handle: SkillHandle,
    root: resolve::SkillRoot,
    pub instructions: String,
    pub provenance: ContentProvenance,
}

fn skill_source(uri: &str) -> String {
    serde_json::json!({"uri": uri, "mediaType": "text/plain"}).to_string()
}

fn parse_skill(receipt: &ResolvedContentReceipt) -> ContentResult<(String, String, String)> {
    let ContentPayload::Text { text } = &receipt.payload else {
        return Err(error("CONTENT_FORMAT", "SKILL.md must be text."));
    };
    let uri = receipt
        .provenance
        .final_uri
        .as_deref()
        .ok_or_else(|| error("CORRELATION", "Skill file has no resolved location."))?;
    let url = resolve::absolute_uri(uri)?;
    if !url.path().ends_with("/SKILL.md") {
        return Err(error("INVALID_SKILL", "Skill URI must name SKILL.md."));
    }
    crate::skill_text::parse_admitted_skill_text(Path::new(url.path()), text)
        .map_err(|e| error("INVALID_SKILL", e))
}

pub async fn discover_skills(context: &ContentContext) -> ContentResult<Vec<SkillDescriptor>> {
    let selected = argentic_core::durable_json::at_path(
        &context.document_json,
        &["agents", &context.agent, "skills"],
    )
    .map_err(|e| error("PARSE", e))?;
    let names: Vec<String> = match selected {
        Some(value) => serde_json::from_str(value.get()).map_err(|e| error("INVALID_SKILL", e))?,
        None => Vec::new(),
    };
    let mut seen = std::collections::BTreeSet::new();
    if names.len() > 32 {
        return Err(error(
            "INPUT_LIMIT",
            "Too many selected skills for the native discovery bound.",
        ));
    }
    let mut retained = 0u64;
    let mut result = Vec::with_capacity(names.len());
    for key in names {
        if !seen.insert(key.clone()) {
            return Err(error("INVALID_SKILL", "Duplicate skill name."));
        }
        let raw =
            argentic_core::durable_json::at_path(&context.document_json, &["skills", &key, "uri"])
                .map_err(|e| error("PARSE", e))?
                .ok_or_else(|| error("INVALID_SKILL", "Unknown declared skill."))?;
        let declared_uri: String =
            serde_json::from_str(raw.get()).map_err(|e| error("INVALID_SKILL", e))?;
        let receipt = resolve_content(
            context,
            ContentUse {
                source_json: skill_source(&declared_uri),
                usage: ContentUsage::SkillDiscovery,
                pointer: format!("/skills/{}/uri", key.replace('~', "~0").replace('/', "~1")),
            },
        )
        .await?;
        let bytes = receipt
            .provenance
            .size_bytes
            .parse::<u64>()
            .map_err(|_| error("CORRELATION", "Invalid native skill size."))?;
        retained = retained.saturating_add(bytes);
        if bytes > 262_144 || retained > 1_000_000 {
            return Err(error(
                "INPUT_LIMIT",
                "Selected skills exceed the native content bound.",
            ));
        }
        let (name, description, _) = parse_skill(&receipt)?;
        let final_uri = receipt
            .provenance
            .final_uri
            .clone()
            .ok_or_else(|| error("CORRELATION", "Skill URI was not resolved."))?;
        let handle = SkillHandle {
            capture_id: context.capture_id.clone(),
            agent: context.agent.clone(),
            name: key.clone(),
            declared_uri,
            final_uri,
            sha256: receipt.provenance.sha256.clone(),
        };
        result.push(SkillDescriptor {
            source: receipt,
            path: resolve::absolute_uri(&handle.final_uri)?
                .to_file_path()
                .map_err(|_| error("CONTENT_URI", "Native skills require a local file"))?
                .to_string_lossy()
                .into_owned(),
            key,
            name,
            description,
            handle,
        });
    }
    Ok(result)
}

pub async fn load_skill(
    context: &ContentContext,
    handle: &SkillHandle,
) -> ContentResult<LoadedSkill> {
    load_skill_with_deadline(context, handle, &context.read_deadline()).await
}

pub(crate) async fn load_skill_with_deadline(
    context: &ContentContext,
    handle: &SkillHandle,
    deadline: &ReadDeadline<'_>,
) -> ContentResult<LoadedSkill> {
    if handle.capture_id != context.capture_id || handle.agent != context.agent {
        return Err(error(
            "CORRELATION",
            "Skill handle belongs to another capture.",
        ));
    }
    let receipt = resolve_content_with_deadline(
        context,
        ContentUse {
            source_json: skill_source(&handle.declared_uri),
            usage: ContentUsage::SkillActivation,
            pointer: format!(
                "/skills/{}/uri",
                handle.name.replace('~', "~0").replace('/', "~1")
            ),
        },
        deadline,
    )
    .await?;
    if receipt.provenance.sha256 != handle.sha256
        || receipt.provenance.final_uri.as_deref() != Some(handle.final_uri.as_str())
    {
        return Err(error("CORRELATION", "Skill changed since discovery."));
    }
    let (_, _, instructions) = parse_skill(&receipt)?;
    let root = handle.root_uri()?;
    if root.scheme() == "file" {
        let path = root
            .to_file_path()
            .map_err(|_| error("CONTENT_URI", "Invalid skill root."))?;
        let (canonical, identity) = deadline
            .blocking(move || {
                let canonical = path
                    .canonicalize()
                    .map_err(|e| error("CONTENT_UNAVAILABLE", e))?;
                let identity = crate::content_io::admitted::workspace_root_identity(&canonical)
                    .map_err(|e| error("CONTENT_UNAVAILABLE", e))?
                    .observation_key();
                Ok((canonical, identity))
            })
            .await
            .map_err(|error| file_read_error(deadline.cancel.is_cancelled(), error))??;
        if Url::from_directory_path(&canonical).ok().as_ref() != Some(&root)
            || identity.is_none()
            || identity != receipt.provenance.file_parent_identity
        {
            return Err(error(
                "CORRELATION",
                "Skill root changed during activation.",
            ));
        }
    }
    Ok(LoadedSkill {
        handle: handle.clone(),
        root: resolve::SkillRoot {
            uri: root,
            file_identity: receipt.provenance.file_parent_identity.clone(),
        },
        instructions,
        provenance: receipt.provenance,
    })
}

pub async fn read_skill_resource(
    context: &ContentContext,
    loaded: &LoadedSkill,
    relative_path: &str,
    media_type: Option<&str>,
) -> ContentResult<ResolvedContentReceipt> {
    read_skill_resource_with_deadline(
        context,
        loaded,
        relative_path,
        media_type,
        &context.read_deadline(),
    )
    .await
}

pub(crate) async fn read_skill_resource_with_deadline(
    context: &ContentContext,
    loaded: &LoadedSkill,
    relative_path: &str,
    media_type: Option<&str>,
    deadline: &ReadDeadline<'_>,
) -> ContentResult<ResolvedContentReceipt> {
    if loaded.handle.capture_id != context.capture_id || loaded.handle.agent != context.agent {
        return Err(error(
            "CORRELATION",
            "Skill handle belongs to another capture.",
        ));
    }
    let target = skill_resource_uri(&loaded.root.uri, relative_path)?;
    let mut source = serde_json::json!({"uri": target.as_str()});
    if let Some(media) = media_type {
        source["mediaType"] = media.into();
    }
    resolve::resolve_content_in_root(
        context,
        ContentUse {
            source_json: source.to_string(),
            usage: ContentUsage::SkillResource,
            pointer: format!("/skills/{}/resources", loaded.handle.name),
        },
        Some(&loaded.root),
        deadline,
    )
    .await
}

/// Resolve a slash-separated literal path relative to the skill directory.
fn skill_resource_uri(root: &Url, relative_path: &str) -> ContentResult<Url> {
    let invalid_path = || {
        error(
            "CONTENT_URI",
            "Skill resource must be a relative path within its root.",
        )
    };
    if relative_path.contains(['\\', '\0']) {
        return Err(invalid_path());
    }
    #[cfg(windows)]
    if relative_path.contains(':') {
        return Err(invalid_path());
    }
    let mut target = root
        .to_file_path()
        .map_err(|()| error("CONTENT_URI", "Invalid skill root."))?;
    for part in relative_path.split('/') {
        if Path::new(part).file_name() != Some(OsStr::new(part)) {
            return Err(invalid_path());
        }
        target.push(part);
    }
    Url::from_file_path(target).map_err(|()| invalid_path())
}

#[cfg(test)]
#[test]
fn skill_resource_uri_preserves_literal_path_components() {
    let temp = tempfile::tempdir().unwrap();
    let root = Url::from_directory_path(temp.path()).unwrap();
    let nested = temp.path().join("nested%2f");
    std::fs::create_dir(&nested).unwrap();
    let file = nested.join("notes#2.md");
    std::fs::write(&file, b"literal resource").unwrap();
    let target = skill_resource_uri(&root, "nested%2f/notes#2.md").unwrap();
    assert_eq!(target.fragment(), None);
    let path = resolve::local_file_path(target.as_str(), None).unwrap();
    assert_eq!(path, file);
    assert_eq!(std::fs::read(path).unwrap(), b"literal resource");
    #[cfg(unix)]
    {
        let literal = "notes\t2.md";
        let target = skill_resource_uri(&root, literal).unwrap();
        assert_eq!(target.to_file_path().unwrap(), temp.path().join(literal));
    }
    for path in ["", "/absolute", "../escape", "nested/./file", "a\\b"] {
        assert!(skill_resource_uri(&root, path).is_err(), "{path}");
    }
}
