use super::{
    archive, invalid, io, parse_json, read_regular, Files, MANIFEST, MAX_FILES, MAX_JSON,
    MAX_JSON_TOTAL, MAX_TOTAL,
};
use crate::persistence::Result;
use cap_std::{ambient_authority, fs::Dir};
use percent_encoding::{percent_decode_str, utf8_percent_encode, NON_ALPHANUMERIC};
use serde_json::{Map, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    sync::OnceLock,
};

const SECTIONS: &[&str] = &["agents", "compositions", "content", "skills", "mcpServers"];

pub(super) struct Project {
    pub document: Value,
    pub secrets: BTreeMap<String, Vec<String>>,
    pub mcp_files: BTreeMap<String, Vec<String>>,
    pub files: Files,
    origins: BTreeMap<String, String>,
}

impl Project {
    pub(super) fn origin<'a>(&'a self, pointer: &'a str) -> (&'a str, &'a str) {
        let mut prefix = pointer;
        loop {
            if let Some(file) = self.origins.get(prefix) {
                return (file, &pointer[prefix.len()..]);
            }
            let Some((parent, _)) = prefix.rsplit_once('/') else {
                return (MANIFEST, pointer);
            };
            prefix = parent;
        }
    }
}

fn child(pointer: &str, name: &str) -> String {
    format!("{pointer}/{}", name.replace('~', "~0").replace('/', "~1"))
}

fn scheme(uri: &str) -> Option<&str> {
    let (prefix, _) = uri.split_once(':')?;
    (prefix.starts_with(|c: char| c.is_ascii_alphabetic())
        && prefix
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"+-.".contains(&b)))
    .then_some(prefix)
}

struct Reader {
    dir: Option<(Dir, PathBuf)>,
    files: Files,
    selected: BTreeSet<String>,
    skills: BTreeSet<String>,
    spellings: BTreeMap<String, String>,
    json_total: u64,
    total: u64,
}

pub(super) fn assemble_folder(
    path: &Path,
    manifest: Option<super::ProjectFile>,
) -> Result<Project> {
    let dir = Dir::open_ambient_dir(path, ambient_authority()).map_err(|e| io(path, e))?;
    let root = path.canonicalize().map_err(|e| io(path, e))?;
    let mut reader = Reader::new(Some((dir, root)), Files::new());
    if let Some(file) = manifest {
        let canonical = reader.resolve(MANIFEST, MANIFEST, "")?;
        reader.total = file.bytes.len() as u64;
        reader.files.insert(canonical, file);
    }
    reader.assemble()
}

pub(super) fn assemble_archive(files: Files) -> Result<Project> {
    Reader::new(None, files).assemble()
}

impl Reader {
    fn new(dir: Option<(Dir, PathBuf)>, files: Files) -> Self {
        Self {
            dir,
            files,
            selected: BTreeSet::new(),
            skills: BTreeSet::new(),
            spellings: BTreeMap::new(),
            json_total: 0,
            total: 0,
        }
    }

    fn select(&mut self, path: &str, limit: u64) -> Result<()> {
        // Folder and archive inputs select the same portable names.
        archive::portable(path)?;
        for (end, _) in path
            .match_indices('/')
            .chain(std::iter::once((path.len(), "")))
        {
            self.spell(&path[..end], path)?;
        }
        self.selected.insert(path.to_owned());
        if self.selected.len() > MAX_FILES {
            return Err(invalid(path, "", "Too many selected project files"));
        }
        if !self.files.contains_key(path) {
            let (dir, _) = self
                .dir
                .as_ref()
                .ok_or_else(|| invalid(path, "", "File absent from archive inventory"))?;
            let remaining = MAX_TOTAL - self.total;
            let file = read_regular(dir, Path::new(path), limit.min(remaining)).map_err(|e| {
                let own_size = dir.symlink_metadata(path).map_or(0, |meta| meta.len());
                if e.kind() == std::io::ErrorKind::FileTooLarge && own_size <= limit {
                    invalid(path, "", "Project selection exceeds the 256 MiB total")
                } else {
                    invalid(path, "", &e.to_string())
                }
            })?;
            self.total += file.bytes.len() as u64;
            self.files.insert(path.to_owned(), file);
        }
        Ok(())
    }

    /// Record one path's spelling. Names that differ only by case folding
    /// or Unicode normalization collide, including directory names.
    fn spell(&mut self, part: &str, path: &str) -> Result<()> {
        if self
            .spellings
            .insert(archive::path_key(part), part.into())
            .is_some_and(|before| before != part)
            || self.spellings.len() > 16_384
        {
            return Err(invalid(
                path,
                "",
                "Case collision or too many project paths",
            ));
        }
        Ok(())
    }

    fn resolve(&mut self, owner: &str, name: &str, pointer: &str) -> Result<String> {
        let error = |detail| invalid(owner, pointer, &format!("{detail}: {name:?}"));
        if name.is_empty()
            || name.starts_with('/')
            || name.contains('\\')
            || name.chars().any(char::is_control)
            || scheme(name).is_some()
            || name.len() > 4096
            || name.split('/').count() > 64
        {
            return Err(error("Invalid project-relative path"));
        }
        // Resolve lexically in both modes, as the official reader does. A
        // component traversed only on the way to `..` may be absent, but an
        // existing component must be neither a symlink nor a regular file.
        let mut parts: Vec<_> = owner
            .rsplit_once('/')
            .map_or("", |(parent, _)| parent)
            .split('/')
            .filter(|part| !part.is_empty())
            .collect();
        for part in name.split('/') {
            match part {
                "" | "." => {}
                ".." => {
                    if parts.pop().is_none() {
                        return Err(error("Path escapes the project"));
                    }
                }
                _ => {
                    if !parts.is_empty() && self.is_file(&parts.join("/")) {
                        return Err(error("Path traverses a regular file"));
                    }
                    parts.push(part);
                    // As the official reader does, a component used only on
                    // the way to `..` still needs a portable, consistent name.
                    let walked = parts.join("/");
                    archive::portable(&walked).map_err(|_| error("Invalid portable path"))?;
                    self.spell(&walked, &walked)
                        .map_err(|_| error("Case collision or too many project paths"))?;
                    if let Some((dir, _)) = &self.dir {
                        if dir
                            .symlink_metadata(parts.join("/"))
                            .is_ok_and(|meta| meta.is_symlink())
                        {
                            return Err(error("Path contains a symbolic link"));
                        }
                    }
                }
            }
        }
        let path = parts.join("/");
        if path.is_empty() {
            return Err(error("Expected a file inside the project"));
        }
        if self.dir.is_none() && !self.files.contains_key(&path) {
            return Err(error("File absent from archive inventory"));
        }
        Ok(path)
    }

    fn is_file(&self, path: &str) -> bool {
        match &self.dir {
            Some((dir, _)) => dir.symlink_metadata(path).is_ok_and(|meta| meta.is_file()),
            None => self.files.contains_key(path),
        }
    }

    fn json(&mut self, file: &str) -> Result<Value> {
        self.select(file, MAX_JSON)?;
        let bytes = &self.files[file].bytes;
        // As in the official reader, every reference counts toward the
        // aggregate, so any project accepted here can be packed by the tool.
        self.json_total += bytes.len() as u64;
        if bytes.len() as u64 > MAX_JSON || self.json_total > MAX_JSON_TOTAL {
            return Err(invalid(
                file,
                "",
                "Project JSON exceeds per-file or aggregate limit",
            ));
        }
        parse_json(bytes, file, true)
    }

    fn component(
        &mut self,
        origins: &mut BTreeMap<String, String>,
        value: Value,
        pointer: &str,
    ) -> Result<Value> {
        if value
            .as_object()
            .is_some_and(|object| object.len() == 1 && object.contains_key("file"))
        {
            let path = self.resolve(
                MANIFEST,
                string(&value["file"], MANIFEST, &child(pointer, "file"))?,
                &child(pointer, "file"),
            )?;
            let component = self.json(&path)?;
            origins.insert(pointer.into(), path);
            return Ok(component);
        }
        Ok(value)
    }

    fn assemble(mut self) -> Result<Project> {
        let entry = self.resolve(MANIFEST, MANIFEST, "")?;
        let config = self.json(&entry)?;
        let mut manifest = object(config, MANIFEST, "")?;
        if manifest.remove("projectFormat") != Some(Value::from("agsdl-project-1")) {
            return Err(invalid(
                MANIFEST,
                "/projectFormat",
                "Expected agsdl-project-1",
            ));
        }
        if manifest.get("contract").and_then(Value::as_str) != Some(argentic_core::AGSDL_040) {
            return Err(invalid(
                MANIFEST,
                "/contract",
                "Project format 1 requires agsdl-0.4.0",
            ));
        }
        if let Some(schema) = manifest.remove("$schema") {
            string(&schema, MANIFEST, "/$schema")?;
        }
        let settings = take_map(&mut manifest, "settingsFiles")?;
        let mcp = take_map(&mut manifest, "mcpFiles")?;
        let secrets = manifest.remove("secrets");
        let mut project = Project {
            document: Value::Null,
            secrets: BTreeMap::new(),
            mcp_files: BTreeMap::new(),
            files: Files::new(),
            origins: BTreeMap::new(),
        };
        for key in SECTIONS {
            if let Some(section) = manifest.remove(*key) {
                let pointer = child("", key);
                let mut components = Map::new();
                for (name, item) in object(section, MANIFEST, &pointer)? {
                    let value =
                        self.component(&mut project.origins, item, &child(&pointer, &name))?;
                    components.insert(name, value);
                }
                manifest.insert((*key).into(), Value::Object(components));
            }
        }
        if let Some(flow) = manifest.remove("flow") {
            manifest.insert(
                "flow".into(),
                self.component(&mut project.origins, flow, "/flow")?,
            );
        }
        for (agent, name) in settings {
            let pointer = child("/settingsFiles", &agent);
            let Some(target) = manifest
                .get_mut("agents")
                .and_then(Value::as_object_mut)
                .and_then(|agents| agents.get_mut(&agent))
                .and_then(Value::as_object_mut)
            else {
                return Err(invalid(
                    MANIFEST,
                    &pointer,
                    "Unknown agent or invalid agent component",
                ));
            };
            if target.contains_key("harnessSettings") {
                return Err(invalid(
                    MANIFEST,
                    &pointer,
                    "Agent already declares harnessSettings",
                ));
            }
            let path = self.resolve(MANIFEST, string(&name, MANIFEST, &pointer)?, &pointer)?;
            let value = self.json(&path)?;
            let fields = object(value, &path, "")?;
            target.insert("harnessSettings".into(), Value::Object(fields));
            project
                .origins
                .insert(child(&child("/agents", &agent), "harnessSettings"), path);
        }
        project.document = Value::Object(manifest);
        validate(&project)?;
        let mut doc = std::mem::take(&mut project.document);
        for key in SECTIONS {
            if let Some(section) = doc.get_mut(key).and_then(Value::as_object_mut) {
                for (name, component) in section {
                    let pointer = child(&child("", key), name);
                    let (owner, local) = project.origin(&pointer);
                    self.rebase(key, component, owner, local)?;
                }
            }
        }
        if let Some(flow) = doc.get_mut("flow") {
            let (owner, local) = project.origin("/flow");
            self.rebase("flow", flow, owner, local)?;
        }
        self.mcp_declarations(&mut project, &doc, secrets, mcp)?;
        if self.dir.is_none() && self.selected.len() != self.files.len() {
            let extra = self
                .files
                .keys()
                .find(|name| !self.selected.contains(*name))
                .unwrap();
            return Err(invalid(
                extra,
                "",
                "Inventory file is not selected by the project",
            ));
        }
        project.document = doc;
        project.files = self.files;
        Ok(project)
    }

    fn mcp_declarations(
        &mut self,
        project: &mut Project,
        doc: &Value,
        secrets: Option<Value>,
        mcp: Map<String, Value>,
    ) -> Result<()> {
        if let Some(secrets) = secrets {
            let reference = object(secrets, MANIFEST, "/secrets")?;
            if reference.len() != 1 || !reference.contains_key("file") {
                return Err(invalid(MANIFEST, "/secrets", "Expected a file reference"));
            }
            let path = self.resolve(
                MANIFEST,
                string(&reference["file"], MANIFEST, "/secrets/file")?,
                "/secrets/file",
            )?;
            let requirements = object(self.json(&path)?, &path, "")?;
            static SECRET: OnceLock<regex::Regex> = OnceLock::new();
            let name_pattern =
                SECRET.get_or_init(|| regex::Regex::new(r"^[A-Za-z_][A-Za-z0-9_]*\z").unwrap());
            for (server, values) in requirements {
                let pointer = child("", &server);
                if doc["mcpServers"].get(&server).is_none() {
                    return Err(invalid(&path, &pointer, "Unknown MCP server"));
                }
                let names = list(&values, &path, &pointer)?;
                for (i, name) in names.iter().enumerate() {
                    if !name_pattern.is_match(name) {
                        return Err(invalid(
                            &path,
                            &child(&pointer, &i.to_string()),
                            "Invalid secret name",
                        ));
                    }
                }
                project.secrets.insert(server, names);
            }
        }
        for (server, values) in mcp {
            let pointer = child("/mcpFiles", &server);
            if doc["mcpServers"][&server].get("command").is_none() {
                return Err(invalid(
                    MANIFEST,
                    &pointer,
                    "Expected a declared local MCP server",
                ));
            }
            let mut paths = Vec::new();
            for (i, name) in list(&values, MANIFEST, &pointer)?.iter().enumerate() {
                let path = self.resolve(MANIFEST, name, &child(&pointer, &i.to_string()))?;
                self.select(&path, MAX_TOTAL)?;
                paths.push(path);
            }
            project.mcp_files.insert(server, paths);
        }
        Ok(())
    }

    fn rebase(
        &mut self,
        kind: &str,
        component: &mut Value,
        owner: &str,
        pointer: &str,
    ) -> Result<()> {
        match kind {
            "skills" | "content" => self.uri(component, owner, pointer, kind == "skills"),
            "mcpServers" => {
                if let Some(url) = component["url"].as_str() {
                    if scheme(url).is_none_or(|scheme| scheme.eq_ignore_ascii_case("file")) {
                        return Err(invalid(
                            owner,
                            &child(pointer, "url"),
                            "Remote MCP endpoint requires an absolute non-file URI",
                        ));
                    }
                }
                Ok(())
            }
            "agents" => self.message(component, owner, pointer, "instructions"),
            _ => {
                self.step(component, owner, pointer)?;
                if let Some(steps) = component.get_mut("steps").and_then(Value::as_object_mut) {
                    for (name, step) in steps {
                        self.step(step, owner, &child(&child(pointer, "steps"), name))?;
                    }
                }
                Ok(())
            }
        }
    }

    fn step(&mut self, step: &mut Value, owner: &str, pointer: &str) -> Result<()> {
        self.message(step, owner, pointer, "prompt")?;
        if let Some(next) = step.get_mut("next") {
            let at = child(pointer, "next");
            if next.is_object() && next.get("step").is_none() && next.get("output").is_none() {
                for (name, route) in next.as_object_mut().unwrap() {
                    self.route(route, owner, &child(&at, name))?;
                }
            } else {
                self.route(next, owner, &at)?;
            }
        }
        if let Some(route) = step.get_mut("onError") {
            self.route(route, owner, &child(pointer, "onError"))?;
        }
        Ok(())
    }

    fn route(&mut self, route: &mut Value, owner: &str, pointer: &str) -> Result<()> {
        let mut target = |value: &mut Value, pointer: &str| -> Result<()> {
            if let Some(message) = value.get_mut("message") {
                self.message(message, owner, &child(pointer, "message"), "prompt")?;
            }
            Ok(())
        };
        if let Some(items) = route.as_array_mut() {
            for (i, item) in items.iter_mut().enumerate() {
                target(item, &child(pointer, &i.to_string()))?;
            }
        } else {
            target(route, pointer)?;
        }
        Ok(())
    }

    fn message(&mut self, value: &mut Value, owner: &str, pointer: &str, key: &str) -> Result<()> {
        if let Some(items) = value.get_mut(key).and_then(Value::as_array_mut) {
            for (i, source) in items.iter_mut().enumerate() {
                self.uri(
                    source,
                    owner,
                    &child(&child(pointer, key), &i.to_string()),
                    false,
                )?;
            }
        }
        if let Some(items) = value.get_mut("resources").and_then(Value::as_object_mut) {
            for (name, source) in items {
                self.uri(
                    source,
                    owner,
                    &child(&child(pointer, "resources"), name),
                    false,
                )?;
            }
        }
        Ok(())
    }

    fn uri(&mut self, source: &mut Value, owner: &str, pointer: &str, skill: bool) -> Result<()> {
        let Some(uri) = source.get("uri").and_then(Value::as_str) else {
            return Ok(());
        };
        let pointer = child(pointer, "uri");
        if let Some(scheme) = scheme(uri) {
            return if scheme.eq_ignore_ascii_case("file") {
                Err(invalid(
                    owner,
                    &pointer,
                    "Absolute file URIs are not allowed",
                ))
            } else {
                Ok(())
            };
        }
        let split = uri.find(['?', '#']).unwrap_or(uri.len());
        let path = percent_decode_str(&uri[..split])
            .decode_utf8()
            .map_err(|_| invalid(owner, &pointer, "URI path is not UTF-8"))?;
        let task = if path.is_empty() {
            if owner == MANIFEST {
                self.resolve(MANIFEST, MANIFEST, &pointer)?
            } else {
                owner.into()
            }
        } else {
            self.resolve(owner, &path, &pointer)?
        };
        self.select(&task, MAX_TOTAL).map_err(|cause| {
            // `select` already names the selected file; keep one location.
            let detail = cause
                .message
                .strip_prefix(&format!("{task}#: "))
                .unwrap_or(&cause.message);
            invalid(owner, &pointer, &format!("{task}: {detail}"))
        })?;
        const ENCODE: percent_encoding::AsciiSet = NON_ALPHANUMERIC
            .remove(b'/')
            .remove(b'-')
            .remove(b'.')
            .remove(b'_')
            .remove(b'~');
        source["uri"] = Value::from(format!(
            "{}{}",
            utf8_percent_encode(&task, &ENCODE),
            &uri[split..]
        ));
        if skill {
            let (parent, _) = task.rsplit_once('/').ok_or_else(|| {
                invalid(
                    owner,
                    &pointer,
                    "A skill directory must be below the project root",
                )
            })?;
            self.skill_tree(parent)?;
        }
        Ok(())
    }

    fn skill_tree(&mut self, directory: &str) -> Result<()> {
        if !self.skills.insert(directory.to_owned()) {
            return Ok(());
        }
        let mut pending = vec![PathBuf::from(directory)];
        let mut entries = 0;
        if let Some((dir, _)) = &self.dir {
            let mut names = Vec::new();
            let mut directories = Vec::new();
            while let Some(path) = pending.pop() {
                for item in dir.read_dir(&path).map_err(|e| io(&path, e))? {
                    let item = item.map_err(|e| io(&path, e))?;
                    let relative = path.join(item.file_name());
                    entries += 1;
                    if entries > 16_384 || relative.components().count() > 64 {
                        return Err(invalid(directory, "", "Skill tree exceeds path limits"));
                    }
                    let ty = item.file_type().map_err(|e| io(&relative, e))?;
                    if ty.is_dir() {
                        // The official walk checks directories too, including
                        // empty ones that select no file.
                        let name = relative
                            .to_str()
                            .ok_or_else(|| invalid(directory, "", "Expected UTF-8 skill path"))?
                            .replace(std::path::MAIN_SEPARATOR, "/");
                        directories.push(name);
                        pending.push(relative);
                    } else if ty.is_file() {
                        let name = relative
                            .to_str()
                            .ok_or_else(|| invalid(directory, "", "Expected UTF-8 skill path"))?;
                        names.push(name.replace(std::path::MAIN_SEPARATOR, "/"));
                    } else {
                        return Err(invalid(
                            directory,
                            "",
                            "Skill tree contains a link or special file",
                        ));
                    }
                }
            }
            for name in directories {
                archive::portable(&name)?;
                self.spell(&name, &name)?;
            }
            for name in names {
                self.select(&name, MAX_TOTAL)?;
            }
        } else {
            let prefix = format!("{directory}/");
            let names: Vec<_> = self
                .files
                .range(prefix.clone()..)
                .map(|(name, _)| name)
                .take_while(|name| name.starts_with(&prefix))
                .cloned()
                .collect();
            for name in names {
                self.select(&name, MAX_TOTAL)?;
            }
        }
        Ok(())
    }
}

fn validate(project: &Project) -> Result<()> {
    let authored = serde_json::to_vec(&project.document)?;
    let report = argentic_core::validate_document_json(&authored, None)
        .map_err(|e| invalid(MANIFEST, &e.pointer, &e.message))?;
    let report: Value = serde_json::from_str(&report)?;
    if !report["valid"].as_bool().unwrap_or(false) {
        let diagnostic = report["diagnostics"]
            .as_array()
            .and_then(|items| items.iter().find(|d| d["severity"] == "error"));
        if let Some(diagnostic) = diagnostic {
            let location = diagnostic["path"].as_str().unwrap_or("");
            let (file, pointer) = project.origin(location);
            return Err(invalid(
                file,
                pointer,
                diagnostic["message"].as_str().unwrap_or("Invalid document"),
            ));
        }
        return Err(invalid(MANIFEST, "", "Invalid assembled document"));
    }
    Ok(())
}

fn object(value: Value, file: &str, pointer: &str) -> Result<Map<String, Value>> {
    match value {
        Value::Object(map) => Ok(map),
        _ => Err(invalid(file, pointer, "Expected a JSON object")),
    }
}

fn take_map(manifest: &mut Map<String, Value>, field: &str) -> Result<Map<String, Value>> {
    manifest
        .remove(field)
        .map(|v| object(v, MANIFEST, &child("", field)))
        .transpose()
        .map(Option::unwrap_or_default)
}

fn string<'a>(value: &'a Value, file: &str, pointer: &str) -> Result<&'a str> {
    value
        .as_str()
        .ok_or_else(|| invalid(file, pointer, "Expected a string"))
}

fn list(value: &Value, file: &str, pointer: &str) -> Result<Vec<String>> {
    let items = value
        .as_array()
        .filter(|items| !items.is_empty())
        .ok_or_else(|| invalid(file, pointer, "Expected a nonempty list"))?;
    let mut seen = BTreeSet::new();
    let mut names = Vec::new();
    for (index, item) in items.iter().enumerate() {
        let at = child(pointer, &index.to_string());
        let name = string(item, file, &at)?;
        if !seen.insert(name) {
            return Err(invalid(file, &at, "Duplicate declaration"));
        }
        names.push(name.into());
    }
    Ok(names)
}
