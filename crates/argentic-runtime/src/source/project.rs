use super::{
    archive, encode_document, invalid, io, member, open_regular, parse_json, project_failure,
    read_regular, source_limit, Files, SourceBudget, GRAPH_LIMIT_CODE, MANIFEST, MAX_FILES,
    MAX_JSON, MAX_JSON_TOTAL, MAX_TOTAL,
};
use crate::persistence::{dto::SourceErrorKind, Result};
use cap_std::fs::Dir;
use percent_encoding::{percent_decode_str, utf8_percent_encode, NON_ALPHANUMERIC};
use serde_json::{Map, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    io::Read,
    path::{Path, PathBuf},
    sync::OnceLock,
};

const MODE_SIDECAR: &str = ".agsdl-executables";
const MODE_HEADER: &[u8] = b"AgSDL executable paths v1\n";

const SECTIONS: &[&str] = &["agents", "compositions", "content", "skills", "mcpServers"];

pub(super) struct Project {
    pub document: Value,
    pub(super) budget: SourceBudget,
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

struct Reader<'a> {
    dir: Option<&'a Dir>,
    budget: SourceBudget,
    files: Files,
    selected: BTreeSet<String>,
    skills: BTreeSet<String>,
    spellings: BTreeMap<String, String>,
    json_total: u64,
    total: u64,
}

#[cfg(test)]
pub(super) fn assemble_folder(
    dir: Dir,
    manifest: Option<super::ProjectFile>,
    native_modes: bool,
) -> Result<Project> {
    assemble_folder_with_budget(dir, manifest, native_modes, SourceBudget::default())
}

pub(super) fn assemble_folder_with_budget(
    dir: Dir,
    manifest: Option<super::ProjectFile>,
    native_modes: bool,
    budget: SourceBudget,
) -> Result<Project> {
    let mut reader = Reader::new(Some(&dir), Files::new(), budget);

    if let Some(file) = manifest {
        let canonical = reader.resolve(MANIFEST, MANIFEST, "")?;
        reader.total = file.bytes.len() as u64;
        reader.files.insert(canonical, file);
    }
    let mut project = reader.assemble()?;
    if !native_modes {
        for file in project.files.values_mut() {
            file.executable = false;
        }
        restore_executable_modes(&dir, &mut project.files, &project.budget)?;
    }
    Ok(project)
}

fn sidecar_name(name: &str) -> bool {
    let Some((prefix, suffix)) = name.split_at_checked(MODE_SIDECAR.len()) else {
        return false;
    };
    prefix == MODE_SIDECAR
        && (suffix.is_empty()
            || suffix.strip_prefix('.').is_some_and(|suffix| {
                !suffix.is_empty() && suffix.bytes().all(|b| b.is_ascii_digit())
            }))
}

fn restore_executable_modes(dir: &Dir, files: &mut Files, budget: &SourceBudget) -> Result<()> {
    let root = Path::new(MANIFEST);
    // Both folder and archive assembly preserve the project's selection.
    // Only a root file outside it can carry unpacker metadata.
    let selected = files
        .keys()
        .map(|name| archive::path_key(name))
        .collect::<BTreeSet<_>>();
    for entry in dir.entries().map_err(|e| io(root, e))? {
        budget.work(16, "").map_err(|e| source_limit(MANIFEST, e))?;
        let entry = entry.map_err(|e| io(root, e))?;
        let name = entry.file_name();
        let Some(name) = name.to_str().filter(|name| sidecar_name(name)) else {
            continue;
        };
        if selected.contains(&archive::path_key(name))
            || !entry
                .file_type()
                .map_err(|e| io(Path::new(name), e))?
                .is_file()
        {
            continue;
        }
        let path = Path::new(name);
        let mut input = open_regular(dir, path).map_err(|e| io(path, e))?;
        let mut header = [0; MODE_HEADER.len()];
        match input.read_exact(&mut header) {
            Ok(()) if header == MODE_HEADER => {}
            Ok(()) => continue,
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => continue,
            Err(error) => return Err(io(path, error)),
        }
        let mut bytes = Vec::new();
        input
            .take(archive::INDEX_LIMIT + 1)
            .read_to_end(&mut bytes)
            .map_err(|e| io(path, e))?;
        if bytes.len() as u64 > archive::INDEX_LIMIT {
            return Err(invalid(
                name,
                "",
                "Executable metadata exceeds the 2 MiB limit",
            ));
        }
        let paths = std::str::from_utf8(&bytes)
            .map_err(|_| invalid(name, "", "Executable metadata must be UTF-8"))?;
        for path in paths.split_terminator('\n') {
            if let Some(file) = files.get_mut(path) {
                file.executable = true;
            }
        }
        return Ok(());
    }
    Ok(())
}

pub(super) fn assemble_archive(files: Files, budget: SourceBudget) -> Result<Project> {
    Reader::new(None, files, budget).assemble()
}

impl<'a> Reader<'a> {
    fn new(dir: Option<&'a Dir>, files: Files, budget: SourceBudget) -> Self {
        Self {
            dir,
            budget,
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
        self.budget
            .work(1 + path.len().div_ceil(8), "")
            .map_err(|e| source_limit(path, e))?;
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
            let dir = self
                .dir
                .as_ref()
                .ok_or_else(|| invalid(path, "", "File absent from archive inventory"))?;
            let remaining = MAX_TOTAL - self.total;
            let file = read_regular(dir, Path::new(path), limit.min(remaining)).map_err(|e| {
                let own_size = member::metadata(dir, Path::new(path)).map_or(0, |meta| meta.len());
                match e.kind() {
                    std::io::ErrorKind::FileTooLarge if own_size <= limit => {
                        invalid(path, "", "Project selection exceeds the 256 MiB total")
                    }
                    std::io::ErrorKind::FileTooLarge | std::io::ErrorKind::InvalidData => {
                        invalid(path, "", &e.to_string())
                    }
                    _ => io(Path::new(path), e),
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
        self.budget
            .work(1 + part.len().div_ceil(8), "")
            .and_then(|()| self.budget.bytes(part.len() * 3 + 64, ""))
            .map_err(|e| source_limit(path, e))?;
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
        self.budget
            .work(1 + name.len().div_ceil(8), pointer)
            .map_err(|e| source_limit(owner, e))?;
        let error = |detail| {
            let shown = &name[..name.floor_char_boundary(name.len().min(160))];
            invalid(owner, pointer, &format!("{detail}: {shown:?}"))
        };
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
                    let bytes = parts.iter().map(|part| part.len() + 1).sum::<usize>();
                    self.budget
                        .work(1 + bytes.div_ceil(8), pointer)
                        .map_err(|e| source_limit(owner, e))?;
                    self.budget
                        .bytes(128 + bytes * 5, pointer)
                        .map_err(|e| source_limit(owner, e))?;
                    let walked = parts.join("/");
                    archive::portable(&walked).map_err(|_| error("Invalid portable path"))?;
                    self.spell(&walked, &walked).map_err(|cause| {
                        if cause.code == GRAPH_LIMIT_CODE {
                            cause
                        } else {
                            error("Case collision or too many project paths")
                        }
                    })?;
                    if let Some(dir) = &self.dir {
                        if member::metadata(dir, Path::new(&walked))
                            .is_ok_and(|meta| member::is_link(&meta))
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
            Some(dir) => member::metadata(dir, Path::new(path)).is_ok_and(|meta| meta.is_file()),
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
        parse_json(bytes, file, true, &self.budget)
    }

    fn component(
        &mut self,
        origins: &mut BTreeMap<String, String>,
        value: Value,
        pointer: &str,
    ) -> Result<Value> {
        self.budget
            .work(1, pointer)
            .map_err(|e| source_limit(MANIFEST, e))?;
        self.budget
            .bytes(pointer.len() + 96, pointer)
            .map_err(|e| source_limit(MANIFEST, e))?;
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
            budget: self.budget.clone(),
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
        self.budget
            .work(1 + uri.len().div_ceil(8), &pointer)
            .map_err(|e| source_limit(owner, e))?;
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
            if cause.code == GRAPH_LIMIT_CODE {
                return cause;
            }
            // Keep the authored URI in the error and locate an I/O failure
            // at the resource itself.
            let detail = cause
                .message
                .strip_prefix(&format!("{task}#: "))
                .unwrap_or(&cause.message);
            let mut error = invalid(owner, &pointer, &format!("{task}: {detail}"));
            if cause
                .source
                .as_ref()
                .is_some_and(|location| location.kind == SourceErrorKind::Io)
            {
                error.source = cause.source;
            }
            error
        })?;
        const ENCODE: percent_encoding::AsciiSet = NON_ALPHANUMERIC
            .remove(b'/')
            .remove(b'-')
            .remove(b'.')
            .remove(b'_')
            .remove(b'~');
        self.budget
            .bytes(task.len() * 3 + uri.len() - split, &pointer)
            .map_err(|e| source_limit(owner, e))?;
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
        if let Some(dir) = &self.dir {
            let mut names = Vec::new();
            let mut directories = Vec::new();
            while let Some(path) = pending.pop() {
                for item in
                    member::with_directory(dir, &path, Dir::entries).map_err(|e| io(&path, e))?
                {
                    self.budget
                        .work(1, "")
                        .map_err(|e| source_limit(directory, e))?;
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
    let authored = encode_document(&project.document, &project.budget)?;
    argentic_core::validate_source(authored.as_bytes(), &project.budget).map_err(|error| {
        let (file, pointer) = project.origin(&error.pointer);
        let code = if error.code == GRAPH_LIMIT_CODE {
            GRAPH_LIMIT_CODE
        } else {
            "PROJECT"
        };
        project_failure(
            code,
            file,
            pointer,
            SourceErrorKind::InvalidContent,
            &error.message,
        )
    })
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
