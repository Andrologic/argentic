//! Detached native observations and frozen configurations, ported from Macro's
//! configuration030.ts. Neither a module nor a shape check qualifies an adapter.
use super::metadata as m;
use crate::argentic_core::{
    exact::{self, Object, Value as E},
    graph::{analyse_joins, ordered::OrderedMap, ExpansionBudget, Graph, InBranch},
    native::{self, a, fail, o, s, Error, Text, Value},
    reader,
    validate::ModuleMap,
    valueschema::{self, PointerPart},
    SourceBudget,
};
type Result<T> = std::result::Result<T, Error>;
use std::collections::{HashMap, HashSet};

pub(crate) struct CapturedRegistry {
    pub(crate) json: Value,
    pub(crate) modules: ModuleMap,
    pub(crate) ranges: HashMap<String, HashSet<String>>,
}
fn unique(names: impl IntoIterator<Item = Text>, pointer: &str) -> Result<()> {
    let mut seen = std::collections::HashSet::new();
    for (i, name) in names.into_iter().enumerate() {
        if !seen.insert(name) {
            return fail(
                "CORRELATION",
                format!("{pointer}/{i}"),
                "Duplicate observation identity.",
            );
        }
    }
    Ok(())
}
fn json(value: &Value, p: &str, budget: &SourceBudget) -> Result<(String, E)> {
    let units = value.text().map_or(0, |text| text.units().len());
    budget
        .work(units.div_ceil(8), p)
        .map_err(super::limit_error)?;
    budget.bytes(units * 8, p).map_err(super::limit_error)?;
    let raw = m::text(value, p, "UNSUPPORTED")?;
    let Some(raw) = raw.utf8() else {
        return fail("PARSE", p, "Invalid UTF-16 in JSON transport text.");
    };
    budget
        .json_input(raw.as_bytes(), p)
        .map_err(super::limit_error)?;
    let parsed = exact::decode(&raw)
        .map_err(|_| Error::new("UNSUPPORTED", p, "Expected validated exact JSON metadata."))?;
    Ok((raw, parsed))
}
pub(crate) fn nonempty(value: &E) -> bool {
    match value {
        E::String(v) => !Text::from(v.as_str()).trimmed_empty(),
        E::Array(v) => !v.is_empty(),
        E::Object(v) => v.len() > 0,
        _ => false,
    }
}
fn receipt(value: &Value, p: &str, budget: &SourceBudget) -> Result<String> {
    let (captured, parsed) = json(value, p, budget)?;
    if !nonempty(&parsed) {
        return fail(
            "CORRELATION",
            p,
            "A nonempty native authority receipt or envelope is required.",
        );
    }
    Ok(captured)
}
fn nullable(v: &Value, p: &str) -> Result<Value> {
    if matches!(v, Value::Null) {
        Ok(Value::Null)
    } else {
        Ok(s(m::identity(v, p, "UNSUPPORTED")?))
    }
}
fn identities(value: &Value, p: &str) -> Result<Value> {
    let items = m::array(value, p)?
        .iter()
        .enumerate()
        .map(|(i, v)| m::identity(v, &format!("{p}/{i}"), "UNSUPPORTED"))
        .collect::<Result<Vec<_>>>()?;
    unique(items.iter().cloned(), p)?;
    Ok(a(items.into_iter().map(s)))
}
fn objects<'a>(v: &'a Value, p: &str) -> Result<Vec<(&'a Value, String)>> {
    m::array(v, p)?
        .iter()
        .enumerate()
        .map(|(i, v)| {
            let p = format!("{p}/{i}");
            m::object(v, &p, "UNSUPPORTED")?;
            Ok((v, p))
        })
        .collect()
}
fn capabilities(
    v: &Value,
    p: &str,
    budget: &SourceBudget,
    mut collect_range: impl FnMut(&E) -> Result<()>,
) -> Result<Value> {
    let mut result = vec![];
    for (entry, p) in objects(v, p)? {
        let name = m::identity(entry.get("name"), &format!("{p}/name"), "UNSUPPORTED")?;
        if !["native", "approximate", "absent"]
            .iter()
            .any(|s| entry.get("support").eq_str(s))
        {
            return fail(
                "UNSUPPORTED",
                format!("{p}/support"),
                "Unknown observed support level.",
            );
        }
        let evidence_at = format!("{p}/evidenceJson");
        let (evidence, parsed) = json(entry.get("evidenceJson"), &evidence_at, budget)?;
        if !entry.get("support").eq_str("absent") && !nonempty(&parsed) {
            return fail(
                "CORRELATION",
                evidence_at,
                "A nonempty native authority receipt or envelope is required.",
            );
        }
        if entry.get("support").eq_str("native") {
            collect_range(&parsed)?;
        }
        result.push(o([
            ("name", s(name)),
            ("support", entry.get("support").clone()),
            ("evidenceJson", s(evidence)),
        ]));
    }
    unique(
        result.iter().map(|v| v.get("name").text().unwrap().clone()),
        p,
    )?;
    Ok(a(result))
}
fn no_credentials(value: &E, pointer: &str, budget: &SourceBudget) -> Result<()> {
    static PRIVATE: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    static TOKEN: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    let patterns = || {
        let private = PRIVATE
            .get_or_init(|| regex::Regex::new(r"-----BEGIN (?:[A-Z]+ )*PRIVATE KEY-----").unwrap());
        let token=TOKEN.get_or_init(||regex::Regex::new(r"\A(?:(?i-u:Bearer)[\t\n\x0B\x0C\r \x{a0}\x{1680}\x{2000}-\x{200a}\x{2028}\x{2029}\x{202f}\x{205f}\x{3000}\x{feff}]+[^\t\n\x0B\x0C\r \x{a0}\x{1680}\x{2000}-\x{200a}\x{2028}\x{2029}\x{202f}\x{205f}\x{3000}\x{feff}]+|(?i-u:sk-(?:proj-|ant-)?[A-Za-z0-9_-]{16,}))\z").unwrap());
        (private, token)
    };
    enum Task<'a> {
        Visit(&'a E, String),
        Field(String, &'a E, String),
    }
    let mut todo = vec![Task::Visit(value, pointer.to_owned())];
    while let Some(task) = todo.pop() {
        match task {
            Task::Visit(v, p) => {
                budget.work(1, &p).map_err(super::limit_error)?;
                if let E::String(v) = v {
                    budget
                        .work(v.len().div_ceil(8), &p)
                        .map_err(super::limit_error)?;
                    if PRIVATE.get().is_none() || TOKEN.get().is_none() {
                        budget.work(2_048, &p).map_err(super::limit_error)?;
                        budget.bytes(512 * 1024, &p).map_err(super::limit_error)?;
                    }
                    let (private, token) = patterns();
                    if private.is_match(v) || token.is_match(v) {
                        return fail(
                            "SECRET_SETTINGS",
                            p,
                            "Credential material cannot enter a frozen configuration.",
                        );
                    }
                }
                let fields: Vec<(String, &E)> = match v {
                    E::Object(v) => {
                        let keys = v.iter().map(|(k, _)| k.len()).sum::<usize>();
                        budget
                            .work(keys.div_ceil(8) + v.len(), &p)
                            .map_err(super::limit_error)?;
                        budget
                            .bytes(keys * 8 + v.len() * (p.len() + 128), &p)
                            .map_err(super::limit_error)?;
                        v.iter().map(|(k, v)| (k.to_owned(), v)).collect()
                    }
                    E::Array(v) => {
                        budget.work(v.len(), &p).map_err(super::limit_error)?;
                        budget
                            .bytes(v.len() * (p.len() + 144), &p)
                            .map_err(super::limit_error)?;
                        v.iter()
                            .enumerate()
                            .map(|(i, v)| (i.to_string(), v))
                            .collect()
                    }
                    _ => vec![],
                };
                for (k, v) in fields.into_iter().rev() {
                    let child = exact::at(&p, &k);
                    todo.push(Task::Field(k, v, child));
                }
            }
            Task::Field(key, v, p) => {
                let normalized = key.replace(['-', '_'], "").to_lowercase();
                if [
                    "apikey",
                    "secret",
                    "clientsecret",
                    "password",
                    "passwd",
                    "token",
                    "accesstoken",
                    "refreshtoken",
                    "authorization",
                    "credential",
                    "credentials",
                    "privatekey",
                ]
                .contains(&normalized.as_str())
                {
                    return fail(
                        "SECRET_SETTINGS",
                        p,
                        "Credentials belong outside captured settings.",
                    );
                }
                todo.push(Task::Visit(v, p));
            }
        }
    }
    Ok(())
}
fn settings(v: &Value, p: &str, budget: &SourceBudget) -> Result<String> {
    let (captured, value) = json(v, p, budget)?;
    exact::record(Some(&value), p)?;
    no_credentials(&value, p, budget)?;
    Ok(captured)
}
pub(crate) fn capture_registry(input: &Value, budget: &SourceBudget) -> Result<CapturedRegistry> {
    m::object(input, "/registry", "UNSUPPORTED")?;
    if !input.get("format").eq_str("macro-agsdl-registry/030") {
        return fail(
            "CORRELATION",
            "/registry/format",
            "Unknown captured registry format.",
        );
    }
    let version = m::identity(input.get("version"), "/registry/version", "UNSUPPORTED")?;
    let mut modules = vec![];
    let mut module_map = ModuleMap::new();
    for (entry, p) in objects(input.get("modules"), "/registry/modules")? {
        let harness = m::identity(entry.get("harness"), &format!("{p}/harness"), "UNSUPPORTED")?;
        let at = format!("{p}/moduleJson");
        let raw = m::text(entry.get("moduleJson"), &at, "UNSUPPORTED")?
            .utf8()
            .ok_or_else(|| {
                Error::new(
                    "PARSE",
                    at.as_str(),
                    "Invalid UTF-16 in JSON transport text.",
                )
            })?;
        let loaded =
            reader::load_harness_module_with_budget(raw.as_bytes(), budget).map_err(|error| {
                match error {
                    crate::validate::ReadError::Internal(error)
                        if error.limit_pointer.is_some() =>
                    {
                        Error::new(crate::GRAPH_LIMIT_CODE, at.as_str(), error.message)
                    }
                    _ => Error::new(
                        "HARNESS_MODULE",
                        at.as_str(),
                        "Invalid AgSDL 0.3 harness module.",
                    ),
                }
            })?;
        if !harness.eq_str(exact::text(
            exact::record(Some(&loaded), "")?.get("name"),
            "",
        )?) {
            return fail(
                "CORRELATION",
                format!("{p}/harness"),
                "Module name differs from its registry identity.",
            );
        }
        module_map.set(harness.utf8().expect("module name is exact JSON"), loaded);
        modules.push(o([("harness", s(harness)), ("moduleJson", s(raw))]));
    }
    unique(
        modules
            .iter()
            .map(|v| v.get("harness").text().unwrap().clone()),
        "/registry/modules",
    )?;
    let host = if matches!(input.get("hostCapabilities"), Value::Undefined) {
        None
    } else {
        Some(capabilities(
            input.get("hostCapabilities"),
            "/registry/hostCapabilities",
            budget,
            |_| Ok(()),
        )?)
    };
    let mut harnesses = vec![];
    let mut ranges = HashMap::new();
    for (entry, p) in objects(input.get("harnesses"), "/registry/harnesses")? {
        let harness = m::identity(entry.get("harness"), &format!("{p}/harness"), "UNSUPPORTED")?;
        if !entry.get("installation").eq_str("available")
            && !entry.get("installation").eq_str("missing")
        {
            return fail(
                "UNSUPPORTED",
                format!("{p}/installation"),
                "Expected an installation observation.",
            );
        }
        if !matches!(entry.get("qualified"), Value::Bool(_)) {
            return fail(
                "UNSUPPORTED",
                format!("{p}/qualified"),
                "Expected a qualification observation.",
            );
        }
        let mut verified_ranges = HashSet::new();
        let caps = capabilities(
            entry.get("capabilities"),
            &format!("{p}/capabilities"),
            budget,
            |proof| {
                let Some(proof) = proof.as_object() else {
                    return Ok(());
                };
                if proof.get("kind").and_then(E::as_str) == Some("harness-version-range")
                    && proof
                        .get("harness")
                        .and_then(E::as_str)
                        .is_some_and(|s| harness.eq_str(s))
                    && proof
                        .get("version")
                        .and_then(E::as_str)
                        .is_some_and(|s| entry.get("version").eq_str(s))
                    && matches!(proof.get("satisfied"), Some(E::Bool(true)))
                {
                    if let Some(range) = proof.get("range").and_then(E::as_str) {
                        if proof.get("receipt").is_some_and(nonempty)
                            && !verified_ranges.contains(range)
                        {
                            budget
                                .bytes(range.len() + 128, &p)
                                .map_err(super::limit_error)?;
                            verified_ranges.insert(range.to_owned());
                        }
                    }
                }
                Ok(())
            },
        )?;
        if !verified_ranges.is_empty() {
            if let Some(harness) = harness.utf8() {
                ranges.insert(harness, verified_ranges);
            }
        }
        let mut models = vec![];
        for (model, p) in objects(entry.get("models"), &format!("{p}/models"))? {
            let name = m::identity(model.get("name"), &format!("{p}/name"), "UNSUPPORTED")?;
            let efforts = identities(model.get("efforts"), &format!("{p}/efforts"))?;
            let default = nullable(model.get("defaultEffort"), &format!("{p}/defaultEffort"))?;
            if !matches!(default, Value::Null)
                && !efforts
                    .array()
                    .unwrap()
                    .iter()
                    .any(|v| native::equal(v, &default))
            {
                return fail(
                    "MODEL_UNAVAILABLE",
                    format!("{p}/defaultEffort"),
                    "Default effort is absent from the observed model catalogue.",
                );
            }
            models.push(o([
                ("name", s(name)),
                ("efforts", efforts),
                ("defaultEffort", default),
            ]));
        }
        unique(
            models.iter().map(|v| v.get("name").text().unwrap().clone()),
            &format!("{p}/models"),
        )?;
        let default = nullable(entry.get("defaultModel"), &format!("{p}/defaultModel"))?;
        if !matches!(default, Value::Null)
            && !models
                .iter()
                .any(|v| native::equal(v.get("name"), &default))
        {
            return fail(
                "MODEL_UNAVAILABLE",
                format!("{p}/defaultModel"),
                "Default model is absent from the observed catalogue.",
            );
        }
        let version = nullable(entry.get("version"), &format!("{p}/version"))?;
        let adapter = nullable(entry.get("adapterId"), &format!("{p}/adapterId"))?;
        harnesses.push(o([
            ("harness", s(harness)),
            ("version", version),
            ("installation", entry.get("installation").clone()),
            ("adapterId", adapter),
            ("qualified", entry.get("qualified").clone()),
            ("capabilities", caps),
            ("models", a(models)),
            ("defaultModel", default),
        ]));
    }
    unique(
        harnesses
            .iter()
            .map(|v| v.get("harness").text().unwrap().clone()),
        "/registry/harnesses",
    )?;
    let mut agents = vec![];
    for (entry, p) in objects(input.get("agents"), "/registry/agents")? {
        let (raw, value) = json(
            entry.get("agentDeclarationJson"),
            &format!("{p}/agentDeclarationJson"),
            budget,
        )?;
        let declaration = exact::record(Some(&value), &format!("{p}/agentDeclarationJson"))?;
        if let Some(v) = declaration.get("harnessSettings") {
            no_credentials(
                v,
                &format!("{p}/agentDeclarationJson/harnessSettings"),
                budget,
            )?;
        }
        let mut agent = o([
            (
                "agentName",
                s(m::identity(
                    entry.get("agentName"),
                    &format!("{p}/agentName"),
                    "UNSUPPORTED",
                )?),
            ),
            ("agentDeclarationJson", s(raw)),
            (
                "settingsJson",
                s(settings(
                    entry.get("settingsJson"),
                    &format!("{p}/settingsJson"),
                    budget,
                )?),
            ),
            (
                "permissionsJson",
                s(receipt(
                    entry.get("permissionsJson"),
                    &format!("{p}/permissionsJson"),
                    budget,
                )?),
            ),
            (
                "scopeJson",
                s(receipt(
                    entry.get("scopeJson"),
                    &format!("{p}/scopeJson"),
                    budget,
                )?),
            ),
            (
                "evidenceJson",
                s(receipt(
                    entry.get("evidenceJson"),
                    &format!("{p}/evidenceJson"),
                    budget,
                )?),
            ),
            (
                "provenanceJson",
                s(receipt(
                    entry.get("provenanceJson"),
                    &format!("{p}/provenanceJson"),
                    budget,
                )?),
            ),
        ]);
        let model = entry.get("modelOverride");
        if !matches!(model, Value::Undefined) {
            let pointer = format!("{p}/modelOverride");
            if m::object(model, &pointer, "MODEL_OVERRIDE")?
                .iter()
                .any(|(key, _)| !["name", "effort"].iter().any(|field| key.eq_str(field)))
            {
                return fail("MODEL_OVERRIDE", pointer, "Unknown model override field.");
            }
            let name = m::identity(
                model.get("name"),
                &format!("{pointer}/name"),
                "MODEL_OVERRIDE",
            )?;
            let mut replacement = o([("name", s(name))]);
            let effort = model.get("effort");
            if !matches!(effort, Value::Undefined) {
                let effort = m::identity(effort, &format!("{pointer}/effort"), "MODEL_OVERRIDE")?;
                replacement.set("effort", s(effort));
            }
            agent.set("modelOverride", replacement);
        }
        agents.push(agent);
    }
    unique(
        agents
            .iter()
            .map(|v| v.get("agentName").text().unwrap().clone()),
        "/registry/agents",
    )?;
    let mut captured = o([
        ("format", s("macro-agsdl-registry/030")),
        ("version", s(version)),
        ("modules", a(modules)),
        ("harnesses", a(harnesses)),
        ("agents", a(agents)),
    ]);
    if let Some(host) = host {
        captured.set("hostCapabilities", host);
    }
    Ok(CapturedRegistry {
        json: captured,
        modules: module_map,
        ranges,
    })
}
fn capability(observation: &Value, name: &str, p: &str, native: bool) -> Result<()> {
    let cap = observation
        .get("capabilities")
        .array()
        .unwrap()
        .iter()
        .find(|v| v.get("name").eq_str(name));
    if cap.is_none()
        || cap.unwrap().get("support").eq_str("absent")
        || native && !cap.unwrap().get("support").eq_str("native")
    {
        return fail(
            "HARNESS_CAPABILITY",
            p,
            format!(
                "The observed adapter lacks {}qualification for {name}.",
                if native { "native " } else { "" }
            ),
        );
    }
    Ok(())
}
fn range(
    observation: &Value,
    verified: Option<&HashSet<String>>,
    range: &str,
    p: &str,
    budget: &SourceBudget,
) -> Result<()> {
    budget
        .work(range.len().div_ceil(8) + 1, p)
        .map_err(super::limit_error)?;
    if verified.is_some_and(|ranges| ranges.contains(range)) {
        return Ok(());
    }
    budget
        .bytes(range.len() * 4 + 512, p)
        .map_err(super::limit_error)?;
    let mut message = Text::from("No native range evaluation binds harness ");
    message.push(observation.get("harness").text().unwrap());
    message.push(&Text::from(", version "));
    if let Some(v) = observation.get("version").text() {
        message.push(v)
    } else {
        message.push(&Text::from("null"));
    }
    message.push(&Text::from(format!(", and range {range}.")));
    fail("HARNESS_VERSION_UNVERIFIED", p, message)
}
fn internal<T>(_: T) -> Error {
    Error::new(
        "COMPILATION",
        "",
        "Invalid compilation input or inconsistent dependency contract.",
    )
}
fn empty() -> &'static Object {
    crate::argentic_core::graph::empty_object()
}
fn optional_object<'a>(v: Option<&'a E>, p: &str) -> Result<&'a Object> {
    match v {
        None | Some(E::Null) => Ok(empty()),
        v => Ok(exact::record(v, p)?),
    }
}
fn receiver_graph<'a>(document: &'a Object, budget: &SourceBudget) -> Result<Graph<'a>> {
    let mut initial_budget = ExpansionBudget::new(budget);
    let graph = Graph::for_scope(document, None, None, &mut initial_budget)?;
    let (initial, _) = analyse_joins(&graph, &mut initial_budget)?;
    let members = initial
        .values()
        .flat_map(|info| info.branches.values().flat_map(|v| v.iter().cloned()))
        .collect();
    let covered = Graph::flow_error_covered(document, &members, &initial_budget)?;
    let compositions = optional_object(document.get("compositions"), "")?;
    let mut in_branch = InBranch::new();
    let mut local_budget = ExpansionBudget::new(budget);
    for composition in std::iter::once(None).chain(compositions.iter().map(|(name, _)| Some(name)))
    {
        let g = Graph::for_scope(
            document,
            composition,
            composition.is_none().then(|| covered.clone()),
            &mut local_budget,
        )?;
        let (infos, _) = analyse_joins(&g, &mut local_budget)?;
        in_branch.set(
            composition.map(str::to_owned),
            infos
                .values()
                .flat_map(|info| info.branches.values().flat_map(|v| v.iter().cloned()))
                .collect(),
        );
    }
    Ok(Graph::for_flow(
        document,
        Some(std::rc::Rc::new(in_branch)),
        Some(covered),
        &mut ExpansionBudget::new(budget),
    )?)
}

fn receivers<'g>(graph: &'g Graph<'_>) -> Result<std::collections::HashSet<&'g str>> {
    use crate::graph::RouteKind;
    graph.work(graph.edges.len() * 2)?;
    graph.bytes(graph.nodes.len() * 80)?;
    let mut receivers = std::collections::HashSet::new();
    let mut pending = Vec::new();
    let mut add = |addr: &'g str, pending: &mut Vec<&'g str>| {
        if receivers.insert(addr) {
            pending.push(addr);
        }
    };
    if let Some(root) = &graph.root {
        add(root, &mut pending);
    }
    for edge in &graph.edges {
        let has = if let Some(message) = edge.message {
            let message = exact::record(Some(message), "")?;
            message.contains_key("resources")
                && exact::record(message.get("resources"), "")?.len() > 0
        } else {
            edge.kind == RouteKind::Error
                || ["agent", "call", "join"].contains(&graph.node_at(&edge.src)?.kind)
        };
        if has {
            add(&edge.dst, &mut pending);
        }
    }
    while let Some(addr) = pending.pop() {
        if graph.node_at(addr)?.kind != "approval" {
            continue;
        }
        for &edge in graph.outgoing(addr)? {
            let edge = &graph.edges[edge];
            if edge.message.is_none() && edge.kind == RouteKind::Normal {
                add(&edge.dst, &mut pending);
            }
        }
    }
    Ok(receivers)
}

pub(crate) type Use = (String, String, Option<Vec<String>>);
pub(crate) fn used_properties(agent: &Object, p: &str, step_uses: &[Use]) -> Result<Vec<Use>> {
    let mut used: OrderedMap<String, OrderedMap<String, (String, Option<Vec<String>>)>> =
        OrderedMap::new();
    let mut add = |name: &str, where_: String, step: Option<Vec<String>>| {
        if !used.contains_key(name) {
            used.set(name.to_owned(), OrderedMap::new());
        }
        let key = format!(
            "{where_}\0{}",
            step.as_ref().map(|s| s.join("/")).unwrap_or_default()
        );
        used.get_mut(name).unwrap().set(key, (where_, step));
    };
    for key in ["resources", "skills", "mcpServers"] {
        if agent.get(key).is_some_and(|v| match v {
            E::Object(v) => v.len() > 0,
            E::Array(v) => !v.is_empty(),
            E::Number(_) => true,
            _ => false,
        }) {
            add(key, exact::at(p, key), None);
        }
    }
    let model = optional_object(agent.get("model"), &exact::at(p, "model"))?;
    for key in ["name", "effort"] {
        if model.contains_key(key) {
            add(&format!("model.{key}"), format!("{p}/model/{key}"), None);
        }
    }
    if matches!(agent.get("plan"), Some(E::Bool(true))) {
        add("plan", format!("{p}/plan"), None);
    }
    if let Some(value) = agent.get("planFirst") {
        let value = exact::text(Some(value), "")?;
        add(
            &format!("planFirst.{value}"),
            format!("{p}/planFirst"),
            None,
        );
    }
    if matches!(
        agent.get("permissions"),
        Some(E::Object(_)) | Some(E::Null) | Some(E::Array(_)) | Some(E::Number(_))
    ) {
        let permissions = exact::record(agent.get("permissions"), &format!("{p}/permissions"))?;
        if let Some(v) = permissions.get("reviewer") {
            let reviewer = exact::text(Some(v), "")?;
            add(
                &format!("reviewer.{reviewer}"),
                format!("{p}/permissions/reviewer"),
                None,
            );
        }
        if optional_object(permissions.get("directories"), "")?.len() > 0 {
            add("directories", format!("{p}/permissions/directories"), None);
        }
    }
    for (name, at, step) in step_uses {
        add(name, at.clone(), step.clone());
    }
    Ok(used
        .iter()
        .flat_map(|(name, paths)| {
            paths
                .values()
                .map(|(where_, step)| (name.clone(), where_.clone(), step.clone()))
        })
        .collect())
}
pub(crate) fn step_uses(
    document: &Object,
    budget: &SourceBudget,
) -> Result<std::collections::HashMap<String, Vec<Use>>> {
    let mut all = std::collections::HashMap::new();
    let mut record =
        |graph: &Graph<'_>, receivers: &std::collections::HashSet<&str>, is_flow| -> Result<()> {
            graph.work(graph.nodes.len())?;
            graph.bytes(graph.nodes.len() * 384)?;
            for node in graph.nodes.values() {
                let Some(decl) = node.decl else {
                    continue;
                };
                let Some(agent) = &node.agent else { continue };
                let uses = all.entry(agent.clone()).or_insert_with(Vec::new);
                let mut add = |name: &str, at, step| uses.push((name.to_owned(), at, step));
                let where_ = valueschema::path_pointer(&node.pointer);
                let step = if is_flow {
                    Some(node.addr.split('/').map(str::to_owned).collect())
                } else {
                    None
                };
                if decl.get("instance").and_then(E::as_str) == Some("new") {
                    add("instance.new", format!("{where_}/instance"), step.clone());
                } else if decl.get("delivery").and_then(E::as_str) != Some("steer") {
                    add("delivery.queue", format!("{where_}/delivery"), step.clone());
                }
                if decl.get("delivery").and_then(E::as_str) == Some("steer") {
                    add("delivery.steer", format!("{where_}/delivery"), step.clone());
                }
                if is_flow && receivers.contains(node.addr.as_str()) {
                    add("resources", where_, step);
                }
            }
            Ok(())
        };
    if document.contains_key("flow") {
        let graph = receiver_graph(document, budget)?;
        record(&graph, &receivers(&graph)?, true)?;
    }
    let mut expansion = ExpansionBudget::new(budget);
    for (name, _) in optional_object(document.get("compositions"), "")?.iter() {
        let graph = Graph::for_composition(document, name, None, &mut expansion)?;
        record(&graph, &Default::default(), false)?;
    }
    Ok(all)
}

pub(crate) fn capture_agent(
    document: &Object,
    agent_name: &Value,
    captured: &Value,
    modules: &ModuleMap,
    uses: &[Use],
    budget: &SourceBudget,
    ranges: &HashMap<String, HashSet<String>>,
) -> Result<Value> {
    let name = m::identity(agent_name, "/agentName", "UNSUPPORTED")?;
    let pointer = m::at(&Text::from("/agents"), &name);
    let definitions = exact::record(document.get("agents"), "/agents")?;
    let Some(name_utf8) = name.utf8().filter(|name| definitions.contains_key(name)) else {
        return fail("CORRELATION", pointer, "No such agent definition.");
    };
    let p = pointer.utf8().unwrap();
    let agent = exact::record(definitions.get(&name_utf8), &p)?;
    let proof = captured
        .get("agents")
        .array()
        .unwrap()
        .iter()
        .find(|v| v.get("agentName").text() == Some(&name));
    if proof.is_none()
        || !exact::equal(
            &json(proof.unwrap().get("agentDeclarationJson"), &p, budget)?.1,
            definitions.get(&name_utf8).unwrap(),
        )
    {
        return fail(
            "CORRELATION",
            p,
            "No native observation matches the complete agent declaration.",
        );
    }
    let proof = proof.unwrap();
    let harness = exact::text(agent.get("harness"), &format!("{p}/harness"))?;
    let harnesses = captured.get("harnesses").array().unwrap();
    budget
        .work(harnesses.len() * (1 + harness.len().div_ceil(8)), &p)
        .map_err(super::limit_error)?;
    let observation = captured
        .get("harnesses")
        .array()
        .unwrap()
        .iter()
        .find(|v| v.get("harness").eq_str(harness));
    if observation.is_none() || !observation.unwrap().get("installation").eq_str("available") {
        return fail(
            "HARNESS_INSTALLATION",
            format!("{p}/harness"),
            "The requested harness is not observed as installed.",
        );
    }
    let observation = observation.unwrap();
    if matches!(observation.get("adapterId"), Value::Null) {
        return fail(
            "HARNESS_ADAPTER",
            format!("{p}/harness"),
            "No native adapter is registered for this harness.",
        );
    }
    if !matches!(observation.get("qualified"), Value::Bool(true)) {
        return fail(
            "HARNESS_QUALIFICATION",
            format!("{p}/harness"),
            "The installed adapter is not qualified.",
        );
    }
    if matches!(observation.get("version"), Value::Null) {
        return fail(
            "HARNESS_VERSION_UNVERIFIED",
            format!("{p}/harness"),
            "The installed harness version was not observed.",
        );
    }
    let verified_ranges = ranges.get(harness);
    budget
        .work(
            observation.get("capabilities").array().unwrap().len() * (uses.len() + 10),
            &p,
        )
        .map_err(super::limit_error)?;
    for name in [
        "result-submission",
        "continuity",
        "permission-enforcement",
        "contained-stop",
        "response-attribution",
    ] {
        capability(observation, name, &format!("{p}/harness"), true)?;
    }
    for (key, cap) in [
        ("completionCheck", "completion-check"),
        ("answerTimeoutMs", "answer-timeout"),
        ("instructions", "instructions"),
    ] {
        if agent.contains_key(key) {
            capability(observation, cap, &format!("{p}/{key}"), true)?;
        }
    }
    if let Some(v) = agent.get("harnessVersion") {
        range(
            observation,
            verified_ranges,
            exact::text(Some(v), "")?,
            &format!("{p}/harnessVersion"),
            budget,
        )?;
    }
    let module = modules.get(harness);
    for (key, where_, _) in uses {
        capability(
            observation,
            key,
            where_,
            key == "planFirst.ask" || key == "delivery.queue",
        )?;
        if let Some(support) = module
            .and_then(E::as_object)
            .and_then(|m| m.get("properties"))
            .and_then(E::as_object)
            .and_then(|p| p.get(key))
            .and_then(E::as_object)
        {
            if let Some(v) = support.get("versions") {
                range(
                    observation,
                    verified_ranges,
                    exact::text(Some(v), "")?,
                    where_,
                    budget,
                )?;
            }
        }
    }
    let authored_settings = optional_object(
        agent.get("harnessSettings"),
        &format!("{p}/harnessSettings"),
    )?;
    if let Some(v) = agent.get("harnessSettings") {
        no_credentials(v, &format!("{p}/harnessSettings"), budget)?;
    }
    let (_, effective_value) = json(proof.get("settingsJson"), &p, budget)?;
    let effective = exact::record(Some(&effective_value), "")?;
    if agent.contains_key("harnessSettings") || effective.len() > 0 {
        capability(
            observation,
            "harness-settings",
            &format!("{p}/harnessSettings"),
            true,
        )?;
        if let Some(schema) = module
            .and_then(E::as_object)
            .and_then(|m| m.get("settings"))
        {
            let problems = valueschema::validate_with_budget(
                &effective_value,
                schema,
                &[
                    PointerPart::Key("agents".into()),
                    PointerPart::Key(name_utf8.clone()),
                    PointerPart::Key("harnessSettings".into()),
                ],
                budget,
            )
            .map_err(|error| {
                error
                    .limit
                    .map(super::limit_error)
                    .unwrap_or_else(|| internal(()))
            })?;
            if let Some((pointer, message)) = problems.first() {
                return fail("HARNESS_SETTINGS", pointer.clone(), message.clone());
            }
        } else if module.is_some() {
            return fail(
                "HARNESS_SETTINGS",
                format!("{p}/harnessSettings"),
                "The module declares no effective settings schema.",
            );
        }
    }
    for (key, v) in authored_settings.iter() {
        if !effective.get(key).is_some_and(|e| exact::equal(e, v)) {
            return fail(
                "CORRELATION",
                exact::at(&format!("{p}/harnessSettings"), key),
                "Effective settings differ from the requested settings.",
            );
        }
    }
    let replacement = proof.get("modelOverride");
    if !matches!(replacement, Value::Undefined) {
        for key in ["name", "effort"] {
            let value = replacement.get(key);
            if matches!(value, Value::Undefined) {
                continue;
            }
            let capability_name = format!("model.{key}");
            let pointer = format!("{p}/model/{key}");
            capability(observation, &capability_name, &pointer, false)?;
            let support = module
                .and_then(E::as_object)
                .and_then(|m| m.get("properties"))
                .and_then(E::as_object)
                .and_then(|properties| properties.get(&capability_name))
                .and_then(E::as_object)
                .filter(|support| {
                    matches!(
                        support.get("support").and_then(E::as_str),
                        Some("native" | "approximate")
                    )
                })
                .ok_or_else(|| {
                    Error::new(
                        "HARNESS_CAPABILITY",
                        pointer.as_str(),
                        "The harness module does not support the replacement model property.",
                    )
                })?;
            if let Some(E::Array(values)) = support.get("values") {
                let chosen = value.text().and_then(Text::utf8);
                if !values
                    .iter()
                    .any(|allowed| allowed.as_str() == chosen.as_deref())
                {
                    return fail(
                        "HARNESS_VALUE",
                        pointer,
                        "The replacement model property is outside the harness vocabulary.",
                    );
                }
            }
            if let Some(versions) = support.get("versions") {
                range(
                    observation,
                    verified_ranges,
                    exact::text(Some(versions), "")?,
                    &pointer,
                    budget,
                )?;
            }
        }
    }
    let requested = optional_object(agent.get("model"), &format!("{p}/model"))?;
    let model_name = if !matches!(replacement, Value::Undefined) {
        replacement.get("name").clone()
    } else if let Some(v) = requested.get("name") {
        s(exact::text(Some(v), "")?)
    } else {
        observation.get("defaultModel").clone()
    };
    let model_units = model_name.text().map_or(0, |name| name.units().len());
    budget
        .work(
            observation.get("models").array().unwrap().len() * (1 + model_units.div_ceil(8)),
            &p,
        )
        .map_err(super::limit_error)?;
    let model = observation
        .get("models")
        .array()
        .unwrap()
        .iter()
        .find(|v| native::equal(v.get("name"), &model_name));
    let Some(model) = model else {
        return fail(
            "MODEL_UNAVAILABLE",
            format!("{p}/model/name"),
            "The requested or default model is not observed as available.",
        );
    };
    let effort = if !matches!(replacement, Value::Undefined) {
        if matches!(replacement.get("effort"), Value::Undefined) {
            model.get("defaultEffort").clone()
        } else {
            replacement.get("effort").clone()
        }
    } else if let Some(v) = requested.get("effort") {
        s(exact::text(Some(v), "")?)
    } else {
        model.get("defaultEffort").clone()
    };
    if !matches!(effort, Value::Null)
        && !model
            .get("efforts")
            .array()
            .unwrap()
            .iter()
            .any(|v| native::equal(v, &effort))
    {
        return fail(
            "MODEL_UNAVAILABLE",
            format!("{p}/model/effort"),
            "The requested or default effort is not advertised by the observed model.",
        );
    }
    let mut frozen = proof.clone_with_budget(budget, &p)?;
    frozen.set("harness", s(harness));
    frozen.set("harnessVersion", observation.get("version").clone());
    frozen.set("adapterId", observation.get("adapterId").clone());
    frozen.set("modelName", model.get("name").clone());
    frozen.set("modelEffort", effort);
    Ok(frozen)
}
