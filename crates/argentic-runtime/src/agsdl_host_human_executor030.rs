//! Production human waits and the flow-owned monotone clock.
use crate::{
    agsdl_host::HostState,
    agsdl_host_flow030::{self as flow, NativeFlowInspection030},
    agsdl_host_runtime030::{HostExecutor030, RuntimeClock030},
    persistence::{
        failure,
        flow030::{FlowEffect030, FlowIdentity030},
        Result,
    },
    tasks::StreamTasks,
};
use futures::future::BoxFuture;
use serde_json::Value;
use sqlx::SqlitePool;
use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

pub(crate) struct HumanExecutor030 {
    pub pool: SqlitePool,
    pub tasks: StreamTasks,
}

impl HostExecutor030 for HumanExecutor030 {
    fn start(
        &self,
        app: HostState,
        inspection: NativeFlowInspection030,
        effect: FlowEffect030,
        _cancel: CancellationToken,
    ) -> BoxFuture<'static, Result<Option<crate::agsdl_host_call_executor030::StartedCall030>>>
    {
        let (pool, tasks) = (self.pool.clone(), self.tasks.clone());
        Box::pin(async move {
            let host = app.clone();
            #[cfg(test)]
            host.flow030().operator_test_yield("approval.claim").await;
            let claim =
                flow::claim_expected(host.flow030(), &host, &pool, &effect.identity, &effect.id)
                    .await?
                    .ok_or_else(|| {
                        failure("STALE_REFERENCE", "approval", "Approval start changed")
                    })?;
            claim.handoff_host(&tasks, std::future::pending()).await?;
            let admission = async {
                let broker = host
                    .flow030()
                    .humans
                    .broker(&host, &pool, &effect.identity)
                    .await?;
                let _operation = broker.operation.lock().await;
                #[cfg(test)]
                host.flow030()
                    .operator_test_yield("approval.admission")
                    .await;
                broker.admit_host_work(&host, &effect.id).await?;
                broker.pump(&host, &CancellationToken::new()).await?;
                Ok::<_, crate::persistence::dto::EngineFailure>(())
            }
            .await;
            if let Err(error) = admission {
                // Stop may commit after the native task is registered but before
                // the clock publishes the approval. Leave the task registered so
                // StopWork can join it and persist the physical receipt.
                if error.code != "CORRELATION"
                    || error.pointer != "humanClock"
                    || !flow::inspect_run(host.flow030(), &host, &pool, &inspection.owner)
                        .await?
                        .flow
                        .state
                        .stopping
                {
                    return Err(error);
                }
            }
            Ok(None)
        })
    }
}

pub(crate) struct HumanClockRuntime030 {
    pub app: HostState,
    pub pool: SqlitePool,
    pub tasks: StreamTasks,
    pub identity: FlowIdentity030,
}
impl RuntimeClock030 for HumanClockRuntime030 {
    fn tracks_humans(&self) -> bool {
        true
    }
    fn tick(&self) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            let result = async {
                let host = self.app.clone();
                let broker = host
                    .flow030()
                    .humans
                    .broker(&host, &self.pool, &self.identity)
                    .await?;
                let _operation = broker.operation.lock().await;
                // Timers are wake hints. Journal only actual deadlines.
                broker.observe_deadlines(&host).await?;
                for request in broker.pending_resolutions().await? {
                    // Protocol questions are consumed by their native transport.
                    if broker.request_kind(&request).await? != "approval" {
                        continue;
                    }
                    let decision = broker.take_resolution(&host, &request).await?;
                    broker
                        .finish_approval(&host, &self.tasks, &decision)
                        .await?;
                }
                Ok(())
            }
            .await;
            if result.is_err() {
                let state: String = sqlx::query_scalar("SELECT state_json FROM engine_flow030_runs WHERE system_id=? AND invocation_id=?")
                    .bind(&self.identity.system_start_id).bind(&self.identity.flow_invocation_id).fetch_one(&self.pool).await?;
                let state: crate::persistence::flow030::FlowState030 =
                    serde_json::from_str(&state)?;
                if state.stopping {
                    return Ok(());
                }
            }
            result
        })
    }
    fn next_wake(&self) -> BoxFuture<'_, Result<Option<Duration>>> {
        Box::pin(async move {
            Ok(self
                .app
                .flow030()
                .humans
                .next_wake_ms(&self.identity)
                .await?
                .map(Duration::from_millis))
        })
    }
}

pub(crate) struct CallHumans030 {
    pub app: HostState,
    pub pool: SqlitePool,
    pub identity: FlowIdentity030,
}
struct McpAnswer030(jsonschema::Validator);

fn strings(value: &Value) -> bool {
    value
        .as_array()
        .is_some_and(|items| items.iter().all(Value::is_string))
}

fn titled_choices(value: &Value) -> bool {
    value.as_array().is_some_and(|items| {
        items.iter().all(|item| {
            item.as_object().is_some_and(|fields| fields.len() == 2)
                && item["const"].is_string()
                && item["title"].is_string()
        })
    })
}

fn enum_items(value: &Value) -> bool {
    value.as_object().is_some_and(|fields| {
        (fields.len() == 2 && value["type"] == "string" && strings(&value["enum"]))
            || (fields.len() == 1 && titled_choices(&value["anyOf"]))
    })
}

fn primitive_field(field: &Value) -> bool {
    let Some(kind @ ("string" | "number" | "integer" | "boolean" | "array")) =
        field["type"].as_str()
    else {
        return false;
    };
    // Arrays are only allowed for multi-select enums. Combinators can only
    // contain constant options with display titles.
    (kind != "array" || enum_items(&field["items"]))
        && !(field.get("enum").is_some() && field.get("oneOf").is_some())
        && field.as_object().is_some_and(|fields| {
            fields
                .iter()
                .all(|(key, value)| match (kind, key.as_str()) {
                    (_, "type") => true,
                    (_, "title" | "description") | ("string", "default" | "pattern") => {
                        value.is_string()
                    }
                    ("string", "minLength" | "maxLength") | ("array", "minItems" | "maxItems") => {
                        value.is_number()
                    }
                    ("string", "format") => {
                        matches!(value.as_str(), Some("email" | "uri" | "date" | "date-time"))
                    }
                    ("string", "enum") => strings(value),
                    ("string", "enumNames") => field.get("enum").is_some() && strings(value),
                    ("string", "oneOf") => titled_choices(value),
                    ("number" | "integer", "minimum" | "maximum" | "default") => value.is_number(),
                    ("boolean", "default") => value.is_boolean(),
                    ("array", "items") => true,
                    ("array", "default") => strings(value),
                    _ => false,
                })
        })
}

#[path = "agsdl_host_human_pattern030.rs"]
mod pattern;

fn integer_bound(value: &Value) -> Option<u64> {
    let number = value.as_number()?;
    if let Some(bound) = number.as_u64() {
        return Some(bound);
    }
    // arbitrary_precision retains the source digits. Work in decimal so an
    // out-of-range or fractional value cannot round to a valid bound.
    let (mantissa, exponent) = number
        .as_str()
        .split_once(['e', 'E'])
        .unwrap_or((number.as_str(), "0"));
    let unsigned = mantissa.strip_prefix('-').unwrap_or(mantissa);
    let (whole, fractional) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    let digits = format!("{whole}{fractional}");
    let digits = digits.trim_start_matches('0');
    if digits.is_empty() {
        return Some(0);
    }
    if mantissa.starts_with('-') {
        return None;
    }
    let scale = exponent
        .parse::<i64>()
        .ok()?
        .checked_sub(fractional.len().try_into().ok()?)?;
    let trimmed = digits.trim_end_matches('0');
    let zeros = scale.checked_add((digits.len() - trimmed.len()).try_into().ok()?)?;
    trimmed
        .parse::<u64>()
        .ok()?
        .checked_mul(10_u64.checked_pow(zeros.try_into().ok()?)?)
}

impl McpAnswer030 {
    fn compile(schema: &Value) -> Result<Self> {
        let invalid_schema = || {
            failure(
                "INPUT_CONSTRAINT",
                "requestedSchema",
                "Expected an MCP elicitation schema with a flat object of supported fields",
            )
        };
        let mut fields = schema.as_object().ok_or_else(invalid_schema)?.clone();
        if let Some(dialect) = fields.get("$schema") {
            if !matches!(
                dialect.as_str(),
                Some(
                    "https://json-schema.org/draft/2020-12/schema"
                        | "https://json-schema.org/draft/2020-12/schema#"
                )
            ) {
                return Err(failure(
                    "INPUT_CONSTRAINT",
                    "requestedSchema/$schema",
                    &format!("Unsupported MCP form schema dialect: {dialect}"),
                ));
            }
        }
        if schema["type"] != "object"
            || !fields.iter().all(|(key, value)| match key.as_str() {
                "type" => true,
                "$schema" => true,
                "additionalProperties" => value.is_boolean(),
                "properties" => value
                    .as_object()
                    .is_some_and(|properties| properties.values().all(primitive_field)),
                "required" => strings(value),
                _ => false,
            })
        {
            return Err(invalid_schema());
        }
        if let Some(properties) = fields.get_mut("properties").and_then(Value::as_object_mut) {
            for (name, field) in properties {
                for keyword in ["minLength", "maxLength", "minItems", "maxItems"] {
                    if let Some(value) = field.get_mut(keyword) {
                        *value = integer_bound(value)
                            .ok_or_else(|| {
                                let path = jsonschema::paths::Location::new()
                                    .join("properties")
                                    .join(name.as_str())
                                    .join(keyword);
                                failure(
                                    "INPUT_CONSTRAINT",
                                    &format!("requestedSchema{path}"),
                                    "Expected an exact non-negative integer bound in the u64 range",
                                )
                            })?
                            .into();
                    }
                }
            }
        }
        jsonschema::options()
            .with_draft(jsonschema::Draft::Draft202012)
            .with_keyword("pattern", pattern::compile)
            .should_validate_formats(true)
            .build(&Value::Object(fields))
            .map(Self)
            .map_err(|error| {
                let path = error.schema_path();
                let message = if error.kind().keyword() == "pattern" {
                    error.to_string()
                } else {
                    "Invalid MCP form schema".into()
                };
                failure(
                    "INPUT_CONSTRAINT",
                    &format!("requestedSchema{path}"),
                    &message,
                )
            })
    }
}

impl crate::agsdl_host_human030::QuestionValidator030 for McpAnswer030 {
    fn validate(&self, _: &Value, answer: &str) -> Result<()> {
        let value: serde_json::Value = serde_json::from_str(answer)?;
        if !matches!(
            value["action"].as_str(),
            Some("accept" | "decline" | "cancel")
        ) {
            return Err(failure(
                "INPUT_CONSTRAINT",
                "answer/action",
                "MCP form field /action must be accept, decline, or cancel",
            ));
        }
        if value["action"] == "accept" {
            self.0.validate(&value["content"]).map_err(|error| {
                let path = error.instance_path();
                let path = match error.kind() {
                    jsonschema::error::ValidationErrorKind::Required { property } => property
                        .as_str()
                        .map_or_else(|| path.clone(), |name| path.join(name)),
                    _ => path.clone(),
                };
                let field = format!("/content{path}");
                let constraint = match error.kind().keyword() {
                    "oneOf" | "anyOf" => "enum",
                    keyword => keyword,
                };
                failure(
                    "INPUT_CONSTRAINT",
                    &format!("answer{field}"),
                    &format!("MCP form field {field} violates {constraint}"),
                )
            })?;
        }
        Ok(())
    }
}
fn transport_error(
    error: crate::persistence::dto::EngineFailure,
) -> crate::agsdl_host_calls::CallError {
    crate::agsdl_host_calls::CallError::new(&error.code, error.message)
}
#[async_trait::async_trait]
impl crate::agsdl_host_calls::HostPort for CallHumans030 {
    fn supports_human_requests(&self) -> bool {
        true
    }
    #[cfg(test)]
    async fn claim(
        &self,
        _: &crate::agsdl_host_calls::CallRequest,
    ) -> crate::agsdl_host_calls::Result<crate::agsdl_host_calls::ClaimedCall> {
        Err(crate::agsdl_host_calls::CallError::new(
            "AUTHORITY",
            "Use the flow call admission",
        ))
    }
    #[cfg(test)]
    fn launch_gate(&self) -> Arc<tokio::sync::Mutex<()>> {
        self.app.clone().admission_lock()
    }
    #[cfg(test)]
    async fn authorize_launch(
        &self,
        _: &crate::agsdl_host_calls::CallRequest,
        _: &str,
    ) -> crate::agsdl_host_calls::Result<crate::agsdl_host_calls::LaunchGrant> {
        Err(crate::agsdl_host_calls::CallError::new(
            "AUTHORITY",
            "Use the flow call admission",
        ))
    }
    #[cfg(test)]
    async fn finish(
        &self,
        _: &str,
        _: &crate::agsdl_host_calls::Completion,
    ) -> crate::agsdl_host_calls::Result<()> {
        Err(crate::agsdl_host_calls::CallError::new(
            "AUTHORITY",
            "Use the flow call join",
        ))
    }
    async fn human_open(
        &self,
        call: &crate::agsdl_host_calls::CallIdentity,
        server: &str,
        raw: &str,
    ) -> crate::agsdl_host_calls::Result<crate::agsdl_host_calls::HumanTicket> {
        if raw.len() > 65_536 {
            return Err(crate::agsdl_host_calls::CallError::new(
                "INPUT_LIMIT",
                "MCP question exceeds 65536 bytes",
            ));
        }
        let request: Value = serde_json::from_str(raw)
            .map_err(|e| crate::agsdl_host_calls::CallError::transport(e.to_string()))?;
        if request["method"] != "elicitation/create"
            || request["params"]["mode"]
                .as_str()
                .is_some_and(|m| m != "form")
        {
            return Err(crate::agsdl_host_calls::CallError::new(
                "HOST_CAPABILITY",
                "Only MCP form questions are supported",
            ));
        }
        let validator = McpAnswer030::compile(&request["params"]["requestedSchema"])
            .map_err(transport_error)?;
        let host = self.app.clone();
        let broker = host
            .flow030()
            .humans
            .broker(&host, &self.pool, &self.identity)
            .await
            .map_err(transport_error)?;
        let _operation = broker.operation.lock().await;
        let work = broker
            .owned_work(&call.work_id)
            .await
            .map_err(transport_error)?;
        let payload = serde_json::json!({"nativeCallId":call.native_call_id,"protocolReceiptId":uuid::Uuid::new_v4().to_string(),"server":server,"question":request["params"]["message"],"expectedResponse":"","schema":{"type":"object","required":["action"],"properties":{"action":{"enum":["accept","decline","cancel"]},"content":request["params"]["requestedSchema"]}},"protocolRequest":request});
        let ticket = broker
            .open_question(&host, &work, payload, Arc::new(validator))
            .await
            .map_err(transport_error)?;
        broker
            .pump(&host, &CancellationToken::new())
            .await
            .map_err(transport_error)?;
        host.flow030()
            .supervisors
            .wake(&self.identity.system_start_id);
        Ok(ticket)
    }
    async fn human_answer(
        &self,
        ticket: &str,
    ) -> crate::agsdl_host_calls::Result<crate::agsdl_host_calls::HumanReply> {
        let host = self.app.clone();
        let broker = host
            .flow030()
            .humans
            .broker(&host, &self.pool, &self.identity)
            .await
            .map_err(transport_error)?;
        loop {
            if broker
                .resolution_ready(ticket)
                .await
                .map_err(transport_error)?
            {
                let _operation = broker.operation.lock().await;
                let decision = broker
                    .take_resolution(&host, ticket)
                    .await
                    .map_err(transport_error)?;
                let answer = broker
                    .protocol_answer(&decision)
                    .await
                    .map_err(transport_error)?
                    .unwrap_or_else(|| "{\"action\":\"decline\"}".into());
                drop(_operation);
                return Ok(crate::agsdl_host_calls::HumanReply {
                    body: answer,
                    acknowledge: Box::pin(async move {
                        let _operation = broker.operation.lock().await;
                        broker
                            .acknowledge(&host, &decision)
                            .await
                            .map_err(transport_error)?;
                        broker
                            .pump(&host, &CancellationToken::new())
                            .await
                            .map_err(transport_error)?;
                        Ok(())
                    }),
                });
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
    async fn human_close(
        &self,
        _: &str,
        _: &crate::agsdl_host_calls::Result<String>,
    ) -> crate::agsdl_host_calls::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agsdl_host_human030::QuestionValidator030;
    use serde_json::json;

    fn string_form(constraints: Value) -> Value {
        let mut field = json!({"type":"string"});
        field
            .as_object_mut()
            .unwrap()
            .extend(constraints.as_object().unwrap().clone());
        json!({"type":"object","properties":{"value":field}})
    }

    #[test]
    fn mcp_form_rejects_unsupported_patterns_with_diagnostic() {
        for pattern in [r"^(?=.{3}$)[a-z]+$", r"^(a)\1$"] {
            let Err(error) = McpAnswer030::compile(&string_form(json!({"pattern":pattern}))) else {
                panic!("Accepted unsupported pattern {pattern}");
            };
            assert!(error.message.contains(pattern), "{}", error.message);
        }
    }

    #[test]
    fn mcp_form_word_boundary_is_ascii() {
        let validator =
            McpAnswer030::compile(&string_form(json!({"pattern":r"^\Bélan$"}))).unwrap();
        validator
            .validate(
                &Value::Null,
                r#"{"action":"accept","content":{"value":"élan"}}"#,
            )
            .unwrap();
    }

    #[test]
    fn mcp_form_dot_excludes_ecmascript_line_terminators() {
        let validator = McpAnswer030::compile(&string_form(json!({"pattern":"^A.B$"}))).unwrap();
        for value in ["A\rB", "A\nB", "A\u{2028}B", "A\u{2029}B", "A😀B"] {
            let answer = json!({"action":"accept","content":{"value":value}}).to_string();
            assert!(
                validator.validate(&Value::Null, &answer).is_err(),
                "{value:?}"
            );
        }
        let answer = r#"{"action":"accept","content":{"value":"AéB"}}"#;
        validator.validate(&Value::Null, answer).unwrap();
    }

    #[test]
    fn mcp_form_patterns_preserve_ecmascript_character_classes() {
        for (pattern, matched, rejected) in [
            (r"^\w\d\s$", "_2\u{feff}", "_2\u{85}"),
            (r"^[\W\D]\S$", "éx", "é\u{feff}"),
            (r"^[a&&b]$", "&", "x"),
            (r"^(?:\uD83D\uDE00|x){1,2}?$", "😀x", "xxx"),
            (r"^[\uD7FF-\uE000]{2}$", "😀", "xx"),
            (r"^\bA$", "A", "A\n"),
        ] {
            let validator =
                McpAnswer030::compile(&string_form(json!({"pattern":pattern}))).unwrap();
            for (value, valid) in [(matched, true), (rejected, false)] {
                let answer = json!({"action":"accept","content":{"value":value}}).to_string();
                assert_eq!(
                    validator.validate(&Value::Null, &answer).is_ok(),
                    valid,
                    "{pattern} on {value:?}"
                );
            }
        }
    }

    #[test]
    fn mcp_form_normalizes_exact_integer_bounds() {
        for bound in ["2.0", "2e0", "20e-1"] {
            let bound: Value = serde_json::from_str(bound).unwrap();
            let mut schema = string_form(json!({"minLength":bound,"maxLength":bound}));
            schema["properties"]["tags"] = json!({
                "type":"array","minItems":bound,"maxItems":bound,
                "items":{"type":"string","enum":["red","blue","green"]}
            });
            let validator = McpAnswer030::compile(&schema).unwrap();
            validator
                .validate(
                    &Value::Null,
                    r#"{"action":"accept","content":{"value":"ok","tags":["red","blue"]}}"#,
                )
                .unwrap();
            for answer in [
                r#"{"action":"accept","content":{"value":"x"}}"#,
                r#"{"action":"accept","content":{"value":"xyz"}}"#,
                r#"{"action":"accept","content":{"tags":["red"]}}"#,
                r#"{"action":"accept","content":{"tags":["red","blue","green"]}}"#,
            ] {
                assert!(validator.validate(&Value::Null, answer).is_err());
            }
        }
        assert!(McpAnswer030::compile(&string_form(json!({
            "minLength": serde_json::from_str::<Value>("18446744073709551615.0").unwrap()
        })))
        .is_ok());
        for bound in [
            "2.00000000000000000001",
            "-2.0",
            "18446744073709551616.0",
            "1e9999999999",
        ] {
            let bound = serde_json::from_str::<Value>(bound).unwrap();
            assert!(
                McpAnswer030::compile(&string_form(json!({"minLength":bound}))).is_err(),
                "{bound}"
            );
        }
    }

    #[test]
    fn mcp_form_refuses_unrecognized_dialects() {
        for dialect in [
            json!("http://json-schema.org/draft-07/schema#"),
            json!("https://example.invalid/schema"),
            Value::Null,
        ] {
            let mut schema = string_form(json!({}));
            schema["$schema"] = dialect.clone();
            let Err(error) = McpAnswer030::compile(&schema) else {
                panic!("Accepted unsupported dialect {dialect}");
            };
            assert_eq!(error.pointer, "requestedSchema/$schema");
            assert!(
                error.message.contains(&dialect.to_string()),
                "{}",
                error.message
            );
        }
    }

    #[test]
    fn mcp_form_confirmation_without_properties_is_valid() {
        let validator = McpAnswer030::compile(&json!({"type":"object"})).unwrap();
        validator
            .validate(&Value::Null, r#"{"action":"accept","content":{}}"#)
            .unwrap();
        assert!(validator
            .validate(&Value::Null, r#"{"action":"accept","content":"yes"}"#)
            .is_err());
    }

    #[test]
    fn mcp_form_can_disallow_additional_properties() {
        let mut schema = string_form(json!({}));
        schema["additionalProperties"] = json!(false);
        let validator = McpAnswer030::compile(&schema).unwrap();
        validator
            .validate(
                &Value::Null,
                r#"{"action":"accept","content":{"value":"ok"}}"#,
            )
            .unwrap();
        let error = validator
            .validate(
                &Value::Null,
                r#"{"action":"accept","content":{"extra":"no"}}"#,
            )
            .unwrap_err();
        assert!(error.message.ends_with("additionalProperties"));
    }

    #[test]
    fn mcp_form_acceptance_checks_retained_schema() {
        let validator = McpAnswer030::compile(&json!({
            "type":"object","properties":{"a/b":{"type":"string","minLength":2}},
            "required":["a/b"]
        }))
        .unwrap();
        for (answer, keyword) in [
            (r#"{"action":"accept","content":{}}"#, "required"),
            (r#"{"action":"accept","content":{"a/b":91357}}"#, "type"),
            (r#"{"action":"accept","content":{"a/b":"x"}}"#, "minLength"),
        ] {
            let err = validator.validate(&Value::Null, answer).unwrap_err();
            assert_eq!(err.code, "INPUT_CONSTRAINT");
            assert_eq!(err.pointer, "answer/content/a~1b");
            assert_eq!(
                err.message,
                format!("MCP form field /content/a~1b violates {keyword}")
            );
        }
        for answer in [
            r#"{"action":"accept","content":{"a/b":"valid"}}"#,
            r#"{"action":"decline"}"#,
            r#"{"action":"cancel"}"#,
        ] {
            validator.validate(&Value::Null, answer).unwrap();
        }
    }

    #[test]
    fn mcp_form_compiles_patterns_formats_and_enum_options() {
        let validator = McpAnswer030::compile(&json!({
            "$schema":"https://json-schema.org/draft/2020-12/schema",
            "type":"object",
            "properties":{
                "email":{"type":"string","format":"email"},
                "tag":{"type":"string","pattern":"^[a-z]+$"},
                "color":{"type":"string","oneOf":[{"const":"red","title":"Red"}]},
                "tags":{"type":"array","minItems":1,"items":{"type":"string","enum":["one"]}},
                "sizes":{"type":"array","items":{"anyOf":[{"const":"m","title":"Medium"}]}},
                "count":{"type":"integer","minimum":1,"maximum":5},
                "check":{"type":"boolean","default":false}
            }
        }))
        .unwrap();
        validator
            .validate(
                &Value::Null,
                r#"{"action":"accept","content":{
            "email":"a@example.com","tag":"test","color":"red","tags":["one"],
            "sizes":["m"],"count":2,"check":true
        }}"#,
            )
            .unwrap();
        for (content, constraint) in [
            (json!({"email":"invalid"}), "format"),
            (json!({"tag":"123"}), "pattern"),
            (json!({"color":"invalid"}), "enum"),
            (json!({"tags":[]}), "minItems"),
            (json!({"sizes":["invalid"]}), "enum"),
            (json!({"count":6}), "maximum"),
        ] {
            let answer = json!({"action":"accept","content":content}).to_string();
            let err = validator.validate(&Value::Null, &answer).unwrap_err();
            assert!(err.message.ends_with(constraint), "{}", err.message);
        }
    }
}
