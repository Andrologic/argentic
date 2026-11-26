use crate::persistence::{failure, Result};
use argentic_harness::{
    config::Settings,
    providers::{
        reasoning_capability, AnthropicMessages, ChatCompletions, ModelName, OpenAiResponses,
        Provider,
    },
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, path::PathBuf, sync::Arc};
use url::Url;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ArgenticConfig030 {
    providers: BTreeMap<String, ProviderConfig>,
    pub(super) default_model: Option<String>,
    pub(super) shell_staging_directory: Option<PathBuf>,
    #[serde(default = "empty_settings")]
    settings: Value,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ProviderConfig {
    protocol: Protocol,
    base_url: String,
    api_key_env: Option<String>,
    reasoning_catalog: Option<String>,
    models: Vec<String>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    prompt_caching: bool,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
enum Protocol {
    ChatCompletions,
    AnthropicMessages,
    #[serde(rename = "openai-responses")]
    OpenAiResponses,
}

fn empty_settings() -> Value {
    json!({})
}

fn config_error(message: &str) -> crate::persistence::dto::EngineFailure {
    failure("ARGENTIC_PROVIDER_CONFIG", "providers", message)
}

pub(super) fn invalid_model(pointer: &str) -> crate::persistence::dto::EngineFailure {
    failure(
        "MODEL_UNAVAILABLE",
        pointer,
        "Select a configured Argentic provider/model or provider/model#variant",
    )
}

impl ProviderConfig {
    fn catalog<'a>(&'a self, provider_id: &'a str) -> &'a str {
        self.reasoning_catalog
            .as_deref()
            .unwrap_or(match self.protocol {
                Protocol::ChatCompletions => provider_id,
                Protocol::AnthropicMessages => "anthropic",
                Protocol::OpenAiResponses => "openai",
            })
    }

    fn endpoint(&self) -> Result<Url> {
        let mut endpoint =
            Url::parse(&self.base_url).map_err(|_| config_error("Invalid provider base URL"))?;
        if !matches!(endpoint.scheme(), "http" | "https")
            || endpoint.host().is_none()
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
        {
            return Err(config_error(
                "Expected an HTTP(S) base URL without credentials, query or fragment",
            ));
        }
        let mut path = endpoint
            .path_segments_mut()
            .map_err(|_| config_error("Invalid provider base URL"))?;
        path.pop_if_empty();
        match self.protocol {
            Protocol::ChatCompletions => path.extend(["chat", "completions"]),
            Protocol::AnthropicMessages => path.extend(["v1", "messages"]),
            Protocol::OpenAiResponses => path.push("responses"),
        };
        drop(path);
        Ok(endpoint)
    }
}

impl ArgenticConfig030 {
    pub(super) fn validate(&self) -> Result<()> {
        if self.providers.is_empty() {
            return Err(config_error("Configure at least one Argentic provider"));
        }
        for (id, provider) in &self.providers {
            provider.endpoint()?;
            if provider.prompt_caching && !matches!(provider.protocol, Protocol::AnthropicMessages)
            {
                return Err(config_error("promptCaching requires anthropic-messages"));
            }
            if provider.models.is_empty()
                || provider
                    .api_key_env
                    .as_ref()
                    .is_some_and(|name| name.is_empty() || name.contains(['=', '\0']))
            {
                return Err(config_error(
                    "Provider models and credential environment variable names must be nonempty",
                ));
            }
            let mut seen = std::collections::HashSet::new();
            for model in &provider.models {
                let full = format!("{id}/{model}");
                let parsed: ModelName = full
                    .parse()
                    .map_err(|_| config_error("Invalid configured model name"))?;
                if parsed.provider() != id || !seen.insert(model) {
                    return Err(config_error("Invalid provider id or duplicate model name"));
                }
            }
        }
        if let Some(name) = &self.default_model {
            if !self.catalogue()?.iter().any(|model| model["name"] == *name) {
                return Err(config_error(
                    "The default model must be a configured provider/model",
                ));
            }
        }
        if self
            .shell_staging_directory
            .as_ref()
            .is_some_and(|path| !path.is_absolute() || !path.is_dir())
        {
            return Err(failure(
                "HARNESS_SETTINGS",
                "/shellStagingDirectory",
                "Expected an existing absolute shell staging directory",
            ));
        }
        let settings = self.settings(&empty_settings(), "/settings")?;
        if self
            .providers
            .values()
            .any(|p| matches!(p.protocol, Protocol::OpenAiResponses))
        {
            validate_responses_limits(&settings, "/settings")?;
        }
        Ok(())
    }

    pub(super) fn catalogue(&self) -> Result<Vec<Value>> {
        let mut models = Vec::new();
        for (id, provider) in &self.providers {
            for name in &provider.models {
                let full = format!("{id}/{name}");
                let model: ModelName = full
                    .parse()
                    .map_err(|_| config_error("Invalid configured model name"))?;
                let capability = reasoning_capability(provider.catalog(id), model.model())
                    .map_err(|_| config_error("Argentic reasoning catalogue is unavailable"))?;
                models.push(json!({"name":full,"efforts":capability.map(|c| c.efforts.as_slice()).unwrap_or_default(),
                    "defaultEffort":capability.map(|c| &c.default_effort)}));
            }
        }
        Ok(models)
    }

    pub(super) fn fingerprint(&self) -> Result<String> {
        let legacy = (&self.providers, &self.settings);
        let bytes = match (&self.default_model, &self.shell_staging_directory) {
            (name, Some(path)) => serde_json::to_vec(&(legacy.0, legacy.1, name, path))?,
            (Some(name), None) => serde_json::to_vec(&(legacy.0, legacy.1, name))?,
            (None, None) => serde_json::to_vec(&legacy)?,
        };
        Ok(format!("sha256:{:x}", Sha256::digest(bytes)))
    }

    fn settings(&self, settings: &Value, pointer: &str) -> Result<Settings> {
        Settings::resolve(&self.settings, &empty_settings(), settings).map_err(|_| {
            failure(
                "HARNESS_SETTINGS",
                pointer,
                "Invalid Argentic settings or a limit exceeds the host ceiling",
            )
        })
    }

    pub(super) fn settings_for_model(
        &self,
        model: &ModelName,
        settings: &Value,
        pointer: &str,
    ) -> Result<Settings> {
        let settings = self.settings(settings, pointer)?;
        if self
            .providers
            .get(model.provider())
            .is_some_and(|provider| matches!(provider.protocol, Protocol::OpenAiResponses))
        {
            validate_responses_limits(&settings, pointer)?;
        }
        Ok(settings)
    }

    pub(super) fn provider(
        &self,
        model: &ModelName,
    ) -> Result<(Arc<dyn Provider>, Option<String>)> {
        let provider = self
            .providers
            .get(model.provider())
            .ok_or_else(|| invalid_model("model"))?;
        let key = provider
            .api_key_env
            .as_ref()
            .map(|name| {
                std::env::var(name)
                    .ok()
                    .filter(|key| !key.trim().is_empty())
                    .ok_or_else(|| {
                        config_error("The configured provider credential is unavailable")
                    })
            })
            .transpose()?;
        let endpoint = provider.endpoint()?;
        let id = model.provider();
        let credential = key.as_deref();
        let map_error = |_| config_error("Cannot initialize the configured provider or credential");
        let client: Arc<dyn Provider> = match provider.protocol {
            Protocol::ChatCompletions => Arc::new(
                ChatCompletions::new(id, endpoint, credential)
                    .map_err(map_error)?
                    .with_attempt_effort_fallback(),
            ),
            Protocol::AnthropicMessages => Arc::new(
                AnthropicMessages::new(id, endpoint, credential)
                    .map_err(map_error)?
                    .with_reasoning_catalog(provider.catalog(id))
                    .with_prompt_caching(provider.prompt_caching)
                    .with_attempt_effort_fallback(),
            ),
            Protocol::OpenAiResponses => Arc::new(
                OpenAiResponses::new(id, endpoint, credential)
                    .map_err(map_error)?
                    .with_attempt_effort_fallback(),
            ),
        };
        Ok((client, key))
    }
}

fn validate_responses_limits(settings: &Settings, pointer: &str) -> Result<()> {
    for key in ["maxOutputTokens", "maxTotalTokens"] {
        if settings
            .get(key)
            .is_some_and(|(value, _)| value < OpenAiResponses::MIN_OUTPUT_TOKENS)
        {
            return Err(failure(
                "HARNESS_SETTINGS",
                &format!("{pointer}/{key}"),
                &format!(
                    "OpenAI Responses requires {key} of at least {}",
                    OpenAiResponses::MIN_OUTPUT_TOKENS
                ),
            ));
        }
    }
    Ok(())
}
