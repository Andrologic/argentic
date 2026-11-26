use crate::persistence::{failure, Result};
use argentic_harness::{
    config::Settings,
    providers::{reasoning_capability, ChatCompletions, ModelName},
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use url::Url;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ArgenticConfig030 {
    providers: BTreeMap<String, ProviderConfig>,
    pub(super) default_model: Option<String>,
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
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
enum Protocol {
    ChatCompletions,
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
        endpoint
            .path_segments_mut()
            .map_err(|_| config_error("Invalid provider base URL"))?
            .pop_if_empty()
            .push("chat")
            .push("completions");
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
        self.settings(&empty_settings(), "settings")?;
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
                let capability = reasoning_capability(
                    provider.reasoning_catalog.as_deref().unwrap_or(id),
                    model.model(),
                )
                .map_err(|_| config_error("Argentic reasoning catalogue is unavailable"))?;
                models.push(json!({"name":full,"efforts":capability.map(|c| c.efforts.as_slice()).unwrap_or_default(),
                    "defaultEffort":capability.map(|c| &c.default_effort)}));
            }
        }
        Ok(models)
    }

    pub(super) fn fingerprint(&self) -> Result<String> {
        let bytes = serde_json::to_vec(&(&self.providers, &self.settings, &self.default_model))?;
        Ok(format!("sha256:{:x}", Sha256::digest(bytes)))
    }

    pub(super) fn settings(&self, settings: &Value, pointer: &str) -> Result<Settings> {
        Settings::resolve(&self.settings, &empty_settings(), settings).map_err(|_| {
            failure(
                "HARNESS_SETTINGS",
                pointer,
                "Invalid Argentic settings or a limit exceeds the host ceiling",
            )
        })
    }

    pub(super) fn provider(&self, model: &ModelName) -> Result<(ChatCompletions, Option<String>)> {
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
        let client = ChatCompletions::new(model.provider(), provider.endpoint()?, key.as_deref())
            .map_err(|_| config_error("Cannot initialize the configured provider or credential"))?
            .with_attempt_effort_fallback();
        Ok((client, key))
    }
}
