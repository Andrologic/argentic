use std::{collections::BTreeMap, sync::LazyLock};

use serde_json::Value;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Layer {
    Default,
    Host,
    User,
    Project,
}

#[derive(Clone, Debug)]
pub struct Settings {
    limits: BTreeMap<&'static str, (u32, Layer)>,
}

const DEFAULTS: [(&str, u32); 5] = [
    ("maxTurns", 32),
    ("maxOutputTokens", 4096),
    ("maxTotalTokens", u32::MAX),
    ("cancelGraceMs", 250),
    ("maxParallelReads", 4),
];

static SCHEMA: LazyLock<Result<jsonschema::Validator, String>> = LazyLock::new(|| {
    let module: Value = serde_json::from_str(crate::MODULE_JSON).map_err(|e| e.to_string())?;
    jsonschema::validator_for(&module["settings"]).map_err(|e| e.to_string())
});

impl Default for Settings {
    fn default() -> Self {
        Self {
            limits: DEFAULTS
                .into_iter()
                .map(|(key, value)| (key, (value, Layer::Default)))
                .collect(),
        }
    }
}

impl Settings {
    /// Apply host, user and project settings in order. Project values can
    /// only lower the inherited limits. Absent fields retain their source.
    pub fn resolve(host: &Value, user: &Value, project: &Value) -> Result<Self, String> {
        let mut settings = Self::default();
        for (layer, data) in [
            (Layer::Host, host),
            (Layer::User, user),
            (Layer::Project, project),
        ] {
            let schema = SCHEMA.as_ref().map_err(Clone::clone)?;
            schema
                .validate(data)
                .map_err(|e| format!("{layer:?}: {e}"))?;
            for (key, default) in DEFAULTS {
                if let Some(value) = data.get(key) {
                    // Integer-valued decimals are permitted by AgSDL.
                    let number = value
                        .as_f64()
                        .filter(|number| (0.0..=f64::from(u32::MAX)).contains(number))
                        .ok_or_else(|| {
                            format!("{layer:?}: {key} must fit in an unsigned 32-bit integer")
                        })?;
                    let value = number as u32;
                    let inherited = settings.get(key).map_or(default, |(value, _)| value);
                    if value == 0 && key != "cancelGraceMs" {
                        return Err(format!("{layer:?}: {key} must be positive"));
                    }
                    if layer == Layer::Project && value > inherited {
                        return Err(format!("Project: {key} exceeds the inherited limit"));
                    }
                    settings.limits.insert(key, (value, layer));
                }
            }
        }
        Ok(settings)
    }

    pub fn get(&self, key: &str) -> Option<(u32, Layer)> {
        self.limits.get(key).copied()
    }

    pub(crate) fn limit(&self, key: &'static str) -> u32 {
        self.limits[key].0
    }
}
