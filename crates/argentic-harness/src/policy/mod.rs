//! AgSDL read decisions. Filesystem admission is enforced independently.

use regex::Regex;
use serde::Deserialize;
use std::path::{Component, Path, PathBuf};

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "lowercase")]
pub enum Effect {
    Allow,
    Ask,
    Deny,
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Preset {
    ReadOnly,
    Workspace,
    Full,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ReadRule {
    #[serde(rename = "match")]
    pub pattern: Option<String>,
    pub effect: Effect,
}

/// Only '*' and '**' are special. All other characters are literal.
#[derive(Clone, Debug)]
pub struct PathGlob(Regex);

impl PathGlob {
    pub fn compile(pattern: &str) -> Result<Self, regex::Error> {
        Self::prefixed("", pattern, false)
    }

    fn prefixed(prefix: &str, pattern: &str, fold_case: bool) -> Result<Self, regex::Error> {
        let mut expression = String::from(if fold_case { "(?si)\\A" } else { "(?s)\\A" });
        expression.push_str(&regex::escape(prefix));
        let mut chars = pattern.chars().peekable();
        while let Some(character) = chars.next() {
            if character == '*' {
                expression.push_str(if chars.next_if_eq(&'*').is_some() {
                    ".*"
                } else {
                    "[^/]*"
                });
            } else {
                expression.push_str(&regex::escape(&character.to_string()));
            }
        }
        expression.push_str("\\z");
        Regex::new(&expression).map(Self)
    }

    pub fn matches(&self, path: &str) -> bool {
        self.0.is_match(path)
    }
}

#[derive(Clone, Debug)]
pub struct ReadPolicy {
    rules: Vec<(Option<PathGlob>, Effect)>,
    preset: Option<Preset>,
    outside_read: Option<bool>,
}

impl ReadPolicy {
    /// Compile authored paths against the host-admitted workspace and home.
    /// Both paths use the host's canonical spelling.
    pub fn compile(
        workspace: &Path,
        home: Option<&Path>,
        preset: Option<Preset>,
        outside_read: Option<bool>,
        rules: &[ReadRule],
    ) -> Result<Self, String> {
        if !workspace.is_absolute() || home.is_some_and(|path| !path.is_absolute()) {
            return Err("Permission bases must be absolute".into());
        }
        let rules = rules
            .iter()
            .map(|rule| {
                let pattern = rule
                    .pattern
                    .as_deref()
                    .map(|pattern| compile_rule(workspace, home, pattern, rule.effect))
                    .transpose()?;
                Ok::<_, String>((pattern, rule.effect))
            })
            .collect::<Result<_, _>>()?;
        Ok(Self {
            rules,
            preset,
            outside_read,
        })
    }

    pub fn evaluate(&self, absolute_path: &Path, in_zone: bool) -> Effect {
        let Some(path) = slash_path(absolute_path) else {
            return Effect::Deny;
        };
        self.rules
            .iter()
            .filter(|(pattern, _)| pattern.as_ref().is_none_or(|glob| glob.matches(&path)))
            .map(|(_, effect)| *effect)
            .max()
            .unwrap_or_else(|| match self.preset {
                Some(preset)
                    if in_zone || self.outside_read.unwrap_or(matches!(preset, Preset::Full)) =>
                {
                    Effect::Allow
                }
                _ => Effect::Deny,
            })
    }

    /// Slice 1 has no reviewer. Uncovered asks are denied.
    pub fn allows(&self, absolute_path: &Path, in_zone: bool) -> bool {
        self.evaluate(absolute_path, in_zone) == Effect::Allow
    }
}

fn compile_rule(
    workspace: &Path,
    home: Option<&Path>,
    pattern: &str,
    effect: Effect,
) -> Result<PathGlob, String> {
    let (base, pattern) = if let Some(relative) = pattern.strip_prefix("~/") {
        (
            home.ok_or("A home-relative rule requires a home directory")?,
            Path::new(relative),
        )
    } else {
        (workspace, Path::new(pattern))
    };
    let mut literal = base.to_path_buf();
    let mut wildcard = PathBuf::new();
    let resolved;
    let pattern = if pattern.has_root() {
        resolved = base.join(pattern);
        literal.clear();
        &resolved
    } else {
        pattern
    };
    for part in pattern.components() {
        match part {
            Component::CurDir => {}
            Component::ParentDir => {
                if wildcard.to_string_lossy().contains('*') || (!wildcard.pop() && !literal.pop()) {
                    return Err("Ambiguous parent component in permission rule".into());
                }
            }
            part => wildcard.push(part),
        }
    }
    let mut literal = slash_path(&literal).ok_or("Permission path must be UTF-8")?;
    let wildcard = slash_path(&wildcard).ok_or("Permission path must be UTF-8")?;
    if !literal.is_empty() && !wildcard.is_empty() && !literal.ends_with('/') {
        literal.push('/');
    }
    // On Windows, match restrictive rules across case aliases. Allow rules
    // still require the exact name in case-sensitive directories.
    PathGlob::prefixed(
        &literal,
        &wildcard,
        cfg!(windows) && effect != Effect::Allow,
    )
    .map_err(|e| e.to_string())
}

pub(crate) fn slash_path(path: &Path) -> Option<String> {
    path.to_str().map(|path| {
        if cfg!(windows) {
            let path = path.replace('\\', "/");
            if let Some(unc) = path.strip_prefix("//?/UNC/") {
                format!("//{unc}")
            } else {
                path.strip_prefix("//?/")
                    .map_or_else(|| path.clone(), str::to_owned)
            }
        } else {
            path.to_owned()
        }
    })
}
