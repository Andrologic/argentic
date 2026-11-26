//! AgSDL decisions. Filesystem admission is enforced independently.

mod shell;
pub(crate) use shell::parse as parse_shell;
pub use shell::subcommands;

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
pub struct Policy {
    rules: Vec<(Action, Option<Matcher>, Effect)>,
    preset: Option<Preset>,
    outside_read: Option<bool>,
    directories: Vec<(PathBuf, Zone)>,
}

pub type ReadPolicy = Policy;

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Action {
    Read,
    Write,
    Shell,
    Network,
    Mcp,
    Delegate,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Zone {
    Workspace,
    ReadDirectory,
    WriteDirectory,
    Outside,
}

#[derive(Clone, Debug, Deserialize)]
pub struct Rule {
    pub action: Action,
    #[serde(rename = "match")]
    pub pattern: Option<String>,
    pub effect: Effect,
}

#[derive(Clone, Debug)]
enum Matcher {
    Path(PathGlob),
    Text(Regex),
}

impl Matcher {
    fn matches(&self, subject: &str) -> bool {
        match self {
            Self::Path(glob) => glob.matches(subject),
            Self::Text(glob) => glob.is_match(subject),
        }
    }
}

impl Policy {
    /// Compatibility constructor for standalone read-only registries.
    pub fn compile(
        workspace: &Path,
        home: Option<&Path>,
        preset: Option<Preset>,
        outside_read: Option<bool>,
        rules: &[ReadRule],
    ) -> Result<Self, String> {
        let rules = rules
            .iter()
            .map(|rule| Rule {
                action: Action::Read,
                pattern: rule.pattern.clone(),
                effect: rule.effect,
            })
            .collect::<Vec<_>>();
        Self::compile_rules(workspace, home, preset, outside_read, &rules)
    }

    pub fn compile_rules(
        workspace: &Path,
        home: Option<&Path>,
        preset: Option<Preset>,
        outside_read: Option<bool>,
        rules: &[Rule],
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
                    .filter(|pattern| {
                        matches!(rule.action, Action::Read | Action::Write)
                            || pattern.is_empty()
                            || !pattern.bytes().all(|byte| byte == b'*')
                    })
                    .map(|pattern| match rule.action {
                        Action::Read | Action::Write => {
                            compile_rule(workspace, home, pattern, rule.effect).map(Matcher::Path)
                        }
                        action => text_glob(action, pattern)
                            .map(Matcher::Text)
                            .map_err(|error| error.to_string()),
                    })
                    .transpose()?;
                Ok::<_, String>((rule.action, pattern, rule.effect))
            })
            .collect::<Result<_, _>>()?;
        Ok(Self {
            rules,
            preset,
            outside_read,
            directories: Vec::new(),
        })
    }

    pub fn with_directories(mut self, directories: Vec<(PathBuf, Zone)>) -> Self {
        self.directories = directories;
        self
    }

    pub fn decide(&self, action: Action, subject: &str, zone: Zone) -> Effect {
        let subject = if action == Action::Network {
            subject.trim_end_matches('.')
        } else {
            subject
        };
        self.rules
            .iter()
            .filter(|(kind, pattern, _)| {
                *kind == action
                    && pattern
                        .as_ref()
                        .is_none_or(|pattern| pattern.matches(subject))
            })
            .map(|(_, _, effect)| *effect)
            .max()
            .unwrap_or_else(|| self.default_effect(action, zone))
    }

    pub fn path_effect(&self, action: Action, path: &Path, zone: Zone) -> Effect {
        let zone =
            if self.directories.iter().any(|(directory, kind)| {
                *kind == Zone::ReadDirectory && path.starts_with(directory)
            }) {
                Zone::ReadDirectory
            } else if zone == Zone::Outside
                && self
                    .directories
                    .iter()
                    .any(|(directory, _)| path.starts_with(directory))
            {
                Zone::WriteDirectory
            } else {
                zone
            };
        slash_path(path).map_or(Effect::Deny, |path| self.decide(action, &path, zone))
    }

    pub fn shell_effect(&self, source: &str) -> Effect {
        parse_shell(source).map_or(Effect::Deny, |parsed| self.parsed_shell_effect(&parsed))
    }

    pub(crate) fn parsed_shell_effect(&self, parsed: &shell::ParsedShell<'_>) -> Effect {
        let commands = parsed
            .commands
            .iter()
            .map(|command| self.decide(Action::Shell, command, Zone::Workspace));
        let opaque = self.rules.iter().filter_map(|(action, _, effect)| {
            (parsed.opaque && *action == Action::Shell && *effect != Effect::Allow)
                .then_some(*effect)
        });
        commands.chain(opaque).max().unwrap_or(Effect::Deny)
    }

    /// A whole mount or network namespace needs a grant that covers every
    /// subject. A narrow allow can never grant this broader authority.
    pub(crate) fn blanket_allows(&self, action: Action, zone: Zone) -> bool {
        if action == Action::Write
            && self
                .directories
                .iter()
                .any(|(_, kind)| *kind == Zone::ReadDirectory)
        {
            return false;
        }
        let mut allowed = self.default_effect(action, zone) == Effect::Allow;
        for (kind, matcher, effect) in &self.rules {
            if *kind == action {
                if *effect != Effect::Allow {
                    return false;
                }
                allowed |= matcher.is_none();
            }
        }
        allowed
    }

    fn default_effect(&self, action: Action, zone: Zone) -> Effect {
        let allowed = match (action, self.preset) {
            (Action::Read, Some(preset)) => {
                zone != Zone::Outside || self.outside_read.unwrap_or(matches!(preset, Preset::Full))
            }
            (Action::Write, Some(Preset::Workspace)) => {
                matches!(zone, Zone::Workspace | Zone::WriteDirectory)
            }
            (Action::Write, Some(Preset::Full)) => zone != Zone::ReadDirectory,
            (Action::Shell, Some(Preset::Workspace | Preset::Full))
            | (Action::Network, Some(Preset::Full)) => true,
            _ => false,
        };
        if allowed {
            Effect::Allow
        } else {
            Effect::Deny
        }
    }

    pub fn evaluate(&self, absolute_path: &Path, in_zone: bool) -> Effect {
        let zone = if in_zone {
            Zone::Workspace
        } else {
            Zone::Outside
        };
        self.path_effect(Action::Read, absolute_path, zone)
    }

    pub fn allows(&self, absolute_path: &Path, in_zone: bool) -> bool {
        self.evaluate(absolute_path, in_zone) == Effect::Allow
    }
}

fn text_glob(action: Action, pattern: &str) -> Result<Regex, regex::Error> {
    let mut expression = String::from(if action == Action::Network {
        "(?si)\\A"
    } else {
        "(?s)\\A"
    });
    let arguments = (action == Action::Shell)
        .then(|| pattern.strip_suffix(" *"))
        .flatten();
    for piece in arguments.unwrap_or(pattern).split_inclusive('*') {
        if let Some(literal) = piece.strip_suffix('*') {
            expression.push_str(&regex::escape(literal));
            expression.push_str(".*");
        } else {
            expression.push_str(&regex::escape(piece));
        }
    }
    if arguments.is_some() {
        expression.push_str("(?: .*)?");
    }
    expression.push_str("\\z");
    Regex::new(&expression)
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
