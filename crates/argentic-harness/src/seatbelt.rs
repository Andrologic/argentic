//! Seatbelt profile primitives shared by the native launcher and shell.
use std::path::{Path, PathBuf};

pub const BASE_PROFILE: &str = "(version 1)\n(deny default)\n";
pub const PROCESS_DEFAULTS: &str = "(allow signal (target self))\n(allow sysctl-read)\n";

#[derive(Debug, Clone)]
pub enum PathScope {
    Literal(PathBuf),
    Subtree(PathBuf),
}

#[derive(Debug, Clone, Default)]
pub struct FileAccess {
    pub allow: Vec<PathScope>,
    /// Includes denied and unmediated ask regions. A shell cannot answer an ask.
    pub block: Vec<PathScope>,
}

fn quoted(path: &Path) -> Result<String, String> {
    if !path.is_absolute() {
        return Err("Sandbox paths must be absolute".into());
    }
    let text = path.to_str().ok_or("Sandbox path is not UTF-8")?;
    if text.chars().any(char::is_control) {
        return Err("Sandbox path contains a control character".into());
    }
    // SBPL strings use the same escapes needed here for quotes and backslashes.
    Ok(format!(
        "\"{}\"",
        text.replace('\\', "\\\\").replace('"', "\\\"")
    ))
}

pub fn scope(scope: &PathScope) -> Result<String, String> {
    let (kind, path) = match scope {
        PathScope::Literal(p) => ("literal", p),
        PathScope::Subtree(p) => ("subpath", p),
    };
    // The authority must bind physical roots, not a mutable symlink name.
    // Literal nonexisting files are supported when their parent is canonical.
    let canonical = if path.exists() {
        path.canonicalize().map_err(|e| e.to_string())?
    } else if matches!(scope, PathScope::Literal(_)) {
        let parent = path.parent().ok_or("Sandbox path has no parent")?;
        parent
            .canonicalize()
            .map_err(|e| e.to_string())?
            .join(path.file_name().ok_or("Sandbox path has no basename")?)
    } else {
        return Err("Sandbox subtree does not exist".into());
    };
    if canonical != *path {
        return Err("Sandbox authority must supply canonical physical paths".into());
    }
    Ok(format!("({kind} {})", quoted(path)?))
}

pub fn access(access: &FileAccess) -> Result<Option<String>, String> {
    if access.allow.is_empty() {
        return Ok(None);
    }
    let allowed = access
        .allow
        .iter()
        .map(scope)
        .collect::<Result<Vec<_>, _>>()?;
    let mut clauses = vec![format!("(require-any {})", allowed.join(" "))];
    for blocked in &access.block {
        clauses.push(format!("(require-not {})", scope(blocked)?));
    }
    Ok(Some(format!("(require-all {})", clauses.join(" "))))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seatbelt_paths_reject_controls_and_escape_syntax() {
        let (control, quote, escaped) = if cfg!(windows) {
            (
                "C:\\tmp\\a\n(allow default)",
                r#"C:\tmp\a"b"#,
                r#""C:\\tmp\\a\"b""#,
            )
        } else {
            ("/tmp/a\n(allow default)", r#"/tmp/a"b"#, r#""/tmp/a\"b""#)
        };
        assert!(quoted(Path::new(control)).is_err());
        assert_eq!(quoted(Path::new(quote)).as_deref(), Ok(escaped));
    }
}
