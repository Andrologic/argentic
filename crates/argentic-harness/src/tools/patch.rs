//! Patch parser and hunk application adapted from Andrologic/Macro,
//! src-tauri/src/core/workspace_execution/mod.rs (a13d3bdb, AGPL-3.0-or-later).

use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone)]
pub(crate) enum ParsedPatchOperation {
    Add {
        path: String,
        lines: Vec<String>,
    },
    Update {
        path: String,
        hunks: Vec<PatchHunk>,
        move_to: Option<String>,
    },
    Delete {
        path: String,
    },
}

impl ParsedPatchOperation {
    pub(crate) fn path(&self) -> &str {
        match self {
            Self::Add { path, .. } | Self::Update { path, .. } | Self::Delete { path } => path,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct PatchHunk {
    pub(crate) lines: Vec<PatchHunkLine>,
}

#[derive(Debug, Clone)]
pub(crate) struct PatchHunkLine {
    pub(crate) kind: char,
    pub(crate) content: String,
}

pub(crate) fn parse_apply_patch(patch_text: &str) -> Result<Vec<ParsedPatchOperation>, String> {
    let lines: Vec<&str> = patch_text.lines().collect();
    if lines.first().copied() != Some("*** Begin Patch") {
        return Err(String::from(
            "Invalid apply_patch payload: missing '*** Begin Patch' header.",
        ));
    }
    if lines.last().copied() != Some("*** End Patch") {
        return Err(String::from(
            "Invalid apply_patch payload: missing '*** End Patch' footer.",
        ));
    }

    let mut operations = Vec::new();
    let mut index = 1usize;
    while index + 1 < lines.len() {
        let line = lines[index];
        if line.trim().is_empty() {
            index += 1;
            continue;
        }

        if let Some(path) = line.strip_prefix("*** Add File: ") {
            let mut added_lines = Vec::new();
            index += 1;
            while index + 1 < lines.len() && !lines[index].starts_with("*** ") {
                let current = lines[index];
                let Some(content) = current.strip_prefix('+') else {
                    return Err(format!(
                        "Invalid add-file line for {}: expected '+' prefix.",
                        path
                    ));
                };
                added_lines.push(content.to_string());
                index += 1;
            }
            operations.push(ParsedPatchOperation::Add {
                path: path.trim().to_string(),
                lines: added_lines,
            });
            continue;
        }

        if let Some(path) = line.strip_prefix("*** Update File: ") {
            let mut hunks = Vec::new();
            let mut hunk_lines = Vec::new();
            index += 1;
            let move_to = lines
                .get(index)
                .and_then(|line| line.strip_prefix("*** Move to: "))
                .map(|path| path.trim().to_string());
            if move_to.is_some() {
                index += 1;
            }
            while index + 1 < lines.len() && !lines[index].starts_with("*** ") {
                let current = lines[index];
                if current == "@@" || current.starts_with("@@ ") {
                    if !hunk_lines.is_empty() {
                        hunks.push(PatchHunk { lines: hunk_lines });
                        hunk_lines = Vec::new();
                    }
                    index += 1;
                    continue;
                }

                let Some(kind) = current.chars().next() else {
                    return Err(format!("Invalid update hunk line for {}.", path));
                };
                if !matches!(kind, ' ' | '+' | '-') {
                    return Err(format!(
                        "Invalid update hunk line for {}: expected ' ', '+', or '-'.",
                        path
                    ));
                }
                hunk_lines.push(PatchHunkLine {
                    kind,
                    content: current[1..].to_string(),
                });
                index += 1;
            }
            if !hunk_lines.is_empty() {
                hunks.push(PatchHunk { lines: hunk_lines });
            }
            if hunks.is_empty() {
                return Err(format!(
                    "Update patch for {} must contain at least one hunk.",
                    path
                ));
            }
            operations.push(ParsedPatchOperation::Update {
                path: path.trim().to_string(),
                hunks,
                move_to,
            });
            continue;
        }

        if let Some(path) = line.strip_prefix("*** Delete File: ") {
            operations.push(ParsedPatchOperation::Delete {
                path: path.trim().to_string(),
            });
            index += 1;
            continue;
        }

        return Err(format!("Invalid apply_patch section header: {}", line));
    }

    if operations.is_empty() {
        return Err(String::from(
            "Invalid apply_patch payload: no file operations were provided.",
        ));
    }

    Ok(operations)
}

fn split_text_lines(content: &str) -> Vec<(&str, &str)> {
    content
        .split_inclusive('\n')
        .map(|line| {
            if let Some(text) = line.strip_suffix("\r\n") {
                (text, "\r\n")
            } else if let Some(text) = line.strip_suffix('\n') {
                (text, "\n")
            } else {
                (line, "")
            }
        })
        .collect()
}

pub(crate) fn join_text_lines(lines: &[String], trailing_newline: bool) -> String {
    let mut joined = lines.join("\n");
    if trailing_newline {
        joined.push('\n');
    }
    joined
}

fn find_line_sequence(
    lines: &[(&str, &str)],
    needle: &[&str],
    start_index: usize,
    cancel: &CancellationToken,
) -> Result<Option<usize>, String> {
    if needle.is_empty() {
        return Ok(Some(start_index.min(lines.len())));
    }
    if needle.len() > lines.len() {
        return Ok(None);
    }

    for candidate_start in start_index..=lines.len().saturating_sub(needle.len()) {
        super::check_cancel(cancel).map_err(|error| error.to_string())?;
        if lines[candidate_start..candidate_start + needle.len()]
            .iter()
            .map(|(text, _)| *text)
            .eq(needle.iter().copied())
        {
            return Ok(Some(candidate_start));
        }
    }
    Ok(None)
}

pub(crate) fn apply_patch_hunks_to_content(
    path: &str,
    current_content: &str,
    hunks: &[PatchHunk],
    cancel: &CancellationToken,
) -> Result<String, String> {
    let mut lines = split_text_lines(current_content);
    let newline = lines
        .iter()
        .map(|(_, ending)| *ending)
        .find(|ending| !ending.is_empty())
        .unwrap_or("\n");
    let mut search_start = 0usize;

    for hunk in hunks {
        let old_lines = hunk
            .lines
            .iter()
            .filter(|line| line.kind != '+')
            .map(|line| line.content.as_str())
            .collect::<Vec<_>>();

        let replace_at = find_line_sequence(&lines, &old_lines, search_start, cancel)?
            .ok_or_else(|| format!("Patch hunk could not be applied cleanly to {}.", path))?;
        let replace_end = replace_at + old_lines.len();
        let mut old = lines[replace_at..replace_end].iter().copied();
        let mut replacement = Vec::new();
        for line in &hunk.lines {
            match line.kind {
                '+' => replacement.push((line.content.as_str(), newline)),
                '-' => {
                    old.next();
                }
                _ => replacement.extend(old.next()),
            }
        }
        search_start = replace_at + replacement.len();
        lines.splice(replace_at..replace_end, replacement);
    }

    let mut output = String::new();
    for (index, (text, ending)) in lines.iter().enumerate() {
        output.push_str(text);
        if index + 1 < lines.len() || current_content.ends_with('\n') {
            output.push_str(if ending.is_empty() { newline } else { ending });
        }
    }
    Ok(output)
}
