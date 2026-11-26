use std::{
    collections::HashMap,
    ops::ControlFlow,
    time::{Duration, Instant},
};
use tree_sitter::{Node, ParseOptions, Parser};

/// Match raw Bash command text, including redirections and nested commands.
pub fn subcommands(source: &str) -> Result<Vec<&str>, String> {
    if source.len() > 64 * 1024 || source.contains('\0') {
        return Err("Invalid shell command length or NUL byte".into());
    }
    let mut parser = Parser::new();
    parser
        .set_language(&tree_sitter_bash::LANGUAGE.into())
        .map_err(|error| error.to_string())?;
    let end = Instant::now() + Duration::from_millis(250);
    let mut progress = |_: &tree_sitter::ParseState| {
        if Instant::now() >= end {
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(())
        }
    };
    let tree = parser
        .parse_with_options(
            &mut |offset, _| source.as_bytes().get(offset..).unwrap_or_default(),
            None,
            Some(ParseOptions::new().progress_callback(&mut progress)),
        )
        .ok_or("Shell parsing exceeded its budget")?;
    let root = tree.root_node();
    if root.has_error() {
        return Err("Invalid Bash syntax".into());
    }

    let mut commands = Vec::new();
    let mut redirects = HashMap::new();
    let mut pending = vec![root];
    while let Some(node) = pending.pop() {
        if node.kind() == "redirected_statement" {
            if let Some(command) = node.child_by_field_name("body").and_then(last_command) {
                // The grammar sometimes attaches redirections to the whole
                // pipeline. Bash attaches them to its final command.
                redirects.insert(command.id(), redirection_end(node));
            } else {
                commands.push(&source[node.start_byte()..redirection_end(node)]);
            }
        } else if is_command(node) {
            let end = redirects.remove(&node.id()).unwrap_or(node.end_byte());
            commands.push(&source[node.start_byte()..end]);
        }
        for i in (0..node.named_child_count()).rev() {
            pending.extend(node.named_child(i as u32));
        }
    }
    if commands.is_empty() {
        return Err("No shell commands found".into());
    }
    Ok(commands)
}

fn redirection_end(node: Node<'_>) -> usize {
    let mut cursor = node.walk();
    let heredoc = node
        .children_by_field_name("redirect", &mut cursor)
        .find(|child| child.kind() == "heredoc_redirect");
    heredoc
        .and_then(|heredoc| {
            let mut cursor = heredoc.walk();
            let delimiter = heredoc
                .named_children(&mut cursor)
                .find(|child| child.kind() == "heredoc_start");
            delimiter
        })
        .map_or(node.end_byte(), |delimiter| delimiter.end_byte())
}

fn last_command(mut node: Node<'_>) -> Option<Node<'_>> {
    while matches!(node.kind(), "pipeline" | "negated_command") {
        node = node.named_child((node.named_child_count().checked_sub(1)?) as u32)?;
    }
    is_command(node).then_some(node)
}

fn is_command(node: Node<'_>) -> bool {
    match node.kind() {
        "command"
        | "declaration_command"
        | "unset_command"
        | "test_command"
        | "variable_assignments" => true,
        "variable_assignment" => node.parent().is_none_or(|parent| {
            !matches!(
                parent.kind(),
                "command" | "declaration_command" | "variable_assignments" | "variable_assignment"
            )
        }),
        _ => false,
    }
}
