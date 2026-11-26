use argentic_harness::{
    providers::{ToolCall, ToolDefinition},
    tools::{Extensions, Page, SKILL},
};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, sync::Arc};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::{
    agsdl_host_agent_mcp030::Equipment,
    agsdl_host_content030::Sources030,
    content::{LoadedSkill, SkillDescriptor},
    persistence::{failure, Result as HostResult},
};

pub(super) struct Tools {
    sources: Arc<Mutex<Sources030>>,
    skills: Vec<SkillDescriptor>,
    loaded: Mutex<BTreeMap<String, LoadedSkill>>,
    equipment: Arc<Equipment>,
    mcp: BTreeMap<String, McpTool>,
}

struct McpTool {
    server: String,
    name: String,
    definition: ToolDefinition,
}

fn mcp_alias(server: &str, tool: &str) -> String {
    // Preserve readable aliases when neither component can contain a separator.
    if server.len() + tool.len() <= 57
        && server
            .bytes()
            .chain(tool.bytes())
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
    {
        return format!("mcp__{server}__{tool}");
    }
    let mut digest = Sha256::new();
    digest.update((server.len() as u64).to_be_bytes());
    digest.update(server.as_bytes());
    digest.update(tool.as_bytes());
    format!("mcp_pair_{}", URL_SAFE_NO_PAD.encode(digest.finalize()))
}

#[cfg(test)]
#[test]
fn mcp_aliases_distinguish_pairs_across_rediscovery() {
    let long = "outils".repeat(40);
    let pairs = [
        ("a__b", "c"),
        ("a", "b__c"),
        ("a_", "b"),
        ("a", "_b"),
        ("a", long.as_str()),
        ("a", "révision"),
    ];
    let first: BTreeMap<_, _> = pairs
        .iter()
        .map(|&(server, tool)| (mcp_alias(server, tool), (server, tool)))
        .collect();
    assert_eq!(first.len(), pairs.len());
    for (server, tool) in pairs.into_iter().rev() {
        let alias = mcp_alias(server, tool);
        assert_eq!(first[&alias], (server, tool));
        assert!((1..=64).contains(&alias.len()));
        assert!(alias
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"_-".contains(&byte)));
    }
    assert_eq!(mcp_alias("first", "check"), "mcp__first__check");
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SkillArguments {
    name: Option<String>,
    path: Option<String>,
}

impl Tools {
    pub fn new(
        sources: Arc<Mutex<Sources030>>,
        skills: Vec<SkillDescriptor>,
        equipment: Arc<Equipment>,
    ) -> HostResult<Self> {
        let mut mcp = BTreeMap::new();
        for (server, name, tool) in equipment.tools() {
            let alias = mcp_alias(server, name);
            if mcp.contains_key(&alias) {
                return Err(failure(
                    "MCP_TOOL_LIST",
                    "/mcpServers",
                    "MCP tools have a conflicting provider alias",
                ));
            }
            let description = tool["description"].as_str().unwrap_or_default();
            mcp.insert(
                alias.clone(),
                McpTool {
                    server: server.to_owned(),
                    name: name.to_owned(),
                    definition: ToolDefinition {
                        name: alias,
                        description: format!("MCP {server}/{name}. {description}"),
                        parameters: tool["inputSchema"].clone(),
                    },
                },
            );
        }
        Ok(Self {
            sources,
            skills,
            loaded: Mutex::new(BTreeMap::new()),
            equipment,
            mcp,
        })
    }

    fn catalogue(&self) -> Vec<Value> {
        self.skills
            .iter()
            .map(|skill| json!({"key":skill.key,"name":skill.name,"description":skill.description}))
            .collect()
    }

    pub fn definitions(&self) -> Vec<ToolDefinition> {
        let mut definitions = self
            .mcp
            .values()
            .map(|tool| tool.definition.clone())
            .collect::<Vec<_>>();
        if self.skills.is_empty() {
            return definitions;
        }
        definitions.push(ToolDefinition {
            name: SKILL.into(),
            description: format!(
                "List the selected AgSDL skills, activate one by its key, or read a relative file from an activated skill by providing its key and path. Skills grant no permissions. Selected skills: {}",
                json!(self.catalogue())
            ),
            parameters: json!({
                "type":"object","properties":{"name":{"type":"string"},"path":{"type":"string"}},
                "additionalProperties":false
            }),
        });
        definitions
    }
}

#[async_trait::async_trait]
impl Extensions for Tools {
    async fn execute(&self, call: &ToolCall, cancel: &CancellationToken) -> Result<Page, String> {
        if call.name != SKILL {
            let tool = self
                .mcp
                .get(&call.name)
                .ok_or("Unknown or denied MCP tool")?;
            let arguments =
                serde_json::from_str(&call.arguments).map_err(|_| "Invalid MCP arguments")?;
            let reply = self
                .equipment
                .call(&tool.server, &tool.name, arguments)
                .await
                .ok_or("MCP call denied, cancelled or unavailable")?;
            return Page::one_shot(&reply.to_string(), &call.id).map_err(|error| error.to_string());
        }
        if self.skills.is_empty() {
            return Err("No skill is selected for this work".into());
        }
        let args: SkillArguments =
            serde_json::from_str(&call.arguments).map_err(|_| "Invalid skill arguments")?;
        let Some(name) = args.name else {
            if args.path.is_some() {
                return Err("A skill key is required to read a skill resource".into());
            }
            return Page::one_shot(&json!(self.catalogue()).to_string(), &call.id)
                .map_err(|error| error.to_string());
        };
        let skill = self
            .skills
            .iter()
            .find(|skill| skill.key == name)
            .ok_or("Skill is not selected for this work")?;
        let serve = async {
            let sources = self.sources.lock().await;
            let mut loaded = self.loaded.lock().await;
            if let Some(path) = args.path {
                let active = loaded
                    .get(&name)
                    .ok_or("Activate the skill before reading its files")?;
                let text = sources
                    .skill_resource(skill, active, &path)
                    .await
                    .map_err(|error| format!("{}: Could not read skill resource", error.code))?;
                return Page::one_shot(&text, &call.id).map_err(|error| error.to_string());
            }
            let active = sources
                .load_skill(skill)
                .await
                .map_err(|error| format!("{}: Skill activation failed", error.code))?;
            // The shared resolver bounds the entire selected set at discovery.
            let text = active.instructions.clone();
            loaded.insert(name, active);
            Ok(Page {
                text,
                next_cursor: None,
                overflow: None,
            })
        };
        tokio::select! {
            biased;
            _ = cancel.cancelled() => Err("Skill call cancelled".into()),
            result = serve => result,
        }
    }
}
