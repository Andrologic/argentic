# Argentic harness

In-process Rust runner for a fresh AgSDL 0.4.0 agent instance. The crate has its
own manifest and lockfile. Its [module draft](module.json) declares the features
available at the current integration boundary.

## Try it

Run Ollama or LM Studio with a model that supports tool calls. Use the model ID
reported by the server. The example below uses Ollama at
`http://127.0.0.1:11434/v1` with `qwen3:8b`. For LM Studio, use its local
server address, usually `http://127.0.0.1:1234/v1`, and the ID of the loaded
model.

Create `providers.json` on the host.

```json
{
  "providers": {
    "local": {
      "protocol": "chat-completions",
      "baseUrl": "http://127.0.0.1:11434/v1",
      "models": ["qwen3:8b"]
    }
  }
}
```

The host appends `/chat/completions` to `baseUrl`. Each model in `models`
can have a `#variant` suffix. Use the suffix in the document and the provider
configuration. The server receives the model ID without the suffix. Set
`apiKeyEnv` to an environment variable name if the server requires a bearer
token. Set `reasoningCatalog` when the provider ID differs from the catalogue
name. Preflight checks the host-configured catalogue. The first provider
request happens when the work starts.

Save this document as `read.json`.

```json
{
  "contract": "agsdl-0.4.0",
  "id": "read-memo",
  "agents": {
    "reader": {
      "harness": "argentic",
      "model": { "name": "local/qwen3:8b" },
      "permissions": "read-only",
      "results": { "memo": { "required": true, "mediaType": "text/plain" } }
    }
  },
  "flow": {
    "entry": "read",
    "steps": {
      "read": {
        "type": "agent",
        "agent": "reader",
        "instance": "new",
        "prompt": "Read memo.txt with the read tool. Submit its text as the memo result.",
        "next": []
      }
    }
  }
}
```

Save this host policy as `policy.json`.

```json
{
  "owner": { "projectId": "demo", "planId": "read-memo", "branch": "local" },
  "sourceRevision": "1",
  "permissions": "read-only",
  "readOnly": true
}
```

From the repository root, create the workspace and start the host. Keep the
data directory separate from the workspace.

```sh
mkdir -p workspace
printf '%s\n' 'The memo is ready.' > workspace/memo.txt
cargo run --locked --manifest-path crates/argentic-runtime/Cargo.toml \
  --example argentic-host -- \
  --data "$PWD/host-data" --workspace "$PWD/workspace" \
  --source "$PWD/read.json" --policy "$PWD/policy.json" \
  --approve-policy "$(shasum -a 256 policy.json | cut -d ' ' -f 1)" \
  --stop-on-exit --argentic-config "$PWD/providers.json"
```

Send one JSON command per line. Wait for the work to finish before quitting.
EOF requests Stop for active runs.

```jsonl
{"op":"preflight","entry":{"deliveryId":"entry","messageJson":"{}"}}
{"op":"start","requestId":"demo","entry":{"deliveryId":"entry","messageJson":"{}"}}
{"op":"runs"}
```

`providers.json` can include a top-level `settings` object. An agent's
`harnessSettings` can lower those ceilings. See [the host guide](../argentic-runtime/HOST.md)
for the inspection commands. `maxTurnMs` defaults to 600,000 ms and covers
requests, retries and body reads. `maxTurnBytes` defaults to 16 MiB of
response data per provider turn, including error bodies and SSE comments.

## Running a work

1. Resolve settings with `Settings::resolve(host, user, project)`. Pass an
   empty object for an omitted layer. All settings are ceilings. A project
   can lower inherited values, including the cancellation grace period.
   `Settings::get` returns the effective value and its source.
2. Compile `ReadPolicy` against the canonical workspace path and open a
   `Registry` with the host-admitted roots and the current step's submission
   envelope schema. Use that schema to describe named results, media types,
   allowed choices, and the requirements for route instructions.
3. Construct `Harness::new(provider, registry, settings)`. For Chat Completions,
   configure a provider ID and endpoint on `ChatCompletions`. A name such as
   `openrouter/vendor/model#fast` selects the provider, keeps `vendor/model`
   as the wire model ID, and exposes `fast` to the host for configuration.
4. Run `Harness::run(work, port, steer_receiver, parent_cancel)`. Each run
   starts with the supplied messages and keeps one history across provider
   turns. Use `()` as the port for local validation, or implement `WorkPort`
   to receive events and participate in submission decisions. Pass
   `max_corrections = 2` when the document omits it. Hooks added with
   `with_hook` run in registration order.

The work uses a child cancellation token. The runner gives cooperative
shutdown a grace period, then drops the provider or hook future and waits
for tracked filesystem jobs. `RunReport::physical_end` is returned after
the workers exit. A blocked kernel filesystem call can outlast the grace
period. Local HTTP cancellation closes the request and does not attest to
the remote provider's process state.

`maxTurns` caps provider turns and `maxOutputTokens` caps requested output
per turn. When supplied, `maxTotalTokens` also gates tool execution and
subsequent turns using reported prompt and completion usage. Missing usage
fails that work. A provider can exceed the total by charging a request's
prompt before reporting its size.

## Direct tools

`read`, `list`, `glob` and `grep` take a zero-based `root` index and a
relative `path`. Directory tools default to the root. `glob` matches against
paths relative to the selected directory. `grep` takes a regular expression
and returns filenames, one-based line numbers and matching lines. Read calls
run in parallel up to `maxParallelReads`. Replies retain the provider's
call order. A submission is a barrier and an accepted submission ends work.

All tools use admitted directory handles. They refuse parent components,
absolute paths and symbolic links. File reads also refuse multiply linked
files. In no-human mode, an unmatched policy falls back to the configured
preset and zone, and an `ask` whose reviewer is unavailable is denied.

A preview contains up to 2,000 lines and 50 KiB of UTF-8 text. Use its
`next_cursor` with the same tool arguments to continue. A truncated first
page also saves the complete bounded output to a temporary file. The
`RunReport` keeps that file available to the host for as long as it owns
the artifact. The model receives a cursor, and the host receives the file.
Read and scan budgets bound the work behind a preview.

## Mapping to AgentExecutor030

| Runtime boundary | Harness mapping |
| --- | --- |
| `prepare(PreparedEffectiveAgentWork030)` | Validate captured provider configuration and authored properties. Compile a read policy for each independent permission ceiling and attest it through the native capture and lease checks. |
| `run(AgentWork030, WorkEventSink030, CancellationToken)` | Decode the admitted capture and start message into `Work`. Pass the attempt token as parent and keep the harness future in the runtime's ownership registry until join. |
| Visible text | Project each `Event::VisibleText` to a `visibleText` frame with a unique message ID per part. Reasoning and tool replies belong in separate diagnostic channels. |
| Submit and logical turn end | Project `Proposal::Submit` and `Proposal::EndTurn` into the native work protocol. The controller validates the raw arguments and returns acceptance or a correction in the current conversation. |
| Steer | Pull from the admitted inbox at a safe point. Gate conversation insertion with `wire_admission` and turn consumption into a runtime receipt. The host queues a message that does not reach the active work. |
| Approvals | The slice denies reads that would require approval. Runtime review ports are future work. |
| Physical termination | After `RunReport::physical_end`, return from the owned executor future. The native registry and its join generate durable `physicalEnded` evidence. |

The standalone host registers the adapter when configured and requires explicit
opt-in. Shared and named instances keep their conversation in memory for the
run. Route and delivery metadata travels as untrusted message data. The
module leaves authored resources unsupported because the adapter has no
separate resource channel. Conversation compaction, permission reviewers, completion checks, skills,
MCP, writing, shell tools, and other provider protocols are outside this slice.

## Source notes

The SSE framing and call accumulation use ideas from Macro's
`src-tauri/src/ai/openai_compatible.rs`. Effort fallback follows
`src/services/ai/chatCompletionsAdapter.ts` and `providerErrors.ts`.
The reasoning catalog is from `src/shared/ai/reasoningCatalog.json`, with
the resolver adapted from `src-tauri/src/ai/reasoning_catalog.rs`.
Scoped cursors and bounded previews adapt
`src-tauri/src/core/workspace_execution/tool_output.rs`. Capability-relative
reads follow the approach in `src-tauri/src/fs/confined.rs`. Macro and this
crate are AGPL-3.0-or-later.

Run `cargo test --locked --all-targets` here. The HTTP fixtures bind
`127.0.0.1`.
