# Argentic harness

In-process Rust runner for a fresh AgSDL 0.4.0 agent instance. The crate has its
own manifest and lockfile. The [engine module](../argentic-core/assets/harnesses/argentic/module.json)
describes the registered adapter. The [crate module](module.json) describes
the default standalone loop.

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

Set top-level `defaultModel` to a configured identifier such as
`local/qwen3:8b` to let agents omit `model.name`.

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
EOF requests Stop for active runs. Stop evidence covers local termination,
including closing the provider response stream and joining tool workers.
The remote server may continue generating after the connection closes.

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
2. Compile a `Policy` against the canonical workspace path for each permission
   ceiling. Open a `Registry` with the host-admitted roots and the current
   step's submission envelope schema. Use that schema to describe named
   results, media types, allowed choices, and route instruction requirements.
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
for tracked filesystem and shell jobs. `RunReport::physical_end` is returned after
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
can run in parallel up to `maxParallelReads` when no reviewer is attached.
Replies retain the provider's call order. Mutations, shell calls and submissions
are barriers. An accepted submission ends work.

All tools use admitted directory handles. They refuse parent components,
absolute paths, symbolic links and private project metadata. File reads and
mutations also refuse multiply linked files. Matching rules override the
preset and its zone. When several rules match, the strictest effect wins, in
the order `deny`, `ask`, `allow`. The registry checks each ceiling separately
and denies an `ask` without a reviewer.

The engine can enable `write`, `edit` and `apply_patch` for workspace-relative
files. Edits and patch updates require read and write permission. Patch
moves also require permission on the destination. Writes use per-file queues
and atomic replacement. A patch validates its whole plan before changing files.
An I/O failure during publication can leave some files changed, and the reply
reports how many changes were applied. Parent directories must already exist.

Use `reviewer: "person"` to have the engine ask about a direct read, write or
shell call. The reply covers that specific action in that call. A refused or
expired answer returns a tool error. Engine Stop closes pending questions.
`answerTimeoutMs` bounds the human wait, which the engine excludes from step
machine time.

## Linux shell

Shell qualification runs a probe with system Bubblewrap. Install `bwrap`
and Bash on Linux ARM64 or x86-64. The host kernel must allow unprivileged user
and PID namespaces, seccomp filters and pidfds. The probe detects container
or AppArmor restrictions. On Ubuntu 24.04, deploy with an AppArmor policy
that permits user namespaces for Bubblewrap. CI disables the restriction
on its disposable runners. macOS and Windows refuse shell at qualification.
To use file mutations without shell, declare a global `shell` denial.

The sandbox starts with a cleared environment, a private PID namespace and
a private network namespace. It mounts the system runtime read-only and
provides private `/tmp`, `/dev` and `/proc`. The process drops capabilities
and seccomp blocks tracing, process introspection syscalls and other privileged
kernel interfaces. Shell networking requires a blanket grant from every
ceiling. The network-enabled profile allows IPv4 and IPv6 sockets. Per-host
grants cannot enable networking.

Bash sees a bounded copy of readable files at `/workspace`. Symlinks and
multiply linked files are omitted. The copy is writable only when every
ceiling grants unrestricted workspace reads and writes. The host applies
changed regular files and deletions after the process exits, using the
same atomic writer as the file tools. Publication requires existing parent
directories. On timeout or Stop the host kills PID 1 through a pidfd and
waits for it and the Bubblewrap supervisor before returning. The kernel
terminates all processes remaining in that PID namespace.

Untrusted commands have no live mount of the workspace. Workspace roots
are host capabilities and should be managed by the host. File tools check
directory and file identity immediately before publication. A different
host process with write access can still rename a directory between that
check and the filesystem operation.

A preview contains up to 2,000 lines and 50 KiB of UTF-8 text. Use its
`next_cursor` with the same tool arguments to continue. A truncated first
page also saves the complete bounded output to a temporary file. The
`RunReport` keeps that file available to the host for as long as it owns
the artifact. The model receives a cursor, and the host receives the file.
Read and scan budgets bound the work behind a preview. Shell output also has
a capture cap. Shell previews omit the cursor because rerunning a shell call
could repeat its effects.

## Mapping to AgentExecutor030

| Runtime boundary | Harness mapping |
| --- | --- |
| `prepare(PreparedEffectiveAgentWork030)` | Validate captured provider configuration and authored properties. Attest the permission ceilings through the native capture and lease checks. |
| `run(AgentWork030, WorkEventSink030, CancellationToken)` | Decode the admitted capture into `Work` and compile the independent permission ceilings. Use the attempt token as the parent and keep the harness future in the runtime's ownership registry until join. |
| Visible text | Emit one `visibleText` frame per complete assistant message through `WorkPort::assistant_message`. Stream fragments remain available to harness consumers through `Event::VisibleText`. |
| Submit and logical turn end | Project `Proposal::Submit` and `Proposal::EndTurn` into the native work protocol. The controller validates the raw arguments and returns acceptance or a correction in the current conversation. |
| Steer | Pull from the admitted inbox at a safe point and gate the next request with `wire_admission`. For HTTP, acknowledge on 2xx response headers. An explicit refusal or a connection failure before sending leaves the steer eligible for queue fallback. Missing evidence after a possible send leaves delivery uncertain. |
| Approvals | Deliver one-shot action decisions through the shared engine human broker. |
| Physical termination | After `RunReport::physical_end`, return from the owned executor future. The native registry and its join generate durable `physicalEnded` evidence. |

The standalone host registers the adapter when configured and requires explicit
opt-in. Shared and named instances keep their conversation in memory for the
run. Route and delivery metadata travels as untrusted message data. The
module leaves authored resources unsupported because the adapter has no
separate resource channel. Additional authored directories, model reviewers,
conversation compaction, completion checks, skills, MCP and other provider
protocols remain unsupported.

## Source notes

The SSE framing and call accumulation use ideas from Macro's
`src-tauri/src/ai/openai_compatible.rs`. Effort fallback follows
`src/services/ai/chatCompletionsAdapter.ts` and `providerErrors.ts`.
The reasoning catalog is from `src/shared/ai/reasoningCatalog.json`, with
the resolver adapted from `src-tauri/src/ai/reasoning_catalog.rs`.
Scoped cursors and bounded previews adapt
`src-tauri/src/core/workspace_execution/tool_output.rs`. Capability-relative
reads follow the approach in `src-tauri/src/fs/confined.rs`. The patch parser
and hunk application come from `src-tauri/src/core/workspace_execution/mod.rs`.
Macro and this crate are AGPL-3.0-or-later.

Run `cargo test --locked --all-targets` here. The HTTP fixtures bind
`127.0.0.1`.
