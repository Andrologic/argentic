# Argentic harness

In-process Rust runner for a fresh AgSDL 0.4.0 agent instance. The crate has its
own manifest and lockfile. The [engine module](../argentic-core/assets/harnesses/argentic/module.json)
describes the registered adapter. The [crate module](module.json) describes
the standalone loop and the features an embedding host can enable.

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

For Claude Messages and OpenAI Responses, a single host can configure both
protocols.

```json
{
  "providers": {
    "claude": {
      "protocol": "anthropic-messages",
      "baseUrl": "https://api.anthropic.com",
      "apiKeyEnv": "ANTHROPIC_API_KEY",
      "models": ["claude-sonnet-4-6#work"],
      "promptCaching": true
    },
    "openai": {
      "protocol": "openai-responses",
      "baseUrl": "https://api.openai.com/v1",
      "apiKeyEnv": "OPENAI_API_KEY",
      "models": ["gpt-5.4"]
    }
  },
  "defaultModel": "claude/claude-sonnet-4-6#work"
}
```

- `anthropic-messages` appends `/v1/messages` and uses the key as `x-api-key`.
  Set `promptCaching: true` to mark the end of the tools and system prefix
  for Anthropic's ephemeral cache. It defaults to `false`. Prompt usage
  includes uncached input, cache reads and cache creation.
- `openai-responses` appends `/responses` and uses the key as a bearer token.
  Requests use `store: false` and ask for encrypted reasoning. The harness
  replays encrypted reasoning items and completed output items with each
  subsequent request. Cached tokens, if reported, are already included in
  the input total. Responses requires `maxOutputTokens` and
  `maxTotalTokens` of at least 16. The work stops at the token limit if
  fewer than 16 tokens remain before another request.

Messages requires the `anthropic` reasoning catalogue, even when the provider
uses an alias. For listed Claude models, `low`, `medium` and `high` request
1,024, 2,048 and 8,192 thinking tokens respectively. The harness caps this
budget below the turn's output limit and omits the thinking configuration if
fewer than 1,024 tokens fit. `none` explicitly disables thinking. Without an
effort selection, the provider uses its default. The harness keeps signed and
redacted thinking blocks in order for follow-up requests.
[Anthropic extended thinking](https://platform.claude.com/docs/en/build-with-claude/extended-thinking)

Responses defaults to the `openai` reasoning catalogue and sends the selected
effort as `reasoning.effort`. GPT-5.4 supports `none`.
[OpenAI GPT-5.4 model documentation](https://developers.openai.com/api/docs/models/gpt-5.4)

HTTP 429 and 5xx responses can be retried within the turn's time and byte
limits. A transient error event can also be retried before any output has
been emitted. After output, the error ends the turn. The Host remembers a
provider's explicit effort rejection for the rest of the work attempt.

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
3. Construct `Harness::new(provider, registry, settings)`. Configure a provider
   ID and the full endpoint on `ChatCompletions`, `AnthropicMessages` or
   `OpenAiResponses`. A name such as
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
and returns filenames, one-based line numbers and matching lines. The 64 MiB
scan budget counts bytes read from skipped binary and oversized files too.
Read calls can run in parallel up to `maxParallelReads` when no reviewer is attached.
Replies retain the provider's call order. Mutations, shell calls and submissions
are barriers. An accepted submission ends work.

All tools use admitted directory handles. They refuse parent components,
absolute paths, symbolic links and private project metadata. File reads and
mutations also refuse multiply linked files. Matching rules override the
preset and its zone. When several rules match, the strictest effect wins, in
the order `deny`, `ask`, `allow`. The registry checks each ceiling separately
and denies an `ask` without a reviewer.

The engine can enable `write`, `edit` and `apply_patch` for paths relative to
the selected root. Edits and patch updates require read and write permission.
Patch moves also require permission on the destination. Writes use per-file queues
and atomic replacement. For an approved file operation, the Host rechecks every
requested path against the live policy after the tool acquires its locks. It holds
that policy lease through file I/O and publication. A patch validates its whole
plan before changing files.
An I/O failure during publication can leave some files changed, and the reply
reports how many changes were applied. Parent directories must already exist.

## Planning, review and delegation

Set `"plan": true` to give the agent a `plan` tool. It accepts a nonempty
`steps` array with `step` and `status` fields. A status is `pending`,
`in_progress` or `completed`. The Host journals each update as a
`planPresented` work event. The current plan remains verbatim in model
requests even when compaction summarizes its original tool call.

`"planFirst": "auto"` enables the same tool and accepts its first valid plan
for the work. `"planFirst": "ask"` submits that plan for human review. Until
acceptance, the agent can read files and selected skills. Writes, shell, MCP
discovery and calls, delegation and final submission remain blocked. A refusal
with a comment asks the agent to revise. A refusal without a comment, an
`answerTimeoutMs` expiry or an unavailable person ends the work with
`PLAN_REJECTED`.

Set `"reviewer": "person"` inside `permissions` to send an action with effect
`ask` through the engine's human request broker. An answer applies to one
action in one call. Refused or expired requests return tool errors. The engine
excludes the human wait from step machine time and closes pending requests on
Stop.

For `"reviewer": "model"`, set `"reviewerModel": "local/reviewer-model"` at the
top level of the Host's provider configuration and list `reviewer-model` in
that provider's `models`. The reviewer receives the exact action and arguments,
the policy context, the active mission, delivered steer messages and bounded
excerpts from the recent conversation. It has no tools and must return a JSON
object such as `{"verdict":"APPROVE"}`. The Host records `APPROVE`, `DENY` or
`ESCALATE` as a `reviewerDecision` work event.
The Host rechecks policy before delivering an approval and records the final
outcome as a `reviewerAuthorization` event with the same request ID. Provider
errors, invalid replies and a 15-second deadline yield denial. Shell, proxy
network and MCP asks require a person.

Model reviews are limited to four requests per agent turn and 32 per work.
With `maxTotalTokens`, the host reserves prompt and output tokens before a
review and charges the reported usage to the work. A review that cannot fit
is refused. If usage is missing, the reservation stays charged and the
model's verdict cannot authorize the action.

Set `"personReviewAvailable": false` in the Host's provider configuration for
headless runs. Human-only asks and escalations then return a tool refusal.
`planFirst: "ask"` fails with `PLAN_REJECTED` when the agent presents its plan.
The setting defaults to `true`.

An effective `delegate` grant exposes `delegate({ "task": "…" })`. An `ask`
goes through the configured reviewer. Each call starts one child conversation
with the parent's provider and model, the supplied task and read-only access
under the parent's effective policy. The child has no shell or network tools.
It has up to eight turns within the parent's remaining turn and token budgets.
The parent receives the child's final text or a tool error. Stop cancels the child and the engine
waits for its provider stream and tool workers before recording physical
termination.

## Shell sandbox

Linux ARM64 and x86-64 require system Bubblewrap and Bash. The qualification
probe checks unprivileged user and PID namespaces, seccomp and pidfds. On
Ubuntu 24.04, deploy with an AppArmor policy that permits user namespaces for
Bubblewrap. CI disables the restriction on its disposable runners. Linux
shells start with a cleared environment, private PID and network namespaces,
private `/tmp`, `/dev` and `/proc`, and read-only system binaries and libraries.
They drop capabilities, and seccomp blocks tracing and other privileged
interfaces.

macOS ARM64 qualification probes `sandbox-exec`. The generated Seatbelt profile
exposes staged roots, private scratch space and required system files. Executable paths are limited to Bash and
a list of system commands, including curl. Bash starts in the staged workspace and sees
other staged roots as `ARGENTIC_DIRECTORY_<index>`. Windows refuses shell at
qualification. To use file mutations without shell, declare a global `shell`
denial.

### Shell networking

On Linux, building the shell network relay requires a C linker and the target's
libc development files. The build tries static linking first, then dynamic
linking against the system libraries mounted in the sandbox. If both attempts
fail, the crate still builds and isolated shell commands remain available.
Preflight refuses shell networking until the relay can be built.

Set `shellNetwork` in the host's Argentic provider configuration. It defaults
to `"blanket"`. In that mode a shell can use the proxy only if every policy
layer grants network access unconditionally. Qualification rejects network
rules with a host match when the agent can run shell commands. Blanket mode
passes CONNECT bytes through without inspecting TLS or checking the
authorities inside the tunnel.

Set `shellNetwork: "intercept"` to use host patterns. Argentic terminates
client TLS with a CA generated for the shell invocation. It keeps the CA
private key in memory and exposes a read-only trust bundle containing the
system roots and the invocation CA. This lets the host process read HTTPS
request and response contents, including any credentials the command sends.
Clients must trust the bundle and support HTTP/1.1. Certificate pinning or
clients that require a different trust store can cause connections to fail.

Intercept mode requires the TLS SNI and each HTTP request's authority to match
the CONNECT host, with the usual absence of SNI on IP literal connections.
It checks each request against the effective network policy and verifies the
upstream certificate against the host's trust roots. It offers only HTTP/1.1
over TLS. Redirects that clients follow go through the same checks as new
requests. Rules and request hosts use the same IDNA normalization. Use a host
pattern without a scheme, path, or port.

The OS blocks direct egress in both modes. On Linux, Bubblewrap installs the
embedded Rust relay on its read-only root. The relay forwards private loopback
traffic to the mounted Unix socket and exits with the sandbox namespace. Seatbelt restricts outbound TCP to the
invocation's local proxy port. Argentic sets proxy environment variables and
clears both forms of `NO_PROXY`. Clients that ignore the proxy cannot reach
the network directly.

The proxy resolves names on the host and connects to checked IP addresses.
Private, loopback and link-local addresses need an exact IP grant. Unspecified
addresses are always blocked. In intercept mode an `ask` requires a person's
decision. The proxy remembers each answer for that host for the lifetime of
the invocation and withdraws pending requests when the client or shell exits.
Human wait time pauses the shell and proxy machine deadlines.

Each invocation can use up to 128 proxy connections, with 16 active at once
and a shared 64 MiB wire budget. Each connection has a two-minute machine-time
budget, subject to the shell deadline. DNS resolution, dialing, and TLS
handshakes each have a ten-second timeout.

### Staging and publication

The default outside denial leaves the system runtime readable as AgSDL
specifies. Qualification refuses shell with an explicit `outside: "deny"`
or any read denial or approval rule that overlaps a required runtime mount.
The executor checks all effective policy layers before enabling shell.

Bash starts in a private copy built from verified directory and file handles.
On Linux the copy is mounted at `/workspace`. Each staged root can contain up
to 50,000 entries and 256 MiB, with an 8 MiB limit per source or published file.
Up to four workers create fresh inodes. Linux shares data blocks through
copy-on-write when the source and staging filesystems support reflinks. Other
filesystems use bounded copies. Writable staging keeps SHA-256 digests for
publication checks.
The scan omits paths without a read grant, private metadata, symlinks, special
files and files with multiple hard links. Any ceiling that restricts workspace
reads or writes makes the entire copy read-only. The direct file tools can
still change individual paths according to the policy.

For a writable shell, the launcher checks the staging tree before starting
Bash, approximately every 50 ms during execution, and after all sandbox
processes exit. A sample exceeding 256 MiB, 50,000 directory entries, or the
depth budget aborts the call without publishing. It counts the greater of
logical size and allocated blocks per regular file, and allocated blocks
for other entries. The kernel also limits each file to 16 MiB and open file
descriptors to 256 per process. The process limit is 1,024, subject to Linux's
real-UID accounting and exemptions. Lower inherited limits still apply.

Sampling can miss transient growth and storage held by open, unlinked files.
For a hard disk budget, set the host's top-level `shellStagingDirectory` to an
existing absolute directory outside the workspace on a volume with byte and
inode quotas. The default is the host's temporary directory.

After a writable run, the host compares the staged bytes and modes with the
originals and checks read and write permission for every changed path before
applying any change. Only regular files and deletions are published, and their
parent directories must already exist in the workspace. Publication uses the
same atomic file replacement as the mutation tools. On timeout, Stop, or a
staging budget error Linux kills PID 1 through a pidfd and joins it and the
Bubblewrap supervisor. The kernel terminates the remaining namespace processes.
On macOS the launcher kills the process group and checks the private session
for remaining jobs, including processes in other job-control groups. It waits
for the session to end before publishing.

The staging mount contains separate inodes from the host workspace. Workspace
roots are host capabilities and should be managed by the host. Mutations
check the location of the parent handle and the staged file immediately
before publication, then replace or create the destination relative to that
same parent handle. A different host process with write access can still
rename an entry or directory between the final check and the operation.

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
| Plan | Journal structured `planPresented` events. Plan-first ask uses the native plan lock and the human broker. |
| Approvals | Review exact asks with the configured model or deliver one-shot decisions through the shared engine human broker. |
| Delegation | Execute a read-only child in the same process under a narrower registry and the same work budget. |
| Physical termination | After `RunReport::physical_end`, return from the owned executor future. The native registry and its join generate durable `physicalEnded` evidence. |

The standalone host registers the adapter when configured and requires explicit
opt-in. Shared and named instances keep their conversation in memory for the run.
Compaction runs when the provider entry sets `contextWindowTokens`.
Credential screening covers provider keys of at least 16 bytes. Short local
placeholders such as `lm-studio` can still authenticate a request. The
harness masks literal echoes in assistant text and reasoning across stream
deltas and counts the filtered bytes toward its response budget. Structured
screening checks decoded JSON string values. Protocol IDs, tool names, object
keys and JSON syntax remain intact. Screened tool arguments receive a
correction in the same conversation, including `agsdl_submit_result`.
A credential in signed replay data invalidates the reply. Tool results are
screened before pagination and overflow-file creation.

- Agent and message resources appear as origin-labelled, untrusted reference
  data in a separate user message. The Host resolves local URIs under the
  recipient's effective read permissions.
- The `skill` tool lists selected skills. Pass a skill key as `name` to
  activate its instructions, and pass `name` and `path` to read an activated
  skill's text or JSON companion file. Compaction preserves the activation
  exchange in the conversation.
- The Host discovers tools from selected MCP servers and exposes them with
  provider-safe names, typically `mcp__server__tool`. Permissions apply to the
  original `server/tool` name. An `ask` requires a person and never grants
  lasting access. MCP arguments containing the provider credential are refused
  before approval or dispatch. Local stdio requires macOS ARM64. Remote servers use the
  Host's authorized Streamable HTTP transport. Stop closes admission and
  joins local sessions. A dispatched remote call whose termination cannot be
  confirmed yields `MCP_TERMINATION_UNCERTAIN`. Operator Hold is unavailable
  for runs that select MCP servers.
- File tools use `root: 0` for the workspace and positive indices for up to
  eight additional roots. Shell commands see filtered snapshots under
  `/directories/<index>` on Linux and through `ARGENTIC_DIRECTORY_<index>` on
  macOS. A root is writable in the shell only when
  every effective policy grants unrestricted read and write on that root.
  Work admission refuses overlapping shell roots and roots covering private
  Host data.

The [Host's Argentic module](../argentic-core/assets/harnesses/argentic/module.json)
declares native planning and reviewers and harness enforcement for delegation.
`completionCheck` is unavailable in this adapter.

After a missing required submission, Argentic forces the result tool by name
by default. If the provider rejects the named choice, Argentic retries with
`required` (`any` for Anthropic Messages) and only the result tool. If the
provider rejects that form, Argentic restores all tools and disables forced
choice for the rest of the attempt. Later corrections use the fallback level
reached in that attempt. A request that delivers steering offers all tools
with automatic selection.

When Anthropic Messages retries a correction with only the result tool, the
shorter tool list changes the prompt-cache prefix. A cache breakpoint on the
tool list moves to the remaining tool.

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
