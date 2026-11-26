# Argentic harness

In-process Rust runner for a fresh AgSDL 0.4.0 agent instance. The crate has its
own manifest and lockfile. Its [module draft](module.json) declares the features
available at the current integration boundary.

## Running a work

1. Resolve settings with `Settings::resolve(host, user, project)`. Pass an
   empty object for an omitted layer. All five settings are ceilings. A project
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
| `prepare(PreparedEffectiveAgentWork030)` | Validate provider selection and support for requested properties. Resolve settings and compile the effective read rules and submission schema locally. The runtime's Argentic-specific constructor for `AttestedWorkPolicy030` must bind these choices to the existing capture and lease checks. |
| `run(AgentWork030, WorkEventSink030, CancellationToken)` | Decode the admitted capture and start message into `Work`. Pass the attempt token as parent and keep the harness future in the runtime's ownership registry until join. |
| Visible text | Project each `Event::VisibleText` to a `visibleText` frame with a unique message ID per part. Reasoning and tool replies belong in separate diagnostic channels. |
| Submit and logical turn end | Map `Proposal::Submit` to `submit` with a work-unique call ID and the original `argumentsJson`. Map `Proposal::EndTurn` to `turnEnded`. Feed the receipt's acceptance or correction back through `Decision`. The sink owns completion-check execution and the durable result. Keep the precise check and submission correction categories. |
| Steer | Bridge the admitted inbox to `Steer`, retaining the delivery's attempt, sender, deadline and receipt. The loop appends messages at tool-batch or end-of-turn boundaries and acknowledges that insertion. The adapter must gate insertion with `wire_admission` and translate consumed entries to runtime receipts, with unsent entries falling back to the host queue. Buffer text events if emission could wait on the pending delivery. |
| Approvals | The slice denies reads that would require approval. Runtime review ports are future work. |
| Physical termination | After `RunReport::physical_end`, return from the owned executor future. The native registry and its join generate durable `physicalEnded` evidence. |

The crate does not register the executor with the host. Shared and named
instances, durable steer receipts, human or model reviewers, compaction,
MCP, writing and shell tools, and additional provider protocols remain
outside this slice. The exact host controller remains authoritative for
AgSDL submission semantics and deadlines.

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
