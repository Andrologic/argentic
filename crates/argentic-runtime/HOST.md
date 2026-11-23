# Standalone host

`argentic-host` is a local JSON-lines operator over the production runtime. It
uses no Tauri, Macro source checkout or Bun. Opening it does not observe a
provider. Codex preflight and Start require both `--codex` and an explicit command.
It offers no HTTP fallback, automatically approved action or fabricated native
receipt. Default tests use synthetic inputs only.

The host accepts documents marked `agsdl-0.4.0` and `agsdl-0.3.0`.
The compiled plan and persisted capture keep the authored marker. The
Argentic crate version and protocol DTO names are independent of that
marker. The bundled harness descriptions come from AgSDL v0.4.0.

AgSDL v0.5.0 distributes `agsdl-project-1` directories and
`agsdl-archive-1` files. They assemble to an `agsdl-0.4.0` document and enter
the same compiler and Start admission as JSON. The published
[project format](https://github.com/Andrologic/AgSDL/blob/v0.5.0/spec/project.md)
and [archive format](https://github.com/Andrologic/AgSDL/blob/v0.5.0/spec/archive.md)
define the distribution rules.

## Embedding Argentic

In the native host, open a trusted local path with `OpenedSource::open_path`.
Pass a directory, its `agsdl.json` manifest, a `.agsdl` archive, or the
ordinary JSON document. For an archive uploaded by a remote client, pass the
bytes to `OpenedSource::open_archive_bytes`. A client path describes files on
the client's machine. It must never become an argument to `open_path` in
the host process. As in the archive format, a file whose name ends in
`.agsdl`, in any case, is read as an archive. A storage failure while
retaining a project returns `STORAGE` without host paths.

```rust
use argentic_runtime::{persistence::{dto::EngineSourceRevision, Result},
    source::OpenedSource, Engine};
use serde_json::Value;
use std::path::Path;

async fn compile_project(
    engine: &Engine,
    host_data: &Path,
    project_path: &Path,
    mut revision: EngineSourceRevision,
    scope: &mut Value,
    build_registry: impl FnOnce(&OpenedSource, &EngineSourceRevision, &Value) -> Result<String>,
) -> Result<String> {
    let input = OpenedSource::open_path(project_path)?;
    let retained = input.retain(host_data)?;
    retained.bind_scope(scope)?;
    revision.source_sha256 = input.source_sha256();
    let registry_json = build_registry(&input, &revision, scope)?;
    let result = engine
        .compile_document(revision, input.authored_json().to_owned(), registry_json)
        .await
        .map_err(|error| input.locate_error(error))?;
    Ok(result.result_json)
}
```

`locate_error` adds a component location when a failure has an assembled
JSON pointer. Compiler diagnostics arrive in `result_json` under
`/static/diagnostics` or `/runtime/diagnostics`. Pass each diagnostic's code,
pointer and message to `persistence::failure`, then pass that failure to
`locate_error`. The standalone host locates the first error and appends the
full compile report.

For a project folder or an opened archive, `selected_files()` returns a
borrowed iterator in ascending path order over the original selected files.
Each `SelectedFile` carries a portable project-relative `path`, exact `bytes`
and the `executable` flag. Plain JSON documents return `None`. Use
`component_origin(pointer)` to map an assembled JSON pointer to its
project-relative file and local JSON pointer. It uses the closest declaring
component, so callers can also locate a missing field. Inline fields map
to `agsdl.json`.

To save the opened project as an immutable version, call
`input.write_archive(&mut output)?` with an empty file or buffer. The writer
uses the selected bytes from the open operation. It emits deterministic
stored ZIP32 data without history, and accepts an opened archive for
re-export. Plain JSON sources and archive limit violations return `PROJECT`
before writing. The host owns the output. When saving to disk, write to a
private temporary file, flush it and publish the completed file at a new
`.agsdl` path. Discard the temporary file on failure.

The writer flushes the output before reporting success. A flush or write
error returns an error, and the partial output must be discarded.

Path checks use the Unicode tables of this crate's dependencies. The AgSDL
v0.5.0 reference tool uses the tables of its Python runtime, so on Python 3.9
it rejects file names containing characters assigned after Unicode 13. Keep
such names out of projects meant for that tool until AgSDL fixes a Unicode
baseline.

The optional interoperability test uses Python 3.9 or newer and the
AgSDL v0.5.0 reference tool. Set `AGSDL_REFERENCE_CHECKOUT` to the root
of that checkout and run the test with Cargo. An explicit path that is not
an AgSDL checkout fails the test instead of skipping it.

```sh
cargo test --locked --manifest-path crates/argentic-runtime/Cargo.toml \
  --lib archive_interchanges_with_reference_tool -- --nocapture
```

The test also looks in `~/github/AgSDL` when the variable is unset. It skips
when Python 3 or the reference tool is unavailable.

Pass the bound scope to native registry preparation and save it unchanged as
`callScopeJson` in the private Start capture. Build the registry with the same
scope and source digest used for admission. The engine's native reader uses
that capture to reopen project files. Custom content readers can bind the same
capture with `ContentContext::with_captured_source`. Keep the existing workspace,
permissions and approval checks. For packaged content, native Read policy is
evaluated on the member's path relative to the approved workspace, while the
bytes come from the retained project. That path is evaluated as spelled:
symbolic links in the workspace neither redirect nor refuse a member, and a
deny rule for the path still applies. Inside the retained copy, only inventory
members are readable. A file outside it, such as a workspace resource attached
to an entry or steer message, follows the same workspace rules as for a JSON
document. A relative URI in a message resolves against the document location,
which for a project is the retained copy, so it reaches only inventory
members. Attach workspace files to a project run with absolute `file:` URIs.

A folder and its archive select the same project files. Both apply the
archive name rules, including the refusal of `.git`, `.env` and `.env.*`
in skill trees. Opening a folder or its `agsdl.json` pins the root directory
before reading the manifest. Members are opened relative to that handle.
Unix opens the root with no-follow flags. Windows opens the root without
following reparse points and keeps the handle open without delete sharing.
A reference can pass through `..` lexically. Any component traversed on
the way to `..` needs a portable name without a case collision, even when
it does not exist.

Unix uses the source file's POSIX executable bits. On Windows, Argentic
restores flags from the first unselected regular root file whose name is
`.agsdl-executables` or `.agsdl-executables.<number>` in lowercase and whose
header is `AgSDL executable paths v1\n`. The UTF-8 body is limited to 2 MiB.
Each LF-separated line that exactly matches a selected project path sets
its executable flag. A selected file with the same sidecar name and header
is ordinary project content. ZIP exports record the flags in `archive.json`.

A folder can select at most 8192 files and 256 MiB, and its inventory must
fit in 2 MiB. An archive's 8192 entries and 256 MiB also count
`archive.json` and any `history.bundle`. ZIP local records and their
descriptors must cover every byte up to the central directory. The central
directory and final end record must account for the rest of the container.
`ARCHIVE_LAYOUT` identifies a layout refusal, including unaccounted bytes
before or between structures or after the end record.

Source opening and compilation each use a shared `argentic_core::SourceBudget`.
An open charges JSON parsing, project assembly, validation and diagnostic
production to the same budget. Compilation shares its budget across parsing,
static validation, graph expansion, adapter qualification and host checks.
Each operation permits 8,000,000 work units and 256 MiB of estimated cumulative
allocation. Work units count input scans, visits and comparisons. The byte
allowance includes temporary allocations and retained output. JSON parsing
allows 128 levels of nesting and property names up to 4,096 source bytes.
Folder and ZIP opens reserve selected file bytes before reading them. ZIP opens
also charge decompression buffers. The archive and component size limits still
apply, and temporary allocations share the 256 MiB budget with retained data.

The native compiler accepts up to 4,000,000 UTF-8 bytes each for the assembled
document and the registry. A project can open successfully and assemble a
document above that limit. `Engine::compile_document` then returns
`INPUT_LIMIT` at `/source` or `/registry`. The native compiler result has a
separate 8,000,000-byte transport limit.

Expansion allows 8,192 graph nodes, 32,768 route targets and 32 nested
composition bodies. Join analysis allows 131,072 branch and membership
records per pass. Join diagnostics have their own limit of 1,024.
Validation retains up to 1,024 diagnostics and
reserves space for a final limit diagnostic within 512 KiB of encoded
diagnostics. The limit includes JSON escapes. Join metadata has its own
expansion limits. Messages retain up to 2 KiB, with short excerpts of long
authored values. The compiler charges retained
graph JSON and the encoded plan together against the 32 MiB graph JSON cap.

`GRAPH_LIMIT` covers source work, bytes, nesting, expansion and diagnostic
limits. Its pointer identifies the relevant field, composition or `/flow`.
Project open maps that location to the declaring file and returns a structured
`SourceDiagnostic` with kind `invalidContent`. The native compiler preserves
`GRAPH_LIMIT` and its pointer when a calculation refuses an input.

Plain JSON read failures retain their host codes, `SOURCE_UNAVAILABLE`
and `INPUT_LIMIT`. For project diagnostics, `EngineFailure.source` can carry
a `SourceDiagnostic` with `path`, `pointer` and `kind`. For an unreadable
declared file the code is `PROJECT` and `source` gives the unreadable file's
path, an empty pointer and kind `io`. For a URI resource, the error's top-level
`pointer` and `message` still identify the URI declaration. Invalid project
JSON uses the same code with kind `invalidContent`.
The path is relative to the project root and the pointer is relative to that
file. An empty pointer refers to the entire file. A UI can show a read
failure separately from an invalid declaration by matching `source.kind`.
Use `code` to identify refusals that apply to the whole input, including
`ARCHIVE_LAYOUT`. Existing `code`, `pointer`, `message` and `work` fields
remain available. If there is no source location, the serialized failure
omits `source`.

`secrets()` returns the environment variable names declared for each MCP
server. `mcp_files()` returns its file paths relative to
`RetainedSource::resource_root()`. Opening and retaining the project works
offline with a closed vault.

For local stdio MCP call steps in a native embedding, configure Macro's
`HostContext` with the same private data root passed to `OpenedSource::retain`.
Call `retained.bind_scope(&mut scope)?` before preparing the registry and
persist that scope unchanged in `callScopeJson`. It binds the retained root,
the declared paths and the secret names. Use the existing local call
capability and current host and project policy checks. The native local call
backend uses Seatbelt on macOS ARM64 and requires the existing workspace
permissions. Codex agent MCP equipment and remote MCP still require separate
adapter qualification.

The process runs with the captured workspace (`scope.root`) as its working
directory. A relative program with a path separator must be listed in that
server's `mcpFiles`. A listed bare program name also selects the retained
file, which must have its executable flag set. An argument that matches a
declared path, such as `mcp/server.sh` in `["sh", "mcp/server.sh"]`, becomes
the absolute path to that retained file. Remaining arguments pass through
as written. Bare system programs use `/usr/bin:/bin`. For a plain JSON
source a relative program path still resolves against the workspace.

Set the provider on the context passed to `StartSession::attach_committed_start`.

```rust
use argentic_runtime::host::{HostContext, McpSecretProvider};
use std::sync::Arc;

fn configure_mcp(context: HostContext, vault: Arc<McpSecretProvider>) -> HostContext {
    context.with_mcp_secrets(vault)
}
```

`McpSecretProvider` is a `Send + Sync` callback with arguments
`(&EngineSourceRevision, &str, &[String])` and return type
`Option<BTreeMap<String, String>>`. Macro looks up the values by source,
server ID and requested names. Return exactly those names, or `None` to
refuse spawn. The synchronous callback runs under the native launch gate.
Keep it short and use a host-owned vault handle. It runs afresh on each
physical spawn for a server that declares secrets. The subprocess gets a
cleared environment with the fixed runtime PATH and that server's values.
Argentic keeps values in memory and in the child environment. It discards
stderr and rejects literal secret echoes in tool results and MCP requests
for human input before retaining them. Macro must approve the server code
and ensure it keeps credentials out of files, derived output and child
command lines. Restart fences interrupted work and does not respawn an
earlier MCP call.

Argentic checks the declared assets against the retained index before launch.
The confined MCP process has read access to its declared assets and a write
ceiling that excludes the retained project. Keep the entire private data
directory outside all agent write grants. Macro must protect it from changes
by other processes and from replacement between validation and launch,
including when Macro provides its own sandbox or workspace mounts.

When present, the ZIP's Git bundle is verified by size and SHA-256. The
current snapshot opens without Git. Importing and browsing historical
revisions are outside the host profile.

Retain packages under the same private data root used by `HostContext`.
Argentic publishes a content-addressed copy of the selected files and records
its identity in the Start capture. Keep that data at the same path as long as
a run may need it. Dropping the open handles or removing the original folder
or archive leaves the retained bytes available. If a retained file or its
inventory is missing or altered, the runtime refuses the read with
`CONTENT_UNAVAILABLE`. It never substitutes current project bytes. A damaged
copy is not repaired automatically, and opening the same project again is
refused with the same code. With no host running, remove
`agsdl-snapshots/<id>` under the private data root. The next open then
retains the project again under the same identity. Argentic never removes
retained copies: each revision keeps its own, and a crash during retention
can leave a `.tmp*` staging folder. Remove them only when no run can still
need them. Existing
standalone restart rules still apply. A new process shows prior interrupted
runs and reconciles their pending Stops.

## Per-run model selection

For each Start, Macro can provide a map from agent ID to a replacement
model. Prepare the observed registry for the authored document as usual.
Apply the replacement choices before calculating the registry version and
calling `Engine::compile_document`.

```rust
use argentic_runtime::{
    capture::{apply_model_overrides, ModelOverride},
    persistence::Result,
};
use serde_json::Value;
use std::collections::BTreeMap;

fn apply_launch_models(
    registry: &mut Value,
    models: &BTreeMap<String, ModelOverride>,
) -> Result<()> {
    apply_model_overrides(registry, models)
}
```

`ModelOverride { name: String, effort: Option<String> }` replaces the
complete authored model choice for that agent. Omit `effort` to use the
replacement model's observed default. The compiler checks the harness
module, installed adapter capabilities and observed catalogue. The
document's harness, version constraint and permissions govern the run
as authored, with the harness module selected by the validator.

Pass the compiler's plan and `capturedRegistryJson` into the usual native
Start capture. Keep the same authored bytes, `source_sha256`, project
snapshot and bound scope. Use the same choices when rechecking inputs
before commit. A Start request ID always returns its original admitted
capture on retry. Use a new request ID to submit different choices.

Inspection returns the selected replacements and effective effort in
`RuntimeProjectionPage030::model_overrides` (`modelOverrides` in JSON).
The run stores the requested choices in the registry and plan, alongside
the plan's resolved model and effort. Replay checks their agreement with
the retained catalogue without contacting the provider.

## Build and open

Keep `argentic-core` beside `argentic-runtime`. Each owns a manifest and lockfile.
With the locked dependencies available locally:

```sh
cargo build --offline --locked --manifest-path crates/argentic-runtime/Cargo.toml --example argentic-host
```

The executable is in `crates/argentic-runtime/target/debug/examples/argentic-host`.
If `CARGO_TARGET_DIR` is set, use its `debug/examples/argentic-host` instead. On macOS the runtime build requires the Swift toolchain. The example calls
`content_io::file_helper::initialize` before Tokio. Downstream host packages must
also preserve `-Wl,-weak_framework,LightweightCodeRequirements` for deployment
before macOS 14.4.

Choose a disposable workspace containing an existing regular `sentinel.txt` with
`before` and a newline. Keep the private data directory outside that workspace,
with neither directory containing the other. Review `crates/argentic-runtime/examples/fixtures/policy.json`:
`owner` and canonical decimal `sourceRevision` identify this source; `permissions`
is the approved host ceiling; `readOnly` is the project restriction. Increment the
revision for a changed source. Review the source separately. Approve the exact
policy bytes by supplying their SHA-256, not by accepting an implicit default.

```sh
shasum -a 256 crates/argentic-runtime/examples/fixtures/policy.json
/path/to/argentic-host --data /absolute/private-host-data \
  --workspace /absolute/disposable-workspace \
  --source /absolute/write-review.json --policy /absolute/policy.json \
  --approve-policy THE_REVIEWED_SHA256 --stop-on-exit --codex
```

For a packaged source, use the same command with
`--source /absolute/review.agsdl` or `--source /absolute/review-project`.
You can also pass `--source /absolute/review-project/agsdl.json`. The host
verifies and retains the project before it reports `ready`. Reopen the host
and increment the source revision to run an edited project. Local project
files and archive assets remain subject to the existing execution profile.

Omit `--codex` and use `human-only.json` for a provider-free human-flow exercise
on Linux, Windows or macOS. `--stop-on-exit` explicitly consents to Stop/drain of
this process's admitted runs on EOF, Ctrl-C or output disconnection. Keep the
process alive for all commands and human answers. A separate invocation never
adopts a live run.

The official private Codex profile is rooted at the explicit `--data` directory:
`native-engines/codex-v1`, with the isolated context in `native-contexts/codex-v1`.
Use the official login procedure in [Native Codex](https://github.com/Andrologic/Macro/blob/develop/docs/agsdl-codex-native.md#private-login),
substituting this data root. The embedding API never imports credentials from
Macro or a default Codex home. `CODEX_LOGIN_REQUIRED` is an unmet prerequisite,
not a passing recipe. A model catalogue comes only from the native observation.
Install the complete official same-release package. In 0.167.x it contains
the executable `codex-code-mode-host` and `codex-exec-server` siblings.
The Linux ARM64 package for 0.171.0 bundles `exec-server` in the main
executable and supplies `codex-code-mode-host` as a sibling. Argentic checks
the expected helpers and binds their digests to the capture. An incomplete
package reports `CODEX_PACKAGE_INCOMPLETE` before a model thread.

For Codex 0.171.0 on Linux ARM64, provision the private login outside
`/tmp` and use the read-only host profile. The app-server gets a private
temporary runtime directory for its Unix sockets. The profile keeps
temporary roots and network denied for model tools.

Alternatively, explicitly supply `--codex-auth-file /absolute/ordinary-home/auth.json`
to the operator, or call `Host::with_codex_auth_file` before preflight/Start.
Embedding hosts use `HostContext::with_codex_auth_file`. Construction still
starts no provider and reads no credentials. This experimental opt-in uses the
internal app-server external-auth API, limited to the exact official 0.167.0
execution binary. It reads only a private, owned, regular ChatGPT file session,
never its configuration or refresh mechanism. The default private login remains
unchanged. Keep the file outside the workspace and execution profile, and use a
private execution profile without stored auth. See [authentication limits and
diagnostics](https://github.com/Andrologic/Macro/blob/develop/docs/agsdl-codex-native.md#explicit-existing-file-authentication).
Do not expose credentials in operator input/output, logs or evidence. A status
login or synthetic peer does not qualify this opt-in for product use.

## Commands

One JSON object per stdin line; stdout contains one JSON object per response.
The first record contains `ready`, the exact `source` revision/digest and retained
`runs`. Responses contain `result` or a typed `error`; shutdown errors also report
`shutdownStarted`. Invalid commands leave the process available for another one.

```json
{"op":"preflight","entry":{"deliveryId":"entry-1","messageJson":"{\"prompt\":\"Perform the bounded write-review.\"}"}}
{"op":"start","requestId":"start-1","entry":{"deliveryId":"entry-1","messageJson":"{\"prompt\":\"Perform the bounded write-review.\"}"}}
{"op":"runs"}
```

Retain the returned `receipt.identity.systemStartId` and the returned `source`.
Replace `SYSTEM` and `SOURCE_OBJECT` below with those actual values. Never invent
snapshot identities, human request ids, revisions or decisions.

```text
{"op":"inspect","query":{"systemStartId":"SYSTEM","source":SOURCE_OBJECT,"snapshotId":null}}
{"op":"humans","scope":{"systemStartId":"SYSTEM","source":SOURCE_OBJECT}}
{"op":"answer","input":{"scope":{"systemStartId":"SYSTEM","source":SOURCE_OBJECT},"requestId":"ACTUAL_REQUEST","expectedRevision":"ACTUAL_REVISION","answer":{"kind":"decision","decision":"yes","comment":"Reviewed the exact command and file grant."}}}
{"op":"stop","systemStartId":"SYSTEM","requestId":"stop-1"}
{"op":"quit","approved":["SYSTEM"]}
```

Read every human request before answering. Native action approvals use `decision`
with `yes` or `no`; ordinary flow choices use `{"kind":"approval","choice":"yes","comment":"..."}`.
Question answers use `{"kind":"question","answerJson":"..."}`. These are person
submissions, not callbacks authorizing a physical response. The engine correlates
and consumes them under its existing guards. A final result is validated by the
work controller; the operator has no command to forge an agent result.

`inspect` and `humans` accept the returned `nextPage` as `continuation`; revisions
and scopes must still match. `deliver` accepts the existing `DeliveryRequest030`
contract from `api`. Start ids are idempotency keys: after an uncertain reply,
inspect `runs` and use the same request, never submit a new id to guess success.
`shutdown` has the same `approved` list as `quit` but keeps inspection available.
Stop/shutdown waiter deadlines do not prove a physical end. Retry the same
retirement. Once shutdown starts, admissions stay closed; the operator never
resumes work or releases storage before successful drainage. EOF drainage retries
errors while retaining owners. Fix the reported storage/resource issue instead
of interpreting a timeout or forced process exit as success.

## AgSDL 0.4.0 delivery and cancellation

An accepted steer remains pending until the adapter proves receipt by the
target work. A confirmed message is reported as delivered. Definite
non-receipt after the target becomes unavailable returns the message to the
queue in arrival order. Lost or expired evidence produces `STEER_UNCERTAIN`
on the sender path. Inspect the destination activation's message for
`targetActivationId`, delivery statuses and `steerFailure` when present.
For new work, `delivered` requires the durable executor handoff. For a
steer to existing work, the Codex RPC reply leaves receipt pending until
the matching input notification arrives or the evidence deadline expires.
A hold can commit while an admitted steer is awaiting confirmation. The
receipt retains the checkpoint where the provider send was admitted and
the later checkpoint where the host recorded the outcome.

A 0.4.0 `stop` response with kind `requested` acknowledges a durable whole-run
stop. Poll `inspect` for `stopping: true` while the executors are still
terminating. The terminal verdict is `cancelled` after physical confirmation.
The stop withdraws queued work and closes human requests when it commits.
Late evidence of receipt before the stop is retained for reporting.
Codex notifications identify a turn and message. An observed turn end while
the recipient still counts can establish the timing of a later confirmation.
When the first usable evidence comes after a committed exclusion, the
identifiers alone leave receipt timing uncertain.
After the turn ends, the runtime gives the app-server up to two seconds to
report a pending receipt, within the steer deadline. This observation uses
none of an accepted result's machine-time budget. The runtime still joins
the process and its descendants before routing the recipient's result.

## Operator controls for AgSDL 0.4.0

The Rust embedding API is `Engine::control_run(&self, pool: &SqlitePool,
request: OperatorRequest) -> Result<FlowReceipt030>`. The standalone
`Host::control_run(&self, request: OperatorRequest)` returns the same
receipt. Supply the exact scope from `inspect` and a request ID. Each
distinct action gets a new ID. Retrying the same action and ID returns
its original receipt. Before dispatch resumes after a lost commit
acknowledgement, the supervisor checks the exact durable control or delivery
event. Changing the action under that ID returns `CONFLICT`. An
`OPERATOR_TRANSITION` response means the action cannot
run in the current state.

The JSON-lines command is `controlRun`. Its input is an `OperatorRequest`
with `scope`, `requestId` and `action`. The action takes one of these forms.

```json
{"kind":"interruptWork","activationId":"ACTIVATION"}
{"kind":"resumeWork","activationId":"ACTIVATION"}
{"kind":"pauseRun"}
{"kind":"resumeRun"}
```

- **Interrupt.** Use the current activation ID from the instance projection.
  The receipt records the request. Display `operatorWorks[activationId].kind`
  as `holding` until Codex confirms the end of the turn, then `held`. The
  activation stays open and retains its machine-time budget, conversation
  and remaining corrections. An automatic plan reported before Codex
  confirms the turn ended carries over to the continued work. An action
  approval for the interrupted turn closes. The journal retains a recorded
  answer or a timeout reached at the hold and records when the turn closed
  before delivery.
  Any effects already committed remain in place.
- **Resume an agent.** Once held, use `resumeWork` to continue on the same
  Codex thread. A person can also use the existing `deliver` operation on a
  held shared or named instance. A `steer` attempts receipt by the continued
  work through the ordinary steer evidence path. A `queue` releases the hold
  and appends the message in arrival order for its own work. In a flow,
  external messages require an exact held instance. For an instance created
  with `instance: "new"`, use `resumeWork`.
- **Pause a run.** `paused: true` means the dispatch gate is closed.
  Admitted agent work goes through the same `holding` and `held` states.
  Starts, new deliveries, and routes wait. Flow approvals still accept
  answers and expire on their original deadlines. To warn about expiry,
  read pending `flowChoice` requests in `humans` and use the available
  clock's `openedAtMs` and `timeoutMs`. An approval outcome can appear
  while its route is pending. An admitted host call cannot be suspended by
  this API, so Pause returns `HOST_CAPABILITY` while one is active.
- **Resume a run.** Call `resumeRun` once its turn interruptions have been
  confirmed. This releases the held agents and the deferred dispatch work.
  A steer known to have fallen back joins the waiting queue in its original
  arrival order. Deferred failures retain the evidence used to evaluate
  their routes and joins. Individual resume commands are refused while the
  run is paused.
- **Stop.** Use the existing whole-run Stop for a paused run or a run with a
  held agent. It discards pending dispatch, records any unevaluated clock
  ticket or pending machine deadline it supersedes, and continues native
  termination until physical confirmation. A superseded deadline stays in
  history and produces no timeout result. Results committed before Stop keep
  their clock retirement in the same transaction.

Operator transitions and clock changes are journaled. A new host cannot
reattach to the held Codex work. On reopening an unfinished run, `inspect`
reports `verdict: "interrupted"`. Each retained hold has `kind: "interrupted"`
and `code: "INTERRUPTED"`. `interruptedWhilePaused` records a pause at
the restart boundary. The work-level code is a host status. The whole-run
AgSDL recovery outcome follows no normal or error route and creates no
step failure. If a clock ticket was outstanding, fencing retains its exact
identity and digest for replay without applying its output. A previously
committed Stop keeps its existing recovery rules.

## Storage, API and limits

`standalone::Host::open` owns an exclusive OS file lock for its lifetime. It
opens a new engine-only database or verifies the complete existing schema and
`post-042` revision before interruption/fencing. Extra, missing or modified DDL,
unknown revisions and Macro databases are refused. The verified schema may
include the clock evidence table created when fencing an interrupted run.
`sqlite::memory()` is an independent shared-memory pool; file and
memory use the same engine SQL. Raw-pool embedders own their process exclusion.
After a crash, an ordinary unfinished 0.4.0 run reports
`verdict: "interrupted"` in `inspect`. The `runs` list also reports
`interrupted: true`. The host fences the old owner and does not retry its work
or adopt its session. `terminationUncertain` indicates that activity may have
survived the interruption.

An explicit 0.4.0 whole-run stop survives reopening the file. Recovery can
settle a Start that never crossed the executor boundary and a local approval
whose in-memory wait ended with the previous host. If other previously admitted
work still lacks physical termination evidence, inspection continues to show
`stopping: true` with `terminationUncertain: true`. The standalone host has
no transport for reconnecting to a lost external executor. Run history and
projections remain available for 0.3.0 databases.

The small embedding API exposes `Host`, `Policy`, preflight, Start, projections,
human submissions, message delivery, Stop, shutdown and close. It keeps the Start
adapter and native capabilities private. Source, policy bytes and workspace
identity are rechecked with retained input guards. Changes are refused until a
new process explicitly approves its inputs. `close` succeeds only after a
successful native shutdown; dropping the host is not a Stop receipt.

Authored JSON bytes and numeric lexemes use the existing exact codec. Keep JSON
payload fields encoded as strings where specified; do not round them through
JavaScript numbers. Start uses the existing immediate transaction, generation
checks, correlation, idempotency barrier and post-commit readback.

Codex supports 0.167.x on macOS ARM64 and Linux ARM64. The exact 0.171.0
release also supports the read-only Linux ARM64 profile with the private
login. Documents combining Codex and approval steps require macOS ARM64. Local host
calls, other harnesses, network content fetches and remote MCP are unavailable
in this host.
Local relative content URIs resolve from the source file's directory. The
content reader supports Linux, Windows and macOS ARM64 14.4 or later. Files
must be readable under the approved workspace and agent policies. On Windows,
the reader is available to runtime embedders.
Model reviewers, broader write grants and unsupported capabilities remain
refused. An accepted version string, a synthetic scenario or a successful build
does not qualify a new provider version or platform. Use the bounded
[requalification procedure](https://github.com/Andrologic/Macro/blob/develop/docs/agsdl-codex-native.md#bounded-requalification).
