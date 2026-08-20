# Plugin system

## Boundary and trust

Plugins are trusted independent processes. They may read or write documents,
inspect the complete directory tree, use oll's abstract CRDT API, evaluate their
own user configuration, emit artifacts, and perform external side effects.

There is no permission sandbox, publisher signature requirement, official
marketplace, or language-specific ABI. A plugin may be implemented in any
language with a gRPC client capable of using the protobuf contract. Trust does
not waive validation at the process, protobuf, revision, path, or artifact
boundary.

Official language runtimes, their common state-machine obligations, package
identities, and `oll plugin new` are defined in
[plugin-sdk.md](plugin-sdk.md). An SDK hides envelope routing but does not
weaken any validation or lifecycle rule in this document.

The first implementation is local-only. The local CLI may invoke a plugin
running under the same daemon. Remote plugin invocation and a separate input-file
upload protocol are deferred; no unapproved remote authorization or routing
contract is implied by the local API.

## Target workflows

The API is designed to support at least these concrete workloads:

- turn one document into Anki cards, upload them to a selected user/deck, and
  return an `.apkg` artifact when upload is unavailable;
- generate a document at a selected path from keywords, notes, prompts, and an
  AI-provider configuration;
- suggest a directory for a document or short text using the current replica
  tree as context;
- publish selected documents through a private blog workflow;
- export rendered PDF artifacts;
- format Markdown content.

Whole-replica search is intentionally local when every node has a complete
replica. It does not need a server plugin merely to run a grep-like operation.

## Package and state ownership

Publisher manifests, source recipes, release indexes, installation declarations,
typed masks, update rules, and reconciliation are defined in
[plugin-packaging.md](plugin-packaging.md). Package generations, SQL authority,
jobs, artifact publication, removal, and crash recovery are defined in
[plugin-storage.md](plugin-storage.md).

The package manager reads publisher-owned `oll.toml` and `oll-release.json`, the
user/CLI-owned `<config-root>/plugins.lua`, and an optional typed
`<config-root>/plugin-masks/<plugin-id>.toml`. Per-plugin runtime configuration
is a separate live Lua file at `<config-root>/plugins/<plugin-id>.lua`.
`plugins.lua` contains no process desired state.

One immutable `PluginId` owns every package, process, job, and filesystem path.
The effective `PluginName` is a unique mutable selector and display value; a
typed mask may change it without changing identity or moving storage to a
name-derived path.

## Process supervisor

The plugin supervisor is an internal node-runtime component. It owns one
event-driven controller and at most one direct child process for each installed
PluginId. It never discovers or adopts externally started processes.

Each installed plugin has two independent state axes:

- SQL-authoritative desired state is persistently `running` or `stopped`;
- observed process state is transiently `starting`, `ready`, `stopping`,
  `exited`, or `failed` and is reconstructed after daemon start.

A newly installed plugin starts desired-stopped. `plugin start` atomically sets
desired-running; `plugin stop` atomically sets desired-stopped and reconciles the
process; `plugin restart` sets or retains desired-running and records one
edge-triggered recycle sequence. Start and stop are short and idempotent.
Restart never creates overlapping instances. Their Admin responses acknowledge
the durable state transition, not eventual readiness or exit.

Only these lifecycle commands change desired state. Package update, a plugin
crash, startup/session/heartbeat failure, a failed job, job cancellation, and a
job timeout leave it unchanged. A desired-running plugin that exits unexpectedly
restarts with bounded backoff; a desired-stopped plugin remains exited. Clean
daemon shutdown stops children without changing desired state.

`last_lifecycle_failure` remains visible while a replacement instance is
starting; beginning a retry is not evidence that the failure was repaired. It
is cleared only when that same active replacement instance completes its
handshake and becomes ready. A stale ready notice from an earlier instance can
neither clear the failure nor change the observed state.

The controller owns the direct child handle and waits asynchronously for its
exit. It must not poll the process table. The configured runtime command remains
the foreground leader of its own Unix process group and must not daemonize,
detach, or delegate the session to an untracked process.

## Spawn and parent liveness

For each process instance, oll first binds a private loopback TCP listener on
port `0`, retains the bound listener, and then spawns the selected package
generation. The plugin is the gRPC client and oll hosts
`PluginRuntime.Connect`. The endpoint is supplied only as:

```text
OLL_PLUGIN_ENDPOINT=http://127.0.0.1:<kernel-selected-port>
```

The exact loopback address may be the supported platform's IPv6 equivalent.
The endpoint is neither a public listener nor a Unix socket. The first protocol
does not add a bearer token: the listener is instance-owned, loopback-only, and
the local plugin trust model is explicit.

The child's stdin is reserved as a parent-liveness pipe. oll keeps the write end
open for the instance and never sends application data through it. The plugin
contract requires continuously observing stdin and exiting promptly on EOF, so
a kernel close after host death gives the plugin a liveness signal. oll cannot
prove that third-party code obeys this contract after oll itself has crashed;
it trusts the plugin author. During an orderly shutdown oll still uses the
protobuf shutdown request and signal enforcement described below.

Plugin stdout and stderr are piped into `plugin.log`; they are not the liveness
channel and are not returned as job results. Source-recipe processes are a
separate package-install boundary and receive closed stdin as defined in
`plugin-packaging.md`.

## Runtime session

After the plugin connects, the encrypted-localhost assumption is not invented:
the first local transport is ordinary plaintext HTTP/2 gRPC over loopback. The
instance-owned listener determines the expected PluginId and instance ID.
Session and instance identifiers prevent accidental cross-wiring; they are not
authentication credentials.

The application handshake is:

1. oll sends a `HostHello` envelope. The envelope's nonempty session and
   instance IDs establish the authoritative identity pair for the stream;
   `HostHello` carries `NodeIdentity`, the expected PluginId and effective
   PluginName, depth limits, and artifact chunk limit;
2. the plugin validates it and sends `PluginHello` repeating the expected
   identity and declaring actions;
3. both endpoints send `SessionReady`;
4. jobs and host calls are legal only after both ready messages were observed.

The process becomes observed-ready only after step 3. Identity or handshake
mismatch, startup deadline expiry, unexpected stream closure, or a missed
heartbeat changes the instance to failed and begins process teardown. The
supervisor then reconciles against unchanged desired state.

The instance-owned listener accepts exactly the expected process instance. Once
that instance's session ends, its session ID and instance ID are stale: later
envelopes, job updates, cancellation acknowledgements, or artifact messages
from it are rejected and cannot attach to a replacement instance. Rejecting
stale output closes or fails only the stale session or work item and must not
wait on, or block admission and shutdown of, the current instance.

The first `HostHello` envelope is the only bootstrap exception to ordinary
identity comparison because the plugin has not yet learned the pair. The plugin
requires both outer identifiers to be nonempty, adopts them, and copies that
exact pair onto `PluginHello` and every later envelope. `HostHello` does not
duplicate them as payload fields. After bootstrap, either endpoint rejects an
envelope whose outer identity differs from the established pair.

All calls share the one bidirectional stream. `PluginEnvelope.message_id` is
nonzero and strictly increasing per sender within the session. Gaps are allowed
and the first value need not be `1`; a receiver needs only the last accepted ID
to reject a duplicate or older envelope in O(1) state. A direct response sets
`reply_to`. The stream reader keeps dispatching while requests are pending.
Configuration calls are ordinary plugin-originated host requests: oll resolves
or executes the requested value in its Lua owner and sends one host response.
Lua does not originate a `PluginEnvelope` or make an outward RPC to the plugin.

The encoded protobuf message for one `PluginEnvelope` is limited to 64 MiB in
both directions before application dispatch. This transport bound applies to
ordinary host-call requests and responses as well as plugin messages; it is not
waived by the trusted-plugin model. Artifact bytes remain subject to their
smaller advertised chunk limit and must not be placed in one oversized
envelope.

After readiness oll may send a `Heartbeat`. The plugin replies with the same
nonce and sets `reply_to`. Heartbeat detects a live but protocol-unresponsive
process; normal exit is detected from the owned child handle.

Every envelope carries correlation, parent-call, call-depth, causal-depth, task,
and task-group context. Initial maximum call and causal depths are 10. Messages
over a limit are rejected before execution. There is no scheduler in the first
implementation.

## Process shutdown

Process-scoped stop, restart, removal, daemon shutdown, and session-fatal
stream/framing/identity/heartbeat failure begin with one `ShutdownRequest`.
When the child remains alive past the grace period, oll sends `SIGTERM` to the
process group and waits within the node's single absolute shutdown deadline
where applicable, then sends `SIGKILL` and reaps it. Signals enforce the
graceful request; there is no separate public force-termination operation for a
process or job.

Process shutdown is distinct from job cancellation. A job stop or timeout must
not send `ShutdownRequest`, change desired state, signal the process, or disturb
another job merely to enforce cancellation.

## Jobs

A local invocation is asynchronous:

```text
StartJobRequest -> JobAccepted -> JobUpdate... -> terminal JobUpdate
```

`StartPluginJob` durably admits the normalized operation before sending
`StartJobRequest`, and its Admin call waits only for `JobAccepted` or a terminal
admission failure. The same nonempty operation ID and same normalized domain
payload return the same JobId; the same operation ID with another payload is
`ALREADY_EXISTS`. Protobuf encoding bytes are never the equality definition.

One plugin process may own multiple concurrent jobs. An action carries its name
and ordered shell-style UTF-8 argv strings. Empty strings, duplicates, and
leading `-` values are preserved without type inference. Without an explicit
deadline the host uses 24 hours.

`job stop` and deadline expiry send a job-scoped `CancelJobRequest`. The plugin
ceases only that job and returns `CancelJobAcknowledged`; the host then records
`cancelled` or `timed_out` according to the trigger. A rejection, protocol
violation, or missing acknowledgement fails that job without killing the plugin.
Cancellation cannot roll back completed replica writes or external effects.

If the plugin process, session, or daemon ends while a job is nonterminal, the
host records that job as failed. Daemon startup marks every job left nonterminal
by the preceding process failed before spawning plugins. A retry with the same
operation ID returns that original terminal job; a deliberate new execution
uses a new operation ID.

Small structured job results use `ConfigValue`. PDF, `.apkg`, and other file
results use the verified chunked artifact protocol and become downloadable only
after host publication. Runtime stdout/stderr are logs, not a result channel.

## Host document API

Plugins can request complete document content, list directories, read the whole
tree, read abstract oll CRDT values, and submit mutations. Loro container IDs,
frontiers, version vectors, and update bytes never enter this API.

A long-running plugin submits the revision pair relevant to the state it relied
on: `DocumentId` plus `DocumentRevision` for body or abstract-CRDT writes, and
`CatalogNodeId` plus `CatalogRevision` for a move, rename, delete, or metadata
change. A conservative operation can include both. If guarded state changed,
oll returns `REVISION_CONFLICT` without applying the mutation.

## Live Lua configuration

Lua runs inside oll rather than inside every plugin language. A plugin can read
only its own `<config-root>/plugins/<plugin-id>.lua` through the host config API.
The file is reopened and evaluated from current disk contents for each top-level
configuration request; startup does not freeze all per-plugin values in memory.
The file may use the controlled config-root `require` mechanism, including to
compose `config.lua` or `plugins.lua`, but the plugin has no API that names and
reads arbitrary other configuration files.

All configuration evaluations use the daemon's one LuaJIT state and registry.
A returned closure is stored in that registry and represented by
`ConfigFunctionRef { session_id, function_id }`. No Lua bytecode or upvalue is
serialized, and no runtime-generation field is needed. The host resolves a
handle only for the exact active plugin session and removes that session's
registry entries when it ends. A new evaluation may create new function IDs;
existing session handles continue to identify their original closures.

Adapters reject cyclic tables, unsupported userdata, threads, and values outside
`ConfigValue`. For `InvokeConfigFunction`, oll resolves the exact active
`session_id + function_id`, converts the arguments, executes that host-owned
closure, converts its return values, and completes the original host response.
The closure has no implicit client for calling back into the plugin, so this
path contains no synchronous Lua-to-plugin reentry.

## Artifacts and logs

For an artifact, the plugin declares one ID, safe filename, media type, size,
SHA-256, and chunk count; waits for host acceptance; sends contiguous bounded
chunks; and completes the transfer. The host verifies all declared properties,
publishes with no-replace semantics under the startup-resolved artifact download
directory, stores metadata, and only then returns `ArtifactStored`. A terminal
job may reference only stored artifacts. The full recovery and collision rules
are in `plugin-storage.md`.

Plugins emit structured `LogRecord` messages. oll combines them with the
envelope correlation context. Runtime stdout/stderr are wrapped into the same
structured sink, while package build output stays in its per-install build log.
Routing, redaction, and `oll plugin log` are defined in
[observability.md](observability.md).

## External side effects

oll cannot atomically combine a replica commit with AnkiWeb, a blog deployment,
an AI provider, or another external service. Plugins own retry, idempotency, and
partial-failure behavior for those systems. oll does not claim to compensate an
operation it cannot understand or reverse.

## Deferred work

The first plugin stage does not implement a scheduler, scheduled callbacks,
document/catalog event subscriptions, event-triggered jobs, fairness, quotas, a
remote plugin-call transport, or an independent file-upload protocol. These are
deferred capabilities, not unsupported variants or placeholders hidden in the
initial protobuf contract.
