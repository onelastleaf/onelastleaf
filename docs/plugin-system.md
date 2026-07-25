# Plugin system

## Boundary and trust

Plugins are trusted independent processes. They may read or write any document,
inspect the complete directory tree, use oll's abstract CRDT API, access their
configuration, and perform external side effects.

There is no permission sandbox, signature requirement, official marketplace, or
host intervention in publication. A plugin repository is installed directly
from a Git URL.

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
Markdown linting is not a planned host responsibility.

## Packaging

The oll package manager is responsible for repository layout and installation
configuration. It must eventually support GitHub, GitLab, Codeberg, Gitea, and
Forgejo-compatible remotes.

The default installation path builds from source using a configured build
command. A plugin may alternatively provide a release binary. Build commands and
release-selection fields are package-manager configuration and cannot be
overridden by code running inside the plugin.

Plugins may use any implementation language that can implement the protobuf
gRPC service. oll does not load a plugin ABI, inject an SDK, or require a Rust
dynamic library.

## Process lifecycle

oll starts every enabled plugin; there is no automatic discovery of externally
started plugin processes. The plugin hosts `PluginRuntime.Connect`, and oll is
the transport client.

At spawn time oll passes the configured endpoint and a parent-liveness file
descriptor. oll keeps the descriptor open. If oll crashes, the kernel closes it
and the plugin reads EOF and exits.

The protobuf session handshake is:

1. oll sends `HostHello` with node/session/instance IDs, exact schema hash,
   depth limits, and artifact chunk limit;
2. the plugin validates it and sends `PluginHello` with identity, actions, and
   event subscriptions;
3. both sides send `SessionReady`;
4. jobs and host calls are legal only after both ready messages were observed.

Session and instance IDs prevent accidental cross-wiring; plugins are trusted,
so these IDs are not authentication.

## Multiplexed stream

All runtime calls share one bidi stream. `PluginEnvelope.message_id` is non-zero
and unique per sender in the session; direct responses set `reply_to`.

The stream reader must keep dispatching while calls are outstanding. Waiting for
a response in the reader task would prevent nested plugin -> host -> Lua ->
plugin calls.

Every envelope carries correlation, parent-call, call-depth, causal-depth,
task, and task-group context. The initial maximum call and causal depths are 10.
Messages above the limit are rejected and not executed. Known event cycles may
be rejected earlier.

## Jobs

An oll invocation is asynchronous:

```text
StartJobRequest -> JobAccepted -> JobUpdate... -> terminal JobUpdate
```

Acceptance does not mean completion. Without an explicit deadline the host uses
24 hours. Small structured results use `ConfigValue`; PDF, `.apkg`, and other
large results use the chunked artifact protocol with declared size and SHA-256.

An executing job is not cooperatively cancelled over RPC. Timeout or `killjob`
terminates the plugin process: graceful shutdown request, `SIGTERM`, a grace
period, then `SIGKILL`. Cancellation does not roll back CRDT writes or external
side effects.

## Host document API

Plugins can request complete document content, list directories, read the whole
tree, read oll CRDT values, and submit mutations.

A long-running plugin should submit the `Revision` read with its source
document. If another node or client changed that document before commit, oll
returns `REVISION_CONFLICT` without applying any requested mutation. There is no
lock wait and no deadlock path.

## Lua configuration

Lua runs inside oll, not in every plugin language. Values crossing the stream
use `ConfigValue`.

A closure is stored in oll's Lua registry and represented remotely by a
session-scoped `ConfigFunctionRef`. The plugin invokes the reference through a
host call; oll resolves the registry entry, converts arguments, executes it on
the Lua-owning thread, and converts results back to protobuf values. Closure
bytecode and upvalues are never serialized.

Config adapters reject cyclic tables, unsupported userdata, threads, and values
outside the schema. Function handles expire with their configuration runtime.
Hot reload requires a runtime generation in the handle, and dynamically created
handles require a release/lease mechanism before those features are added.

Synchronous same-thread Lua reentry requires a dedicated implementation proof.
No Rust mutable borrow, Lua stack reference, or assumed host state may be held
across an outward plugin call. After nested execution returns, host state must be
read and validated again.

## Scheduler and observability

An optional Tokio-owned queue implements `schedule_task`. Child tasks inherit
the current logical task group unless oll assigns another. The first version
does not promise fairness, quotas, queue bounds, or CFS-like behavior.

Plugins emit structured `LogRecord` messages. oll aggregates them using the
envelope's correlation, call, causal, task, and group identifiers. Additional
metrics or tracing systems are not required for the first version.

## External side effects

oll cannot atomically combine a CRDT commit with AnkiWeb, a blog deployment, an
AI provider, or another external service. Plugins own retry, idempotency, and
partial-failure behavior for those systems. oll does not pretend to compensate
operations it cannot understand or reverse.
