# Plugin system

## Boundary and trust

Plugins are trusted independent processes. They may read or write any document,
inspect the complete directory tree, use oll's abstract CRDT API, access their
configuration, and perform external side effects.

There is no permission sandbox, signature requirement, official marketplace, or
host intervention in publication. A plugin repository is installed directly
from a `GitRemote` accepted by the package contract.

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

The package manager reads publisher-owned `oll.toml` and `oll.json` files and
the user-owned `<config-root>/plugins.lua` declaration module. Its complete
source recipe, direct-release-download, validation, mutation, and diagnostic
contract is defined in [plugin-packaging.md](plugin-packaging.md). Hosting
platform APIs are not part of that contract.

Plugins may use any implementation language that can implement the protobuf
gRPC service. oll does not load a plugin ABI, inject an SDK, or require a Rust
dynamic library. The configuration runtime is LuaJIT through `mlua`; other Lua
implementations are not supported by the first version.

## Process lifecycle

The plugin supervisor is an internal node-runtime component, not an external
service. It owns one event-driven controller and at most one direct child
process for each installed plugin. There is no automatic discovery or adoption
of externally started plugin processes. The plugin hosts
`PluginRuntime.Connect`, and oll is the transport client.

Each installed plugin has two independent state axes:

- desired state is persistently `running` or `stopped`; the existing term
  `enabled` means desired `running`;
- observed process state is transiently `starting`, `ready`, `stopping`,
  `exited`, or `failed` and is reconstructed after every daemon start.

A newly installed plugin starts with desired `stopped`. `plugin start` first
persists desired `running`; plugin-level `stop` and `kill` first persist desired
`stopped`; `plugin restart` persists desired `running` and requests one process
recycle. If persisting the desired-state change fails, oll MUST NOT report
success or perform the corresponding process transition. Start, stop, and kill
are idempotent. Restart starts an absent process and never creates two
instances.

Only plugin lifecycle commands change desired state. A plugin crash, startup or
session failure, heartbeat timeout, job timeout, `job stop`, or `killjob`
terminates the current instance without disabling the plugin. After that
instance exits, a desired-running plugin is restarted with bounded backoff; a
desired-stopped plugin remains exited. An explicit plugin-level stop or kill
cancels pending restart timers. Clean daemon shutdown terminates child processes
without changing their desired states, so desired-running plugins start again
on the next daemon start. Plugin calls do not implicitly start stopped plugins.

The controller reconciles desired and observed state after every lifecycle
command or runtime event:

| Desired | Observed | Required action |
| --- | --- | --- |
| `running` | `exited` | Start immediately or after the active restart backoff. |
| `running` | `failed` | Finish teardown, then restart after backoff. |
| `running` | `stopping` | Wait for child exit, then restart without overlapping instances. |
| `running` | `starting` or `ready` | Do not start another instance. |
| `stopped` | `starting` or `ready` | Begin the graceful shutdown sequence. |
| `stopped` | `stopping` or `failed` | Finish teardown and do not restart. |
| `stopped` | `exited` | Do not restart. |

The controller owns the spawned process handle and asynchronously waits for its
exit through the operating system (`Child::wait` in the Tokio implementation).
It also reacts to `SessionReady`, stream closure, lifecycle commands, and
startup and heartbeat deadlines. It MUST NOT poll the process table, and the
plugin does not provide a reverse liveness FD. A reverse FD would depend on
plugin cooperation and could remain open in an inherited descendant even after
the main plugin process exited.

The executable named by a plugin installation MUST remain the foreground plugin
host process. It MUST NOT daemonize, detach, or exit after delegating the gRPC
service to an untracked process. This process contract is independent of the
plugin's implementation language. oll can always observe and reap the direct
child; protocol cooperation is required only for readiness, heartbeat, and
graceful shutdown.

At spawn time oll passes the configured endpoint and a parent-liveness file
descriptor. oll keeps the descriptor open. If oll crashes, the kernel closes it
and the plugin reads EOF and exits. This one-way pipe lets the plugin observe
host death; it is not needed for oll to observe child exit.

The protobuf session handshake is:

1. oll sends `HostHello` with `NodeIdentity`, session/instance IDs, exact schema
   hash, depth limits, and artifact chunk limit;
2. the plugin validates it and sends `PluginHello` with identity, actions, and
   event subscriptions;
3. both sides send `SessionReady`;
4. jobs and host calls are legal only after both ready messages were observed.

Session and instance IDs prevent accidental cross-wiring; plugins are trusted,
so these IDs are not authentication.

The plugin reaches observed `ready` only after both `SessionReady` messages.
Failure to become ready before the startup deadline, unexpected stream closure,
or a missed heartbeat deadline changes observed state to `failed`, begins the
same shutdown enforcement sequence when a process remains, and then reconciles
against desired state. Restart attempts MUST use delayed, bounded backoff rather
than a tight spawn loop; exact timing is local runtime policy, not protobuf.

## Multiplexed stream

All runtime calls share one bidi stream. `PluginEnvelope.message_id` is non-zero
and unique per sender in the session; direct responses set `reply_to`.

After readiness, oll may send `Heartbeat` when it needs to test protocol
responsiveness. The plugin replies with a `Heartbeat` carrying the same nonce
and sets `reply_to` to the request message ID. A missing response before the
host deadline detects a live-but-unresponsive process; it is not used to detect
normal process exit. Stream closure and the operating-system child-exit event
remain immediate event sources.

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
24 hours. A generic action invocation carries an action name and shell-style
UTF-8 argv strings; oll preserves order, duplicates, empty strings, and leading
`-` values without type inference. Small structured results and configuration,
scheduler, and log fields use recursive `ConfigValue`; PDF, `.apkg`, and other
large results use the chunked artifact protocol with declared size and SHA-256.

An executing job is not cooperatively cancelled over RPC. `stop`, `kill`,
`killjob`, and timeout all begin process termination the same way: oll sends one
graceful `ShutdownRequest`. There is no separate force-kill request or command
mode, and `kill` does not skip graceful shutdown. This shared termination
mechanism is separate from desired state: plugin-level `stop` and `kill` both
persist desired `stopped`, while `job stop`, `killjob`, timeout, and failure
leave desired state unchanged.

If the plugin does not exit, oll enforces that same request with `SIGTERM`, waits
through the configured OS-signal grace period, and finally uses `SIGKILL`. These
signals are escalation mechanics for an unresponsive process, not distinct
management operations. Termination does not roll back CRDT writes or external
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
envelope's correlation, call, causal, task, and group identifiers. Plugin output
is normalized and routed according to [observability.md](observability.md).
Additional metrics or tracing systems are not required for the first version.

## External side effects

oll cannot atomically combine a CRDT commit with AnkiWeb, a blog deployment, an
AI provider, or another external service. Plugins own retry, idempotency, and
partial-failure behavior for those systems. oll does not pretend to compensate
operations it cannot understand or reverse.
