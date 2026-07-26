# Implementation order

Implementation MUST proceed in this order:

```text
CLI (Clap) -> node -> replica -> sync -> plugin system
```

Later stages may depend on stable interfaces from earlier stages. An earlier
stage must not depend on a placeholder implementation of a later stage.

This order is a software-engineering constraint, not part of oll's runtime or
domain model. Stage names and implementation progress MUST NOT appear in source
enums, configuration, protobuf contracts, persisted state, or user-facing error
messages. A command without a handler may temporarily return the generic
`EX_UNAVAILABLE`; its handler replaces that branch directly when implemented.

## 1. CLI

The only executable is `oll`; there is no `olld`.

The first stage establishes:

- Clap command parsing and help output;
- statically embedded LuaJIT through `mlua`/`luajit-src`;
- `config.lua` evaluation, typed schema conversion, path resolution, and
  environment/CLI precedence;
- subcommand-only selection of the `run` daemon entry versus bounded clients;
- parsing of the hidden `run --pingback` launcher handshake argument;
- stable error reporting and process exit codes;
- a single data-directory/configuration context.

The command contract is fixed in `cli.md`. The CLI must preserve the
one-daemon/one-node/one-replica invariant and must not grow a multi-replica
selector.

Completion criteria:

- invalid or non-terminating configuration fails before runtime initialization;
- CLI tests cover parsing, configuration returns, environment/CLI precedence,
  path bases, and exit behavior;
- daemon startup can be invoked through the same `oll` binary.

## 2. Node

The node stage establishes the long-running daemon shell without implementing
replica behavior:

- atomic `NodeId`/`NodeName` identity creation and durable loading;
- process lifecycle and graceful shutdown;
- the typed Admin gRPC service over UDS;
- detached `start` launch, single-instance enforcement, and nonce pingback;
- `connect`/`listen` deployment configuration;
- one data directory and one replica slot;
- Tokio runtime ownership;
- structured JSON logging, user-owned log-directory sinks, aggregation, dynamic filters,
  and correlation context as specified in `observability.md`;
- child-process liveness-pipe support needed later by plugins.

Completion criteria:

- a node starts, reports status, and shuts down deterministically;
- status reports the complete local `NodeIdentity`;
- release builds contain no gRPC reflection service; debug builds expose it only
  on the Admin UDS;
- `oll.log` and `sync.log` initialize with correct ownership and emit valid JSON
  events carrying correlation IDs;
- a second replica cannot be attached to the same process;
- configuration distinguishes connection topology from node authority.

## 3. Replica

The replica stage implements:

- persistent `ReplicaId`;
- the catalog `LoroDoc`;
- stable `DocumentId` values and one `LoroDoc` per document;
- path lookup and directory-tree traversal;
- content and abstract CRDT read/write APIs;
- opaque document `Revision` tokens and optimistic preconditions;
- local commit coordination and crash recovery;
- `.ollsnap` export and import.

Completion criteria:

- create/read/update/move/delete operations survive restart;
- stale revisions reject the complete host-level commit before mutation;
- snapshot round trips preserve catalog and every retained document CRDT;
- malformed archives cannot escape the import staging directory.

## 4. Sync

Sync is implemented only after the replica object model is stable. It adds:

- symmetric bidi gRPC sessions;
- exact protocol/Loro encoding fingerprint checks;
- catalog and document object advertisements;
- per-object delta or snapshot transfer;
- chunk validation, flow control, import acknowledgement, and reconnection;
- durable remote `NodeIdentity` bindings and collision rejection;
- offline edits and concurrent multi-writer convergence tests.

Completion criteria:

- neither transport endpoint has replication authority;
- contradictory remote name-to-ID or ID-to-name bindings are rejected;
- independently edited nodes converge for catalog and document changes;
- a missing document object is requested after its catalog entry arrives;
- interrupted transfers resume by re-advertising state, not by trusting partial
  files;
- one correlation ID links a delta request, transfer, import, and acknowledgement
  across both peers.

## 5. Plugin system

The final stage adds:

- plugin installation from typed Git remotes and data-only `plugins.lua`;
- `oll.toml` source recipes and direct-URL `oll.json` release artifacts;
- literal-only `plugins.lua` validation and reuse of the established LuaJIT
  configuration runtime for plugin values and closures;
- process spawning and parent-liveness pipes;
- persistent plugin desired state and event-driven child-process supervision;
- `PluginRuntime.Connect` multiplexing;
- asynchronous jobs, host document calls, Lua configuration callbacks,
  scheduling, artifact output, logs, and process termination;
- recursion-depth and causal-depth enforcement.

Completion criteria:

- a plugin can read the tree and atomically attempt a revision-guarded write;
- stale plugin output is rejected without blocking;
- nested calls do not stop the stream reader;
- plugin stop survives daemon restart, stopped plugins are not respawned, and
  unexpected exits of desired-running plugins trigger a bounded-backoff restart
  without process-table polling or a plugin-supplied reverse liveness FD;
- PDF/`.apkg`-sized outputs use verified artifact chunks;
- `stop`, `kill`, `killjob`, and timeout all issue the same graceful
  `ShutdownRequest`; an unresponsive process is escalated through `SIGTERM` and
  `SIGKILL` without creating another public termination semantic;
- plugin logs are normalized into `plugin.log` while lifecycle summaries remain
  correlated in `oll.log`.

## Deferred work

The first implementation does not include:

- multi-replica process management;
- a plugin marketplace, signatures, or permission sandbox;
- backward-compatible protobuf negotiation;
- fair scheduling, quotas, or a CFS-like scheduler;
- distributed transactions with external services.
