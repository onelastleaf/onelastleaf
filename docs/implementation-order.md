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

- invalid configuration fails before runtime initialization; configuration that
  does not terminate remains in user-owned Lua execution and cannot reach
  runtime initialization;
- CLI tests cover parsing, configuration returns, environment/CLI precedence,
  path bases, and exit behavior;
- daemon startup can be invoked through the same `oll` binary.

## 2. Node

The node stage establishes the long-running daemon shell without implementing
replica behavior:

- atomic creation, durable loading, and final-state hot watching of the
  user-owned `node.json` `NodeIdentity` record;
- process lifecycle and graceful shutdown;
- the typed Admin gRPC service over UDS;
- detached `start` launch, single-instance enforcement, and nonce pingback;
- `connect`/`listen` deployment configuration;
- one configured user-editable working tree and one replica-store slot;
- Tokio runtime ownership;
- structured JSON logging, user-owned log-directory sinks, aggregation, dynamic filters,
  and correlation context as specified in `observability.md`;
- child-process liveness-pipe support needed later by plugins.

The first node runtime is Unix-only. It establishes one empty replica slot but
does not create a `ReplicaId`, catalog, document store, sync listener, or plugin
runtime; those remain owned by their later stages.

Completion criteria:

- a node starts, reports status, and shuts down deterministically;
- status reports the complete local `NodeIdentity`;
- release builds contain no gRPC reflection service; debug builds expose it only
  on the Admin UDS;
- `oll.log` and `sync.log` initialize with correct ownership and emit valid JSON
  events carrying correlation IDs;
- `oll log set <target>=<level>` changes the live typed filter through the
  Admin UDS without restarting the daemon;
- a second replica cannot be attached to the same process;
- configuration distinguishes connection topology from node authority.
- a valid runtime `node.json` replacement is adopted under the identity
  coordinator, while an invalid transient edit retains the last coherent
  identity.

## 3. Replica

The replica stage implements:

- persistent, user-owned `<config-root>/replica.json` `ReplicaId` identity,
  SQL transition recovery, and coordinated hot edits;
- the catalog `LoroDoc` and its `LoroTree` namespace;
- stable `DocumentId` values and one `LoroDoc` per text document;
- `BinaryId` values, LWW binary metadata, and content-addressed binary blobs;
- the SQL-backed replica store, initial working-tree scan, recursive watcher,
  debounced reconciliation, and crash-safe projection recovery;
- path lookup and directory-tree traversal;
- content and abstract CRDT read/write APIs;
- separate opaque catalog/document revision tokens and optimistic
  preconditions;
- local commit coordination and crash recovery;
- `.ollsnap` export and import.

Completion criteria:

- create/read/update/move/delete operations survive restart;
- stale revisions reject the complete host-level commit before mutation;
- snapshot round trips preserve catalog, every retained document CRDT, and
  every retained binary blob;
- malformed archives cannot escape the import staging directory.
- first initialization and snapshot replacement recover the identity file and
  active generation on both sides of the SQL linearization point;
- a valid runtime `replica.json` edit updates the active SQL cache before future
  commits use it.

## 4. Sync

Sync is implemented only after the replica object model is stable. It adds:

- symmetric TCP sessions protected by
  `Noise_NNpsk0_25519_ChaChaPoly_BLAKE2s` and a configured shared network key;
- u16-length Noise transport frames carrying prost-encoded `SyncEnvelope`
  messages, with allocation limits checked before reads;
- exact protocol-schema fingerprint checks and actual Loro decode/import
  validation;
- finite catalog/document object inventory rounds and atomic candidate
  activation;
- content-addressed binary-blob transfer after catalog metadata arrives;
- per-object Loro update batches without Loro or `.ollsnap` snapshot fallback;
- chunk validation, staging/commit acknowledgement, TCP backpressure, and
  reconnection;
- atomic bootstrap of an uninitialized receiver from one authenticated source;
- durable remote `NodeIdentity` bindings and collision rejection;
- local `oll psk` and typed sync/ping Admin operations;
- session invalidation when the earlier node-owned identity epoch changes;
- offline edits and concurrent multi-writer convergence tests.

Completion criteria:

- neither transport endpoint has replication authority;
- contradictory remote name-to-ID or ID-to-name bindings are rejected;
- independently edited nodes converge for catalog and document changes;
- active state never exposes a catalog entry whose retained document object or
  binary blob is missing;
- interrupted transfers resume by re-advertising state, not by trusting partial
  files;
- bootstrap has one SQL active-generation compare-and-swap linearization point
  and recovers correctly on either side of it;
- one correlation ID links an update request, transfer, import, and acknowledgement
  across both peers.

## 5. Plugin system

The final stage adds:

- plugin installation through the system Git client and data-only `plugins.lua`;
- `oll.toml` source recipes, typed user masks, and direct-URL
  `oll-release.json` release artifacts;
- typed Admin reconciliation, removal, query, lifecycle, release-list, and job
  methods behind the corresponding CLI commands;
- PluginId-keyed package generations, atomic `current` publication, SQL
  transition/removal recovery, and exact-set `plugin reconcile`;
- literal-only `plugins.lua` validation and live per-plugin Lua configuration;
- process spawning, loopback gRPC sessions hosted by oll, and stdin
  parent-liveness pipes;
- persistent plugin desired state and event-driven child-process supervision;
- `PluginRuntime.Connect` multiplexing initiated by the spawned plugin;
- asynchronous jobs, host document calls, Lua configuration callbacks,
  artifact output, logs, and process termination;
- recursion-depth and causal-depth enforcement.

Completion criteria:

- a plugin can read the tree and atomically attempt a revision-guarded write;
- source and release candidates either publish one complete PluginId-keyed
  generation or leave the previous `current` generation active across failure
  and restart;
- `plugins.lua` contains no process desired state; SQL start/stop authority and
  package removal survive daemon restart on both supported store backends;
- duplicate, out-of-order, or stale-session plugin output is rejected without
  blocking current instance work or teardown;
- plugin-originated host calls are dispatched without stopping the stream
  reader, and pending host work cannot block session shutdown;
- desired-stopped state survives daemon restart, stopped plugins are not
  respawned, and
  unexpected exits of desired-running plugins trigger a bounded-backoff restart
  without process-table polling; the stdin EOF liveness contract is covered by
  process tests;
- PDF/`.apkg`-sized outputs use verified artifact chunks;
- retrying the same normalized job admission with one operation ID returns one
  JobId, while reuse for another payload is rejected;
- plugin stop issues a graceful process `ShutdownRequest`, while job stop and
  timeout issue a job-scoped `CancelJobRequest` without terminating unrelated
  jobs;
- plugin logs are normalized into `plugin.log` while lifecycle summaries remain
  correlated in `oll.log`.

## Deferred work

The first implementation does not include:

- multi-replica process management;
- a plugin marketplace, signatures, or permission sandbox;
- backward-compatible protobuf negotiation;
- plugin scheduling, fair scheduling, quotas, or a CFS-like scheduler;
- document/catalog event subscriptions and event-triggered plugin jobs;
- remote plugin invocation and separate file-upload/download protocols;
- distributed transactions with external services.
