# oll protocol

This directory is the wire contract for oll. The package is intentionally named
`oll.protocol`, without a version component. The first implementation requires
an exact `protocol_schema_sha256` match during both replication and plugin
handshakes. A schema change is a coordinated upgrade; this protocol does not
promise backward compatibility.

The schema hash is a build artifact computed from the canonical descriptor set.
The descriptor is not a hand-maintained repository artifact. Builds generate it
from every `proto/oll/*.proto` file in bytewise-sorted pathname order, include
imports, and hash the resulting descriptor-set bytes with SHA-256:

```sh
protoc --fatal_warnings -I proto \
  --include_imports \
  --descriptor_set_out="$OUT_DIR/oll-protocol.pb" \
  $(find proto/oll -name '*.proto' -print | sort)
# The build script computes SHA-256 over $OUT_DIR/oll-protocol.pb.
```

The build embeds that exact hash in the CLI and daemon. SDKs should receive the
published build hash rather than guessing a source-file set or compiler flags.
Changing a proto changes the descriptor and therefore requires coordinated
binary replacement, as intended.

## Files

- `oll/admin.proto`: the local typed gRPC administration service, request
  context, node status, and graceful daemon shutdown.
- `oll/common.proto`: identities, opaque catalog/document revisions, shared log
  severity, tracing/depth metadata, and shared errors.
- `oll/config.proto`: the language-neutral Lua/plugin value boundary and remote
  configuration-function handles.
- `oll/document.proto`: stable catalog/document/binary identities, paths,
  directory access, full document access, oll's CRDT abstraction, and
  optimistic host commits.
- `oll/replication.proto`: symmetric Noise-protected peer replication using
  opaque Loro update batches and hash-addressed blob chunks over TCP.
- `oll/plugin.proto`: the multiplexed host/plugin runtime stream.

Package installation, Git remote parsing, source recipes, direct release
downloads, process spawning, and plugin manifests are not wire protocols and
are therefore outside this directory.

## Local administration

The daemon hosts `Admin` only on its Unix domain socket. It is not served on a
replication TCP listener. CLI syntax is parsed in the client process and reduced
to typed requests; argv and serialized Clap values never cross the socket.
Output selection such as `status --json` is also client-side presentation.

Every Admin request carries `AdminCallContext`, including the canonical schema
fingerprint and correlation context. The daemon requires an exact fingerprint
match. This catches a newly installed CLI connecting to an older still-running
daemon; it does not introduce version negotiation or compatibility promises.

The node stage initially implements `GetStatus`, `Shutdown`, and
`SetLogFilter`. The replica stage adds its four typed methods. The sync stage
adds only `SynchronizePeers` and `PingPeer`; `oll psk` and `oll sync --log` are
local operations. Plugin management methods are added only in their
implementation stage. Future CLI arguments must not be tunneled as generic
strings to avoid extending this schema.

The replica protocol defines three explicit status states
(`uninitialized`, `initialized_empty`, and `initialized_populated`) plus
`InspectReplicaDocument`, `ListReplicaOperations`, `ExportReplica`, and
`ImportReplica`. The first two are document-scoped because the existing CLI
takes a document path; they are not silently broadened into generic catalog
entry inspection. Snapshot source/destination paths and local document paths
use a Unix-native pathname-bytes message on this Admin-only boundary, then the
daemon performs containment and namespace conversion. That native path type is
not reused by the portable document/plugin API.

`GetStatus` returns `NodeIdentity`, the durable one-to-one pairing of UUID-v4
`NodeId` and human-readable `NodeName`, plus the configured listen address when
present. The name is node-declared and globally consistent, not a receiver-local
label or a value inferred from a connect target. Peer rows expose direction,
connection state, optional `oll://` target, and the optional remote identity.
An authenticated inbound-only row has no connect target.

Admin failures are direct gRPC statuses. In particular, a request whose
descriptor fingerprint differs returns `FAILED_PRECONDITION` with a message
telling the user to restart the still-running daemon. It does not return a
`ProtocolError` payload or negotiate a compatible schema. `SetLogFilter` takes
a parsed target and `LogLevel`; `oll log set` is the only shell-style directive
parser and the resulting filter resets at daemon restart.

gRPC Server Reflection is a debug-build facility registered only on the Admin
UDS. It is compiled out of release builds. Production `TRACE` logs include
redacted request metadata, never complete protobuf requests or prohibited
content.

## Document invariants

The replica-stage document contract uses two opaque revision types. A
`CatalogRevision` is scoped to one `CatalogNodeId` and covers path, name,
parent, kind, and metadata. A `DocumentRevision` is scoped to one `DocumentId`
and covers the text body and abstract CRDT containers. They are opaque to
plugins and must not be synthesized or parsed. A plugin includes the explicit
target ID and relevant revision(s) in `CommitDocumentsRequest.preconditions`.
oll checks every precondition immediately before opening one host-level commit.
If any check fails, oll returns `REVISION_CONFLICT` with
`RevisionConflictDetail` and applies no mutation. The catalog and documents are
separate LoroDocs, so local atomic visibility requires the replica write
coordinator and crash-recovery journal; it is not one Loro transaction.

The same replica contract defines UUID-v4 `BinaryId` and `NODE_KIND_BINARY` for
working-tree files whose bytes follow binary LWW rather than document CRDT
semantics. A binary has catalog and blob metadata but no `DocumentRevision`.

Mutations in one commit are evaluated in order. Indexes used by later mutations
observe earlier mutations in that commit. Text offsets count Unicode scalar
values, not UTF-8 bytes, UTF-16 code units, or grapheme clusters. `ListMove` is
valid only for a movable list; its destination is evaluated after removing the
source range.

`operation_id` makes retries idempotent while the receiving oll node retains the
operation result. Callers must reuse it only for a byte-for-byte equivalent
commit. It is not a distributed transaction ID.

The CRDT model in `document.proto` is an oll API. Implementations translate it
to Loro internally. Plugins never receive Loro container IDs, frontiers, version
vectors, or library-specific operations.

Every text document LoroDoc has fixed named `content` (`LoroText`) and `data`
(`LoroMap`) roots. Create initializes both, body replacement and filesystem
reconciliation affect `content`, and abstract CRDT paths are rooted under
`data`. No wire operation creates, replaces, or deletes either fixed root.
Invalid paths, indexes, ranges, or container kinds reject the complete ordered
host commit with `INVALID_ARGUMENT` before any result is published. Binary
entries are not accepted by document operations and never receive a LoroDoc.

CRDT commit and external side effects are not atomic. The runtime provides no
rollback, compensation, saga, or exactly-once guarantee for external systems.

## Synchronization transport

`replication.proto` defines messages only. It deliberately has no gRPC service.
Peers exchange them over TCP protected by
`Noise_NNpsk0_25519_ChaChaPoly_BLAKE2s`, using the exact cleartext preface and
Noise prologue `b"OLLSYNC\x01"`. Handshake messages and transport ciphertexts
are each prefixed by one unsigned big-endian u16 length. Handshake messages have
a 1024-byte limit. Transport ciphertext is at most 65535 bytes; its decrypted
plaintext is exactly one ordinary prost-encoded `SyncEnvelope`, not another
length-delimited protobuf record.

Both peers send `SyncHello` immediately after Noise. It carries `NodeIdentity`,
the exact descriptor hash, maximum chunk-data bytes, and either one `ReplicaId`
or `NoLocalReplica`. `SyncReady` confirms the selected chunk size and one common
session replica. There is no compression negotiation, session nonce, protocol
downgrade, or application flow-control message. TCP backpressure plus bounded
local staging provides flow control.

An authenticated session carries finite inventory rounds. `SyncRoundStart`,
numbered `SyncRoundInventory` batches, and `SyncRoundInventoryComplete` capture
the source object/blob set. The receiver requests missing Loro update ranges and
SHA-256-addressed blobs. Start/chunk/complete messages transfer each payload;
typed ACK means verified private staging, while typed reject identifies a
transfer failure without exposing content. `SyncRoundCommitted` alone means the
fully validated inactive candidate was atomically made active. A base-generation
CAS failure rejects and retries the round rather than publishing partial state.

Normal and bootstrap rounds use the same transfers. Normal rounds export updates
after the receiver's version vector. Bootstrap exports the complete retained
updates from an empty vector and all referenced blobs into an uninitialized
receiver's inactive generation. Neither mode carries a Loro snapshot or an oll
`.ollsnap` archive. Binary bytes are never modeled as a Loro object.

Every `SyncEnvelope` carries a nonempty `correlation_id`. One update request and
its chunks, staging, candidate commit, and acknowledgement reuse an ID across
both peers. All files and the activation of one bootstrap reuse the inherited
bootstrap correlation ID.

Only this internal protocol carries Loro version vectors, frontiers, and encoded
update batches. Applications and plugins use catalog/document revisions. Loro
compatibility is determined by actual decode/import of a verified payload, not
by a separate encoding fingerprint.

## Plugin stream

The plugin process hosts `PluginRuntime`. oll starts the process and opens
`Connect`; once open, either endpoint can initiate messages on the same stream.
The session starts as follows:

1. oll sends `HostHello`.
2. The plugin validates the schema and instance identifiers, then sends
   `PluginHello` with its actions and event subscriptions.
3. Both endpoints send `SessionReady`. No job or host call is valid before both
   ready messages have been observed.

`message_id` is non-zero and unique per sender for the session. A direct response
sets `reply_to` to the request's ID. Stream readers must continue dispatching
messages while calls are pending; waiting for a response in the stream-reader
task would break nested host/config/plugin calls.

After readiness, a `Heartbeat` request is answered by a `Heartbeat` with the
same nonce and `reply_to` set to the request message ID. oll uses a response
deadline to detect a process that still exists but no longer services its
protocol. Normal process exit is observed from the host-owned child-process
handle, not by heartbeat or process-table polling. The plugin does not supply a
reverse liveness FD.

`StartJobRequest` is asynchronous. `JobAccepted` only confirms ownership of the
job ID. Completion is a later terminal `JobUpdate`; the host does not hold a
synchronous call stack open for the duration of a job. If no deadline is
provided, oll applies the default 24-hour deadline.

Generic action invocations carry an action name plus ordered shell-style UTF-8
argv strings. Empty arguments, duplicate arguments, and values beginning with
`-` are preserved verbatim; oll does not infer argument types. `ConfigValue`
continues to carry recursive structured data for Lua configuration, scheduler
inputs, structured job results, and log fields. Large binary results such as PDF
and `.apkg` files use the artifact sub-protocol. The plugin announces the size,
hash, and chunk count; waits for `ArtifactTransferAccepted`; sends zero-based
chunks within the host's advertised size; and finishes with
`ArtifactTransferComplete`. oll verifies the complete size and SHA-256 before
replying with `ArtifactStored`. A terminal job update may reference only stored
artifacts. Failed and partial transfers are discarded.

Nested calls increment `call_depth`. Events caused by another event increment
`causal_depth`, including events deferred through the scheduler. The initial
protocol uses a maximum of 10 for both values. A receiver rejects a message over
the negotiated limit with the matching depth error and does not execute it.
Known recursive event patterns may be rejected before reaching the limit.

The optional scheduler is owned by oll's Tokio runtime. A host without it returns
`UNSUPPORTED`. A scheduled task inherits the envelope's `task_group_id` unless
the host assigns a new group. The first implementation does not promise fairness,
queue bounds, quotas, or CFS-like scheduling.

Lua configuration executes inside oll. `ConfigFunctionRef` is a session-scoped
remote handle, not a serialized closure. It becomes invalid when the session or
Lua runtime ends. Config adapters reject cyclic tables, unsupported userdata,
threads, and functions not converted to a function handle. Reentrant calls are
allowed: after a nested call returns, oll must re-read and validate relevant
state instead of trusting state captured before entering Lua.

Logs are structured `LogRecord` messages. `PluginEnvelope.trace` supplies the
correlation, parent-call, causal, task, and task-group fields used by log
aggregation.

Cancellation does not imply rollback. A queued scheduler task can be removed,
but an executing job is not cooperatively cancelled over RPC. `stop`, `kill`,
`killjob`, and timeout all send the same graceful `ShutdownRequest`; there is no
force-kill RPC or distinct public kill semantic. An unresponsive process is
escalated through `SIGTERM` and `SIGKILL` as enforcement of that request. The
parent-liveness FD is a spawn-time OS contract: oll keeps it open and the plugin
exits after EOF if oll dies. Neither signal delivery nor inherited FDs belong in
protobuf.

Desired and observed plugin process states belong to the local supervisor, not
this plugin wire protocol. Plugin-level stop and kill both persist desired
`stopped`; job stop, killjob, timeout, crash, and protocol failure leave desired
state unchanged. A desired-running plugin is therefore restarted after the
current instance exits, while a desired-stopped plugin is not.

Plugins are trusted. This protocol intentionally has no permission grants,
signatures, marketplace identity, or document capability tokens. Session and
instance identifiers prevent accidental cross-wiring; they are not a sandbox or
authentication mechanism.

## Validation

From the repository root:

```sh
protoc --fatal_warnings -I proto \
  --include_imports \
  --descriptor_set_out=/tmp/oll-protocol.pb \
  $(find proto/oll -name '*.proto' -print | sort)
```
