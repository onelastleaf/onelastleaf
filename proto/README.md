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
- `oll/common.proto`: node, replica, document, binary, and plugin identities;
  opaque catalog/document revisions; shared log severity, tracing/depth
  metadata, and errors.
- `oll/config.proto`: the language-neutral Lua/plugin value boundary and remote
  configuration-function handles.
- `oll/document.proto`: stable catalog/document/binary identities, paths,
  directory access, full document access, oll's CRDT abstraction, and
  optimistic host commits.
- `oll/replication.proto`: symmetric Noise-protected peer replication using
  opaque Loro update batches and hash-addressed blob chunks over TCP.
- `oll/plugin.proto`: the multiplexed host/plugin runtime stream.

Package file formats, Git execution, source-recipe mechanics, archive extraction,
process spawning, and filesystem publication are not wire protocols and remain
in the design documents. Typed Admin requests orchestrating that work are added
with the plugin implementation.

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

At the present design checkpoint, `admin.proto` is intentionally still the
implemented pre-plugin service. The approved plugin Admin methods listed in
`docs/admin-api.md` do not yet exist in the descriptor and must be added with
their runtime implementation. This README does not claim otherwise.

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

An authenticated session starts a bidirectional finite operation with
`SyncRoundRequest`; simultaneous requests are deterministically coalesced by
canonical `NodeId`. `SyncRoundStart`, numbered `SyncRoundInventory` batches, and
`SyncRoundInventoryComplete` capture the source object/blob set. The receiver
requests missing Loro update ranges and SHA-256-addressed blobs.
Start/chunk/complete messages transfer each payload; typed ACK means verified
private staging, while typed reject identifies a transfer failure without
exposing content. `SyncRoundCommitted` alone means the fully validated inactive
candidate was atomically made active. A base-generation CAS failure rejects and
retries the round rather than publishing partial state.

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

oll binds an instance-owned loopback listener on port `0`, starts the plugin,
and passes the endpoint through `OLL_PLUGIN_ENDPOINT`. oll hosts
`PluginRuntime`; the plugin is the gRPC client and opens `Connect`. Once open,
either endpoint can initiate messages on the same bidirectional stream. The
session starts as follows:

1. oll sends `HostHello` with the expected PluginId, effective PluginName,
   session and instance identifiers, schema fingerprint, and limits.
2. The plugin validates those values, then sends `PluginHello` repeating its
   identity and declaring actions and event subscriptions.
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
handle, not by heartbeat or process-table polling.

The child's stdin is the parent-liveness pipe. oll keeps its write end open and
the plugin contract requires exit after EOF. Runtime stdout/stderr go to plugin
logs. Endpoint environment variables, stdin EOF, OS process groups, and signal
delivery are spawn-time operating-system contracts and do not belong in
protobuf. No one-time bearer token is added to the loopback protocol.

`StartJobRequest` is asynchronous. `JobAccepted` only confirms ownership of the
job ID. Completion is a later terminal `JobUpdate`; the host does not hold a
synchronous call stack open for the duration of a job. If no deadline is
provided, oll applies the default 24-hour deadline.

Generic action invocations carry an action name plus ordered shell-style UTF-8
argv strings. Empty arguments, duplicate arguments, and values beginning with
`-` are preserved verbatim; oll does not infer argument types. `ConfigValue`
continues to carry recursive structured data for Lua configuration, structured
job results, and log fields. Large binary results such as PDF and `.apkg` files
use the artifact sub-protocol. The plugin announces the ID, safe filename, size,
hash, and chunk count; waits for `ArtifactTransferAccepted`; sends zero-based
chunks within the host's advertised limit; and finishes with
`ArtifactTransferComplete`. oll verifies and publishes the complete artifact
before replying with `ArtifactStored`. A terminal job update may reference only
stored artifacts. Failed and partial transfers are discarded.

Nested calls increment `call_depth`; derived events increment `causal_depth`.
The initial protocol uses a maximum of 10 for both. A receiver rejects an
over-limit message without executing it, and known cycles may be rejected
earlier. Scheduling is deferred and has no message placeholder in this version.

Lua configuration executes inside oll's one LuaJIT state. The caller's
PluginId selects its live per-plugin file on each top-level read.
`ConfigFunctionRef` uses the active `session_id + function_id` to identify a
closure in that shared registry; it does not serialize a closure or carry a Lua
runtime generation. Session teardown invalidates its handles. Config adapters
reject cyclic tables, unsupported userdata, threads, and unconverted functions.
After a reentrant call returns, oll re-reads and validates relevant host state.

Logs are structured `LogRecord` messages. `PluginEnvelope.trace` supplies the
correlation, parent-call, causal, task, and task-group fields used by log
aggregation.

Job stop and deadline expiry send `CancelJobRequest`; the plugin ceases only
that job and replies `CancelJobAcknowledged`. This does not change process
desired state, stop another job, or terminate the process. Cancellation does
not imply rollback of completed writes or external effects.

Process-scoped stop sends `ShutdownRequest`. If necessary, the supervisor
enforces that request through the documented Unix process-group signals, which
remain outside protobuf. Desired and observed process states belong to local
Admin/supervisor state rather than this stream. Crash or protocol failure leaves
desired state unchanged; an explicit plugin stop persists `stopped`.

Plugins are trusted. This protocol intentionally has no permission grants,
signatures, marketplace identity, or document capability tokens. Session and
instance identifiers prevent accidental cross-wiring; they are not a sandbox or
authentication mechanism.

Remote plugin invocation, input-file upload, and scheduling are deferred and
have no version-1 messages.

## Validation

From the repository root:

```sh
protoc --fatal_warnings -I proto \
  --include_imports \
  --descriptor_set_out=/tmp/oll-protocol.pb \
  $(find proto/oll -name '*.proto' -print | sort)
```
