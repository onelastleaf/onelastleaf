# oll protocol

This directory is the wire contract for oll. The package is intentionally named
`oll.protocol`, without a version component.

## Protobuf evolution policy

Every protobuf protocol in this repository and every related onelastleaf
project MUST NOT compute, publish, exchange, or compare a schema hash or schema
fingerprint. This includes protobuf fields, handshakes, package metadata,
generated constants, and compatibility gates derived from a descriptor set.

Protobuf already defines wire behavior for additive fields and unknown data. A
complete descriptor digest changes even when a new field is wire-compatible,
and it also changes when an unrelated service changes. Digest equality would
therefore reject compatible communication and couple independent APIs without
checking the messages actually used by a session.

These protocols MUST instead evolve wire-compatibly wherever possible:

- existing field numbers and wire types remain stable;
- additions use new fields whose absence has a safe meaning;
- senders do not require receivers to understand a new field merely to execute
  an unchanged operation;
- decoders tolerate unknown protobuf fields and enum values, while application
  code continues to reject invalid identity, state, size, ordering, security,
  and operation semantics.

Generic protobuf API-version fields, versioned protobuf package namespaces, and
schema-version negotiation MUST NOT replace the removed fingerprints. The only
protocol API-versioning mechanism in this repository is the major byte in the
exact sync transport preface and Noise prologue `b"OLLSYNC\x01"`. It exists
outside protobuf because it guards framing, the Noise handshake, and the
pre-protobuf state machine. It MUST remain `\x01` unless an unavoidable
incompatible transport change cannot be expressed through wire-compatible
protobuf evolution; `\x02` is a last resort.

Builds still generate a descriptor set for debug gRPC Server Reflection. The
descriptor is not a compatibility token and is never hashed for protocol use.
Official SDK repositories generate code from canonical `plugin.proto` and its
imports and follow the same evolution policy. Their common state-machine and
release contract is documented in
[`docs/plugin-sdk.md`](../docs/plugin-sdk.md).

## Files

- `oll/admin.proto`: the local typed gRPC administration service, node status,
  and graceful daemon shutdown.
- `oll/admin_common.proto`: the request context shared by every unary Admin
  operation.
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
- `oll/plugin_admin.proto`: typed local plugin package, process, release, and
  job administration messages.

Package file formats, Git execution, source-recipe mechanics, archive extraction,
process spawning, and filesystem publication are not wire protocols and remain
in the design documents. Typed plugin Admin requests orchestrate that work
without carrying CLI argv or package payload bytes. Plugin package inspection
reports the publisher's source checkout policy as a typed enum alongside the
rest of the effective manifest; it does not infer a language or move package
mechanics into protobuf.

## Local administration

The daemon hosts `Admin` only on its Unix domain socket. It is not served on a
replication TCP listener. CLI syntax is parsed in the client process and reduced
to typed requests; argv and serialized Clap values never cross the socket.
Output selection such as `status --json` is also client-side presentation.

Every Admin request carries `AdminCallContext` correlation context. Admin uses
the protobuf evolution policy above and has no schema or API-version gate.

The node stage initially implemented `GetStatus`, `Shutdown`, and
`SetLogFilter`. The replica stage added its four typed methods, and the sync
stage added only `SynchronizePeers` and `PingPeer`; `oll psk` and
`oll sync --log` remain local operations. The plugin stage adds the typed
reconciliation, removal, query, release-list, desired-state, restart, and job
methods defined in `plugin_admin.proto`. `plugin validate` and `plugin log`
remain local operations. Future CLI arguments must not be tunneled as generic
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

Admin failures are direct gRPC statuses; they are not wrapped in a
`ProtocolError` payload. `SetLogFilter` takes a parsed target and `LogLevel`;
`oll log set` is the only shell-style directive parser and the resulting filter
resets at daemon restart.

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
maximum chunk-data bytes, and either one `ReplicaId` or `NoLocalReplica`.
`SyncReady` confirms the selected chunk size and the common
session replica when one exists. Two uninitialized peers omit that ID and keep
one authenticated waiting connection. After either replica atomically appears,
`REPLICA_AVAILABLE` closes that waiting connection normally so the next
`SyncHello` can select bootstrap, normal sync, or a mismatch without changing a
session's selected ID in place. There is no compression negotiation, session
nonce, protocol downgrade, or application flow-control message. TCP
backpressure plus bounded local staging provides flow control.

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

1. oll sends a `HostHello` envelope. Its outer nonempty session and instance
   identifiers establish the authoritative identity pair for the stream;
   `HostHello` carries the expected PluginId, effective PluginName, and limits
   without duplicating those identifiers.
2. The plugin validates those values, then sends `PluginHello` repeating its
   identity and declaring actions.
3. Both endpoints send `SessionReady`. No job or host call is valid before both
   ready messages have been observed.

The plugin copies the first envelope's identity pair onto `PluginHello` and
every later envelope. After that first bootstrap message, either side rejects
any envelope whose outer pair differs. The removed `HostHello` field names and
numbers remain reserved and cannot become a second identity authority later.

Each sender owns an independent `message_id` sequence for the session. Its first
ID and every later ID are non-zero, and every later ID is strictly greater than
that sender's preceding ID. Gaps are valid and an implementation need not start
at one. A direct response sets `reply_to` to the request's ID. This contract lets
the receiver reject duplicates and reordering with one `last_seen` integer
rather than retaining an unbounded set. Stream readers must continue dispatching
messages while calls are pending.

After readiness, a `Heartbeat` request is answered by a `Heartbeat` with the
same nonce and `reply_to` set to the request message ID. oll uses a response
deadline to detect a process that still exists but no longer services its
protocol. Normal process exit is observed from the host-owned child-process
handle, not by heartbeat or process-table polling.

Each encoded `PluginEnvelope` gRPC message is limited to 64 MiB in either
direction before application dispatch. This finite transport bound also covers
document-bearing host-call responses. Artifact transfer continues to use the
smaller chunk limit advertised by `HostHello`; plugin trust does not disable
protobuf transport bounds.

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

Nested calls increment `call_depth`; transitive work carries bounded
`causal_depth`. The initial protocol uses a maximum of 10 for both. A receiver
rejects an over-limit message without executing it. Scheduling and
document/catalog event-driven invocation are deferred and have no message
placeholder in this version.

Lua configuration executes inside oll's one LuaJIT state. The caller's
PluginId selects its live per-plugin file on each top-level read.
`ConfigFunctionRef` uses the active `session_id + function_id` to identify a
closure in that shared registry; it does not serialize a closure or carry a Lua
runtime generation. Session teardown invalidates its handles. Config adapters
reject cyclic tables, unsupported userdata, threads, and unconverted functions.
The plugin may ask oll to invoke such a closure and receives the returned
`ConfigValue`; Lua evaluation does not emit plugin RPCs or envelopes.

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

Remote plugin invocation, input-file upload, scheduling, document/catalog event
subscriptions, and event-triggered jobs are deferred and have no placeholder
messages in the initial runtime contract.

## Validation

From the repository root:

```sh
protoc --fatal_warnings -I proto \
  --include_imports \
  --descriptor_set_out=/tmp/oll-protocol.pb \
  $(find proto/oll -name '*.proto' -print | sort)
```
