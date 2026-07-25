# oll protocol

This directory is the wire contract for oll. The package is intentionally named
`oll.protocol`, without a version component. The first implementation requires
an exact `protocol_schema_sha256` match during both replication and plugin
handshakes. A schema change is a coordinated upgrade; this protocol does not
promise backward compatibility.

The schema hash is a build artifact computed from the canonical, published
descriptor set. SDKs should embed the hash rather than independently guessing
which source files and compiler flags are canonical.

## Files

- `oll/admin.proto`: the local typed gRPC administration service, request
  context, node status, and graceful daemon shutdown.
- `oll/common.proto`: identities, tracing/depth metadata, and shared errors.
- `oll/config.proto`: the language-neutral Lua/plugin value boundary and remote
  configuration-function handles.
- `oll/document.proto`: stable document identities, paths, directory access,
  full document access, oll's CRDT abstraction, and optimistic host commits.
- `oll/replication.proto`: symmetric peer replication using opaque Loro update
  and snapshot payloads.
- `oll/plugin.proto`: the multiplexed host/plugin runtime stream.

Package installation, Git forge adapters, source builds, release-asset
selection, process spawning, and the plugin manifest are not wire protocols and
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

The node stage initially implements `GetStatus` and `Shutdown`. Typed methods
for replica, sync, and plugin management are added only in their respective
implementation stages. Future CLI arguments must not be tunneled as generic
strings to avoid extending this schema.

gRPC Server Reflection is a debug-build facility registered only on the Admin
UDS. It is compiled out of release builds. Production `TRACE` logs include
redacted request metadata, never complete protobuf requests or prohibited
content.

## Document invariants

`Revision` is scoped to the node returned with it. It is opaque to plugins and
must not be synthesized or parsed. A plugin that reads a document and later
modifies it includes that revision in `CommitDocumentsRequest.preconditions`.
oll checks every precondition immediately before opening one host-level commit.
If any check fails, oll returns `REVISION_CONFLICT` with
`RevisionConflictDetail` and applies no mutation. The catalog and documents are
separate LoroDocs, so local atomic visibility requires the replica write
coordinator and crash-recovery journal; it is not one Loro transaction.

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

CRDT commit and external side effects are not atomic. The runtime provides no
rollback, compensation, saga, or exactly-once guarantee for external systems.

## Replication stream

The gRPC client/server distinction describes only who opened the connection.
Both peers are replicas with identical read/write authority and run this state
machine:

1. Each side sends one `SyncHello` and verifies the one `ReplicaId`, exact schema
   hash, and Loro encoding fingerprint. A mismatch closes the stream; there is
   no downgrade.
2. Each side chooses parameters supported by the other and sends `SyncReady`.
3. Either side may advertise catalog/document object summaries and request
   missing updates for each object's LoroDoc.
4. A sender starts a transfer, sends numbered chunks, and completes it. The
   receiver verifies chunk count, size, and SHA-256 before importing it.
5. After a successful Loro import, the receiver sends `ReplicaTransferAck` and
   advertises its new summary when it changes.

The sender must not have more unacknowledged transfer bytes in flight than the
receiver granted through `FlowControl`. A Loro object snapshot is only a
transport fallback when retained update history cannot satisfy a delta request;
it is not an authoritative state replacement. It is distinct from the tar+zstd
oll replica snapshot documented in `docs/snapshot-format.md`. Importing
concurrent updates still follows Loro merge semantics.

Every `SyncEnvelope` carries a non-empty `correlation_id`. A delta request and
its transfer, import result, and acknowledgement reuse one ID across both peers
so their structured logs can be aggregated into one distributed operation.

Only the replication protocol carries Loro version vectors, frontiers, and
encoded update/snapshot bytes. Applications and plugins use document revisions.
The Loro encoding fingerprint is a build artifact covering the Loro export
implementation and oll's chosen encoding policy; it is not a negotiated API
version.

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

Plugins are trusted. This protocol intentionally has no permission grants,
signatures, marketplace identity, or document capability tokens. Session and
instance identifiers prevent accidental cross-wiring; they are not a sandbox or
authentication mechanism.

## Validation

From the repository root:

```sh
protoc -I proto \
  --include_imports \
  --descriptor_set_out=/tmp/oll-protocol.pb \
  $(find proto/oll -name '*.proto' -print | sort)
```
