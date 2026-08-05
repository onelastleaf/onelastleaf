# Synchronization

## Security model

Synchronization is peer-to-peer, multi-writer, and offline-capable. A node
reached through a public address is not authoritative. Membership is proved by
one long-lived symmetric network key shared by every node in the same oll sync
network. A peer that completes the Noise handshake with that key is trusted to
read and modify the complete replica.

This is deliberately a small threat model. The protocol has no certificate
chain, CA, domain validation, per-node signing key, or separate `NetworkId`.
Possession of the shared key permits a participant to impersonate any
`NodeIdentity`; the one-to-one identity-binding checks below detect accidental
collisions, not a malicious key holder. Removing one compromised node requires
rotating the key on every remaining node.

The network key is configuration-only. Its bytes never enter protobuf, the
network, command arguments, or logs. The exact configuration and derivation
rules are in [configuration.md](configuration.md).

## Replica objects

The unit advertised by the protocol is a replica object, not the entire replica
as one Loro blob:

```text
ReplicaId
├── catalog object
├── DocumentId A
├── DocumentId B
├── ...
└── content-addressed binary blobs     # transferred by SHA-256, not as LoroDocs
```

Each catalog or document object has its own Loro version vector and frontier.
Normal synchronization transfers update batches for each object. Bootstrap
transfers the complete retained update history for each object as an ordinary
update batch from an empty version vector. Neither path transfers a Loro
snapshot or an oll `.ollsnap` archive.

Binary bytes have no Loro frontier. Catalog metadata names their retained
SHA-256 hashes, and immutable blob files transfer separately by hash.

## Endpoints and connection management

`listen` is an operating-system bind endpoint such as `0.0.0.0:17384` or
`[::]:17384`. `connect` entries are remote targets such as
`oll://203.0.113.10:17384`, `oll://[2001:db8::10]:17384`, or
`oll://node.example:17384`. Persisted values and runtime overrides require an
explicit nonzero port. As an initialization convenience only,
`oll init --connect` defaults a missing scheme to `oll://` and a missing port
to `17384`, then writes the complete target into `config.lua`. A connect URL
has no user information, query, fragment, or path other than the URI parser's
empty/root path.

The daemon binds its configured listener before Admin readiness. Failure to
bind `listen` is a startup failure. Outbound failures are nonfatal and use
bounded exponential backoff with jitter. Connect-only, listen-only, and mixed
nodes have identical replica rights.

Configured outbound targets and authenticated inbound peers feed one durable
peer directory. Learned bindings are stored outside replica generations and
are not exported in `.ollsnap`. A known `NodeId` presenting another `NodeName`,
or a known name presenting another ID, is rejected. A configured target is
associated with the identity it authenticated as; it is not named by its URL.

## Transport

Sync does not use gRPC, HTTP, WebSocket, TLS, or certificates. Its stack is:

```text
TCP
└── Noise_NNpsk0_25519_ChaChaPoly_BLAKE2s
    └── u16-length oll transport frames
        └── prost-encoded SyncEnvelope
```

`Noise_NNpsk0_25519_ChaChaPoly_BLAKE2s` is used exactly. Version 1 does not
perform Noise rekeying. A connection closes before either transport cipher
nonce can be exhausted.

### Preface and Noise handshake

The initiator first writes the exact cleartext preface `b"OLLSYNC\x01"`. The
same bytes are the Noise prologue. Version 1 accepts only an exact match and
does not negotiate or downgrade. The initiator SHOULD coalesce the preface and
first Noise message into one TCP write, but the receiver parses them as
distinct protocol elements.

The Noise handshake pattern is:

```text
initiator -> responder: psk, e
initiator <- responder: e, ee
```

Both handshake payloads are empty. Each encoded Noise handshake message is an
oll transport frame consisting of a two-byte unsigned big-endian length and
that many Noise message bytes. Handshake-message length is limited to 1024
bytes and is checked before allocation. A wrong preface, timeout, early EOF,
PSK mismatch, or handshake AEAD failure causes an immediate close without an
application error response. The local node records only a redacted structured
warning and never tells an unauthenticated peer that its key was wrong.

TCP establishment, preface parsing, the Noise exchange, `SyncHello`, and
`SyncReady` share one absolute deadline 10 seconds after TCP connection
establishment. Advancing to another handshake step does not reset that deadline.

### Encrypted transport frames

After the handshake both sides enter Noise stateful transport mode. Every later
oll transport frame is:

```text
u16 big-endian ciphertext_length
Noise transport ciphertext
```

The decrypted plaintext is exactly one ordinary prost encoding of
`SyncEnvelope`; it is not a second prost length-delimited record. The visible
length leaks frame size. It is validated before allocation and may not exceed
65535 bytes. A Noise transport ciphertext includes a 16-byte authentication
tag, so its plaintext cannot exceed 65519 bytes.

`SyncHello.max_chunk_bytes` applies to the `data` field of transfer chunks. The
version-1 implementation advertises at most 61440 bytes and chooses the smaller
valid peer value, leaving room for the protobuf envelope and Noise tag. Control
and inventory messages are batched so their complete encrypted frame also fits
the 65535-byte limit. An invalid zero length, oversized length, decode failure,
or out-of-state message is a protocol violation and closes the authenticated
session with `SyncClose` when a close frame can still be sent safely.

TCP provides wire flow control. There is no application credit or flow-control
message. Implementations still stream received chunks directly into private
staging files and use bounded producer queues so they do not defeat TCP
backpressure by buffering an unbounded amount in userspace.

### Connection liveness

Every established TCP stream enables operating-system keepalive as a secondary
failure detector. On supported Unix platforms the initial policy is 60 seconds
idle, 10 seconds between probes, and three unanswered probes. Linux additionally
uses a 120-second `TCP_USER_TIMEOUT` for data that remains unacknowledged or
unsent. Failure to apply one of these optional socket tunings records
`sync_tcp_liveness_configuration_failed` with only the operating-system error
kind; it does not reject an otherwise usable connection because application
liveness remains authoritative.

An authenticated `ready` or `waiting_for_replica` session that has carried no
valid envelope in either direction for 30 seconds sends an encrypted `SyncPing`.
It must receive the matching `SyncPong` within 10 seconds. A valid envelope in
either direction resets the idle interval. Either endpoint may initiate this
probe; simultaneous probes are valid. Heartbeat success is `TRACE` only. A
missed heartbeat records `sync_session_liveness_failed` at `WARN`, fails pending
session work as unavailable, removes that connection from the active-session
registry, and closes the socket so an outbound owner can reconnect. Heartbeats
reuse the existing ping/pong messages and do not add another negotiation or
flow-control protocol.

An active normal or bootstrap round suspends the idle-heartbeat timer. Ordinary
round traffic proves liveness. During a prolonged local operation that cannot
be safely cancelled after authoritative SQL activation begins, the owner may
send the same encrypted Ping/Pong as a round-progress keepalive; round receivers
consume it transparently without changing message order or commit meaning. A
sent round keepalive remains matchable until its Pong is consumed or the session
ends; the ordinary phase deadline must not turn a queued valid Pong into an
unknown message after a long local commit.
Local candidate validation, inactive-generation construction, activation, and
projection are one cancellation-safe commit path rather than wire progress
phases. A duplicate-session or identity-change request that arrives during that
path is observed after the finite round reaches its terminal commit/reject
boundary; it must not drop a future whose SQL activation may already have
committed. Once a finite round has begun, daemon shutdown stops admitting new
work but lets that round reach its terminal boundary until the node's single
absolute deadline. Only the node shutdown coordinator may abort the connection
task at that deadline, after which ordinary store/projection recovery is
authoritative.

## Application handshake

Immediately after Noise completes, both peers independently send one encrypted
`SyncHello`. It carries the complete `NodeIdentity`, exact protobuf descriptor
hash, maximum chunk-data size, and exactly one local replica state:

- `replica_id` when a complete active replica exists;
- `no_local_replica` when the local slot is uninitialized.

There is no compression negotiation or session nonce. After validation both
peers send the same `SyncReady`, containing the selected chunk size and the
selected `ReplicaId` when one exists:

- equal nonempty IDs select normal synchronization;
- one initialized peer and one uninitialized peer select bootstrap from the
  initialized source to the uninitialized receiver;
- two uninitialized peers omit `session_replica_id` and enter the authenticated
  waiting state described below;
- two different IDs close with `REPLICA_MISMATCH`.

Schema mismatch, invalid chunk limits, identity collision, and self-connection
also close the session. Application close reasons are sent only after Noise has
authenticated the shared network key. `SyncClose` includes at least normal,
shutdown, protocol, schema, identity-collision, self-connection,
duplicate-session, replica-mismatch, replica-available, bootstrap-in-progress,
resource-exhausted, and internal-error codes. A received close reason is a
diagnostic and never changes local authority rules.

After identities are known, duplicate sessions are resolved without a random
session field. The connection initiated by the lexicographically smaller
canonical `NodeId` is preferred. If more than one connection has that same
preferred direction, both endpoints keep the one with the lexicographically
smallest Noise handshake hash and close the rest as duplicates. Both endpoints
can compute the same choice.

### Waiting for the first replica

When both authenticated peers are uninitialized, the completed connection is a
long-lived `waiting_for_replica` session. It participates in identity binding,
duplicate-session arbitration, status, ping, shutdown, and identity-epoch
invalidation, but it admits no synchronization round. It does not close as a
failure, enter reconnect backoff, or poll SQL or the working tree. A manual
`oll sync` receives one peer-local `FAILED_PRECONDITION` result while the
background session remains connected; the command does not wait indefinitely
for unrelated future filesystem work.

`ReplicaRuntime` publishes a status transition only after an active replica has
been atomically committed to the replica store and installed as the in-memory
active state. A waiting session subscribes to that authoritative notification.
When either endpoint first becomes initialized, it sends the authenticated
`SyncClose(REPLICA_AVAILABLE)` and ends the waiting connection normally. The
configured outbound owner reconnects immediately without failure backoff, and
the new `SyncHello` pair selects normal synchronization, bootstrap, or
`REPLICA_MISMATCH` using the ordinary rules above. No session changes its
selected `ReplicaId` in place and version 1 has no second mid-session replica
negotiation protocol.

If both endpoints create different replicas concurrently, the next handshake
rejects the mismatch and neither replica is overwritten. If a local
initialization races with remote bootstrap admission, the existing durable
bootstrap claim, commit guard, and active-generation compare-and-swap determine
which transition linearizes. The working-tree merge policy applies only when
bootstrap acquired that authority before local replica activation.

"Long-lived" means that a connection is retained while its negotiated state is
stable; it does not promise that one TCP file descriptor spans the complete
daemon lifetime. Network failure, identity changes, replica availability,
bootstrap completion, duplicate arbitration, and shutdown may deliberately
replace or close a connection.

## Finite synchronization rounds

A ready session may carry independent rounds in either direction. A node that
wants one bidirectional finite synchronization sends `SyncRoundRequest`; the
peer answers by sourcing the first round, and the requester sources the reverse
round after committing the first. If both nodes request at the same time, the
request sent by the lexicographically smaller canonical `NodeId` wins and the
other local request is coalesced into that same bidirectional operation. This
request arbitration prevents two inventories from deadlocking or being mistaken
for responses; it is not application flow control or replica authority.

A normal or bootstrap round has no fixed total-duration limit. Each transport
send, each wait for the next expected envelope, and the read-only starting
inventory capture instead has a 120-second no-progress deadline. Completing a
valid protocol step starts a fresh deadline, so a transfer that continues to
produce frames may run for hours. The deadline is not applied by cancelling a
store operation after an authoritative activation has begun.

When a progress deadline expires, the local node records
`sync_round_progress_timeout` with the connection, peer, correlation,
`failure_stage`, `failure_source`, and `idle_ms`. Inventory capture uses
`failure_source = local_store`; encrypted frame send/receive uses `transport`.
The pending round returns unavailable, the entire session is removed and
closed, and a configured outbound owner enters its normal reconnect path. A
remaining `oll sync -n` attempt waits for a newly authenticated session rather
than reusing the failed socket. Partial staging is discarded or recovered
through the existing candidate rules.

For each accepted request, the first source starts a round, captures a coherent
starting inventory under a short replica write barrier, and then releases the
barrier. The inventory includes every
retained catalog/document object summary and every blob referenced by the
catalog observed at that point; it is not a frozen copy of all Loro payloads.

When the source later handles `RequestReplicaUpdates`, it exports from the
object's current state, so the transfer includes every update available when
that response is prepared rather than being artificially limited to the
starting inventory's version vector. `ReplicaTransferStart` describes the
actual resulting version vector of that payload. Each individual export is
finite, but a write concurrent with the round may therefore be included in the
current transfer. If a newer catalog payload introduces an object or blob that
was not present in the starting inventory, complete-candidate validation cannot
publish a partial graph: the receiver rejects that candidate and the next round
advertises a fresh inventory containing the new references.

The source sends numbered inventory batches followed by an inventory-complete
message with exact object, blob, and batch counts. The receiver requests every
missing update range and blob. Transfers may be interleaved and arrive in any
object/file order, but chunks within one transfer are numbered and complete
exactly once.

For each object transfer:

```text
RequestReplicaUpdates
        |
        v
ReplicaTransferStart
ReplicaTransferChunk x N
ReplicaTransferComplete
        |
        v
size/hash/Loro decode/import into private round candidate
        |
        v
ReplicaTransferAck or ReplicaTransferReject
```

Blob transfers use corresponding hash-addressed start/chunk/complete/ack/reject
messages. A transfer acknowledgement means that exact payload was verified and
staged for the named round; it does not claim that active replica state changed.
Malformed chunks, size/hash contradictions, Loro decode/import failure, and
unknown objects receive a typed transfer rejection. Partial staging is discarded
when its session or round ends.

The receiver starts from a private candidate copy of its active generation,
imports every verified update, obtains every newly referenced object and blob,
and validates the complete catalog/document/blob graph and business metadata.
It then commits the candidate with one SQL transaction that compares the active
generation with the round's base and switches it only if unchanged. A concurrent
local or remote commit makes that comparison fail; the candidate is discarded
and a later round retries from the new active generation. Active state therefore
never exposes a catalog entry whose retained document or blob is missing. A
crash after an inactive normal-round generation is built but before its
compare-and-swap leaves the old generation active; startup discards that
unreferenced candidate and any blob bytes referenced by no retained generation.

`SyncRoundCommitted` is sent only after the candidate transaction succeeds. A
manual `oll sync` succeeds for a peer when one finite round in both directions
has committed or was already satisfied. An update included while preparing a
requested object transfer belongs to that round. Writes that occur after an
individual payload is prepared, and newly referenced objects that require a
fresh inventory, are handled by a later round; they do not prolong one command
indefinitely.

There is no snapshot fallback. Because the initial retention policy keeps all
required Loro history, a sender exports an update batch from the requested
version vector. A future history-compaction feature must define a new compatible
recovery mechanism before discarding required updates; it may not silently turn
an oll snapshot or Loro snapshot into version-1 sync traffic.

## Bootstrap of an uninitialized receiver

Bootstrap uses the same object and blob chunk messages, not `.ollsnap`. The
initialized peer is the bootstrap source and the uninitialized peer is the
bootstrap receiver. At most one authenticated source may bootstrap a receiver
at a time. The first session to acquire the local durable bootstrap claim wins;
another receives `SyncClose(BOOTSTRAP_IN_PROGRESS)`.

While that claim is held, the replica coordinator stops admitting new local,
filesystem, and remote commits. The working tree remains directly editable and
the watcher continues recording final-state triggers, but those events cannot
create a local `ReplicaId` or active generation. Source writes after its captured
inventory are outside this bootstrap and arrive in a later normal round.

The source advertises a complete retained inventory and sends each catalog or
document's full update history from an empty version vector plus every referenced
blob. Files may be transferred in arbitrary order. The receiver stores all
payloads in private staging associated with the bootstrap claim; no transfer
modifies active store rows. Each per-transfer ACK confirms only verified staging.

After all advertised transfers are staged, the receiver builds an inactive SQL
generation and performs complete structural, Loro, catalog-reference, encoding,
byte-size, and blob validation. It also reconciles the working tree state queued
during bootstrap into that candidate using this product policy:

- a local path absent from the received portable catalog namespace is imported
  into the candidate, together with local-only descendants whose parents remain
  directories;
- if the received catalog already occupies or conflicts with the same portable
  path, the received entry wins and the local item is discarded from candidate
  import; a remote non-directory parent also discards the local subtree.

The candidate uses the source `ReplicaId` and a fresh local `LoroPeerId` absent
from every received version vector. The receiver prepares the authoritative
`replica.json` identity transition described in
[replica-store.md](replica-store.md), then performs one SQL compare-and-swap from
`active_generation = NULL` to the complete candidate. That transaction commit is
the only bootstrap linearization point: before it the deployment is
uninitialized; after it a complete validated replica is active. The same
transaction sets whole-tree projection recovery, after which the working tree
is rebuilt from the active generation before normal watcher imports resume.

A crash before the compare-and-swap leaves `active_generation` null; startup
removes the candidate, staging, bootstrap claim, and an exactly matching prepared
identity file. A crash after it loads the active generation and completes
projection. Bootstrap envelopes, staging, validation, activation, and recovery
share the correlation ID inherited from the session operation that acquired the
claim.

## Ping, Admin requests, and status

The sync stage adds only two Admin RPCs:

- `SynchronizePeers`, used by `oll sync`, starts an immediate finite round for
  every configured peer or one learned `NodeName` and waits for its result;
- `PingPeer`, used by `oll ping <node-name>`, measures an authenticated
  protocol ping/pong to that learned identity.

`oll psk` is a pure local CSPRNG command and `oll sync --log` is a local log
viewer; neither is an Admin RPC. Admin connection establishment retains its
short deadline. `PingPeer` is a short RPC. `SynchronizePeers` has no fixed
10-second request deadline because connection attempts and valid transfers may
take longer; the transport and round liveness deadlines above still prevent a
silent connection from holding it forever. Its `total_attempts` counts the
initial attempt, is greater than zero, and is enforced by the daemon. Cancelling
the Admin waiter does not tear down a durable background peer session.

Status lists configured outbound targets even before authentication and also
authenticated inbound-only peers. A connect target is optional on an inbound
row; connection direction, state, and learned `NodeIdentity` are explicit. An
authenticated peer with neither replica is reported as
`waiting_for_replica`, not `ready`, `backoff`, or failed.

## Shutdown and observability

The sync listener and all connection tasks belong to the node's existing
shutdown coordinator. When stopping begins, the daemon closes the listener,
admits no new session or round, sends best-effort authenticated `SyncClose` frames,
and drains or aborts connection tasks under the same absolute 10-second node
deadline. Sync cannot extend deployment-lock ownership with another deadline.

Every envelope has a nonempty correlation ID. One normal transfer keeps the
request's ID through chunks, staging, commit/reject, and ACK. A complete
bootstrap uses one inherited ID across every transfer and activation step. A
background connection or inbound operation without an external ID creates one
at its first local boundary. Network keys, handshake material, raw frames, Loro
updates, and blobs are never logged.

Normal-round phase visibility includes `sync_round_request_sent`,
`sync_round_request_received`, `sync_inventory_capture_started`, and
`sync_inventory_capture_completed`. The first two prove that the local channel
respectively wrote or received the wire request; `sync_round_started` alone
continues to mean that the Admin operation began. Liveness failures use
`sync_session_liveness_failed` or `sync_round_progress_timeout`, never a generic
protocol bucket. Heartbeat successes remain `TRACE` to avoid idle log noise.

`sync_session_failed` is an actionable local diagnostic, not one generic
`protocol` bucket. It always records:

- `failure_stage`: `transport_handshake`, `sync_hello`, or `sync_ready`;
- `failure_source`: `transport`, `local_validation`, or `remote_close`;
- a stable `error_code` naming the specific locally known cause.

Outbound failures also retain their sanitized `connect_target`, allowing the
foreground view and structured logs to distinguish equivalent failures from
different configured peers.

Transport error codes are `transport_io`, `handshake_deadline_exceeded`,
`invalid_preface`, `invalid_frame_length`, `noise_handshake_failed`,
`noise_transport_authentication_failed`, `envelope_too_large`, and
`invalid_protobuf_envelope`. `transport_io` additionally records the
non-sensitive operating-system I/O error kind.

Application-handshake validation uses specific codes rather than collapsing
them into `protocol_violation`: `hello_reply_to_present`,
`expected_sync_hello`, `schema_mismatch`, `invalid_max_chunk_bytes`,
`invalid_node_identity`, `self_connection`, `invalid_replica_id`,
`missing_replica_state`, `replica_mismatch`,
`ready_reply_to_present`, `bootstrap_correlation_mismatch`,
`expected_sync_ready`, and `ready_negotiation_mismatch`. Channel invariant
failures likewise retain `empty_local_correlation_id`, `message_id_exhausted`,
or `invalid_envelope_metadata`. A locally generated failure may add its static,
host-authored `message` and the normalized `sync_close_code` sent to the peer.

For an authenticated remote `SyncClose`, `error_code` and `sync_close_code`
contain its normalized enum reason, such as `schema_mismatch`,
`replica_mismatch`, or `bootstrap_in_progress`; `failure_source` makes clear
that the reason came from the peer. The peer-controlled free-text close message
is never copied into local logs. Before authentication, the local log may say
that the Noise handshake failed, but it does not claim that the PSK differed:
wrong key bytes, malformed Noise traffic, and authentication failure remain
indistinguishable and no key bytes, derived PSK, hash, or handshake material are
recorded.

## Required tests

Sync tests cover:

- exact preface/prologue, one absolute handshake deadline, wrong-PSK silent
  close, and frame limits checked before allocation;
- exact schema rejection, self/identity collision rejection, simultaneous
  duplicate-session arbitration, and no protocol downgrade;
- connect-only, listen-only, and mixed topologies using explicit `oll://` ports;
- offline concurrent document edits and catalog move/rename/delete convergence;
- coherent normal-round activation when catalog and content transfers arrive in
  either order, including active-generation CAS restart after a concurrent local
  commit;
- missing, duplicated, interrupted, reordered, hash-invalid, and decode-invalid
  object/blob transfers without exposing partial active state;
- bootstrap claim exclusion, arbitrary transfer order, different-replica
  rejection, local-only working-tree merge, remote path-win behavior, and
  crashes immediately before and after atomic activation;
- both-uninitialized peers retaining one authenticated waiting session without
  failure/backoff churn, then bootstrapping when either topology side creates
  its first replica;
- idle ready/waiting sessions exchanging encrypted heartbeats, a silent peer
  being removed after the heartbeat deadline, and TCP keepalive options being
  applied where the platform exposes them;
- a silent peer timing out each finite-round wire phase, unregistering the
  failed session, reconnecting before a remaining attempt, and a continuously
  progressing transfer exceeding 120 seconds in total without failure;
- a stalled read-only inventory capture timing out without mutating active
  replica state;
- concurrent first-replica creation selecting normal sync for equal IDs or
  rejecting different IDs without overwriting either active replica;
- fresh bootstrap `LoroPeerId` selection and `replica.json` recovery;
- bounded userspace buffering that relies on TCP backpressure without a wire
  credit protocol;
- inherited correlation through a normal transfer and an entire bootstrap;
- stable, cause-specific session-failure diagnostics, including redaction of a
  peer-controlled close message and all network-key material;
- sent/received round-request, inventory-capture, heartbeat-failure, and
  progress-timeout events retaining the operation correlation and connection
  identity;
- shutdown draining an in-flight finite round until its terminal boundary while
  retaining the node's absolute hard-stop deadline;
- listener/session shutdown under the node's single absolute deadline.
