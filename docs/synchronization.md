# Synchronization

## State model

Synchronization is peer-to-peer, multi-writer, offline-capable CRDT
replication. A node reached through a public address is not authoritative.

The unit advertised by the transport is a replica object, not the entire
replica as one Loro blob:

```text
ReplicaId
├── catalog object
├── DocumentId A
├── DocumentId B
├── ...
└── content-addressed binary blobs     # transferred by SHA-256, not as LoroDocs
```

Each catalog or document object has its own Loro version vector and frontier.
Deltas and snapshots are requested and imported per object. Binary bytes are
not replica objects with a Loro frontier: catalog metadata names their retained
SHA-256 hashes, and those immutable blobs transfer separately by hash.

Every endpoint also has one durable `NodeIdentity`: a UUID-v4 `NodeId` paired
one-to-one with its human-readable `NodeName`. The node declares this same pair
to every peer. Names are not chosen by receivers and are never derived from a
connect URL.

## Transport

`Replication.Synchronize` is one gRPC bidirectional stream. gRPC decides which
side opens the transport; after connection, both peers send the same message
types and have identical replication rights.

The stream handshake is:

1. both peers send `SyncHello` with their complete `NodeIdentity`;
2. both verify the exact protobuf schema hash and the one-to-one
   `NodeId`/`NodeName` binding against durable identities already known locally;
3. both select mutually supported compression/chunk parameters and send
   `SyncReady`;
4. either side advertises replica-object summaries;
5. either side requests missing object updates.

An identity collision closes the stream with `ALREADY_EXISTS`; schema and
replica mismatches also close it. There is no protocol downgrade,
receiver-local renaming, or fallback from `NodeName` to URL or `NodeId` as the
CLI selector.

## Transfer

For each object:

```text
RequestReplicaDelta
        |
        v
ReplicaTransferStart
ReplicaTransferChunk × N
ReplicaTransferComplete
        |
        v
hash/decompress/Loro import
        |
        v
ReplicaTransferAck
```

The normal payload is a Loro update batch from the receiver's advertised
version vector. A Loro snapshot is allowed when retained update history cannot
satisfy the request. A snapshot is a transport fallback, not an authoritative
replacement of concurrent local state.

The receiver verifies chunk count, size, and SHA-256 before Loro import. Partial
transfers are discarded. Reconnection starts from newly advertised object
summaries; CRDT idempotence makes already imported operations harmless.

There is no Loro encoding fingerprint in `SyncHello`. A received Loro update or
snapshot is compatible only when the actual Loro decoder and importer accept
it. Decode or import failure rejects that transfer as malformed or unsupported;
oll does not invent a second hash that claims to predict Loro compatibility.

Application-level flow-control credit limits unacknowledged transfer bytes in
addition to HTTP/2 flow control.

## Binary blobs

The catalog's CRDT state replicates each binary version's `BinaryId`, LWW stamp,
media type, byte count, and SHA-256 hash. Once that metadata arrives, a receiver
requests every referenced hash it does not already retain. The sync-stage wire
contract adds hash-addressed blob advertise/request/chunk/ack messages for this
purpose; it MUST NOT model a blob as a Loro object or give it a Loro peer,
frontier, or snapshot.

Blob transfer is streaming and checksum-verified. A catalog binary entry whose
winning blob has not arrived is retained as pending and is not materialized into
the working tree until the hash verifies. Catalog conflict resolution retains
concurrent binary records; the deterministic `(lamport, writer-node-id)` rule in
[replica.md](replica.md) selects the visible version after all available metadata
has imported.

## Catalog/document ordering

Catalog and document objects can arrive in either order.

- A document object arriving first is retained by `DocumentId` until catalog
  state references or tombstones it.
- A catalog document node arriving first is shown as pending until its document
  object arrives, and the node requests that object.
- A catalog binary entry arriving first is retained until its winning blob hash
  has been received and verified.
- A path is never used as a sync object key because concurrent moves change
  paths without changing document identity.

## Conflict behavior

Concurrent replica writes are imported normally and resolved by Loro. The sync
protocol does not run host revision preconditions and does not reject remote
operations because local state advanced.

`CatalogRevision` and `DocumentRevision` are host API guards for stale
application intent. Loro version vectors and frontiers are replication internals.
Neither should be substituted for the other.

## Required tests

Sync tests must cover:

- offline concurrent edits to one document;
- concurrent directory moves/renames in the catalog;
- document creation where catalog and content arrive in opposite orders;
- catalog binary metadata followed by a missing, duplicated, and verified blob
  transfer;
- deletion/tombstone propagation;
- interrupted and duplicated transfers;
- delta fallback to object snapshot;
- nodes configured as connect-only, listen-only, and both;
- stable `NodeIdentity` presentation and rejection of both name-to-ID and
  ID-to-name collisions;
- exact schema rejection and Loro decode/import failure handling.
