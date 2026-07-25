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
└── ...
```

Each object has its own Loro version vector and frontier. Deltas and snapshots
are requested and imported per object.

Every endpoint also has one durable `NodeIdentity`: an opaque `NodeId` paired
one-to-one with its human-readable `NodeName`. The node declares this same pair
to every peer. Names are not chosen by receivers and are never derived from a
connect URL.

## Transport

`Replication.Synchronize` is one gRPC bidirectional stream. gRPC decides which
side opens the transport; after connection, both peers send the same message
types and have identical replication rights.

The stream handshake is:

1. both peers send `SyncHello` with their complete `NodeIdentity`;
2. both verify the exact protobuf schema hash, Loro encoding fingerprint, and
   the one-to-one `NodeId`/`NodeName` binding against durable identities already
   known locally;
3. both select mutually supported compression/chunk parameters and send
   `SyncReady`;
4. either side advertises replica-object summaries;
5. either side requests missing object updates.

An identity collision closes the stream with `ALREADY_EXISTS`; fingerprint and
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

Application-level flow-control credit limits unacknowledged transfer bytes in
addition to HTTP/2 flow control.

## Catalog/document ordering

Catalog and document objects can arrive in either order.

- A document object arriving first is retained by `DocumentId` until catalog
  state references or tombstones it.
- A catalog document node arriving first is shown as pending until its document
  object arrives, and the node requests that object.
- A path is never used as a sync object key because concurrent moves change
  paths without changing document identity.

## Conflict behavior

Concurrent replica writes are imported normally and resolved by Loro. The sync
protocol does not run plugin-style `Revision` preconditions and does not reject
remote operations because local state advanced.

`Revision` is a host API guard for stale application intent. Loro version vectors
and frontiers are replication internals. Neither should be substituted for the
other.

## Required tests

Sync tests must cover:

- offline concurrent edits to one document;
- concurrent directory moves/renames in the catalog;
- document creation where catalog and content arrive in opposite orders;
- deletion/tombstone propagation;
- interrupted and duplicated transfers;
- delta fallback to object snapshot;
- nodes configured as connect-only, listen-only, and both;
- stable `NodeIdentity` presentation and rejection of both name-to-ID and
  ID-to-name collisions;
- exact schema and Loro fingerprint rejection.
