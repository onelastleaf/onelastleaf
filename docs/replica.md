# Replica model

## Composition

An oll replica is a document tree, not a single Loro document. It consists of
one catalog `LoroDoc` and zero or more document `LoroDoc` objects.

The catalog is responsible for namespace state:

- stable tree-node identity;
- directory/document kind;
- parent-child relationships;
- catalog entry names;
- the `DocumentId` referenced by each document node;
- deletion/tombstone state needed for replication.

Each document `LoroDoc` stores that document's content and document-local CRDT
containers. Paths are resolved through the catalog.

## Identity

`ReplicaId`, `NodeIdentity`, and `DocumentId` have different lifetimes:

| Identifier | Meaning | Preserved by snapshot |
|---|---|---|
| `ReplicaId` | Logical document tree | Yes |
| `NodeIdentity` | One-to-one `NodeId`/`NodeName` deployment identity | No when initializing another deployment |
| `DocumentId` | Stable document object | Yes |

Renaming or moving a document changes only catalog state. Synchronization and
snapshot storage address its `LoroDoc` by `DocumentId`, never by path.

## Document API

Plugins access documents by canonical absolute paths because paths are ergonomic
for user workflows. Read responses include stable metadata and an opaque
document `Revision`.

The API exposes two projections:

- complete document content;
- oll's abstract CRDT value model.

The abstract model supports scalar values, text, map, list, movable list, tree,
and counter containers. It is translated to Loro internally and does not expose
Loro container IDs or methods.

Text indexes count Unicode scalar values. A scalar string is an atomic value;
`CrdtText` is an editable sequence. A scalar number is atomically replaced;
`CrdtCounter` combines concurrent numeric increments.

## Revisions and stale writes

`Revision` is scoped to one catalog node or document object. It is not a global
replica version and must not change merely because an unrelated document was
edited.

A long-running plugin includes the revision it previously read as a commit
precondition. The replica checks all preconditions immediately before applying
the host-level commit. A mismatch returns `REVISION_CONFLICT` and applies none
of the requested mutations.

Omitting a precondition is an explicit blind write. Loro can still merge that
write, but oll does not claim that the plugin's stale application intent was
preserved.

## Commit boundary

One `LoroDoc` provides one CRDT transaction boundary. A host-level operation may
touch the catalog and one or more document objects, so it cannot become one Loro
transaction.

oll MUST provide these local semantics:

1. acquire the replica write coordinator;
2. validate every precondition before the first mutation;
3. record enough durable intent to recover from a process crash;
4. apply per-object Loro transactions;
5. publish the new local visible state only after all local object writes
   succeed;
6. release the coordinator.

This is local atomic visibility, not distributed atomic delivery. Remote nodes
may receive the catalog and document object updates in different stream frames.
A catalog entry whose document object has not arrived is a pending object and
must trigger synchronization; it must not be treated as permanent corruption.

## Deletion and retention

Deleting a visible document removes it from the live catalog namespace but does
not immediately prove that its CRDT history is globally unnecessary. The local
store retains catalog tombstones and any document objects required for future
merge correctness.

Garbage collection requires a separately designed causal-stability policy. Until
that exists, snapshot export includes every retained document object, including
tombstoned objects, not only currently visible paths.

## Single-replica invariant

The replica service is constructed exactly once by the node. It must not expose
mount, switch, list-replicas, or attach-second-replica operations. Snapshot
import targets this one replica slot as described in
[snapshot-format.md](snapshot-format.md).
