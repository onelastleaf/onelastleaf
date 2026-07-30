# Replica model

## Composition

An oll replica has two deliberately separate storage surfaces:

- the user-editable working tree at `replica_root`;
- the oll-managed SQL-backed replica store described in
  [replica-store.md](replica-store.md).

The logical replica is a document tree, not a single Loro document. Its CRDT
state consists of one catalog LoroDoc and zero or more text-document LoroDocs.
Binary file bytes are content-addressed store blobs, not Loro documents.

```text
replica
├── catalog LoroDoc       # tree, names, node identities, metadata, tombstones
├── document LoroDocs     # one per text document
└── binary blobs           # LWW bytes, addressed by SHA-256 through the catalog
```

The catalog uses LoroTree for parent-child topology. Its fixed root is not
movable, deletable, or renameable. A catalog entry has a user-facing UUID-v4
`CatalogNodeId` stored in its metadata; it is distinct from LoroTree's internal
node identifier and is the stable oll identifier for a directory, document, or
binary entry.

The catalog owns namespace state:

- node kind: directory, text document, or binary;
- parent-child relationships and entry names;
- a `DocumentId` for each text document;
- a `BinaryId`, media type, content encoding or binary LWW version records as
  appropriate for each file;
- deletion/tombstone state needed for replication.

## Catalog Loro schema

The catalog LoroDoc has three fixed named root containers. Their names and
roles are persistent format, not implementation-local aliases:

- `tree`: one `LoroTree` containing all non-root entries and their parent-child
  topology; its implicit root cannot be moved, deleted, or renamed;
- `catalog`: one `LoroMap` containing `format_version` and the UUID-v4
  `root_catalog_node_id` assigned when the replica is initialized;
- `entries`: one `LoroMap` keyed by canonical `CatalogNodeId`, containing the
  metadata record for every live or retained tombstoned entry.

Each non-root `tree` node carries its `CatalogNodeId` as metadata so topology
can be joined to `entries`; oll never exposes the LoroTree-internal node ID as a
catalog identity. Every entry record contains the exact UTF-8 name and kind.
A directory has no object ID. A text-document record additionally contains its
`DocumentId`, media type, encoding, and BOM state. A binary record additionally
contains its `BinaryId`, media type, and retained version records. Each binary
version is keyed by its `(lamport_clock, writer_node_id)` stamp and contains its
SHA-256 and byte size. Unknown format versions or a record whose fields do not
match its kind are store corruption errors.

LoroTree's CRDT result decides concurrent move and move/delete outcomes. The
host projects that converged result and does not emit a second "repair" move or
rename. Concurrent entries may therefore converge to the same visible parent
and name; the deterministic conflict projection below preserves all of them.
The catalog root's `CatalogNodeId` represents `/`, but the immutable implicit
LoroTree root itself is not inserted as an ordinary child entry.

Paths are derived from catalog state. The working tree is the user-facing
projection of those paths; no path is used as the persistent identity of a
document or binary object.

## Identity

All oll-level replica object IDs use canonical lower-case UUID-v4 strings.

| Identifier | Meaning | Preserved by `.ollsnap` |
| --- | --- | --- |
| `ReplicaId` | logical document tree | yes |
| `CatalogNodeId` | directory, document, or binary tree entry | yes through catalog |
| `DocumentId` | one text document LoroDoc | yes |
| `BinaryId` | one binary file identity | yes through catalog |
| `NodeIdentity` | deployment identity | no |
| `LoroPeerId` | local store's Loro actor | no |

Renaming or moving a visible text document changes catalog state only; its
`DocumentId` does not change when oll observes a reliable live rename. A binary
similarly retains its `BinaryId` across a reliable live move. A move made while
the daemon is offline has no reliable event identity and is intentionally
treated as deletion plus creation.

`LoroPeerId` is an oll-internal local-store model, not a document or plugin API
identifier. It is necessary for correct Loro operations but has no relation to
the node or replica UUIDs.

## Paths and collisions

Document/plugin APIs use canonical absolute UTF-8 `DocumentPath` values in the
replica namespace. Every managed relative working-tree segment therefore MUST
be non-empty UTF-8 and cannot be `.` or `..`. The local CLI starts from native
OS paths beneath `replica_root`; its Admin handler checks containment and
converts the path to that namespace. Native paths never cross into the
document/plugin API.

Catalog entries retain their UTF-8 display spelling, but collision detection
uses a portable sibling key: Unicode NFC normalization followed by Unicode full
case folding. This avoids a catalog that projects two ordinary names on Linux
but one colliding name on a default case-insensitive or normalization-aware
Darwin filesystem. A name that cannot become a valid managed `DocumentPath` is
never lossy-converted into one.

Catalog entries with distinct parents and distinct portable sibling keys have
their ordinary projected paths. Distinct catalog entries with the same parent
and portable sibling key are all projected with a conflict suffix, including
entries produced by concurrent creation:

```text
/notes/todo.md.conflict-<catalog-node-id>
```

This suffix is a deterministic display, filesystem-projection, and addressing
rule. It is not an additional catalog rename operation. Every member of the
same-key conflict receives a suffix, so no peer treats one concurrent entry as
the privileged original. If a generated candidate collides with another
candidate's portable key, oll appends that candidate's full `CatalogNodeId`
again until all projected sibling keys are unique. This rule also handles a
user-created name that resembles a conflict path.

Users resolve a conflict with an ordinary move, rename, or deletion. Once only
one original-name entry remains, it projects without the suffix.

## Text documents

A text document is an accepted text-encoded regular file and one LoroDoc. Its
user-visible file body is the fixed `content` LoroText. Its fixed `data`
LoroMap stores the abstract CRDT value model used by oll APIs; it is not written
into the text file.

Creating a document creates both named roots in the same document LoroDoc:
`content` is initialized from the UTF-8 API value and `data` is an empty map.
`ReplaceDocument` replaces only `content`; changing path, media type, encoding,
or other catalog metadata is a catalog mutation. Filesystem reconciliation also
changes only `content`, except that a detected encoding change updates the
catalog metadata in the same host-level commit.

Every abstract `CrdtObjectPath` is resolved beneath the fixed `data` root. The
empty object path names that root map. No document API operation may delete,
replace, or create either fixed root, and abstract CRDT operations never target
`content`. Conversely, text-body replacement and filesystem reconciliation do
not overwrite `data`.

The abstract model supports scalar values, text, map, list, movable list, tree,
and counter containers. It is translated to Loro internally and does not expose
Loro container IDs or methods. Text indexes count Unicode scalar values. A
scalar string is atomic; `CrdtText` is an editable sequence. A scalar number is
atomically replaced; `CrdtCounter` combines concurrent numeric increments.

`CreateDocument` and `ReplaceDocument` accept UTF-8 text at the document/plugin
wire boundary. A newly created API document starts with UTF-8 encoding and no
BOM. oll transcodes later body replacements into the working tree's
catalog-recorded encoding while materializing the file, using the
exact-representation rule in [replica-store.md](replica-store.md). A document
whose working-tree bytes stop qualifying as supported text becomes a deleted
document plus a newly created binary entry rather than a non-CRDT document.
The reverse classification change likewise replaces a binary with a new text
document. Either transition allocates a new `CatalogNodeId` and the new kind's
`DocumentId` or `BinaryId`; object identity never changes kind in place.

Requests are validated against the ordered mutation view before durable state
is changed. A path into a missing value, a container-kind mismatch, an invalid
text/list/tree index or range, an attempt to mutate a fixed root, invalid UTF-8
content, or use of a document operation on a binary returns `INVALID_ARGUMENT`
and rejects the complete host-level commit. Later mutations in one request see
the temporary results of earlier mutations, but validation failure publishes
none of them.

## Binary files

A binary is a regular working-tree file that is not a supported text document.
It has a catalog entry and `BinaryId`, but no LoroDoc. Its bytes are stored as a
SHA-256-addressed blob in the replica store.

Each binary write creates a retained catalog version record. oll compares
records by `(lamport_clock, writer_node_id)`: the larger clock wins, and the
larger canonical `NodeId` wins a clock tie. `BinaryId` identifies the file; it
does not break ties between versions of that same file.

The local Lamport clock is persisted in the store. A local binary write advances
it past every observed clock; receiving remote metadata advances the local
clock without changing the remote record's stamp. The current working-tree
binary is the content of the winning record. Binary content therefore follows
deterministic LWW behavior rather than CRDT merge behavior.

## Revisions and stale writes

Catalog and document state deliberately have separate opaque revision types:

```text
CatalogRevision  -> one CatalogNodeId: path, parent, name, kind, catalog metadata
DocumentRevision -> one DocumentId: content and CRDT containers
```

A read of `/notes/a.md` returns its `CatalogNodeId`, `CatalogRevision`,
`DocumentId`, and `DocumentRevision`. A body update guards the document pair; a
move, rename, or deletion guards the catalog pair; a caller that needs both
properties stable includes both explicit preconditions. Unrelated document
changes affect neither revision.

Preconditions name their target ID and revision explicitly. Existence checks
remain path-based because they describe an entry that does not yet have an ID.
The replica validates every precondition immediately before opening a
host-level commit. A mismatch returns `REVISION_CONFLICT` and applies none of
the requested mutations. Omitting a precondition is an explicit blind write.

## Commit boundary

One LoroDoc provides one CRDT transaction boundary. A host-level operation can
touch catalog state, one or more document LoroDocs, binary version records, and
operation history, so it cannot be one Loro transaction.

oll MUST provide these local semantics:

1. acquire the replica write coordinator;
2. validate every precondition before the first mutation;
3. apply the relevant Loro transactions and prepare binary versions on an
   unpublished candidate view;
4. persist all affected replica-store rows, high-level operation history, and
   any required projection generation in one SQL transaction;
5. publish the new daemon-visible state only after that transaction succeeds;
6. materialize the resulting working-tree projection and retain a durable
   recovery marker if that work is incomplete;
7. release the coordinator.

This is local atomic visibility, not distributed atomic delivery. The
unpublished candidate view is discarded if validation or the SQL transaction
fails, so a failed commit cannot leave mutated live Loro handles behind. The
working tree is recoverable projection work rather than an excuse to replay an
interrupted mutation from ambiguous filesystem state. The precise startup rule
is in [replica-store.md](replica-store.md).

## Deletion and retention

Deleting a visible entry removes it from the live catalog namespace but does not
immediately prove that its Loro history or binary-version metadata is globally
unnecessary. The store retains catalog tombstones, document objects, and binary
version/blob references needed for future correctness.

Garbage collection requires a separately designed causal-stability policy.
Until then, snapshot export includes every retained document object and every
blob referenced by a retained binary version, not only currently visible paths.

## Required tests

Replica-model tests cover UUID-v4 validation, fixed catalog/document roots,
concurrent catalog move/rename/delete outcomes, and same-name projection where
every conflicting entry receives its full deterministic suffix. They also cover
ordered multi-mutation validation, separate catalog/document revision conflicts,
text-to-binary and binary-to-text replacement, binary LWW ordering by
`(lamport_clock, writer_node_id)`, and rollback of unpublished candidate state
when any part of a host-level commit fails.

## Single-replica invariant

The replica service is constructed exactly once by the node. It must not expose
mount, switch, list-replicas, or attach-second-replica operations. Snapshot
import replaces this one slot as described in
[snapshot-format.md](snapshot-format.md); it may deliberately replace its
`ReplicaId` after the required user confirmations.
