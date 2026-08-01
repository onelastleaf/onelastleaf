# Replica store and working tree

## Two storage boundaries

The configured `replica_root` is the user's editable working tree. It contains
ordinary directories and files; oll does not put catalog metadata, Loro data,
operation journals, or hidden control files beneath it. A user may edit, add,
move, rename, and remove its entries with an editor or file manager.

The configured `replica_store` is oll-managed durable state. It contains the
catalog and document CRDT state, replica metadata, content-addressed binary
blobs, high-level operation records, and recovery state. Both locations are
owned by the deployment user, but normal user edits belong in the working tree.
Manual edits to the store are supported only in the sense that oll trusts its
user: malformed or contradictory state is reported as a store error and is not
silently repaired.

The working tree is an editable projection of the logical replica. The store is
the recovery authority after a daemon-owned mutation, snapshot import, or
crash. A filesystem edit becomes a new local replica mutation only after the
watcher has read and reconciled it.

## Store backends

`config.lua` selects exactly one SQL backend:

```lua
replica_store = {
    driver = "sqlite",
    path = "/home/alice/.local/share/oll/stores/<node-id>/replica.sqlite3",
}
```

or:

```lua
replica_store = {
    driver = "postgres",
    url = oll.getenv("OLL_POSTGRES_URL"),
}
```

SQLite requires `path` and forbids `url`. PostgreSQL requires `url` and
forbids `path`. The two backends implement the same logical store; the first
implementation does not make application-visible behavior depend on a
database-specific feature.

A PostgreSQL database or schema belongs to exactly one oll deployment. It is
not a shared multi-daemon replica backend. Pointing two deployments at the same
store violates the one-daemon/one-replica invariant.

`oll init` generates the new `NodeIdentity` in memory before writing either
initialization file. When the user does not supply a store location, it writes
an explicit SQLite path beneath the platform data directory:

```text
<platform-data-dir>/oll/stores/<generated-node-id>/replica.sqlite3
```

The generated path is persisted in `config.lua`; later edits to `node.json` do
not move it. `init` may create its parent directories but does not create a
database, a `ReplicaId`, or a catalog.

## Logical store contents

The physical SQL schema is private, but every backend MUST durably represent:

- initialization state, the active `ReplicaId`, and the local `LoroPeerId`;
- the catalog LoroDoc and every retained document LoroDoc;
- the monotonic local Lamport clock used for binary versions;
- content-addressed binary blobs keyed by lower-case SHA-256;
- high-level local operation records used by `oll replica ops`;
- working-tree projection generations and durable recovery records, including
  a whole-tree `projection_pending` marker for snapshot replacement.

The blob namespace is logical rather than a required directory layout. A
backend streams a blob by SHA-256; SQLite and PostgreSQL may store those bytes
in their own native representation. The `.ollsnap` archive uses
`blobs/<sha256>` as its independent, portable layout.

All catalog, document, binary-version, operation-record, and recovery-marker
changes for one host-level mutation are committed in one SQL transaction. This
is the durable host-level commit boundary. It does not make remote delivery or
working-tree materialization part of one distributed transaction.

## Initialization state

The replica slot has three externally observable states:

| State | Store metadata | Working tree | Meaning |
| --- | --- | --- | --- |
| Uninitialized | no active replica metadata | empty or non-empty | no `ReplicaId` exists yet |
| Initialized, empty | active metadata and catalog | no visible entries | a stable empty replica |
| Initialized, populated | active metadata and catalog | one or more visible entries | normal operation |

`oll init` creates only the uninitialized slot. On daemon startup, oll opens
the configured store before serving Admin requests and applies this rule:

1. no active metadata and an empty working tree: remain uninitialized;
2. no active metadata and one or more supported working-tree entries: perform
   the initial scan, create a UUID-v4 `ReplicaId`, create the catalog, and
   import those entries as the first local state;
3. active metadata: load the existing replica.

When rule 1 leaves the slot uninitialized, the daemon still starts the recursive
watcher. The first later reconciliation that finds one or more supported
working-tree entries performs the same initialization as rule 2. Under the
replica write coordinator, one SQL transaction creates the active generation,
a UUID-v4 `ReplicaId`, the catalog and its fixed root, a fresh local
`LoroPeerId`, and the first imported entries. No Admin client can observe a
catalog without its `ReplicaId` or a first document without its catalog entry.
If that transaction fails, the slot remains uninitialized and the filesystem
entries remain untouched for a later reconciliation.

An uninitialized deployment reports that state through status. It cannot sync,
export, inspect a document, or list document operations; those operations fail
with `FAILED_PRECONDITION`. Snapshot import is allowed and initializes the slot
directly without first creating a throwaway local replica.

## Local Loro identity

Each initialized local store owns one `u64` `LoroPeerId`, generated from the
operating system CSPRNG in `0..u64::MAX`. Loro 1.13.7 rejects `u64::MAX` as
`InvalidPeerID` because that value is reserved for its root identity, so oll
redraws that value. The peer ID is persisted in the same transaction that
initializes the active replica and before the first local catalog or document
Loro commit. The catalog and every locally edited document use that peer
identity for their local Loro operations. It is neither a `NodeId` nor a
`ReplicaId`:

- one node can discard one replica and create another, so its peer identity
  changes;
- multiple nodes may host the same logical `ReplicaId`, so their peer
  identities differ.

On restart oll reloads the persisted value; it never silently generates a new
actor for an existing active generation. Snapshot import does not copy an
exporting node's peer identity. After decoding all imported Loro objects, the
importer draws a fresh value that does not occur in any imported catalog or
document version vector, retrying on collision, and persists it with the new
active generation before any local post-import commit. The peer identity is
not exposed through the document or plugin API.

## Working-tree reconciliation

After store recovery, the daemon registers a recursive `notify` watcher through
`notify-debouncer-full` before it begins the complete startup scan. Events that
arrive during the scan are queued and reconciled afterward against final
filesystem state. Registering first closes the otherwise missed-change window
between "scan finished" and "watcher started"; duplicate observations are
harmless because reconciliation is idempotent. oll does not poll the working
tree. Filesystem events are triggers to reconcile observed state, not trusted
descriptions of what happened.

Only directories and regular files with UTF-8 relative path segments participate.
oll never follows symlinks and does not import devices, sockets, FIFOs, or a
path that cannot become the canonical `DocumentPath` defined in
[replica.md](replica.md). Unsupported entries produce a structured error with a
sanitized path and are never silently imported under a lossy spelling. An
initial scan with such an entry fails without creating active replica metadata;
a later watcher event leaves existing catalog state unchanged and records the
error.

Regular files are classified by one ordered decision path:

1. `infer` is a media-signature input, not the final text/binary decision. A
   `MatcherType::Text` result such as HTML, XML, or shell script remains a text
   candidate and supplies its media type. A non-text `infer` result is a strong
   binary signature and is binary.
2. Unicode signatures and byte structure are checked before generic UTF-8:
   UTF-8, UTF-16LE/BE, UTF-32LE/BE, UTF-7, and UTF-EBCDIC are accepted only
   after strict complete decoding. A recognized BOM is recorded. Conservative
   zero-lane checks may recognize BOM-less UTF-16 and UTF-32, but arbitrary NUL
   bytes are not accepted as UTF-8 text.
3. Otherwise, exact UTF-8 is accepted only when its decoded content passes the
   text control-character policy below.
4. `chardetng` selects among its supported legacy candidates. oll supplements
   that detector only for formats outside its candidate set or for an
   unambiguous structural extension: GB18030 four-byte sequences,
   ISO-8859-15, MacRoman, and IBM037 EBCDIC. Every candidate must decode the
   complete byte sequence without replacement and pass the same text-content
   policy.
5. Data not accepted by the preceding rules is binary. A known binary signature
   supplies its media type; otherwise the media type is
   `application/octet-stream`.

The text-content policy rejects NUL and rejects content dominated by C0/C1
controls other than ordinary text whitespace. This prevents the fact that some
single-byte encodings define nearly every byte from turning arbitrary data into
text. It is still impossible to prove from bytes alone that every surviving
single-byte sequence was intended as text, so the implementation and CLI must
not describe heuristic legacy detection as certainty.

The supported input encoding families include ASCII, UTF-8, UTF-16, UTF-16LE/BE, UTF-32,
UTF-32LE/BE, UTF-7, UTF-EBCDIC, GB2312, GBK, GB18030, Big5, Shift-JIS, EUC-JP,
ISO-2022-JP, EUC-KR, KS X 1001, TIS-620, ISO-8859-1, ISO-8859-2, ISO-8859-5,
ISO-8859-15, Windows-1252, Windows-1251, Windows-1250, MacRoman, IBM037
EBCDIC, KOI8-R, and KOI8-U. Where the listed byte formats are subsets or aliases
that cannot be distinguished without external metadata, the catalog records a
reversible canonical encoding: ASCII as UTF-8, generic UTF-16/UTF-32 as the
detected endian form, GB2312 as GBK, KS X 1001 as EUC-KR, TIS-620 as
windows-874, ISO-8859-1 as windows-1252 when no distinguishing bytes exist, and
generic recognized EBCDIC as IBM037. KOI8-R is preferred for the shared
KOI8-R/U repertoire; KOI8-U is recorded when its distinguishing Ukrainian byte
positions occur. Canonicalization must preserve the decoded text and exact
re-encoding; it is not permission to decode with replacement.

The catalog records an accepted document's canonical encoding and any BOM
needed to write it back. Loro text is Unicode; materialization encodes that text
using the catalog value. A user saving a document in a different accepted
encoding updates that metadata together with the text. A text signature
recognized by `infer` supplies media types such as `text/html`, `text/xml`, or
`text/x-shellscript`; other filesystem-created text starts as `text/plain`.
Media type is catalog metadata and may later be changed through a catalog
mutation without changing the file's object identity or classification.

Materialization MUST NOT replace an unrepresentable Unicode scalar with a
substitute byte sequence. If merged document text cannot be encoded exactly in
the catalog's current legacy encoding, oll records a catalog update that promotes
the file to UTF-8 without a BOM, then writes the exact text as UTF-8. The event
is logged as an encoding promotion. This preserves content and gives every peer
a representable projection instead of silently corrupting a document.

For a text document, the user-visible file content is the `content` LoroText
inside its LoroDoc. A filesystem reconciliation reads and decodes the final
text, compares it with `content`, and uses `LoroText::update` only when they
differ. `update` derives the necessary character-level changes; when the text
is already equal it creates no operation. It is therefore safe for a delayed
event caused by oll's own materialization to be reconciled again.

`HashSet<Path>` style in-progress tracking and short delays are permitted
performance optimizations to avoid reading a file while oll is replacing it.
They are not correctness mechanisms. A late event is harmless because a second
`LoroText::update` against equal text is a no-op. Watcher reconciliation MUST
NOT use positional `insert`, `delete`, or `apply_delta` merely because it knows
the final filesystem text.

Remote replication imports verified Loro updates with `LoroDoc::import_with`
or `LoroDoc::import_batch`; Loro operation IDs make a duplicated remote update
idempotent. Those APIs are distinct from filesystem reconciliation, where oll
knows a final text value rather than a Loro update. The first implementation
uses the precise character-level `LoroText::update`; `update_by_line` is deferred
until benchmarks show that its coarser merge granularity is an acceptable
tradeoff.

A reliable live rename pair preserves the existing catalog, document, or binary
identity. When no reliable pair exists, including moves made while oll is not
running, oll treats the observation as a deletion plus a new creation. The new
file receives a new `DocumentId` or `BinaryId`.

Editor-style atomic save is not a move of the logical document: when an
unmanaged temporary file is renamed over an already managed destination, the
debounced final-state reconciliation updates that destination's existing
document or binary identity and does not create a catalog entry for the
temporary name. Identity-preserving move applies when the rename source was an
existing managed entry and the destination was not another managed entry.

Reconciliation is final-state based for every entry kind. An observed directory
that already matches catalog topology is a no-op. An observed binary is hashed;
when its SHA-256, size, and media type equal the currently projected winning
version, reconciliation creates no Lamport version. Only different bytes or
metadata create a new local binary version. Thus a late watcher event caused by
oll materializing a synced binary cannot turn that remote version into a new
local winning write.

## Documents and binary files

Every text document has one LoroDoc with two fixed logical roots:

- `content`: the LoroText projected to the user-editable file;
- `data`: a LoroMap root used by oll's abstract CRDT map/list/text/tree/counter
  API.

Filesystem edits affect only `content`. CRDT data stored under `data` is not
serialized into the text file and does not overwrite it. Plugin and future host
calls may use both roots through oll's stable document API.

Binary files have a UUID-v4 `BinaryId` but have no LoroDoc, Loro peer identity,
or Loro operation history. The catalog retains an append-only version record
for every binary write. A record contains the blob SHA-256, byte length, media
type, Lamport clock, and writer `NodeId`. Its key is the unique
`(lamport, writer-node-id)` pair. The visible binary is the record with the
largest lexicographic `(lamport, writer-node-id)` stamp.

The catalog retains all concurrent records so that Loro's own map conflict rule
cannot discard a binary candidate before oll applies the binary LWW rule. The
blob bytes remain content-addressed in the store. Garbage collection of old
binary versions, like document-object garbage collection, waits for a future
causal-stability design.

## Projection recovery and replacement

Before a daemon-owned change requires working-tree output, oll commits the new
store state, an incremented projection generation, and a recovery record naming
the affected output paths. On restart it completes that targeted projection
before treating those paths as filesystem input. It then scans and watches every
unaffected user path normally. This prevents a crash while writing one synced or
plugin-produced file from discarding unrelated files a user created in the
editable working tree.

Targeted recovery records are a durable set accumulated across host-level
commits in the active generation. Adding records for a later commit MUST NOT
replace earlier records, and completing that later commit MUST NOT clear every
record in the generation. oll projects each recorded path independently. Each
path receives at most three total materialization attempts, with a non-zero
bounded delay between failed attempts and structured retry events containing the
path, attempt, error code, and backoff. A successful materialization or removal
is acknowledged by deleting exactly that path's record. When a batch contains
multiple paths, oll continues with the other paths after one exhausts its
attempts, acknowledges only successful paths, and returns the projection
failure while leaving every failed path durable.

Consequently, a store commit remains authoritative even when all three attempts
to write its working-tree file fail. A later unrelated commit can acknowledge
only the paths it actually projected; it cannot erase the failed record. The
next reconciliation or restart retries the retained path from current store
state before that path can be imported as filesystem input. If a newer commit
touches the same pending path, projecting the newest authoritative state and
then acknowledging that exact path completes both obligations.

Snapshot import is deliberately different: it replaces the complete logical
replica after two explicit user confirmations. Its candidate-store transaction
sets the whole-tree `projection_pending` marker. While that marker is set,
startup and recovery MUST NOT scan or import any old working-tree file. They
first rebuild the entire managed namespace from the imported store, deleting
paths absent from the new catalog and materializing the visible documents and
binary blobs. Only after that succeeds may oll clear the marker and start normal
watcher reconciliation. Thus a crash after an import has changed the store but
before its files were written cannot re-import the old tree into the new
replica.

SQL backends implement replacement with logical generations, not by renaming a
SQLite file or pretending PostgreSQL has a filesystem layout:

1. the importer allocates a new generation ID and builds all candidate rows
   under that inactive generation;
2. a single SQL transaction rechecks that the active generation has not
   changed, points `active_generation` at the candidate, stores its fresh local
   `LoroPeerId`, and sets `projection_pending`;
3. a crash before that transaction leaves the old generation active; the
   incomplete inactive candidate is cleanup state;
4. a crash after it leaves the imported generation authoritative, so startup
   rebuilds the working tree from it and never scans the old projection;
5. the former active generation may be retained internally until projection
   completes, but it is never mounted as a second replica or automatically
   reactivated after the switch; it and other inactive generations may then be
   deleted.

Rebuilding an already pending generation is idempotent. A crash after all files
were written but before the marker was cleared simply causes the same complete
projection to run again. These rules are identical for SQLite and PostgreSQL.

## High-level operation history

`oll replica ops` reads local high-level operation records from the store. A
record contains time, operation ID, source (`filesystem`, `plugin`, `sync`, or
`snapshot_import`), operation kind (`create`, `update`, `move`, `delete`, or
`replace`), affected IDs and sanitized paths, and the correlation ID. It never
contains document text, binary content, Loro operation IDs, container IDs,
frontiers, or version vectors.

Records are local diagnostic history rather than replicated replica state. They
are excluded from `.ollsnap`. `--limit` returns the newest records first.

## Required tests

Replica persistence tests cover both SQLite and PostgreSQL logical behavior:

- the complete supported text-encoding matrix decodes and re-encodes exactly;
  `infer` HTML/XML/shell results remain documents with their text media types,
  while known binary signatures and NUL/control-rich data remain binaries;
- extended UTF-32 and EBCDIC documents survive a host commit, snapshot
  verification, projection, and restart without changing their recorded
  encoding or text;
- an empty startup remains uninitialized, then the first live file event
  atomically creates one replica and imports that file;
- an entry created while the startup scan is running is not missed;
- delayed duplicate self-events are no-ops for text, directories, and binaries;
- editor temporary-file replacement preserves the destination identity, while
  an offline or otherwise unpaired move is deletion plus creation;
- a failed precondition or SQL transaction leaves every live LoroDoc, catalog
  entry, blob version, operation record, and projection marker unchanged;
- a targeted file projection retries three times with delay, acknowledges only
  successfully projected paths, and an unrelated later commit cannot erase a
  failed path's marker or allow stale working-tree bytes to overwrite the
  committed store state;
- restart completes a targeted projection before importing that path;
- snapshot replacement crashes immediately before and immediately after the
  active-generation switch, and each restart selects the documented generation
  without importing the old working tree;
- rebuilding a pending complete-tree projection is idempotent.

The live PostgreSQL contract test is marked ignored by default because it
requires an externally managed database. A normal `cargo test` run therefore
reports it as `ignored`, never as a successful test that silently returned
without exercising PostgreSQL. Complete validation runs it explicitly:

```sh
OLL_TEST_POSTGRES_URL='postgresql://user@%2Frun%2Fpostgresql/database' \
  cargo test \
  replica::store::tests::postgres_implements_the_logical_store_contract_when_configured \
  --lib -- --ignored --exact
```

When explicitly selected, a missing or non-UTF-8 `OLL_TEST_POSTGRES_URL`, a
connection failure, or any contract violation fails the test. CI treats this
explicit PostgreSQL invocation as a required job rather than inferring coverage
from the ordinary test suite.
