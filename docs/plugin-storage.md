# Plugin storage and recovery

## Storage boundaries

Plugin state belongs to one oll deployment, not to the logical replica. The
configured SQL backend is reused for durable plugin tables, but those tables
remain outside replica generations and are never synchronized, bootstrapped,
exported in `.ollsnap`, or replaced by snapshot import. SQLite and PostgreSQL
implement the same plugin-state contract.

Installed source trees and executables require a local filesystem even when the
configured SQL backend is PostgreSQL. The default plugin data root is:

```text
<platform-data-dir>/oll/deployments/<deployment-key>/plugins/
```

`deployment-key` is the same SHA-256-derived key of the canonical config-root
path used by the deployment lock. This prevents two config roots from sharing
mutable package state and does not couple installations to the user-editable
`NodeId`. On Linux the platform data directory follows `XDG_DATA_HOME` and its
ordinary fallback. The plugin data root is oll-managed, owner-only, and must be
disjoint from `replica_root` so package files never enter the recursive working-
tree watcher.

Each installation is addressed by immutable `PluginId`, never by mutable
`PluginName`:

```text
<plugin-data-root>/<plugin-id>/
├── candidates/
│   └── <install-generation>/
├── generations/
│   └── <install-generation>/
├── current -> generations/<install-generation>
└── build-logs/
```

An install generation is a host-generated UUID v4. `current` is an oll-owned
relative symlink published by atomic sibling replacement on the first Unix
implementation. A running process remains bound to the generation from which
it was spawned; switching `current` does not restart it. An old generation is
retained until no running instance uses it and no recovery record references
it.

## Plugin identity

`PluginId` is an immutable path-safe publisher identity. It is a lower-case
ASCII dotted name containing at least two DNS-label-shaped segments; the full
value is at most 191 bytes and each segment is at most 63 bytes. The lower total
bound leaves room for `.toml`, `.lua`, and private sibling suffixes under the
common 255-byte filesystem component limit. `PluginName` is a lower-case ASCII
DNS label of 1 to 63 bytes. The distinct grammars make a CLI selector containing
a dot unambiguously an ID and a selector without a dot a name.

The publisher declares both values in `oll.toml`. A typed user mask may replace
the name but cannot replace the ID. Effective names are unique within one
deployment. SQL stores a one-to-one binding from every installed ID to its
current effective name; a masked or publisher-supplied name change is committed
only when it conflicts with neither another ID nor another name. Removing a
plugin deletes the binding, after which a later installation may reuse either
value.

## SQL authority

The plugin tables durably represent at least:

- the immutable PluginId and current effective PluginName binding;
- the normalized installation declaration and its digest;
- the effective manifest, selected Git commit, source/release mode, selected
  opaque release ID, current install generation, and a running instance's
  generation;
- package transition and removal recovery records;
- authoritative desired process state, restart sequence, restart-backoff state,
  and last lifecycle failure;
- host job records, operation-ID admission records, terminal results, and
  stored-artifact metadata;
- the startup-resolved artifact download directory used for publication.

These rows do not live beneath `active_generation`. Replacing a replica changes
none of them. A PostgreSQL database or schema remains exclusive to one oll
deployment under the existing store ownership rule.

`plugins.lua` is authoritative only for installation declarations. It contains
no desired process state. A new installation creates its SQL desired state as
`stopped`; thereafter only `SetPluginDesiredState` changes that authority.
Manual edits to `plugins.lua` have no runtime effect until an explicit install
or reconciliation command reads them.

## Package publication and recovery

Install and update serialize per PluginId. Work for different IDs may progress
independently, while `plugins.lua` replacement remains serialized for the whole
deployment.

One source build or release extraction follows this recovery boundary:

1. create a private candidate generation and a per-install build log;
2. fetch, build or extract, then validate the complete effective manifest,
   protocol fingerprint, target, runtime entrypoint, and candidate contents;
3. persist a SQL publish intent containing the declaration digest, candidate
   generation, and expected current generation, and block new spawns for that
   PluginId while the transition is prepared;
4. move the verified candidate beneath `generations/` and atomically replace
   the `current` symlink if the expected generation still matches;
5. finalize the SQL current-generation pointer and clear the publish intent;
6. delete generations no longer referenced by current state, a running
   instance, or recovery.

Step 4 is the atomic filesystem switch. The per-PluginId package gate remains
held through step 5, so a new spawn can observe neither a switched symlink with
old SQL nor new SQL with an old symlink; after the gate opens it uses the new
generation. There is no transaction spanning SQL and filesystem rename, so the
intent makes either side recoverable. Startup deletes an incomplete build for
which no verified publish intent exists. A verified candidate with a matching
intent completes publication; a pointer already switched before SQL finalization
causes SQL to be finalized idempotently. A candidate whose declaration or
expected current generation no longer matches is discarded. No `.lock` file or
semantic package-version directory is used.

A failed install/update of an existing ID leaves `current` and the old SQL
current generation unchanged. Publishing any replacement generation does not
recycle a running process. `GetPlugin` therefore reports current and running
install generations separately. If a later start of the new generation fails,
the supervisor records failure and applies normal restart backoff; it never
silently rolls `current` back.

## Desired and observed process state

Desired state is persistently `running` or `stopped`. Observed state is
transiently `starting`, `ready`, `stopping`, `exited`, or `failed`. Setting the
desired state is one short, idempotent SQL transaction. Its Admin response means
that the authoritative state was stored and reconciliation was queued; it does
not wait for a process to become ready or exit.

`RestartPlugin` is an edge-triggered operation. It leaves or sets desired state
to `running`, atomically advances a restart sequence, and queues one recycle of
the current instance. Concurrent reconciliation never creates overlapping
instances. The response acknowledges the recorded sequence rather than waiting
for readiness.

Unexpected child exit, startup/session failure, or heartbeat failure does not
change desired state. A desired-running plugin restarts with bounded backoff;
a desired-stopped plugin remains exited. Clean daemon shutdown stops children
without changing desired state. On the next start, the supervisor reconstructs
observed state and admits desired-running spawns after package-transition and
job recovery.

## Jobs and idempotent admission

The host persists jobs with states `dispatching`, `running`, `cancelling`,
`succeeded`, `failed`, `cancelled`, or `timed_out`. Terminal rows and their
operation-ID records are retained until the owning plugin is removed. Removing
a plugin does not delete already published artifact files from the user's
download directory. JobId is a host-generated canonical UUID v4.

`StartPluginJob` normalizes and validates the request, resolves a name to its
immutable PluginId, and compares this domain value rather than protobuf bytes.
The normalized payload contains the PluginId, action, ordered argument strings,
and either the caller's exact deadline or a `default_24_hours` marker. The
absolute deadline computed for the first admission is stored on the job but is
not recomputed when an omitted-deadline retry is compared. Operation IDs are
unique across the deployment, not merely within one plugin process. Admission
uses one SQL transaction:

- an unseen nonempty operation ID creates one JobId and a `dispatching` row;
- the same operation ID and normalized payload returns the same JobId and
  current result;
- the same operation ID with another normalized payload returns
  `ALREADY_EXISTS` and creates nothing.

The host sends `StartJobRequest` only after persistence and returns from the
Admin call after `JobAccepted` or a terminal admission failure. Acceptance
changes the SQL state to `running`; it does not mean the action completed.

`StopPluginJob` changes a nonterminal job to `cancelling` and sends the
job-scoped `CancelJobRequest`. The plugin MUST cease that job before replying
with `CancelJobAcknowledged`; acknowledgement publishes `cancelled`. The
24-hour default job deadline follows the same path but publishes `timed_out`.
Neither path changes plugin desired state or terminates unrelated jobs. A
missing cancellation acknowledgement fails that job and records a job-scoped
protocol error; it is not promoted into process shutdown merely to enforce one
job request.

Completion and cancellation may cross in flight. The job coordinator serializes
them and the first terminal transition durably committed wins. A success/failure
committed before the cancellation acknowledgement remains that result; a later
acknowledgement is ignored. Once cancellation or timeout commits its terminal
state, a later `JobUpdate` cannot replace it. Repeated stop requests return the
current row and do not send process-scoped shutdown.

A plugin process exit or session failure marks every nonterminal job owned by
that instance `failed`. At daemon startup, every job left in `dispatching`,
`running`, or `cancelling` by the preceding process is marked `failed` before
plugins are spawned. Completed CRDT writes and external effects are not rolled
back. Retrying the original operation ID still returns its original failed job;
a deliberate new attempt uses a new operation ID.

## Artifact publication

`node.artifact_download_dir` is loaded from `config.lua` at daemon startup. The
validated resolved path is cached in SQL before plugin work begins, and runtime
artifact publication reads that cache. Editing `config.lua` takes effect only
after restart; the first implementation does not hot reload it.

Startup creates the directory when absent using owner-only permissions. An
existing path must resolve to a directory accessible by the deployment user;
oll does not loosen its permissions. Temporary artifact files are created in
that same directory so final no-replace publication does not cross filesystems.

The artifact sub-protocol has no total artifact-size limit. It still enforces
the advertised per-chunk limit, declared chunk count, contiguous zero-based
indexes, exact total size, and SHA-256. Bytes are staged in a private sibling of
the destination and published with atomic no-replace. A filename must be one
nonempty UTF-8 basename of at most 191 encoded bytes and cannot contain a
separator, NUL, `.` or `..`; the bound leaves room for the collision suffix.
PluginArtifactId is a plugin-generated canonical UUID v4 that must be unique in
the deployment; rejecting a duplicate prevents one transfer from aliasing
another transfer or its filename suffix.

Publication crosses SQL and the download filesystem and therefore uses a
durable intent. After complete byte/hash validation, oll chooses one no-replace
destination, persists an intent containing the ArtifactId, JobId, staging path,
destination, size, and hash, atomically publishes the file, then commits artifact
metadata and clears the intent. Startup recovers intents before marking old jobs
failed: a destination whose bytes match the intent is finalized idempotently; a
complete matching staging file is published and finalized; a missing or
contradictory pair fails the artifact and owning job without overwriting a user
file. The absolute destination recorded in an older intent remains authoritative
for its recovery even if `artifact_download_dir` changed at this startup.

The first artifact attempts `<artifact_download_dir>/<file_name>`. On collision,
oll inserts `.artifact-<full-artifact-id>` before the final extension and again
uses no-replace publication. A collision at that identity-qualified name fails
rather than inventing another unstable suffix. An existing file is never
truncated. Failed, cancelled, timed-out, or interrupted transfers remove private
staging. A terminal job may reference only artifacts for which publication and SQL
metadata both completed. Artifact bytes and final download files are not part
of the replica and are excluded from `.ollsnap`.

## Removal

Explicit `RemovePlugin` and exact-set `plugin reconcile` share one removal
owner. Before the destructive intent, the owner strictly parses `plugins.lua`,
captures its digest, and prepares the exact declaration-free replacement; a
parse failure changes nothing. Removal then persists a SQL intent and prevents
new jobs or spawns, stops and reaps the current process, rechecks the digest and
atomically publishes the prepared file when the declaration is present, renames
the package directory to private trash, deletes plugin desired state,
installation state, jobs, and identity binding in one SQL transaction, and
finally deletes the trash. A concurrent user edit detected before declaration
publication aborts before package/SQL destruction and reconciles the stopped
process against its still-authoritative desired state.

A crash leaves an intent that startup completes before normal supervision. A
failure before the declaration or package state changed clears the intent and
restores ordinary reconciliation; a failure after either destructive step
continues removal rather than resurrecting half-removed state. Client
cancellation cannot undo a removal whose durable destructive intent has begun.
If the user deliberately adds a new declaration after the old one was removed,
the removal does not overwrite that later edit; a future explicit reconcile may
install it as a new local installation.

## Required tests

Tests cover SQLite and explicit PostgreSQL behavior for desired-state
atomicity, job admission idempotency, name collision, package publication on
both sides of the `current` switch, incomplete-candidate cleanup, running-old/
current-new generations, removal at every recovery boundary, and snapshot/
bootstrap isolation. Process tests cover unexpected exit, bounded restart
backoff, stdin EOF conformance, normal shutdown, job-only cancellation with
other jobs still running, and daemon restart marking nonterminal jobs failed.
Artifact tests cover chunk order, size/hash mismatch, filename traversal,
no-replace races, collision projection, interrupted staging cleanup, and
download files surviving plugin removal.

The live PostgreSQL plugin-store contract follows the replica-store test rule:
it may be ignored in the default suite, but an explicit invocation without its
configured database URL fails rather than reporting a skipped test as passed.
