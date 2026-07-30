# Command-line interface

## Scope and availability

`oll` is the only executable. The CLI schema is defined before node, replica,
sync, and plugin implementations so their public command surface can be tested
without fake side effects.

Until an operational command has a real handler, it parses and validates its
arguments, then fails with exit code `69` (`EX_UNAVAILABLE`) and the generic
message `command is not implemented`. It MUST NOT create files, start a daemon,
connect to a peer, or mutate a replica. The implementation order is not a
runtime concept and MUST NOT appear in errors or source-level domain types. Clap
syntax errors use exit code `2`.

`run` is the only daemon-side subcommand. Every other subcommand is a bounded
client selected directly by the parsed subcommand, never by a global mode flag.
`init` performs local bootstrap and `start` launches and verifies a detached
`run` child. Operations that need daemon-owned state send typed requests to the
Admin UDS; local snapshot inspection, snapshot verification, and log viewing do
not. See `admin-api.md`.

## Paths and environment

Default paths are:

```text
replica root: platform Documents directory / oll
config root:  platform configuration directory / oll
replica store: SQLite at
               <platform-data-dir>/oll/stores/<new-node-id>/replica.sqlite3
log dir:      platform state directory / oll
```

On Linux, the default replica root uses the XDG user Documents directory when
available and otherwise falls back to `$HOME/Documents/oll`. The platform
directory helper supplies the corresponding Documents, configuration, data, and
state locations on Darwin. The resolved paths and their HOME-less behavior are
defined in [configuration.md](configuration.md).

For `init`, command-line root options override their corresponding `OLL_*`
variables, which override the defaults above. `run` first resolves only the
config root using `--config`, `OLL_CONFIG`, or its platform configuration
directory default. The node then takes its single-instance lock before it
validates `node.json`, executes `<config-root>/config.lua`, and applies
command-line values over environment values over the persisted replica and log
roots, then reads the required typed replica-store table from configuration. It
MUST NOT require HOME to derive replica or log defaults before reading a
configuration selected by an absolute config root. Other commands that need the
config/Admin API or a log file use the same intent-specific resolution without
command-line root overrides. The complete returned-table schema and precedence
rules are defined in `configuration.md`.

oll is a user-level daemon. Each root and every file beneath it belongs to the
user who initialized and runs that deployment; oll does not require a system
`oll` account, membership in a log group, root privileges, or a service manager.

Paths are accepted as OS paths and are not required to exist during argument
parsing. Relative CLI and environment root paths are joined to the process
startup working directory without `canonicalize`; relative roots returned by
`config.lua` are joined to the config root. `init` persists resolved absolute
replica, log, and SQLite-store paths, which must be representable as UTF-8 Lua
strings; the config root itself stays a native OS path. `start` passes an
absolute config root to its detached `run` child. Document and snapshot paths
remain native `PathBuf` values and do not acquire this persistence restriction.

Clap types are raw syntax only. After parsing, oll converts them into a
validated `CliIntent` whose enums enumerate every supported operation and mode.
Intent-specific preparation then captures the startup working directory,
resolves only the required environment and OS paths, and produces a
`PreparedCliIntent`. For `run`, that form contains the resolved config root and
runtime overrides; the node handler acquires the lock before it evaluates
`config.lua`. Runtime handlers accept this prepared form, never the raw Clap
structs or an environment-dependent `CliIntent`. Clap conflicts provide early
diagnostics, while the conversion independently rejects every field combination
that is not on the supported-intent whitelist.

Environment and runtime dependencies are selected from the concrete intent,
not from its top-level command. In particular, local snapshot inspection,
snapshot verification, and fixed log-file viewing do not require a config path,
Admin connection, or replica root.

## Node commands

```text
oll init <node-name>
oll init home-laptop --replica /path/to/replica/root
oll init home-laptop --config /path/to/config/root
oll init home-laptop --log-dir /path/to/log/dir
oll init home-laptop --listen 127.0.0.1:7443 \
  --connect https://oll.example.com

oll run
oll run --replica /path/to/replica/root
oll run --config /path/to/config/root
oll run --log-dir /path/to/log/dir
oll run --listen 127.0.0.1:7443
oll run --connect https://oll.example.com
oll run --listen 127.0.0.1:7443 \
  --connect https://node-a.example.com \
  --connect https://node-b.example.com

oll start
oll stop
oll status
oll status --json
oll log set oll::sync=trace
```

`--connect` is repeatable; `--listen` accepts one socket address. Together they
fully express deployment topology. There is no `client`/`server` profile wrapper
and no topology-derived authority level.

`init` and `run` each define their own root and topology flags. They are not one
shared argument group: future command-specific options need not be added to both
commands. `init` creates the selected directories, writes a complete
`config.lua` containing the replica root, default SQLite replica store, log
directory, and topology, and writes `<config-root>/node.json`. It establishes
only an empty replica slot; `ReplicaId` and replica data are created later. A
PostgreSQL store is selected by replacing the generated `replica_store` table in
`config.lua` with the documented tagged configuration. When either
initialization file already exists, it warns and asks for an interactive `y`/`n`
replacement confirmation. `run` locates those files through its selected config
root; its `--replica`, `--log-dir`, `--listen`, and `--connect` values are
temporary runtime overrides and do not rewrite configuration. `config.lua` is
an executable LuaJIT module that must return the complete versioned table
defined in `configuration.md`; oll reads the returned table rather than named
Lua globals. Full layout, recovery, and lock behavior are in `node.md` and
`replica-store.md`.

`node-name` is required when initializing a deployment. It is the durable,
globally presented human name paired one-to-one with the generated `NodeId`, not
a receiver-local name for a connection. It uses the lowercase DNS-label syntax
defined in `architecture.md`. `init` writes the initial UUID-v4 pair to
`node.json`; a deployment user may later edit that strict record, with the
identity-binding consequences described in `node.md`.

`run` starts the same `oll` binary in the foreground. It also has a hidden
internal `--pingback <loopback-address>` option used only by
`start`; this is not a second public daemon mode. `start` launches the daemon in
the background and verifies readiness with the nonce exchange specified in
`admin-api.md`. `stop` uses the configured Admin API to gracefully stop oll and
all child processes, then waits for actual daemon termination rather than
treating the initial `accepted` response as completion.

`status` reports the local `NodeName` prominently, its `NodeId`, configured
listen and connection targets, and, after the replica stage, whether the local
replica is uninitialized, initialized with no visible entries, or initialized
and populated. Both initialized states include the active `ReplicaId`. A target
learned through `SyncHello` is displayed with the remote node's
protocol-declared `NodeName` and `NodeId`; a target whose first handshake has
not completed is displayed by URL as pending. `--json` selects the
machine-readable schema; human-readable output is the default.

`oll log set <target>=<level>` is an Admin client command. It changes only the
live target filter and does not rewrite `config.lua` or `node.json`. Its accepted
syntax and runtime behavior are defined in `observability.md`.

## Replica commands

```text
oll replica inspect <document-path>
oll replica ops <document-path> [--limit <count>] [--format text|json]
oll replica export -o <snapshot>
oll replica import <snapshot>
oll replica snapshot inspect <snapshot> [--json]
oll replica snapshot verify <snapshot>
```

`inspect` addresses one managed text document and reports its catalog and
document identities, separate revisions, path, media type, encoding, and byte
size. A directory or binary path is rejected rather than being assigned a fake
`DocumentId`. `ops` returns the local high-level records associated with that
document; it is not a Loro operation log and never prints document content.
`--limit` must be greater than zero and records are rendered newest first. The
CLI does not enforce a snapshot file extension; the format itself is defined in
`snapshot-format.md`.

`ops --format json` is the stable machine-readable form:

```json
{
  "operations": [
    {
      "timestamp": "2026-07-30T00:00:00Z",
      "operation_id": "opaque-operation-id",
      "source": "filesystem",
      "kind": "move",
      "catalog_node_id": "catalog-node-uuid-v4",
      "document_id": "document-uuid-v4",
      "path_before": "/notes/a.md",
      "path_after": "/archive/a.md",
      "correlation_id": "opaque-correlation-id"
    }
  ]
}
```

`source` is `filesystem`, `plugin`, `sync`, or `snapshot_import`; `kind` is
`create`, `update`, `move`, `delete`, or `replace`. `path_before` and
`path_after` are nullable strings when that side of the operation does not
exist. No other field is nullable. The default text format presents the same
fields for a human but is not a parsing interface; scripts use JSON.

Before the first scan or snapshot import has initialized the replica, `inspect`,
`ops`, and `export` return `FAILED_PRECONDITION`; `import` remains available so
a snapshot can initialize the slot.

`replica import` replaces the node's one replica and its visible working tree
with the complete replica in the snapshot; it is not a CRDT merge and never
adds a second replica. Immediately before submitting the import, an interactive
client MUST separately ask the user to confirm that the current replica has
been exported to a backup snapshot and that the destructive replacement should
proceed. Either negative answer, end-of-file, or inability to read interactive
confirmation cancels the import without sending the Admin request. The first
implementation has no flag that bypasses these confirmations.

Replica document and snapshot path arguments are OS paths represented by
`PathBuf`. Before an Admin request is constructed, the client captures its
startup working directory and joins it to each relative path. Absolute paths
are passed through unchanged; the client does not check existence or call
`canonicalize`, and the daemon working directory is never used to reinterpret
them. Replica handlers later verify root containment and convert document paths
to the normalized replica namespace. Snapshot import, export, inspect, and
verify apply the same client-working-directory rule, with their operation
specific output or input checks performed by the handler. Document operations,
export, and import use the configured Admin API. Snapshot `inspect` and `verify`
operate only on their input file and do not require the current node's config or
replica root.

## Sync commands

```text
oll sync
oll sync <node-name>
oll sync <node-name> -n 3
oll sync --log
oll ping <node-name>
```

`node-name` is the remote node's protocol-declared `NodeName`, obtained from
`oll status`; it is not a `NodeId`, URL-derived display name, or local peer
label. The daemon maps a learned remote `NodeIdentity` back to its configured
connection target. A URL-only target cannot be selected by name until one
successful handshake has established and persisted that identity binding.
`-n`/`--retries` is the maximum total number of synchronization attempts and
must be greater than zero. The initial attempt counts toward this limit, so
`--retries 3` means at most three attempts in total, not one initial attempt
plus three additional retries. `sync --log` views
`<log-dir>/sync.log`; it is a log-view mode and conflicts with `node-name` and
`--retries`. It reads that file locally and does not require node configuration
or an Admin connection.

## Plugin commands

```text
oll plugin install
oll plugin install <git-remote>
  [--rev <revision> | --branch <branch>]
  [--release | --source]
oll plugin validate
oll plugin list
oll plugin info <plugin-id>
oll plugin start <plugin-id>
oll plugin stop <plugin-id>
oll plugin restart <plugin-id>
oll plugin update <plugin-id>
oll plugin remove <plugin-id>
oll plugin --log [<plugin-id>]
oll plugin call <plugin-id> <action> [arguments...]
```

`git-remote` accepts standard Git URLs and SCP-style SSH syntax such as
`git@github.com:example/oll-anki.git`. Source installation is the default.
`--release` and `--source` conflict; `--rev` and `--branch` conflict. Selection
applies to both modes: source reads `oll.toml`, while release reads `oll.toml`
and the direct artifact URLs in `oll.json` from the same selected repository
state. Version ranges and Git tags have no oll semantics; publishers may expose
version branches such as `release/v0.3.1`.

Without a remote, `plugin install` reads `<config-root>/plugins.lua` and
reconciles its declarations. With a remote, the daemon resolves the `PluginId`,
atomically adds the declaration to that file, and then invokes the same
file-driven reconciliation. An identical declaration is left unchanged. A
different declaration for the same ID requires an interactive overwrite
confirmation; a negative answer, EOF, or unavailable input changes neither the
file nor the installation. Installation failure after a successful write does
not roll back desired configuration.

`plugin validate` is local and read-only. It validates the literal-only syntax
and schema of `<config-root>/plugins.lua` without opening the Admin API,
accessing a remote, running a recipe, or rewriting the file. Publisher and user
file formats, source/release behavior, LuaJIT requirements, and stable error
codes are defined in `plugin-packaging.md`.

A generic plugin call returns a job ID after the plugin stage is implemented.
Its arguments are shell-style UTF-8 argv strings; the client preserves their
order, duplicates, empty strings, and leading `-` characters without parsing or
inferring types.

`plugin --log` reads `<log-dir>/plugin.log` locally, optionally filtering for
one plugin ID. It does not require node configuration or an Admin connection.

A newly installed plugin is initially stopped. `plugin start` persistently sets
its desired state to running; `plugin stop` persistently sets it to stopped
before beginning shutdown; `plugin restart` sets it to running and recycles the
current process if one exists. These commands are reconciled by the daemon and
do not spawn plugin processes in the CLI client. A call does not implicitly
start a stopped plugin. `plugin list` and `plugin info` report desired and
observed process state separately; a failed or restarting plugin remains
desired-running unless explicitly stopped.

Plugin `stop` uses the same graceful `ShutdownRequest` and signal-enforcement
sequence documented in `plugin-system.md`; it does not introduce a stronger
kill operation. Desired state is a separate concern from this common process
termination sequence.

## Job commands

```text
oll job list
oll job info <job-id>
oll job stop <job-id>
```

`job stop` uses the same graceful plugin-process shutdown path as plugin stop,
kill, timeout, and `killjob`. It does not promise rollback of completed writes or
external effects. It does not change the plugin's desired state, so a plugin
whose desired state remains running is restarted after its current process
exits.
