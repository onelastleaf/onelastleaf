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
replica root: $HOME/.local/share/oll
Lua config:   $HOME/.config/oll/config.lua
```

`--replica` overrides `OLL_REPLICA`, which overrides the default replica root.
`oll run --config` overrides `OLL_CONFIG`, which overrides the default config
path. Other commands that need the config/admin API use `OLL_CONFIG` and then
the default. An unavailable home directory is a configuration error when a
default is needed.

Paths are accepted as OS paths and are not required to exist during argument
parsing.

Clap types are raw syntax only. After parsing, oll converts them into a
validated `CliIntent` whose enums enumerate every supported operation and mode.
Runtime handlers accept `CliIntent`, never the raw Clap structs. Clap conflicts
provide early diagnostics, while the conversion independently rejects every
field combination that is not on the supported-intent whitelist.

Environment and runtime dependencies are selected from the concrete intent,
not from its top-level command. In particular, local snapshot inspection,
snapshot verification, and fixed log-file viewing do not require a config path,
Admin connection, or replica root.

## Node commands

```text
oll init <node-name>
oll init home-server --profile server
oll init home-laptop --profile client --connect https://oll.example.com
oll init home-server --profile server --listen 127.0.0.1:7443
oll init home-laptop --replica /path/to/replica/root

oll run
oll run --config ~/.config/oll/config.lua
oll run --listen 127.0.0.1:7443
oll run --connect https://oll.example.com
oll run --listen 127.0.0.1:7443 \
  --connect https://node-a.example.com \
  --connect https://node-b.example.com

oll start
oll stop
oll status
oll status --json
```

`profile` is optional and has values `client` and `server`. It is an
initialization profile, not a replication role or authority level. Either
profile may use `connect`, `listen`, both, or neither. `--connect` is repeatable;
`--listen` accepts one socket address.

`node-name` is required when initializing a deployment. It is the durable,
globally presented human name paired one-to-one with the generated `NodeId`, not
a receiver-local name for a connection. It uses the lowercase DNS-label syntax
defined in `architecture.md`. The first implementation does not rename a node
or reuse its name for another `NodeId`.

`run` starts the same `oll` binary in the foreground. CLI `listen`/`connect`
values are temporary runtime overrides and do not rewrite Lua configuration.
It also has a hidden internal `--pingback <loopback-address>` option used only by
`start`; this is not a second public daemon mode. `start` launches the daemon in
the background and verifies readiness with the nonce exchange specified in
`admin-api.md`. `stop` uses the configured Admin API to gracefully stop oll and
all child processes.

`status` reports the local `NodeName` prominently, its `NodeId`, and configured
connection targets. A target learned through `SyncHello` is displayed with the
remote node's protocol-declared `NodeName` and `NodeId`; a target whose first
handshake has not completed is displayed by URL as pending. `--json` selects the
machine-readable schema; human-readable output is the default.

## Replica commands

```text
oll replica inspect <document-path>
oll replica ops <document-path> [--limit <count>] [--format text|json]
oll replica export -o <snapshot>
oll replica import <snapshot>
oll replica snapshot inspect <snapshot> [--json]
oll replica snapshot verify <snapshot>
```

`--limit` must be greater than zero. The CLI does not enforce a snapshot file
extension; the format itself is defined in `snapshot-format.md`.

`replica import` replaces the node's one replica with the complete replica in
the snapshot; it is not a CRDT merge and never adds a second replica. Immediately
before submitting the import, an interactive client MUST separately ask the
user to confirm that the current replica has been exported to a backup snapshot
and that the destructive replacement should proceed. Either negative answer,
end-of-file, or inability to read interactive confirmation cancels the import
without sending the Admin request. The first implementation has no flag that
bypasses these confirmations.

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
`-n`/`--retries` must be greater than zero. `sync --log` views
`/var/log/oll/sync.log`; it is a log-view mode and conflicts with `node-name`
and `--retries`. It reads that fixed file locally and does not require node
configuration or an Admin connection.

## Plugin commands

```text
oll plugin install <git-url>
  [--rev <revision> | --branch <branch>]
  [--release | --source]
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

Source installation is the default. `--release` and `--source` conflict;
`--rev` and `--branch` conflict. A generic plugin call returns a job ID after the
plugin stage is implemented. Its arguments are shell-style UTF-8 argv strings;
the client preserves their order, duplicates, empty strings, and leading `-`
characters without parsing or inferring types.

`plugin --log` reads `/var/log/oll/plugin.log` locally, optionally filtering for
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
