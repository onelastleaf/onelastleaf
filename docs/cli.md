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
`run` child; all other operational clients send typed requests to the Admin UDS.
See `admin-api.md`.

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

## Node commands

```text
oll init
oll init --profile server
oll init --profile client --connect https://oll.example.com
oll init --profile server --listen 127.0.0.1:7443
oll init --replica /path/to/replica/root

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

`run` starts the same `oll` binary in the foreground. CLI `listen`/`connect`
values are temporary runtime overrides and do not rewrite Lua configuration.
It also has a hidden internal `--pingback <loopback-address>` option used only by
`start`; this is not a second public daemon mode. `start` launches the daemon in
the background and verifies readiness with the nonce exchange specified in
`admin-api.md`. `stop` uses the configured Admin API to gracefully stop oll and
all child processes.

`status` reports node state and configured connection targets. `--json` selects
the machine-readable schema; human-readable output is the default.

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

Replica document and snapshot path arguments are OS paths represented by
`PathBuf`. Before an Admin request is constructed, the client captures its
startup working directory and joins it to each relative path. Absolute paths
are passed through unchanged; the client does not check existence or call
`canonicalize`, and the daemon working directory is never used to reinterpret
them. Replica handlers later verify root containment and convert document paths
to the normalized replica namespace. Snapshot import, export, inspect, and
verify apply the same client-working-directory rule, with their operation
specific output or input checks performed by the handler.

## Sync commands

```text
oll sync
oll sync <node-name>
oll sync <node-name> -n 3
oll sync --log
oll ping <node-name>
```

`node-name` is obtained from `oll status`. `-n`/`--retries` must be greater than
zero. `sync --log` views `/var/log/oll/sync.log`; it is a log-view mode and
conflicts with `node-name` and `--retries`.

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

Plugin `stop` uses the same graceful `ShutdownRequest` semantics documented in
`plugin-system.md`; it does not introduce a stronger kill operation.

## Job commands

```text
oll job list
oll job info <job-id>
oll job stop <job-id>
```

`job stop` uses the same graceful plugin-process shutdown path as plugin stop,
kill, timeout, and `killjob`. It does not promise rollback of completed writes or
external effects.
