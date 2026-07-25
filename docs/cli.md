# Command-line interface

## Scope and stage behavior

`oll` is the only executable. The CLI schema is defined before node, replica,
sync, and plugin implementations so their public command surface can be tested
without fake side effects.

During the CLI-only implementation stage, operational commands parse and
validate their arguments, then fail with exit code `69` (`EX_UNAVAILABLE`) and
name the required implementation stage. They MUST NOT create files, start a
daemon, connect to a peer, or mutate a replica. Clap syntax errors use exit code
`2`.

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
`start` will launch the daemon in the background. `stop` will use the configured
admin API to gracefully stop oll and all child processes.

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
oll plugin call <plugin-id> <method> [arguments...]
```

Source installation is the default. `--release` and `--source` conflict;
`--rev` and `--branch` conflict. A generic plugin call returns a job ID after the
plugin stage is implemented.

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
