# Configuration runtime

## Boundaries

oll has Lua sources with deliberately different contracts, typed plugin masks,
and two strict JSON identity records:

| File | Contract | Mutation owner |
| --- | --- | --- |
| `<config-root>/config.lua` | trusted executable LuaJIT module returning the effective daemon configuration | user; `oll init` creates the initial file |
| `<config-root>/plugins.lua` | literal-only data module returning desired plugin installations | user and oll plugin installer |
| `<config-root>/plugins/<plugin-id>.lua` | trusted live per-plugin values and closures | user only |
| `<config-root>/plugin-masks/<plugin-id>.toml` | strict typed publisher-manifest overrides | user only |
| `<config-root>/node.json` | strict versioned `NodeId`/`NodeName` record | user; `oll init` creates the initial file |
| `<config-root>/replica.json` | strict versioned `ReplicaId` record | user; replica initialization, bootstrap, or snapshot import creates it |

`config.lua` is a program. oll evaluates it and validates only its returned
value; local variables, functions, conditionals, and allowed module composition
may participate in computing that value. oll does not parse the source to infer
configuration assignments and does not read named Lua globals as configuration.

`plugins.lua` is different because oll must safely rewrite it. It retains the
restricted syntax, stable serialization, locking, and atomic replacement rules
in [plugin-packaging.md](plugin-packaging.md). Executing `config.lua` does not
weaken those rules.

A per-plugin Lua file is executable trusted configuration, but it is not part of
the daemon's returned node schema and oll never rewrites it. Its PluginId-derived
path, live-read behavior, function handles, and access boundary are defined in
[plugin-system.md](plugin-system.md). A typed mask is TOML rather than Lua and is
merged only through the field rules in
[plugin-packaging.md](plugin-packaging.md).

`node.json` is not executable Lua configuration, but it remains user-owned
deployment configuration. Its schema, UUID-v4 validation, initialization, and
recovery rules are in [node.md](node.md). It is intentionally separate from
`config.lua` so the daemon can validate its stable identity without executing
user code.

`replica.json` is absent in an uninitialized deployment. Its authority, atomic
activation journal, runtime edit behavior, and relationship to the SQL cache are
defined in [replica-store.md](replica-store.md).

## Module result

`config.lua` MUST return exactly one Lua table. The initial schema is:

```lua
return {
    format_version = 1,

    node = {
        replica_root = "/home/user/Documents/oll",
        replica_store = {
            driver = "sqlite",
            path = "/home/user/.local/share/oll/stores/<node-id>/replica.sqlite3",
        },
        log_dir = "/home/user/.local/state/oll",
        artifact_download_dir = "/home/user/Downloads/oll",
        listen = "0.0.0.0:17384",
        connect = {
            "oll://node-a.example.com:17384",
            "oll://[2001:db8::10]:17384",
        },

        -- Added manually by the user when sync topology is enabled.
        -- network_key = oll.read_network_key("/etc/oll/network.key"),
    },
}
```

`format_version` is required and initially must equal integer `1`. `node` is a
required table with these fields:

| Field | Type | Rule |
| --- | --- | --- |
| `replica_root` | string | Required non-empty UTF-8 OS path. |
| `replica_store` | table | Required tagged SQLite or PostgreSQL store configuration. |
| `log_dir` | string | Required non-empty UTF-8 OS path. |
| `artifact_download_dir` | string | Required non-empty UTF-8 OS path used for verified plugin artifact publication. |
| `listen` | string or `nil` | At most one local bind socket address with an explicit nonzero port. |
| `connect` | array of strings | Zero or more `oll://host:port` targets in declared order; the port is required. |
| `network_key` | raw Lua byte string or `nil` | Required only by an effective nonempty sync topology; no text normalization is applied. |

`listen` is parsed as a local `SocketAddr`; wildcard addresses are valid bind
targets, but port zero is not. A `connect` entry uses only the `oll` scheme and
must contain a DNS name, IPv4 address, or bracketed IPv6 address plus an
explicit nonzero port. User information, query, fragment, and a non-root path
are rejected. A wildcard listen address is never rewritten into a connect URL.

`node.replica_store` has exactly two valid shapes:

```lua
-- A local SQLite store.
replica_store = {
    driver = "sqlite",
    path = "/absolute/or/config-relative/replica.sqlite3",
}

-- A PostgreSQL store. oll.getenv keeps a password out of the file and argv.
replica_store = {
    driver = "postgres",
    url = oll.getenv("OLL_POSTGRES_URL"),
}
```

SQLite requires `path` and forbids `url`. PostgreSQL requires `url` and forbids
`path`. `driver` is exactly `"sqlite"` or `"postgres"`; a SQLite path is an OS
path and a PostgreSQL URL is a non-empty PostgreSQL connection URL. Unknown
fields in either tagged table, unknown top-level or `node` fields, and every
missing required field other than optional `network_key` are errors. This means
a hand-written configuration with a partial `node` table fails validation. oll
does not append defaults to an arbitrary Lua program after its `return`
statement. `oll init` instead writes a complete initial table with explicit
defaults and deliberately omits `network_key`, even when `--listen` or
`--connect` was supplied. The user must add a key before that topology can run.

Later stages may extend the versioned schema only after documenting their
ownership. `NodeIdentity` and `ReplicaId` are also not Lua configuration: they
reside in the separate user-editable `node.json` and `replica.json` records
rather than being inferred from Lua globals or a returned table.

`artifact_download_dir` is loaded only during daemon startup. Before plugin work
is admitted, its resolved value is stored in the deployment-local SQL plugin
state; artifact publication reads that cached authority. Editing `config.lua`
does not hot-reload the directory and takes effect after the next daemon start.

oll converts the validated node table into a Rust-owned `ResolvedNodeConfig`
before starting the node runtime. Node, replica, sync, and plugin code consume
this typed value and do not read `mlua::Table` values directly. The Lua state
and its returned configuration table remain owned by the configuration
component for the daemon lifetime so later plugin configuration closures can be
registered without rebuilding another configuration model.

## Evaluation

The only supported implementation is LuaJIT embedded through `mlua`. Each
daemon start creates a fresh Lua state and evaluates `config.lua` once. Reload
is not part of the first implementation. A future reload must build and fully
validate a new state before atomically replacing the active configuration; it
must not mutate globals in an existing state field by field.

The runtime exposes normal Lua computation but is not an unrestricted native
extension host. It MUST NOT load the LuaJIT FFI or debug library, native Lua
modules, arbitrary-path loaders, or shell-execution APIs. A controlled `require`
may load UTF-8 Lua source modules contained beneath the config root, with path
containment checked before execution. A module name such as `foo.bar` maps to
`<config-root>/foo/bar.lua`; each dot-separated segment may contain only ASCII
letters, digits, `_`, or `-`. Modules are cached by their declared name and a
cyclic dependency is a configuration error. Symlinks are resolved for the
containment check and cannot load a file outside the config root.

The read-only `oll.getenv(name)` helper exposes environment lookup without
exposing mutation of the process environment. A missing variable returns
`nil`, a UTF-8 value returns a Lua string, and a present non-UTF-8 value is a
configuration evaluation error.

The read-only `oll.read_network_key(path)` helper reads a network key from one
absolute operating-system path. The path is not resolved relative to the config
root. On Unix the Lua string supplies the native pathname bytes; it must be
absolute and NUL-free. The helper reads a regular file exactly and returns one
raw Lua byte string. A trailing newline, leading whitespace, embedded NUL, and
all other file bytes are key material: oll does not trim, normalize Unicode, or
auto-detect hex/base64. Missing, unreadable, non-regular, or relative paths are
configuration evaluation errors. This helper is the supported file-based secret
input; there is no special dotenv parser.

Because the working tree is replicated user content, operators SHOULD keep a
network-key file outside `replica_root`. oll does not infer secret status from an
arbitrary user path or silently exclude a working-tree file from the catalog.

After CLI overrides are applied, `network_key` is required when `listen` is not
`nil` or `connect` is nonempty. It is ignored when both are absent, and in that
case it may be omitted. Any byte string, including an empty or obviously weak
one, is accepted because the deployment user owns this trust decision. A value
shorter than 32 bytes emits one redacted `WARNING` on daemon stdout and one
structured `WARN` after log sinks open; the value and its derived key are never
included. This length warning is only a heuristic: a
32-byte repeated string can still be weak. File permissions are not forced to
`0600`.

Exactly 32 input bytes are used directly as the Noise PSK. Every other length is
an HKDF-SHA256 input keying material value with these exact byte strings:

```text
salt = b"oll-sync-network-key\0v1"
info = b"oll-sync-noise-psk\0v1\0" ||
       b"Noise_NNpsk0_25519_ChaChaPoly_BLAKE2s"
output length = 32 bytes
```

`NodeId`, `ReplicaId`, addresses, and schema fingerprints do not participate.
Future uses of the configured key must define a different `info` label rather
than reuse the Noise output. Network-key bytes and HKDF intermediates are never
exposed through typed status or Admin APIs and are zeroized when their owning
Rust state is dropped where the type permits it.

Configuration is trusted user code, not a security sandbox. These restrictions
still protect daemon integrity and make startup behavior diagnosable; they do
not make arbitrary user code safe or terminating. oll does not install an
instruction hook, impose an instruction-count or wall-clock deadline, or apply
a configuration-specific memory ceiling. LuaJIT's JIT compiler remains enabled.
An infinite loop, unbounded recursion, or unbounded allocation in `config.lua`
or a required module can therefore occupy the process indefinitely or exhaust
its resources. This is an accepted consequence of treating the configuration
author as trusted.

No node service, Tokio task, log sink, Admin socket, replica, or network
listener starts until evaluation and schema conversion succeed. The
single-instance lock is intentionally acquired before evaluation; it is a
process-ownership guard, not a node service. Ordinary Lua and schema failures
before file logging is available are written to stderr. Code that never returns
does not produce an execution-limit error because no such limit exists.

## Per-plugin Lua evaluation

The daemon uses the same LuaJIT state and registry for `config.lua`, controlled
modules, per-plugin files, and their closures. It does not create a fresh Lua
runtime for each plugin request and therefore does not add a runtime generation
to a function handle.

For every top-level plugin configuration read, oll resolves the caller's
immutable PluginId to `<config-root>/plugins/<plugin-id>.lua`, reopens that final
path, applies the same symlink-resolved config-root containment rule, evaluates
its current source, and converts the requested value. The file is the only
per-plugin configuration authority; its values are not copied into
SQL or loaded eagerly at daemon startup. A missing file is equivalent to no
plugin configuration only when the requesting operation permits an absent
value; malformed source or an invalid returned value is a request error. The
module returns exactly one representable value. An empty `ConfigPath` selects
that complete value; a key selects a string-keyed map entry and an index selects
a zero-based `ConfigList` element. Missing keys and out-of-range indexes are
`NOT_FOUND`, while applying a segment to another value kind is
`INVALID_ARGUMENT`.

The controlled `require` loader remains available, so a user file may compose
other config-root Lua modules. Required modules keep the cache-by-module-name
semantics described above; the top-level per-plugin file itself is always
reopened. The plugin wire API cannot choose another filename or directly read
`config.lua`, `plugins.lua`, a sibling plugin file, or a mask. Any such
composition is an explicit act of the user's Lua source.

Closures returned to a plugin are stored in the shared registry under the
active `session_id` and a newly allocated `function_id`. Resolution requires
both values. Session teardown releases all of that session's handles; a handle
from an earlier process instance is invalid even though the daemon's Lua state
still exists. Values and closure internals are never logged.

`ConfigValue` validation is recursive in both directions. The root is depth
zero and depth 33 is the deepest accepted value; numbers must be finite, and
timestamps and durations must be inside their protobuf domains. A function
argument is accepted only when its `session_id` names the current active
session and its `function_id` still names an entry in that session's Lua
registry. Durable values and log fields reject function handles. Depth 33 is
the common list/map limit that remains inside prost's default decode-recursion
guard; oll does not enable prost's `no-recursion-limit` feature.

Lua's unannotated empty table `{}` deterministically converts to an empty
`ConfigMap`. A wire `ConfigList`, including an empty one, preserves its list
type when it passes through Lua and the same table is returned. Nonempty
contiguous positive-integer literal tables remain the unambiguous Lua list
form; version 1 exposes no literal syntax for an empty `ConfigList`.

Per-plugin files and invoked closures have the same trusted-code/no-instruction-
hook policy as `config.lua`. They execute on the serialized Lua owner and may
hang or exhaust resources if the user writes nonterminating code; oll does not
claim a timeout that LuaJIT cannot safely enforce.

## Path resolution and precedence

The process startup working directory is captured before dispatch. Relative
root paths from CLI options or `OLL_*` environment variables are joined to this
directory without checking existence or calling `canonicalize`. Absolute paths
remain unchanged. This applies to config, replica, and log roots. A relative
SQLite `replica_store.path` or `artifact_download_dir` returned by `config.lua`
is instead joined to the config root. A PostgreSQL store URL is not an OS path.

`oll init` has no existing deployment configuration. It resolves:

```text
config root:  --config  > OLL_CONFIG  > platform configuration directory / oll
replica root: --replica > OLL_REPLICA > platform Documents directory / oll
log dir:      --log-dir > OLL_LOG_DIR > platform state directory / oll
replica store: generated explicit SQLite path using the in-memory NodeId
artifact download dir: platform Downloads directory / oll
```

The config-root fallback is the platform configuration directory plus `oll`.
On Linux, the Documents fallback uses `XDG_DOCUMENTS_DIR` when configured and
otherwise `$HOME/Documents/oll`; the artifact location uses the platform user
Downloads directory; and the data and state fallbacks use the ordinary XDG
data/state locations. The platform directory helper provides corresponding
locations on Darwin. If a needed platform directory cannot be determined,
initialization fails rather than inventing a path.

The implementation obtains these platform locations through the `directories`
crate (`UserDirs` for Documents and Downloads, and `ProjectDirs` for
configuration, data, and state) rather than maintaining a second parser for
`user-dirs.dirs` or a table of Darwin paths. oll still appends its own `oll`
component and applies the fallback/error behavior above; the crate chooses only
the platform base.

The generated SQLite path is
`<platform-data-dir>/oll/stores/<generated-node-id>/replica.sqlite3`, as
defined in [replica-store.md](replica-store.md). `init` writes the resolved
absolute replica root, store path, log directory, and artifact download
directory into the initial `config.lua`. A relative path returned by a
hand-written `config.lua` is instead resolved relative to the config root, never
relative to the daemon's current working directory. Because persisted
filesystem paths are Lua strings, an `init` replica root, log directory,
artifact directory, or SQLite path that cannot be represented as UTF-8 is
rejected as a configuration error. The config root itself and document/snapshot
arguments remain native `PathBuf` values and are not subject to this
persisted-config restriction.

`oll run` first resolves only the config root. The node runtime then takes the
deployment lock, validates `node.json`, evaluates `config.lua`, and applies
runtime overrides:

```text
replica root: --replica > OLL_REPLICA > config.lua node.replica_root
log dir:      --log-dir > OLL_LOG_DIR > config.lua node.log_dir
replica store: config.lua node.replica_store
artifact download dir: config.lua node.artifact_download_dir
listen:       --listen > config.lua node.listen
connect:      non-empty --connect list > config.lua node.connect
```

Runtime overrides do not rewrite `config.lua`. Omitting a topology option uses
the persisted value. Temporarily clearing a persisted topology value requires a
future explicit CLI operation; an empty string is never a clearing sentinel.
The current CLI surface has no generic string store override: choosing SQLite
versus PostgreSQL and supplying its required path or URL is the typed tagged
table above. A future CLI store option must construct exactly one of those
shapes rather than infer a backend from an ambiguous string. A user who wants
PostgreSQL writes that complete table before `run`.

After applying all runtime overrides, oll MUST validate the resolved local
storage layout before it initializes log sinks, opens the replica store, binds
the Admin socket, or starts the working-tree watcher. The working tree and every
daemon-managed filesystem location must be disjoint:

- `config_root` and `replica_root` MUST NOT be equal, and neither may be an
  ancestor of the other;
- `log_dir` and `replica_root` MUST NOT be equal, and neither may be an ancestor
  of the other;
- the derived plugin data root and `replica_root` MUST NOT be equal, and neither
  may be an ancestor of the other;
- `artifact_download_dir` and `replica_root` MUST NOT be equal, and neither may
  be an ancestor of the other, because published artifacts are local job output
  rather than replicated documents;
- for a SQLite store, the database's management directory (the configured
  database file's immediate parent, which also owns SQLite journal, WAL, shared
  memory, and temporary siblings) and `replica_root` MUST NOT be equal, and
  neither may be an ancestor of the other.

These checks compare normalized filesystem locations rather than only the
original strings. Existing symlinked ancestors are resolved for the comparison
so two spellings of the same or nested location cannot bypass isolation; this
does not rewrite the configured paths or require their final components to
already exist. PostgreSQL has no local store path to compare. `oll init`
applies the same checks before creating the corresponding deployment
directories. This prevents configuration, Admin runtime files, logs, package
generations, artifact output, SQLite files, or a working tree nested beneath
their management directories from ever entering the recursive watcher namespace.

`oll start` resolves its config root to an absolute path before detaching and
passes that path to the `oll run` child. The child must not reinterpret a
relative deployment path from the launcher's working directory. It can precheck
the deployment lock without executing `config.lua`; the spawned child owns the
real lock before evaluating it. Administrative clients resolve only the config
root needed to locate the Admin UDS and do not execute `config.lua`. Local
snapshot and log clients retain the intent-specific dependencies defined in
[cli.md](cli.md).

## Errors and tests

Missing or unreadable files, Lua syntax failures, evaluation failures, a
non-table or multiple return values, unsupported format versions, unknown or
missing fields, invalid tagged-store combinations, invalid field types, invalid
`oll://` targets or socket addresses, a topology without `network_key`, invalid
paths, and overlapping local storage locations are configuration errors and
exit with `EX_CONFIG` (`78`).
Diagnostics identify the config file and field path without printing Lua values
that may contain secrets. In particular they never print a network-key value,
file content, derived PSK, or HKDF intermediate.

Tests cover successful computed returns, wrong return arity and type, schema
errors including both store variants and missing fields, controlled module
containment, CLI and environment precedence, relative-path bases, and a
HOME-less deployment whose absolute config root supplies its persisted replica,
store, log, and artifact paths. They also cover equality, both ancestor
directions, and existing symlink aliases between the working tree and each local
daemon-managed location, including layouts produced by runtime overrides and
`oll init`. Sync configuration tests cover raw Lua byte strings, exact key-file
bytes including a trailing newline, weak-key warning redaction, direct 32-byte
use, the specified HKDF vector for every other length, omitted-key topology
failure, and strict explicit-port `oll://` parsing. Tests do not execute
intentionally non-terminating Lua in-process.

Plugin configuration tests cover a disk edit observed by the next top-level
read, shared-registry closure invocation by `session_id + function_id`, stale
session rejection and cleanup, controlled `require` containment, artifact
download-directory startup-only behavior, and redaction of returned values.

## Troubleshooting a startup that never becomes ready

If `oll run` does not become ready, creates no Admin socket, emits no node log,
and neither exits with `EX_CONFIG` nor prints a configuration diagnostic, it may
still be evaluating `config.lua`. A compute loop commonly keeps one CPU core
busy; a loop that grows a table or string also causes the process's memory use
to keep increasing. For example, this configuration never reaches its return:

```lua
local result = {}

while true do
    table.insert(result, "x")
end

return result
```

`oll start` has the same underlying symptom: the child never completes its
readiness handshake, so the launcher reaches its startup deadline, terminates
that unready child, and reports startup failure. This deadline protects the
launcher contract; it is not a Lua execution limit. A foreground `oll run`
must instead be terminated by the user or the process manager.

To confirm the cause, run the same deployment in the foreground with `oll run
--config <config-root>`. Inspect `config.lua` and every module it requires for
loops, recursion, or computations whose input can grow without a bound. Then
temporarily reduce the file to a literal table conforming to the schema above.
If that version reaches readiness, restore the computation incrementally until
the non-terminating expression or required module is isolated. The correction
belongs in the Lua program; oll deliberately does not interrupt it with a hook.

## Embedded LuaJIT build

LuaJIT is a build-time native dependency of `oll`, not a separately installed
runtime program. The Rust dependency enables the `luajit` and `vendored`
features of `mlua`. Through `mlua-sys`, the `luajit-src` crate supplies the
upstream LuaJIT sources and builds a static library, which is linked into the
platform-specific `oll` executable. A released `oll` binary MUST NOT require a
system `lua` or `luajit` executable or a separately installed LuaJIT shared
library.

The oll repository does not maintain a second LuaJIT source mirror and does not
need `cargo vendor` merely to embed the runtime. `Cargo.lock` and the crate
checksum select the dependency source used by normal builds. A complete Cargo
vendor directory remains an optional release-engineering choice for offline or
audited builds, not part of the Lua embedding architecture. A local
`[patch.crates-io]` override is justified only when oll must carry a LuaJIT or
`luajit-src` patch that is unavailable upstream.

`luajit-src` bridges Cargo's native-build environment to LuaJIT's build system.
It builds LuaJIT's generator programs for the build host and builds the VM and C
API for Cargo's target, producing static `libluajit.a` or `lua51.lib` output.
The build environment still provides the host C compiler and the target C
compiler, linker, archiver, C runtime, sysroot or platform SDK, and `make` where
required. These are release-build inputs, not oll configuration fields.

The first node runtime supports Linux and Darwin only. LuaJIT's ability to build
for Windows does not make Windows an oll deployment target until its named-pipe,
process, and lifecycle contract is designed. Linux GNU and musl artifacts may
use target-specific containers or a tested cross toolchain; Darwin artifacts
should build on Darwin with the matching Apple SDK. Every supported target MUST
compile and test the Rust executable and the embedded LuaJIT together; compiling
only the Rust portion does not establish support for that target.
