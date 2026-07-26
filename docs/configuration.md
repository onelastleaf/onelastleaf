# Configuration runtime

## Boundaries

oll has two Lua files with deliberately different source contracts:

| File | Contract | Mutation owner |
| --- | --- | --- |
| `<config-root>/config.lua` | trusted executable LuaJIT module returning the effective daemon configuration | user; `oll init` creates the initial file |
| `<config-root>/plugins.lua` | literal-only data module returning desired plugin installations | user and oll plugin installer |

`config.lua` is a program. oll evaluates it and validates only its returned
value; local variables, functions, conditionals, and allowed module composition
may participate in computing that value. oll does not parse the source to infer
configuration assignments and does not read named Lua globals as configuration.

`plugins.lua` is different because oll must safely rewrite it. It retains the
restricted syntax, stable serialization, locking, and atomic replacement rules
in [plugin-packaging.md](plugin-packaging.md). Executing `config.lua` does not
weaken those rules.

## Module result

`config.lua` MUST return exactly one Lua table. The initial schema is:

```lua
local replica = "/home/user/.local/share/oll"

return {
    format_version = 1,

    node = {
        replica_root = replica,
        log_dir = "/home/user/.local/state/oll",
        listen = "127.0.0.1:7443",
        connect = {
            "https://node-a.example.com",
            "https://node-b.example.com",
        },
    },
}
```

`format_version` is required and initially must equal integer `1`. `node` is a
required table with these fields:

| Field | Type | Rule |
| --- | --- | --- |
| `replica_root` | string | Required non-empty UTF-8 OS path. |
| `log_dir` | string | Required non-empty UTF-8 OS path. |
| `listen` | string or `nil` | At most one socket address. |
| `connect` | array of strings | Zero or more HTTP(S) connect URLs in declared order. |

Unknown top-level and `node` fields are errors so misspelled configuration does
not silently select another behavior. Later stages may extend the versioned
schema only after documenting their ownership. The initialization `profile` is
not a runtime authority field and is not persisted as part of this returned
schema. `NodeIdentity` is also not Lua configuration: oll stores its immutable
`NodeId`/`NodeName` pair in a separate host-owned identity record so executing
user code cannot rename or regenerate the node.

oll converts the validated node table into a Rust-owned `ResolvedNodeConfig`
before starting the node runtime. Node, replica, and sync code consume this
typed value and do not read `mlua::Table` values directly. The Lua state and its
returned configuration table remain owned by the configuration component for
the daemon lifetime so later plugin configuration closures can be registered
without rebuilding another configuration model.

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
listener starts until evaluation and schema conversion succeed. Ordinary Lua
and schema failures before file logging is available are written to stderr.
Code that never returns does not produce an execution-limit error because no
such limit exists.

## Path resolution and precedence

The process startup working directory is captured before dispatch. Relative
root paths from CLI options or `OLL_*` environment variables are joined to this
directory without checking existence or calling `canonicalize`. Absolute paths
remain unchanged. This applies to config, replica, and log roots.

`oll init` has no existing deployment configuration. It resolves:

```text
config root:  --config  > OLL_CONFIG  > HOME default
replica root: --replica > OLL_REPLICA > HOME default
log dir:      --log-dir > OLL_LOG_DIR > XDG_STATE_HOME/HOME default
```

It writes the resolved absolute replica root and log directory into the initial
`config.lua`. A relative path returned by a hand-written `config.lua` is instead
resolved relative to the config root, never relative to the daemon's current
working directory. Because these roots are persisted as Lua strings, an `init`
root that cannot be represented as UTF-8 is rejected as a configuration error.
Document and snapshot arguments remain native `PathBuf` values and are not
subject to this persisted-config restriction.

`oll run` first resolves only the config root, evaluates `config.lua`, and then
applies runtime overrides:

```text
replica root: --replica > OLL_REPLICA > config.lua node.replica_root
log dir:      --log-dir > OLL_LOG_DIR > config.lua node.log_dir
listen:       --listen > config.lua node.listen
connect:      non-empty --connect list > config.lua node.connect
```

Runtime overrides do not rewrite `config.lua`. Omitting a topology option uses
the persisted value. Temporarily clearing a persisted topology value requires a
future explicit CLI operation; an empty string is never a clearing sentinel.

`oll start` resolves its config root to an absolute path before detaching and
passes that path to the `oll run` child. The child must not reinterpret a
relative deployment path from the launcher's working directory. Administrative
clients resolve only the config root needed to locate the Admin UDS and do not
execute `config.lua`. Local snapshot and log clients retain the intent-specific
dependencies defined in [cli.md](cli.md).

## Errors and tests

Missing or unreadable files, Lua syntax failures, evaluation failures, a
non-table or multiple return values, unsupported format versions, unknown
fields, invalid field types, invalid URLs or socket addresses, and invalid paths
are configuration errors and exit with `EX_CONFIG` (`78`). Diagnostics identify
the config file and field path without printing Lua values that may contain
secrets.

Tests cover successful computed returns, wrong return arity and type, schema
errors, controlled module containment, CLI and environment precedence,
relative-path bases, and a HOME-less deployment whose absolute config root
supplies its persisted replica and log paths. Tests do not execute intentionally
non-terminating Lua in-process.

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

Release CI SHOULD build Windows artifacts on Windows with the Visual Studio
toolchain and Darwin artifacts on Darwin with the matching Apple SDK. Linux GNU
and musl artifacts may use target-specific containers or a tested cross
toolchain. Every supported target MUST compile and test the Rust executable and
the embedded LuaJIT together; compiling only the Rust portion does not establish
support for that target.
