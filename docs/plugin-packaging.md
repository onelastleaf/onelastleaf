# Plugin packaging and installation

## Scope and file ownership

Plugins may be implemented in any language that can use the protobuf gRPC
client contract. oll does not infer a language or classify a plugin as compiled
or interpreted. A source package declares commands that prepare an install
tree; a release package declares downloadable ready-to-launch artifacts. Both
publish the same effective runtime manifest.

The package contract has four files with separate owners:

| File | Owner | Purpose |
| --- | --- | --- |
| repository-root `oll.toml` | publisher | immutable identity, display name, source recipe, and runtime entrypoint |
| repository-root `oll-release.json` | publisher | opaque releases and direct artifact URLs, targets, sizes, and hashes |
| `<config-root>/plugins.lua` | user and oll CLI | desired installation declarations |
| `<config-root>/plugin-masks/<plugin-id>.toml` | user | typed overrides for permitted publisher-manifest fields |

Per-plugin runtime values and closures live in
`<config-root>/plugins/<plugin-id>.lua` and are not package declarations. Their
execution contract is defined in [plugin-system.md](plugin-system.md).

`PluginId` and `PluginName` use the strict grammars in
[plugin-storage.md](plugin-storage.md). One installed ID is immutable until
removal. A mask may change its effective name, but names remain globally unique
within the deployment.

## System Git and source selection

oll invokes the user's installed `git` executable as an argv program. It does
not embed a Rust Git implementation and does not invoke a shell. This preserves
the user's normal Git configuration, SSH agent, credential helpers, proxy
settings, and transport support. Missing `git` is an installation diagnostic.
Credentials and credential-bearing remotes are never copied into structured
logs.

`GitRemote` accepts Git URL forms and SCP-style SSH remotes, including:

```text
https://github.com/example/oll-anki.git
ssh://git@github.com/example/oll-anki.git
git@github.com:example/oll-anki.git
```

The original spelling is retained for Git. `branch` and `rev` are mutually
exclusive. Without either, Git's remote default branch is selected. A branch or
default-branch declaration records the resolved commit but remains updateable;
an exact revision is pinned and `plugin update` reports it already satisfied.
Git tags and semantic-version ranges have no oll meaning.

Git fetches use a private staging checkout. oll does not adopt a pre-existing
user checkout, run a hosting-platform Release API, or parse Git command output
as a package-manager protocol beyond the exit status and the exact commit it
asked Git to resolve.

Each Git child inherits the daemon user's environment, runs without a shell as
the foreground leader of its own Unix process group, and writes stdout/stderr to
the operation build log with stdin closed. oll sets no fixed Git timeout.
Cancellation uses the same `SIGTERM`, bounded grace, `SIGKILL`, and reap
sequence as a source recipe process group. Git receives no plugin runtime
endpoint or liveness stdin contract.

## Publisher manifest

`oll.toml` is strict UTF-8 TOML with explicit format version `1`. Its initial
logical shape is:

```toml
format_version = 1

[plugin]
id = "oll.anki"
name = "oll-anki"
protocol_fingerprint = "hex-encoded-schema-fingerprint"

[[source.dependencies]]
executable = "cargo"
hint = "Install the Rust toolchain and ensure cargo is in PATH."

[[source.steps]]
argv = ["cargo", "install", "--locked", "--path", "{source}", "--root", "{install}"]

[runtime]
argv = ["{install}/bin/oll-anki"]
```

The manifest has no language or compiled/interpreted enum. An interpreted
plugin declares the interpreter in its dependency, source steps, and runtime
argv. `plugin.id`, `plugin.name`, the exact protocol fingerprint, and a nonempty
runtime argv are required. Fingerprints are lower-case 64-character SHA-256
hex and must match the running oll descriptor. Once installed, every later
fetch for that declaration must present the same ID; an upstream ID change is a
manifest failure and cannot silently install a second identity or move state.

Commands are argv arrays, never shell programs. Placeholder replacement occurs
inside individual argv values and recognizes only:

- `{source}`: the private selected Git checkout;
- `{staging}`: the private operation staging root;
- `{install}`: the candidate install-generation root, not the public `current`
  symlink;
- `{mask_dir}`: the directory containing the selected user mask.

An absent mask still gives `{mask_dir}` its deterministic
`<config-root>/plugin-masks` parent. Unknown or malformed placeholders are
errors. There is no shell expansion, command substitution, glob expansion, or
environment interpolation. Source steps may use all four placeholders. Runtime
argv may use only `{install}` and `{mask_dir}` because source/staging trees do
not survive package publication.

Dependencies declare executable capabilities only. A basename is resolved using
the inherited PATH rules; an absolute path is checked directly; a relative value
containing a path separator is rejected. oll does not invoke a system package
manager, execute `--version`, parse version output, or enforce a dependency
version. A missing executable aborts that plugin and displays its required
nonempty hint.

Each source recipe step:

- runs with the selected source directory as its working directory;
- inherits the daemon user's complete environment;
- has stdin closed;
- writes stdout and stderr to that install operation's retained build log;
- runs as the foreground leader of a distinct Unix process group;
- succeeds only after normal exit with status zero.

Source recipes are trusted code and can read inherited environment values. oll
does not elevate them or set a fixed build timeout. Admin cancellation, daemon
shutdown, or a later fatal package error sends `SIGTERM` to the complete step
process group, waits through a bounded grace capped by the node's absolute
shutdown deadline, then uses `SIGKILL` and reaps it. A recipe that daemonizes or
leaves work outside its process group violates the package contract.

After every step, oll validates the candidate without executing its runtime
command. Runtime `argv[0]` may be an executable found through inherited PATH or
a path beneath `{install}`/`{mask_dir}`; a relative pathname containing a
separator is resolved from the install generation and must remain contained.
Any other runtime argv value produced from a path placeholder is checked for
the same containment but is not interpreted to guess whether it must already
exist. This supports both a compiled binary and a
system interpreter plus an installed script without a language enum.

At process spawn, oll invokes this argv directly without a shell, uses the
published install generation as working directory, inherits the daemon user's
environment, and overwrites any inherited `OLL_PLUGIN_ENDPOINT` with its own
loopback endpoint. stdin, stdout/stderr, foreground/process-group, and shutdown
semantics are defined in [plugin-system.md](plugin-system.md). Publication and
crash recovery follow
[plugin-storage.md](plugin-storage.md); recipe success never writes through the
public `current` pointer directly.

## Typed user masks

A mask is optional strict UTF-8 TOML with `format_version = 1`. It is parsed into
a separate typed structure; oll never performs a generic TOML tree merge. Its
initial shape permits only these optional overrides:

```toml
format_version = 1

[plugin]
name = "personal-anki"

[[source.dependencies]]
executable = "/usr/bin/cargo"
hint = "Use the system Cargo installation."

[[source.steps]]
argv = ["{mask_dir}/build-anki", "{source}", "{install}"]

[runtime]
argv = ["{install}/bin/oll-anki", "--profile", "personal"]
```

For each permitted scalar field, presence replaces the publisher value and
absence retains it. A table header only scopes its typed child fields; it does
not erase publisher siblings that the mask omits. An array field such as
`source.dependencies`, `source.steps`, or `runtime.argv` is replaced in full
when present; array elements are never matched or merged. Unknown fields are
errors.

A mask cannot contain or replace `plugin.id`, `protocol_fingerprint`, release
IDs, artifact target, URL, archive kind, size, SHA-256, or other artifact
integrity metadata. It may replace `plugin.name`, which is accepted only if the
result remains unique. oll parses the publisher manifest and mask separately,
rejecting syntax, type, unknown-field, and forbidden-field errors in their own
schemas. Required immutable publisher identity/integrity fields cannot come
from a mask. oll then builds one effective manifest and performs complete
required-field, command, placeholder, uniqueness, and cross-field validation on
that result.

Mask files are user-owned and are not rewritten or watched. Existing symlinked
ancestors/final targets are resolved and must remain beneath the mask directory.
A changed mask is applied by the next install, update, or explicit
reconciliation that builds a new candidate. It does not mutate an already
published generation in place.

## Release index

`oll-release.json` is strict UTF-8 JSON read from the same selected Git state as
`oll.toml`. Release IDs are publisher-defined nonempty opaque UTF-8 strings and
are compared exactly; oll does not parse semantic versions or infer an ID from
a filename or URL.

```json
{
  "format_version": 1,
  "plugin_id": "oll.anki",
  "protocol_fingerprint": "hex-encoded-schema-fingerprint",
  "releases": {
    "v0.3.1": {
      "artifacts": [
        {
          "target": "x86_64-unknown-linux-gnu",
          "url": "https://downloads.example/oll-anki-v0.3.1-x86_64-unknown-linux-gnu.tar.gz",
          "archive": "tar.gz",
          "size_bytes": 123456,
          "sha256": "hex-encoded-sha256"
        }
      ]
    },
    "v0.4.0": {
      "artifacts": []
    }
  }
}
```

Duplicate JSON object keys are invalid. The index PluginId and fingerprint must
match the publisher manifest. `oll plugin releases <selector>` returns the
opaque IDs in bytewise order and the canonical targets declared for each. It
does not choose a newest release.

A release-mode declaration names one exact release. Exactly one artifact in
that release must match the local canonical target; zero and duplicate matches
are errors. Initial target names include:

```text
x86_64-unknown-linux-gnu
x86_64-unknown-linux-musl
aarch64-unknown-linux-gnu
x86_64-apple-darwin
aarch64-apple-darwin
```

Windows target strings may be introduced only when the node process platform is
supported; the first implementation does not pretend that archive support alone
implements Windows process lifecycle.

Artifact URLs accept `https`, `http`, and `file`. HTTP redirects are followed
only while every resulting URL retains an allowed scheme. A `file` URL must
name an absolute local regular file and have no remote authority. Authentication
and transport confidentiality come from the selected URL and the user's normal
environment; SHA-256 verifies bytes but is not a publisher signature.

Linux release archives use `tar.gz`; Darwin releases use `zip`. The declared
kind and URL path suffix must agree. Before extraction oll verifies the exact
declared byte size and SHA-256 while streaming into private staging rather than
buffering the archive in memory. Extraction rejects absolute and `..` paths,
duplicate normalized entries, links escaping staging, and unsupported entry
types. There is deliberately no compressed or expanded archive-size limit; the
user initiating installation is responsible for local resources. An exhaustion
failure cannot publish a partial generation.

The archive includes the publisher `oll.toml`. After strict parsing, its complete
canonical publisher-manifest value must equal the repository `oll.toml`, and its
ID/fingerprint must also match the release index, before the typed mask is
applied. The effective runtime argv must pass the same executable resolution,
path containment, and required-file checks as a source candidate.

## Installation declarations

`<config-root>/plugins.lua` is the durable source of desired installations. It
contains no process desired state:

```lua
return {
    ["oll.anki"] = {
        remote = "git@github.com:example/oll-anki.git",
        mode = "release",
        branch = "releases",
        release = "v0.3.1",
    },
    ["oll.pdf"] = {
        remote = "https://codeberg.org/example/oll-pdf.git",
        mode = "source",
        rev = "0123456789abcdef",
    },
}
```

`mode` is `source` or `release` and defaults to `source`. `branch` and `rev` are
optional and mutually exclusive. `release` is forbidden in source mode. It may
be omitted from a release-mode declaration so the user can first run
`oll plugin releases <plugin-id>` against that declaration; install, update, or
reconcile then reports `release_selection_required` for that item without
publishing anything until one opaque ID is written. There is no process-state,
semantic-version range, tag, hosting-platform, or user mask field.

The file accepts one return statement containing a recursively literal table of
strings, booleans, integers, lists, and maps. Calls, operators, local bindings,
loops, conditions, functions, `require`, metatables, userdata, threads,
bytecode, cyclic tables, duplicate PluginIds, unknown fields, and values outside
the schema are errors. A syntax/AST restriction runs before LuaJIT evaluation;
recursive schema validation follows conversion.

oll mutations take the deployment-local package lock, serialize keys in stable
PluginId order, write and synchronize a sibling temporary file, atomically
rename it over `plugins.lua`, and synchronize the parent. A parse or validation
failure never rewrites the original. The daemon does not watch this file;
manual edits are applied only by an explicit command.

## Package commands and reconciliation

All package-changing commands enter one package reconciliation owner, but retain
typed operation modes rather than forwarding generic command strings:

```text
oll plugin install
oll plugin install <git-remote> [--branch <branch> | --rev <revision>]
  [--source | --release <release-id>]
oll plugin update <plugin-id-or-name>
oll plugin reconcile
oll plugin remove <plugin-id-or-name>
oll plugin releases <plugin-id-or-name>
oll plugin validate
```

- `install` without a remote installs missing declarations and rebuilds a
  declaration whose normalized declaration or mask input changed. It does not
  remove undeclared installations or advance an unchanged branch merely because
  its head moved.
- `install <remote>` resolves `oll.toml`, adds or replaces that PluginId's
  declaration, then installs only from the newly reread persisted declaration.
  A later package failure does not roll back the successful declaration write;
  a future install or reconcile retries it.
- `update` fetches the selected default branch or named branch and publishes a
  candidate only when the selected commit or local declaration/mask input
  changed. An exact `rev` never advances remotely, but a changed local mask can
  still produce a new generation. It never rewrites `plugins.lua` or restarts a
  process.
- `reconcile` makes the installed set exactly match `plugins.lua`: it installs
  missing declarations, applies declaration changes, and removes IDs absent
  from the file. It preserves every existing SQL desired process state and does
  not advance an otherwise unchanged branch.
- `remove` uses the durable removal owner in `plugin-storage.md` and removes the
  declaration as well as local state.
- `releases` reads the selected repository state and lists opaque release IDs;
  for a branch it fetches the current remote head in private staging, while a
  `rev` remains exact. It changes no declaration, recorded/current generation,
  or installation.
- `validate` is a bounded local command. It validates `plugins.lua` and every
  present typed mask without opening Admin, accessing a remote, or changing a
  file.

When a remote install finds another declaration for the same ID, the first
`ReconcilePluginInstallations` call returns `confirmation_required` together
with the current normalized declaration SHA-256. The CLI asks interactively.
A negative answer, EOF, or unavailable input changes nothing. A positive answer
calls the same RPC with the ID, expected digest, and overwrite authorization;
the daemon applies it only if the current declaration still has that digest.
There is no separate confirmation RPC and no daemon-side terminal prompt.
Structured `--json` mode never prompts: it returns the
`confirmation_required` item and exits nonzero, so a deliberate overwrite is
performed from an interactive human invocation.

No-argument install and exact reconciliation may process multiple plugins. Each
PluginId produces an independent success, already-satisfied, removed,
confirmation-required, or failed result plus diagnostics. A download or build
failure for one ID does not discard another ID's success. The CLI exits nonzero
when any requested item failed or remained unresolved. A global `plugins.lua`
parse failure prevents item admission because no coherent declaration set
exists. Before publishing a multi-item call, oll checks all resolved effective
names as one set; two requested IDs claiming the same new name both receive
`plugin_name_conflict` rather than making success depend on build completion
order. Concurrent independent calls remain serialized by the SQL unique
binding at publication.

`ReconcilePluginInstallations`, `RemovePlugin`, and `ListPluginReleases` have no
fixed client request deadline. Git, a source recipe, download, archive work, or
process cleanup may legitimately exceed ten seconds. Cancelling reconciliation
terminates an active system-Git or recipe process group, cancels an active
download, and discards unpublished work; items already atomically published
remain successful. A durable removal intent continues to recovery-safe
completion even if its waiting client disconnects.

## Diagnostics and output

Package operations expose stable diagnostics with a code, phase, PluginId/name
when known, sanitized remote, branch/revision, release, target, optional hint,
and retained build-log path. Initial codes include:

| Code | Meaning |
| --- | --- |
| `git_missing` | The system `git` executable is unavailable. |
| `git_remote_invalid` | The remote syntax is not accepted by `GitRemote`. |
| `git_fetch_failed` | Git could not fetch the selected state. |
| `git_selection_not_found` | The requested branch or revision does not exist. |
| `plugin_config_missing` | `plugins.lua` does not exist where required. |
| `plugin_config_syntax` | `plugins.lua` is not a literal-only data module. |
| `plugin_config_schema` | A declaration violates the schema. |
| `plugin_config_duplicate` | A PluginId appears more than once. |
| `manifest_missing` | Publisher `oll.toml` is absent. |
| `manifest_invalid` | Publisher `oll.toml` is malformed or inconsistent. |
| `manifest_version_unsupported` | A manifest format version is unsupported. |
| `mask_invalid` | A typed user mask is malformed or contains a forbidden override. |
| `plugin_name_conflict` | The effective PluginName is already bound to another ID. |
| `dependency_missing` | A declared executable is not resolvable in PATH. |
| `recipe_step_failed` | A source recipe did not exit normally with status zero. |
| `recipe_output_missing` | The effective runtime entrypoint was not produced. |
| `release_index_missing` | `oll-release.json` is absent in release mode. |
| `release_index_invalid` | The release index is malformed or inconsistent. |
| `release_selection_required` | A release declaration must select one listed opaque ID before installation. |
| `release_not_found` | The selected opaque release ID is absent. |
| `artifact_unavailable` | No artifact matches the canonical local target. |
| `artifact_ambiguous` | Multiple artifacts match the target. |
| `artifact_download_failed` | An HTTP, HTTPS, or file download failed. |
| `artifact_checksum_mismatch` | Declared size or SHA-256 verification failed. |
| `archive_unsafe` | Archive structure violates extraction constraints. |
| `entrypoint_invalid` | The effective runtime entrypoint is unusable. |
| `protocol_incompatible` | The package targets another protocol fingerprint. |
| `overwrite_confirmation_required` | A declaration changed without matching authorization. |
| `install_publish_failed` | Atomic package publication failed. |

Human output may use terminal-aware color, tables, and an indeterminate spinner
on stderr. `anstream` supplies terminal/`NO_COLOR` behavior; progress rendering
must disappear for non-terminals and structured output. Plugin list, info,
releases, reconciliation results, and job commands provide `--json`. In JSON
mode stdout contains exactly one stable JSON document with no ANSI sequences or
progress output. Diagnostics never expose credentials, inherited environment
values, build output, document content, or plugin configuration values.
