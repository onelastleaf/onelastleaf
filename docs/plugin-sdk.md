# Plugin SDKs and project generation

## Scope

Official plugin SDKs hide the `PluginEnvelope` state machine behind an
idiomatic action API. All SDKs implement the same protocol behavior and plugin
capabilities, but their public source APIs follow their language's ordinary
concurrency, error, and package conventions. Semantic equivalence is enforced
by one language-neutral conformance suite; source-level API uniformity is not a
goal.

The initial SDK targets are:

| Repository | Languages | Distribution identity |
| --- | --- | --- |
| `dotnet-plugin-sdk` | C#/.NET | NuGet `Onelastleaf.PluginSdk` |
| `cpp-plugin-sdk` | C++ | CMake package `onelastleaf-plugin-sdk`, target `onelastleaf::plugin_sdk` |
| `go-plugin-sdk` | Go | module `github.com/onelastleaf/go-plugin-sdk` |
| `jvm-plugin-sdk` | Java, Kotlin, Scala, Clojure | Maven group `org.onelastleaf` with language-specific artifacts |
| `node-plugin-sdk` | Node.js JavaScript and TypeScript | npm `@onelastleaf/plugin-sdk` |
| `python-plugin-sdk` | Python | Python distribution `onelastleaf-plugin-sdk`, import `onelastleaf_plugin_sdk` |
| `rust-plugin-sdk` | Rust | crate `onelastleaf-plugin-sdk` |
| `swift-plugin-sdk` | Swift | SwiftPM product `OnelastleafPluginSDK` |
| `elixir-plugin-sdk` | Elixir | Hex package `onelastleaf_plugin_sdk` |
| `haskell-plugin-sdk` | Haskell | Hackage package `onelastleaf-plugin-sdk` |

The repositories are maintained independently under the `onelastleaf` GitHub
organization and checked out together beneath a local `oll-plugin-sdk`
development directory. C, Lua, Objective-C, and OCaml are not initial SDK
targets. In particular, the available OCaml gRPC release does not provide the
bounded, backpressured long-lived bidirectional client required by this
protocol; oll does not maintain a private fork to fill that gap.

## Shared runtime contract

Every SDK MUST:

- read the oll-owned loopback endpoint only from `OLL_PLUGIN_ENDPOINT`, open
  `PluginRuntime.Connect` as the gRPC client, and keep the bidirectional stream
  open until shutdown or failure;
- continuously observe stdin as the parent-liveness pipe and exit promptly
  after EOF without treating stdin as application input;
- require nonempty session and instance IDs on the first `HostHello` envelope,
  adopt those outer fields as the stream's authoritative identity pair, and
  require the exact pair on every later envelope in either direction;
- validate `HostHello`, including the immutable PluginId and exact published
  schema fingerprint, and echo the effective PluginName supplied by the host so
  a user mask does not conflict with a publisher-compiled name;
- complete the `HostHello`/`PluginHello`/two-sided `SessionReady` handshake
  before admitting work;
- serialize writes through one ordered sender, accept inbound messages
  concurrently, and maintain a nonzero strictly increasing per-sender
  `message_id` without retaining an unbounded seen-ID set;
- preserve session/instance identity and the complete trace context across
  responses, nested host calls, jobs, cancellation, logs, and artifacts;
- answer a host heartbeat promptly with the same nonce, direct `reply_to`, and
  correlation context;
- admit multiple concurrent jobs, send `JobAccepted` before asynchronous job
  progress, and ensure a job-scoped `CancelJobRequest` cancels only that job;
- stop new admission on `ShutdownRequest`, acknowledge it, settle local work,
  and exit without inventing a process-kill operation;
- reject or terminate on malformed identity, ordering, depth, correlation,
  framing, or handshake state instead of silently repairing peer input;
- enforce the 64 MiB encoded-envelope receive limit and the artifact chunk
  limit advertised by `HostHello` before allocation or application dispatch;
- expose host document/configuration calls, structured logs, job results, and
  verified artifact transfer through the SDK rather than requiring plugin
  authors to construct envelope routing manually.

The SDK may expose generated protobuf document request/response types where
that is idiomatic, but it MUST NOT expose Loro internals. Configuration remains
host-owned: `GetConfig` asks oll to evaluate the caller's current per-plugin Lua
file and returns `ConfigValue`; a Lua closure is represented only by its
session-bound `ConfigFunctionRef`, and `InvokeConfigFunction` asks oll to run it.

## JVM and Node sharing

`jvm-plugin-sdk` has one Java core containing protobuf types, gRPC transport,
and the complete state machine. Its public core surface uses Java interfaces
and standard Java concurrency types, not Kotlin-specific values. Java consumes
the core directly. Kotlin, Scala, and Clojure artifacts are thin facades over
that same core and MUST NOT duplicate the state machine. The build and Maven
publication use Gradle Kotlin DSL. Initial artifact IDs are:

```text
onelastleaf-plugin-sdk-java
onelastleaf-plugin-sdk-kotlin
onelastleaf-plugin-sdk-scala
onelastleaf-plugin-sdk-clojure
```

Likewise, `node-plugin-sdk` contains one JavaScript runtime implementation and
ships TypeScript declarations. The JavaScript and TypeScript project templates
select different source/build defaults but depend on the same npm package.
Browser gRPC-Web is not a plugin runtime because it cannot open the required
local process stream.

## C++ consumption

`cpp-plugin-sdk` is an installable CMake package with a namespaced target. It is
not embedded into the oll executable and is not initially published through
Conan. A generated C++ plugin uses CMake `FetchContent` with an exact SDK tag or
commit, then links `onelastleaf::plugin_sdk`. Fetching happens during the
plugin's build, not during project generation. The SDK in turn presents normal
CMake dependency diagnostics for gRPC and protobuf.

## Protocol publication and versioning

Each SDK release contains or generates from the canonical `plugin.proto` and
its transitive imports, and embeds the exact full descriptor fingerprint
published by the matching oll build. SDK packages and generated projects pin
an exact initial SDK version rather than a floating branch or unconstrained
range. The manifest, `PluginHello`, generated protocol code, and SDK constant
MUST describe the same fingerprint.

The current protocol deliberately requires coordinated exact-fingerprint
upgrades. An SDK release therefore records its supported fingerprint in package
metadata and tests. A generator built from a development tree may scaffold the
source shape, but the generated dependency becomes installable only after the
matching SDK release is available from its stated package source.

## `oll plugin new`

The local project generator is:

```text
oll plugin new <path> --language <language>
  [--id <plugin-id>] [--name <plugin-name>]
```

Initial language values are:

```text
dotnet cpp go java kotlin scala clojure javascript typescript python rust
swift elixir haskell
```

The command is a bounded, pure local operation. It does not load deployment
configuration, connect to Admin, access a network, run Git, install a package,
or initialize a repository. Relative output is resolved from the CLI startup
directory. An explicit PluginId uses the normal dotted identity grammar. When
`--id` is omitted, oll generates one collision-resistant identity in the form
`generated.<uuid-v4>` and writes that same immutable value everywhere in the
project. An omitted name is derived from the final path component and accepted
only if it already satisfies the PluginName grammar.

The destination MUST NOT already exist. oll writes a private sibling candidate,
synchronizes the generated files, and publishes the completed directory with a
single no-replace rename. Failure removes the private candidate and never
modifies another path. Generated source contains no secrets and receives
ordinary user permissions subject to umask.

Every project contains a source-mode `oll.toml`, build/package metadata, an
idiomatic action entry point, one example `echo` action, tests, `.gitignore`,
and a short README. The generated manifest explicitly selects `source` checkout
for ordinary compiled projects, `install` checkout for Node.js and Haskell, and
`generation` checkout for Python. These are generator choices, not runtime
language detection. Registry-backed templates declare the matching published
SDK dependency and do not copy SDK implementation source. The C++ template uses
the pinned CMake `FetchContent` contract above. Templates do not create
`plugins.lua`, per-plugin Lua configuration, masks, `oll-release.json`, a
license chosen on the author's behalf, CI tied to one forge, or a Git history.

The generated `oll.toml` uses only the placeholders allowed by its declared
checkout and launches only from the published generation. Recipe commands
remain argv arrays with no shell. A generated project is an independent plugin
source tree, not a fork of a language-template repository.

## Conformance and release gate

The common conformance suite exercises at least:

1. endpoint validation, connection, exact handshake, and readiness;
2. ordered concurrent send/receive and the encoded receive bound;
3. echo job admission, progress, success, and structured result;
4. multiple simultaneous jobs and cancellation of exactly one job;
5. heartbeat response while jobs and host calls are pending;
6. configuration read/function invocation and one document host call;
7. structured log correlation and a complete chunked artifact transfer;
8. graceful shutdown, stream failure, stale-session rejection, and stdin EOF.

An SDK is not released merely because its package compiles. It must pass its
unit tests and the shared black-box suite against the matching oll protocol.
Unavailable local compilers or package tools may leave a target unverified on a
development machine, but that target remains incomplete and the repository
TODO remains unchecked until another environment runs the release gate.
