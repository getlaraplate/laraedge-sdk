# Plugin handshake and protocol

This document is for authors who write a Laraplate edge plugin in a language other than Go, and for anyone who wants
to know exactly what the Go SDK does on the wire. A Go plugin does not need it: `sdk.Serve` does everything described
here. It describes protocol `laraplate.edge.plugin.v1`, whose messages are the `.proto` files in
[`proto/laraplate/edge/plugin/v1/`](../proto/laraplate/edge/plugin/v1/) (the normative definition of every message
and field) and whose process handshake is implemented by package [`handshake`](../handshake/).

What you need besides the `.proto` files: the official gRPC library of your language (HTTP/2, no TLS, over a local
transport), the standard `grpc.health.v1` health service, and a JSON library.

Statements below that describe behaviour of the Go SDK rather than a rule of the protocol are marked as such. A plugin
in another language should behave the same way where the conformance suite checks it (see
[the conformance suite](#the-conformance-suite)).

## Life of a plugin

1. The host (the Laraplate edge agent) starts the plugin binary directly, never through a shell, with a minimal
   environment that carries the cookie, the protocols the host speaks and, on Unix, a private socket directory.
2. The plugin checks the cookie, picks a protocol, opens a Unix socket or a Windows named pipe that only the current
   user can reach, and prints one line on stdout, the handshake line.
3. The host reads the line, checks it, connects with gRPC and calls `Info` (5 s deadline) and then the `Configure`
   calls of the instances it needs.
4. The host uses the services the plugin offers (`Input`, `Processor`, `Output`) and polls the health service.
5. The host calls `Shutdown`; the plugin stops and exits with code 0. A plugin that does not exit in time is killed.

## Environment

The host sets these variables and passes no others of its own. The rest of the plugin's environment is whatever
the host's configuration lists (typically `PATH`, `TZ` and, on Windows, `SystemRoot`). Secrets never travel in the
environment: they arrive resolved inside the configuration of an instance (`Configure`).

| Variable | Value | Notes |
|---|---|---|
| `LARAPLATE_EDGE_PLUGIN_COOKIE` | `ea6f1c2b-laraplate-edge-plugin-v1` | Always this constant. It stops a plugin that someone runs by hand; it is not a security control. |
| `LARAPLATE_EDGE_PLUGIN_PROTOCOLS` | comma-separated protocol versions, preferred first, for example `v1` | Each entry is trimmed; empty entries are ignored. |
| `LARAPLATE_EDGE_PLUGIN_SOCKET_DIR` | absolute path of a directory | Unix only. Not set on Windows. |

The working directory of the plugin is the directory of its binary unless the host says otherwise. Do not rely on it:
the conformance suite starts every plugin with its working directory in a fresh temporary directory.

## Negotiation and exit codes

1. If `LARAPLATE_EDGE_PLUGIN_COOKIE` is not exactly the constant above, the plugin must exit at once with a non-zero
   code, print an explanation on stderr and print no handshake line. The Go SDK exits with code 1. The conformance
   check `no-cookie` runs the binary without the cookie and requires a non-zero exit within the timeout and no line
   starting with `1|` on stdout.
2. The plugin picks the first protocol in `LARAPLATE_EDGE_PLUGIN_PROTOCOLS` that it supports. Today the only protocol
   is `v1`.
3. If none is shared, the plugin writes the reason on stderr and exits with code **3**. The host reports an exit with
   code 3 before the handshake as "no protocol version in common".
4. Any other failure before the handshake line (no socket directory, a path that is too long, a transport that cannot
   be opened) is an exit with a non-zero code other than 3 and a reason on stderr. The Go SDK uses 1.

The Go SDK uses exit code 2 when the command line cannot be parsed and exit code 0 after a completed `Shutdown`.

## Transport

### Unix (Linux and other Unix systems)

- The host creates the socket directory with mode `0700` when it is missing and narrows it to `0700` when it is wider.
  It refuses a directory that is a symbolic link or belongs to another user. Before each launch it removes every
  `*.sock` file left in the directory.
- The plugin binds a Unix stream socket named `p<8 random lowercase hex digits>.sock` directly inside the socket
  directory, then sets the mode of the socket file to `0600` (after the bind, so it does not depend on the umask).
  The name must match this pattern: the removal of leftovers by the host relies on the `.sock` extension, and the
  conformance check `restart` fails when a killed plugin's socket survives the next launch.
- The whole socket path must not exceed the size of `sun_path` of the platform minus one: 107 bytes on Linux (the
  size is 108), 103 on darwin and the BSDs (104). A longer path is an error: the Go SDK exits with a message that
  names the length and the limit, and the host refuses the socket directory in the same case before it starts the
  plugin. Keep the socket directory short.
- The plugin should remove its socket when it exits normally (the Go SDK does, by closing its listener). A plugin that
  is killed cannot, which is why the host removes leftovers.

### Windows

- The plugin listens on a named pipe `\\.\pipe\laraedge-<pid>-<8 random lowercase hex digits>`, where `<pid>` is the
  plugin's process id. No socket directory is involved.
- The pipe has a protected DACL with exactly one entry that allows full access to the user of the plugin process: the
  security descriptor `D:P(A;;GA;;;<SID of the current user>)`. The Go SDK creates it with go-winio. The conformance
  check `permissions` reads the descriptor and fails when the DACL is not protected, has more than one entry, is not an
  allow entry, names another SID or does not grant full access.

## The handshake line

The plugin prints exactly one line on stdout, after the transport is ready to accept connections, and flushes it.
Nothing may precede it on stdout.

```text
line      = version "|" protocol "|" network "|" address LF
version   = "1"
protocol  = the negotiated protocol, for example "v1"
network   = "unix" | "npipe"
address   = the socket path (unix) | the pipe name (npipe)
```

How the host reads it (`handshake/host.go`):

- It reads the first line of stdout, at most 4096 bytes. The line must end with LF; a CR before the LF is tolerated.
  A longer line, or one without LF, is a bad line.
- The text is split on the first three `|`; everything after the third belongs to `address`. There must be four
  fields and the first must be `1`.
- `protocol` must be one of the protocols the host offered, compared as a string.
- `network` must be the one of the host's platform: `unix` on Unix hosts, `npipe` on Windows hosts. A host never
  accepts the other one.
- The plugin has 10 seconds from the start of the process to print the line (the host's default; the host may
  configure another value). If the plugin exits first, the host reports its exit code and the last 20 lines of
  stderr (at most 4096 bytes); if the line is wrong, the host reports a bad handshake line and kills the plugin.

What the host verifies in `address`:

- Unix: the path is absolute and, after cleaning, its directory is exactly the socket directory the host created. It
  must exist at that moment and be a socket (checked without following symbolic links). So the socket has to be
  bound before the line is printed.
- Windows: the name matches `^\\.\pipe\[A-Za-z0-9_-][A-Za-z0-9._-]*$`: a pipe of the local machine whose name has no
  path separator, so a line can never point the host at a remote pipe or a file.

After the check the host connects with gRPC over that address, without TLS, with a limit of 16 MB (16 777 216
bytes) on every message in both directions, and waits until the connection is ready.

## stdout and stderr after the handshake

The line is the only protocol use of stdout. Anything the plugin writes to stdout after it, and everything it writes
to stderr, goes to the writers the host configured; the host discards them when it configured none. The host reads
both continuously, so a plugin never blocks on a full pipe. Use stderr for logs. Never write secrets or values from
the configuration there: configuration errors name a path and never a value.

## Services

All services are on the one connection. `Plugin` and `grpc.health.v1.Health` are served by every plugin; `Input`,
`Processor` and `Output` only by a plugin that offers that kind in its `plugin.json`.

| Service | RPC | Deadline | Notes |
|---|---|---|---|
| `Plugin` | `Info` | 5 s | Identity and manifest. |
| `Plugin` | `Shutdown` | carries its own `deadline` | See [Shutdown](#shutdown). |
| `Input` | `Configure` | 10 s | Per instance. |
| `Input` | `Subscribe` (server stream) | none | Events until the stream is cancelled. |
| `Input` | `Probe` | 30 s | Every point once. |
| `Input` | `Browse` | none fixed | Optional: `UNIMPLEMENTED` when the plugin does not browse. |
| `Processor` | `Configure` | 10 s | Per instance. |
| `Processor` | `Process` (bidirectional stream) | none | See below. |
| `Output` | `Configure` | 10 s | Per instance. |
| `Output` | `Send` | 30 s | One batch, one outcome. A `Send` past the deadline counts as `RETRY_AFTER`. |

The deadlines are enforced by the host as gRPC deadlines; the plugin must honour them (the Go SDK passes them to the
author's code through its `context`).

Every call of `Input`, `Processor` and `Output` names the `instance_id` it is about (`Process` does so in its first
message, the `ProcessStart`). The host configures one instance per section of its configuration and then addresses it
by that identifier. Instances of one plugin are independent.

### Configure

`ConfigureRequest.config` is the JSON of the instance's section with secrets already resolved:

- Input: `id`, `connection` (free object), `devices`, each with `device` and `points` (`signal`, `address`, `read`,
  `type`, `byte_order`, `unit`, `priority`). `read` is `"subscribe"`, `"event"`, `{"poll": "<duration>"}` or
  `{"subscribe": {...}}`.
- Processor: `id`, `observe` (device keys), `devices` (derived devices and their signals) and `config` (free object).
- Output: `id` and `config` (free object).

A non-empty `ConfigureResponse.errors` means the configuration is refused; each entry is a `path` such as
`devices[0].points[1].address` and a `message`, and never contains a value, because values can be secrets. A refusal
is an answer, not an RPC error: an RPC error from `Configure` is a failure of the plugin. Go plugins decode the
section with `sdk.DecodeInput`, `sdk.DecodeProcessor` and `sdk.DecodeOutput`, which ignore the keys they do not know
so that a newer agent can add some; do likewise.

### Input events

`Subscribe` sends `InputEvent`s of three kinds: `sample`, `answered(device)` (the device answered, also when it had no
new samples) and `health(connected, read_error)`, where `read_error` is empty or one of `timeout`, `connection`,
`exception`, `decode`. A plugin never sends births or deaths: the agent derives them. A sample carries the device and
signal keys, a UTC timestamp, exactly one value (`double`, `int`, `bool` or `string`; NaN and infinities are refused),
a quality (`GOOD`, `UNCERTAIN`, `BAD`) and a priority (`CRITICAL`, `BULK`). The `UNSPECIFIED` enum values are never
valid on the wire.

### Processor stream

The first message the agent sends on `Process` is a `ProcessStart` naming the instance; every later message is a
`record` (a copy of a record of an observed device: a sample or a device event). The plugin answers with `Sample`s of
its declared derived devices and signals only; the agent refuses and counts anything else. A message that carries
something else is a protocol violation: the Go SDK ends the stream with `INVALID_ARGUMENT` when the first message is
not a `start`, or a later one is not a `record` (or is an invalid record).

### Output outcomes

`Send` answers exactly one of: `ACCEPTED`, `DUPLICATE` (stored earlier; the agent advances), `RETRY_AFTER` (with
`retry_after`; the agent backs off), `SPLIT` (batch too large; the agent splits it in two), `REJECT_INVALID` (with
`reason`; the agent drops the batch and records a loss) and `SUSPEND_AUTH` (credentials refused; the agent suspends
sending and keeps buffering). `OUTCOME_UNSPECIFIED` is never valid. The message identifier of a batch (`message_id`, a
UUIDv7) is the key of idempotency: sending the same identifier twice must give `ACCEPTED` or `DUPLICATE` both times. A
plugin output never buffers; the agent does.

### One stream per instance

An input instance has at most one `Subscribe` stream, and a processor instance at most one `Process` stream. While one
is open, a second one for the same instance is refused with the gRPC status `ALREADY_EXISTS`. A plugin that wants
parallel streams needs separate instances. The conformance check `input.stream` verifies the rule for inputs and
that a new stream is served once the first was cancelled, which proves that the first one ended; the `Process` rule
is written in `processor.proto` and is not probed by the suite.

### Behaviour of the Go SDK

Not part of the wire contract, but what a host can observe from a Go plugin:

- A call that needs a configured instance gets `FAILED_PRECONDITION` when the instance was never configured or its
  last `Configure` was refused; an empty `instance_id` gets `INVALID_ARGUMENT`.
- A new `Configure` of an instance cancels its running stream first; a refused `Configure` leaves the instance
  unconfigured.
- A panic in the author's code fails that one call with `INTERNAL`, writes one line to stderr (the type of the panic,
  never request data) and the plugin keeps serving.
- Messages are limited to 16 MB in both directions.

## Info and the manifest

`Info` (deadline 5 s) answers `name`, `version`, `protocol_versions` and `manifest`:

- `name`, `version` and `protocol_versions` equal those of `plugin.json`.
- `manifest` is the **canonical form** of the content of `plugin.json`. The host compares canonical forms, not bytes:
  it canonicalises both `plugin.json` and `Info.manifest` and requires them to be equal. The protocol does not
  duplicate the schemas.

The conformance check `info` makes these comparisons and also requires that the negotiated protocol is among
`protocol_versions`.

### Canonical JSON

`manifest.Canonical` (and so the host) defines the canonical form of a JSON document as:

- Exactly one JSON value, valid UTF-8, nesting at most 512 levels. Invalid UTF-8, a `\uD800`-`\uDFFF` escape that is not
  a high surrogate followed by a low one, an empty document, data after the value and an object that repeats a key at
  the same level are all errors.
- Object keys sorted at every depth by the byte order of their UTF-8 encoding. This is not RFC 8785 (which sorts by
  UTF-16 code units) and does not normalise numbers.
- No whitespace outside strings.
- Numbers exactly as written (`1.0` stays `1.0`, `1e2` stays `1e2`).
- Strings in UTF-8 with only the escapes JSON requires: `\"`, `\\`, `\b`, `\f`, `\n`, `\r`, `\t` and `\u00xx` (lower-case
  hexadecimal) for the other characters below U+0020. Every other character, `/` and U+007F included, is written
  as itself, and an escape such as `é` in the input is decoded and written as the character.

### `--manifest`

By convention the Go SDK builds the plugin's `plugin.json` from the code: a binary started with `--manifest` prints the
canonical manifest followed by one LF and exits 0, without a cookie and without a handshake. The trailing LF is a
command-line convention: `Info.manifest` carries the canonical bytes without it, which is why hosts and the
conformance suite compare canonical forms. A plugin in another language may offer the same flag; the protocol does
not require it, and the suite does not call it.

## plugin.json

A plugin ships `plugin.json` next to its binary. It may be formatted any way the author likes (the host canonicalises
it) but must be accepted by the check of package `manifest`. The example below shows the shape of every kind at once;
a real plugin lists only the kinds it offers, with real schemas:

```json
{
  "name": "line-state",
  "version": "0.1.0",
  "protocol_versions": ["v1"],
  "kinds": {
    "input": {
      "connection_schema": {},
      "address_schema": {},
      "read_modes": ["poll"],
      "types": ["int64"],
      "browse": false
    },
    "processor": {"config_schema": {}},
    "output": {"config_schema": {}}
  }
}
```

- `name` matches `^[a-z0-9][a-z0-9._-]{0,63}$`. `version` is a non-empty string. `protocol_versions` lists at least one
  version; today the only one accepted is `v1`.
- `kinds` offers at least one of `input`, `processor`, `output`. A plugin serves exactly the services of the kinds it
  offers.
- `input` needs `connection_schema` (the schema of the free `connection` object), `address_schema` (the schema of a
  point's `address` string), `read_modes` (at least one of `poll`, `subscribe`, `event`), `types` (at least one of
  `bool`, `int16`, `uint16`, `int32`, `uint32`, `int64`, `uint64`, `float32`, `float64`, `string`) and `browse`
  (`true` only for a plugin that serves `Browse`).
- `processor` and `output` need `config_schema` (the schema of the free `config` object).
- Every schema is a JSON Schema document of draft 2020-12, as an object or a boolean. A `$schema` keyword, when
  present, must name that draft. The schema cannot reference anything outside itself: a `$ref` to a file or URL fails.
- The file must be canonicalisable (see above: a duplicate key is an error). A key that differs from a field name only
  by case (`Name` for `name`) is refused. Keys this version does not know are tolerated.

The package also validates a section against the manifest offline, without starting the plugin
(`Manifest.ValidateInput`, `ValidateProcessor`, `ValidateOutput`); its messages name a path and never a value.

## conformance.json

The conformance suite reads `conformance.json` from the plugin directory. It is a JSON object with the keys of the kinds
the plugin offers, and no others:

```json
{
  "input":     {"valid": [<section>], "invalid": [<section>]},
  "processor": {"valid": [<section>], "invalid": [<section>]},
  "output":    {"valid": [<section>], "invalid": [<section>]}
}
```

- Each `<section>` is a JSON object, the configuration section of an instance exactly as `Configure` receives it
  (see [Configure](#configure)).
- Every kind that `plugin.json` offers needs at least one valid and one invalid section. A fixture for a kind the
  plugin does not offer fails the suite, and so does an unknown key.
- Valid sections must be accepted by `Configure` with no configuration errors. Invalid sections must be refused with
  configuration errors in the response; an RPC error does not count as a refusal.
- The suite uses the first valid section to run the kind: the input stream and probe, the processor signals, the
  output sends.
- A valid processor section may carry an optional member `records`: the records the suite sends to `Process`, in
  order. The suite removes it before `Configure`, since the agent never sends it. Without it the suite sends synthetic
  records, and a processor that emits nothing for them fails `processor.signals` with a message asking the author to
  supply `records`. The shape of the records is in the documentation of package `conformance` (`go doc
  ./conformance`).

The examples in [`examples/`](../examples/README.md) have a `conformance.json` each.

## The plugin binary

The binary lives in the same directory as `plugin.json`, named after the manifest's `name`: `<name>`, and `<name>.exe`
on Windows. The agent host and `laraedge-conformance` both use this rule to find it.

## Shutdown

`Shutdown` carries a `deadline` (a `google.protobuf.Duration`): how long the plugin has to stop before the host kills
it. The Go SDK uses 5 seconds when the request carries none.

The plugin answers (the Go SDK answers at once with an empty response), then stops: it reports `NOT_SERVING`,
cancels its streams, stops every input instance, stops the gRPC server gracefully (forcefully once the deadline has
passed), closes its listener (which removes the Unix socket) and exits with code **0**. A handler that ignores its
context cannot hold the process past the deadline. The call is idempotent: only the first one starts the sequence.

Because the plugin may exit before its answer is flushed, **the host treats `UNAVAILABLE` and EOF as the answer to
`Shutdown`**, and success is the exit with code 0 within the deadline. The conformance check `shutdown` checks a health
answer first, calls `Shutdown` with a 5 s deadline and requires exit code 0 within that deadline plus one second.

## Health

Every plugin serves the standard `grpc.health.v1.Health` service on the same connection and answers `Check` with the
empty service name: `SERVING` while it runs, `NOT_SERVING` once `Shutdown` has begun. Hosts poll `Check`, and the
conformance suite uses `Check` only; the `Watch` RPC of the standard service is not part of this contract.

## Process tree

The host ties the plugin to its own life. What each platform guarantees (see `handshake/plugin.go` and the
platform files):

| Platform | Mechanism | Plugin's descendants |
|---|---|---|
| Linux | The plugin leads its own process group and gets `SIGKILL` when the host dies (parent-death signal). | `Kill` and the plugin's own exit end the whole group while the host lives. When the host dies suddenly only the plugin is killed: the processes it started survive, and the agent's systemd unit (`KillMode=control-group`) is the backstop. A descendant that leaves the group (`setsid`, `setpgid`) is not reached by the group kill. |
| Windows | The plugin runs in a job object that kills on close. | When the host dies or the plugin has been reaped the whole job is killed. The plugin joins the job right after it starts: a child started before that moment stays outside it. |
| Other Unix | The plugin leads its own process group. | No parent-death guarantee: a plugin can outlive a host that dies. Development only. |

For an author this means: do not daemonize or detach, keep child processes inside the plugin's process group, and
expect that a plugin whose host disappears is ended by the operating system on Linux and Windows.

## The conformance suite

`laraedge-conformance <plugin dir>` (package `conformance`) runs the host side of everything above against your
binary, as the agent would. It checks, in order: `manifest`, `fixtures`, `no-cookie`, `handshake`, `permissions`, `info`,
`health`, the checks of each offered kind (`input.configure`, `input.stream`, `input.probe`; `processor.configure`,
`processor.signals`; `output.configure`, `output.send`, `output.duplicate`), `shutdown` and `restart` (a `kill -9` and a
new launch with the same socket directory must perform a new handshake on a new address). The exit code is 0 when every
check passed, 1 when one failed and 2 on a usage error. A check that cannot run because one it builds on failed is
reported as failed with a detail that starts with `not run:`; the suite never passes a check it did not perform.
