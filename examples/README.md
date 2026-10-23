# Example plugins

Three small plugins, one per kind, that show how to write a plugin with the SDK. They are examples,
not production plugins: they exist to be read, to be copied from, and to be run as real binaries by
the conformance suite and by the agent's end-to-end tests.

Each directory is a `package main` with the same layout: `main.go` calls `sdk.Serve`, the plugin
logic is in the other `.go` file (so tests can drive it in process), `plugin.json` is the manifest,
`conformance.json` holds the fixtures of the conformance suite (valid and invalid sections of the
kind the plugin offers) and the tests sit next to the code.

Build one with `go build` from its directory, for example:

```sh
go build -o simulated-counter ./examples/simulated-counter
```

`plugin.json` is the output of the binary's `--manifest` flag, committed. It needs no handshake and
prints the canonical manifest and a newline. After a change to a manifest, regenerate the file; a
test fails while the committed file and the binary disagree.

```sh
./simulated-counter --manifest > examples/simulated-counter/plugin.json
```

A plugin started by hand, without the agent, refuses to serve and exits with an explanation.

To run the conformance suite on an example, put the binary, named after the manifest, next to
`plugin.json` and `conformance.json` in one directory and pass it to `laraedge-conformance`, for
example in a temporary directory:

```sh
dir="$(mktemp -d)/line-state"
mkdir "$dir"
cp examples/line-state/plugin.json examples/line-state/conformance.json "$dir"
go build -o "$dir/line-state" ./examples/line-state
go run ./cmd/laraedge-conformance "$dir"
```

A valid processor fixture may carry a `records` list: the records the suite sends to `Process`
instead of its synthetic ones (see the package documentation of `conformance` for their shape). The
suite removes it before `Configure`.

## simulated-counter (Input)

A simulated machine whose points are counters, so it runs anywhere. The address of a point is
`counter:<n>`: every poll adds `n` to the counter, which is reported as `int32` (wrapping like a
32 bit register) or `int64`. Points are read by polling only, at the interval of each point. It
implements `Browse` (one node per configured point), `Probe` (every point once, as its first poll
would, consuming nothing) and `Run` (a sample and then `answered` for the device on each poll). The
clock of the poll loops is injected, which is how the tests drive time without sleeping.

## line-state (Processor)

Derives the state of a production line from its machines: it observes the integer `state` signal of
each observed device and emits the `state` of the derived device whenever the maximum changes, with
the timestamp of the sample that changed it. It ignores samples whose value is not an integer,
samples of bad quality, other signals, devices that are not observed and device events. It refuses a
section without `observe`, one that does not declare exactly one derived device with a `state`
signal, and any key in `config`. Its valid fixture carries `records`: births and `state` samples of
the observed presses, which the conformance suite sends to `Process`.

## jsonl (Output)

Appends each batch to a file as one JSON object per line (`message_id`, `source_seq`, `records`,
`losses`) and fsyncs before answering `ACCEPTED`. The configuration is `{"path": "<file>"}`. A
`message_id` already in the file, also from before a restart, answers `DUPLICATE`; a batch without
an id or with a record without a device answers `REJECT_INVALID`; a write that fails answers
`RETRY_AFTER`; and while a file named `<path>.suspend` exists every send answers `SUSPEND_AUTH`,
which is a switch for trying the behaviour of the agent without a real destination. The path of
`conformance.json` is relative, so a run that sends batches writes it in the working directory of the
plugin.
