# laraedge-sdk

The Apache-2.0 SDK for writing plugins of the Laraplate edge agent (`laraedge`).

Vendor drivers (for example FANUC FOCAS, Siemens S7, EtherNet/IP), cross-device logic and destinations other than
Laraplate run as separate processes. A plugin imports only this module, so the AGPL agent never contains plugin code
and a plugin never contains agent code.

> This project is a work in progress and has no usable release yet. Do not use it in production.

## What it contains

| Package or directory | What it is |
|---|---|
| `proto/`, `pluginv1/` | The plugin protocol `laraplate.edge.plugin.v1` (gRPC) and its generated Go code, committed and regenerated with `scripts/generate.sh`. |
| `handshake/` | The plugin side (`Listen`) and the host side (`Launch`) of the process handshake over Unix sockets and Windows named pipes. |
| `manifest/` | The model of `plugin.json`, its canonical JSON form and the offline validation of a configuration section. |
| `sdk/` | The Go SDK: implement `Input`, `Processor` or `Output` and call `Serve`. |
| `conformance/`, `cmd/laraedge-conformance/` | The conformance suite and its command. |
| `examples/` | Three example plugins, one per kind. See [examples/README.md](examples/README.md). |
| `docs/handshake.md` | The handshake and the protocol for authors of plugins in other languages. |

The design lives in the Laraplate stack: `docs/superpowers/specs/2026-10-22-laraedge-plugins-design.md`. The plan this
repository was built from is in [docs/superpowers/plans/INDEX.md](docs/superpowers/plans/INDEX.md).

## Quick start

A plugin is a Go program in a module that requires `github.com/getlaraplate/laraedge-sdk`. This one is an Input that
reports the current Unix time as the signal `now` of a device `clock` (about 60 lines with the imports; the
logic is in `Configure`, `Probe` and `Run`):

```go
package main

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/getlaraplate/laraedge-sdk/manifest"
	"github.com/getlaraplate/laraedge-sdk/sdk"
)

// clock is an Input with one signal, clock.now, that reports the Unix time.
type clock struct{}

func (clock) Configure(_ context.Context, _ string, raw json.RawMessage) []sdk.ConfigError {
	if _, err := sdk.DecodeInput(raw); err != nil {
		var ce sdk.ConfigError
		if !errors.As(err, &ce) {
			ce = sdk.ConfigError{Message: "the section cannot be decoded"}
		}
		return []sdk.ConfigError{ce}
	}
	return nil
}

func reading() sdk.Sample {
	now := time.Now().UTC()
	return sdk.Sample{Device: "clock", Signal: "now", TS: now, Value: sdk.IntValue(now.Unix()), Quality: sdk.QualityGood, Priority: sdk.PriorityBulk}
}

func (clock) Probe(context.Context, string) ([]sdk.Sample, error) {
	return []sdk.Sample{reading()}, nil
}

func (clock) Stop(context.Context, string) error { return nil }

func (clock) Run(ctx context.Context, _ string, out sdk.InputEmitter) error {
	for {
		if err := out.Sample(reading()); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(time.Second):
		}
	}
}

func main() {
	sdk.ServeInput("clock", "0.1.0", clock{}, manifest.InputKind{
		ConnectionSchema: json.RawMessage(`{"type":"object"}`),
		AddressSchema:    json.RawMessage(`{"const":"unix-time"}`),
		ReadModes:        []string{"poll"},
		Types:            []string{"int64"},
	})
}
```

Build it, let it write its manifest and add the fixtures of the conformance suite. `--manifest` needs no agent: it
prints the canonical `plugin.json` and a newline.

```bash
go build -o clock .
./clock --manifest > plugin.json
```

`conformance.json` holds sections the plugin must accept (`valid`) and must refuse (`invalid`), for each kind the
manifest offers:

```json
{
  "input": {
    "valid": [
      {
        "id": "clock-1",
        "connection": {},
        "devices": [
          {"device": "clock", "points": [{"signal": "now", "address": "unix-time", "read": {"poll": "1s"}, "type": "int64"}]}
        ]
      }
    ],
    "invalid": [
      {"id": "clock-bad-devices", "connection": {}, "devices": "none"}
    ]
  }
}
```

Put the binary, named after the manifest, next to `plugin.json` and `conformance.json`, and run the suite from a
checkout of this repository:

```bash
go run ./cmd/laraedge-conformance /path/to/the/plugin/directory
```

It starts the plugin the way the agent does and prints one `PASS` or `FAIL` line per check (handshake, socket
permissions, `Info` against `plugin.json`, health, `Configure` with your fixtures, a stream, `Probe`, `Shutdown`, a
restart after a kill), then a summary; the exit code is 0 only when every check passed.

To write a plugin in another language, read [docs/handshake.md](docs/handshake.md): the environment, the handshake
line, the sockets and pipes, the services with their deadlines, the shutdown and the formats of `plugin.json` and
`conformance.json`. The `.proto` files are in `proto/laraplate/edge/plugin/v1/`.

## Licence boundary

| Repository | Licence | Relation |
|---|---|---|
| `getlaraplate/laraedge-sdk` (this one) | Apache-2.0 | The only module a plugin imports. |
| `getlaraplate/laraedge` (the agent) | AGPL-3.0 | Imports this module for the host side of the protocol. |

- The dependency is one way: the agent imports the SDK, the SDK never imports the agent.
- A plugin that depends only on this module may be open source or proprietary, at the author's choice.
- Do not copy code from the AGPL agent into this repository.
- Plugins in other languages need only the `.proto` files and the handshake specification, with the official gRPC
  libraries.

## Platforms

Targeted: Linux amd64 and arm64, and Windows amd64 on Windows Server 2016 and later (named pipes); Linux armv7 builds.
No CGO.

Run in CI: Linux amd64 and arm64 and Windows amd64 (the `windows-latest` runner, not Server 2016 itself), each with
vet, `go test -race ./...` and the conformance suite on the examples; Linux armv7 is only cross-built, and a 32-bit
(386) test run on Linux stands in for it.

## Development

```bash
gofmt -l .
go vet ./...
GOOS=windows go vet ./...
go test -race ./...
GOARCH=386 go test ./...
```

See [CONTRIBUTING.md](CONTRIBUTING.md). Report vulnerabilities as described in [SECURITY.md](SECURITY.md).

## Licence

Apache License 2.0, see [LICENSE](LICENSE).
