# laraedge-sdk

The Apache-2.0 SDK for writing plugins of the Laraplate edge agent (`laraedge`).

Vendor drivers (for example FANUC FOCAS, Siemens S7, EtherNet/IP), cross-device logic and destinations other than
Laraplate run as separate processes. A plugin imports only this module, so the AGPL agent never contains plugin code
and a plugin never contains agent code.

> This project is a work in progress and has no usable release yet. Do not use it in production.

## What it will contain

The plugin protocol `laraplate.edge.plugin.v1` (gRPC, with the generated Go code committed), the plugin and host sides of
the handshake over Unix sockets and Windows named pipes, the plugin manifest, a Go SDK for the Input, Processor and
Output plugin kinds, a conformance suite and three example plugins. The design lives in the Laraplate stack:
`docs/superpowers/specs/2026-10-22-laraedge-plugins-design.md`. The plan being built is
[docs/superpowers/plans/INDEX.md](docs/superpowers/plans/INDEX.md).

## Licence boundary

| Repository | Licence | Relation |
|---|---|---|
| `getlaraplate/laraedge-sdk` (this one) | Apache-2.0 | The only module a plugin imports. |
| `getlaraplate/laraedge` (the agent) | AGPL-3.0 | Imports this module for the host side of the protocol. |

- The dependency is one way: the agent imports the SDK, the SDK never imports the agent.
- A plugin that depends only on this module may be open source or proprietary, at the author's choice.
- Do not copy code from the AGPL agent into this repository, or the reverse.
- Plugins in other languages need only the `.proto` files and the handshake specification, with the official gRPC
  libraries.

## Platforms

Tested on Linux amd64 and arm64 and on Windows amd64 (Windows Server 2016 and later, through named pipes); builds for
Linux armv7. No CGO.

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
