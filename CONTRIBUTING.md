# Contributing

Thank you for helping with the laraedge SDK.

## Contributor License Agreement

Every contributor must sign the CLA before a pull request can be merged. You do not need to do anything in advance:
when you open a pull request, the CLA workflow comments on it with the instructions, and the pull request stays blocked
until the agreement is signed. The workflow is shared by all `getlaraplate` repositories, so you sign once.

## Workflow

- Fork the repository and branch from `develop`. Changes reach `master` through a pull request.
- Keep a pull request to one subject, with tests.
- Commit messages follow the conventional style of the repository history (`feat(handshake): ...`, `fix(sdk): ...`,
  `docs: ...`).

## Go conventions

- Go 1.27, as in `go.mod`. No CGO.
- Standard library first. A new dependency needs a permissive licence (Apache-2.0, BSD-2-Clause, BSD-3-Clause, MIT or
  ISC) and a reason in the pull request. The `dependency licences` CI job enforces the licence list.
- The module must never import `github.com/getlaraplate/laraedge` (the AGPL agent), and no AGPL code may be copied in.
- Exported identifiers carry a doc comment. Errors are wrapped with context; configuration errors never contain a
  value, because values can be secrets.
- Tests are table-driven, use fakes rather than mocks, and never synchronise with sleeps.
- Generated code (`pluginv1/`) is committed and never edited by hand. It comes from the `.proto` files in `proto/`:
  change those, then regenerate with `scripts/generate.sh`, which needs `protoc` 36.1 on `PATH` and fails on any other
  version. The `generated code` CI job regenerates it with the pinned release and fails when the result differs from
  what is committed. Field numbers are stable: never reuse or renumber one. `pluginv1/testdata/descriptors.golden`
  pins the number, name, type and oneof of every field, the enum values and the methods, so a renumbering fails the
  tests even when it is regenerated; after a compatible addition rewrite it with
  `go test ./pluginv1 -run TestDescriptorsMatchTheGolden -update-descriptors` and commit it with the generated code.
- A change to the protocol, the handshake or the formats of `plugin.json` and `conformance.json` updates
  [docs/handshake.md](docs/handshake.md) in the same pull request.

## Checks to run before pushing

```bash
gofmt -l .                # must print nothing
go vet ./...
GOOS=windows go vet ./...
go test -race ./...
GOARCH=386 go test ./...
```

CI runs vet and `go test -race ./...` on Linux amd64 and arm64 and on Windows amd64, the conformance suite on the
three example plugins on the same three platforms, a 32-bit (386) test run and an armv7 cross build on Linux,
`govulncheck`, the dependency licence check and the generated code check.

## Security

Do not report vulnerabilities in public issues. See [SECURITY.md](SECURITY.md).
