# Plans

Plans of `laraedge-sdk`. The design lives at the stack root:
`docs/superpowers/specs/2026-10-22-laraedge-plugins-design.md`.

- `2026-10-23-sdk.md` (**closed**, 2026-10-23): the plugin protocol `laraplate.edge.plugin.v1`, both sides of the handshake (Unix sockets, Windows named pipes), the manifest, the Go SDK for Input, Processor and Output, the conformance suite and three example plugins. Plan 1 of the design; the agent's host plans follow in `laraedge`.
- `2026-11-04-deferred-minors-cleanup.md` (**open**): the minor findings `2026-10-23-sdk.md` deferred and still open: the Windows job assignment race, `emit` after a failed send, manifest problem paths and order, conformance rules from `docs/handshake.md`, examples and documentation minors; released as `v0.1.2`.
