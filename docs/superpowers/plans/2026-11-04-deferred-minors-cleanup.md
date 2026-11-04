---
status: open
created_on: 2026-11-04
---
# SDK deferred minors cleanup. Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax. Tick each step when it is done, in the same work block.

**Status:** Open.

**Goal:** resolve the minor review findings that plan `2026-10-23-sdk.md` deferred and that are still open, fix the
two that matter to every plugin (the Windows job assignment race and the emit that can report success for a dropped
item), and release them as `v0.1.2`. Each finding ends fixed, or decided with its reason written where a reader finds
it.

**Architecture:** no protocol change: `pluginv1` and its golden descriptor dump stay as they are, so `v0.1.2` is a
drop-in for plugins and for the agent. Changes are local to `handshake`, `sdk`, `manifest`, `conformance`, the
examples and the docs.

**Tech Stack:** Go 1.27, the dependencies of `go.mod` (no new one).

**Spec:** stack root `docs/superpowers/specs/2026-10-22-laraedge-plugins-design.md` and `docs/handshake.md` of this
repository (the protocol contract). The inventory below was verified against the code on 2026-11-04 (`master` at
`d492ec0`, tag `v0.1.1`); an item marked "verify" was matched by area only, and its task starts by confirming it.

## Global Constraints

- No protocol change: `proto/` and `pluginv1/` are untouched, and the golden descriptor test passes unchanged.
- No new dependency.
- The public Go API (`sdk`, `handshake`, `manifest`, `conformance`) keeps every exported identifier and signature;
  additions are allowed only where an item needs one.
- `conformance` may become stricter only where `docs/handshake.md` already states the rule; each stricter check is
  named in the delivery status and the release notes.
- CI runs on Linux and Windows (`.github/workflows/ci.yml`); a Windows-only change is proven by the Windows job.
- Tests wait on conditions, never on fixed sleeps or `runtime.Gosched` spins, in tests this plan writes or touches.
- Code, comments and docs in English; sibling style; commits without `Co-Authored-By` trailers.

## Review Focus

1. A plugin child that spawns a grandchild at once on Windows: the grandchild is in the job before it can run, so
   killing the job kills it. Test in Task 1 (Windows job).
2. `emit` after the stream's `Send` failed: it returns an error every time, never `nil` for an item that was not sent.
   Test in Task 2.
3. Manifest problems on keys containing `.` or `[`: the path in the message identifies the key unambiguously, and
   `devices[2]` sorts before `devices[10]`. Test in Task 3.
4. A conformance run against a plugin that answers `RETRY_AFTER` without `retry_after`: it fails with a message naming
   the rule of `docs/handshake.md`. Test in Task 4.
5. The examples under concurrent cancellation: `simulated-counter` and `jsonl` tests pass with `-race -count=20`.
   Checked in Task 5.

## Inventory

| # | Plan task | Finding | Evidence | Task |
|---|---|---|---|---|
| S1 | T4 | Windows: a grandchild may escape the job before the child is assigned to it | `handshake/proc_windows.go:21-47` (job created, process started, then assigned; no `CREATE_SUSPENDED`) | 1 |
| S2 | T4 | ownership refusal untested (needs root); output-join test weak; possible flake on the `child=` prefix; drain test costs about 2 s | verify, `handshake/*_test.go` | 1 |
| S3 | T5 | `emit` racing a failed `stream.Send` can return `nil` for a dropped item | `sdk/sdk.go:301-335` | 2 |
| S4 | T5 | inbound enums narrowed without a range check; `Send`/`Probe`/`Browse` not serialised against a re-`Configure` (document); no deterministic back-pressure test | verify, `sdk/` | 2 |
| S5 | T3 | `joinKey` ambiguous for keys containing `.` or `[`; problems sorted lexicographically | `manifest/manifest.go:407-409`, `sortProblems` | 3 |
| S6 | T3 | `keywordMessage` handles `dependentRequired` but not `dependentSchemas` | `manifest/manifest.go:432` | 3 |
| S7 | T3 | test gaps: max depth, local and http `$ref`, escaped surrogate pair, subscribe-only kind refusing poll, a misnamed test, key leakage; a stale comment; an overlong doc line | verify, `manifest/*_test.go` | 3 |
| S8 | T7 | conformance accepts `RETRY_AFTER` without `retry_after` and `REJECT_INVALID` without `reason`, which `docs/handshake.md:205-206` require | `conformance/checks.go` | 4 |
| S9 | T7 | `"records": null` in a fixture reports "list is empty" | `conformance/records.go:92` | 4 |
| S10 | T7 | the one-`Process`-stream rule (second stream refused `ALREADY_EXISTS`) is written but not probed | verify, `conformance/checks.go` | 4 |
| S11 | T6 | `waitForTickers` polls with `time.Sleep(1ms)` | `examples/simulated-counter/counter_test.go:320-332` | 5 |
| S12 | T6 | jsonl: a failed `Sync` after a good `Write` may duplicate a line on resend; no parent directory fsync; enum words depend on `iota` order; `Process` cancel return conventions differ; fixture carries a `driver` key outside S7; `answered` per point on poll | verify, `examples/jsonl/sink.go:155,180`, `examples/` | 5 |
| S13 | T2 | `chmod +x` after unzip in the protoc download path; one `ConfigureRequest`/`Response` shared by the kinds; `Health` shadows `grpc.health.v1.Health` in imports | verify, `scripts/`, `proto/` | 6 |
| S14 | T1, T8 | README "or the reverse" copy rule stricter than the spec; CONTRIBUTING names `generate.sh` before it exists in the reading order; `.gitattributes`/`.editorconfig` extra; long lines and "an error from Run ends the stream" wording in `docs/handshake.md` | verify, `README.md`, `CONTRIBUTING.md`, `docs/handshake.md` | 6 |

Already resolved (no task): licence check on Windows (`ci.yml:106-123`), README Server 2016 wording
(`README.md:153-156`), field numbers pinned (`e7dd94f`), `dependentRequired` messages, `buildManifest` protocol
versions (`sdk/sdk.go:149,207`), `acquire` after `Shutdown` and with a parent context that ended (`1742ce0`,
`12e0668`), key order documented as UTF-8 bytes (`docs/handshake.md:251-258`).

Outside this plan, by name:

- **`Release` of removed instances** (laraedge plan 3, Q7; ruling R18 of `2026-10-23-sdk.md`): a protocol addition,
  so a new RPC and a minor version; its own plan across `laraedge-sdk` and `laraedge`.
- **Bumping `laraedge` to `v0.1.2`**: a `go.mod` change in `laraedge`, done there after this release, on its own
  branch and pull request.

---

### Task 1: Windows job assignment and handshake test minors

**Files:**
- Modify: `handshake/proc_windows.go`
- Test: `handshake/proc_windows_test.go` (create if absent), the handshake tests S2 names

- [x] **Step 1: Write the failing test** (Windows build tag): a child that starts a grandchild as its first action
  (a test helper binary, the shape the existing process-tree tests use); after `Kill`, the grandchild is gone
  (Review Focus 1). On Linux the test file does not build.

- [x] **Step 2: Implement S1.** Start the child with `CREATE_SUSPENDED`, assign it to the job, then resume its main
  thread; on any failure between start and resume, terminate the child and return the error. Use `golang.org/x/sys/windows`
  (already a dependency).

- [x] **Step 3: S2.** Verify each sub-item; fix the output-join test so it fails when lines are lost (a deterministic
  slow writer), make the `child=` check a `Contains`, and shorten the drain test with a smaller bound if the bound is
  a parameter; the ownership refusal stays untested on CI (needs root): record that in the test file as a comment
  naming the manual check.

- [ ] **Step 4: Verify**: `go test ./handshake -race -count=1` on Linux; `GOOS=windows go vet ./handshake`; the
  Windows CI job green on the pull request. (Linux run and Windows vet and test compile done; the Windows CI job
  is pending on the pull request.)

- [x] **Step 5: Commit**

```bash
git add handshake
git commit -m "fix(handshake): assign the plugin to its job before it runs on Windows"
```

### Task 2: `emit` after a failed send, and SDK runtime minors

**Files:**
- Modify: `sdk/sdk.go`
- Test: `sdk/*_test.go`
- Docs: `docs/handshake.md` (S4 serialisation note)

- [x] **Step 1: Write the failing test** (S3, Review Focus 2): a fake stream whose `Send` fails on the second item;
  every `emit` call after the failure returns a non-nil error, under `-race -count=50`.

- [x] **Step 2: Implement S3.** Once a send error is recorded, `emit` returns it (or `errStreamEnded`) without handing
  the item to the pump; the window between the failure and the cancel is closed by checking the recorded error under
  the same synchronisation the pump uses.

- [x] **Step 3: S4.** Verify: an inbound enum value outside the known range is refused with `INVALID_ARGUMENT` (test);
  the concurrency of `Send`/`Probe`/`Browse` with a re-`Configure` is documented in `docs/handshake.md` as the
  author's responsibility (the SDK does not serialise them), with the reason; a deterministic back-pressure test
  (a slow consumer, a bounded number of in-flight items) if the pump has a bound to test, else record why not.

- [x] **Step 4: Verify**: `go test ./sdk -race -count=20`. Expected: PASS.

- [x] **Step 5: Commit**

```bash
git add sdk docs
git commit -m "fix(sdk): emit never reports success after the stream's send failed"
```

### Task 3: Manifest paths, ordering and test gaps

**Files:**
- Modify: `manifest/manifest.go`
- Test: `manifest/*_test.go`
- Docs: `docs/handshake.md` if it shows a problem path format

- [x] **Step 1: Write the failing tests** (Review Focus 3):
  - a key `a.b` and a key `a[0]` produce different, unambiguous paths (quote such keys, e.g. `config["a.b"]`, the
    form JSON-path readers expect; state the chosen form in the doc comment of the path function);
  - problems at `devices[2]` and `devices[10]` sort in that order;
  - S6: a `dependentSchemas` failure gets a readable message like `dependentRequired`.

- [x] **Step 2: Implement S5 and S6.**

- [x] **Step 3: S7.** Verify each gap; add the tests that are missing (max depth, local `$ref`, an `http` `$ref`
  refused, an escaped surrogate pair, a subscribe-only kind refusing `poll`, no value in any message); rename the
  misnamed test; fix the stale comment and the overlong line.

- [x] **Step 4: Verify**: `go test ./manifest -race -count=1`. Expected: PASS.

- [x] **Step 5: Commit**

```bash
git add manifest docs
git commit -m "fix(manifest): unambiguous problem paths, natural order, dependentSchemas"
```

### Task 4: Conformance rules from the handshake document

**Files:**
- Modify: `conformance/checks.go`, `conformance/records.go`
- Test: `conformance/*_test.go`, `conformance/testdata/broken/` (a behaviour per new refusal)
- Docs: `docs/handshake.md` (the conformance section lists the checks)

- [x] **Step 1: Write the failing tests**
  - S8 (Review Focus 4): the broken test plugin answers `RETRY_AFTER` without `retry_after`, then `REJECT_INVALID`
    without `reason`; conformance fails each with a message naming the rule.
  - S9: `"records": null` reports `records must be a list`.
  - S10 (if not probed): a second `Process` stream on the same instance must be refused `ALREADY_EXISTS`; the broken
    plugin that accepts it fails conformance.

- [x] **Step 2: Implement**, then `go test ./conformance -race -count=1`, and run the conformance command against the
  three examples (the command CI runs). Expected: PASS for the examples, the broken plugin refused for each new rule.

- [x] **Step 3: Commit**

```bash
git add conformance docs
git commit -m "feat(conformance): enforce retry_after, reason and one Process stream as documented"
```

### Task 5: Examples

**Files:**
- Modify: `examples/simulated-counter/counter_test.go`, `examples/jsonl/*.go`, `examples/line-state/*` only if an item
  names it
- Test: the examples' tests

- [x] **Step 1: S11.** `waitForTickers` waits on a signal from the example's clock (a channel the fake clock closes
  when a ticker is registered, or the SDK test clock's equivalent), not on a sleep loop.

- [x] **Step 2: S12.** Verify each sub-item, then:
  - a failed `Sync` after a good `Write`: answer `RETRY_AFTER` and truncate back to the last synced offset before
    the next write, so a resend cannot duplicate the line; test with a failing file;
  - fsync the parent directory after creating the file;
  - the jsonl enum words switch on the constants, not on `iota` order; test `bad` and `death`;
  - the other sub-items: fix or record the decision in the example's README.

- [x] **Step 3: Verify** (Review Focus 5): `go test ./examples/... -race -count=20`, and the conformance command on
  each example. Expected: PASS.

- [x] **Step 4: Commit**

```bash
git add examples
git commit -m "fix(examples): no sleep waits, jsonl cannot duplicate a line after a failed sync"
```

### Task 6: Repository and documentation minors

**Files:**
- Modify: `scripts/*`, `README.md`, `CONTRIBUTING.md`, `docs/handshake.md`, `.gitattributes`, `.editorconfig`

- [ ] **Step 1: Verify S13 and S14** item by item.

- [ ] **Step 2: Fix**: the protoc download sets the executable bit only on the binary it runs; the README copy rule
  matches the spec's wording; CONTRIBUTING introduces `generate.sh` where the reader needs it; drop `.gitattributes`
  or `.editorconfig` lines that do nothing for this repository (keep what CI or the generator relies on); wrap
  `docs/handshake.md` to its width and correct the "an error from Run ends the stream" sentence to what `sdk`
  does. The shared `ConfigureRequest` and the `Health` name are protocol shapes: no change (Global Constraints),
  record the reason in `docs/handshake.md`.

- [ ] **Step 3: Verify**: `go test ./... -count=1`; `scripts/generate.sh` (or the CI check that the generated code is
  current) reports no diff.

- [ ] **Step 4: Commit**

```bash
git add scripts README.md CONTRIBUTING.md docs .gitattributes .editorconfig
git commit -m "docs: README, CONTRIBUTING and handshake.md minors"
```

### Task 7: Verify, release and close

**Files:**
- Modify: this plan, `docs/superpowers/plans/INDEX.md`, the release notes (the repository's existing place for them:
  `README.md` changelog section or the GitHub release body; follow `v0.1.1`)

- [ ] **Step 1:** `go test ./... -race -count=1`, `go vet ./...`, `GOOS=windows go vet ./...`, the conformance command
  on the three examples. Expected: PASS, clean.

- [ ] **Step 2:** every inventory row S1-S14 gets its resolution: the commit, FIXED/OBSOLETE with evidence, or the
  decision and where it is written.

- [ ] **Step 3: Close the plan**: `status: closed`, Status "Closed (date)", boxes ticked, a delivery status with the
  divergences, the stricter conformance checks (S8, S10) named, the items outside this plan, and `**Documented in:**`;
  mark the INDEX row closed. Commit.

- [ ] **Step 4: Release.** After the user merges the pull request: tag `v0.1.2` on the merge commit with release notes
  listing S1, S3, the stricter conformance checks and the fixes, the way `v0.1.1` was tagged. The tag is pushed only
  with the user's go-ahead.

- [ ] **Step 5: Follow-up in `laraedge`**: a branch that bumps `github.com/getlaraplate/laraedge-sdk` to `v0.1.2`,
  runs the agent's suite and e2e, and opens a pull request.
