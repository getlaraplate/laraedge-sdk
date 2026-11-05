---
status: closed
created_on: 2026-11-04
---
# SDK deferred minors cleanup. Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax. Tick each step when it is done, in the same work block.

**Status:** Closed (2026-11-04). Built and reviewed on branch `chore/deferred-minors-cleanup` (pull request
getlaraplate/laraedge-sdk#3); the `v0.1.2` tag and the `laraedge` bump wait for the user's merge and go-ahead. See
"Delivery status" below.

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
4. A conformance run against a plugin that answers `REJECT_INVALID` without `reason`: it fails with a message naming
   the rule of `docs/handshake.md`. Test in Task 4. (Written first for `RETRY_AFTER` without `retry_after` as well;
   that half was dropped by a ruling of the final review, a recorded divergence: a bare `RETRY_AFTER` passes
   `output.send`, see "Delivery status".)
5. The examples under concurrent cancellation: `simulated-counter` and `jsonl` tests pass with `-race -count=20`.
   Checked in Task 5.

## Inventory

| # | Plan task | Finding | Evidence | Task | Resolution |
|---|---|---|---|---|---|
| S1 | T4 | Windows: a grandchild may escape the job before the child is assigned to it | `handshake/proc_windows.go:21-47` (job created, process started, then assigned; no `CREATE_SUSPENDED`) | 1 | FIXED `8d12417`: `CREATE_SUSPENDED`, assigned to the job, then resumed; Windows CI green on PR #3 at `8d12417` and `66892bf`. The window left (host dies between start and assignment: plugin suspended outside the job, runs no code) is named in the `processTree` doc, `528bac1` |
| S2 | T4 | ownership refusal untested (needs root); output-join test weak; possible flake on the `child=` prefix; drain test costs about 2 s | verify, `handshake/*_test.go` | 1 | FIXED `8d12417`: ownership refusal now tested without root (`/` as socket directory, `listen_unix_test.go`); output-join test with a gated writer; `child=` parsed with `strings.Cut`. Drain test kept at about 2 s: `outputDrainTimeout` is a constant, reason in a comment. Comments on the drain bound and the probabilistic Wait guard, `528bac1` |
| S3 | T5 | `emit` racing a failed `stream.Send` can return `nil` for a dropped item | `sdk/sdk.go:301-335` | 2 | FIXED `3ca7c81`: the pump stops receiving once a send failed; `TestEmitNeverSucceedsAfterTheSendFailed` (a regression guard whose RED is probabilistic, said in `c9a462c`) |
| S4 | T5 | inbound enums narrowed without a range check; `Send`/`Probe`/`Browse` not serialised against a re-`Configure` (document); no deterministic back-pressure test | verify, `sdk/` | 2 | Enum range: already refused, tests that the refusal names the field, `3ca7c81` and `c9a462c`. Serialisation against re-`Configure`: decided, the author's responsibility, written in `docs/handshake.md` (instances). Back-pressure: `TestPumpHoldsBackAnAuthorThatOutpacesSend`, `3ca7c81` (parked as scheduling-dependent, see Delivery status) |
| S5 | T3 | `joinKey` ambiguous for keys containing `.` or `[`; problems sorted lexicographically | `manifest/manifest.go:407-409`, `sortProblems` | 3 | FIXED `5909f87`, `d6ddad1`: keys that are empty or contain `.`, `[`, `]`, `"`, whitespace or a control character are quoted `config["a.b"]`; indexes sort in natural order; form in the `joinKey` doc and `docs/handshake.md` |
| S6 | T3 | `keywordMessage` handles `dependentRequired` but not `dependentSchemas` | `manifest/manifest.go:432` | 3 | OBSOLETE: jsonschema v6.0.3 has no `dependentSchemas` error kind and reports the failing leaf; the case added in `5909f87` was unreachable and removed in `f523beb`; `TestDependentSchemasHasAReadableMessage` pins the generic message "value does not satisfy the schema" |
| S7 | T3 | test gaps: max depth, local and http `$ref`, escaped surrogate pair, subscribe-only kind refusing poll, a misnamed test, key leakage; a stale comment; an overlong doc line | verify, `manifest/*_test.go` | 3 | FIXED `5909f87`: the missing tests, the misnamed test, the stale comment, the overlong line. The 512/513 depth boundary test, `f523beb`, found an off-by-one: 513 levels were accepted, now refused as documented |
| S8 | T7 | conformance accepts `RETRY_AFTER` without `retry_after` and `REJECT_INVALID` without `reason`, which `docs/handshake.md:205-206` require (the plan read the `retry_after` parenthetical as a requirement; a ruling read it as optional, see Resolution) | `conformance/checks.go` | 4 | PARTIAL by ruling: `REJECT_INVALID` without `reason` fails `output.send`, `ef8153b`. `RETRY_AFTER` without `retry_after` is not enforced (dropped in `ee3997b`): the SDK documents a zero delay as "the agent uses its own backoff", the agent tolerates the absence, and `v0.1.2` is promised as a drop-in; `docs/handshake.md` now says "optionally with `retry_after`". `BROKEN_RETRY_AFTER_BARE` pins that `output.send` passes |
| S9 | T7 | `"records": null` in a fixture reports "list is empty" | `conformance/records.go:92` | 4 | FIXED `ef8153b`: `"records": null` reports `records must be a list` |
| S10 | T7 | the one-`Process`-stream rule (second stream refused `ALREADY_EXISTS`) is written but not probed | verify, `conformance/checks.go` | 4 | FIXED `ef8153b`, `163489f`, `ee3997b`, `2844c2b`, `41597aa`: `processor.signals` probes a second stream while the first is open (refused `ALREADY_EXISTS` or fails); not probed only when the processor ended its stream on its own first; a processor that ends its running stream to serve the newest one fails |
| S11 | T6 | `waitForTickers` polls with `time.Sleep(1ms)` | `examples/simulated-counter/counter_test.go:320-332` | 5 | FIXED `a3138a3`: `waitForTickers` waits on a channel the fake clock closes |
| S12 | T6 | jsonl: a failed `Sync` after a good `Write` may duplicate a line on resend; no parent directory fsync; enum words depend on `iota` order; `Process` cancel return conventions differ; fixture carries a `driver` key outside S7; `answered` per point on poll | verify, `examples/jsonl/sink.go:155,180`, `examples/` | 5 | FIXED `a3138a3`, `66892bf`, `84c5864`, `f798d62`: truncate back after a failed `Write` or `Sync`, parent directory fsync, enum words switch on constants, per-path file state, replacement and rotation noticed. Cancel return conventions, the `driver` key and `answered` per point: decided, written in `examples/README.md` |
| S13 | T2 | `chmod +x` after unzip in the protoc download path; one `ConfigureRequest`/`Response` shared by the kinds; `Health` shadows `grpc.health.v1.Health` in imports | verify, `scripts/`, `proto/` | 6 | FIXED `f906dc6`: the executable bit set on `bin/protoc` only. Shared `ConfigureRequest` and the `Health` name: protocol shapes, no change, reason in `docs/handshake.md` (Services) |
| S14 | T1, T8 | README "or the reverse" copy rule stricter than the spec; CONTRIBUTING names `generate.sh` before it exists in the reading order; `.gitattributes`/`.editorconfig` extra; long lines and "an error from Run ends the stream" wording in `docs/handshake.md` | verify, `README.md`, `CONTRIBUTING.md`, `docs/handshake.md` | 6 | `f906dc6`, `8f2cf1c`, `f9806fe`: the "or the reverse" phrase was no longer in `README.md` (its copy line reworded to the spec); CONTRIBUTING introduces `scripts/generate.sh`; `.gitattributes` and `.editorconfig` lines that did nothing dropped; `docs/handshake.md` prose wrapped to 120 and the Run-error sentence matched to `sdk` |

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

- [x] **Step 4: Verify**: `go test ./handshake -race -count=1` on Linux; `GOOS=windows go vet ./handshake`; the
  Windows CI job green on the pull request. (Windows CI green on pull request getlaraplate/laraedge-sdk#3 at `8d12417`
  and `66892bf`.)

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
    without `reason`; conformance fails each with a message naming the rule. (The `retry_after` half was dropped
    later by ruling; see "Delivery status".)
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

- [x] **Step 1: Verify S13 and S14** item by item.

- [x] **Step 2: Fix**: the protoc download sets the executable bit only on the binary it runs; the README copy rule
  matches the spec's wording; CONTRIBUTING introduces `generate.sh` where the reader needs it; drop `.gitattributes`
  or `.editorconfig` lines that do nothing for this repository (keep what CI or the generator relies on); wrap
  `docs/handshake.md` to its width and correct the "an error from Run ends the stream" sentence to what `sdk`
  does. The shared `ConfigureRequest` and the `Health` name are protocol shapes: no change (Global Constraints),
  record the reason in `docs/handshake.md`.

- [x] **Step 3: Verify**: `go test ./... -count=1`; `scripts/generate.sh` (or the CI check that the generated code is
  current) reports no diff.

- [x] **Step 4: Commit**

```bash
git add scripts README.md CONTRIBUTING.md docs .gitattributes .editorconfig
git commit -m "docs: README, CONTRIBUTING and handshake.md minors"
```

### Task 7: Verify, release and close

**Files:**
- Modify: this plan, `docs/superpowers/plans/INDEX.md`, the release notes (the repository's existing place for them:
  `README.md` changelog section or the GitHub release body; follow `v0.1.1`)

- [x] **Step 1:** `go test ./... -race -count=1`, `go vet ./...`, `GOOS=windows go vet ./...`, the conformance command
  on the three examples. Expected: PASS, clean.

- [x] **Step 2:** every inventory row S1-S14 gets its resolution: the commit, FIXED/OBSOLETE with evidence, or the
  decision and where it is written.

- [x] **Step 3: Close the plan**: `status: closed`, Status "Closed (date)", boxes ticked, a delivery status with the
  divergences, the stricter conformance checks (S8, S10) named, the items outside this plan, and `**Documented in:**`;
  mark the INDEX row closed. Commit.

- [x] **Step 4: Release.** After the user merges the pull request: tag `v0.1.2` on the merge commit with release notes
  listing S1, S3, the stricter conformance checks and the fixes, the way `v0.1.1` was tagged. The tag is pushed only
  with the user's go-ahead. (Done: annotated tag `v0.1.2` on merge commit `4e9e10e`, pushed on 2026-11-05.)

- [x] **Step 5: Follow-up in `laraedge`**: a branch that bumps `github.com/getlaraplate/laraedge-sdk` to `v0.1.2`,
  runs the agent's suite and e2e, and opens a pull request. (Done: getlaraplate/laraedge#11, merged into `develop` as `b781e61`; suite
  2964 passed with `-race`, e2e 8 passed.)

## Delivery status (2026-11-04)

Tasks 1 to 6 are built on branch `chore/deferred-minors-cleanup`, each reviewed. A whole-branch review followed, with
one fix wave of 17 items (`ee3997b` to `8f2cf1c`). Its re-review found four residual minors, fixed in Task 7 (`2844c2b`
to `85083ca`), and the Task 7 review's minors (`41597aa`, `f821c0e`). Commits run from `8d12417` to `f821c0e`, plus the
closing commits. Every inventory row has its resolution in the table above. `proto/`, `pluginv1/`, `go.mod` and `go.sum`
are unchanged, and the golden descriptor test passes. No exported identifier or signature changed. Windows CI was green
on pull request getlaraplate/laraedge-sdk#3 at `8d12417` and `66892bf`. Task 7 Step 1 passed on Linux: `go test ./...
-race -count=1`, `go vet ./...`, `GOOS=windows go vet ./...`, `gofmt -l .`, and the conformance command on the three
examples as CI runs it (simulated-counter 12 checks, line-state 11, jsonl 12).

**Divergences from the plan**, each with its reason (the rulings are in "Controller rulings" below):

- **S8 is partial.** `REJECT_INVALID` without `reason` is enforced. `RETRY_AFTER` without `retry_after` is not:
  - `sdk.Outcome` documents a zero delay as "the agent uses its own backoff";
  - the agent tolerates a missing `retry_after`;
  - `v0.1.2` is promised as a drop-in for existing plugins.

  `docs/handshake.md` now says "optionally with `retry_after`". Review Focus 4 and the S8 row keep their history with
  a note. `BROKEN_RETRY_AFTER_BARE` stays as a case pinning that `output.send` passes.
- **S6 is obsolete.** jsonschema v6.0.3 reports the failing leaf, never a `dependentSchemas` error. The case Task 3
  added was dead code, removed; a test pins the generic message.
- **The ownership refusal is tested without root.** The plan expected root. An unprivileged run passes `/`, owned by
  root, as the socket directory. A root run skips the case; its comment names the manual check.
- **Manifest depth off-by-one.** The S7 boundary test found that 513 nesting levels were accepted, although the doc
  refuses anything deeper than 512. `manifest.Canonical` now refuses them.
- **jsonl per-path state and rotation.** Instances on one path share one lock and one dedupe state, keyed by the
  absolute path, rather than refusing a shared path. A file removed, replaced or shortened behind the sink is read
  again, and a pending truncation is dropped; since R2 this also holds before the first durable append. Every
  remaining limit is written in `examples/README.md`:
  - the ids are read once per process, so a reconfigure does not re-read the file;
  - symlink and hardlink aliases of one file get separate states;
  - a replacement between the check and the append waits for the next send.
- **`O_APPEND` dropped in jsonl.** On Windows an `O_APPEND` file cannot be truncated, so the sink writes at explicit
  offsets under the file lock.
- **Final review fix wave (17 items).**
  - First Process stream: the record sends are bounded by the timeout, and the probe is skipped after an early end.
  - jsonl: a failed `Close` after a successful `Sync` keeps the line; a failed partial write is tested; the file is
    read outside the sink lock.
  - `Outcome.Reason` is documented as required with `REJECT_INVALID`.
  - The Windows suspended-outside-the-job window is named, and `isSuspended` tolerates a thread that exits.
  - An interrupted run is reported as interrupted, not as "not refused".
  - Test comments and assertions; the CONTRIBUTING wrap.
- **Residuals of the re-review (Task 7).**
  - R1 `2844c2b`, `41597aa`: a processor that serves the second stream and ends the first one OK used to pass as
    "not probed". Now "not probed" is reported only when the first stream had already ended when the probe started.
    When it ends after the second stream was opened, a third stream decides. Refused with `ALREADY_EXISTS`: the first
    had ended on its own (pass, not probed). Served, or the second stream ends: the processor replaces its running
    stream (fail). Refused with another code: fail, naming the code. A sample on the second stream is not its end.
    The post-answer grace now only tells whether the first stream ended. New broken-plugin defects:
    `BROKEN_NEWEST_WINS`, `BROKEN_SAMPLE_ON_REFUSAL`.
  - Task 7 review: the jsonl `refresh` doc rewrapped and the `identity` comment corrected, `f821c0e`.
  - R2 `f798d62`: jsonl records identity and size at the clean point too.
  - R3 `f9806fe`: the Output outcomes paragraph of `docs/handshake.md` is wrapped.
  - R4 `85083ca`: the `BROKEN_RETRY_AFTER_BARE` comment says why `output.duplicate` fails.

**Stricter conformance checks** (release notes):

- `output.send`: `REJECT_INVALID` must carry a non-empty `reason`.
- `processor.signals`: while the first `Process` stream is open, a second stream of the instance must be refused
  `ALREADY_EXISTS`. A processor that serves it, or that ends its running stream to serve the new one, fails.
- The fixtures: `"records"` must be a list (`null` is refused).

**Behaviour changes a `v0.1.1` user may notice:**

- Manifest problem paths quote a key that is empty or contains `.`, `[`, `]`, `"`, whitespace or a control character:
  `config["a.b"]`.
- Problems sort in natural index order (`devices[2]` before `devices[10]`).
- A manifest config nested 513 levels deep is refused.

**Parked** (ledger "Parked" line), each with its reason:

- T1-d: the Wait-not-returned check cannot fail on correct code. It is a probabilistic regression guard, commented.
- T2-a: the back-pressure test catches a buffered pump only by scheduling.
- T2-b: the RED of the S3 test is probabilistic. It is commented as a regression guard: timing-dependent guards cannot
  be made deterministic.
- T5-d: the `faultyDisk` counters are unguarded. They are used from a single goroutine, which is documented.
- T5-f: symlink and hardlink aliases of one jsonl file get separate states. Documented in `examples/README.md`.

**Outside this plan:**

- `Release` of removed instances: a protocol addition, so a new RPC and a minor version. It gets its own plan across
  `laraedge-sdk` and `laraedge` (laraedge plan 3, Q7; ruling R18 of `2026-10-23-sdk.md`).
- Bumping `laraedge` to `v0.1.2`: done as Task 7 Step 5 (getlaraplate/laraedge#11).

**Release (2026-11-05):** `v0.1.2` tagged on merge commit `4e9e10e` (getlaraplate/laraedge-sdk#3); `laraedge` bumped
to it in getlaraplate/laraedge#11.

**Documented in:** `docs/handshake.md`, `examples/README.md`, `README.md`, `CONTRIBUTING.md`, and the package
documentation of `sdk`, `handshake`, `manifest` and `conformance`.

## Controller rulings (subagent-driven execution, 2026-11-04)

Every ruling the controller took while executing this plan, copied verbatim from its execution ledger.

- Ruling: T5 jsonl RETRY_AFTER sets retry_after — the T4 conformance rule from docs/handshake.md requires it — cost if wrong: one field on a response.
- Ruling: T7 stops after Step 3 (close) and PR; tag v0.1.2 and laraedge bump wait for the user's merge and go-ahead — norms on publish — cost if wrong: one extra exchange.
- Ruling: Task 3 path form — plain keys keep dotted/indexed form, only keys with . [ ] " or non-identifier get config["..."] (JSON-escaped) — minimal change to existing messages, unambiguous — cost if wrong: message format rework in manifest+docs
- Ruling: Task 3 path quoting loosened — quote only empty keys or keys with . [ ] " whitespace/control; hyphens stay plain — operators read config.read-timeout more easily, ambiguity only comes from separators — cost if wrong: one predicate. Sent to implementer before review.
- Ruling: Task 5 fix — per-path file state map on sink (shared lock, dirty/clean) rather than refusing shared paths — keeps prior behaviour of two instances on one path — cost if wrong: refactor of sink state
- Ruling: final whole-branch review and its fix wave run before Task 7 (close) — the close records resolutions, which must include the fix wave; the user asked that minors be resolved, so the fix wave takes the deferred minors the final review triages as fixable — cost if wrong: Task 7 reorder only
- Ruling: drop the RETRY_AFTER-requires-retry_after conformance check, keep REJECT_INVALID/reason — the SDK's public contract (zero delay = agent backoff), the agent's tolerance and the v0.1.2 drop-in promise outrank the handshake.md parenthetical the plan read as a requirement; handshake.md reworded to "optionally" — cost if wrong: plugins without retry_after pass conformance, the agent backs off on its own
- Ruling: fix wave takes every MUST-FIX and SHOULD-FIX (17 items) — the user asked that this plan's minors be resolved, not carried — cost if wrong: a larger fix diff
- Parked: T1-d, T2-a, T2-b, T5-d, T5-f(README sentence) — Ruling: timing-dependent guards cannot be deterministic; single-goroutine counters documented; link aliases documented — cost if wrong: weaker regression guards
- Ruling: residuals R1-R4 are folded into Task 7's dispatch (a plan task with its own task review), not a second fix wave — they are small, real, and the user asked that this plan's minors be resolved — cost if wrong: one more review seat
