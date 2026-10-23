// Package conformance checks that a plugin binary keeps the contract of protocol
// laraplate.edge.plugin.v1 (design spec section 7). It drives the plugin through the host side
// of package handshake, as the agent does, and reports one [Check] per item of the contract.
// The command laraedge-conformance runs it from the shell.
//
// The plugin directory holds plugin.json, conformance.json (fixtures, decision S8) and the
// binary, named after the manifest's name (plus .exe on Windows). Every process the suite starts
// runs with a minimal environment (PATH, TZ, SystemRoot on Windows), its working directory in a
// fresh temporary directory and its socket directory in another, never in the plugin directory.
//
// The checks, in report order, with their stable names:
//
//	manifest             plugin.json loads and is an acceptable manifest, and the binary
//	                     named after it is in the directory
//	fixtures             conformance.json holds valid and invalid sections for every kind
//	                     plugin.json offers, and none for a kind it does not offer
//	no-cookie            started by hand without the cookie, the plugin exits at once with a
//	                     non-zero code and prints no handshake line
//	handshake            the plugin prints a valid handshake line and accepts a connection
//	permissions          socket 0600 in a 0700 directory (Unix); a protected pipe DACL that
//	                     grants only the current user (Windows)
//	info                 Info matches plugin.json: name, version, protocol versions and the
//	                     manifest in canonical form
//	health               grpc.health.v1 Check answers SERVING
//	input.configure      every valid input fixture is accepted, every invalid one refused with
//	                     configuration errors
//	input.stream         the stream of the first valid fixture delivers an event within the
//	                     timeout and ends on cancellation: a new stream of the same instance is
//	                     served within the timeout afterwards (a plugin refuses a second stream
//	                     of an instance while the first runs, as the SDK does)
//	input.probe          Probe answers within its 30 s deadline with one sample per point
//	processor.configure  as input.configure, for processor fixtures
//	processor.signals    fed a birth of every observed device of the first valid fixture, then
//	                     integer samples 1, 2, 3 of each signal name the fixture declares, the
//	                     processor emits at least one sample, and only of the devices and
//	                     signals the fixture declares
//	output.configure     as input.configure, for output fixtures
//	output.send          a valid batch is answered with one of the six outcomes
//	output.duplicate     the same message_id sent twice is answered ACCEPTED or DUPLICATE both
//	                     times, never with an error
//	shutdown             after Shutdown with a 5 s deadline the plugin exits 0 within it (plus
//	                     1 s to be reaped); an UNAVAILABLE answer or EOF counts as an answer
//	restart              after a kill -9, a new launch with the same socket directory performs
//	                     a new handshake on a new address, and the old socket file is gone
//
// The checks of a kind appear only when plugin.json offers that kind. A check that cannot run
// because one it builds on failed (for instance every check that needs a connection when the
// handshake failed) is reported failed with a Detail starting with "not run: ": a report never
// passes a check it did not perform. For the same reason a check with nothing to judge fails: an
// input stream without events, a processor that emits nothing for the records it is fed.
package conformance

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/getlaraplate/laraedge-sdk/manifest"
)

// DefaultTimeout is the timeout when [Options.Timeout] is not set.
const DefaultTimeout = 10 * time.Second

// Deadlines the protocol fixes (design spec sections 4 and 6).
const (
	infoDeadline      = 5 * time.Second
	configureDeadline = 10 * time.Second
	probeDeadline     = 30 * time.Second
	sendDeadline      = 30 * time.Second
	healthDeadline    = 2 * time.Second
	shutdownDeadline  = 5 * time.Second
	// exitGrace is the time allowed past the Shutdown deadline for the process to be reaped.
	exitGrace = time.Second
)

// notRunPrefix starts the Detail of a check that could not run.
const notRunPrefix = "not run: "

// Options tunes a run.
type Options struct {
	// Timeout bounds every wait the protocol does not fix: the exit of a plugin started without
	// the cookie, the connection after the handshake, the first event of an input stream, the
	// end of a cancelled stream, the samples of a processor. Zero means [DefaultTimeout]. The
	// deadlines of the protocol (Info 5 s, Configure 10 s, Probe 30 s, Send 30 s, Shutdown 5 s,
	// health 2 s, handshake 10 s) are not affected.
	Timeout time.Duration
	// Stderr receives the stderr of every process the suite starts, and its stdout after the
	// handshake line. Nil discards.
	Stderr io.Writer
}

// Report is the result of a run.
type Report struct {
	// Checks are in the order of the package documentation.
	Checks []Check
	// Passed is true when every check passed.
	Passed bool
}

// Check is the result of one check.
type Check struct {
	// Name is one of the stable names of the package documentation.
	Name string
	// Passed is true when the check ran and the plugin met it.
	Passed bool
	// Detail says what was observed, or why the check did not run.
	Detail string
}

// Run checks the plugin in dir. ctx bounds the whole run; every process the suite started has
// exited when Run returns.
func Run(ctx context.Context, dir string, opts Options) Report {
	return run(ctx, dir, opts, nil)
}

// run is Run with extra environment variables for the plugin, which the tests use to select
// the defects of their broken plugin.
func run(ctx context.Context, dir string, opts Options, extraEnv []string) Report {
	s := &suite{ctx: ctx, dir: dir, timeout: opts.Timeout, extraEnv: extraEnv}
	if s.timeout <= 0 {
		s.timeout = DefaultTimeout
	}
	s.output = &lockedWriter{w: io.Discard}
	if opts.Stderr != nil {
		s.output.w = opts.Stderr
	}
	defer s.cleanup()
	s.runChecks()
	report := Report{Checks: s.checks, Passed: len(s.checks) > 0}
	for _, c := range s.checks {
		if !c.Passed {
			report.Passed = false
		}
	}
	return report
}

// kindFixtures are the sections of one kind in conformance.json.
type kindFixtures struct {
	Valid   []json.RawMessage `json:"valid"`
	Invalid []json.RawMessage `json:"invalid"`
}

// fixtures is conformance.json (decision S8).
type fixtures struct {
	Input     *kindFixtures `json:"input"`
	Processor *kindFixtures `json:"processor"`
	Output    *kindFixtures `json:"output"`
}

// suite is the state of one run.
type suite struct {
	ctx      context.Context
	dir      string
	timeout  time.Duration
	extraEnv []string
	output   *lockedWriter

	checks []Check

	manifest    manifest.Manifest
	manifestRaw []byte
	fixtures    fixtures
	binary      string

	tempRoot string
	tempSeq  int
	cleanups []func()
}

func (s *suite) pass(name, format string, args ...any) {
	s.checks = append(s.checks, Check{Name: name, Passed: true, Detail: fmt.Sprintf(format, args...)})
}

func (s *suite) fail(name, format string, args ...any) {
	s.checks = append(s.checks, Check{Name: name, Detail: fmt.Sprintf(format, args...)})
}

func (s *suite) notRun(reason string, names ...string) {
	for _, name := range names {
		s.checks = append(s.checks, Check{Name: name, Detail: notRunPrefix + reason})
	}
}

// kinds lists the kinds plugin.json offers, in report order.
func (s *suite) kinds() []string {
	var kinds []string
	if s.manifest.Kinds.Input != nil {
		kinds = append(kinds, "input")
	}
	if s.manifest.Kinds.Processor != nil {
		kinds = append(kinds, "processor")
	}
	if s.manifest.Kinds.Output != nil {
		kinds = append(kinds, "output")
	}
	return kinds
}

// kindChecks are the names of the checks of a kind.
var kindChecks = map[string][]string{
	"input":     {"input.configure", "input.stream", "input.probe"},
	"processor": {"processor.configure", "processor.signals"},
	"output":    {"output.configure", "output.send", "output.duplicate"},
}

func (s *suite) fixturesOf(kind string) *kindFixtures {
	switch kind {
	case "input":
		return s.fixtures.Input
	case "processor":
		return s.fixtures.Processor
	default:
		return s.fixtures.Output
	}
}

// runChecks runs every check in report order.
func (s *suite) runChecks() {
	if !s.checkManifest() {
		s.notRun("plugin.json is not usable", "fixtures", "no-cookie", "handshake", "permissions", "info", "health",
			"shutdown", "restart")
		return
	}
	complete := s.checkFixtures()
	s.checkNoCookie()

	sess, ok := s.checkHandshake()
	if !ok {
		names := []string{"permissions", "info", "health"}
		for _, kind := range s.kinds() {
			names = append(names, kindChecks[kind]...)
		}
		s.notRun("the handshake failed", append(names, "shutdown", "restart")...)
		return
	}
	s.checkPermissions(sess)
	s.checkInfo(sess)
	s.checkHealth(sess)
	for _, kind := range s.kinds() {
		if !complete[kind] {
			s.notRun("conformance.json lacks valid or invalid "+kind+" fixtures", kindChecks[kind]...)
			continue
		}
		instance, configured := s.checkConfigure(sess, kind)
		reason := "the first valid " + kind + " fixture was not accepted"
		switch kind {
		case "input":
			if !configured {
				s.notRun(reason, "input.stream", "input.probe")
				break
			}
			s.checkInputStream(sess, instance)
			s.checkInputProbe(sess, instance)
		case "processor":
			if !configured {
				s.notRun(reason, "processor.signals")
				break
			}
			s.checkProcessorSignals(sess, instance)
		case "output":
			if !configured {
				s.notRun(reason, "output.send", "output.duplicate")
				break
			}
			s.checkOutputSend(sess, instance)
		}
	}
	s.checkShutdown(sess)
	s.checkRestart()
}

// checkManifest loads plugin.json and finds the binary.
func (s *suite) checkManifest() bool {
	// The plugin's processes run in other directories: the binary's path must not be relative.
	dir, err := filepath.Abs(s.dir)
	if err != nil {
		s.fail("manifest", "the plugin directory: %v", err)
		return false
	}
	s.dir = dir
	m, raw, err := manifest.Load(filepath.Join(s.dir, "plugin.json"))
	if err != nil {
		s.fail("manifest", "%v", err)
		return false
	}
	if problems := m.Check(); len(problems) > 0 {
		s.fail("manifest", "plugin.json is not acceptable: %s", strings.Join(problems, "; "))
		return false
	}
	s.manifest, s.manifestRaw = m, raw
	name := m.Name
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	s.binary = filepath.Join(s.dir, name)
	info, err := os.Stat(s.binary)
	if err != nil {
		s.fail("manifest", "the plugin binary: %v", err)
		return false
	}
	if !info.Mode().IsRegular() {
		s.fail("manifest", "the plugin binary %s is not a regular file", s.binary)
		return false
	}
	s.pass("manifest", "%s %s offers %s", m.Name, m.Version, strings.Join(s.kinds(), ", "))
	return true
}

// checkFixtures loads conformance.json and reports, per offered kind, whether it has both valid
// and invalid sections.
func (s *suite) checkFixtures() map[string]bool {
	complete := map[string]bool{}
	raw, err := os.ReadFile(filepath.Join(s.dir, "conformance.json"))
	if err != nil {
		s.fail("fixtures", "%v", err)
		return complete
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&s.fixtures); err != nil {
		s.fail("fixtures", "conformance.json: %v", err)
		return complete
	}
	if dec.More() {
		s.fail("fixtures", "conformance.json: data after the JSON object")
		return complete
	}
	var problems, counts []string
	for _, kind := range []string{"input", "processor", "output"} {
		fx := s.fixturesOf(kind)
		offered := slices.Contains(s.kinds(), kind)
		switch {
		case !offered && fx != nil:
			problems = append(problems, "fixtures for "+kind+", which plugin.json does not offer")
		case !offered:
		case fx == nil || len(fx.Valid) == 0 || len(fx.Invalid) == 0:
			problems = append(problems, kind+" is offered but lacks valid or invalid fixtures")
		default:
			if problem := sectionsAreObjects(kind, fx); problem != "" {
				problems = append(problems, problem)
				break
			}
			complete[kind] = true
			counts = append(counts, fmt.Sprintf("%s: %d valid, %d invalid", kind, len(fx.Valid), len(fx.Invalid)))
		}
	}
	if len(problems) > 0 {
		s.fail("fixtures", "%s", strings.Join(problems, "; "))
		return complete
	}
	s.pass("fixtures", "%s", strings.Join(counts, "; "))
	return complete
}

// sectionsAreObjects reports the first fixture of kind that is not a JSON object.
func sectionsAreObjects(kind string, fx *kindFixtures) string {
	for _, group := range []struct {
		name     string
		sections []json.RawMessage
	}{{"valid", fx.Valid}, {"invalid", fx.Invalid}} {
		for i, section := range group.sections {
			var object map[string]json.RawMessage
			if json.Unmarshal(section, &object) != nil || object == nil {
				return fmt.Sprintf("%s.%s[%d] is not a JSON object", kind, group.name, i)
			}
		}
	}
	return ""
}

// env is the minimal environment of the plugin, without the handshake variables.
func (s *suite) env() []string {
	keys := []string{"PATH", "TZ"}
	if runtime.GOOS == "windows" {
		keys = append(keys, "SystemRoot")
	}
	var env []string
	for _, key := range keys {
		if value, ok := os.LookupEnv(key); ok {
			env = append(env, key+"="+value)
		}
	}
	return append(env, s.extraEnv...)
}

// tempDir returns a fresh directory below the run's temporary root, whose short path keeps Unix
// socket paths under their length limit.
func (s *suite) tempDir() (string, error) {
	if s.tempRoot == "" {
		root, err := os.MkdirTemp("", "lec")
		if err != nil {
			return "", err
		}
		s.tempRoot = root
	}
	s.tempSeq++
	dir := filepath.Join(s.tempRoot, fmt.Sprintf("%d", s.tempSeq))
	if err := os.Mkdir(dir, 0o700); err != nil {
		return "", err
	}
	return dir, nil
}

// onCleanup registers fn to run when the run ends, latest first.
func (s *suite) onCleanup(fn func()) {
	s.cleanups = append(s.cleanups, fn)
}

func (s *suite) cleanup() {
	for _, fn := range slices.Backward(s.cleanups) {
		fn()
	}
	if s.tempRoot != "" {
		_ = os.RemoveAll(s.tempRoot)
	}
}

// lockedWriter serialises the writes of the processes' output goroutines.
type lockedWriter struct {
	mu sync.Mutex
	w  io.Writer
}

func (l *lockedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(p)
}
