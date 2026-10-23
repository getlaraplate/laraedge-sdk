package conformance_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/getlaraplate/laraedge-sdk/conformance"
)

// pluginDirs maps each plugin under test to a directory holding its binary, plugin.json and
// conformance.json, as the suite expects them.
var pluginDirs = map[string]string{}

// pluginSources are the packages built for the tests, by plugin name.
var pluginSources = map[string]string{
	"simulated-counter": "../examples/simulated-counter",
	"line-state":        "../examples/line-state",
	"jsonl":             "../examples/jsonl",
	"broken":            "./testdata/broken",
}

func TestMain(m *testing.M) {
	root, err := os.MkdirTemp("", "conformance-plugins-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := buildPlugins(root); err != nil {
		fmt.Fprintln(os.Stderr, err)
		_ = os.RemoveAll(root)
		os.Exit(1)
	}
	code := m.Run()
	_ = os.RemoveAll(root)
	os.Exit(code)
}

func buildPlugins(root string) error {
	for name, source := range pluginSources {
		dir := filepath.Join(root, name)
		if err := os.Mkdir(dir, 0o755); err != nil {
			return err
		}
		build := exec.Command("go", "build", "-o", filepath.Join(dir, binaryName(name)), source)
		if out, err := build.CombinedOutput(); err != nil {
			return fmt.Errorf("building %s: %v\n%s", source, err, out)
		}
		for _, file := range []string{"plugin.json", "conformance.json"} {
			if err := copyFile(filepath.Join(source, file), filepath.Join(dir, file)); err != nil {
				return err
			}
		}
		pluginDirs[name] = dir
	}
	return nil
}

func binaryName(name string) string {
	if runtime.GOOS == "windows" {
		return name + ".exe"
	}
	return name
}

func copyFile(from, to string) error {
	raw, err := os.ReadFile(from)
	if err != nil {
		return err
	}
	return os.WriteFile(to, raw, 0o755)
}

// syncBuffer collects the plugins' output, written from the host's goroutines.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

const notRun = "not run: "

var (
	commonChecks    = []string{"manifest", "fixtures", "no-cookie", "handshake", "permissions", "info", "health"}
	inputChecks     = []string{"input.configure", "input.stream", "input.probe"}
	processorChecks = []string{"processor.configure", "processor.signals"}
	outputChecks    = []string{"output.configure", "output.send", "output.duplicate"}
	closingChecks   = []string{"shutdown", "restart"}
)

// checkNames is the list of checks of a plugin offering the given kinds, in report order.
func checkNames(input, processor, output bool) []string {
	names := slices.Clone(commonChecks)
	if input {
		names = append(names, inputChecks...)
	}
	if processor {
		names = append(names, processorChecks...)
	}
	if output {
		names = append(names, outputChecks...)
	}
	return append(names, closingChecks...)
}

// runSuite runs the suite on dir with extra plugin environment and logs the plugin's output
// when the test fails.
func runSuite(t *testing.T, dir string, timeout time.Duration, env ...string) conformance.Report {
	t.Helper()
	output := &syncBuffer{}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	report := conformance.RunWithEnv(ctx, dir, conformance.Options{Timeout: timeout, Stderr: output}, env)
	t.Logf("report:\n%s", formatReport(report))
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("plugin output:\n%s", output.String())
		}
	})
	return report
}

func formatReport(report conformance.Report) string {
	var b strings.Builder
	for _, c := range report.Checks {
		fmt.Fprintf(&b, "  %s passed=%t: %s\n", c.Name, c.Passed, c.Detail)
	}
	fmt.Fprintf(&b, "  passed=%t", report.Passed)
	return b.String()
}

// assertReport requires the checks named want, in order: failing (when set) failed on its own
// account, the checks in skipped failed as not run, and every other check passed.
func assertReport(t *testing.T, report conformance.Report, want []string, failing string, skipped ...string) {
	t.Helper()
	var names []string
	for _, c := range report.Checks {
		names = append(names, c.Name)
	}
	if !slices.Equal(names, want) {
		t.Fatalf("checks %v, want %v", names, want)
	}
	for _, c := range report.Checks {
		switch {
		case c.Name == failing:
			if c.Passed {
				t.Errorf("check %s passed, want it to fail", c.Name)
			} else if strings.HasPrefix(c.Detail, notRun) {
				t.Errorf("check %s was not run (%s), want it to run and fail", c.Name, c.Detail)
			}
		case slices.Contains(skipped, c.Name):
			if c.Passed || !strings.HasPrefix(c.Detail, notRun) {
				t.Errorf("check %s: passed=%t detail %q, want it failed as not run", c.Name, c.Passed, c.Detail)
			}
		default:
			if !c.Passed {
				t.Errorf("check %s failed: %s", c.Name, c.Detail)
			}
		}
		if c.Detail == "" {
			t.Errorf("check %s has no detail", c.Name)
		}
	}
	wantPassed := failing == "" && len(skipped) == 0
	if report.Passed != wantPassed {
		t.Errorf("report passed=%t, want %t", report.Passed, wantPassed)
	}
}

func TestExamplesPassEveryCheck(t *testing.T) {
	tests := []struct {
		plugin                   string
		input, processor, output bool
	}{
		{plugin: "simulated-counter", input: true},
		{plugin: "line-state", processor: true},
		{plugin: "jsonl", output: true},
	}
	for _, tt := range tests {
		t.Run(tt.plugin, func(t *testing.T) {
			t.Parallel()
			report := runSuite(t, pluginDirs[tt.plugin], 0)
			assertReport(t, report, checkNames(tt.input, tt.processor, tt.output), "")
		})
	}
}

func TestBrokenPluginWithoutDefectsPassesEveryCheck(t *testing.T) {
	report := runSuite(t, pluginDirs["broken"], 5*time.Second)
	assertReport(t, report, checkNames(true, true, true), "")
}

// TestEachDefectFailsItsCheck runs the broken plugin with one defect at a time: the check of
// the defect fails, the checks that build on it are not run, every other check passes.
func TestEachDefectFailsItsCheck(t *testing.T) {
	allKinds := slices.Concat(inputChecks, processorChecks, outputChecks)
	tests := []struct {
		name    string
		env     string
		failing string
		skipped []string
	}{
		{name: "started by hand without the cookie does not exit", env: "BROKEN_IGNORE_COOKIE=1", failing: "no-cookie"},
		{
			name: "stray line before the handshake line", env: "BROKEN_STRAY_LINE=1", failing: "handshake",
			skipped: slices.Concat([]string{"permissions", "info", "health"}, allKinds, closingChecks),
		},
		{name: "wrong permissions", env: "BROKEN_PERMISSIONS=1", failing: "permissions"},
		{name: "Info differs from plugin.json", env: "BROKEN_INFO=1", failing: "info"},
		{name: "health not SERVING", env: "BROKEN_HEALTH=1", failing: "health"},
		{name: "invalid fixture accepted", env: "BROKEN_ACCEPT_INVALID=output", failing: "output.configure"},
		{name: "input stream does not end on cancel", env: "BROKEN_STUCK_STREAM=1", failing: "input.stream"},
		{name: "processor emits an undeclared signal", env: "BROKEN_UNDECLARED_SIGNAL=1", failing: "processor.signals"},
		{name: "same message_id twice gives an error", env: "BROKEN_DUPLICATE_ERROR=1", failing: "output.duplicate"},
		{name: "Shutdown past its deadline", env: "BROKEN_SLOW_SHUTDOWN=1", failing: "shutdown"},
		{name: "no new socket after a kill -9 restart", env: "BROKEN_FIXED_ADDRESS=1", failing: "restart"},
		// A check with nothing to judge fails instead of passing vacuously.
		{name: "input stream without events", env: "BROKEN_SILENT=input", failing: "input.stream"},
		{name: "processor without output", env: "BROKEN_SILENT=processor", failing: "processor.signals"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			report := runSuite(t, pluginDirs["broken"], 3*time.Second, tt.env)
			assertReport(t, report, checkNames(true, true, true), tt.failing, tt.skipped...)
		})
	}
}

// TestMissingFixturesOfAnOfferedKind removes the output fixtures of the broken plugin, which
// offers the output kind (S8).
func TestMissingFixturesOfAnOfferedKind(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	broken := pluginDirs["broken"]
	for _, file := range []string{binaryName("broken"), "plugin.json"} {
		if err := copyFile(filepath.Join(broken, file), filepath.Join(dir, file)); err != nil {
			t.Fatal(err)
		}
	}
	raw, err := os.ReadFile(filepath.Join(broken, "conformance.json"))
	if err != nil {
		t.Fatal(err)
	}
	start := bytes.Index(raw, []byte(`"output"`))
	if start < 0 {
		t.Fatal(`conformance.json of the broken plugin has no "output" member`)
	}
	// "output" is the last member: cut it and the comma before it.
	withoutOutput := append(bytes.TrimRight(raw[:start], ", \n"), []byte("\n}\n")...)
	if err := os.WriteFile(filepath.Join(dir, "conformance.json"), withoutOutput, 0o644); err != nil {
		t.Fatal(err)
	}

	report := runSuite(t, dir, 3*time.Second)
	assertReport(t, report, checkNames(true, true, true), "fixtures", outputChecks...)
}

// TestDirectoryWithoutPlugin: without plugin.json nothing can run, and the report says so.
func TestDirectoryWithoutPlugin(t *testing.T) {
	t.Parallel()
	report := runSuite(t, t.TempDir(), time.Second)
	want := slices.Concat(commonChecks, closingChecks)
	skipped := slices.DeleteFunc(slices.Clone(want), func(name string) bool { return name == "manifest" })
	assertReport(t, report, want, "manifest", skipped...)
}

// TestPluginOutputGoesToStderr: the plugin's stderr reaches Options.Stderr.
func TestPluginOutputGoesToStderr(t *testing.T) {
	t.Parallel()
	output := &syncBuffer{}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	conformance.Run(ctx, pluginDirs["jsonl"], conformance.Options{Stderr: output})
	// The run without the cookie makes the plugin explain on stderr why it refuses to start.
	if !strings.Contains(output.String(), "not meant to be run by hand") {
		t.Errorf("plugin output %q lacks the plugin's stderr", output.String())
	}
}

// TestRelativeDirectory: a plugin directory relative to the working directory of the caller
// works for every check, also for the processes started in their own working directory.
func TestRelativeDirectory(t *testing.T) {
	dir := pluginDirs["jsonl"]
	t.Chdir(filepath.Dir(dir))
	report := runSuite(t, filepath.Base(dir), 0)
	assertReport(t, report, checkNames(false, false, true), "")
}

// TestMalformedFixtures: conformance.json is read strictly; the kinds whose fixtures cannot be
// used are not run.
func TestMalformedFixtures(t *testing.T) {
	valid := `{"valid": [{"id": "o", "config": {}}], "invalid": [{"id": "r", "refuse": true, "config": {}}]}`
	tests := []struct {
		name     string
		plugin   string
		fixtures string
		skipped  []string
	}{
		{
			name: "unknown member", plugin: "jsonl", skipped: outputChecks,
			fixtures: `{"output": {"valid": [{"id": "o", "config": {"path": "x"}}], "invalid": [{"id": "i", "config": {}}], "extra": []}}`,
		},
		{
			name: "data after the object", plugin: "jsonl", skipped: outputChecks,
			fixtures: `{"output": {"valid": [{"id": "o", "config": {"path": "x"}}], "invalid": [{"id": "i", "config": {}}]}} {}`,
		},
		{
			name: "section that is not an object", plugin: "broken", skipped: inputChecks,
			fixtures: `{"input": {"valid": [[]], "invalid": [{}]}, "processor": ` +
				`{"valid": [{"id": "p", "observe": ["m"], "devices": [{"device": "d", "signals": [{"signal": "s"}]}]}], ` +
				`"invalid": [{"refuse": true}]}, "output": ` + valid + `}`,
		},
		{
			name: "fixtures for a kind not offered", plugin: "jsonl",
			fixtures: `{"input": ` + valid + `, "output": {"valid": [{"id": "o", "config": {"path": "x"}}], "invalid": [{"id": "i", "config": {}}]}}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			source := pluginDirs[tt.plugin]
			for _, file := range []string{binaryName(tt.plugin), "plugin.json"} {
				if err := copyFile(filepath.Join(source, file), filepath.Join(dir, file)); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.WriteFile(filepath.Join(dir, "conformance.json"), []byte(tt.fixtures), 0o644); err != nil {
				t.Fatal(err)
			}
			report := runSuite(t, dir, 3*time.Second)
			plugin := tt.plugin == "broken"
			assertReport(t, report, checkNames(plugin, plugin, true), "fixtures", tt.skipped...)
		})
	}
}
