package handshake_test

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/getlaraplate/laraedge-sdk/handshake"
)

// Environment switches of the parent-death test: the test binary re-executes itself as an
// intermediate host that launches the helper and is then killed.
const (
	hostModeKey      = "HANDSHAKE_TEST_HOST_MODE"
	helperPathKey    = "HANDSHAKE_TEST_HELPER"
	hostSocketDirKey = "HANDSHAKE_TEST_SOCKET_DIR"
)

// helperPath is the helper plugin built from testdata/plugin once per test binary.
var helperPath string

func TestMain(m *testing.M) {
	if os.Getenv(hostModeKey) == "1" {
		helperPath = os.Getenv(helperPathKey)
		os.Exit(m.Run())
	}
	dir, err := os.MkdirTemp("", "handshake-helper-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	helperPath = filepath.Join(dir, "plugin")
	if runtime.GOOS == "windows" {
		helperPath += ".exe"
	}
	build := exec.Command("go", "build", "-o", helperPath, "./testdata/plugin")
	build.Stdout = os.Stderr
	build.Stderr = os.Stderr
	if err := build.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "building the helper plugin: %v\n", err)
		_ = os.RemoveAll(dir)
		os.Exit(1)
	}
	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

// syncBuffer is a bytes.Buffer safe for the host's output goroutines and the test.
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

// socketDir returns a short private directory: Unix socket paths have a small length limit.
func socketDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "hs")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

// helperEnv is the minimal environment of the helper plus extra entries.
func helperEnv(extra ...string) []string {
	var env []string
	if runtime.GOOS == "windows" {
		env = append(env, "SystemRoot="+os.Getenv("SystemRoot"))
	}
	return append(env, extra...)
}

func wantNetwork() string {
	if runtime.GOOS == "windows" {
		return "npipe"
	}
	return "unix"
}

func config(t *testing.T, extra ...string) handshake.LaunchConfig {
	t.Helper()
	return handshake.LaunchConfig{
		Path:      helperPath,
		Dir:       filepath.Dir(helperPath),
		SocketDir: socketDir(t),
		Protocols: []string{"v1"},
		Env:       helperEnv(extra...),
		Timeout:   10 * time.Second,
	}
}

func launch(t *testing.T, cfg handshake.LaunchConfig) *handshake.Process {
	t.Helper()
	process, err := handshake.Launch(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Launch: %v", err)
	}
	t.Cleanup(func() {
		_ = process.Kill()
		_ = process.Wait()
	})
	return process
}

// checkHealth calls grpc.health.v1.Health/Check through the process's connection.
func checkHealth(t *testing.T, process *handshake.Process) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := process.Dial(ctx)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = conn.Close() }()
	response, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("Health/Check: %v", err)
	}
	if response.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("health status %v, want SERVING", response.GetStatus())
	}
}

// readLine reads one line from r, failing the test after timeout.
func readLine(t *testing.T, r *bufio.Reader, timeout time.Duration) string {
	t.Helper()
	type result struct {
		line string
		err  error
	}
	done := make(chan result, 1)
	go func() {
		line, err := r.ReadString('\n')
		done <- result{strings.TrimRight(line, "\r\n"), err}
	}()
	select {
	case got := <-done:
		if got.err != nil {
			t.Fatalf("reading a line: %v (got %q)", got.err, got.line)
		}
		return got.line
	case <-time.After(timeout):
		t.Fatalf("no line within %v", timeout)
		return ""
	}
}

func TestLaunchCompletesTheHandshakeAndServesHealth(t *testing.T) {
	process := launch(t, config(t))
	if process.Protocol != "v1" {
		t.Errorf("Protocol %q, want v1", process.Protocol)
	}
	if process.Network != wantNetwork() {
		t.Errorf("Network %q, want %q", process.Network, wantNetwork())
	}
	if process.Address == "" {
		t.Error("empty Address")
	}
	if process.Pid <= 0 {
		t.Errorf("Pid %d", process.Pid)
	}
	checkHealth(t, process)
}

func TestPluginStartedByHandExitsWithErrNotLaunched(t *testing.T) {
	cmd := exec.Command(helperPath)
	cmd.Env = helperEnv()
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	err := cmd.Run()
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() == 0 {
		t.Fatalf("helper without cookie: err %v, want a non-zero exit", err)
	}
	if !strings.Contains(stderr.String(), handshake.ErrNotLaunched.Error()) {
		t.Errorf("stderr %q does not carry %q", stderr.String(), handshake.ErrNotLaunched.Error())
	}
}

func TestLaunchFailures(t *testing.T) {
	stray := strings.Repeat("0123456789", 10)
	otherNetwork := "unix"
	if runtime.GOOS != "windows" {
		otherNetwork = "npipe"
	}
	cases := []struct {
		name       string
		env        []string
		timeout    time.Duration
		ctxTimeout time.Duration
		check      func(t *testing.T, err error)
	}{
		{
			name:    "no common protocol exits 3 and is reported",
			env:     []string{"HELPER_SUPPORTED=v2"},
			timeout: time.Hour,
			check: func(t *testing.T, err error) {
				if !errors.Is(err, handshake.ErrNoProtocol) {
					t.Errorf("err %v, want ErrNoProtocol", err)
				}
				var exitErr *handshake.ExitError
				if !errors.As(err, &exitErr) || exitErr.ExitCode != handshake.ExitCodeNoProtocol {
					t.Fatalf("err %v, want an ExitError with code %d", err, handshake.ExitCodeNoProtocol)
				}
				if !strings.Contains(strings.Join(exitErr.Stderr, "\n"), "v2") {
					t.Errorf("stderr tail %q does not name the plugin's protocols", exitErr.Stderr)
				}
			},
		},
		{
			name:    "crash before the line returns at once with the stderr tail",
			env:     []string{"HELPER_CRASH=1"},
			timeout: time.Hour,
			check: func(t *testing.T, err error) {
				var exitErr *handshake.ExitError
				if !errors.As(err, &exitErr) {
					t.Fatalf("err %v, want an ExitError", err)
				}
				if exitErr.ExitCode != 7 {
					t.Errorf("exit code %d, want 7", exitErr.ExitCode)
				}
				var want []string
				for i := 11; i <= 30; i++ {
					want = append(want, "crash line "+strconv.Itoa(i))
				}
				if !slices.Equal(exitErr.Stderr, want) {
					t.Errorf("stderr tail %q, want lines 11 to 30", exitErr.Stderr)
				}
				if !strings.Contains(err.Error(), "crash line 30") {
					t.Errorf("message %q lacks the last stderr line", err)
				}
				if errors.Is(err, handshake.ErrNoProtocol) {
					t.Error("a crash is reported as ErrNoProtocol")
				}
			},
		},
		{
			name:    "stray line before the handshake is quoted to 80 characters",
			env:     []string{"HELPER_STRAY=" + stray},
			timeout: time.Hour,
			check: func(t *testing.T, err error) {
				if !errors.Is(err, handshake.ErrBadHandshakeLine) {
					t.Fatalf("err %v, want ErrBadHandshakeLine", err)
				}
				if !strings.Contains(err.Error(), strconv.Quote(stray[:80])) {
					t.Errorf("message %q does not quote the first 80 characters", err)
				}
				if strings.Contains(err.Error(), stray[:81]) {
					t.Errorf("message %q quotes more than 80 characters", err)
				}
			},
		},
		{
			name:    "protocol the host did not offer",
			env:     []string{"HELPER_LINE=1|v9|" + wantNetwork() + "|x"},
			timeout: time.Hour,
			check:   wantErr(handshake.ErrBadHandshakeLine),
		},
		{
			name:    "network of the other platform",
			env:     []string{"HELPER_LINE=1|v1|" + otherNetwork + "|x"},
			timeout: time.Hour,
			check:   wantErr(handshake.ErrBadHandshakeLine),
		},
		{
			name:    "unknown handshake version",
			env:     []string{"HELPER_LINE=2|v1|" + wantNetwork() + "|x"},
			timeout: time.Hour,
			check:   wantErr(handshake.ErrBadHandshakeLine),
		},
		{
			name:    "no line within the timeout is killed and reported",
			env:     []string{"HELPER_SLEEP=1"},
			timeout: 300 * time.Millisecond,
			check:   wantErr(handshake.ErrHandshakeTimeout),
		},
		{
			name:       "context ends during the handshake",
			env:        []string{"HELPER_SLEEP=1"},
			timeout:    time.Hour,
			ctxTimeout: 300 * time.Millisecond,
			check:      wantErr(context.DeadlineExceeded),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := config(t, tc.env...)
			cfg.Timeout = tc.timeout
			ctx := context.Background()
			if tc.ctxTimeout > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, tc.ctxTimeout)
				defer cancel()
			}
			process, err := handshake.Launch(ctx, cfg)
			if err == nil {
				_ = process.Kill()
				_ = process.Wait()
				t.Fatal("Launch succeeded")
			}
			tc.check(t, err)
			assertNoChildren(t)
		})
	}
}

// killLeftover kills a process a failed test left running.
func killLeftover(pid int) {
	if process, err := os.FindProcess(pid); err == nil {
		_ = process.Kill()
	}
}

func wantErr(target error) func(t *testing.T, err error) {
	return func(t *testing.T, err error) {
		t.Helper()
		if !errors.Is(err, target) {
			t.Errorf("err %v, want %v", err, target)
		}
	}
}

func TestLaunchRejectsInvalidConfig(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*handshake.LaunchConfig)
		want   string
	}{
		{"no path", func(c *handshake.LaunchConfig) { c.Path = "" }, "Path"},
		{"no protocols", func(c *handshake.LaunchConfig) { c.Protocols = nil }, "Protocols"},
		{"empty protocol", func(c *handshake.LaunchConfig) { c.Protocols = []string{""} }, "protocol"},
		{"protocol with a comma", func(c *handshake.LaunchConfig) { c.Protocols = []string{"v1,v2"} }, "protocol"},
		{"protocol with a bar", func(c *handshake.LaunchConfig) { c.Protocols = []string{"v1|x"} }, "protocol"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := config(t)
			tc.mutate(&cfg)
			process, err := handshake.Launch(context.Background(), cfg)
			if err == nil {
				_ = process.Kill()
				_ = process.Wait()
				t.Fatal("Launch accepted an invalid config")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("message %q does not name %s", err, tc.want)
			}
			assertNoChildren(t)
		})
	}
}

func TestOutputAfterTheHandshakeReachesTheWriters(t *testing.T) {
	var stdout, stderr syncBuffer
	cfg := config(t, "HELPER_AFTER_STDOUT=after stdout", "HELPER_AFTER_STDERR=after stderr")
	cfg.Stdout = &stdout
	cfg.Stderr = &stderr
	process := launch(t, cfg)
	// The helper prints both lines before it serves, so a served call orders them first.
	checkHealth(t, process)
	_ = process.Kill()
	_ = process.Wait()
	if got := stdout.String(); got != "after stdout\n" {
		t.Errorf("stdout %q, want only the line after the handshake", got)
	}
	if got := stderr.String(); !strings.Contains(got, "after stderr\n") {
		t.Errorf("stderr %q lacks the line after the handshake", got)
	}
}

func TestLaunchPassesOnlyTheGivenEnvironment(t *testing.T) {
	t.Setenv("HANDSHAKE_TEST_PARENT_ONLY", "1")
	var stdout syncBuffer
	cfg := config(t, "HELPER_ECHO_ENV=1", "EXTRA=1")
	cfg.Stdout = &stdout
	process := launch(t, cfg)
	checkHealth(t, process)
	_ = process.Kill()
	_ = process.Wait()

	var keys []string
	for line := range strings.Lines(stdout.String()) {
		if key, ok := strings.CutPrefix(strings.TrimRight(line, "\r\n"), "env="); ok {
			keys = append(keys, key)
		}
	}
	for _, absent := range []string{"HANDSHAKE_TEST_PARENT_ONLY", "PATH", "HOME"} {
		if slices.Contains(keys, absent) {
			t.Errorf("the plugin received %s from the host's environment", absent)
		}
	}
	want := []string{handshake.CookieKey, handshake.ProtocolsKey, "HELPER_ECHO_ENV", "EXTRA"}
	if runtime.GOOS != "windows" {
		want = append(want, handshake.SocketDirKey)
	}
	for _, key := range want {
		if !slices.Contains(keys, key) {
			t.Errorf("the plugin did not receive %s; got %q", key, keys)
		}
	}
	if runtime.GOOS == "linux" {
		slices.Sort(keys)
		slices.Sort(want)
		if !slices.Equal(keys, want) {
			t.Errorf("environment keys %q, want exactly %q", keys, want)
		}
	}
}

func TestKillEndsThePluginAndItsChildren(t *testing.T) {
	reader, writer := io.Pipe()
	cfg := config(t, "HELPER_CHILD=1")
	cfg.Stdout = writer
	process := launch(t, cfg)
	line := readLine(t, bufio.NewReader(reader), 10*time.Second)
	_ = reader.Close()
	childPid, err := strconv.Atoi(strings.TrimPrefix(line, "child="))
	if err != nil {
		t.Fatalf("helper printed %q, want child=<pid>", line)
	}
	pluginExited := watchExit(t, process.Pid)
	childExited := watchExit(t, childPid)

	if err := process.Kill(); err != nil {
		t.Fatalf("Kill: %v", err)
	}
	first := process.Wait()
	if first == nil {
		t.Error("Wait after Kill returned nil, want the kill status")
	}
	if second := process.Wait(); second != first {
		t.Errorf("second Wait returned %v, want the first result %v", second, first)
	}
	if !pluginExited(10 * time.Second) {
		killLeftover(process.Pid)
		t.Error("the plugin survived Kill")
	}
	if !childExited(10 * time.Second) {
		killLeftover(childPid)
		t.Error("the plugin's child survived Kill")
	}
	if err := process.Kill(); !errors.Is(err, os.ErrProcessDone) {
		t.Errorf("Kill after Wait: %v, want os.ErrProcessDone", err)
	}
	if err := process.Signal(os.Kill); !errors.Is(err, os.ErrProcessDone) {
		t.Errorf("Signal after Wait: %v, want os.ErrProcessDone", err)
	}
}

// callPlugin makes one health call and ignores its outcome: with HELPER_EXIT_ON_CALL it makes
// the helper exit by itself.
func callPlugin(t *testing.T, process *handshake.Process) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := process.Dial(ctx)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = conn.Close() }()
	_, _ = healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
}

func TestPluginExitingByItselfTakesItsChildrenAlong(t *testing.T) {
	reader, writer := io.Pipe()
	cfg := config(t, "HELPER_CHILD=1", "HELPER_EXIT_ON_CALL=1")
	cfg.Stdout = writer
	process := launch(t, cfg)
	line := readLine(t, bufio.NewReader(reader), 10*time.Second)
	_ = reader.Close()
	childPid, err := strconv.Atoi(strings.TrimPrefix(line, "child="))
	if err != nil {
		t.Fatalf("helper printed %q, want child=<pid>", line)
	}
	childExited := watchExit(t, childPid)

	callPlugin(t, process)
	if err := process.Wait(); err != nil {
		t.Errorf("Wait: %v, want the plugin's clean exit", err)
	}
	if !childExited(10 * time.Second) {
		killLeftover(childPid)
		t.Error("the child of a plugin that exited by itself survived it")
	}
}

func TestDialReturnsWhenThePluginExits(t *testing.T) {
	cases := []struct {
		name       string
		exitBefore bool
	}{
		{"exits while connecting", false},
		{"exited before the call", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			process := launch(t, config(t, "HELPER_NO_SERVE=1"))
			if tc.exitBefore {
				_ = process.Kill()
				_ = process.Wait()
			}
			type result struct {
				err error
			}
			done := make(chan result, 1)
			go func() {
				// No deadline: only the plugin's exit can end this Dial.
				conn, err := process.Dial(context.Background())
				if conn != nil {
					_ = conn.Close()
				}
				done <- result{err}
			}()
			if !tc.exitBefore {
				_ = process.Kill()
			}
			select {
			case got := <-done:
				if !errors.Is(got.err, handshake.ErrExited) {
					t.Errorf("Dial: %v, want ErrExited", got.err)
				}
				var exitErr *exec.ExitError
				if !errors.As(got.err, &exitErr) {
					t.Errorf("Dial: %v, want it to wrap the exit status", got.err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("Dial did not return after the plugin exited")
			}
		})
	}
}

func TestWaitReturnsAfterTheLastOutputWrite(t *testing.T) {
	// Deliberately not synchronised: under -race a copier write after Wait is reported.
	var stdout bytes.Buffer
	cfg := config(t, "HELPER_HOLD_OUTPUT=1", "HELPER_EXIT_ON_CALL=1")
	cfg.Stdout = &stdout
	process := launch(t, cfg)
	callPlugin(t, process)
	_ = process.Wait()
	// The flooding child may write before the helper prints its line, so the line is looked for
	// anywhere in the output.
	if !strings.Contains(stdout.String(), "child=") {
		t.Errorf("stdout %.40q... lacks the helper's child line", stdout.String())
	}
}

// TestPluginDiesWithItsHost kills an intermediate host process and expects its plugin to go
// with it: parent-death signal on Linux, job object on Windows.
func TestPluginDiesWithItsHost(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOOS != "windows" {
		t.Skip("no parent-death guarantee on " + runtime.GOOS)
	}
	host := exec.Command(os.Args[0], "-test.run=^TestHelperHost$", "-test.count=1")
	host.Env = append(os.Environ(),
		hostModeKey+"=1",
		helperPathKey+"="+helperPath,
		hostSocketDirKey+"="+socketDir(t),
	)
	var hostStderr syncBuffer
	host.Stderr = &hostStderr
	stdin, err := host.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stdin.Close() }()
	stdout, err := host.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := host.Start(); err != nil {
		t.Fatal(err)
	}
	line := readLine(t, bufio.NewReader(stdout), 30*time.Second)
	pid, err := strconv.Atoi(strings.TrimPrefix(line, "helper-pid="))
	if err != nil {
		_ = host.Process.Kill()
		_ = host.Wait()
		t.Fatalf("intermediate host printed %q (stderr %q)", line, hostStderr.String())
	}
	pluginExited := watchExit(t, pid)

	if err := host.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = host.Wait()
	if !pluginExited(10 * time.Second) {
		killLeftover(pid)
		t.Fatal("the plugin outlived its killed host")
	}
}

// TestHelperHost is the intermediate host of TestPluginDiesWithItsHost; it does nothing in a
// normal run.
func TestHelperHost(t *testing.T) {
	if os.Getenv(hostModeKey) != "1" {
		t.Skip("subprocess of TestPluginDiesWithItsHost")
	}
	process, err := handshake.Launch(context.Background(), handshake.LaunchConfig{
		Path:      helperPath,
		Dir:       filepath.Dir(helperPath),
		SocketDir: os.Getenv(hostSocketDirKey),
		Protocols: []string{"v1"},
		Env:       helperEnv(),
	})
	if err != nil {
		t.Fatalf("Launch: %v", err)
	}
	fmt.Printf("helper-pid=%d\n", process.Pid)
	// Stay alive until the parent test kills this process.
	_, _ = io.Copy(io.Discard, os.Stdin)
}
