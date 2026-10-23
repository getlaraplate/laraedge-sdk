package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/getlaraplate/laraedge-sdk/handshake"
	"github.com/getlaraplate/laraedge-sdk/manifest"
	pb "github.com/getlaraplate/laraedge-sdk/pluginv1"
)

// serveHooks replaces the package-level process hooks for the duration of a test. Tests that use it
// must not run in parallel.
type serveHooks struct {
	exitCodes chan int
	stdout    *syncBuffer
	stderr    *syncBuffer
	lis       *bufconn.Listener
}

func installHooks(t *testing.T, listenErr error) *serveHooks {
	t.Helper()
	h := &serveHooks{exitCodes: make(chan int, 4), stdout: &syncBuffer{}, stderr: &syncBuffer{}, lis: bufconn.Listen(1 << 20)}
	oldExit, oldListen, oldStdout, oldStderr, oldArgs := exit, listen, stdout, stderr, programArgs
	exit = func(code int) { h.exitCodes <- code }
	listen = func([]string) (net.Listener, string, error) {
		if listenErr != nil {
			return nil, "", listenErr
		}
		return h.lis, "v1", nil
	}
	stdout, stderr = h.stdout, h.stderr
	t.Cleanup(func() {
		exit, listen, stdout, stderr, programArgs = oldExit, oldListen, oldStdout, oldStderr, oldArgs
		_ = h.lis.Close()
	})
	return h
}

func (h *serveHooks) waitExit(t *testing.T) int {
	t.Helper()
	select {
	case code := <-h.exitCodes:
		return code
	case <-testCtx(t).Done():
		t.Fatal("the process did not exit")
		return -1
	}
}

func fullPlugin() Plugin {
	return Plugin{
		Name: "fake", Version: "1.2.3",
		Input: newFakeInput(), Processor: &fakeProcessor{}, Output: &fakeOutput{},
		Manifest: manifest.Kinds{Input: inputKind(), Processor: configKind(), Output: configKind()},
	}
}

func TestManifestFlagPrintsTheCanonicalManifestWithoutTheCookie(t *testing.T) {
	h := installHooks(t, errors.New("listen must not be called by --manifest"))
	programArgs = func() []string { return []string{"--manifest"} }
	p := fullPlugin()
	Serve(p)
	if code := h.waitExit(t); code != 0 {
		t.Fatalf("exit %d, stderr %q", code, h.stderr.String())
	}
	out := h.stdout.String()
	if !strings.HasSuffix(out, "\n") {
		t.Errorf("output %q does not end with a newline", out)
	}
	var m manifest.Manifest
	if err := json.Unmarshal([]byte(out), &m); err != nil {
		t.Fatalf("output is not a manifest: %v", err)
	}
	if m.Name != "fake" || m.Version != "1.2.3" || len(m.ProtocolVersions) != 1 || m.ProtocolVersions[0] != "v1" ||
		m.Kinds.Input == nil || m.Kinds.Processor == nil || m.Kinds.Output == nil {
		t.Errorf("manifest = %+v", m)
	}
	canonical, err := manifest.Canonical([]byte(out))
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSuffix(out, "\n") != string(canonical) {
		t.Errorf("output is not canonical:\n%s\nwant\n%s", out, canonical)
	}
	if problems := m.Check(); len(problems) != 0 {
		t.Errorf("manifest has problems: %v", problems)
	}
}

func TestInfoReturnsTheManifestOfTheFlag(t *testing.T) {
	h := installHooks(t, nil)
	programArgs = func() []string { return []string{"--manifest"} }
	Serve(fullPlugin())
	h.waitExit(t)
	flagOutput := strings.TrimSuffix(h.stdout.String(), "\n")

	served := start(t, fullPlugin())
	info, err := pb.NewPluginClient(served.conn).Info(testCtx(t), &pb.InfoRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if string(info.GetManifest()) != flagOutput {
		t.Errorf("Info.manifest differs from --manifest:\n%s\n%s", info.GetManifest(), flagOutput)
	}
	if info.GetName() != "fake" || info.GetVersion() != "1.2.3" || len(info.GetProtocolVersions()) != 1 || info.GetProtocolVersions()[0] != "v1" {
		t.Errorf("info = %v", info)
	}
}

func TestBrokenPluginsFailLoudly(t *testing.T) {
	inputOnly := func() Plugin {
		return Plugin{Name: "fake", Version: "1", Input: newFakeInput(), Manifest: manifest.Kinds{Input: inputKind()}}
	}
	cases := []struct {
		name   string
		mutate func(p *Plugin)
		args   []string
		want   string
	}{
		{"bad name", func(p *Plugin) { p.Name = "Bad Name" }, []string{"--manifest"}, "name"},
		{"no version", func(p *Plugin) { p.Version = "" }, []string{"--manifest"}, "version"},
		{"input set but not in the manifest", func(p *Plugin) { p.Manifest.Input = nil; p.Manifest.Output = configKind() }, []string{"--manifest"}, "Input"},
		{"manifest offers a processor that is not implemented", func(p *Plugin) { p.Manifest.Processor = configKind() }, []string{"--manifest"}, "Processor"},
		{"manifest offers browse without a Browser", func(p *Plugin) { p.Manifest.Input.Browse = true }, []string{"--manifest"}, "Browse"},
		{"no kind at all", func(p *Plugin) { *p = Plugin{Name: "fake", Version: "1"} }, []string{"--manifest"}, "kinds"},
		{"broken schema", func(p *Plugin) { p.Manifest.Input.ConnectionSchema = json.RawMessage(`{"type": 5}`) }, []string{"--manifest"}, "connection_schema"},
		{"broken manifest without the flag", func(p *Plugin) { p.Version = "" }, nil, "version"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := installHooks(t, errors.New("not launched"))
			p := inputOnly()
			p.Manifest.Input = inputKind()
			tc.mutate(&p)
			programArgs = func() []string { return tc.args }
			Serve(p)
			if code := h.waitExit(t); code != 1 {
				t.Fatalf("exit %d, want 1", code)
			}
			if !strings.Contains(h.stderr.String(), tc.want) {
				t.Errorf("stderr %q does not mention %q", h.stderr.String(), tc.want)
			}
			if h.stdout.String() != "" {
				t.Errorf("stdout = %q, want nothing", h.stdout.String())
			}
		})
	}
}

func TestUnknownFlagExitsWithUsageError(t *testing.T) {
	h := installHooks(t, nil)
	programArgs = func() []string { return []string{"--nope"} }
	Serve(fullPlugin())
	if code := h.waitExit(t); code != 2 {
		t.Fatalf("exit %d, want 2", code)
	}
	if h.stderr.String() == "" {
		t.Error("no usage message on stderr")
	}
}

func TestServeExitCodesOfTheHandshake(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want int
	}{
		{"started by hand", handshake.ErrNotLaunched, 1},
		{"no common protocol", handshake.ErrNoProtocol, handshake.ExitCodeNoProtocol},
		{"other failure", errors.New("socket directory is not writable"), 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := installHooks(t, tc.err)
			programArgs = func() []string { return nil }
			Serve(fullPlugin())
			if code := h.waitExit(t); code != tc.want {
				t.Fatalf("exit %d, want %d", code, tc.want)
			}
			if !strings.Contains(h.stderr.String(), tc.err.Error()) {
				t.Errorf("stderr %q does not carry %q", h.stderr.String(), tc.err)
			}
		})
	}
}

func TestHealthIsServing(t *testing.T) {
	h := start(t, fullPlugin())
	c := grpc_health_v1.NewHealthClient(h.conn)
	for _, service := range []string{""} {
		res, err := c.Check(testCtx(t), &grpc_health_v1.HealthCheckRequest{Service: service})
		if err != nil {
			t.Fatal(err)
		}
		if res.GetStatus() != grpc_health_v1.HealthCheckResponse_SERVING {
			t.Errorf("status of %q = %v", service, res.GetStatus())
		}
	}
}

func TestOnlyTheKindsThatAreSetAreServed(t *testing.T) {
	h := start(t, outputPlugin(&fakeOutput{}))
	_, err := pb.NewInputClient(h.conn).Configure(testCtx(t), &pb.ConfigureRequest{InstanceId: "a"})
	wantCode(t, err, codes.Unimplemented)
	_, err = pb.NewProcessorClient(h.conn).Configure(testCtx(t), &pb.ConfigureRequest{InstanceId: "a"})
	wantCode(t, err, codes.Unimplemented)
	configureOutput(t, pb.NewOutputClient(h.conn), "a")
}

// servedProcess runs Serve over the hook listener and returns a client connection to it.
func servedProcess(t *testing.T, p Plugin) (*serveHooks, *grpc.ClientConn) {
	t.Helper()
	h := installHooks(t, nil)
	programArgs = func() []string { return nil }
	go Serve(p)
	conn := dialBufconn(t, h.lis)
	t.Cleanup(func() { _ = conn.Close() })
	return h, conn
}

func TestShutdownStopsTheInstancesAndExitsZero(t *testing.T) {
	in := newFakeInput()
	p := inputPlugin(in)
	h, conn := servedProcess(t, p)
	plugin := pb.NewPluginClient(conn)
	input := pb.NewInputClient(conn)
	configureInput(t, input, "never-run")
	configureInput(t, input, "running")
	stream, err := input.Subscribe(testCtx(t), &pb.SubscribeRequest{InstanceId: "running"})
	if err != nil {
		t.Fatal(err)
	}
	in.waitStarted(t, "running")

	if _, err := plugin.Shutdown(testCtx(t), &pb.ShutdownRequest{Deadline: durationpb.New(5 * time.Second)}); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if code := h.waitExit(t); code != 0 {
		t.Fatalf("exit %d, want 0 (stderr %q)", code, h.stderr.String())
	}
	stopped := map[string]int{}
	for len(in.stops) > 0 {
		stopped[<-in.stops]++
	}
	if stopped["never-run"] != 1 || stopped["running"] != 1 || len(stopped) != 2 {
		t.Errorf("Stop calls = %v, want each instance stopped exactly once", stopped)
	}
	if _, err := stream.Recv(); err == nil {
		t.Error("the stream survived the shutdown")
	}
	// The listener is closed: nobody can connect any more.
	dialCtx, cancel := context.WithTimeout(testCtx(t), time.Second)
	defer cancel()
	if c, err := h.lis.DialContext(dialCtx); err == nil {
		_ = c.Close()
		t.Error("the listener still accepts connections after the exit")
	}
}

func TestShutdownIsBoundedByTheDeadline(t *testing.T) {
	release := make(chan struct{})
	entered := make(chan struct{})
	o := &fakeOutput{send: func(context.Context, string, Batch) Outcome {
		close(entered)
		<-release // ignores its context: the plugin is stuck
		return Outcome{Kind: OutcomeAccepted}
	}}
	h, conn := servedProcess(t, outputPlugin(o))
	defer close(release)
	plugin := pb.NewPluginClient(conn)
	out := pb.NewOutputClient(conn)
	configureOutput(t, out, "a")
	sendCtx := testCtx(t)
	go func() { _, _ = out.Send(sendCtx, sendRequest("a", "m")) }()
	<-entered

	started := time.Now()
	if _, err := plugin.Shutdown(testCtx(t), &pb.ShutdownRequest{Deadline: durationpb.New(300 * time.Millisecond)}); err != nil {
		t.Fatal(err)
	}
	if code := h.waitExit(t); code != 0 {
		t.Fatalf("exit %d, want 0", code)
	}
	if elapsed := time.Since(started); elapsed > 10*time.Second {
		t.Errorf("shutdown took %v with a 300ms deadline", elapsed)
	}
}

func TestShutdownTwiceExitsOnce(t *testing.T) {
	h, conn := servedProcess(t, outputPlugin(&fakeOutput{}))
	plugin := pb.NewPluginClient(conn)
	if _, err := plugin.Shutdown(testCtx(t), &pb.ShutdownRequest{}); err != nil {
		t.Fatal(err)
	}
	_, _ = plugin.Shutdown(testCtx(t), &pb.ShutdownRequest{}, grpc.WaitForReady(false)) // may find the server already gone
	if code := h.waitExit(t); code != 0 {
		t.Fatalf("exit %d", code)
	}
	select {
	case code := <-h.exitCodes:
		t.Errorf("second exit with %d", code)
	default:
	}
}

func TestHealthTurnsNotServingWhenShutdownBegins(t *testing.T) {
	h := start(t, outputPlugin(&fakeOutput{}))
	srv := h.srv
	srv.beginShutdown(time.Second)
	<-srv.finished
	res, err := srv.health.Check(testCtx(t), &grpc_health_v1.HealthCheckRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if res.GetStatus() != grpc_health_v1.HealthCheckResponse_NOT_SERVING {
		t.Errorf("status = %v, want NOT_SERVING", res.GetStatus())
	}
}

func TestShutdownStopsAnInstanceOnlyAfterItsRunReturned(t *testing.T) {
	in := newFakeInput()
	stoppedWhileRunning := make(chan bool, 1)
	in.run = func(ctx context.Context, _ string, _ InputEmitter) error {
		<-ctx.Done()
		select {
		case <-in.stops:
			stoppedWhileRunning <- true
		default:
			stoppedWhileRunning <- false
		}
		return ctx.Err()
	}
	h, conn := servedProcess(t, inputPlugin(in))
	input := pb.NewInputClient(conn)
	configureInput(t, input, "a")
	if _, err := input.Subscribe(testCtx(t), &pb.SubscribeRequest{InstanceId: "a"}); err != nil {
		t.Fatal(err)
	}
	in.waitStarted(t, "a")

	if _, err := pb.NewPluginClient(conn).Shutdown(testCtx(t), &pb.ShutdownRequest{Deadline: durationpb.New(5 * time.Second)}); err != nil {
		t.Fatal(err)
	}
	if code := h.waitExit(t); code != 0 {
		t.Fatalf("exit %d", code)
	}
	if <-stoppedWhileRunning {
		t.Error("Stop was called while Run was still running")
	}
	if len(in.stops) != 1 {
		t.Errorf("Stop was called %d times, want 1 (after Run returned)", len(in.stops))
	}
}

// TestShutdownRefusesSubscribeAndConfigureOnceItsStopPassBegan reproduces calls that grpc-go still
// dispatches after Shutdown began (they reached it before the client processed GOAWAY): on an idle
// configured instance whose Stop is running, a Subscribe must not start Run (which would call Stop
// a second time) and a Configure must not run the author's Configure, for that instance or for one
// created after the Stop pass took its snapshot.
func TestShutdownRefusesSubscribeAndConfigureOnceItsStopPassBegan(t *testing.T) {
	in := newFakeInput()
	var stopCalls, configures atomic.Int32
	stopEntered := make(chan struct{})
	releaseStop := make(chan struct{})
	in.stop = func(context.Context, string) {
		if stopCalls.Add(1) == 1 {
			close(stopEntered)
			<-releaseStop
		}
	}
	in.configure = func(context.Context, string, json.RawMessage) []ConfigError {
		configures.Add(1)
		return nil
	}
	h := start(t, inputPlugin(in))
	configureInput(t, pb.NewInputClient(h.conn), "a")

	h.srv.beginShutdown(5 * time.Second)
	select {
	case <-stopEntered:
	case <-testCtx(t).Done():
		t.Fatal("Stop was never called")
	}
	is := h.srv.input
	err := is.Subscribe(&pb.SubscribeRequest{InstanceId: "a"}, &fakeSubscribeStream{ctx: testCtx(t)})
	wantCode(t, err, codes.Unavailable)
	for _, id := range []string{"a", "created-after-the-snapshot"} {
		_, err := is.Configure(testCtx(t), &pb.ConfigureRequest{InstanceId: id, Config: []byte(`{}`)})
		wantCode(t, err, codes.Unavailable)
	}
	close(releaseStop)
	<-h.srv.finished

	select {
	case got := <-in.started:
		t.Errorf("Run of %s started during the shutdown", got)
	default:
	}
	if n := configures.Load(); n != 1 {
		t.Errorf("the author's Configure ran %d times, want 1 (before the shutdown)", n)
	}
	if n := stopCalls.Load(); n != 1 {
		t.Errorf("Stop was called %d times, want 1", n)
	}
}

// TestShutdownStopsNoInstanceWhileItsConfigureRuns begins Shutdown while the author's Configure
// of an instance is running: Stop of that instance must wait for it (here the deadline passes
// first, so Stop is skipped).
func TestShutdownStopsNoInstanceWhileItsConfigureRuns(t *testing.T) {
	in := newFakeInput()
	stopEntered := make(chan struct{}, 1)
	in.stop = func(context.Context, string) {
		select {
		case stopEntered <- struct{}{}:
		default:
		}
	}
	h := start(t, inputPlugin(in))
	finished := h.srv.finished
	configuring := make(chan struct{})
	overlapped := make(chan bool, 1)
	in.configure = func(context.Context, string, json.RawMessage) []ConfigError {
		close(configuring)
		select {
		case <-stopEntered:
			overlapped <- true
		case <-finished:
			overlapped <- false
		}
		return nil
	}
	go func() {
		_, _ = pb.NewInputClient(h.conn).Configure(testCtx(t), &pb.ConfigureRequest{InstanceId: "a", Config: []byte(`{}`)})
	}()
	<-configuring
	h.srv.beginShutdown(300 * time.Millisecond)
	if <-overlapped {
		t.Error("Stop ran while Configure of the same instance was running")
	}
}

func TestCallsAreRefusedOnceShutdownBegan(t *testing.T) {
	h := start(t, fullPlugin())
	h.srv.beginShutdown(time.Second)
	<-h.srv.finished
	unary := []struct {
		method string
		served bool
	}{
		{pb.Plugin_Info_FullMethodName, true},
		{pb.Plugin_Shutdown_FullMethodName, true},
		{grpc_health_v1.Health_Check_FullMethodName, true},
		{pb.Input_Configure_FullMethodName, false},
		{pb.Input_Probe_FullMethodName, false},
		{pb.Input_Browse_FullMethodName, false},
		{pb.Processor_Configure_FullMethodName, false},
		{pb.Output_Configure_FullMethodName, false},
		{pb.Output_Send_FullMethodName, false},
	}
	for _, tc := range unary {
		called := false
		_, err := h.srv.refuseUnaryWhenShuttingDown(testCtx(t), nil, &grpc.UnaryServerInfo{FullMethod: tc.method},
			func(context.Context, any) (any, error) { called = true; return nil, nil })
		if called != tc.served {
			t.Errorf("%s: handler called = %v, want %v", tc.method, called, tc.served)
		}
		if !tc.served {
			wantCode(t, err, codes.Unavailable)
		}
	}
	streams := []struct {
		method string
		served bool
	}{
		{grpc_health_v1.Health_Watch_FullMethodName, true},
		{pb.Input_Subscribe_FullMethodName, false},
		{pb.Processor_Process_FullMethodName, false},
	}
	for _, tc := range streams {
		called := false
		err := h.srv.refuseStreamWhenShuttingDown(nil, nil, &grpc.StreamServerInfo{FullMethod: tc.method},
			func(any, grpc.ServerStream) error { called = true; return nil })
		if called != tc.served {
			t.Errorf("%s: handler called = %v, want %v", tc.method, called, tc.served)
		}
		if !tc.served {
			wantCode(t, err, codes.Unavailable)
		}
	}
}

func TestStopContextIsBounded(t *testing.T) {
	type stopCall struct {
		instance string
		left     time.Duration
		ok       bool
	}
	in := newFakeInput()
	calls := make(chan stopCall, 8)
	in.stop = func(ctx context.Context, instance string) {
		deadline, ok := ctx.Deadline()
		calls <- stopCall{instance, time.Until(deadline), ok}
	}
	h, conn := servedProcess(t, inputPlugin(in))
	input := pb.NewInputClient(conn)
	configureInput(t, input, "ended")
	configureInput(t, input, "idle")
	configureInput(t, input, "running")

	streamCtx, cancel := context.WithCancel(testCtx(t))
	stream, err := input.Subscribe(streamCtx, &pb.SubscribeRequest{InstanceId: "ended"})
	if err != nil {
		t.Fatal(err)
	}
	in.waitStarted(t, "ended")
	cancel()
	_, _ = stream.Recv()
	got := <-calls
	if got.instance != "ended" || !got.ok || got.left <= stopTimeout/2 || got.left > stopTimeout {
		t.Errorf("Stop after the stream ended: %+v, want a deadline of %v", got, stopTimeout)
	}

	if _, err := input.Subscribe(testCtx(t), &pb.SubscribeRequest{InstanceId: "running"}); err != nil {
		t.Fatal(err)
	}
	in.waitStarted(t, "running")
	const deadline = 3 * time.Second
	if _, err := pb.NewPluginClient(conn).Shutdown(testCtx(t), &pb.ShutdownRequest{Deadline: durationpb.New(deadline)}); err != nil {
		t.Fatal(err)
	}
	if code := h.waitExit(t); code != 0 {
		t.Fatalf("exit %d", code)
	}
	for range 2 {
		got := <-calls
		if !got.ok || got.left > deadline {
			t.Errorf("Stop of %s at Shutdown: %+v, want the Shutdown deadline (%v)", got.instance, got, deadline)
		}
	}
}
