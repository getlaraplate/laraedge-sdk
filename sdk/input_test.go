package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/getlaraplate/laraedge-sdk/manifest"
	pb "github.com/getlaraplate/laraedge-sdk/pluginv1"
)

func inputPlugin(in Input) Plugin {
	return Plugin{Name: "fake", Version: "1.0.0", Input: in, Manifest: manifest.Kinds{Input: inputKind()}}
}

func configureInput(t *testing.T, c pb.InputClient, instance string) {
	t.Helper()
	resp, err := c.Configure(testCtx(t), &pb.ConfigureRequest{InstanceId: instance, Config: []byte(`{}`)})
	if err != nil {
		t.Fatalf("Configure(%s): %v", instance, err)
	}
	if len(resp.GetErrors()) != 0 {
		t.Fatalf("Configure(%s) refused: %v", instance, resp.GetErrors())
	}
}

func wantCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	if got := status.Code(err); got != want {
		t.Fatalf("code %v (err %v), want %v", got, err, want)
	}
}

func TestInputConfigureErrorsPassThrough(t *testing.T) {
	in := newFakeInput()
	in.configure = func(_ context.Context, instance string, cfg json.RawMessage) []ConfigError {
		if string(cfg) != `{"bad":true}` {
			return nil
		}
		return []ConfigError{{Path: "devices[0].points[1].address", Message: "must be counter:<n>"}, {Path: "connection", Message: "is required"}}
	}
	c := pb.NewInputClient(start(t, inputPlugin(in)).conn)

	resp, err := c.Configure(testCtx(t), &pb.ConfigureRequest{InstanceId: "a", Config: []byte(`{"bad":true}`)})
	if err != nil {
		t.Fatal(err)
	}
	got := resp.GetErrors()
	if len(got) != 2 || got[0].GetPath() != "devices[0].points[1].address" || got[0].GetMessage() != "must be counter:<n>" || got[1].GetPath() != "connection" {
		t.Fatalf("errors = %v", got)
	}

	resp, err = c.Configure(testCtx(t), &pb.ConfigureRequest{InstanceId: "a", Config: []byte(`{}`)})
	if err != nil || len(resp.GetErrors()) != 0 {
		t.Fatalf("valid config: %v %v", resp, err)
	}
}

func TestInputRefusesWhatItCannotServe(t *testing.T) {
	cases := []struct {
		name string
		call func(c pb.InputClient) error
		want codes.Code
	}{
		{"configure without instance", func(c pb.InputClient) error {
			_, err := c.Configure(context.Background(), &pb.ConfigureRequest{})
			return err
		}, codes.InvalidArgument},
		{"probe before configure", func(c pb.InputClient) error {
			_, err := c.Probe(context.Background(), &pb.ProbeRequest{InstanceId: "a"})
			return err
		}, codes.FailedPrecondition},
		{"subscribe before configure", func(c pb.InputClient) error {
			s, err := c.Subscribe(context.Background(), &pb.SubscribeRequest{InstanceId: "a"})
			if err != nil {
				return err
			}
			_, err = s.Recv()
			return err
		}, codes.FailedPrecondition},
		{"subscribe without instance", func(c pb.InputClient) error {
			s, err := c.Subscribe(context.Background(), &pb.SubscribeRequest{})
			if err != nil {
				return err
			}
			_, err = s.Recv()
			return err
		}, codes.InvalidArgument},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := pb.NewInputClient(start(t, inputPlugin(newFakeInput())).conn)
			wantCode(t, tc.call(c), tc.want)
		})
	}
}

func TestInputRefusedConfigureLeavesInstanceUnconfigured(t *testing.T) {
	in := newFakeInput()
	in.configure = func(context.Context, string, json.RawMessage) []ConfigError {
		return []ConfigError{{Path: "x", Message: "no"}}
	}
	c := pb.NewInputClient(start(t, inputPlugin(in)).conn)
	if _, err := c.Configure(testCtx(t), &pb.ConfigureRequest{InstanceId: "a"}); err != nil {
		t.Fatal(err)
	}
	_, err := c.Probe(testCtx(t), &pb.ProbeRequest{InstanceId: "a"})
	wantCode(t, err, codes.FailedPrecondition)
}

func TestInputSubscribeStreamsEventsAndEndsOnCancel(t *testing.T) {
	in := newFakeInput()
	runEnded := make(chan error, 1)
	in.run = func(ctx context.Context, instance string, out InputEmitter) error {
		steps := []func() error{
			func() error { return out.Sample(testSample(instance)) },
			func() error { return out.Answered("press-1") },
			func() error { return out.Health(true, "") },
			func() error { return out.Health(false, "timeout") },
		}
		for _, step := range steps {
			if err := step(); err != nil {
				runEnded <- err
				return err
			}
		}
		<-ctx.Done()
		runEnded <- ctx.Err()
		return ctx.Err()
	}
	c := pb.NewInputClient(start(t, inputPlugin(in)).conn)
	configureInput(t, c, "a")

	ctx, cancel := context.WithCancel(testCtx(t))
	defer cancel()
	stream, err := c.Subscribe(ctx, &pb.SubscribeRequest{InstanceId: "a"})
	if err != nil {
		t.Fatal(err)
	}
	var events []*pb.InputEvent
	for range 4 {
		ev, err := stream.Recv()
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
		events = append(events, ev)
	}
	if s := events[0].GetSample(); s.GetDevice() != "a" || s.GetSignal() != "temperature" || s.GetValue().GetIntValue() != 7 ||
		s.GetQuality() != pb.Quality_QUALITY_GOOD || s.GetPriority() != pb.Priority_PRIORITY_CRITICAL ||
		!s.GetTs().AsTime().Equal(time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)) {
		t.Errorf("sample event = %v", events[0])
	}
	if events[1].GetAnswered().GetDevice() != "press-1" {
		t.Errorf("answered event = %v", events[1])
	}
	if h := events[2].GetHealth(); !h.GetConnected() || h.GetReadError() != "" {
		t.Errorf("health event 1 = %v", events[2])
	}
	if h := events[3].GetHealth(); h.GetConnected() || h.GetReadError() != "timeout" {
		t.Errorf("health event 2 = %v", events[3])
	}

	cancel()
	if err := <-runEnded; !errors.Is(err, context.Canceled) {
		t.Errorf("Run ended with %v, want context.Canceled", err)
	}
	if got := <-in.stops; got != "a" {
		t.Errorf("Stop(%q), want a", got)
	}
}

func TestInputEmitterRefusesBadEvents(t *testing.T) {
	bad := testSample("a")
	bad.Quality = 0

	in := newFakeInput()
	results := make(chan []error, 1)
	in.run = func(ctx context.Context, _ string, out InputEmitter) error {
		noDevice := testSample("")
		badFloat := testSample("a")
		badFloat.Value = Value{Kind: ValueKindFloat, Float: math.NaN()}
		results <- []error{
			out.Sample(bad),
			out.Sample(noDevice),
			out.Sample(badFloat),
			out.Answered(""),
			out.Health(true, "weird"),
			out.Health(false, "decode"),
		}
		<-ctx.Done()
		return nil
	}
	c := pb.NewInputClient(start(t, inputPlugin(in)).conn)
	configureInput(t, c, "a")
	ctx, cancel := context.WithCancel(testCtx(t))
	defer cancel()
	stream, err := c.Subscribe(ctx, &pb.SubscribeRequest{InstanceId: "a"})
	if err != nil {
		t.Fatal(err)
	}
	// Only the valid health event may reach the client.
	ev, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if ev.GetHealth().GetReadError() != "decode" {
		t.Fatalf("first event = %v, want the decode health event", ev)
	}
	got := <-results
	for i, err := range got[:5] {
		if err == nil {
			t.Errorf("emit %d accepted, want an error", i)
		}
	}
	if got[5] != nil {
		t.Errorf("valid health refused: %v", got[5])
	}
}

func TestInputEmitterFailsAfterTheStreamEnded(t *testing.T) {
	in := newFakeInput()
	late := make(chan error, 1)
	in.run = func(ctx context.Context, instance string, out InputEmitter) error {
		<-ctx.Done()
		late <- out.Sample(testSample(instance))
		return ctx.Err()
	}
	c := pb.NewInputClient(start(t, inputPlugin(in)).conn)
	configureInput(t, c, "a")
	ctx, cancel := context.WithCancel(testCtx(t))
	if _, err := c.Subscribe(ctx, &pb.SubscribeRequest{InstanceId: "a"}); err != nil {
		t.Fatal(err)
	}
	in.waitStarted(t, "a")
	cancel()
	if err := <-late; err == nil {
		t.Fatal("emit after the stream ended succeeded")
	}
}

func TestInputSecondSubscribeIsRefused(t *testing.T) {
	in := newFakeInput()
	c := pb.NewInputClient(start(t, inputPlugin(in)).conn)
	configureInput(t, c, "a")
	ctx, cancel := context.WithCancel(testCtx(t))
	defer cancel()
	if _, err := c.Subscribe(ctx, &pb.SubscribeRequest{InstanceId: "a"}); err != nil {
		t.Fatal(err)
	}
	in.waitStarted(t, "a")
	second, err := c.Subscribe(ctx, &pb.SubscribeRequest{InstanceId: "a"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = second.Recv()
	wantCode(t, err, codes.AlreadyExists)
}

func TestInputConfigureStopsTheRunningStream(t *testing.T) {
	in := newFakeInput()
	runEnded := make(chan struct{}, 2)
	in.run = func(ctx context.Context, _ string, _ InputEmitter) error {
		<-ctx.Done()
		runEnded <- struct{}{}
		return ctx.Err()
	}
	c := pb.NewInputClient(start(t, inputPlugin(in)).conn)
	configureInput(t, c, "a")
	stream, err := c.Subscribe(testCtx(t), &pb.SubscribeRequest{InstanceId: "a"})
	if err != nil {
		t.Fatal(err)
	}
	in.waitStarted(t, "a")

	configureInput(t, c, "a") // reconfigure: the running Run must be stopped first
	select {
	case <-runEnded:
	default:
		t.Fatal("Configure returned while Run was still running")
	}
	if got := <-in.stops; got != "a" {
		t.Errorf("Stop(%q), want a", got)
	}
	if _, err := stream.Recv(); err == nil {
		t.Error("the stream is still open after reconfiguration")
	}

	// The reconfigured instance can be subscribed again.
	if _, err := c.Subscribe(testCtx(t), &pb.SubscribeRequest{InstanceId: "a"}); err != nil {
		t.Fatal(err)
	}
	in.waitStarted(t, "a")
}

func TestInputInstancesAreIndependent(t *testing.T) {
	in := newFakeInput()
	release := make(chan struct{})
	in.run = func(ctx context.Context, instance string, out InputEmitter) error {
		select {
		case <-release: // both instances are running at the same time
		case <-ctx.Done():
			return ctx.Err()
		}
		if err := out.Sample(testSample(instance)); err != nil {
			return err
		}
		<-ctx.Done()
		return ctx.Err()
	}
	c := pb.NewInputClient(start(t, inputPlugin(in)).conn)
	ctx, cancel := context.WithCancel(testCtx(t))
	defer cancel()
	streams := map[string]pb.Input_SubscribeClient{}
	for _, id := range []string{"a", "b"} {
		configureInput(t, c, id)
		s, err := c.Subscribe(ctx, &pb.SubscribeRequest{InstanceId: id})
		if err != nil {
			t.Fatal(err)
		}
		streams[id] = s
	}
	in.waitStarted(t, "a")
	in.waitStarted(t, "b")
	close(release)
	for id, s := range streams {
		ev, err := s.Recv()
		if err != nil {
			t.Fatal(err)
		}
		if got := ev.GetSample().GetDevice(); got != id {
			t.Errorf("stream %s received a sample of %s", id, got)
		}
	}
}

func TestInputProbeHonoursTheClientDeadline(t *testing.T) {
	in := newFakeInput()
	sawDeadline := make(chan bool, 1)
	in.probe = func(ctx context.Context, _ string) ([]Sample, error) {
		_, ok := ctx.Deadline()
		sawDeadline <- ok
		<-ctx.Done()
		return nil, ctx.Err()
	}
	c := pb.NewInputClient(start(t, inputPlugin(in)).conn)
	configureInput(t, c, "a")
	ctx, cancel := context.WithTimeout(testCtx(t), 100*time.Millisecond)
	defer cancel()
	_, err := c.Probe(ctx, &pb.ProbeRequest{InstanceId: "a"})
	wantCode(t, err, codes.DeadlineExceeded)
	if !<-sawDeadline {
		t.Error("the plugin's Probe context carries no deadline")
	}
}

func TestInputProbeReturnsOneSamplePerPoint(t *testing.T) {
	in := newFakeInput()
	in.probe = func(_ context.Context, instance string) ([]Sample, error) {
		return []Sample{testSample(instance), testSample(instance)}, nil
	}
	c := pb.NewInputClient(start(t, inputPlugin(in)).conn)
	configureInput(t, c, "a")
	resp, err := c.Probe(testCtx(t), &pb.ProbeRequest{InstanceId: "a"})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.GetSamples()) != 2 || resp.GetSamples()[0].GetDevice() != "a" {
		t.Fatalf("samples = %v", resp.GetSamples())
	}
}

func TestInputProbeErrors(t *testing.T) {
	bad := testSample("a")
	bad.Priority = 0
	cases := []struct {
		name string
		fn   func(context.Context, string) ([]Sample, error)
		want codes.Code
	}{
		{"author error", func(context.Context, string) ([]Sample, error) { return nil, errors.New("device unreachable") }, codes.Unknown},
		{"status passes through", func(context.Context, string) ([]Sample, error) {
			return nil, status.Error(codes.Unavailable, "down")
		}, codes.Unavailable},
		{"invalid sample", func(context.Context, string) ([]Sample, error) { return []Sample{bad}, nil }, codes.Internal},
		{"panic", func(context.Context, string) ([]Sample, error) { panic("boom") }, codes.Internal},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := newFakeInput()
			in.probe = tc.fn
			c := pb.NewInputClient(start(t, inputPlugin(in)).conn)
			configureInput(t, c, "a")
			_, err := c.Probe(testCtx(t), &pb.ProbeRequest{InstanceId: "a"})
			wantCode(t, err, tc.want)
		})
	}
}

func TestInputBrowse(t *testing.T) {
	t.Run("not a Browser answers UNIMPLEMENTED", func(t *testing.T) {
		c := pb.NewInputClient(start(t, inputPlugin(newFakeInput())).conn)
		configureInput(t, c, "a")
		_, err := c.Browse(testCtx(t), &pb.BrowseRequest{InstanceId: "a"})
		wantCode(t, err, codes.Unimplemented)
	})
	t.Run("a Browser lists nodes", func(t *testing.T) {
		in := &fakeBrowserInput{fakeInput: newFakeInput(), nodes: map[string][]Node{
			"": {{Address: "counter:1", Name: "One", Type: "int32", Access: "read"}},
		}}
		p := inputPlugin(in)
		p.Manifest.Input.Browse = true
		c := pb.NewInputClient(start(t, p).conn)
		configureInput(t, c, "a")
		resp, err := c.Browse(testCtx(t), &pb.BrowseRequest{InstanceId: "a", Depth: 2})
		if err != nil {
			t.Fatal(err)
		}
		n := resp.GetNodes()
		if len(n) != 1 || n[0].GetAddress() != "counter:1" || n[0].GetName() != "One" || n[0].GetType() != "int32" || n[0].GetAccess() != "read" {
			t.Fatalf("nodes = %v", n)
		}
	})
}

func TestInputPanicInRunEndsOnlyThatStream(t *testing.T) {
	in := newFakeInput()
	in.run = func(ctx context.Context, instance string, _ InputEmitter) error {
		if instance == "bad" {
			panic("secret-panic-value")
		}
		<-ctx.Done()
		return ctx.Err()
	}
	h := start(t, inputPlugin(in))
	c := pb.NewInputClient(h.conn)
	configureInput(t, c, "bad")
	configureInput(t, c, "good")

	stream, err := c.Subscribe(testCtx(t), &pb.SubscribeRequest{InstanceId: "bad"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = stream.Recv()
	wantCode(t, err, codes.Internal)
	if got := <-in.stops; got != "bad" {
		t.Errorf("Stop(%q), want bad", got)
	}
	if !strings.Contains(h.stderr.String(), "panic") || strings.Contains(h.stderr.String(), "secret-panic-value") {
		t.Errorf("stderr = %q", h.stderr.String())
	}

	// The plugin keeps serving: the other instance subscribes fine.
	ctx, cancel := context.WithCancel(testCtx(t))
	defer cancel()
	if _, err := c.Subscribe(ctx, &pb.SubscribeRequest{InstanceId: "good"}); err != nil {
		t.Fatal(err)
	}
	in.waitStarted(t, "good")
}

func TestInputProbeRefusesAValueWithoutKind(t *testing.T) {
	// A Value of the zero kind is refused when it leaves the plugin.
	in := newFakeInput()
	zero := testSample("a")
	zero.Value = Value{}
	in.probe = func(context.Context, string) ([]Sample, error) { return []Sample{zero}, nil }
	c := pb.NewInputClient(start(t, inputPlugin(in)).conn)
	configureInput(t, c, "a")
	_, err := c.Probe(testCtx(t), &pb.ProbeRequest{InstanceId: "a"})
	wantCode(t, err, codes.Internal)
}

// TestAcquireRefusesAStreamWhoseClientLeft: a Subscribe whose client has already gone (its
// context is done when the handler runs, e.g. the agent closed the stream and configured the
// instance again before the handler got scheduled) must not take the instance, or the stream
// the agent opens next is refused with ALREADY_EXISTS and Stop runs for a Run nobody wanted.
func TestAcquireRefusesAStreamWhoseClientLeft(t *testing.T) {
	inst := newInstance()
	inst.configured = true
	parent, cancel := context.WithCancel(context.Background())
	cancel()

	_, run, err := inst.acquire(parent, context.Background())

	wantCode(t, err, codes.Canceled)
	if run != nil || inst.active != nil {
		t.Fatal("the instance was taken by a stream whose client had left")
	}
	if inst.takeStop() == false {
		t.Error("the instance was marked stopped although no Run began")
	}
}
