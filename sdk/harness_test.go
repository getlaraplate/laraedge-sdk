package sdk

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/getlaraplate/laraedge-sdk/handshake"
	"github.com/getlaraplate/laraedge-sdk/manifest"
	pb "github.com/getlaraplate/laraedge-sdk/pluginv1"
)

// testTimeout bounds every blocking wait of a test so a bug fails instead of hanging.
const testTimeout = 20 * time.Second

func testCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	t.Cleanup(cancel)
	return ctx
}

// syncBuffer is a bytes.Buffer safe for concurrent writers.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func inputKind() *manifest.InputKind {
	return &manifest.InputKind{
		ConnectionSchema: json.RawMessage(`{"type":"object"}`),
		AddressSchema:    json.RawMessage(`{"type":"string"}`),
		ReadModes:        []string{"poll"},
		Types:            []string{"int32"},
	}
}

func configKind() *manifest.ConfigKind {
	return &manifest.ConfigKind{ConfigSchema: json.RawMessage(`{"type":"object"}`)}
}

// harness is a plugin served over an in-memory connection.
type harness struct {
	conn   *grpc.ClientConn
	srv    *server
	stderr *syncBuffer
	done   chan int
	lis    *bufconn.Listener
}

// start serves p over bufconn and returns a client connection to it.
func start(t *testing.T, p Plugin) *harness {
	t.Helper()
	h := &harness{stderr: &syncBuffer{}, done: make(chan int, 1), lis: bufconn.Listen(1 << 20)}
	_, manifestJSON, err := buildManifest(p)
	if err != nil {
		t.Fatalf("buildManifest: %v", err)
	}
	h.srv = newServer(p, manifestJSON, h.stderr)
	go func() { h.done <- h.srv.serve(h.lis) }()
	h.conn = dialBufconn(t, h.lis)
	t.Cleanup(func() {
		_ = h.conn.Close()
		h.srv.grpc.Stop()
		_ = h.lis.Close()
		<-h.done
	})
	return h
}

func dialBufconn(t *testing.T, lis *bufconn.Listener) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.WaitForReady(true),
			grpc.MaxCallSendMsgSize(handshake.MaxMessageSize*2),
			grpc.MaxCallRecvMsgSize(handshake.MaxMessageSize*2),
		),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	return conn
}

// fakeInput is an Input whose behaviour the test supplies.
type fakeInput struct {
	configure func(ctx context.Context, instance string, cfg json.RawMessage) []ConfigError
	run       func(ctx context.Context, instance string, out InputEmitter) error
	probe     func(ctx context.Context, instance string) ([]Sample, error)
	stop      func(ctx context.Context, instance string) // runs before the instance is recorded in stops
	stops     chan string
	started   chan string
}

func newFakeInput() *fakeInput {
	return &fakeInput{stops: make(chan string, 64), started: make(chan string, 64)}
}

// waitStarted returns once Run of the instance has been called.
func (f *fakeInput) waitStarted(t *testing.T, instance string) {
	t.Helper()
	ctx := testCtx(t)
	for {
		select {
		case got := <-f.started:
			if got == instance {
				return
			}
		case <-ctx.Done():
			t.Fatalf("Run of %s never started", instance)
		}
	}
}

func (f *fakeInput) Configure(ctx context.Context, instance string, cfg json.RawMessage) []ConfigError {
	if f.configure != nil {
		return f.configure(ctx, instance, cfg)
	}
	return nil
}

func (f *fakeInput) Run(ctx context.Context, instance string, out InputEmitter) error {
	f.started <- instance
	if f.run != nil {
		return f.run(ctx, instance, out)
	}
	<-ctx.Done()
	return ctx.Err()
}

func (f *fakeInput) Probe(ctx context.Context, instance string) ([]Sample, error) {
	if f.probe != nil {
		return f.probe(ctx, instance)
	}
	return nil, nil
}

func (f *fakeInput) Stop(ctx context.Context, instance string) error {
	if f.stop != nil {
		f.stop(ctx, instance)
	}
	f.stops <- instance
	return nil
}

// fakeBrowserInput is a fakeInput that is also a Browser.
type fakeBrowserInput struct {
	*fakeInput
	nodes map[string][]Node
}

func (f *fakeBrowserInput) Browse(_ context.Context, instance, node string, depth int) ([]Node, error) {
	_ = instance
	_ = depth
	return f.nodes[node], nil
}

type fakeProcessor struct {
	configure func(ctx context.Context, instance string, cfg json.RawMessage) []ConfigError
	process   func(ctx context.Context, instance string, in <-chan Record, out func(Sample) error) error
}

func (f *fakeProcessor) Configure(ctx context.Context, instance string, cfg json.RawMessage) []ConfigError {
	if f.configure != nil {
		return f.configure(ctx, instance, cfg)
	}
	return nil
}

func (f *fakeProcessor) Process(ctx context.Context, instance string, in <-chan Record, out func(Sample) error) error {
	if f.process != nil {
		return f.process(ctx, instance, in, out)
	}
	for range in {
	}
	return nil
}

type fakeOutput struct {
	configure func(ctx context.Context, instance string, cfg json.RawMessage) []ConfigError
	send      func(ctx context.Context, instance string, b Batch) Outcome
}

func (f *fakeOutput) Configure(ctx context.Context, instance string, cfg json.RawMessage) []ConfigError {
	if f.configure != nil {
		return f.configure(ctx, instance, cfg)
	}
	return nil
}

func (f *fakeOutput) Send(ctx context.Context, instance string, b Batch) Outcome {
	if f.send != nil {
		return f.send(ctx, instance, b)
	}
	return Outcome{Kind: OutcomeAccepted}
}

func testSample(device string) Sample {
	return Sample{
		Device:   device,
		Signal:   "temperature",
		TS:       time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC),
		Value:    IntValue(7),
		Quality:  QualityGood,
		Priority: PriorityCritical,
	}
}

// fakeSubscribeStream is the server side of a Subscribe stream, for calling the handler directly
// as grpc-go does with a stream that arrived before the client processed GOAWAY.
type fakeSubscribeStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (f *fakeSubscribeStream) Context() context.Context { return f.ctx }

func (f *fakeSubscribeStream) Send(*pb.InputEvent) error { return nil }
