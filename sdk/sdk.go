// Package sdk is what a Laraplate edge plugin is written with. The author implements the small
// interfaces [Input], [Processor] and [Output]; [Serve] turns them into the gRPC services of
// protocol laraplate.edge.plugin.v1 and owns everything around them: the process handshake, the
// socket or named pipe, health, Shutdown, deadlines, instances, panic recovery and the
// conversion between the wire messages and the Go types of this package.
//
// A plugin is a program whose main calls [Serve]:
//
//	func main() {
//		sdk.ServeOutput("jsonl", version, &sink{}, manifest.ConfigKind{ConfigSchema: schema})
//	}
//
// Run with --manifest the program prints its canonical plugin.json and exits; started by the
// agent it serves until the agent calls Shutdown.
//
// Instances. The agent configures one instance of a kind per section of its configuration and
// every call names the instance. The SDK keeps the state of each instance (configured or not, and
// for inputs and processors the running stream) so that two instances of one Input run
// concurrently and independently. An instance a Configure refused is not configured.
//
// Panics. A panic in the author's code is recovered per call: the call fails with gRPC INTERNAL, one
// line goes to stderr (the panic's type, never request data) and the plugin keeps serving.
package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	"github.com/getlaraplate/laraedge-sdk/handshake"
	"github.com/getlaraplate/laraedge-sdk/manifest"
	pb "github.com/getlaraplate/laraedge-sdk/pluginv1"
)

// Input reads machines and hands their samples to the agent. The agent configures an instance,
// then opens its stream (Run) and cancels the stream's context to end it.
type Input interface {
	// Configure validates and applies the configuration of an instance (see [DecodeInput]). A
	// non-empty result refuses it. The context carries the 10 second deadline of the call.
	Configure(ctx context.Context, instance string, cfg json.RawMessage) []ConfigError
	// Run reads the machines of a configured instance and reports through out until ctx is
	// cancelled, when it must return. Its error, if any, ends the stream with that error.
	Run(ctx context.Context, instance string, out InputEmitter) error
	// Probe reads every point of the instance once, within the deadline of ctx (30 seconds).
	Probe(ctx context.Context, instance string) ([]Sample, error)
	// Stop releases what the instance holds. The SDK calls it after every Run has returned and,
	// at Shutdown, once for every configured instance that is not already stopped. Stop is never
	// called while Run or Configure of the same instance is running, Shutdown included (unless
	// the Shutdown deadline expires first, in which case Stop is skipped and the process exits),
	// and once Shutdown has begun no instance is configured or run again. An instance stays
	// configured after Stop, so Run may be called again. Stop must tolerate an instance it does
	// not know. Its context carries the deadline of the Shutdown at Shutdown and a deadline of
	// 10 seconds after a stream ended; Stop should return by that deadline.
	Stop(ctx context.Context, instance string) error
}

// Browser is implemented by an [Input] that lists the nodes of its source. An Input without it
// answers UNIMPLEMENTED to Browse.
type Browser interface {
	Browse(ctx context.Context, instance, node string, depth int) ([]Node, error)
}

// InputEmitter is how Run reports to the agent. Its methods are safe for concurrent use. They
// deliver in order and block while the stream to the agent is backed up, so a Run that outpaces
// the agent slows down instead of piling samples up in memory. After the stream has ended they
// return an error.
type InputEmitter interface {
	// Sample reports a reading. An invalid sample is refused with an error and not sent.
	Sample(Sample) error
	// Answered reports that the device answered, also when it has no new samples.
	Answered(device string) error
	// Health reports whether the instance is connected and, when it is not, the kind of the last
	// read error (empty when the last read succeeded). A kind other than the four ReadError
	// constants is refused with an error and not sent.
	Health(connected bool, readError ReadError) error
}

// Processor derives samples from the records of the devices it observes. A plugin never
// emits births or deaths.
type Processor interface {
	// Configure validates and applies the configuration of an instance (see [DecodeProcessor]).
	Configure(ctx context.Context, instance string, cfg json.RawMessage) []ConfigError
	// Process receives copies of the observed records on in, which is closed when the agent ends
	// its side of the stream, and emits samples of its declared derived devices and signals
	// through out (safe for concurrent use; it blocks while the agent is backed up and returns an
	// error once the stream has ended). It returns when in is closed or ctx is cancelled.
	Process(ctx context.Context, instance string, in <-chan Record, out func(Sample) error) error
}

// Output delivers batches to a destination. A plugin output never buffers: the agent does.
type Output interface {
	// Configure validates and applies the configuration of an instance (see [DecodeOutput]).
	Configure(ctx context.Context, instance string, cfg json.RawMessage) []ConfigError
	// Send delivers one batch within the deadline of ctx (30 seconds) and answers with one
	// outcome. Sending the same MessageID twice must give OutcomeAccepted or OutcomeDuplicate.
	Send(ctx context.Context, instance string, b Batch) Outcome
}

// Plugin is a plugin: its identity, the kinds it serves and the manifest of those kinds. A kind
// is served when its field is set, and Manifest must offer exactly the kinds that are set.
type Plugin struct {
	Name    string
	Version string
	// Input, Processor and Output are each served when set.
	Input     Input
	Processor Processor
	Output    Output
	// Manifest holds the schemas and capabilities of the kinds, for plugin.json.
	Manifest manifest.Kinds
}

// ServeInput serves a plugin that offers one Input.
func ServeInput(name, version string, in Input, kind manifest.InputKind) {
	Serve(Plugin{Name: name, Version: version, Input: in, Manifest: manifest.Kinds{Input: &kind}})
}

// ServeProcessor serves a plugin that offers one Processor.
func ServeProcessor(name, version string, p Processor, kind manifest.ConfigKind) {
	Serve(Plugin{Name: name, Version: version, Processor: p, Manifest: manifest.Kinds{Processor: &kind}})
}

// ServeOutput serves a plugin that offers one Output.
func ServeOutput(name, version string, o Output, kind manifest.ConfigKind) {
	Serve(Plugin{Name: name, Version: version, Output: o, Manifest: manifest.Kinds{Output: &kind}})
}

// Hooks of the process, replaced by the tests.
var (
	exit                       = os.Exit
	listen                     = handshake.Listen
	stdout           io.Writer = os.Stdout
	stderr           io.Writer = os.Stderr
	programArgs                = func() []string { return os.Args[1:] }
	protocolVersions           = []string{"v1"}
)

// defaultShutdownDeadline applies when Shutdown carries no deadline.
const defaultShutdownDeadline = 5 * time.Second

// stopTimeout bounds the Stop the SDK calls after a stream of an input instance ended.
const stopTimeout = 10 * time.Second

// Serve runs the plugin and exits the process; it does not return.
//
// With the flag --manifest it prints the canonical plugin.json and exits 0 (this needs no cookie),
// or exits 1 when the plugin or its manifest is inconsistent. Otherwise it performs the
// handshake of package handshake (exit 1 when the agent did not start it, exit 3 when no
// protocol is shared), serves the kinds that are set and the standard grpc.health.v1 service
// until the agent calls Shutdown, stops gracefully within the deadline of that call and exits 0.
func Serve(p Plugin) {
	exit(run(p, programArgs(), stdout, stderr))
}

func run(p Plugin, args []string, out, errw io.Writer) int {
	fs := flag.NewFlagSet(p.Name, flag.ContinueOnError)
	fs.SetOutput(errw)
	printManifest := fs.Bool("manifest", false, "print the canonical plugin.json and exit")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	_, manifestJSON, err := buildManifest(p)
	if err != nil {
		_, _ = fmt.Fprintf(errw, "plugin: %v\n", err)
		return 1
	}
	if *printManifest {
		if _, err := out.Write(append(append([]byte(nil), manifestJSON...), '\n')); err != nil {
			_, _ = fmt.Fprintf(errw, "plugin: writing the manifest: %v\n", err)
			return 1
		}
		return 0
	}
	lis, _, err := listen(protocolVersions)
	switch {
	case errors.Is(err, handshake.ErrNoProtocol):
		_, _ = fmt.Fprintln(errw, err)
		return handshake.ExitCodeNoProtocol
	case err != nil:
		_, _ = fmt.Fprintln(errw, err)
		return 1
	}
	return newServer(p, manifestJSON, errw).serve(lis)
}

// buildManifest returns the manifest of the plugin and its canonical JSON, or the reasons the
// plugin is inconsistent: the kinds set must be the kinds the manifest offers, and the manifest
// must pass [manifest.Manifest.Check].
func buildManifest(p Plugin) (manifest.Manifest, []byte, error) {
	m := manifest.Manifest{Name: p.Name, Version: p.Version, ProtocolVersions: protocolVersions, Kinds: p.Manifest}
	var problems []string
	pair := func(set, offered bool, kind string) {
		switch {
		case set && !offered:
			problems = append(problems, kind+" is set but Manifest."+kind+" is not")
		case !set && offered:
			problems = append(problems, "Manifest."+kind+" is set but "+kind+" is not")
		}
	}
	pair(p.Input != nil, m.Kinds.Input != nil, "Input")
	pair(p.Processor != nil, m.Kinds.Processor != nil, "Processor")
	pair(p.Output != nil, m.Kinds.Output != nil, "Output")
	if m.Kinds.Input != nil && m.Kinds.Input.Browse && p.Input != nil {
		if _, ok := p.Input.(Browser); !ok {
			problems = append(problems, "Manifest.Input.Browse is true but the Input does not implement Browser")
		}
	}
	problems = append(problems, m.Check()...)
	if len(problems) > 0 {
		return manifest.Manifest{}, nil, fmt.Errorf("invalid plugin: %s", strings.Join(problems, "; "))
	}
	raw, err := json.Marshal(m)
	if err != nil {
		return manifest.Manifest{}, nil, fmt.Errorf("encode manifest: %w", err)
	}
	canonical, err := manifest.Canonical(raw)
	if err != nil {
		return manifest.Manifest{}, nil, fmt.Errorf("canonical manifest: %w", err)
	}
	return m, canonical, nil
}

// logger writes the plugin's own lines to stderr.
type logger struct {
	mu sync.Mutex
	w  io.Writer
}

func (l *logger) printf(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	_, _ = fmt.Fprintf(l.w, "plugin: "+format+"\n", args...)
}

// recovered is deferred by every place that runs the author's code. It turns a panic into an
// INTERNAL error stored in *errp (when errp is not nil) and logs one line naming where, with the
// type of the panic value and, for runtime errors such as an index out of range, their text. It
// never prints other panic values, which could hold request data.
func (l *logger) recovered(where string, errp *error) {
	v := recover()
	if v == nil {
		return
	}
	what := fmt.Sprintf("%T", v)
	if re, ok := v.(runtime.Error); ok {
		what = re.Error()
	}
	l.printf("panic in %s (%s)", where, strings.ReplaceAll(what, "\n", " "))
	if errp != nil {
		*errp = status.Error(codes.Internal, "the plugin panicked in "+where)
	}
}

// rpcError maps an error of the author's code to a gRPC error: status errors pass through,
// context errors keep their code, anything else is UNKNOWN.
func rpcError(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	}
	return status.Error(codes.Unknown, err.Error())
}

// streamResult is the result of a stream handler once the author's function returned.
func streamResult(ctx context.Context, runErr, sendErr error) error {
	switch {
	case sendErr != nil:
		return sendErr
	case runErr == nil:
		return nil
	case ctx.Err() != nil && (errors.Is(runErr, context.Canceled) || errors.Is(runErr, context.DeadlineExceeded)):
		return nil
	}
	return rpcError(runErr)
}

var errStreamEnded = errors.New("sdk: the stream has ended")

// pump runs fn, the author's streaming function, in its own recovered goroutine and delivers what
// it emits to send, on the calling goroutine and in order. emit hands one item over at a time, so
// an author that outpaces send is held back instead of buffered; it fails once ctx is done. pump
// returns when fn has returned. A failing send cancels the stream (cancel) and later items are
// dropped.
func pump[T any](ctx context.Context, cancel context.CancelFunc, log *logger, where string,
	fn func(emit func(T) error) error, send func(T) error,
) (runErr, sendErr error) {
	items := make(chan T)
	done := make(chan error, 1)
	go func() {
		var err error
		defer func() { done <- err }()
		defer log.recovered(where, &err)
		err = fn(func(item T) error {
			if ctx.Err() != nil {
				return errStreamEnded
			}
			select {
			case items <- item:
				return nil
			case <-ctx.Done():
				return errStreamEnded
			}
		})
	}()
	for {
		select {
		case item := <-items:
			if sendErr == nil {
				if err := send(item); err != nil {
					sendErr = err
					cancel()
				}
			}
		case runErr = <-done:
			return runErr, sendErr
		}
	}
}

// activeRun is the running stream of an instance.
type activeRun struct {
	cancel context.CancelFunc
	done   chan struct{}
}

// instance is the state the SDK keeps for one instance of a kind.
type instance struct {
	sem        chan struct{} // held by a Configure in progress
	mu         sync.Mutex
	configured bool
	stopped    bool // input only: Stop was called since the last Configure or Run began
	active     *activeRun
}

func newInstance() *instance { return &instance{sem: make(chan struct{}, 1)} }

// errShuttingDown refuses the calls that arrive once Shutdown has begun.
var errShuttingDown = status.Error(codes.Unavailable, "the plugin is shutting down")

// configure runs call (the author's Configure) as the only Configure of the instance, with its
// stream stopped first. The instance is configured afterwards only when call reported no error.
// Once serving is done (Shutdown has begun) it refuses with UNAVAILABLE and leaves the instance
// as it is: the check is made while holding the semaphore, which the Stop pass of Shutdown takes
// too, so the author's Configure and Stop never overlap.
func (i *instance) configure(ctx, serving context.Context, call func() []ConfigError) ([]ConfigError, error) {
	if serving.Err() != nil {
		return nil, errShuttingDown
	}
	select {
	case i.sem <- struct{}{}:
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	if serving.Err() != nil {
		<-i.sem
		return nil, errShuttingDown
	}
	i.mu.Lock()
	i.configured = false
	run := i.active
	i.mu.Unlock()
	if run != nil {
		run.cancel()
		select {
		case <-run.done:
		case <-ctx.Done():
			<-i.sem
			return nil, status.FromContextError(ctx.Err()).Err()
		}
	}
	ok := false
	defer func() {
		i.mu.Lock()
		i.configured = ok
		i.stopped = false
		i.mu.Unlock()
		<-i.sem
	}()
	errs := call()
	ok = len(errs) == 0
	return errs, nil
}

// acquire starts the stream of a configured instance; one stream at a time per instance. Once
// serving is done (Shutdown has begun) it refuses with UNAVAILABLE: the check is made under i.mu,
// as is the Stop pass's decision, so no stream starts on an instance that pass stopped. A stream
// whose client has already left (parent done) is refused with its context error, so it neither
// holds the instance against the client's next stream nor causes a Run and a Stop for nobody.
func (i *instance) acquire(parent, serving context.Context) (context.Context, *activeRun, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if serving.Err() != nil {
		return nil, nil, errShuttingDown
	}
	if err := parent.Err(); err != nil {
		return nil, nil, status.FromContextError(err).Err()
	}
	if !i.configured {
		return nil, nil, status.Error(codes.FailedPrecondition, "the instance is not configured")
	}
	if i.active != nil {
		return nil, nil, status.Error(codes.AlreadyExists, "the instance already has a running stream")
	}
	ctx, cancel := context.WithCancel(parent)
	i.active = &activeRun{cancel: cancel, done: make(chan struct{})}
	i.stopped = false
	return ctx, i.active, nil
}

func (i *instance) release(run *activeRun) {
	i.mu.Lock()
	i.active = nil
	i.mu.Unlock()
	run.cancel()
	close(run.done)
}

// isConfigured reports whether calls that need a configured instance may proceed.
func (i *instance) isConfigured() bool {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.configured
}

// takeStop reports whether the caller must call Stop: true once per Configure or Run.
func (i *instance) takeStop() bool {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.stopped {
		return false
	}
	i.stopped = true
	return true
}

// cancelActive cancels the running stream, if any, and returns it.
func (i *instance) cancelActive() *activeRun {
	i.mu.Lock()
	run := i.active
	i.mu.Unlock()
	if run != nil {
		run.cancel()
	}
	return run
}

// registry holds the instances of one kind.
type registry struct {
	mu sync.Mutex
	m  map[string]*instance
}

func (r *registry) obtain(id string) *instance {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.m == nil {
		r.m = map[string]*instance{}
	}
	inst, ok := r.m[id]
	if !ok {
		inst = newInstance()
		r.m[id] = inst
	}
	return inst
}

// lookup returns the instance named by a call that needs it configured.
func (r *registry) lookup(id string) (*instance, error) {
	if id == "" {
		return nil, status.Error(codes.InvalidArgument, "instance_id is required")
	}
	r.mu.Lock()
	inst := r.m[id]
	r.mu.Unlock()
	if inst == nil || !inst.isConfigured() {
		return nil, status.Error(codes.FailedPrecondition, "the instance is not configured")
	}
	return inst, nil
}

func (r *registry) snapshot() map[string]*instance {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[string]*instance, len(r.m))
	for id, inst := range r.m {
		out[id] = inst
	}
	return out
}

// configureRPC is the Configure of every kind. serving is the server's context, done once
// Shutdown has begun.
func configureRPC(ctx, serving context.Context, reg *registry, req *pb.ConfigureRequest,
	call func(id string, cfg json.RawMessage) []ConfigError,
) (*pb.ConfigureResponse, error) {
	id := req.GetInstanceId()
	if id == "" {
		return nil, status.Error(codes.InvalidArgument, "instance_id is required")
	}
	errs, err := reg.obtain(id).configure(ctx, serving, func() []ConfigError { return call(id, json.RawMessage(req.GetConfig())) })
	if err != nil {
		return nil, err
	}
	return &pb.ConfigureResponse{Errors: toProtoConfigErrors(errs)}, nil
}

// server is the gRPC server of a plugin.
type server struct {
	manifest []byte
	log      *logger
	grpc     *grpc.Server
	health   *health.Server
	// ctx is cancelled when Shutdown begins: it ends the streams and refuses new calls.
	ctx    context.Context
	cancel context.CancelFunc

	input     *inputService
	processor *processorService
	output    *outputService

	mu       sync.Mutex
	lis      net.Listener
	bound    context.Context // the deadline of the Shutdown, set before ctx is cancelled
	once     sync.Once
	finished chan struct{} // closed when the shutdown sequence is complete
}

// newServer registers the services of the kinds p sets. manifestJSON is what Info reports.
func newServer(p Plugin, manifestJSON []byte, errw io.Writer) *server {
	s := &server{manifest: manifestJSON, log: &logger{w: errw}, health: health.NewServer(), finished: make(chan struct{})}
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.grpc = grpc.NewServer(
		grpc.MaxRecvMsgSize(handshake.MaxMessageSize),
		grpc.MaxSendMsgSize(handshake.MaxMessageSize),
		grpc.ChainUnaryInterceptor(s.recoverUnary, s.refuseUnaryWhenShuttingDown),
		grpc.ChainStreamInterceptor(s.recoverStream, s.refuseStreamWhenShuttingDown),
	)
	pb.RegisterPluginServer(s.grpc, &pluginService{s: s, name: p.Name, version: p.Version})
	healthpb.RegisterHealthServer(s.grpc, s.health)
	if p.Input != nil {
		s.input = &inputService{s: s, in: p.Input}
		pb.RegisterInputServer(s.grpc, s.input)
	}
	if p.Processor != nil {
		s.processor = &processorService{s: s, p: p.Processor}
		pb.RegisterProcessorServer(s.grpc, s.processor)
	}
	if p.Output != nil {
		s.output = &outputService{s: s, o: p.Output}
		pb.RegisterOutputServer(s.grpc, s.output)
	}
	return s
}

// recoverUnary and recoverStream are the last line of defence: a panic in the SDK's own handler
// code, or in the author's code the handler runs directly, fails that call and nothing else.
func (s *server) recoverUnary(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
	defer s.log.recovered(info.FullMethod, &err)
	return handler(ctx, req)
}

func (s *server) recoverStream(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
	defer s.log.recovered(info.FullMethod, &err)
	return handler(srv, ss)
}

// refuseUnaryWhenShuttingDown and refuseStreamWhenShuttingDown refuse with UNAVAILABLE every call
// that arrives once Shutdown has begun, except Info, Shutdown and the health service. grpc-go
// still dispatches calls that reached it before the client processed GOAWAY. A call that passes
// here just before Shutdown begins is refused by the instance itself (acquire, configure) when it
// would start a stream or configure.
func (s *server) refuseUnaryWhenShuttingDown(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if s.ctx.Err() != nil && !servedDuringShutdown(info.FullMethod) {
		return nil, errShuttingDown
	}
	return handler(ctx, req)
}

func (s *server) refuseStreamWhenShuttingDown(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	if s.ctx.Err() != nil && !servedDuringShutdown(info.FullMethod) {
		return errShuttingDown
	}
	return handler(srv, ss)
}

// servedDuringShutdown reports whether a method is still served once Shutdown has begun.
func servedDuringShutdown(fullMethod string) bool {
	switch fullMethod {
	case pb.Plugin_Info_FullMethodName, pb.Plugin_Shutdown_FullMethodName:
		return true
	}
	return strings.HasPrefix(fullMethod, "/"+healthpb.Health_ServiceDesc.ServiceName+"/")
}

// stopContext returns the context of a Stop called after a stream ended: bounded by
// [stopTimeout] and, once Shutdown has begun, by its deadline.
func (s *server) stopContext() (context.Context, context.CancelFunc) {
	s.mu.Lock()
	parent := s.bound
	s.mu.Unlock()
	if parent == nil {
		parent = context.Background()
	}
	return context.WithTimeout(parent, stopTimeout)
}

// serve serves on lis until Shutdown completes (exit code 0) or serving fails (1). It does not
// wait for grpc.Server.Serve after a shutdown began: Serve returns only once Stop or GracefulStop
// has finished, and those wait for handlers that ignore their context.
func (s *server) serve(lis net.Listener) int {
	s.mu.Lock()
	s.lis = lis
	s.mu.Unlock()
	served := make(chan error, 1)
	go func() { served <- s.grpc.Serve(lis) }()
	select {
	case <-s.finished:
		return 0
	case err := <-served:
		if s.ctx.Err() != nil {
			<-s.finished
			return 0
		}
		_ = lis.Close()
		s.cancel()
		if err != nil {
			s.log.printf("serving: %v", err)
			return 1
		}
		return 0
	}
}

// beginShutdown starts the shutdown sequence once.
func (s *server) beginShutdown(deadline time.Duration) {
	s.once.Do(func() { go s.shutdown(deadline) })
}

// shutdown: health turns NOT_SERVING, streams are cancelled, new calls are refused, Stop is
// called on every instance, then the server stops gracefully, forcefully once the deadline
// passes, and the listener is closed (which removes the socket).
func (s *server) shutdown(deadline time.Duration) {
	defer close(s.finished)
	bound, release := context.WithTimeout(context.Background(), deadline)
	defer release()
	s.mu.Lock()
	s.bound = bound
	s.mu.Unlock()
	s.health.Shutdown()
	s.cancel()

	graceful := make(chan struct{})
	go func() {
		s.grpc.GracefulStop()
		close(graceful)
	}()
	if s.input != nil {
		s.input.stopAll(bound)
	}
	select {
	case <-graceful:
	case <-bound.Done():
		// Stop closes the connections at once but, like GracefulStop, then waits for handlers
		// that ignore their context. Neither is awaited: the deadline has passed and the
		// process exits as soon as this sequence ends.
		go s.grpc.Stop()
	}
	s.mu.Lock()
	lis := s.lis
	s.mu.Unlock()
	if lis != nil {
		_ = lis.Close()
	}
}

// pluginService is the Plugin service every plugin serves.
type pluginService struct {
	pb.UnimplementedPluginServer
	s       *server
	name    string
	version string
}

func (p *pluginService) Info(context.Context, *pb.InfoRequest) (*pb.InfoResponse, error) {
	return &pb.InfoResponse{Name: p.name, Version: p.version, ProtocolVersions: protocolVersions, Manifest: p.s.manifest}, nil
}

func (p *pluginService) Shutdown(_ context.Context, req *pb.ShutdownRequest) (*pb.ShutdownResponse, error) {
	deadline := defaultShutdownDeadline
	if d := req.GetDeadline(); d != nil && d.CheckValid() == nil {
		deadline = max(d.AsDuration(), 0)
	}
	p.s.beginShutdown(deadline)
	return &pb.ShutdownResponse{}, nil
}
