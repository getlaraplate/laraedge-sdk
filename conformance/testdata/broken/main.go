// Command broken is the test plugin of package conformance. It offers the three kinds on raw
// gRPC, without the SDK, so that each conformance check can be made to fail on its own. With no
// defect selected it keeps the whole contract. Defects are selected through environment
// variables, since the host passes no arguments:
//
//	BROKEN_IGNORE_COOKIE=1      start without the cookie as if it were there
//	BROKEN_STRAY_LINE=1         print a line on stdout before the handshake line
//	BROKEN_PERMISSIONS=1        socket 0666 and directory 0755 (Unix); an extra entry granting
//	                            everyone in the pipe's DACL (Windows)
//	BROKEN_INFO=1               Info reports a manifest that differs from plugin.json
//	BROKEN_HEALTH=1             health reports NOT_SERVING
//	BROKEN_ACCEPT_INVALID=<kind> Configure of that kind (input, processor, output) accepts
//	                            every section
//	BROKEN_STUCK_STREAM=1       an input stream ignores its cancellation until Shutdown
//	BROKEN_UNDECLARED_SIGNAL=1  the processor also emits a signal its section does not declare
//	BROKEN_DUPLICATE_ERROR=1    a message_id sent again fails with an error
//	BROKEN_SLOW_SHUTDOWN=1      Shutdown answers but the process never exits
//	BROKEN_FIXED_ADDRESS=1      the socket or pipe has a fixed name, so a restart reuses it
//	BROKEN_SILENT=<kind>        the input stream or the processor (input, processor) emits
//	                            nothing
//	BROKEN_CONCURRENT_STREAMS=1 a second stream of an input instance is served, not refused
//	BROKEN_SEND_ERROR=1         every Send fails with an error
//	BROKEN_RETRY_AFTER_BARE=1   every Send answers RETRY_AFTER without retry_after
//	BROKEN_REJECT_BARE=1        every Send answers REJECT_INVALID without a reason
//	BROKEN_NEVER_ENDS=1         a Process stream does not end when the client closes its side
//	BROKEN_NEVER_READS=1        a Process stream reads its start message and nothing more; the
//	                            server keeps the 64 KiB flow-control window, so the client's
//	                            sends block
//	BROKEN_ENDS_EARLY=1         a Process stream ends OK on its own after answering the sample
//	                            of value 3, the last of the suite's synthetic records
//	BROKEN_CONCURRENT_PROCESS=1 a second Process stream of a processor instance is served, not
//	                            refused
//	BROKEN_NEWEST_WINS=1        a new Process stream of a processor instance ends the running one
//	                            with OK and is served in its place
//	BROKEN_SAMPLE_ON_REFUSAL=1  before refusing a second Process stream, the processor emits one
//	                            sample on the running one (allowed: a processor may emit any time)
//	BROKEN_EXIT_BEFORE_SHUTDOWN=1 after answering DUPLICATE, exit 0 at the next call, unanswered
//
// A section is valid unless it has the top-level member "refuse": true. A section with a
// "records" member is refused: the suite must strip it, the agent never sends it.
package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/getlaraplate/laraedge-sdk/handshake"
	"github.com/getlaraplate/laraedge-sdk/manifest"
	pb "github.com/getlaraplate/laraedge-sdk/pluginv1"
	"github.com/getlaraplate/laraedge-sdk/sdk"
)

//go:embed plugin.json
var manifestFile []byte

func defect(name string) bool { return os.Getenv(name) == "1" }

func main() {
	if os.Getenv(handshake.CookieKey) != handshake.CookieValue && !defect("BROKEN_IGNORE_COOKIE") {
		fmt.Fprintln(os.Stderr, handshake.ErrNotLaunched)
		os.Exit(1)
	}
	if defect("BROKEN_STRAY_LINE") {
		fmt.Println("broken: starting")
	}
	if !slices.Contains(strings.Split(os.Getenv(handshake.ProtocolsKey), ","), "v1") {
		fmt.Fprintln(os.Stderr, handshake.ErrNoProtocol)
		os.Exit(handshake.ExitCodeNoProtocol)
	}
	lis, network, address, err := listen()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("1|v1|%s|%s\n", network, address)

	p := newPlugin()
	opts := []grpc.ServerOption{grpc.ChainUnaryInterceptor(p.exitWhenArmed)}
	if defect("BROKEN_NEVER_READS") {
		// A fixed window turns off the dynamic window of gRPC, which would otherwise grow to
		// megabytes and take the records without the processor reading them.
		opts = append(opts, grpc.InitialWindowSize(64<<10), grpc.InitialConnWindowSize(64<<10))
	}
	p.server = grpc.NewServer(opts...)
	pb.RegisterPluginServer(p.server, &pluginService{p: p})
	pb.RegisterInputServer(p.server, &inputService{p: p})
	pb.RegisterProcessorServer(p.server, &processorService{p: p})
	pb.RegisterOutputServer(p.server, &outputService{p: p})
	healthServer := health.NewServer()
	if defect("BROKEN_HEALTH") {
		healthServer.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
	}
	healthpb.RegisterHealthServer(p.server, healthServer)
	if err := p.server.Serve(lis); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	// Serve returns once Shutdown stopped the server; the shutdown goroutine exits the process.
	select {}
}

// plugin is the state shared by the services: configured sections per kind and instance.
type plugin struct {
	server *grpc.Server

	mu       sync.Mutex
	sections map[string]json.RawMessage // key: kind + "/" + instance
	active   map[string]bool            // key: kind + "/" + instance, with a running stream
	newest   map[string]chan struct{}   // BROKEN_NEWEST_WINS: closed when a newer stream replaces it
	running  map[string]func()          // BROKEN_SAMPLE_ON_REFUSAL: emits a sample on the running stream
	seen     map[string]bool            // output instance + "/" + message_id

	shutdownOnce sync.Once
	shutdown     chan struct{}

	exitArmed atomic.Bool // BROKEN_EXIT_BEFORE_SHUTDOWN: exit at the next call
}

// exitWhenArmed exits the process, without answering, at the first call after it was armed.
func (p *plugin) exitWhenArmed(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if p.exitArmed.Load() {
		os.Exit(0)
	}
	return handler(ctx, req)
}

func newPlugin() *plugin {
	return &plugin{
		sections: map[string]json.RawMessage{},
		active:   map[string]bool{},
		newest:   map[string]chan struct{}{},
		running:  map[string]func(){},
		seen:     map[string]bool{},
		shutdown: make(chan struct{}),
	}
}

func (p *plugin) configure(kind string, req *pb.ConfigureRequest) (*pb.ConfigureResponse, error) {
	id := req.GetInstanceId()
	if id == "" {
		return nil, status.Error(codes.InvalidArgument, "instance_id is required")
	}
	var section struct {
		Refuse  bool            `json:"refuse"`
		Records json.RawMessage `json:"records"`
	}
	if err := json.Unmarshal(req.GetConfig(), &section); err != nil {
		section.Refuse = true
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if section.Records != nil {
		delete(p.sections, kind+"/"+id)
		return &pb.ConfigureResponse{Errors: []*pb.ConfigError{{Path: "records", Message: "the agent never sends records"}}}, nil
	}
	if section.Refuse && os.Getenv("BROKEN_ACCEPT_INVALID") != kind {
		delete(p.sections, kind+"/"+id)
		return &pb.ConfigureResponse{Errors: []*pb.ConfigError{{Path: "refuse", Message: "the section asks to be refused"}}}, nil
	}
	p.sections[kind+"/"+id] = append(json.RawMessage(nil), req.GetConfig()...)
	return &pb.ConfigureResponse{}, nil
}

// replaceStream makes the caller the running stream of the instance and ends the one it
// replaces: the returned channel is closed when a newer stream replaces the caller.
func (p *plugin) replaceStream(kind, id string) <-chan struct{} {
	key := kind + "/" + id
	p.mu.Lock()
	defer p.mu.Unlock()
	if replaced, ok := p.newest[key]; ok {
		close(replaced)
	}
	replaced := make(chan struct{})
	p.newest[key] = replaced
	return replaced
}

// claimStream marks the instance as streaming. Another stream of the instance is refused with
// ALREADY_EXISTS unless shared. The returned function releases the claim.
func (p *plugin) claimStream(kind, id string, shared bool) (release func(), err error) {
	key := kind + "/" + id
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.active[key] && !shared {
		return nil, status.Error(codes.AlreadyExists, "the instance already has a running stream")
	}
	p.active[key] = true
	return func() {
		p.mu.Lock()
		delete(p.active, key)
		p.mu.Unlock()
	}, nil
}

func (p *plugin) section(kind, id string) (json.RawMessage, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	section, ok := p.sections[kind+"/"+id]
	if !ok {
		return nil, status.Error(codes.FailedPrecondition, "the instance is not configured")
	}
	return section, nil
}

func sample(device, signal string, value int64) *pb.Sample {
	return &pb.Sample{
		Device: device, Signal: signal, Ts: timestamppb.Now(),
		Value:   &pb.Value{Kind: &pb.Value_IntValue{IntValue: value}},
		Quality: pb.Quality_QUALITY_GOOD, Priority: pb.Priority_PRIORITY_CRITICAL,
	}
}

type pluginService struct {
	pb.UnimplementedPluginServer
	p *plugin
}

func (s *pluginService) Info(context.Context, *pb.InfoRequest) (*pb.InfoResponse, error) {
	raw := manifestFile
	if defect("BROKEN_INFO") {
		var doc map[string]any
		if err := json.Unmarshal(raw, &doc); err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		doc["version"] = "9.9.9"
		changed, err := json.Marshal(doc)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		raw = changed
	}
	canonical, err := manifest.Canonical(raw)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pb.InfoResponse{Name: "broken", Version: "0.1.0", ProtocolVersions: []string{"v1"}, Manifest: canonical}, nil
}

func (s *pluginService) Shutdown(_ context.Context, req *pb.ShutdownRequest) (*pb.ShutdownResponse, error) {
	if defect("BROKEN_SLOW_SHUTDOWN") {
		return &pb.ShutdownResponse{}, nil
	}
	deadline := req.GetDeadline().AsDuration()
	s.p.shutdownOnce.Do(func() {
		close(s.p.shutdown)
		go func() {
			stopped := make(chan struct{})
			go func() {
				s.p.server.GracefulStop()
				close(stopped)
			}()
			select {
			case <-stopped:
			case <-time.After(deadline):
			}
			os.Exit(0)
		}()
	})
	return &pb.ShutdownResponse{}, nil
}

type inputService struct {
	pb.UnimplementedInputServer
	p *plugin
}

func (s *inputService) Configure(_ context.Context, req *pb.ConfigureRequest) (*pb.ConfigureResponse, error) {
	return s.p.configure("input", req)
}

func (s *inputService) points(id string) (sdk.InputConfig, error) {
	section, err := s.p.section("input", id)
	if err != nil {
		return sdk.InputConfig{}, err
	}
	cfg, err := sdk.DecodeInput(section)
	if err != nil {
		return sdk.InputConfig{}, status.Error(codes.FailedPrecondition, err.Error())
	}
	return cfg, nil
}

// Subscribe reports every point and then answered for its device every 20 ms. One stream per
// instance: while one runs, another is refused with ALREADY_EXISTS.
func (s *inputService) Subscribe(req *pb.SubscribeRequest, stream pb.Input_SubscribeServer) error {
	id := req.GetInstanceId()
	cfg, err := s.points(id)
	if err != nil {
		return err
	}
	release, err := s.p.claimStream("input", id, defect("BROKEN_CONCURRENT_STREAMS"))
	if err != nil {
		return err
	}
	defer release()

	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-stream.Context().Done():
			if defect("BROKEN_STUCK_STREAM") {
				<-s.p.shutdown
			}
			return nil
		case <-ticker.C:
			if os.Getenv("BROKEN_SILENT") == "input" {
				continue
			}
			for _, device := range cfg.Devices {
				for _, point := range device.Points {
					_ = stream.Send(&pb.InputEvent{Event: &pb.InputEvent_Sample{Sample: sample(device.Device, point.Signal, 1)}})
				}
				_ = stream.Send(&pb.InputEvent{Event: &pb.InputEvent_Answered{Answered: &pb.Answered{Device: device.Device}}})
			}
		}
	}
}

func (s *inputService) Probe(_ context.Context, req *pb.ProbeRequest) (*pb.ProbeResponse, error) {
	cfg, err := s.points(req.GetInstanceId())
	if err != nil {
		return nil, err
	}
	out := &pb.ProbeResponse{}
	for _, device := range cfg.Devices {
		for _, point := range device.Points {
			out.Samples = append(out.Samples, sample(device.Device, point.Signal, 1))
		}
	}
	return out, nil
}

type processorService struct {
	pb.UnimplementedProcessorServer
	p *plugin
}

func (s *processorService) Configure(_ context.Context, req *pb.ConfigureRequest) (*pb.ConfigureResponse, error) {
	return s.p.configure("processor", req)
}

// Process answers every sample of an observed device with one sample of each declared signal.
func (s *processorService) Process(stream pb.Processor_ProcessServer) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	section, err := s.p.section("processor", first.GetStart().GetInstanceId())
	if err != nil {
		return err
	}
	cfg, err := sdk.DecodeProcessor(section)
	if err != nil {
		return status.Error(codes.FailedPrecondition, err.Error())
	}
	if defect("BROKEN_NEWEST_WINS") {
		replaced := s.p.replaceStream("processor", first.GetStart().GetInstanceId())
		answered := make(chan error, 1)
		go func() { answered <- s.answer(stream, cfg) }()
		select {
		case err := <-answered:
			return err
		case <-replaced:
			return nil
		}
	}
	id := first.GetStart().GetInstanceId()
	release, err := s.p.claimStream("processor", id, defect("BROKEN_CONCURRENT_PROCESS"))
	if err != nil {
		if defect("BROKEN_SAMPLE_ON_REFUSAL") {
			s.p.mu.Lock()
			emit := s.p.running[id]
			s.p.mu.Unlock()
			if emit != nil {
				emit()
			}
		}
		return err
	}
	defer release()
	if defect("BROKEN_SAMPLE_ON_REFUSAL") {
		locked := &lockedStream{Processor_ProcessServer: stream}
		stream = locked
		s.p.mu.Lock()
		s.p.running[id] = func() {
			for _, device := range cfg.Devices {
				for _, signal := range device.Signals {
					_ = locked.Send(&pb.ProcessorOut{Sample: sample(device.Device, signal.Signal, 1)})
					return
				}
			}
		}
		s.p.mu.Unlock()
		defer func() {
			s.p.mu.Lock()
			delete(s.p.running, id)
			s.p.mu.Unlock()
		}()
	}
	if defect("BROKEN_NEVER_READS") {
		<-stream.Context().Done()
		return stream.Context().Err()
	}
	return s.answer(stream, cfg)
}

// lockedStream serialises the sends of a Process stream that two goroutines emit on.
type lockedStream struct {
	pb.Processor_ProcessServer
	mu sync.Mutex
}

func (l *lockedStream) Send(out *pb.ProcessorOut) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.Processor_ProcessServer.Send(out)
}

// answer reads the records of a started Process stream and answers them until the client ends
// its side.
func (s *processorService) answer(stream pb.Processor_ProcessServer, cfg sdk.ProcessorConfig) error {
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			if defect("BROKEN_NEVER_ENDS") {
				<-stream.Context().Done()
			}
			return nil
		}
		if err != nil {
			return err
		}
		in := msg.GetRecord().GetSample()
		if in == nil || !slices.Contains(cfg.Observe, in.GetDevice()) || os.Getenv("BROKEN_SILENT") == "processor" {
			continue
		}
		for _, device := range cfg.Devices {
			for _, signal := range device.Signals {
				if err := stream.Send(&pb.ProcessorOut{Sample: sample(device.Device, signal.Signal, 1)}); err != nil {
					return err
				}
			}
			if defect("BROKEN_UNDECLARED_SIGNAL") {
				if err := stream.Send(&pb.ProcessorOut{Sample: sample(device.Device, "undeclared", 1)}); err != nil {
					return err
				}
			}
		}
		if defect("BROKEN_ENDS_EARLY") && in.GetValue().GetIntValue() == 3 {
			return nil
		}
	}
}

type outputService struct {
	pb.UnimplementedOutputServer
	p *plugin
}

func (s *outputService) Configure(_ context.Context, req *pb.ConfigureRequest) (*pb.ConfigureResponse, error) {
	return s.p.configure("output", req)
}

func (s *outputService) Send(_ context.Context, req *pb.SendRequest) (*pb.SendResult, error) {
	id := req.GetInstanceId()
	if _, err := s.p.section("output", id); err != nil {
		return nil, err
	}
	if defect("BROKEN_SEND_ERROR") {
		return nil, status.Error(codes.Unavailable, "the destination is down")
	}
	if defect("BROKEN_RETRY_AFTER_BARE") {
		return &pb.SendResult{Outcome: pb.Outcome_OUTCOME_RETRY_AFTER}, nil
	}
	if defect("BROKEN_REJECT_BARE") {
		return &pb.SendResult{Outcome: pb.Outcome_OUTCOME_REJECT_INVALID}, nil
	}
	key := id + "/" + req.GetBatch().GetMessageId()
	s.p.mu.Lock()
	defer s.p.mu.Unlock()
	if s.p.seen[key] {
		if defect("BROKEN_DUPLICATE_ERROR") {
			return nil, status.Error(codes.Internal, "message_id already stored")
		}
		if defect("BROKEN_EXIT_BEFORE_SHUTDOWN") {
			s.p.exitArmed.Store(true)
		}
		return &pb.SendResult{Outcome: pb.Outcome_OUTCOME_DUPLICATE}, nil
	}
	s.p.seen[key] = true
	return &pb.SendResult{Outcome: pb.Outcome_OUTCOME_ACCEPTED}, nil
}
