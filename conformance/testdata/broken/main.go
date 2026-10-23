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
//
// A section is valid unless it has the top-level member "refuse": true.
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
	p.server = grpc.NewServer()
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
	active   map[string]bool            // input instances with a running stream
	seen     map[string]bool            // output instance + "/" + message_id

	shutdownOnce sync.Once
	shutdown     chan struct{}
}

func newPlugin() *plugin {
	return &plugin{
		sections: map[string]json.RawMessage{},
		active:   map[string]bool{},
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
		Refuse bool `json:"refuse"`
	}
	if err := json.Unmarshal(req.GetConfig(), &section); err != nil {
		section.Refuse = true
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if section.Refuse && os.Getenv("BROKEN_ACCEPT_INVALID") != kind {
		delete(p.sections, kind+"/"+id)
		return &pb.ConfigureResponse{Errors: []*pb.ConfigError{{Path: "refuse", Message: "the section asks to be refused"}}}, nil
	}
	p.sections[kind+"/"+id] = append(json.RawMessage(nil), req.GetConfig()...)
	return &pb.ConfigureResponse{}, nil
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
	s.p.mu.Lock()
	if s.p.active[id] {
		s.p.mu.Unlock()
		return status.Error(codes.AlreadyExists, "the instance already has a running stream")
	}
	s.p.active[id] = true
	s.p.mu.Unlock()
	defer func() {
		s.p.mu.Lock()
		delete(s.p.active, id)
		s.p.mu.Unlock()
	}()

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
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
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
	key := id + "/" + req.GetBatch().GetMessageId()
	s.p.mu.Lock()
	defer s.p.mu.Unlock()
	if s.p.seen[key] {
		if defect("BROKEN_DUPLICATE_ERROR") {
			return nil, status.Error(codes.Internal, "message_id already stored")
		}
		return &pb.SendResult{Outcome: pb.Outcome_OUTCOME_DUPLICATE}, nil
	}
	s.p.seen[key] = true
	return &pb.SendResult{Outcome: pb.Outcome_OUTCOME_ACCEPTED}, nil
}
