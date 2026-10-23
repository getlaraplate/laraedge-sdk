package sdk

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/getlaraplate/laraedge-sdk/pluginv1"
)

// inputService adapts an [Input] to the Input gRPC service.
type inputService struct {
	pb.UnimplementedInputServer
	s     *server
	in    Input
	insts registry
}

func (is *inputService) Configure(ctx context.Context, req *pb.ConfigureRequest) (*pb.ConfigureResponse, error) {
	return configureRPC(ctx, &is.insts, req, func(id string, cfg json.RawMessage) []ConfigError {
		return is.in.Configure(ctx, id, cfg)
	})
}

func (is *inputService) Subscribe(req *pb.SubscribeRequest, stream pb.Input_SubscribeServer) error {
	id := req.GetInstanceId()
	inst, err := is.insts.lookup(id)
	if err != nil {
		return err
	}
	ctx, run, err := inst.acquire(stream.Context())
	if err != nil {
		return err
	}
	defer func() {
		if inst.takeStop() {
			is.stop(id)
		}
		inst.release(run)
	}()
	defer context.AfterFunc(is.s.ctx, run.cancel)()

	runErr, sendErr := pump(ctx, run.cancel, is.s.log, "Input.Run",
		func(emit func(*pb.InputEvent) error) error { return is.in.Run(ctx, id, &inputEmitter{emit: emit}) },
		stream.Send)
	return streamResult(ctx, runErr, sendErr)
}

// stop calls the author's Stop, recovering a panic.
func (is *inputService) stop(id string) {
	defer is.s.log.recovered("Input.Stop", nil)
	if err := is.in.Stop(id); err != nil {
		is.s.log.printf("Input.Stop failed: %v", err)
	}
}

// stopAll is the Stop pass of Shutdown: every stream is cancelled, and every instance that is not
// stopped yet is stopped once its Run has returned (the Subscribe handler then stops it itself),
// all within ctx. An instance whose Run outlasts ctx is not stopped: the process is about to exit.
func (is *inputService) stopAll(ctx context.Context) {
	var wg sync.WaitGroup
	for id, inst := range is.insts.snapshot() {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if run := inst.cancelActive(); run != nil {
				select {
				case <-run.done:
				case <-ctx.Done():
					return
				}
			}
			if inst.takeStop() {
				is.stop(id)
			}
		}()
	}
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
	}
}

func (is *inputService) Probe(ctx context.Context, req *pb.ProbeRequest) (*pb.ProbeResponse, error) {
	id := req.GetInstanceId()
	if _, err := is.insts.lookup(id); err != nil {
		return nil, err
	}
	samples, err := is.in.Probe(ctx, id)
	if err != nil {
		return nil, rpcError(err)
	}
	out := &pb.ProbeResponse{Samples: make([]*pb.Sample, 0, len(samples))}
	for i, s := range samples {
		wire, err := toProtoSample(s)
		if err != nil {
			return nil, status.Error(codes.Internal, fmt.Sprintf("the plugin returned an invalid sample (index %d): %v", i, err))
		}
		out.Samples = append(out.Samples, wire)
	}
	return out, nil
}

func (is *inputService) Browse(ctx context.Context, req *pb.BrowseRequest) (*pb.BrowseResponse, error) {
	browser, ok := is.in.(Browser)
	if !ok {
		return nil, status.Error(codes.Unimplemented, "this plugin does not browse")
	}
	id := req.GetInstanceId()
	if _, err := is.insts.lookup(id); err != nil {
		return nil, err
	}
	depth := int(min(req.GetDepth(), math.MaxInt32))
	nodes, err := browser.Browse(ctx, id, req.GetNode(), depth)
	if err != nil {
		return nil, rpcError(err)
	}
	out := &pb.BrowseResponse{Nodes: make([]*pb.Node, 0, len(nodes))}
	for _, n := range nodes {
		out.Nodes = append(out.Nodes, &pb.Node{Address: n.Address, Name: n.Name, Type: n.Type, Access: n.Access})
	}
	return out, nil
}

// inputEmitter implements [InputEmitter] over the delivery function of pump.
type inputEmitter struct {
	emit func(*pb.InputEvent) error
}

func (e *inputEmitter) Sample(s Sample) error {
	wire, err := toProtoSample(s)
	if err != nil {
		return fmt.Errorf("sdk: refusing to emit: %w", err)
	}
	return e.emit(&pb.InputEvent{Event: &pb.InputEvent_Sample{Sample: wire}})
}

func (e *inputEmitter) Answered(device string) error {
	if device == "" {
		return fmt.Errorf("sdk: refusing to emit: answered without device")
	}
	return e.emit(&pb.InputEvent{Event: &pb.InputEvent_Answered{Answered: &pb.Answered{Device: device}}})
}

func (e *inputEmitter) Health(connected bool, readError string) error {
	switch readError {
	case "", "timeout", "connection", "exception", "decode":
	default:
		return fmt.Errorf("sdk: refusing to emit: read error kind must be timeout, connection, exception or decode")
	}
	return e.emit(&pb.InputEvent{Event: &pb.InputEvent_Health{Health: &pb.Health{Connected: connected, ReadError: readError}}})
}
