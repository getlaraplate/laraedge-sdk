package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/getlaraplate/laraedge-sdk/pluginv1"
)

// processorService adapts a [Processor] to the Processor gRPC service.
type processorService struct {
	pb.UnimplementedProcessorServer
	s     *server
	p     Processor
	insts registry
}

func (ps *processorService) Configure(ctx context.Context, req *pb.ConfigureRequest) (*pb.ConfigureResponse, error) {
	return configureRPC(ctx, ps.s.ctx, &ps.insts, req, func(id string, cfg json.RawMessage) []ConfigError {
		return ps.p.Configure(ctx, id, cfg)
	})
}

func (ps *processorService) Process(stream pb.Processor_ProcessServer) error {
	first, err := stream.Recv()
	if errors.Is(err, io.EOF) {
		return status.Error(codes.InvalidArgument, "the stream ended before the start message")
	}
	if err != nil {
		return err
	}
	start := first.GetStart()
	if start == nil {
		return status.Error(codes.InvalidArgument, "the first message of the stream must be start")
	}
	id := start.GetInstanceId()
	inst, err := ps.insts.lookup(id)
	if err != nil {
		return err
	}
	ctx, run, err := inst.acquire(stream.Context(), ps.s.ctx)
	if err != nil {
		return err
	}
	defer inst.release(run)
	defer context.AfterFunc(ps.s.ctx, run.cancel)()

	in := make(chan Record)
	violation := make(chan error, 1)
	go readRecords(ctx, run.cancel, stream, in, violation)

	runErr, sendErr := pump(ctx, run.cancel, ps.s.log, "Processor.Process",
		func(emit func(*pb.ProcessorOut) error) error {
			return ps.p.Process(ctx, id, in, func(s Sample) error {
				wire, err := toProtoSample(s)
				if err != nil {
					return fmt.Errorf("sdk: refusing to emit: %w", err)
				}
				return emit(&pb.ProcessorOut{Sample: wire})
			})
		},
		stream.Send)
	select {
	case err := <-violation:
		return err
	default:
	}
	return streamResult(ctx, runErr, sendErr)
}

// readRecords feeds the records the agent sends to in and closes it when the agent ends its side
// of the stream or the stream is over. A message the protocol does not allow ends the stream with
// INVALID_ARGUMENT and never reaches the author.
func readRecords(ctx context.Context, cancel context.CancelFunc, stream pb.Processor_ProcessServer,
	in chan<- Record, violation chan<- error,
) {
	defer close(in)
	refuse := func(reason string) {
		violation <- status.Error(codes.InvalidArgument, reason)
		cancel()
	}
	for {
		msg, err := stream.Recv()
		if err != nil {
			return
		}
		item, ok := msg.GetMsg().(*pb.ProcessorIn_Record)
		if !ok {
			refuse("a message after the first must carry a record")
			return
		}
		rec, err := fromProtoRecord(item.Record)
		if err != nil {
			refuse("invalid record: " + err.Error())
			return
		}
		select {
		case in <- rec:
		case <-ctx.Done():
			return
		}
	}
}
