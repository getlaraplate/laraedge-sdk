package sdk

import (
	"context"
	"encoding/json"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/getlaraplate/laraedge-sdk/pluginv1"
)

// outputService adapts an [Output] to the Output gRPC service.
type outputService struct {
	pb.UnimplementedOutputServer
	s     *server
	o     Output
	insts registry
}

func (svc *outputService) Configure(ctx context.Context, req *pb.ConfigureRequest) (*pb.ConfigureResponse, error) {
	return configureRPC(ctx, svc.s.ctx, &svc.insts, req, func(id string, cfg json.RawMessage) []ConfigError {
		return svc.o.Configure(ctx, id, cfg)
	})
}

func (svc *outputService) Send(ctx context.Context, req *pb.SendRequest) (*pb.SendResult, error) {
	id := req.GetInstanceId()
	if _, err := svc.insts.lookup(id); err != nil {
		return nil, err
	}
	batch, err := fromProtoBatch(req.GetBatch())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid batch: "+err.Error())
	}
	res, err := toProtoOutcome(svc.o.Send(ctx, id, batch))
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("the plugin returned an invalid outcome: %v", err))
	}
	return res, nil
}
