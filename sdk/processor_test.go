package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/getlaraplate/laraedge-sdk/manifest"
	pb "github.com/getlaraplate/laraedge-sdk/pluginv1"
)

func processorPlugin(p Processor) Plugin {
	return Plugin{Name: "fake", Version: "1.0.0", Processor: p, Manifest: manifest.Kinds{Processor: configKind()}}
}

func configureProcessor(t *testing.T, c pb.ProcessorClient, instance string) {
	t.Helper()
	resp, err := c.Configure(testCtx(t), &pb.ConfigureRequest{InstanceId: instance, Config: []byte(`{}`)})
	if err != nil || len(resp.GetErrors()) != 0 {
		t.Fatalf("Configure(%s): %v %v", instance, resp, err)
	}
}

func startMsg(instance string) *pb.ProcessorIn {
	return &pb.ProcessorIn{Msg: &pb.ProcessorIn_Start{Start: &pb.ProcessStart{InstanceId: instance}}}
}

func recordMsg(r *pb.Record) *pb.ProcessorIn {
	return &pb.ProcessorIn{Msg: &pb.ProcessorIn_Record{Record: r}}
}

func sampleRecord(device string, v int64) *pb.Record {
	return &pb.Record{Item: &pb.Record_Sample{Sample: &pb.Sample{
		Device: device, Signal: "state", Ts: timestamppb.New(time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)),
		Value: &pb.Value{Kind: &pb.Value_IntValue{IntValue: v}}, Quality: pb.Quality_QUALITY_GOOD, Priority: pb.Priority_PRIORITY_CRITICAL,
	}}}
}

func TestProcessorConfigureErrorsPassThrough(t *testing.T) {
	p := &fakeProcessor{configure: func(context.Context, string, json.RawMessage) []ConfigError {
		return []ConfigError{{Path: "observe", Message: "must list at least one device"}}
	}}
	c := pb.NewProcessorClient(start(t, processorPlugin(p)).conn)
	resp, err := c.Configure(testCtx(t), &pb.ConfigureRequest{InstanceId: "a", Config: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	if e := resp.GetErrors(); len(e) != 1 || e[0].GetPath() != "observe" || e[0].GetMessage() != "must list at least one device" {
		t.Fatalf("errors = %v", e)
	}
	_, err = c.Configure(testCtx(t), &pb.ConfigureRequest{})
	wantCode(t, err, codes.InvalidArgument)
}

func TestProcessorReceivesRecordsAndItsSamplesReachTheClient(t *testing.T) {
	type seen struct {
		samples, events int
		instance        string
	}
	result := make(chan seen, 1)
	p := &fakeProcessor{process: func(_ context.Context, instance string, in <-chan Record, out func(Sample) error) error {
		var s seen
		s.instance = instance
		for r := range in {
			switch {
			case r.Sample != nil:
				s.samples++
				d := *r.Sample
				d.Device = "line-1"
				d.Value = IntValue(r.Sample.Value.Int + 100)
				if err := out(d); err != nil {
					return err
				}
			case r.Event != nil:
				s.events++
			}
		}
		result <- s
		return nil
	}}
	c := pb.NewProcessorClient(start(t, processorPlugin(p)).conn)
	configureProcessor(t, c, "a")
	stream, err := c.Process(testCtx(t))
	if err != nil {
		t.Fatal(err)
	}
	event := &pb.Record{Item: &pb.Record_DeviceEvent{DeviceEvent: &pb.DeviceEvent{Kind: pb.EventKind_EVENT_KIND_DEATH, Device: "press-1"}}}
	for _, m := range []*pb.ProcessorIn{startMsg("a"), recordMsg(sampleRecord("press-1", 1)), recordMsg(event), recordMsg(sampleRecord("press-2", 5))} {
		if err := stream.Send(m); err != nil {
			t.Fatal(err)
		}
	}
	for _, want := range []int64{101, 105} {
		out, err := stream.Recv()
		if err != nil {
			t.Fatal(err)
		}
		if out.GetSample().GetDevice() != "line-1" || out.GetSample().GetValue().GetIntValue() != want {
			t.Errorf("out = %v, want value %d on line-1", out, want)
		}
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("after CloseSend: %v, want EOF", err)
	}
	got := <-result
	if got.samples != 2 || got.events != 1 || got.instance != "a" {
		t.Errorf("processor saw %+v", got)
	}
}

// withSample returns a valid sample record changed by mutate.
func withSample(mutate func(*pb.Sample)) *pb.Record {
	r := sampleRecord("d", 1)
	mutate(r.GetSample())
	return r
}

// birthRecord returns a birth record of one signal; kind and dataType are the enums under test.
func birthRecord(kind pb.EventKind, dataType pb.DataType) *pb.Record {
	return &pb.Record{Item: &pb.Record_DeviceEvent{DeviceEvent: &pb.DeviceEvent{
		Kind: kind, Device: "d", Ts: timestamppb.New(time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)),
		Signals: []*pb.SignalInfo{{Signal: "state", DataType: dataType}},
	}}}
}

// enumOutOfRangeRecords are records whose enums are outside the defined values; the SDK must
// refuse them rather than narrow them into a defined (or the zero) value.
func enumOutOfRangeRecords() map[string]*pb.Record {
	return map[string]*pb.Record{
		"quality unspecified":    withSample(func(s *pb.Sample) { s.Quality = pb.Quality_QUALITY_UNSPECIFIED }),
		"quality above range":    withSample(func(s *pb.Sample) { s.Quality = 4 }),
		"quality 257 (wraps)":    withSample(func(s *pb.Sample) { s.Quality = 257 }),
		"priority unspecified":   withSample(func(s *pb.Sample) { s.Priority = pb.Priority_PRIORITY_UNSPECIFIED }),
		"priority above range":   withSample(func(s *pb.Sample) { s.Priority = 3 }),
		"priority 257 (wraps)":   withSample(func(s *pb.Sample) { s.Priority = 257 }),
		"event kind unknown":     birthRecord(3, pb.DataType_DATA_TYPE_NUMBER),
		"event kind 257":         birthRecord(257, pb.DataType_DATA_TYPE_NUMBER),
		"event kind unspecified": birthRecord(pb.EventKind_EVENT_KIND_UNSPECIFIED, pb.DataType_DATA_TYPE_NUMBER),
		"data type unknown":      birthRecord(pb.EventKind_EVENT_KIND_BIRTH, 4),
		"data type unspecified":  birthRecord(pb.EventKind_EVENT_KIND_BIRTH, pb.DataType_DATA_TYPE_UNSPECIFIED),
	}
}

func TestProcessorRefusesBadStreams(t *testing.T) {
	cases := []struct {
		name     string
		messages []*pb.ProcessorIn
		want     codes.Code
	}{
		{"first message is a record", []*pb.ProcessorIn{recordMsg(sampleRecord("d", 1))}, codes.InvalidArgument},
		{"empty first message", []*pb.ProcessorIn{{}}, codes.InvalidArgument},
		{"start without instance", []*pb.ProcessorIn{startMsg("")}, codes.InvalidArgument},
		{"unconfigured instance", []*pb.ProcessorIn{startMsg("nobody")}, codes.FailedPrecondition},
		{"second start", []*pb.ProcessorIn{startMsg("a"), startMsg("a")}, codes.InvalidArgument},
		{"record without item", []*pb.ProcessorIn{startMsg("a"), recordMsg(&pb.Record{})}, codes.InvalidArgument},
		{"sample without value", []*pb.ProcessorIn{startMsg("a"), recordMsg(&pb.Record{Item: &pb.Record_Sample{Sample: &pb.Sample{Device: "d"}}})}, codes.InvalidArgument},
		{"message without start or record", []*pb.ProcessorIn{startMsg("a"), {}}, codes.InvalidArgument},
	}
	for name, r := range enumOutOfRangeRecords() {
		cases = append(cases, struct {
			name     string
			messages []*pb.ProcessorIn
			want     codes.Code
		}{"record with " + name, []*pb.ProcessorIn{startMsg("a"), recordMsg(r)}, codes.InvalidArgument})
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var records atomic.Int32
			p := &fakeProcessor{process: func(_ context.Context, _ string, in <-chan Record, _ func(Sample) error) error {
				for range in {
					records.Add(1)
				}
				return nil
			}}
			c := pb.NewProcessorClient(start(t, processorPlugin(p)).conn)
			configureProcessor(t, c, "a")
			stream, err := c.Process(testCtx(t))
			if err != nil {
				t.Fatal(err)
			}
			for _, m := range tc.messages {
				if err := stream.Send(m); err != nil {
					break // the server already ended the stream
				}
			}
			_, err = stream.Recv()
			wantCode(t, err, tc.want)
			if n := records.Load(); n != 0 {
				t.Errorf("the plugin received %d records from a bad stream", n)
			}
		})
	}
}

func TestProcessorOnlySamplesAreValidOutput(t *testing.T) {
	result := make(chan error, 1)
	p := &fakeProcessor{process: func(_ context.Context, _ string, in <-chan Record, out func(Sample) error) error {
		bad := testSample("line-1")
		bad.Quality = 0
		result <- out(bad)
		for range in {
		}
		return nil
	}}
	c := pb.NewProcessorClient(start(t, processorPlugin(p)).conn)
	configureProcessor(t, c, "a")
	stream, err := c.Process(testCtx(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(startMsg("a")); err != nil {
		t.Fatal(err)
	}
	if err := <-result; err == nil {
		t.Fatal("an invalid sample was accepted")
	}
	_ = stream.CloseSend()
}

func TestProcessorEndsWhenTheClientCancels(t *testing.T) {
	ended := make(chan error, 1)
	started := make(chan struct{})
	p := &fakeProcessor{process: func(ctx context.Context, instance string, _ <-chan Record, out func(Sample) error) error {
		close(started)
		<-ctx.Done()
		ended <- out(testSample(instance))
		return ctx.Err()
	}}
	c := pb.NewProcessorClient(start(t, processorPlugin(p)).conn)
	configureProcessor(t, c, "a")
	ctx, cancel := context.WithCancel(testCtx(t))
	stream, err := c.Process(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(startMsg("a")); err != nil {
		t.Fatal(err)
	}
	<-started
	cancel()
	if err := <-ended; err == nil {
		t.Error("out succeeded after the stream ended")
	}
}

func TestProcessorSecondStreamOfAnInstanceIsRefused(t *testing.T) {
	started := make(chan struct{}, 2)
	p := &fakeProcessor{process: func(ctx context.Context, _ string, _ <-chan Record, _ func(Sample) error) error {
		started <- struct{}{}
		<-ctx.Done()
		return nil
	}}
	c := pb.NewProcessorClient(start(t, processorPlugin(p)).conn)
	configureProcessor(t, c, "a")
	ctx, cancel := context.WithCancel(testCtx(t))
	defer cancel()
	first, err := c.Process(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_ = first.Send(startMsg("a"))
	<-started
	second, err := c.Process(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_ = second.Send(startMsg("a"))
	_, err = second.Recv()
	wantCode(t, err, codes.AlreadyExists)
}

func TestProcessorPanicGivesInternalAndTheNextStreamWorks(t *testing.T) {
	var calls atomic.Int32
	p := &fakeProcessor{process: func(_ context.Context, _ string, in <-chan Record, _ func(Sample) error) error {
		if calls.Add(1) == 1 {
			panic("boom")
		}
		for range in {
		}
		return nil
	}}
	h := start(t, processorPlugin(p))
	c := pb.NewProcessorClient(h.conn)
	configureProcessor(t, c, "a")

	first, err := c.Process(testCtx(t))
	if err != nil {
		t.Fatal(err)
	}
	_ = first.Send(startMsg("a"))
	_, err = first.Recv()
	wantCode(t, err, codes.Internal)
	if !strings.Contains(h.stderr.String(), "panic") {
		t.Errorf("stderr = %q", h.stderr.String())
	}

	second, err := c.Process(testCtx(t))
	if err != nil {
		t.Fatal(err)
	}
	_ = second.Send(startMsg("a"))
	_ = second.CloseSend()
	if _, err := second.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("second stream: %v, want EOF", err)
	}
}
