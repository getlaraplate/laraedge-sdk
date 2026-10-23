package sdk

import (
	"context"
	"encoding/json"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/getlaraplate/laraedge-sdk/manifest"
	pb "github.com/getlaraplate/laraedge-sdk/pluginv1"
)

func outputPlugin(o Output) Plugin {
	return Plugin{Name: "fake", Version: "1.0.0", Output: o, Manifest: manifest.Kinds{Output: configKind()}}
}

func configureOutput(t *testing.T, c pb.OutputClient, instance string) {
	t.Helper()
	resp, err := c.Configure(testCtx(t), &pb.ConfigureRequest{InstanceId: instance, Config: []byte(`{}`)})
	if err != nil || len(resp.GetErrors()) != 0 {
		t.Fatalf("Configure(%s): %v %v", instance, resp, err)
	}
}

func sendRequest(instance, messageID string, records ...*pb.Record) *pb.SendRequest {
	return &pb.SendRequest{InstanceId: instance, Batch: &pb.Batch{MessageId: messageID, SourceSeq: 9, Records: records}}
}

func TestOutputConfigureErrorsPassThrough(t *testing.T) {
	o := &fakeOutput{configure: func(context.Context, string, json.RawMessage) []ConfigError {
		return []ConfigError{{Path: "path", Message: "is required"}}
	}}
	c := pb.NewOutputClient(start(t, outputPlugin(o)).conn)
	resp, err := c.Configure(testCtx(t), &pb.ConfigureRequest{InstanceId: "a", Config: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	if e := resp.GetErrors(); len(e) != 1 || e[0].GetPath() != "path" || e[0].GetMessage() != "is required" {
		t.Fatalf("errors = %v", e)
	}
	// A refused instance is not configured: Send is refused.
	_, err = c.Send(testCtx(t), sendRequest("a", "m1"))
	wantCode(t, err, codes.FailedPrecondition)
}

func TestOutputOutcomesMapOneToOneWithTheProtoEnum(t *testing.T) {
	cases := []struct {
		outcome Outcome
		want    pb.Outcome
	}{
		{Outcome{Kind: OutcomeAccepted}, pb.Outcome_OUTCOME_ACCEPTED},
		{Outcome{Kind: OutcomeDuplicate}, pb.Outcome_OUTCOME_DUPLICATE},
		{Outcome{Kind: OutcomeRetryAfter, RetryAfter: 90 * time.Second}, pb.Outcome_OUTCOME_RETRY_AFTER},
		{Outcome{Kind: OutcomeRetryAfter}, pb.Outcome_OUTCOME_RETRY_AFTER},
		{Outcome{Kind: OutcomeSplit}, pb.Outcome_OUTCOME_SPLIT},
		{Outcome{Kind: OutcomeRejectInvalid, Reason: "field x is not a number"}, pb.Outcome_OUTCOME_REJECT_INVALID},
		{Outcome{Kind: OutcomeSuspendAuth}, pb.Outcome_OUTCOME_SUSPEND_AUTH},
	}
	for _, tc := range cases {
		t.Run(tc.want.String(), func(t *testing.T) {
			o := &fakeOutput{send: func(context.Context, string, Batch) Outcome { return tc.outcome }}
			c := pb.NewOutputClient(start(t, outputPlugin(o)).conn)
			configureOutput(t, c, "a")
			res, err := c.Send(testCtx(t), sendRequest("a", "m1"))
			if err != nil {
				t.Fatal(err)
			}
			if res.GetOutcome() != tc.want {
				t.Errorf("outcome %v, want %v", res.GetOutcome(), tc.want)
			}
			if got := res.GetRetryAfter().AsDuration(); got != tc.outcome.RetryAfter {
				t.Errorf("retry_after %v, want %v", got, tc.outcome.RetryAfter)
			}
			if res.GetReason() != tc.outcome.Reason {
				t.Errorf("reason %q, want %q", res.GetReason(), tc.outcome.Reason)
			}
		})
	}
}

func TestOutcomeKindsEqualTheProtoValues(t *testing.T) {
	pairs := map[OutcomeKind]pb.Outcome{
		OutcomeAccepted:      pb.Outcome_OUTCOME_ACCEPTED,
		OutcomeDuplicate:     pb.Outcome_OUTCOME_DUPLICATE,
		OutcomeRetryAfter:    pb.Outcome_OUTCOME_RETRY_AFTER,
		OutcomeSplit:         pb.Outcome_OUTCOME_SPLIT,
		OutcomeRejectInvalid: pb.Outcome_OUTCOME_REJECT_INVALID,
		OutcomeSuspendAuth:   pb.Outcome_OUTCOME_SUSPEND_AUTH,
	}
	for kind, want := range pairs {
		if int32(kind) != int32(want) {
			t.Errorf("%v = %d, want %d", want, kind, want)
		}
	}
	if OutcomeKind(0) == OutcomeAccepted {
		t.Error("the zero OutcomeKind must be invalid")
	}
}

func TestOutputInvalidOutcomesAreInternalErrors(t *testing.T) {
	cases := map[string]Outcome{
		"zero kind":    {},
		"unknown kind": {Kind: OutcomeKind(99)},
	}
	for name, outcome := range cases {
		t.Run(name, func(t *testing.T) {
			o := &fakeOutput{send: func(context.Context, string, Batch) Outcome { return outcome }}
			c := pb.NewOutputClient(start(t, outputPlugin(o)).conn)
			configureOutput(t, c, "a")
			_, err := c.Send(testCtx(t), sendRequest("a", "m1"))
			wantCode(t, err, codes.Internal)
		})
	}
}

func TestOutputSendDeliversTheBatchAndTheDeadline(t *testing.T) {
	got := make(chan Batch, 1)
	deadline := make(chan bool, 1)
	o := &fakeOutput{send: func(ctx context.Context, instance string, b Batch) Outcome {
		_, ok := ctx.Deadline()
		deadline <- ok
		got <- b
		return Outcome{Kind: OutcomeAccepted}
	}}
	c := pb.NewOutputClient(start(t, outputPlugin(o)).conn)
	configureOutput(t, c, "a")

	event := &pb.Record{Item: &pb.Record_DeviceEvent{DeviceEvent: &pb.DeviceEvent{
		Kind: pb.EventKind_EVENT_KIND_BIRTH, Device: "press-1", Ts: timestamppb.New(time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)),
		Signals: []*pb.SignalInfo{{Signal: "temp", DataType: pb.DataType_DATA_TYPE_NUMBER, Unit: "C"}},
	}}}
	req := sendRequest("a", "0198-id", sampleRecord("press-1", 3), event)
	req.Batch.Losses = []*pb.LossMarker{{
		Priority: pb.Priority_PRIORITY_BULK, Count: 4, Reason: "buffer_full",
		From: timestamppb.New(time.Date(2026, 10, 23, 11, 0, 0, 0, time.UTC)), To: timestamppb.New(time.Date(2026, 10, 23, 11, 5, 0, 0, time.UTC)),
	}}
	ctx, cancel := context.WithTimeout(testCtx(t), 10*time.Second)
	defer cancel()
	if _, err := c.Send(ctx, req); err != nil {
		t.Fatal(err)
	}
	if !<-deadline {
		t.Error("the plugin's Send context carries no deadline")
	}
	b := <-got
	if b.MessageID != "0198-id" || b.SourceSeq != 9 || len(b.Records) != 2 || len(b.Losses) != 1 {
		t.Fatalf("batch = %+v", b)
	}
	if s := b.Records[0].Sample; s == nil || s.Device != "press-1" || s.Value.Kind != KindInt || s.Value.Int != 3 || s.Quality != Good || s.Priority != Critical {
		t.Errorf("record 0 = %+v", b.Records[0])
	}
	if e := b.Records[1].Event; e == nil || e.Kind != Birth || e.Device != "press-1" || len(e.Signals) != 1 || e.Signals[0] != (SignalInfo{Signal: "temp", DataType: DataTypeNumber, Unit: "C"}) {
		t.Errorf("record 1 = %+v", b.Records[1])
	}
	l := b.Losses[0]
	if l.Priority != Bulk || l.Count != 4 || l.Reason != "buffer_full" || !l.From.Equal(time.Date(2026, 10, 23, 11, 0, 0, 0, time.UTC)) || !l.To.Equal(time.Date(2026, 10, 23, 11, 5, 0, 0, time.UTC)) {
		t.Errorf("loss = %+v", l)
	}
}

func TestOutputRefusesGarbageWithoutCallingThePlugin(t *testing.T) {
	var calls atomic.Int32
	o := &fakeOutput{send: func(context.Context, string, Batch) Outcome { calls.Add(1); return Outcome{Kind: OutcomeAccepted} }}
	c := pb.NewOutputClient(start(t, outputPlugin(o)).conn)
	configureOutput(t, c, "a")
	cases := map[string]*pb.SendRequest{
		"no batch":            {InstanceId: "a"},
		"no instance":         sendRequest("", "m"),
		"record without item": sendRequest("a", "m", &pb.Record{}),
		"sample without value": sendRequest("a", "m", &pb.Record{Item: &pb.Record_Sample{Sample: &pb.Sample{
			Device: "d", Signal: "s", Quality: pb.Quality_QUALITY_GOOD, Priority: pb.Priority_PRIORITY_BULK,
		}}}),
		"sample is nil": sendRequest("a", "m", &pb.Record{Item: &pb.Record_Sample{}}),
	}
	for name, req := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := c.Send(testCtx(t), req)
			wantCode(t, err, codes.InvalidArgument)
		})
	}
	if n := calls.Load(); n != 0 {
		t.Errorf("Send was called %d times with garbage", n)
	}
}

func TestOutputPanicInSendGivesInternalAndTheNextSendWorks(t *testing.T) {
	var calls atomic.Int32
	o := &fakeOutput{send: func(_ context.Context, _ string, b Batch) Outcome {
		if calls.Add(1) == 1 {
			panic("boom-" + b.MessageID)
		}
		return Outcome{Kind: OutcomeDuplicate}
	}}
	h := start(t, outputPlugin(o))
	c := pb.NewOutputClient(h.conn)
	configureOutput(t, c, "a")

	_, err := c.Send(testCtx(t), sendRequest("a", "secret-message-id"))
	wantCode(t, err, codes.Internal)
	res, err := c.Send(testCtx(t), sendRequest("a", "second"))
	if err != nil {
		t.Fatalf("the Send after the panic failed: %v", err)
	}
	if res.GetOutcome() != pb.Outcome_OUTCOME_DUPLICATE {
		t.Errorf("outcome = %v", res.GetOutcome())
	}
	log := h.stderr.String()
	if strings.Count(log, "\n") != 1 || !strings.Contains(log, "panic") || !strings.Contains(log, "Send") {
		t.Errorf("stderr = %q, want exactly one line naming the panic and Send", log)
	}
	if strings.Contains(log, "secret-message-id") {
		t.Errorf("stderr leaks request data: %q", log)
	}
}

func TestOutputSendLargerThanTheLimitIsRefused(t *testing.T) {
	o := &fakeOutput{}
	c := pb.NewOutputClient(start(t, outputPlugin(o)).conn)
	configureOutput(t, c, "a")
	big := strings.Repeat("x", 17<<20)
	_, err := c.Send(testCtx(t), sendRequest("a", big))
	wantCode(t, err, codes.ResourceExhausted)
}

func TestOutputInstancesAreIndependent(t *testing.T) {
	o := &fakeOutput{send: func(_ context.Context, instance string, _ Batch) Outcome {
		if instance == "a" {
			return Outcome{Kind: OutcomeSuspendAuth}
		}
		return Outcome{Kind: OutcomeAccepted}
	}}
	c := pb.NewOutputClient(start(t, outputPlugin(o)).conn)
	configureOutput(t, c, "a")
	configureOutput(t, c, "b")
	for id, want := range map[string]pb.Outcome{"a": pb.Outcome_OUTCOME_SUSPEND_AUTH, "b": pb.Outcome_OUTCOME_ACCEPTED} {
		res, err := c.Send(testCtx(t), sendRequest(id, "m"))
		if err != nil || res.GetOutcome() != want {
			t.Errorf("instance %s: %v %v, want %v", id, res, err, want)
		}
	}
	_, err := c.Send(testCtx(t), sendRequest("c", "m"))
	wantCode(t, err, codes.FailedPrecondition)
}
