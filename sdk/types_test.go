package sdk

import (
	"math"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/getlaraplate/laraedge-sdk/pluginv1"
)

func TestValueConstructors(t *testing.T) {
	if v, err := FloatValue(1.5); err != nil || v.Kind != ValueKindFloat || v.Float != 1.5 {
		t.Errorf("FloatValue(1.5) = %+v, %v", v, err)
	}
	for _, f := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		if _, err := FloatValue(f); err == nil {
			t.Errorf("FloatValue(%v) accepted", f)
		}
	}
	if v := IntValue(-3); v.Kind != ValueKindInt || v.Int != -3 {
		t.Errorf("IntValue = %+v", v)
	}
	if v := BoolValue(true); v.Kind != ValueKindBool || !v.Bool {
		t.Errorf("BoolValue = %+v", v)
	}
	if v := StringValue("x"); v.Kind != ValueKindString || v.Str != "x" {
		t.Errorf("StringValue = %+v", v)
	}
}

func TestSampleRoundTrip(t *testing.T) {
	float, _ := FloatValue(2.25)
	values := []Value{float, IntValue(9), BoolValue(true), StringValue("run")}
	for _, v := range values {
		in := testSample("press-1")
		in.Value = v
		in.Quality = QualityUncertain
		in.Priority = PriorityBulk
		wire, err := toProtoSample(in)
		if err != nil {
			t.Fatalf("toProtoSample(%+v): %v", v, err)
		}
		out, err := fromProtoSample(wire)
		if err != nil {
			t.Fatalf("fromProtoSample: %v", err)
		}
		if out != in {
			t.Errorf("round trip: %+v, want %+v", out, in)
		}
	}
}

func TestSampleValidation(t *testing.T) {
	cases := map[string]func(s *Sample){
		"no device":    func(s *Sample) { s.Device = "" },
		"no signal":    func(s *Sample) { s.Signal = "" },
		"no timestamp": func(s *Sample) { s.TS = time.Time{} },
		"no value":     func(s *Sample) { s.Value = Value{} },
		"nan":          func(s *Sample) { s.Value = Value{Kind: ValueKindFloat, Float: math.NaN()} },
		"no quality":   func(s *Sample) { s.Quality = 0 },
		"bad quality":  func(s *Sample) { s.Quality = 9 },
		"no priority":  func(s *Sample) { s.Priority = 0 },
		"bad priority": func(s *Sample) { s.Priority = 9 },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			s := testSample("d")
			mutate(&s)
			if _, err := toProtoSample(s); err == nil {
				t.Error("an invalid sample was converted")
			}
		})
	}
}

func TestTimestampsAreUTC(t *testing.T) {
	local := time.Date(2026, 10, 23, 14, 0, 0, 0, time.FixedZone("x", 2*3600))
	s := testSample("d")
	s.TS = local
	wire, err := toProtoSample(s)
	if err != nil {
		t.Fatal(err)
	}
	if got := wire.GetTs().AsTime(); !got.Equal(local) {
		t.Errorf("ts = %v, want %v", got, local)
	}
	back, err := fromProtoSample(wire)
	if err != nil {
		t.Fatal(err)
	}
	if back.TS.Location() != time.UTC {
		t.Errorf("decoded timestamp is in %v, want UTC", back.TS.Location())
	}
}

func TestFromProtoRefusesEmptyOneofs(t *testing.T) {
	good := &pb.Sample{
		Device: "d", Signal: "s", Ts: timestamppb.Now(), Value: &pb.Value{Kind: &pb.Value_BoolValue{BoolValue: true}},
		Quality: pb.Quality_QUALITY_UNCERTAIN, Priority: pb.Priority_PRIORITY_BULK,
	}
	noValue := &pb.Sample{Device: "d", Signal: "s", Ts: timestamppb.Now()}
	emptyValue := &pb.Sample{Device: "d", Signal: "s", Ts: timestamppb.Now(), Value: &pb.Value{}}
	if _, err := fromProtoSample(good); err != nil {
		t.Errorf("good sample refused: %v", err)
	}
	for name, s := range map[string]*pb.Sample{"nil": nil, "no value": noValue, "empty value": emptyValue} {
		if _, err := fromProtoSample(s); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	records := map[string]*pb.Record{
		"nil":          nil,
		"empty":        {},
		"nil sample":   {Item: &pb.Record_Sample{}},
		"nil event":    {Item: &pb.Record_DeviceEvent{}},
		"empty sample": {Item: &pb.Record_Sample{Sample: &pb.Sample{}}},
	}
	for name, r := range records {
		if _, err := fromProtoRecord(r); err == nil {
			t.Errorf("record %s: accepted", name)
		}
	}
}

func TestOutcomeConversion(t *testing.T) {
	if _, err := toProtoOutcome(Outcome{}); err == nil {
		t.Error("the zero outcome was converted")
	}
	res, err := toProtoOutcome(Outcome{Kind: OutcomeRetryAfter, RetryAfter: -time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if res.GetRetryAfter().AsDuration() != 0 {
		t.Errorf("negative retry_after sent as %v, want 0", res.GetRetryAfter().AsDuration())
	}
}

func TestFromProtoNarrowsEveryDefinedEnumValue(t *testing.T) {
	for _, q := range []pb.Quality{pb.Quality_QUALITY_GOOD, pb.Quality_QUALITY_UNCERTAIN, pb.Quality_QUALITY_BAD} {
		for _, p := range []pb.Priority{pb.Priority_PRIORITY_CRITICAL, pb.Priority_PRIORITY_BULK} {
			r := withSample(func(s *pb.Sample) { s.Quality, s.Priority = q, p })
			rec, err := fromProtoRecord(r)
			if err != nil {
				t.Fatalf("%v %v refused: %v", q, p, err)
			}
			if int32(rec.Sample.Quality) != int32(q) || int32(rec.Sample.Priority) != int32(p) {
				t.Errorf("%v %v narrowed to %d %d", q, p, rec.Sample.Quality, rec.Sample.Priority)
			}
		}
	}
	dataTypes := map[pb.DataType]string{
		pb.DataType_DATA_TYPE_NUMBER: DataTypeNumber, pb.DataType_DATA_TYPE_BOOLEAN: DataTypeBoolean, pb.DataType_DATA_TYPE_STRING: DataTypeString,
	}
	for _, k := range []pb.EventKind{pb.EventKind_EVENT_KIND_BIRTH, pb.EventKind_EVENT_KIND_DEATH} {
		for d, want := range dataTypes {
			rec, err := fromProtoRecord(birthRecord(k, d))
			if err != nil {
				t.Fatalf("%v %v refused: %v", k, d, err)
			}
			if int32(rec.Event.Kind) != int32(k) || rec.Event.Signals[0].DataType != want {
				t.Errorf("%v %v narrowed to %d %q", k, d, rec.Event.Kind, rec.Event.Signals[0].DataType)
			}
		}
	}
}

func TestFromProtoRefusesEnumsOutsideTheirRange(t *testing.T) {
	for name, r := range enumOutOfRangeRecords() {
		if rec, err := fromProtoRecord(r); err == nil {
			t.Errorf("%s: accepted as %+v", name, rec)
		}
	}
}
