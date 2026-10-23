package conformance

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/getlaraplate/laraedge-sdk/pluginv1"
)

// recordsKey is the optional member of a valid processor fixture that lists the records the
// suite sends to Process. The suite removes it before Configure: the agent never sends it.
const recordsKey = "records"

// fixtureRecord is one record of a fixture's records list: exactly one of sample and
// device_event, as sdk.Record holds exactly one of Sample and Event.
type fixtureRecord struct {
	Sample      *fixtureSample `json:"sample"`
	DeviceEvent *fixtureEvent  `json:"device_event"`
}

// fixtureSample is an sdk.Sample. TS is RFC 3339; without it the suite stamps the records in list
// order, one millisecond apart. Quality is good (default), uncertain or bad; priority critical
// (default) or bulk.
type fixtureSample struct {
	Device   string       `json:"device"`
	Signal   string       `json:"signal"`
	TS       *time.Time   `json:"ts"`
	Value    fixtureValue `json:"value"`
	Quality  string       `json:"quality"`
	Priority string       `json:"priority"`
}

// fixtureValue is an sdk.Value: exactly one of its members.
type fixtureValue struct {
	Int    *int64   `json:"int"`
	Float  *float64 `json:"float"`
	Bool   *bool    `json:"bool"`
	String *string  `json:"string"`
}

// fixtureEvent is an sdk.DeviceEvent: kind birth or death, with the signals of a birth.
type fixtureEvent struct {
	Kind    string              `json:"kind"`
	Device  string              `json:"device"`
	Signals []fixtureSignalInfo `json:"signals"`
	TS      *time.Time          `json:"ts"`
}

// fixtureSignalInfo is an sdk.SignalInfo; DataType is number, boolean or string.
type fixtureSignalInfo struct {
	Signal   string `json:"signal"`
	DataType string `json:"data_type"`
	Unit     string `json:"unit"`
}

var (
	qualities  = map[string]pb.Quality{"": pb.Quality_QUALITY_GOOD, "good": pb.Quality_QUALITY_GOOD, "uncertain": pb.Quality_QUALITY_UNCERTAIN, "bad": pb.Quality_QUALITY_BAD}
	priorities = map[string]pb.Priority{"": pb.Priority_PRIORITY_CRITICAL, "critical": pb.Priority_PRIORITY_CRITICAL, "bulk": pb.Priority_PRIORITY_BULK}
	eventKinds = map[string]pb.EventKind{"birth": pb.EventKind_EVENT_KIND_BIRTH, "death": pb.EventKind_EVENT_KIND_DEATH}
	dataTypes  = map[string]pb.DataType{"number": pb.DataType_DATA_TYPE_NUMBER, "boolean": pb.DataType_DATA_TYPE_BOOLEAN, "string": pb.DataType_DATA_TYPE_STRING}
)

// splitRecords removes the records member from a processor section. It returns the section as
// the agent would send it (unchanged when it has no records), the decoded records (nil when
// absent) and the first problem of the list.
func splitRecords(section json.RawMessage) (json.RawMessage, []fixtureRecord, error) {
	var members map[string]json.RawMessage
	if err := json.Unmarshal(section, &members); err != nil {
		return nil, nil, err
	}
	raw, ok := members[recordsKey]
	if !ok {
		return section, nil, nil
	}
	delete(members, recordsKey)
	stripped, err := json.Marshal(members)
	if err != nil {
		return nil, nil, err
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var records []fixtureRecord
	if err := dec.Decode(&records); err != nil {
		return nil, nil, fmt.Errorf("records: %w", err)
	}
	if len(records) == 0 {
		return nil, nil, errors.New("records: the list is empty")
	}
	for i, r := range records {
		if _, err := r.toProto(time.Time{}, i); err != nil {
			return nil, nil, fmt.Errorf("records[%d]: %w", i, err)
		}
	}
	return stripped, records, nil
}

// toProto converts the record; one without a timestamp gets base plus index milliseconds.
func (r fixtureRecord) toProto(base time.Time, index int) (*pb.Record, error) {
	stamp := func(ts *time.Time) *timestamppb.Timestamp {
		if ts != nil {
			return timestamppb.New(*ts)
		}
		return timestamppb.New(base.Add(time.Duration(index) * time.Millisecond))
	}
	switch {
	case (r.Sample == nil) == (r.DeviceEvent == nil):
		return nil, errors.New("must hold exactly one of sample and device_event")
	case r.Sample != nil:
		s := r.Sample
		if s.Device == "" || s.Signal == "" {
			return nil, errors.New("sample: device and signal are required")
		}
		value, err := s.Value.toProto()
		if err != nil {
			return nil, fmt.Errorf("sample.value: %w", err)
		}
		quality, ok := qualities[s.Quality]
		if !ok {
			return nil, errors.New("sample.quality: must be good, uncertain or bad")
		}
		priority, ok := priorities[s.Priority]
		if !ok {
			return nil, errors.New("sample.priority: must be critical or bulk")
		}
		return &pb.Record{Item: &pb.Record_Sample{Sample: &pb.Sample{
			Device: s.Device, Signal: s.Signal, Ts: stamp(s.TS), Value: value, Quality: quality, Priority: priority,
		}}}, nil
	default:
		e := r.DeviceEvent
		kind, ok := eventKinds[e.Kind]
		if !ok {
			return nil, errors.New("device_event.kind: must be birth or death")
		}
		if e.Device == "" {
			return nil, errors.New("device_event.device: is required")
		}
		event := &pb.DeviceEvent{Kind: kind, Device: e.Device, Ts: stamp(e.TS)}
		for i, signal := range e.Signals {
			dataType, ok := dataTypes[signal.DataType]
			if !ok || signal.Signal == "" {
				return nil, fmt.Errorf("device_event.signals[%d]: signal is required, data_type must be number, boolean or string", i)
			}
			event.Signals = append(event.Signals, &pb.SignalInfo{Signal: signal.Signal, DataType: dataType, Unit: signal.Unit})
		}
		return &pb.Record{Item: &pb.Record_DeviceEvent{DeviceEvent: event}}, nil
	}
}

func (v fixtureValue) toProto() (*pb.Value, error) {
	var values []*pb.Value
	if v.Int != nil {
		values = append(values, &pb.Value{Kind: &pb.Value_IntValue{IntValue: *v.Int}})
	}
	if v.Float != nil {
		values = append(values, &pb.Value{Kind: &pb.Value_DoubleValue{DoubleValue: *v.Float}})
	}
	if v.Bool != nil {
		values = append(values, &pb.Value{Kind: &pb.Value_BoolValue{BoolValue: *v.Bool}})
	}
	if v.String != nil {
		values = append(values, &pb.Value{Kind: &pb.Value_StringValue{StringValue: *v.String}})
	}
	if len(values) != 1 {
		return nil, errors.New("must hold exactly one of int, float, bool and string")
	}
	return values[0], nil
}

// syntheticRecords are the records sent when the fixture has none: a birth of every observed
// device with the declared signal names, then three rounds of samples of every signal name on
// every observed device, with the integer values 1, 2 and 3.
func syntheticRecords(base time.Time, observe, signals []string) []*pb.Record {
	var records []*pb.Record
	for _, device := range observe {
		birth := &pb.DeviceEvent{Kind: pb.EventKind_EVENT_KIND_BIRTH, Device: device, Ts: timestamppb.New(base)}
		for _, signal := range signals {
			birth.Signals = append(birth.Signals, &pb.SignalInfo{Signal: signal, DataType: pb.DataType_DATA_TYPE_NUMBER})
		}
		records = append(records, &pb.Record{Item: &pb.Record_DeviceEvent{DeviceEvent: birth}})
	}
	for value := int64(1); value <= 3; value++ {
		for _, device := range observe {
			for _, signal := range signals {
				records = append(records, &pb.Record{Item: &pb.Record_Sample{Sample: &pb.Sample{
					Device: device, Signal: signal, Ts: timestamppb.New(base.Add(time.Duration(value) * time.Millisecond)),
					Value:   &pb.Value{Kind: &pb.Value_IntValue{IntValue: value}},
					Quality: pb.Quality_QUALITY_GOOD, Priority: pb.Priority_PRIORITY_CRITICAL,
				}}})
			}
		}
	}
	return records
}
