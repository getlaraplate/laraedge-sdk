package sdk

import (
	"errors"
	"fmt"
	"math"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/getlaraplate/laraedge-sdk/pluginv1"
)

// Quality is the quality of a sample. The zero value is invalid.
type Quality uint8

// Sample qualities.
const (
	// Good: the value was read and can be trusted.
	Good Quality = iota + 1
	// Uncertain: the value was read but may be stale or inexact.
	Uncertain
	// Bad: the value could not be read.
	Bad
)

// Valid reports whether q is one of the defined qualities.
func (q Quality) Valid() bool { return q >= Good && q <= Bad }

// Priority is the buffer class of a sample. The zero value is invalid.
type Priority uint8

// Priority classes.
const (
	// Critical is for states, alarms, counts, measurements and references.
	Critical Priority = iota + 1
	// Bulk is for process values.
	Bulk
)

// Valid reports whether p is one of the defined priorities.
func (p Priority) Valid() bool { return p == Critical || p == Bulk }

// ValueKind says which field of a [Value] is set. The zero value is invalid.
type ValueKind uint8

// Value kinds.
const (
	KindFloat ValueKind = iota + 1
	KindInt
	KindBool
	KindString
)

// Value holds exactly one of float64, int64, bool or string: the one named by Kind. Build it
// with the constructors.
type Value struct {
	Kind  ValueKind
	Float float64
	Int   int64
	Bool  bool
	Str   string
}

// FloatValue returns a float Value. NaN and infinities are refused so that they never reach the
// wire.
func FloatValue(f float64) (Value, error) {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return Value{}, errors.New("float value is not finite")
	}
	return Value{Kind: KindFloat, Float: f}, nil
}

// IntValue returns an integer Value.
func IntValue(i int64) Value { return Value{Kind: KindInt, Int: i} }

// BoolValue returns a boolean Value.
func BoolValue(b bool) Value { return Value{Kind: KindBool, Bool: b} }

// StringValue returns a string Value.
func StringValue(s string) Value { return Value{Kind: KindString, Str: s} }

// Validate reports whether the value has a known kind and, for floats, is finite.
func (v Value) Validate() error {
	switch v.Kind {
	case KindFloat:
		if math.IsNaN(v.Float) || math.IsInf(v.Float, 0) {
			return errors.New("float value is not finite")
		}
	case KindInt, KindBool, KindString:
	default:
		return fmt.Errorf("invalid value kind %d", uint8(v.Kind))
	}
	return nil
}

// Sample is one reading of one signal of one device.
type Sample struct {
	Device   string
	Signal   string
	TS       time.Time
	Value    Value
	Quality  Quality
	Priority Priority
}

// Validate reports whether the sample can be put on the wire.
func (s Sample) Validate() error {
	switch {
	case s.Device == "":
		return errors.New("sample without device")
	case s.Signal == "":
		return errors.New("sample without signal")
	case s.TS.IsZero():
		return errors.New("sample without timestamp")
	}
	if err := s.Value.Validate(); err != nil {
		return fmt.Errorf("sample value: %w", err)
	}
	if !s.Quality.Valid() {
		return fmt.Errorf("sample quality: invalid value %d", uint8(s.Quality))
	}
	if !s.Priority.Valid() {
		return fmt.Errorf("sample priority: invalid value %d", uint8(s.Priority))
	}
	return nil
}

// Data types of a signal declared in a birth.
const (
	DataTypeNumber  = "number"
	DataTypeBoolean = "boolean"
	DataTypeString  = "string"
)

// SignalInfo describes one signal in a birth.
type SignalInfo struct {
	Signal   string
	DataType string
	Unit     string
}

// EventKind is the kind of a [DeviceEvent]. The zero value is invalid.
type EventKind uint8

// Device event kinds.
const (
	// Birth announces a device and lists its signals.
	Birth EventKind = iota + 1
	// Death says the driver lost the device.
	Death
)

// DeviceEvent is a device lifecycle event. The agent drives births and deaths; plugins receive
// them inside records and never emit them.
type DeviceEvent struct {
	Kind    EventKind
	Device  string
	Signals []SignalInfo
	TS      time.Time
}

// Record is what the buffer stores: exactly one of Sample and Event is set.
type Record struct {
	Sample *Sample
	Event  *DeviceEvent
}

// LossMarker is a range of samples of one class that the agent dropped.
type LossMarker struct {
	Priority Priority
	From     time.Time
	To       time.Time
	Count    uint64
	Reason   string
}

// Batch is what an [Output] delivers: the records of one message and the losses before it.
type Batch struct {
	// MessageID is the UUIDv7 of the batch, the key of idempotency.
	MessageID string
	// SourceSeq is the sequence number of the batch in the source buffer.
	SourceSeq uint64
	Records   []Record
	Losses    []LossMarker
}

// OutcomeKind is how the destination handled a batch. The zero value is invalid and the values
// equal those of the protocol enum.
type OutcomeKind int32

// Outcome kinds, with the behaviour of the agent.
const (
	// OutcomeAccepted: the batch was stored, the agent advances.
	OutcomeAccepted OutcomeKind = iota + 1
	// OutcomeDuplicate: the batch was stored earlier, the agent advances.
	OutcomeDuplicate
	// OutcomeRetryAfter: the agent backs off, honouring Outcome.RetryAfter when set.
	OutcomeRetryAfter
	// OutcomeSplit: the batch is too large, the agent splits it in two.
	OutcomeSplit
	// OutcomeRejectInvalid: the agent drops the batch and records a loss; Outcome.Reason says why.
	OutcomeRejectInvalid
	// OutcomeSuspendAuth: the credentials are refused, the agent suspends sending and keeps buffering.
	OutcomeSuspendAuth
)

// Outcome is the answer of an [Output] to one batch.
type Outcome struct {
	Kind OutcomeKind
	// RetryAfter is used with OutcomeRetryAfter; zero lets the agent use its own backoff.
	RetryAfter time.Duration
	// Reason is used with OutcomeRejectInvalid. It must not contain values from the batch.
	Reason string
}

// ConfigError is one problem found in a configuration. It never contains a value, secrets
// included. It is also an error, so the decoders of this package return it.
type ConfigError struct {
	// Path of the offending key, for example devices[0].points[1].address.
	Path string
	// Message says what is wrong, in words and without the value.
	Message string
}

// Error implements error.
func (e ConfigError) Error() string {
	if e.Path == "" {
		return e.Message
	}
	return e.Path + ": " + e.Message
}

// Node is a node found by [Browser.Browse].
type Node struct {
	// Address is usable as the address of a point.
	Address string
	// Name is the display name.
	Name string
	// Type is the data type as the plugin names it.
	Type string
	// Access is for example read or read_write.
	Access string
}

// errEmptyOneof is the reason given for a message the protocol does not allow.
var errEmptyOneof = errors.New("message has an empty oneof or a missing required field")

func toProtoTime(t time.Time) *timestamppb.Timestamp { return timestamppb.New(t.UTC()) }

func fromProtoTime(ts *timestamppb.Timestamp) (time.Time, error) {
	if ts == nil {
		return time.Time{}, nil
	}
	if err := ts.CheckValid(); err != nil {
		return time.Time{}, err
	}
	return ts.AsTime(), nil
}

func toProtoValue(v Value) (*pb.Value, error) {
	if err := v.Validate(); err != nil {
		return nil, err
	}
	switch v.Kind {
	case KindFloat:
		return &pb.Value{Kind: &pb.Value_DoubleValue{DoubleValue: v.Float}}, nil
	case KindInt:
		return &pb.Value{Kind: &pb.Value_IntValue{IntValue: v.Int}}, nil
	case KindBool:
		return &pb.Value{Kind: &pb.Value_BoolValue{BoolValue: v.Bool}}, nil
	default:
		return &pb.Value{Kind: &pb.Value_StringValue{StringValue: v.Str}}, nil
	}
}

func fromProtoValue(v *pb.Value) (Value, error) {
	switch k := v.GetKind().(type) {
	case *pb.Value_DoubleValue:
		return Value{Kind: KindFloat, Float: k.DoubleValue}, nil
	case *pb.Value_IntValue:
		return Value{Kind: KindInt, Int: k.IntValue}, nil
	case *pb.Value_BoolValue:
		return Value{Kind: KindBool, Bool: k.BoolValue}, nil
	case *pb.Value_StringValue:
		return Value{Kind: KindString, Str: k.StringValue}, nil
	default:
		return Value{}, errEmptyOneof
	}
}

// toProtoSample validates s and converts it.
func toProtoSample(s Sample) (*pb.Sample, error) {
	if err := s.Validate(); err != nil {
		return nil, err
	}
	value, err := toProtoValue(s.Value)
	if err != nil {
		return nil, err
	}
	return &pb.Sample{
		Device:   s.Device,
		Signal:   s.Signal,
		Ts:       toProtoTime(s.TS),
		Value:    value,
		Quality:  pb.Quality(s.Quality),
		Priority: pb.Priority(s.Priority),
	}, nil
}

// fromProtoSample converts a sample received from the agent. It refuses a sample without a value
// kind; it does not judge the agent's data otherwise.
func fromProtoSample(s *pb.Sample) (Sample, error) {
	if s == nil {
		return Sample{}, errEmptyOneof
	}
	value, err := fromProtoValue(s.GetValue())
	if err != nil {
		return Sample{}, err
	}
	ts, err := fromProtoTime(s.GetTs())
	if err != nil {
		return Sample{}, err
	}
	return Sample{
		Device:   s.GetDevice(),
		Signal:   s.GetSignal(),
		TS:       ts,
		Value:    value,
		Quality:  Quality(s.GetQuality()),
		Priority: Priority(s.GetPriority()),
	}, nil
}

func fromProtoEvent(e *pb.DeviceEvent) (DeviceEvent, error) {
	if e == nil {
		return DeviceEvent{}, errEmptyOneof
	}
	ts, err := fromProtoTime(e.GetTs())
	if err != nil {
		return DeviceEvent{}, err
	}
	signals := make([]SignalInfo, 0, len(e.GetSignals()))
	for _, si := range e.GetSignals() {
		info := SignalInfo{Signal: si.GetSignal(), Unit: si.GetUnit()}
		switch si.GetDataType() {
		case pb.DataType_DATA_TYPE_NUMBER:
			info.DataType = DataTypeNumber
		case pb.DataType_DATA_TYPE_BOOLEAN:
			info.DataType = DataTypeBoolean
		case pb.DataType_DATA_TYPE_STRING:
			info.DataType = DataTypeString
		}
		signals = append(signals, info)
	}
	if len(signals) == 0 {
		signals = nil
	}
	return DeviceEvent{Kind: EventKind(e.GetKind()), Device: e.GetDevice(), Signals: signals, TS: ts}, nil
}

// fromProtoRecord converts a record received from the agent; a record without an item is
// refused.
func fromProtoRecord(r *pb.Record) (Record, error) {
	switch item := r.GetItem().(type) {
	case *pb.Record_Sample:
		s, err := fromProtoSample(item.Sample)
		if err != nil {
			return Record{}, err
		}
		return Record{Sample: &s}, nil
	case *pb.Record_DeviceEvent:
		e, err := fromProtoEvent(item.DeviceEvent)
		if err != nil {
			return Record{}, err
		}
		return Record{Event: &e}, nil
	default:
		return Record{}, errEmptyOneof
	}
}

func fromProtoBatch(b *pb.Batch) (Batch, error) {
	if b == nil {
		return Batch{}, errEmptyOneof
	}
	out := Batch{MessageID: b.GetMessageId(), SourceSeq: b.GetSourceSeq()}
	if n := len(b.GetRecords()); n > 0 {
		out.Records = make([]Record, 0, n)
	}
	for i, r := range b.GetRecords() {
		rec, err := fromProtoRecord(r)
		if err != nil {
			return Batch{}, fmt.Errorf("records[%d]: %w", i, err)
		}
		out.Records = append(out.Records, rec)
	}
	for i, l := range b.GetLosses() {
		from, err := fromProtoTime(l.GetFrom())
		if err != nil {
			return Batch{}, fmt.Errorf("losses[%d].from: %w", i, err)
		}
		to, err := fromProtoTime(l.GetTo())
		if err != nil {
			return Batch{}, fmt.Errorf("losses[%d].to: %w", i, err)
		}
		out.Losses = append(out.Losses, LossMarker{
			Priority: Priority(l.GetPriority()), From: from, To: to, Count: l.GetCount(), Reason: l.GetReason(),
		})
	}
	return out, nil
}

// toProtoOutcome converts the answer of an Output. A kind outside the six defined is a
// programming error. A negative RetryAfter is sent as zero.
func toProtoOutcome(o Outcome) (*pb.SendResult, error) {
	if o.Kind < OutcomeAccepted || o.Kind > OutcomeSuspendAuth {
		return nil, fmt.Errorf("invalid outcome kind %d", int32(o.Kind))
	}
	res := &pb.SendResult{Outcome: pb.Outcome(o.Kind), Reason: o.Reason}
	if o.RetryAfter > 0 {
		res.RetryAfter = durationpb.New(o.RetryAfter)
	}
	return res, nil
}

func toProtoConfigErrors(errs []ConfigError) []*pb.ConfigError {
	if len(errs) == 0 {
		return nil
	}
	out := make([]*pb.ConfigError, len(errs))
	for i, e := range errs {
		out[i] = &pb.ConfigError{Path: e.Path, Message: e.Message}
	}
	return out
}
