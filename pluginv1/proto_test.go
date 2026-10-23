package pluginv1_test

import (
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	pluginv1 "github.com/getlaraplate/laraedge-sdk/pluginv1"
)

// service returns the named service of a generated file, failing the build of
// the test table loudly when it is missing.
func service(file protoreflect.FileDescriptor, name protoreflect.Name) protoreflect.ServiceDescriptor {
	svc := file.Services().ByName(name)
	if svc == nil {
		panic("service " + string(name) + " not found in " + file.Path())
	}

	return svc
}

func ts() *timestamppb.Timestamp {
	return timestamppb.New(time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC))
}

func sample() *pluginv1.Sample {
	return &pluginv1.Sample{
		Device:   "press-1",
		Signal:   "temperature",
		Ts:       ts(),
		Value:    &pluginv1.Value{Kind: &pluginv1.Value_DoubleValue{DoubleValue: 21.5}},
		Quality:  pluginv1.Quality_QUALITY_GOOD,
		Priority: pluginv1.Priority_PRIORITY_BULK,
	}
}

func deviceEvent() *pluginv1.DeviceEvent {
	return &pluginv1.DeviceEvent{
		Kind:   pluginv1.EventKind_EVENT_KIND_BIRTH,
		Device: "press-1",
		Ts:     ts(),
		Signals: []*pluginv1.SignalInfo{
			{Signal: "temperature", DataType: pluginv1.DataType_DATA_TYPE_NUMBER, Unit: "C"},
		},
	}
}

// TestMessagesRoundTrip marshals and unmarshals one of each message and checks
// that its full descriptor name is the one of protocol laraplate.edge.plugin.v1.
func TestMessagesRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		msg      proto.Message
		fresh    func() proto.Message
		wantName protoreflect.FullName
	}{
		{"value", &pluginv1.Value{Kind: &pluginv1.Value_IntValue{IntValue: -7}}, func() proto.Message { return &pluginv1.Value{} }, "laraplate.edge.plugin.v1.Value"},
		{"sample", sample(), func() proto.Message { return &pluginv1.Sample{} }, "laraplate.edge.plugin.v1.Sample"},
		{"signal info", &pluginv1.SignalInfo{Signal: "s", DataType: pluginv1.DataType_DATA_TYPE_BOOLEAN}, func() proto.Message { return &pluginv1.SignalInfo{} }, "laraplate.edge.plugin.v1.SignalInfo"},
		{"device event", deviceEvent(), func() proto.Message { return &pluginv1.DeviceEvent{} }, "laraplate.edge.plugin.v1.DeviceEvent"},
		{"record sample", &pluginv1.Record{Item: &pluginv1.Record_Sample{Sample: sample()}}, func() proto.Message { return &pluginv1.Record{} }, "laraplate.edge.plugin.v1.Record"},
		{"record device event", &pluginv1.Record{Item: &pluginv1.Record_DeviceEvent{DeviceEvent: deviceEvent()}}, func() proto.Message { return &pluginv1.Record{} }, "laraplate.edge.plugin.v1.Record"},
		{"loss marker", &pluginv1.LossMarker{Priority: pluginv1.Priority_PRIORITY_CRITICAL, From: ts(), To: ts(), Count: 12, Reason: "buffer_full"}, func() proto.Message { return &pluginv1.LossMarker{} }, "laraplate.edge.plugin.v1.LossMarker"},
		{"config error", &pluginv1.ConfigError{Path: "devices[0].points[1].address", Message: "unknown register"}, func() proto.Message { return &pluginv1.ConfigError{} }, "laraplate.edge.plugin.v1.ConfigError"},
		{"configure request", &pluginv1.ConfigureRequest{InstanceId: "in-1", Config: []byte(`{"id":"in-1"}`)}, func() proto.Message { return &pluginv1.ConfigureRequest{} }, "laraplate.edge.plugin.v1.ConfigureRequest"},
		{"configure response", &pluginv1.ConfigureResponse{Errors: []*pluginv1.ConfigError{{Path: "p", Message: "m"}}}, func() proto.Message { return &pluginv1.ConfigureResponse{} }, "laraplate.edge.plugin.v1.ConfigureResponse"},
		{"info response", &pluginv1.InfoResponse{Name: "naxos", Version: "1.2.3", ProtocolVersions: []string{"v1"}, Manifest: []byte(`{}`)}, func() proto.Message { return &pluginv1.InfoResponse{} }, "laraplate.edge.plugin.v1.InfoResponse"},
		{"shutdown request", &pluginv1.ShutdownRequest{Deadline: durationpb.New(5 * time.Second)}, func() proto.Message { return &pluginv1.ShutdownRequest{} }, "laraplate.edge.plugin.v1.ShutdownRequest"},
		{"input event sample", &pluginv1.InputEvent{Event: &pluginv1.InputEvent_Sample{Sample: sample()}}, func() proto.Message { return &pluginv1.InputEvent{} }, "laraplate.edge.plugin.v1.InputEvent"},
		{"input event answered", &pluginv1.InputEvent{Event: &pluginv1.InputEvent_Answered{Answered: &pluginv1.Answered{Device: "press-1"}}}, func() proto.Message { return &pluginv1.InputEvent{} }, "laraplate.edge.plugin.v1.InputEvent"},
		{"input event health", &pluginv1.InputEvent{Event: &pluginv1.InputEvent_Health{Health: &pluginv1.Health{Connected: false, ReadError: "timeout"}}}, func() proto.Message { return &pluginv1.InputEvent{} }, "laraplate.edge.plugin.v1.InputEvent"},
		{"probe response", &pluginv1.ProbeResponse{Samples: []*pluginv1.Sample{sample()}}, func() proto.Message { return &pluginv1.ProbeResponse{} }, "laraplate.edge.plugin.v1.ProbeResponse"},
		{"browse response", &pluginv1.BrowseResponse{Nodes: []*pluginv1.Node{{Address: "ns=2;s=a", Name: "a", Type: "number", Access: "read"}}}, func() proto.Message { return &pluginv1.BrowseResponse{} }, "laraplate.edge.plugin.v1.BrowseResponse"},
		{"processor in start", &pluginv1.ProcessorIn{Msg: &pluginv1.ProcessorIn_Start{Start: &pluginv1.ProcessStart{InstanceId: "p-1"}}}, func() proto.Message { return &pluginv1.ProcessorIn{} }, "laraplate.edge.plugin.v1.ProcessorIn"},
		{"processor in record", &pluginv1.ProcessorIn{Msg: &pluginv1.ProcessorIn_Record{Record: &pluginv1.Record{Item: &pluginv1.Record_Sample{Sample: sample()}}}}, func() proto.Message { return &pluginv1.ProcessorIn{} }, "laraplate.edge.plugin.v1.ProcessorIn"},
		{"processor out", &pluginv1.ProcessorOut{Sample: sample()}, func() proto.Message { return &pluginv1.ProcessorOut{} }, "laraplate.edge.plugin.v1.ProcessorOut"},
		{"send request", &pluginv1.SendRequest{InstanceId: "o-1", Batch: &pluginv1.Batch{MessageId: "m", SourceSeq: 9, Records: []*pluginv1.Record{{Item: &pluginv1.Record_Sample{Sample: sample()}}}, Losses: []*pluginv1.LossMarker{{Priority: pluginv1.Priority_PRIORITY_BULK, From: ts(), To: ts(), Count: 1, Reason: "buffer_full"}}}}, func() proto.Message { return &pluginv1.SendRequest{} }, "laraplate.edge.plugin.v1.SendRequest"},
		{"send result", &pluginv1.SendResult{Outcome: pluginv1.Outcome_OUTCOME_RETRY_AFTER, RetryAfter: durationpb.New(30 * time.Second), Reason: "rate limited"}, func() proto.Message { return &pluginv1.SendResult{} }, "laraplate.edge.plugin.v1.SendResult"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := tt.msg.ProtoReflect().Descriptor().FullName(); got != tt.wantName {
				t.Fatalf("full name = %q, want %q", got, tt.wantName)
			}

			wire, err := proto.Marshal(tt.msg)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}

			got := tt.fresh()
			if err := proto.Unmarshal(wire, got); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}

			if !proto.Equal(tt.msg, got) {
				t.Fatalf("round trip changed the message:\n got %v\nwant %v", got, tt.msg)
			}
		})
	}
}

// TestServiceNames pins the full service names and their methods.
func TestServiceNames(t *testing.T) {
	t.Parallel()

	tests := []struct {
		desc        protoreflect.ServiceDescriptor
		wantName    protoreflect.FullName
		wantMethods []protoreflect.Name
	}{
		{service(pluginv1.File_laraplate_edge_plugin_v1_plugin_proto, "Plugin"), "laraplate.edge.plugin.v1.Plugin", []protoreflect.Name{"Info", "Shutdown"}},
		{service(pluginv1.File_laraplate_edge_plugin_v1_input_proto, "Input"), "laraplate.edge.plugin.v1.Input", []protoreflect.Name{"Configure", "Subscribe", "Probe", "Browse"}},
		{service(pluginv1.File_laraplate_edge_plugin_v1_processor_proto, "Processor"), "laraplate.edge.plugin.v1.Processor", []protoreflect.Name{"Configure", "Process"}},
		{service(pluginv1.File_laraplate_edge_plugin_v1_output_proto, "Output"), "laraplate.edge.plugin.v1.Output", []protoreflect.Name{"Configure", "Send"}},
	}

	for _, tt := range tests {
		t.Run(string(tt.wantName), func(t *testing.T) {
			t.Parallel()

			if got := tt.desc.FullName(); got != tt.wantName {
				t.Fatalf("service name = %q, want %q", got, tt.wantName)
			}

			if tt.desc.Methods().Len() != len(tt.wantMethods) {
				t.Fatalf("%s has %d methods, want %d", tt.wantName, tt.desc.Methods().Len(), len(tt.wantMethods))
			}

			for i, want := range tt.wantMethods {
				if got := tt.desc.Methods().Get(i).Name(); got != want {
					t.Errorf("method %d = %q, want %q", i, got, want)
				}
			}
		})
	}
}

// TestStreamingShapes pins which methods stream: Subscribe is a server stream
// and Process is bidirectional; every other method is unary.
func TestStreamingShapes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		service      protoreflect.ServiceDescriptor
		method       protoreflect.Name
		wantClient   bool
		wantServer   bool
		wantRequest  protoreflect.FullName
		wantResponse protoreflect.FullName
	}{
		{service(pluginv1.File_laraplate_edge_plugin_v1_input_proto, "Input"), "Subscribe", false, true, "laraplate.edge.plugin.v1.SubscribeRequest", "laraplate.edge.plugin.v1.InputEvent"},
		{service(pluginv1.File_laraplate_edge_plugin_v1_processor_proto, "Processor"), "Process", true, true, "laraplate.edge.plugin.v1.ProcessorIn", "laraplate.edge.plugin.v1.ProcessorOut"},
		{service(pluginv1.File_laraplate_edge_plugin_v1_output_proto, "Output"), "Send", false, false, "laraplate.edge.plugin.v1.SendRequest", "laraplate.edge.plugin.v1.SendResult"},
		{service(pluginv1.File_laraplate_edge_plugin_v1_input_proto, "Input"), "Probe", false, false, "laraplate.edge.plugin.v1.ProbeRequest", "laraplate.edge.plugin.v1.ProbeResponse"},
		{service(pluginv1.File_laraplate_edge_plugin_v1_input_proto, "Input"), "Browse", false, false, "laraplate.edge.plugin.v1.BrowseRequest", "laraplate.edge.plugin.v1.BrowseResponse"},
	}

	for _, tt := range tests {
		t.Run(string(tt.service.Name())+"."+string(tt.method), func(t *testing.T) {
			t.Parallel()

			m := tt.service.Methods().ByName(tt.method)
			if m == nil {
				t.Fatalf("method %s not found", tt.method)
			}

			if m.IsStreamingClient() != tt.wantClient || m.IsStreamingServer() != tt.wantServer {
				t.Errorf("streaming client/server = %v/%v, want %v/%v", m.IsStreamingClient(), m.IsStreamingServer(), tt.wantClient, tt.wantServer)
			}

			if got := m.Input().FullName(); got != tt.wantRequest {
				t.Errorf("request = %q, want %q", got, tt.wantRequest)
			}

			if got := m.Output().FullName(); got != tt.wantResponse {
				t.Errorf("response = %q, want %q", got, tt.wantResponse)
			}
		})
	}
}

// TestEnumValues pins the numbers of the enums: the zero value is always the
// unspecified one, and the outcomes follow the order of the protocol.
func TestEnumValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		got  int32
		want int32
	}{
		{"OUTCOME_UNSPECIFIED", int32(pluginv1.Outcome_OUTCOME_UNSPECIFIED), 0},
		{"OUTCOME_ACCEPTED", int32(pluginv1.Outcome_OUTCOME_ACCEPTED), 1},
		{"OUTCOME_DUPLICATE", int32(pluginv1.Outcome_OUTCOME_DUPLICATE), 2},
		{"OUTCOME_RETRY_AFTER", int32(pluginv1.Outcome_OUTCOME_RETRY_AFTER), 3},
		{"OUTCOME_SPLIT", int32(pluginv1.Outcome_OUTCOME_SPLIT), 4},
		{"OUTCOME_REJECT_INVALID", int32(pluginv1.Outcome_OUTCOME_REJECT_INVALID), 5},
		{"OUTCOME_SUSPEND_AUTH", int32(pluginv1.Outcome_OUTCOME_SUSPEND_AUTH), 6},
		{"QUALITY_UNSPECIFIED", int32(pluginv1.Quality_QUALITY_UNSPECIFIED), 0},
		{"QUALITY_GOOD", int32(pluginv1.Quality_QUALITY_GOOD), 1},
		{"QUALITY_UNCERTAIN", int32(pluginv1.Quality_QUALITY_UNCERTAIN), 2},
		{"QUALITY_BAD", int32(pluginv1.Quality_QUALITY_BAD), 3},
		{"PRIORITY_UNSPECIFIED", int32(pluginv1.Priority_PRIORITY_UNSPECIFIED), 0},
		{"PRIORITY_CRITICAL", int32(pluginv1.Priority_PRIORITY_CRITICAL), 1},
		{"PRIORITY_BULK", int32(pluginv1.Priority_PRIORITY_BULK), 2},
		{"DATA_TYPE_UNSPECIFIED", int32(pluginv1.DataType_DATA_TYPE_UNSPECIFIED), 0},
		{"DATA_TYPE_NUMBER", int32(pluginv1.DataType_DATA_TYPE_NUMBER), 1},
		{"DATA_TYPE_BOOLEAN", int32(pluginv1.DataType_DATA_TYPE_BOOLEAN), 2},
		{"DATA_TYPE_STRING", int32(pluginv1.DataType_DATA_TYPE_STRING), 3},
		{"EVENT_KIND_UNSPECIFIED", int32(pluginv1.EventKind_EVENT_KIND_UNSPECIFIED), 0},
		{"EVENT_KIND_BIRTH", int32(pluginv1.EventKind_EVENT_KIND_BIRTH), 1},
		{"EVENT_KIND_DEATH", int32(pluginv1.EventKind_EVENT_KIND_DEATH), 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if tt.got != tt.want {
				t.Fatalf("%s = %d, want %d", tt.name, tt.got, tt.want)
			}
		})
	}
}

// TestEnumNames pins the full names of the enum types and the wire name of a
// value, so a rename of the protocol shows up as a test failure.
func TestEnumNames(t *testing.T) {
	t.Parallel()

	if got := pluginv1.Outcome_OUTCOME_SUSPEND_AUTH.Descriptor().FullName(); got != "laraplate.edge.plugin.v1.Outcome" {
		t.Errorf("outcome enum name = %q", got)
	}

	if got := pluginv1.Outcome_OUTCOME_SUSPEND_AUTH.String(); got != "OUTCOME_SUSPEND_AUTH" {
		t.Errorf("outcome string = %q", got)
	}
}
