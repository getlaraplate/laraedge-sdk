package sdk

import (
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestDecodeInput(t *testing.T) {
	cfg := []byte(`{
		"id": "line-1",
		"driver": "plugin:fake",
		"future_field": {"a": [1, 2]},
		"connection": {"host": "10.0.0.5", "password": "resolved-secret"},
		"devices": [
			{"device": "press-07", "extra": true, "points": [
				{"signal": "temp", "address": "counter:1", "read": {"poll": "1500ms"}, "type": "int32", "byte_order": "ABCD", "unit": "C", "priority": "bulk"},
				{"signal": "alarm", "address": "counter:2", "read": "event"},
				{"signal": "level", "address": "counter:3", "read": {"subscribe": {"sampling": "1s", "queue": 4}}},
				{"signal": "plain", "address": "counter:4", "read": "subscribe"}
			]},
			{"device": "press-08", "points": []}
		]
	}`)
	got, err := DecodeInput(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != "line-1" || len(got.Devices) != 2 {
		t.Fatalf("config = %+v", got)
	}
	if !strings.Contains(string(got.Connection), `"host"`) {
		t.Errorf("connection = %s", got.Connection)
	}
	points := got.Devices[0].Points
	if got.Devices[0].Device != "press-07" || len(points) != 4 || len(got.Devices[1].Points) != 0 {
		t.Fatalf("devices = %+v", got.Devices)
	}
	want := Point{Signal: "temp", Address: "counter:1", Type: "int32", ByteOrder: "ABCD", Unit: "C", Priority: "bulk", Read: Read{Mode: "poll", Interval: 1500 * time.Millisecond}}
	if !reflect.DeepEqual(points[0], want) {
		t.Errorf("point 0 = %+v, want %+v", points[0], want)
	}
	if points[1].Read.Mode != "event" || points[3].Read.Mode != "subscribe" || points[3].Read.Subscribe != nil {
		t.Errorf("string read modes = %+v %+v", points[1].Read, points[3].Read)
	}
	if r := points[2].Read; r.Mode != "subscribe" || !strings.Contains(string(r.Subscribe), `"sampling"`) {
		t.Errorf("subscribe read = %+v", r)
	}
}

func TestDecodeInputWithoutOptionalParts(t *testing.T) {
	got, err := DecodeInput([]byte(`{"id": "x", "devices": []}`))
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != "x" || got.Connection != nil || len(got.Devices) != 0 {
		t.Errorf("config = %+v", got)
	}
}

func TestDecodeProcessor(t *testing.T) {
	got, err := DecodeProcessor([]byte(`{
		"id": "line-1-state", "plugin": "oee", "death_tolerance": "30s",
		"observe": ["press-07", "oven-1"],
		"devices": [{"device": "line-1", "signals": [{"signal": "state", "type": "int", "priority": "critical"}]}],
		"config": {"mode": "worst"}
	}`))
	if err != nil {
		t.Fatal(err)
	}
	want := ProcessorConfig{
		ID: "line-1-state", Observe: []string{"press-07", "oven-1"},
		Devices: []DerivedDevice{{Device: "line-1", Signals: []DerivedSignal{{Signal: "state", Type: "int", Priority: "critical"}}}},
		Config:  got.Config,
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("config = %+v, want %+v", got, want)
	}
	if !strings.Contains(string(got.Config), `"worst"`) {
		t.Errorf("config = %s", got.Config)
	}
}

func TestDecodeOutput(t *testing.T) {
	got, err := DecodeOutput([]byte(`{"id": "sink", "type": "plugin:jsonl", "route": "x", "config": {"path": "/tmp/f"}}`))
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != "sink" || !strings.Contains(string(got.Config), "/tmp/f") {
		t.Errorf("config = %+v", got)
	}
}

func TestDecodeErrorsNameThePathAndNeverTheValue(t *testing.T) {
	const secret = "s3cr3t-value"
	cases := []struct {
		name     string
		decode   func() error
		wantPath string
	}{
		{"input not json", func() error { _, err := DecodeInput([]byte(`{` + secret)); return err }, ""},
		{"input not an object", func() error { _, err := DecodeInput([]byte(`["` + secret + `"]`)); return err }, ""},
		{"id not a string", func() error { _, err := DecodeInput([]byte(`{"id": 5}`)); return err }, "id"},
		{"devices not an array", func() error { _, err := DecodeInput([]byte(`{"devices": "` + secret + `"}`)); return err }, "devices"},
		{"device not an object", func() error { _, err := DecodeInput([]byte(`{"devices": ["` + secret + `"]}`)); return err }, "devices[0]"},
		{"signal not a string", func() error {
			_, err := DecodeInput([]byte(`{"devices": [{"device": "d", "points": [{"signal": "a"}, {"signal": {"x": "` + secret + `"}}]}]}`))
			return err
		}, "devices[0].points[1].signal"},
		{"read of the wrong type", func() error {
			_, err := DecodeInput([]byte(`{"devices": [{"device": "d", "points": [{"signal": "a", "read": 7}]}]}`))
			return err
		}, "devices[0].points[0].read"},
		{"read string unknown", func() error {
			_, err := DecodeInput([]byte(`{"devices": [{"device": "d", "points": [{"signal": "a", "read": "` + secret + `"}]}]}`))
			return err
		}, "devices[0].points[0].read"},
		{"poll not a duration", func() error {
			_, err := DecodeInput([]byte(`{"devices": [{"device": "d", "points": [{"signal": "a", "read": {"poll": "` + secret + `"}}]}]}`))
			return err
		}, "devices[0].points[0].read.poll"},
		{"poll not positive", func() error {
			_, err := DecodeInput([]byte(`{"devices": [{"device": "d", "points": [{"signal": "a", "read": {"poll": "0s"}}]}]}`))
			return err
		}, "devices[0].points[0].read.poll"},
		{"read object without poll or subscribe", func() error {
			_, err := DecodeInput([]byte(`{"devices": [{"device": "d", "points": [{"signal": "a", "read": {}}]}]}`))
			return err
		}, "devices[0].points[0].read"},
		{"observe item", func() error { _, err := DecodeProcessor([]byte(`{"observe": ["a", 4]}`)); return err }, "observe[1]"},
		{"derived signal type", func() error {
			_, err := DecodeProcessor([]byte(`{"devices": [{"device": "d", "signals": [{"signal": ["` + secret + `"]}]}]}`))
			return err
		}, "devices[0].signals[0].signal"},
		{"output id", func() error { _, err := DecodeOutput([]byte(`{"id": ["` + secret + `"]}`)); return err }, "id"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.decode()
			if err == nil {
				t.Fatal("no error")
			}
			if strings.Contains(err.Error(), secret) {
				t.Errorf("error leaks a value: %v", err)
			}
			var ce ConfigError
			if !errors.As(err, &ce) {
				t.Fatalf("error %T is not a ConfigError", err)
			}
			if ce.Path != tc.wantPath {
				t.Errorf("path %q, want %q (message %q)", ce.Path, tc.wantPath, ce.Message)
			}
			if ce.Message == "" {
				t.Error("empty message")
			}
		})
	}
}
