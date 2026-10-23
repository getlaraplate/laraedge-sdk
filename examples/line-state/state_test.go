package main

import (
	"context"
	"encoding/json"
	"slices"
	"testing"
	"time"

	"github.com/getlaraplate/laraedge-sdk/examples/internal/exampletest"
	"github.com/getlaraplate/laraedge-sdk/sdk"
)

const section = `{
  "id": "line-state",
  "observe": ["press-1", "press-2", "press-3"],
  "devices": [{"device": "line-1", "signals": [{"signal": "state", "type": "int", "priority": "critical"}]}],
  "config": {}
}`

var t0 = time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)

func stateSample(device string, v sdk.Value, q sdk.Quality, at int) sdk.Record {
	s := sdk.Sample{Device: device, Signal: "state", TS: t0.Add(time.Duration(at) * time.Second), Value: v, Quality: q, Priority: sdk.Critical}
	return sdk.Record{Sample: &s}
}

func good(device string, v int64, at int) sdk.Record {
	return stateSample(device, sdk.IntValue(v), sdk.Good, at)
}

// run configures a processor, feeds the records and returns what it emitted.
func run(t *testing.T, records []sdk.Record) []sdk.Sample {
	t.Helper()
	p := newProcessor()
	if errs := p.Configure(context.Background(), "i1", json.RawMessage(section)); len(errs) > 0 {
		t.Fatalf("Configure: %v", errs)
	}
	in := make(chan sdk.Record, len(records))
	for _, r := range records {
		in <- r
	}
	close(in)
	var out []sdk.Sample
	if err := p.Process(context.Background(), "i1", in, func(s sdk.Sample) error {
		out = append(out, s)
		return nil
	}); err != nil {
		t.Fatalf("Process: %v", err)
	}
	return out
}

func TestManifestMatchesTheBinary(t *testing.T) {
	exampletest.CheckCommittedManifest(t)
}

func TestFixturesAgreeWithManifestAndConfigure(t *testing.T) {
	m := exampletest.LoadManifest(t)
	f := exampletest.LoadFixtures(t)
	if f.Processor == nil || len(f.Processor.Valid) == 0 || len(f.Processor.Invalid) == 0 {
		t.Fatal("conformance.json needs valid and invalid processor sections")
	}
	if f.Input != nil || f.Output != nil {
		t.Fatal("conformance.json lists a kind this plugin does not offer")
	}
	for i, s := range f.Processor.Valid {
		if problems := m.ValidateProcessor(s); len(problems) > 0 {
			t.Errorf("valid[%d]: manifest refuses it: %v", i, problems)
		}
		if errs := newProcessor().Configure(context.Background(), "x", s); len(errs) > 0 {
			t.Errorf("valid[%d]: Configure refuses it: %v", i, errs)
		}
	}
	for i, s := range f.Processor.Invalid {
		if errs := newProcessor().Configure(context.Background(), "x", s); len(errs) == 0 {
			t.Errorf("invalid[%d]: Configure accepts it", i)
		}
	}
}

func TestConfigureRefusals(t *testing.T) {
	derived := `"devices":[{"device":"line-1","signals":[{"signal":"state","type":"int","priority":"critical"}]}]`
	tests := []struct {
		name     string
		section  string
		wantPath string
	}{
		{"not json", `[`, ""},
		{"no observe", `{"id":"p",` + derived + `}`, "observe"},
		{"empty observe", `{"id":"p","observe":[],` + derived + `}`, "observe"},
		{"observe repeats a device", `{"observe":["a","a"],` + derived + `}`, "observe[1]"},
		{"observe empty name", `{"observe":[""],` + derived + `}`, "observe[0]"},
		{"no derived device", `{"observe":["a"]}`, "devices"},
		{"two derived devices", `{"observe":["a"],"devices":[{"device":"x","signals":[{"signal":"state","type":"int"}]},{"device":"y","signals":[{"signal":"state","type":"int"}]}]}`, "devices"},
		{"derived device without state", `{"observe":["a"],"devices":[{"device":"x","signals":[{"signal":"other","type":"int"}]}]}`, "devices[0].signals"},
		{"derived device without a name", `{"observe":["a"],"devices":[{"signals":[{"signal":"state","type":"int"}]}]}`, "devices[0].device"},
		{"derived state not int", `{"observe":["a"],"devices":[{"device":"x","signals":[{"signal":"state","type":"float"}]}]}`, "devices[0].signals[0].type"},
		{"derived device observed", `{"observe":["x"],"devices":[{"device":"x","signals":[{"signal":"state","type":"int"}]}]}`, "devices[0].device"},
		{"config with a key", `{"observe":["a"],` + derived + `,"config":{"mode":"min"}}`, "config"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			errs := newProcessor().Configure(context.Background(), "i", json.RawMessage(tc.section))
			if len(errs) != 1 {
				t.Fatalf("got %d errors %v, want 1", len(errs), errs)
			}
			if errs[0].Path != tc.wantPath {
				t.Errorf("path = %q, want %q (message %q)", errs[0].Path, tc.wantPath, errs[0].Message)
			}
		})
	}
}

func TestProcessWithoutConfigureFails(t *testing.T) {
	in := make(chan sdk.Record)
	close(in)
	if err := newProcessor().Process(context.Background(), "nope", in, func(sdk.Sample) error { return nil }); err == nil {
		t.Error("Process of an unconfigured instance succeeded")
	}
}

func TestEmitsTheMaximumWheneverItChanges(t *testing.T) {
	out := run(t, []sdk.Record{
		good("press-1", 2, 1), // max 2: emitted
		good("press-2", 1, 2), // max stays 2
		good("press-2", 4, 3), // max 4: emitted
		good("press-2", 3, 4), // max falls to 3: emitted with the time of this sample
		good("press-1", 2, 5), // same value as before: no change
		good("press-3", 2, 6), // below the maximum: no change
		good("press-1", 0, 7), // press-2 still holds 3: no change
		good("press-2", 0, 8), // press-3 holds 2: max falls to 2, emitted
	})
	type step struct {
		at    int
		state int64
	}
	var got []step
	for _, s := range out {
		if s.Device != "line-1" || s.Signal != "state" || s.Quality != sdk.Good || s.Priority != sdk.Critical || s.Value.Kind != sdk.KindInt {
			t.Errorf("unexpected sample %+v", s)
		}
		got = append(got, step{int(s.TS.Sub(t0) / time.Second), s.Value.Int})
	}
	if want := []step{{1, 2}, {3, 4}, {4, 3}, {8, 2}}; !slices.Equal(got, want) {
		t.Errorf("emitted %v, want %v", got, want)
	}
}

func TestIgnoresWhatItCannotUse(t *testing.T) {
	notAState := sdk.Record{Sample: &sdk.Sample{Device: "press-1", Signal: "temp", TS: t0, Value: sdk.IntValue(99), Quality: sdk.Good, Priority: sdk.Bulk}}
	unobserved := good("other", 50, 1)
	birth := sdk.Record{Event: &sdk.DeviceEvent{Kind: sdk.Birth, Device: "press-1", TS: t0}}
	float, _ := sdk.FloatValue(7)
	tests := []struct {
		name string
		rec  sdk.Record
	}{
		{"float value", stateSample("press-1", float, sdk.Good, 1)},
		{"string value", stateSample("press-1", sdk.StringValue("7"), sdk.Good, 1)},
		{"bool value", stateSample("press-1", sdk.BoolValue(true), sdk.Good, 1)},
		{"bad quality", stateSample("press-1", sdk.IntValue(7), sdk.Bad, 1)},
		{"other signal", notAState},
		{"unobserved device", unobserved},
		{"device event", birth},
		{"empty record", sdk.Record{}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if out := run(t, []sdk.Record{tc.rec}); len(out) != 0 {
				t.Errorf("emitted %+v, want nothing", out)
			}
		})
	}
	// Uncertain readings are used: only Bad is dropped.
	if out := run(t, []sdk.Record{stateSample("press-1", sdk.IntValue(7), sdk.Uncertain, 1)}); len(out) != 1 {
		t.Errorf("an uncertain integer state emitted %d samples, want 1", len(out))
	}
}

func TestAnIgnoredSampleKeepsThePreviousState(t *testing.T) {
	out := run(t, []sdk.Record{
		good("press-1", 5, 1),
		stateSample("press-1", sdk.IntValue(9), sdk.Bad, 2),
		good("press-2", 3, 3),
	})
	if len(out) != 1 || out[0].Value.Int != 5 {
		t.Errorf("emitted %+v, want only the first state 5", out)
	}
}

func TestStateIsPerRun(t *testing.T) {
	p := newProcessor()
	if errs := p.Configure(context.Background(), "i1", json.RawMessage(section)); len(errs) > 0 {
		t.Fatal(errs)
	}
	for range 2 {
		in := make(chan sdk.Record, 1)
		in <- good("press-1", 2, 1)
		close(in)
		var n int
		if err := p.Process(context.Background(), "i1", in, func(sdk.Sample) error { n++; return nil }); err != nil || n != 1 {
			t.Fatalf("Process = %v after %d samples, want nil and 1 on every run", err, n)
		}
	}
}

func TestProcessStopsWhenTheContextIsCancelled(t *testing.T) {
	p := newProcessor()
	if errs := p.Configure(context.Background(), "i1", json.RawMessage(section)); len(errs) > 0 {
		t.Fatal(errs)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	in := make(chan sdk.Record)
	go func() { done <- p.Process(ctx, "i1", in, func(sdk.Sample) error { return nil }) }()
	cancel()
	if err := <-done; err == nil {
		t.Error("Process returned nil after the context was cancelled")
	}
}
