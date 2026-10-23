package main

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/getlaraplate/laraedge-sdk/examples/internal/exampletest"
	"github.com/getlaraplate/laraedge-sdk/sdk"
)

var t0 = time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)

func configSection(path string) json.RawMessage {
	raw, _ := json.Marshal(map[string]any{"id": "out", "config": map[string]any{"path": path}})
	return raw
}

func configuredSink(t *testing.T, path string) *sink {
	t.Helper()
	s := newSink()
	if errs := s.Configure(context.Background(), "i1", configSection(path)); len(errs) > 0 {
		t.Fatalf("Configure: %v", errs)
	}
	return s
}

func sampleRecord(device string, v int64) sdk.Record {
	return sdk.Record{Sample: &sdk.Sample{Device: device, Signal: "count", TS: t0, Value: sdk.IntValue(v), Quality: sdk.Good, Priority: sdk.Critical}}
}

func batch(id string, seq uint64, records ...sdk.Record) sdk.Batch {
	return sdk.Batch{MessageID: id, SourceSeq: seq, Records: records}
}

func lines(t *testing.T, path string) []string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var out []string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		out = append(out, sc.Text())
	}
	return out
}

func send(s *sink, b sdk.Batch) sdk.Outcome { return s.Send(context.Background(), "i1", b) }

func TestManifestMatchesTheBinary(t *testing.T) {
	exampletest.CheckCommittedManifest(t)
}

func TestFixturesAgreeWithManifestAndConfigure(t *testing.T) {
	m := exampletest.LoadManifest(t)
	f := exampletest.LoadFixtures(t)
	if f.Output == nil || len(f.Output.Valid) == 0 || len(f.Output.Invalid) == 0 {
		t.Fatal("conformance.json needs valid and invalid output sections")
	}
	if f.Input != nil || f.Processor != nil {
		t.Fatal("conformance.json lists a kind this plugin does not offer")
	}
	for i, s := range f.Output.Valid {
		if problems := m.ValidateOutput(s); len(problems) > 0 {
			t.Errorf("valid[%d]: manifest refuses it: %v", i, problems)
		}
		if errs := newSink().Configure(context.Background(), "x", s); len(errs) > 0 {
			t.Errorf("valid[%d]: Configure refuses it: %v", i, errs)
		}
	}
	for i, s := range f.Output.Invalid {
		if problems := m.ValidateOutput(s); len(problems) == 0 {
			t.Errorf("invalid[%d]: the manifest accepts it", i)
		}
		if errs := newSink().Configure(context.Background(), "x", s); len(errs) == 0 {
			t.Errorf("invalid[%d]: Configure accepts it", i)
		}
	}
}

func TestConfigureRefusals(t *testing.T) {
	tests := []struct {
		name, section, wantPath string
	}{
		{"not json", `{`, ""},
		{"no config", `{"id":"o"}`, "config.path"},
		{"no path", `{"config":{}}`, "config.path"},
		{"path not a string", `{"config":{"path":3}}`, "config.path"},
		{"empty path", `{"config":{"path":""}}`, "config.path"},
		{"unknown key", `{"config":{"path":"x.jsonl","mode":"a"}}`, "config"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			errs := newSink().Configure(context.Background(), "i", json.RawMessage(tc.section))
			if len(errs) != 1 || errs[0].Path != tc.wantPath {
				t.Fatalf("errors = %v, want one at %q", errs, tc.wantPath)
			}
		})
	}
}

func TestConfigureRefusesAPathItCannotRead(t *testing.T) {
	dir := t.TempDir() // a directory is not a file the sink can read ids from
	errs := newSink().Configure(context.Background(), "i", configSection(dir))
	if len(errs) != 1 || errs[0].Path != "config.path" {
		t.Fatalf("errors = %v", errs)
	}
	if strings.Contains(errs[0].Message, dir) {
		t.Errorf("the message contains the path: %q", errs[0].Message)
	}
}

func TestSendAppendsOneJSONLinePerBatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.jsonl")
	s := configuredSink(t, path)
	b1 := sdk.Batch{
		MessageID: "m1", SourceSeq: 7,
		Records: []sdk.Record{
			sampleRecord("press-1", 3),
			{Event: &sdk.DeviceEvent{Kind: sdk.Birth, Device: "press-1", TS: t0, Signals: []sdk.SignalInfo{{Signal: "count", DataType: sdk.DataTypeNumber, Unit: "pcs"}}}},
		},
		Losses: []sdk.LossMarker{{Priority: sdk.Bulk, From: t0, To: t0.Add(time.Minute), Count: 12, Reason: "buffer_full"}},
	}
	if o := send(s, b1); o.Kind != sdk.OutcomeAccepted {
		t.Fatalf("first batch = %+v", o)
	}
	if o := send(s, batch("m2", 8, sampleRecord("press-1", 4))); o.Kind != sdk.OutcomeAccepted {
		t.Fatalf("second batch = %+v", o)
	}
	got := lines(t, path)
	if len(got) != 2 {
		t.Fatalf("file has %d lines, want 2: %q", len(got), got)
	}
	var first struct {
		MessageID string `json:"message_id"`
		SourceSeq uint64 `json:"source_seq"`
		Records   []struct {
			Sample *struct {
				Device    string          `json:"device"`
				Signal    string          `json:"signal"`
				TS        string          `json:"ts"`
				Value     json.RawMessage `json:"value"`
				ValueType string          `json:"value_type"`
				Quality   string          `json:"quality"`
				Priority  string          `json:"priority"`
			} `json:"sample"`
			Event *struct {
				Kind    string `json:"kind"`
				Device  string `json:"device"`
				Signals []struct {
					Signal   string `json:"signal"`
					DataType string `json:"data_type"`
					Unit     string `json:"unit"`
				} `json:"signals"`
			} `json:"event"`
		} `json:"records"`
		Losses []struct {
			Priority string `json:"priority"`
			From     string `json:"from"`
			To       string `json:"to"`
			Count    uint64 `json:"count"`
			Reason   string `json:"reason"`
		} `json:"losses"`
	}
	if err := json.Unmarshal([]byte(got[0]), &first); err != nil {
		t.Fatalf("line 1 is not JSON: %v", err)
	}
	if first.MessageID != "m1" || first.SourceSeq != 7 || len(first.Records) != 2 || len(first.Losses) != 1 {
		t.Fatalf("line 1 = %s", got[0])
	}
	sample := first.Records[0].Sample
	if sample == nil || sample.Device != "press-1" || sample.Signal != "count" || string(sample.Value) != "3" || sample.ValueType != "int" ||
		sample.Quality != "good" || sample.Priority != "critical" || sample.TS != "2026-10-23T12:00:00Z" {
		t.Errorf("sample = %+v", sample)
	}
	event := first.Records[1].Event
	if event == nil || event.Kind != "birth" || event.Device != "press-1" || len(event.Signals) != 1 || event.Signals[0].DataType != "number" || event.Signals[0].Unit != "pcs" {
		t.Errorf("event = %+v", event)
	}
	if l := first.Losses[0]; l.Priority != "bulk" || l.Count != 12 || l.Reason != "buffer_full" || l.From != "2026-10-23T12:00:00Z" || l.To != "2026-10-23T12:01:00Z" {
		t.Errorf("loss = %+v", l)
	}
}

func TestValueTypesAreKept(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.jsonl")
	s := configuredSink(t, path)
	f, _ := sdk.FloatValue(1.5)
	values := []sdk.Value{f, sdk.IntValue(-2), sdk.BoolValue(true), sdk.StringValue("x")}
	var recs []sdk.Record
	for _, v := range values {
		recs = append(recs, sdk.Record{Sample: &sdk.Sample{Device: "d", Signal: "s", TS: t0, Value: v, Quality: sdk.Uncertain, Priority: sdk.Bulk}})
	}
	if o := send(s, batch("m", 1, recs...)); o.Kind != sdk.OutcomeAccepted {
		t.Fatalf("outcome = %+v", o)
	}
	var line struct {
		Records []struct {
			Sample struct {
				Value     json.RawMessage `json:"value"`
				ValueType string          `json:"value_type"`
				Quality   string          `json:"quality"`
			} `json:"sample"`
		} `json:"records"`
	}
	if err := json.Unmarshal([]byte(lines(t, path)[0]), &line); err != nil {
		t.Fatal(err)
	}
	want := [][2]string{{"1.5", "float"}, {"-2", "int"}, {"true", "bool"}, {`"x"`, "string"}}
	for i, w := range want {
		g := line.Records[i].Sample
		if string(g.Value) != w[0] || g.ValueType != w[1] || g.Quality != "uncertain" {
			t.Errorf("record %d = %+v, want %v", i, g, w)
		}
	}
}

func TestDuplicateMessageIDIsNotWrittenAgain(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.jsonl")
	s := configuredSink(t, path)
	b := batch("m1", 1, sampleRecord("d", 1))
	if o := send(s, b); o.Kind != sdk.OutcomeAccepted {
		t.Fatalf("first = %+v", o)
	}
	if o := send(s, b); o.Kind != sdk.OutcomeDuplicate {
		t.Fatalf("second = %+v, want DUPLICATE", o)
	}
	if n := len(lines(t, path)); n != 1 {
		t.Errorf("file has %d lines, want 1", n)
	}
}

func TestDuplicateIsRecognisedForIDsAlreadyInTheFileAtConfigure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.jsonl")
	first := configuredSink(t, path)
	if o := send(first, batch("m1", 1, sampleRecord("d", 1))); o.Kind != sdk.OutcomeAccepted {
		t.Fatal(o)
	}
	second := configuredSink(t, path) // a new process: the ids come from the file
	if o := send(second, batch("m1", 1, sampleRecord("d", 1))); o.Kind != sdk.OutcomeDuplicate {
		t.Errorf("resend after restart = %+v, want DUPLICATE", o)
	}
	if o := send(second, batch("m2", 2, sampleRecord("d", 2))); o.Kind != sdk.OutcomeAccepted {
		t.Errorf("new id = %+v, want ACCEPTED", o)
	}
	if n := len(lines(t, path)); n != 2 {
		t.Errorf("file has %d lines, want 2", n)
	}
}

func TestATornLastLineDoesNotCorruptTheNextAppend(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.jsonl")
	torn := `{"message_id":"m1","source_seq":1,"records":[],"losses":[]}` + "\n" + `{"message_id":"m2","sour`
	if err := os.WriteFile(path, []byte(torn), 0o600); err != nil {
		t.Fatal(err)
	}
	s := configuredSink(t, path)
	if o := send(s, batch("m1", 1)); o.Kind != sdk.OutcomeDuplicate {
		t.Errorf("m1 = %+v, want DUPLICATE", o)
	}
	if o := send(s, batch("m3", 3, sampleRecord("d", 1))); o.Kind != sdk.OutcomeAccepted {
		t.Fatalf("m3 = %+v", o)
	}
	got := lines(t, path)
	if len(got) != 3 {
		t.Fatalf("file has %d lines, want 3: %q", len(got), got)
	}
	var last struct {
		MessageID string `json:"message_id"`
	}
	if err := json.Unmarshal([]byte(got[2]), &last); err != nil || last.MessageID != "m3" {
		t.Errorf("last line = %q (%v), want the m3 batch on its own line", got[2], err)
	}
}

func TestRejectInvalidBatches(t *testing.T) {
	tests := []struct {
		name string
		b    sdk.Batch
	}{
		{"sample without device", batch("m", 1, sampleRecord("d", 1), sampleRecord("", 2))},
		{"event without device", batch("m", 1, sdk.Record{Event: &sdk.DeviceEvent{Kind: sdk.Death, TS: t0}})},
		{"record without sample or event", batch("m", 1, sdk.Record{})},
		{"no message id", batch("", 1, sampleRecord("d", 1))},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "out.jsonl")
			s := configuredSink(t, path)
			o := send(s, tc.b)
			if o.Kind != sdk.OutcomeRejectInvalid || o.Reason == "" {
				t.Fatalf("outcome = %+v, want REJECT_INVALID with a reason", o)
			}
			if _, err := os.Stat(path); err == nil {
				t.Error("a rejected batch created the file")
			}
			if o := send(s, batch("ok", 2, sampleRecord("d", 1))); o.Kind != sdk.OutcomeAccepted {
				t.Errorf("the sink did not recover: %+v", o)
			}
		})
	}
}

func TestSuspendSwitchIsCheckedAtEachSend(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.jsonl")
	s := configuredSink(t, path)
	if o := send(s, batch("m1", 1, sampleRecord("d", 1))); o.Kind != sdk.OutcomeAccepted {
		t.Fatal(o)
	}
	if err := os.WriteFile(path+".suspend", nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if o := send(s, batch("m2", 2, sampleRecord("d", 1))); o.Kind != sdk.OutcomeSuspendAuth {
		t.Fatalf("while the switch exists = %+v, want SUSPEND_AUTH", o)
	}
	if n := len(lines(t, path)); n != 1 {
		t.Errorf("a suspended send wrote: %d lines", n)
	}
	if err := os.Remove(path + ".suspend"); err != nil {
		t.Fatal(err)
	}
	if o := send(s, batch("m2", 2, sampleRecord("d", 1))); o.Kind != sdk.OutcomeAccepted {
		t.Errorf("after the switch was removed = %+v, want ACCEPTED (the suspended batch was not remembered)", o)
	}
}

func TestAnUnwritableFileAsksForARetry(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing-dir", "out.jsonl")
	s := configuredSink(t, path) // a file that does not exist yet is fine at Configure
	o := send(s, batch("m1", 1, sampleRecord("d", 1)))
	if o.Kind != sdk.OutcomeRetryAfter || o.RetryAfter <= 0 {
		t.Fatalf("outcome = %+v, want RETRY_AFTER with a delay", o)
	}
	if err := os.Mkdir(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if o := send(s, batch("m1", 1, sampleRecord("d", 1))); o.Kind != sdk.OutcomeAccepted {
		t.Errorf("after the directory exists = %+v, want ACCEPTED (a failed write must not be remembered)", o)
	}
	if got := lines(t, path); len(got) != 1 || !strings.HasPrefix(got[0], `{"message_id":"m1"`) {
		t.Errorf("file = %q, want exactly the m1 line", got)
	}
}

func TestSendWithACancelledContextAsksForARetry(t *testing.T) {
	s := configuredSink(t, filepath.Join(t.TempDir(), "out.jsonl"))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if o := s.Send(ctx, "i1", batch("m1", 1, sampleRecord("d", 1))); o.Kind != sdk.OutcomeRetryAfter {
		t.Errorf("outcome = %+v, want RETRY_AFTER", o)
	}
}

func TestInstancesAreIndependent(t *testing.T) {
	dir := t.TempDir()
	a, b := filepath.Join(dir, "a.jsonl"), filepath.Join(dir, "b.jsonl")
	s := newSink()
	for id, p := range map[string]string{"a": a, "b": b} {
		if errs := s.Configure(context.Background(), id, configSection(p)); len(errs) > 0 {
			t.Fatal(errs)
		}
	}
	if o := s.Send(context.Background(), "a", batch("m1", 1)); o.Kind != sdk.OutcomeAccepted {
		t.Fatal(o)
	}
	if o := s.Send(context.Background(), "b", batch("m1", 1)); o.Kind != sdk.OutcomeAccepted {
		t.Errorf("the other instance saw a duplicate: %+v", o)
	}
	if o := s.Send(context.Background(), "unknown", batch("m1", 1)); o.Kind != sdk.OutcomeRejectInvalid {
		t.Errorf("unconfigured instance = %+v", o)
	}
}
