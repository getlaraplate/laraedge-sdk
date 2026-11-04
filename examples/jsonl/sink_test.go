package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
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
	return sdk.Record{Sample: &sdk.Sample{Device: device, Signal: "count", TS: t0, Value: sdk.IntValue(v), Quality: sdk.QualityGood, Priority: sdk.PriorityCritical}}
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
			{Event: &sdk.DeviceEvent{Kind: sdk.EventBirth, Device: "press-1", TS: t0, Signals: []sdk.SignalInfo{{Signal: "count", DataType: sdk.DataTypeNumber, Unit: "pcs"}}}},
		},
		Losses: []sdk.LossMarker{{Priority: sdk.PriorityBulk, From: t0, To: t0.Add(time.Minute), Count: 12, Reason: "buffer_full"}},
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
		recs = append(recs, sdk.Record{Sample: &sdk.Sample{Device: "d", Signal: "s", TS: t0, Value: v, Quality: sdk.QualityUncertain, Priority: sdk.PriorityBulk}})
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
		{"event without device", batch("m", 1, sdk.Record{Event: &sdk.DeviceEvent{Kind: sdk.EventDeath, TS: t0}})},
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

// faultyFile is a real file whose next WriteAt, Sync, Truncate or Close calls fail on demand. A
// failing WriteAt writes the first half of its data, as a write cut short does.
type faultyFile struct {
	*os.File
	disk *faultyDisk
}

func (f faultyFile) WriteAt(p []byte, off int64) (int, error) {
	if f.disk.failWrites > 0 {
		f.disk.failWrites--
		n, _ := f.File.WriteAt(p[:len(p)/2], off)
		return n, errors.New("write failed")
	}
	return f.File.WriteAt(p, off)
}

func (f faultyFile) Close() error {
	err := f.File.Close()
	if f.disk.failCloses > 0 {
		f.disk.failCloses--
		return errors.New("close failed")
	}
	return err
}

func (f faultyFile) Sync() error {
	if f.disk.failSyncs > 0 {
		f.disk.failSyncs--
		return errors.New("sync failed")
	}
	return f.File.Sync()
}

func (f faultyFile) Truncate(size int64) error {
	if f.disk.failTruncates > 0 {
		f.disk.failTruncates--
		return errors.New("truncate failed")
	}
	return f.File.Truncate(size)
}

// faultyDisk opens faultyFiles; its counters say how many of the next calls fail. The sink calls
// it under the lock of its target, and the tests send from one goroutine.
type faultyDisk struct {
	failWrites, failSyncs, failTruncates, failCloses int
}

func (d *faultyDisk) open(path string) (file, error) {
	f, err := openFile(path)
	if err != nil {
		return nil, err
	}
	return faultyFile{File: f.(*os.File), disk: d}, nil
}

func messageIDs(t *testing.T, path string) []string {
	t.Helper()
	var ids []string
	for _, l := range lines(t, path) {
		var head struct {
			MessageID string `json:"message_id"`
		}
		if err := json.Unmarshal([]byte(l), &head); err != nil {
			t.Fatalf("line %q is not JSON: %v", l, err)
		}
		ids = append(ids, head.MessageID)
	}
	return ids
}

func TestAFailedSyncAsksForARetryAndTheResendIsWrittenOnce(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.jsonl")
	s := configuredSink(t, path)
	disk := &faultyDisk{}
	s.open = disk.open
	if o := send(s, batch("m0", 1, sampleRecord("d", 1))); o.Kind != sdk.OutcomeAccepted {
		t.Fatalf("m0 = %+v", o)
	}
	disk.failSyncs = 1
	o := send(s, batch("m1", 2, sampleRecord("d", 2)))
	if o.Kind != sdk.OutcomeRetryAfter || o.RetryAfter != retryDelay {
		t.Fatalf("send with a failing Sync = %+v, want RETRY_AFTER after %v", o, retryDelay)
	}
	if o := send(s, batch("m1", 2, sampleRecord("d", 2))); o.Kind != sdk.OutcomeAccepted {
		t.Fatalf("resend = %+v, want ACCEPTED", o)
	}
	if got := messageIDs(t, path); !slices.Equal(got, []string{"m0", "m1"}) {
		t.Errorf("file holds %q, want m0 and m1 once each", got)
	}
}

func TestAFailedTruncationWritesNothingAndAsksForARetry(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.jsonl")
	s := configuredSink(t, path)
	disk := &faultyDisk{}
	s.open = disk.open
	if o := send(s, batch("m0", 1, sampleRecord("d", 1))); o.Kind != sdk.OutcomeAccepted {
		t.Fatalf("m0 = %+v", o)
	}
	disk.failSyncs = 1
	if o := send(s, batch("m1", 2, sampleRecord("d", 2))); o.Kind != sdk.OutcomeRetryAfter {
		t.Fatalf("send with a failing Sync = %+v, want RETRY_AFTER", o)
	}
	disk.failTruncates = 1
	o := send(s, batch("m1", 2, sampleRecord("d", 2)))
	if o.Kind != sdk.OutcomeRetryAfter || o.RetryAfter != retryDelay {
		t.Fatalf("resend with a failing Truncate = %+v, want RETRY_AFTER after %v", o, retryDelay)
	}
	if got := messageIDs(t, path); !slices.Equal(got, []string{"m0", "m1"}) {
		t.Errorf("after the failed truncation the file holds %q, want the unsynced m1 line only once", got)
	}
	if o := send(s, batch("m1", 2, sampleRecord("d", 2))); o.Kind != sdk.OutcomeAccepted {
		t.Fatalf("resend = %+v, want ACCEPTED", o)
	}
	if got := messageIDs(t, path); !slices.Equal(got, []string{"m0", "m1"}) {
		t.Errorf("file holds %q, want m0 and m1 once each", got)
	}
}

func TestAPartialWriteAsksForARetryAndTheResendIsWrittenOnce(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.jsonl")
	s := configuredSink(t, path)
	disk := &faultyDisk{}
	s.open = disk.open
	if o := send(s, batch("m0", 1, sampleRecord("d", 1))); o.Kind != sdk.OutcomeAccepted {
		t.Fatalf("m0 = %+v", o)
	}
	disk.failWrites = 1
	if o := send(s, batch("m1", 2, sampleRecord("d", 2))); o.Kind != sdk.OutcomeRetryAfter || o.RetryAfter != retryDelay {
		t.Fatalf("send with a failing write = %+v, want RETRY_AFTER after %v", o, retryDelay)
	}
	if o := send(s, batch("m1", 2, sampleRecord("d", 2))); o.Kind != sdk.OutcomeAccepted {
		t.Fatalf("resend = %+v, want ACCEPTED", o)
	}
	if got := messageIDs(t, path); !slices.Equal(got, []string{"m0", "m1"}) {
		t.Errorf("file holds %q, want m0 and m1 once each, without the half line", got)
	}
}

// TestAFailedCloseAfterTheSyncKeepsTheLine: once Sync succeeded the line is durable, so a Close
// that fails afterwards does not make the batch look unstored, and its resend is not written again.
func TestAFailedCloseAfterTheSyncKeepsTheLine(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.jsonl")
	s := configuredSink(t, path)
	disk := &faultyDisk{}
	s.open = disk.open
	disk.failCloses = 1
	if o := send(s, batch("m1", 1, sampleRecord("d", 1))); o.Kind != sdk.OutcomeAccepted {
		t.Fatalf("send with a failing Close after the Sync = %+v, want ACCEPTED", o)
	}
	if o := send(s, batch("m1", 1, sampleRecord("d", 1))); o.Kind != sdk.OutcomeDuplicate {
		t.Fatalf("resend = %+v, want DUPLICATE", o)
	}
	if got := messageIDs(t, path); !slices.Equal(got, []string{"m1"}) {
		t.Errorf("file holds %q, want m1 once", got)
	}
}

// TestAFileReplacedBehindTheSinkIsReadAgain: a file rotated away, or rewritten smaller, by
// someone else is not the file the sink knew. Its message ids are read again, and a truncation
// owed to the old file never extends the new one.
func TestAFileReplacedBehindTheSinkIsReadAgain(t *testing.T) {
	t.Run("rotated", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "out.jsonl")
		s := configuredSink(t, path)
		if o := send(s, batch("m1", 1, sampleRecord("d", 1))); o.Kind != sdk.OutcomeAccepted {
			t.Fatalf("m1 = %+v", o)
		}
		if err := os.Rename(path, path+".1"); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(`{"message_id":"m5"}`+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		if o := send(s, batch("m5", 5, sampleRecord("d", 5))); o.Kind != sdk.OutcomeDuplicate {
			t.Errorf("m5, already in the new file = %+v, want DUPLICATE", o)
		}
		if o := send(s, batch("m2", 2, sampleRecord("d", 2))); o.Kind != sdk.OutcomeAccepted {
			t.Fatalf("m2 = %+v", o)
		}
		if got := messageIDs(t, path); !slices.Equal(got, []string{"m5", "m2"}) {
			t.Errorf("new file holds %q, want m5 then m2", got)
		}
	})
	t.Run("rewritten smaller after a failed sync", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "out.jsonl")
		s := configuredSink(t, path)
		disk := &faultyDisk{}
		s.open = disk.open
		for i, id := range []string{"m0", "m1"} {
			if o := send(s, batch(id, uint64(i+1), sampleRecord("d", 1))); o.Kind != sdk.OutcomeAccepted {
				t.Fatalf("%s = %+v", id, o)
			}
		}
		disk.failSyncs = 1
		if o := send(s, batch("m2", 3, sampleRecord("d", 3))); o.Kind != sdk.OutcomeRetryAfter {
			t.Fatalf("send with a failing Sync = %+v, want RETRY_AFTER", o)
		}
		if err := os.WriteFile(path, []byte(`{"message_id":"x"}`+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		if o := send(s, batch("m2", 3, sampleRecord("d", 3))); o.Kind != sdk.OutcomeAccepted {
			t.Fatalf("resend = %+v, want ACCEPTED", o)
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if strings.ContainsRune(string(raw), 0) {
			t.Errorf("the file was extended with NUL bytes: %q", raw)
		}
		if got := messageIDs(t, path); !slices.Equal(got, []string{"x", "m2"}) {
			t.Errorf("file holds %q, want x then m2", got)
		}
	})
}

// TestReadingAFileDoesNotHoldTheSink: while the message ids of one file are read at its first
// Configure, the other instances keep sending.
func TestReadingAFileDoesNotHoldTheSink(t *testing.T) {
	dir := t.TempDir()
	s := configuredSink(t, filepath.Join(dir, "a.jsonl"))
	reading := make(chan struct{})
	release := make(chan struct{})
	s.readIDs = func(path string) (map[string]bool, error) {
		close(reading)
		<-release
		return readMessageIDs(path)
	}
	configured := make(chan []sdk.ConfigError)
	go func() {
		configured <- s.Configure(context.Background(), "i2", configSection(filepath.Join(dir, "b.jsonl")))
	}()
	<-reading
	sent := make(chan sdk.Outcome)
	go func() { sent <- send(s, batch("m1", 1, sampleRecord("d", 1))) }()
	select {
	case o := <-sent:
		if o.Kind != sdk.OutcomeAccepted {
			t.Errorf("send while another file is read = %+v, want ACCEPTED", o)
		}
	case errs := <-configured:
		t.Fatalf("Configure returned %v before its read was released", errs)
	case <-time.After(10 * time.Second):
		close(release)
		<-configured
		<-sent
		t.Fatal("a send waited for the read of another file")
	}
	close(release)
	if errs := <-configured; len(errs) > 0 {
		t.Fatal(errs)
	}
}

func TestTheParentDirectoryIsSyncedBeforeTheFirstLine(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "out.jsonl")
	s := configuredSink(t, path)
	var synced []string
	fail := true
	s.syncDir = func(d string) error {
		synced = append(synced, d)
		if fail {
			fail = false
			return errors.New("directory sync failed")
		}
		return nil
	}
	o := send(s, batch("m1", 1, sampleRecord("d", 1)))
	if o.Kind != sdk.OutcomeRetryAfter || o.RetryAfter != retryDelay {
		t.Fatalf("send with a failing directory sync = %+v, want RETRY_AFTER after %v", o, retryDelay)
	}
	if got := lines(t, path); len(got) != 0 {
		t.Fatalf("a send whose directory sync failed wrote %q", got)
	}
	for i, id := range []string{"m1", "m2"} {
		if o := send(s, batch(id, uint64(i+1), sampleRecord("d", 1))); o.Kind != sdk.OutcomeAccepted {
			t.Fatalf("%s = %+v", id, o)
		}
	}
	if !slices.Equal(synced, []string{dir, dir}) {
		t.Errorf("directories synced = %q, want %q twice (the failed attempt and its retry), then never again", synced, dir)
	}
}

func TestDefaultDirectorySyncSucceeds(t *testing.T) {
	if err := syncDir(t.TempDir()); err != nil {
		t.Fatalf("syncDir: %v", err)
	}
}

func TestBadQualityAndDeathAreWords(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.jsonl")
	s := configuredSink(t, path)
	bad := sdk.Record{Sample: &sdk.Sample{Device: "d", Signal: "s", TS: t0, Value: sdk.IntValue(1), Quality: sdk.QualityBad, Priority: sdk.PriorityBulk}}
	death := sdk.Record{Event: &sdk.DeviceEvent{Kind: sdk.EventDeath, Device: "d", TS: t0}}
	if o := send(s, batch("m1", 1, bad, death)); o.Kind != sdk.OutcomeAccepted {
		t.Fatalf("outcome = %+v", o)
	}
	var line struct {
		Records []struct {
			Sample *struct {
				Quality  string `json:"quality"`
				Priority string `json:"priority"`
			} `json:"sample"`
			Event *struct {
				Kind string `json:"kind"`
			} `json:"event"`
		} `json:"records"`
	}
	if err := json.Unmarshal([]byte(lines(t, path)[0]), &line); err != nil {
		t.Fatal(err)
	}
	if got := line.Records[0].Sample; got == nil || got.Quality != "bad" || got.Priority != "bulk" {
		t.Errorf("sample = %+v, want quality bad and priority bulk", got)
	}
	if got := line.Records[1].Event; got == nil || got.Kind != "death" {
		t.Errorf("event = %+v, want kind death", got)
	}
}

func TestInstancesSharingAFileKeepEveryAcceptedLine(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.jsonl")
	s := newSink()
	for _, id := range []string{"a", "b"} {
		if errs := s.Configure(context.Background(), id, configSection(path)); len(errs) > 0 {
			t.Fatal(errs)
		}
	}
	const perSender = 50
	var (
		mu       sync.Mutex
		accepted []string
		wg       sync.WaitGroup
	)
	for _, instance := range []string{"a", "b"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := range perSender {
				id := fmt.Sprintf("%s-%d", instance, n)
				if o := s.Send(context.Background(), instance, batch(id, uint64(n+1), sampleRecord("d", 1))); o.Kind == sdk.OutcomeAccepted {
					mu.Lock()
					accepted = append(accepted, id)
					mu.Unlock()
				}
			}
		}()
	}
	wg.Add(1)
	go func() { // reconfigures an instance while its sends run
		defer wg.Done()
		for range perSender {
			if errs := s.Configure(context.Background(), "a", configSection(path)); len(errs) > 0 {
				t.Error(errs)
				return
			}
		}
	}()
	wg.Wait()
	if len(accepted) != 2*perSender {
		t.Fatalf("%d sends accepted, want %d", len(accepted), 2*perSender)
	}
	got := messageIDs(t, path)
	slices.Sort(got)
	slices.Sort(accepted)
	if !slices.Equal(got, accepted) {
		t.Errorf("file holds %d lines, want each of the %d accepted ids exactly once", len(got), len(accepted))
	}
}
