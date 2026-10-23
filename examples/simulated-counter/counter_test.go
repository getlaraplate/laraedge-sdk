package main

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/getlaraplate/laraedge-sdk/examples/internal/exampletest"
	"github.com/getlaraplate/laraedge-sdk/sdk"
)

const sectionTwoDevices = `{
  "id": "sim",
  "driver": "plugin:simulated-counter",
  "connection": {},
  "devices": [
    {"device": "press-1", "points": [
      {"signal": "strokes", "address": "counter:1", "read": {"poll": "1s"}, "type": "int32"},
      {"signal": "parts", "address": "counter:5", "read": {"poll": "2s"}, "type": "int64", "priority": "critical"}
    ]},
    {"device": "press-2", "points": [
      {"signal": "strokes", "address": "counter:3", "read": {"poll": "1s"}, "type": "int32"}
    ]}
  ]
}`

// fakeTicker is a ticker the test drives by hand.
type fakeTicker struct {
	interval time.Duration
	ch       chan time.Time
}

func (f *fakeTicker) C() <-chan time.Time { return f.ch }
func (f *fakeTicker) Stop()               {}

// fakeClock hands out fakeTickers and remembers them in creation order.
type fakeClock struct {
	mu      sync.Mutex
	tickers []*fakeTicker
}

func (c *fakeClock) newTicker(d time.Duration) ticker {
	c.mu.Lock()
	defer c.mu.Unlock()
	f := &fakeTicker{interval: d, ch: make(chan time.Time)}
	c.tickers = append(c.tickers, f)
	return f
}

// event is what a Run reported.
type event struct {
	kind     string
	sample   sdk.Sample
	device   string
	read     string
	connects bool
}

// recorder is an InputEmitter that queues what it receives.
type recorder struct{ events chan event }

func newRecorder() *recorder { return &recorder{events: make(chan event, 64)} }

func (r *recorder) Sample(s sdk.Sample) error {
	r.events <- event{kind: "sample", sample: s}
	return nil
}

func (r *recorder) Answered(device string) error {
	r.events <- event{kind: "answered", device: device}
	return nil
}

func (r *recorder) Health(connected bool, readError string) error {
	r.events <- event{kind: "health", connects: connected, read: readError}
	return nil
}

func configured(t *testing.T, section string) (*counter, *fakeClock) {
	t.Helper()
	clock := &fakeClock{}
	c := newCounter()
	c.newTicker = clock.newTicker
	if errs := c.Configure(context.Background(), "i1", json.RawMessage(section)); len(errs) > 0 {
		t.Fatalf("Configure refused a valid section: %v", errs)
	}
	return c, clock
}

func intOf(t *testing.T, s sdk.Sample) int64 {
	t.Helper()
	if s.Value.Kind != sdk.KindInt {
		t.Fatalf("value kind = %d, want int", s.Value.Kind)
	}
	return s.Value.Int
}

func TestManifestMatchesTheBinary(t *testing.T) {
	exampletest.CheckCommittedManifest(t)
}

func TestFixturesAgreeWithManifestAndConfigure(t *testing.T) {
	m := exampletest.LoadManifest(t)
	f := exampletest.LoadFixtures(t)
	if f.Input == nil || len(f.Input.Valid) == 0 || len(f.Input.Invalid) == 0 {
		t.Fatal("conformance.json needs valid and invalid input sections")
	}
	if f.Processor != nil || f.Output != nil {
		t.Fatal("conformance.json lists a kind this plugin does not offer")
	}
	for i, section := range f.Input.Valid {
		if problems := m.ValidateInput(section); len(problems) > 0 {
			t.Errorf("valid[%d]: manifest refuses it: %v", i, problems)
		}
		if errs := newCounter().Configure(context.Background(), "x", section); len(errs) > 0 {
			t.Errorf("valid[%d]: Configure refuses it: %v", i, errs)
		}
	}
	for i, section := range f.Input.Invalid {
		if problems := m.ValidateInput(section); len(problems) == 0 {
			t.Errorf("invalid[%d]: the manifest accepts it", i)
		}
		if errs := newCounter().Configure(context.Background(), "x", section); len(errs) == 0 {
			t.Errorf("invalid[%d]: Configure accepts it", i)
		}
	}
}

func TestConfigureRefusals(t *testing.T) {
	point := func(extra string) string {
		return `{"id":"s","devices":[{"device":"d","points":[{"signal":"a","address":"counter:1","read":{"poll":"1s"},"type":"int32"` + extra + `}]}]}`
	}
	tests := []struct {
		name     string
		section  string
		wantPath string
	}{
		{"not json", `{`, ""},
		{"no devices", `{"id":"s"}`, "devices"},
		{"device without name", `{"devices":[{"points":[]}]}`, "devices[0].device"},
		{"device without points", `{"devices":[{"device":"d"}]}`, "devices[0].points"},
		{"signal missing", `{"devices":[{"device":"d","points":[{"address":"counter:1","read":{"poll":"1s"},"type":"int32"}]}]}`, "devices[0].points[0].signal"},
		{"signal twice", `{"devices":[{"device":"d","points":[` +
			`{"signal":"a","address":"counter:1","read":{"poll":"1s"},"type":"int32"},` +
			`{"signal":"a","address":"counter:2","read":{"poll":"1s"},"type":"int32"}]}]}`, "devices[0].points[1].signal"},
		{"address without prefix", `{"devices":[{"device":"d","points":[{"signal":"a","address":"7","read":{"poll":"1s"},"type":"int32"}]}]}`, "devices[0].points[0].address"},
		{"address zero", `{"devices":[{"device":"d","points":[{"signal":"a","address":"counter:0","read":{"poll":"1s"},"type":"int32"}]}]}`, "devices[0].points[0].address"},
		{"address not a number", `{"devices":[{"device":"d","points":[{"signal":"a","address":"counter:x","read":{"poll":"1s"},"type":"int32"}]}]}`, "devices[0].points[0].address"},
		{"type float", `{"devices":[{"device":"d","points":[{"signal":"a","address":"counter:1","read":{"poll":"1s"},"type":"float64"}]}]}`, "devices[0].points[0].type"},
		{"read subscribe", `{"devices":[{"device":"d","points":[{"signal":"a","address":"counter:1","read":"subscribe","type":"int32"}]}]}`, "devices[0].points[0].read"},
		{"read missing", `{"devices":[{"device":"d","points":[{"signal":"a","address":"counter:1","type":"int32"}]}]}`, "devices[0].points[0].read"},
		{"priority unknown", point(`,"priority":"urgent"`), "devices[0].points[0].priority"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			errs := newCounter().Configure(context.Background(), "i", json.RawMessage(tc.section))
			if len(errs) != 1 {
				t.Fatalf("got %d errors %v, want 1", len(errs), errs)
			}
			if errs[0].Path != tc.wantPath {
				t.Errorf("path = %q, want %q (message %q)", errs[0].Path, tc.wantPath, errs[0].Message)
			}
		})
	}
}

func TestConfigureAcceptsAValidSectionAndPointPriority(t *testing.T) {
	c, _ := configured(t, sectionTwoDevices)
	samples, err := c.Probe(context.Background(), "i1")
	if err != nil {
		t.Fatalf("Probe: %v", err)
	}
	priorities := map[string]sdk.Priority{}
	for _, s := range samples {
		priorities[s.Device+"/"+s.Signal] = s.Priority
	}
	if priorities["press-1/parts"] != sdk.Critical || priorities["press-1/strokes"] != sdk.Bulk {
		t.Errorf("priorities = %v, want parts critical and strokes bulk", priorities)
	}
}

func TestRefusedConfigureLeavesTheInstanceUnconfigured(t *testing.T) {
	c := newCounter()
	if errs := c.Configure(context.Background(), "i", json.RawMessage(`{"id":"s"}`)); len(errs) == 0 {
		t.Fatal("Configure accepted an empty section")
	}
	if _, err := c.Probe(context.Background(), "i"); err == nil {
		t.Error("Probe of an unconfigured instance succeeded")
	}
	if _, err := c.Browse(context.Background(), "i", "", 1); err == nil {
		t.Error("Browse of an unconfigured instance succeeded")
	}
	if err := c.Run(context.Background(), "i", newRecorder()); err == nil {
		t.Error("Run of an unconfigured instance succeeded")
	}
}

func TestProbeGivesOneSampleAfterOnePollPerPoint(t *testing.T) {
	c, _ := configured(t, sectionTwoDevices)
	samples, err := c.Probe(context.Background(), "i1")
	if err != nil {
		t.Fatalf("Probe: %v", err)
	}
	got := map[string]int64{}
	for _, s := range samples {
		if err := s.Validate(); err != nil {
			t.Errorf("invalid sample %s/%s: %v", s.Device, s.Signal, err)
		}
		if s.Quality != sdk.Good {
			t.Errorf("%s/%s quality = %d, want good", s.Device, s.Signal, s.Quality)
		}
		got[s.Device+"/"+s.Signal] = intOf(t, s)
	}
	want := map[string]int64{"press-1/strokes": 1, "press-1/parts": 5, "press-2/strokes": 3}
	if len(samples) != len(want) {
		t.Fatalf("got %d samples, want %d", len(samples), len(want))
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s = %d, want %d", k, got[k], v)
		}
	}
	again, _ := c.Probe(context.Background(), "i1")
	for _, s := range again {
		if intOf(t, s) != want[s.Device+"/"+s.Signal] {
			t.Errorf("a second Probe changed %s/%s: Probe must not consume the counter", s.Device, s.Signal)
		}
	}
}

func TestBrowseListsOneNodePerPoint(t *testing.T) {
	c, _ := configured(t, sectionTwoDevices)
	nodes, err := c.Browse(context.Background(), "i1", "", 1)
	if err != nil {
		t.Fatalf("Browse: %v", err)
	}
	want := []sdk.Node{
		{Address: "counter:1", Name: "press-1/strokes", Type: "int32", Access: "read"},
		{Address: "counter:5", Name: "press-1/parts", Type: "int64", Access: "read"},
		{Address: "counter:3", Name: "press-2/strokes", Type: "int32", Access: "read"},
	}
	if !slices.Equal(nodes, want) {
		t.Errorf("nodes = %+v, want %+v", nodes, want)
	}
	below, err := c.Browse(context.Background(), "i1", "counter:1", 1)
	if err != nil || len(below) != 0 {
		t.Errorf("Browse below a point = %v, %v; want no nodes", below, err)
	}
}

func TestRunPollsAtEachPointsTickAndAnswersPerDevice(t *testing.T) {
	c, clock := configured(t, sectionTwoDevices)
	rec := newRecorder()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx, "i1", rec) }()

	// Run creates its tickers before it returns control to any tick: one per point, in order.
	waitForTickers(t, clock, 3)
	if got := []time.Duration{clock.tickers[0].interval, clock.tickers[1].interval, clock.tickers[2].interval}; !slices.Equal(got,
		[]time.Duration{time.Second, 2 * time.Second, time.Second}) {
		t.Fatalf("ticker intervals = %v", got)
	}

	base := time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)
	tick := func(i int, n int) {
		clock.tickers[i].ch <- base.Add(time.Duration(n) * time.Second)
	}
	next := func() event {
		t.Helper()
		select {
		case e := <-rec.events:
			return e
		case <-time.After(20 * time.Second):
			t.Fatal("no event")
			return event{}
		}
	}

	tick(1, 2) // press-1/parts, increment 5
	s, a := next(), next()
	if s.kind != "sample" || s.sample.Device != "press-1" || s.sample.Signal != "parts" || intOf(t, s.sample) != 5 ||
		!s.sample.TS.Equal(base.Add(2*time.Second)) {
		t.Errorf("first event = %+v", s)
	}
	if a.kind != "answered" || a.device != "press-1" {
		t.Errorf("second event = %+v, want answered(press-1)", a)
	}
	tick(1, 4) // second poll of the same point: 10
	if s = next(); intOf(t, s.sample) != 10 {
		t.Errorf("second poll value = %d, want 10", intOf(t, s.sample))
	}
	if a = next(); a.kind != "answered" || a.device != "press-1" {
		t.Errorf("answered after the second poll = %+v", a)
	}
	tick(2, 1) // press-2/strokes, increment 3
	if s = next(); s.sample.Device != "press-2" || intOf(t, s.sample) != 3 {
		t.Errorf("press-2 sample = %+v", s)
	}
	if a = next(); a.kind != "answered" || a.device != "press-2" {
		t.Errorf("answered after press-2 = %+v", a)
	}

	cancel()
	if err := <-done; err != nil {
		t.Errorf("Run after cancel = %v, want nil", err)
	}
	if err := c.Stop("i1"); err != nil {
		t.Errorf("Stop: %v", err)
	}
	if err := c.Stop("unknown"); err != nil {
		t.Errorf("Stop of an unknown instance: %v", err)
	}
}

func waitForTickers(t *testing.T, c *fakeClock, n int) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		got := len(c.tickers)
		c.mu.Unlock()
		if got >= n {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("Run created fewer than %d tickers", n)
}

func TestInt32CountersWrapAndInt64DoNot(t *testing.T) {
	section := `{"devices":[{"device":"d","points":[
	  {"signal":"a","address":"counter:999999999","read":{"poll":"1s"},"type":"int32"},
	  {"signal":"b","address":"counter:999999999","read":{"poll":"1s"},"type":"int64"}]}]}`
	c, clock := configured(t, section)
	rec := newRecorder()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx, "i1", rec) }()
	waitForTickers(t, clock, 2)
	base := time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)
	var last [2]int64
	for n := 1; n <= 3; n++ {
		for i := range 2 {
			clock.tickers[i].ch <- base.Add(time.Duration(n) * time.Second)
			s := <-rec.events
			<-rec.events
			last[i] = intOf(t, s.sample)
		}
	}
	cancel()
	<-done
	if last[0] != 2999999997-(1<<32) || last[1] != 2999999997 {
		t.Errorf("after three polls int32 = %d and int64 = %d", last[0], last[1])
	}
}

type failingEmitter struct{ err error }

func (f failingEmitter) Sample(sdk.Sample) error { return f.err }
func (f failingEmitter) Answered(string) error   { return f.err }
func (f failingEmitter) Health(bool, string) error {
	return f.err
}

func TestRunReturnsTheEmitterError(t *testing.T) {
	c, clock := configured(t, sectionTwoDevices)
	boom := errors.New("stream ended")
	done := make(chan error, 1)
	go func() { done <- c.Run(context.Background(), "i1", failingEmitter{boom}) }()
	waitForTickers(t, clock, 3)
	clock.tickers[0].ch <- time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)
	if err := <-done; !errors.Is(err, boom) {
		t.Errorf("Run = %v, want the emitter error", err)
	}
}
