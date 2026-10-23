package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/getlaraplate/laraedge-sdk/manifest"
	"github.com/getlaraplate/laraedge-sdk/sdk"
)

const (
	pluginName    = "simulated-counter"
	pluginVersion = "0.1.0"
)

// inputKind is the Input section of plugin.json: no connection settings, the address syntax
// counter:<n> with n between 1 and 999999999, polling only, two integer types, browse supported.
func inputKind() manifest.InputKind {
	return manifest.InputKind{
		ConnectionSchema: json.RawMessage(`{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","additionalProperties":false}`),
		AddressSchema:    json.RawMessage(`{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"string","pattern":"^counter:[1-9][0-9]{0,8}$"}`),
		ReadModes:        []string{"poll"},
		Types:            []string{"int32", "int64"},
		Browse:           true,
	}
}

// ticker is the clock of a poll loop. Production uses time.Ticker; tests drive it by hand.
type ticker interface {
	C() <-chan time.Time
	Stop()
}

type realTicker struct{ *time.Ticker }

func (r realTicker) C() <-chan time.Time { return r.Ticker.C }

func newRealTicker(d time.Duration) ticker { return realTicker{time.NewTicker(d)} }

// point is one configured counter.
type point struct {
	device    string
	signal    string
	typ       string
	increment int64
	interval  time.Duration
	priority  sdk.Priority
}

func (p point) address() string { return "counter:" + strconv.FormatInt(p.increment, 10) }

// value is the counter after polls polls. An int32 counter wraps around like a 32 bit register.
func (p point) value(polls int64) sdk.Value {
	v := polls * p.increment
	if p.typ == "int32" {
		v = int64(int32(v))
	}
	return sdk.IntValue(v)
}

func (p point) sample(polls int64, at time.Time) sdk.Sample {
	return sdk.Sample{Device: p.device, Signal: p.signal, TS: at, Value: p.value(polls), Quality: sdk.QualityGood, Priority: p.priority}
}

// counter implements sdk.Input and sdk.Browser. The points of an instance are fixed by Configure;
// the counters themselves live in Run, so every Run starts from zero.
type counter struct {
	// newTicker makes the clock of one point; tests replace it.
	newTicker func(time.Duration) ticker

	mu        sync.Mutex
	instances map[string][]point
}

func newCounter() *counter {
	return &counter{newTicker: newRealTicker, instances: map[string][]point{}}
}

func (c *counter) Configure(_ context.Context, instance string, cfg json.RawMessage) []sdk.ConfigError {
	points, err := parseSection(cfg)
	if err != nil {
		return []sdk.ConfigError{*err}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.instances[instance] = points
	return nil
}

// parseSection decodes and checks an input section. It reports the first problem, by path and
// never with a value.
func parseSection(cfg json.RawMessage) ([]point, *sdk.ConfigError) {
	section, err := sdk.DecodeInput(cfg)
	if err != nil {
		var ce sdk.ConfigError
		if !errors.As(err, &ce) {
			ce = sdk.ConfigError{Message: "the section cannot be decoded"}
		}
		return nil, &ce
	}
	fail := func(path, message string) ([]point, *sdk.ConfigError) {
		return nil, &sdk.ConfigError{Path: path, Message: message}
	}
	if len(section.Devices) == 0 {
		return fail("devices", "must list at least one device")
	}
	var points []point
	for i, d := range section.Devices {
		dpath := fmt.Sprintf("devices[%d]", i)
		if d.Device == "" {
			return fail(dpath+".device", "is required")
		}
		if len(d.Points) == 0 {
			return fail(dpath+".points", "must list at least one point")
		}
		seen := map[string]bool{}
		for j, p := range d.Points {
			ppath := fmt.Sprintf("%s.points[%d]", dpath, j)
			switch {
			case p.Signal == "":
				return fail(ppath+".signal", "is required")
			case seen[p.Signal]:
				return fail(ppath+".signal", "is used twice in this device")
			}
			seen[p.Signal] = true
			increment, ok := parseAddress(p.Address)
			if !ok {
				return fail(ppath+".address", "must be counter:<n> with n between 1 and 999999999")
			}
			if p.Type != "int32" && p.Type != "int64" {
				return fail(ppath+".type", "must be int32 or int64")
			}
			if p.Read.Mode != "poll" {
				return fail(ppath+".read", "must be an object with poll")
			}
			priority, ok := parsePriority(p.Priority)
			if !ok {
				return fail(ppath+".priority", "must be critical or bulk")
			}
			points = append(points, point{
				device: d.Device, signal: p.Signal, typ: p.Type,
				increment: increment, interval: p.Read.Interval, priority: priority,
			})
		}
	}
	return points, nil
}

// parseAddress returns n of counter:<n>.
func parseAddress(address string) (int64, bool) {
	digits, ok := strings.CutPrefix(address, "counter:")
	if !ok || digits == "" || len(digits) > 9 || digits[0] == '0' {
		return 0, false
	}
	n, err := strconv.ParseInt(digits, 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

// parsePriority maps the priority of a point; a point without one is bulk.
func parsePriority(s string) (sdk.Priority, bool) {
	switch s {
	case "", "bulk":
		return sdk.PriorityBulk, true
	case "critical":
		return sdk.PriorityCritical, true
	}
	return 0, false
}

func (c *counter) points(instance string) ([]point, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	points, ok := c.instances[instance]
	if !ok {
		return nil, errors.New("instance is not configured")
	}
	return points, nil
}

// Probe reports every point as its first poll would, and consumes nothing.
func (c *counter) Probe(ctx context.Context, instance string) ([]sdk.Sample, error) {
	points, err := c.points(instance)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	samples := make([]sdk.Sample, 0, len(points))
	for _, p := range points {
		samples = append(samples, p.sample(1, now))
	}
	return samples, ctx.Err()
}

// Browse lists one node per configured point under the root. A point has no children.
func (c *counter) Browse(_ context.Context, instance, node string, _ int) ([]sdk.Node, error) {
	points, err := c.points(instance)
	if err != nil {
		return nil, err
	}
	nodes := []sdk.Node{}
	if node != "" {
		return nodes, nil
	}
	for _, p := range points {
		nodes = append(nodes, sdk.Node{Address: p.address(), Name: p.device + "/" + p.signal, Type: p.typ, Access: "read"})
	}
	return nodes, nil
}

// Run polls every point at its own interval until ctx is cancelled. Each poll reports the sample
// and then that the device answered. The tickers are created before any poll starts, in the order
// of the points.
func (c *counter) Run(ctx context.Context, instance string, out sdk.InputEmitter) error {
	points, err := c.points(instance)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	tickers := make([]ticker, len(points))
	for i, p := range points {
		tickers[i] = c.newTicker(p.interval)
		defer tickers[i].Stop()
	}
	var (
		wg    sync.WaitGroup
		once  sync.Once
		first error
	)
	for i, p := range points {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := poll(ctx, p, tickers[i], out); err != nil {
				once.Do(func() { first = err })
				cancel()
			}
		}()
	}
	wg.Wait()
	return first
}

// poll is the loop of one point. It returns nil when ctx is cancelled and the error of the
// emitter when the stream to the agent has ended.
func poll(ctx context.Context, p point, t ticker, out sdk.InputEmitter) error {
	var polls int64
	for {
		select {
		case <-ctx.Done():
			return nil
		case at := <-t.C():
			polls++
			err := out.Sample(p.sample(polls, at))
			if err == nil {
				err = out.Answered(p.device)
			}
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}
				return err
			}
		}
	}
}

// Stop has nothing to release: the instance keeps no connection. It tolerates any instance.
func (c *counter) Stop(context.Context, string) error { return nil }
