package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/getlaraplate/laraedge-sdk/manifest"
	"github.com/getlaraplate/laraedge-sdk/sdk"
)

const (
	pluginName    = "line-state"
	pluginVersion = "0.1.0"

	// stateSignal is the signal read from every observed device and the one emitted by the
	// derived device.
	stateSignal = "state"
)

// processorKind is the Processor section of plugin.json: the plugin takes no settings of its own,
// so config is an empty object.
func processorKind() manifest.ConfigKind {
	return manifest.ConfigKind{
		ConfigSchema: json.RawMessage(`{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","additionalProperties":false}`),
	}
}

// settings is a configured instance.
type settings struct {
	observed map[string]bool
	derived  string
	priority sdk.Priority
}

// processor implements sdk.Processor. The state of one stream (the last state of each device and
// the last maximum emitted) lives in Process, so a new stream starts without history.
type processor struct {
	mu        sync.Mutex
	instances map[string]settings
}

func newProcessor() *processor { return &processor{instances: map[string]settings{}} }

func (p *processor) Configure(_ context.Context, instance string, cfg json.RawMessage) []sdk.ConfigError {
	s, ce := parseSection(cfg)
	if ce != nil {
		return []sdk.ConfigError{*ce}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.instances[instance] = s
	return nil
}

// parseSection decodes and checks a processor section. It reports the first problem, by path and
// never with a value. The section must observe at least one device and declare exactly one
// derived device with an integer state signal.
func parseSection(cfg json.RawMessage) (settings, *sdk.ConfigError) {
	section, err := sdk.DecodeProcessor(cfg)
	if err != nil {
		var ce sdk.ConfigError
		if !errors.As(err, &ce) {
			ce = sdk.ConfigError{Message: "the section cannot be decoded"}
		}
		return settings{}, &ce
	}
	fail := func(path, message string) (settings, *sdk.ConfigError) {
		return settings{}, &sdk.ConfigError{Path: path, Message: message}
	}
	if len(section.Observe) == 0 {
		return fail("observe", "must list at least one device")
	}
	s := settings{observed: map[string]bool{}}
	for i, name := range section.Observe {
		path := fmt.Sprintf("observe[%d]", i)
		if name == "" {
			return fail(path, "must not be empty")
		}
		if s.observed[name] {
			return fail(path, "is listed twice")
		}
		s.observed[name] = true
	}
	if len(section.Devices) != 1 {
		return fail("devices", "must declare exactly one derived device")
	}
	device := section.Devices[0]
	if device.Device == "" {
		return fail("devices[0].device", "is required")
	}
	if s.observed[device.Device] {
		return fail("devices[0].device", "must not be one of the observed devices")
	}
	s.derived = device.Device
	s.priority = sdk.Critical
	found := false
	for i, sig := range device.Signals {
		if sig.Signal != stateSignal {
			continue
		}
		found = true
		path := fmt.Sprintf("devices[0].signals[%d]", i)
		if sig.Type != "int" {
			return fail(path+".type", "must be int")
		}
		switch sig.Priority {
		case "", "critical":
		case "bulk":
			s.priority = sdk.Bulk
		default:
			return fail(path+".priority", "must be critical or bulk")
		}
	}
	if !found {
		return fail("devices[0].signals", "must declare the state signal")
	}
	if len(section.Config) > 0 {
		var members map[string]json.RawMessage
		if json.Unmarshal(section.Config, &members) != nil || len(members) > 0 {
			return fail("config", "must be an empty object")
		}
	}
	return s, nil
}

func (p *processor) settings(instance string) (settings, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	s, ok := p.instances[instance]
	if !ok {
		return settings{}, errors.New("instance is not configured")
	}
	return s, nil
}

// Process keeps the last state of every observed device and emits the derived state whenever the
// maximum changes, with the timestamp of the sample that changed it.
//
// What it ignores: samples of other signals or of devices that are not observed, samples whose
// value is not an integer, samples of quality Bad (Uncertain is used), and device events. An
// ignored sample leaves the previous state of its device in place, so a device that stops
// reporting keeps counting at its last state.
func (p *processor) Process(ctx context.Context, instance string, in <-chan sdk.Record, out func(sdk.Sample) error) error {
	cfg, err := p.settings(instance)
	if err != nil {
		return err
	}
	last := map[string]int64{}
	var emitted int64
	var hasEmitted bool
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case rec, open := <-in:
			if !open {
				return nil
			}
			s := rec.Sample
			if s == nil || s.Signal != stateSignal || !cfg.observed[s.Device] || s.Quality == sdk.Bad || s.Value.Kind != sdk.KindInt {
				continue
			}
			last[s.Device] = s.Value.Int
			highest := s.Value.Int
			for _, v := range last {
				highest = max(highest, v)
			}
			if hasEmitted && highest == emitted {
				continue
			}
			emitted, hasEmitted = highest, true
			derived := sdk.Sample{
				Device: cfg.derived, Signal: stateSignal, TS: s.TS,
				Value: sdk.IntValue(highest), Quality: sdk.Good, Priority: cfg.priority,
			}
			if err := out(derived); err != nil {
				return err
			}
		}
	}
}
