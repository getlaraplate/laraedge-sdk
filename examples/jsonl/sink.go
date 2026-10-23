package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/getlaraplate/laraedge-sdk/manifest"
	"github.com/getlaraplate/laraedge-sdk/sdk"
)

const (
	pluginName    = "jsonl"
	pluginVersion = "0.1.0"

	// retryDelay is what the agent is asked to wait after a failed write.
	retryDelay = 5 * time.Second
)

// outputKind is the Output section of plugin.json: a config object with a required path.
func outputKind() manifest.ConfigKind {
	return manifest.ConfigKind{
		ConfigSchema: json.RawMessage(`{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","properties":{"path":{"type":"string","minLength":1}},"required":["path"],"additionalProperties":false}`),
	}
}

// target is a configured instance: the file, the message ids it holds and the switch file.
type target struct {
	mu        sync.Mutex
	path      string
	seen      map[string]bool
	suspended string
}

// sink implements sdk.Output.
type sink struct {
	mu        sync.Mutex
	instances map[string]*target
}

func newSink() *sink { return &sink{instances: map[string]*target{}} }

func (s *sink) Configure(_ context.Context, instance string, cfg json.RawMessage) []sdk.ConfigError {
	section, err := sdk.DecodeOutput(cfg)
	if err != nil {
		var ce sdk.ConfigError
		if !errors.As(err, &ce) {
			ce = sdk.ConfigError{Message: "the section cannot be decoded"}
		}
		return []sdk.ConfigError{ce}
	}
	path, ce := parseConfig(section.Config)
	if ce != nil {
		return []sdk.ConfigError{*ce}
	}
	seen, err := readMessageIDs(path)
	if err != nil {
		return []sdk.ConfigError{{Path: "config.path", Message: "the file cannot be read"}}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.instances[instance] = &target{path: path, seen: seen, suspended: path + ".suspend"}
	return nil
}

// parseConfig returns the path of a config object, or the first problem.
func parseConfig(raw json.RawMessage) (string, *sdk.ConfigError) {
	var members map[string]json.RawMessage
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &members)
	}
	rawPath, ok := members["path"]
	if !ok {
		return "", &sdk.ConfigError{Path: "config.path", Message: "is required"}
	}
	var path string
	if json.Unmarshal(rawPath, &path) != nil || path == "" {
		return "", &sdk.ConfigError{Path: "config.path", Message: "must be a non-empty string"}
	}
	if len(members) > 1 {
		return "", &sdk.ConfigError{Path: "config", Message: "has a key this plugin does not know"}
	}
	return path, nil
}

// readMessageIDs returns the message ids already in the file. A line that is not a batch (for
// example one cut short by a crash) is skipped. A file that does not exist holds nothing.
func readMessageIDs(path string) (map[string]bool, error) {
	seen := map[string]bool{}
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return seen, nil
	}
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	r := bufio.NewReader(f)
	for {
		line, err := r.ReadBytes('\n')
		var head struct {
			MessageID string `json:"message_id"`
		}
		if json.Unmarshal(line, &head) == nil && head.MessageID != "" {
			seen[head.MessageID] = true
		}
		if errors.Is(err, io.EOF) {
			return seen, nil
		}
		if err != nil {
			return nil, err
		}
	}
}

func (s *sink) target(instance string) *target {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.instances[instance]
}

// Send answers, in this order: SUSPEND_AUTH while <path>.suspend exists, DUPLICATE for a message
// id already stored, REJECT_INVALID for a batch without an id or with a record without a device,
// RETRY_AFTER when the file cannot be written, and ACCEPTED once the line is on disk.
func (s *sink) Send(ctx context.Context, instance string, b sdk.Batch) sdk.Outcome {
	t := s.target(instance)
	if t == nil {
		return sdk.Outcome{Kind: sdk.OutcomeRejectInvalid, Reason: "instance is not configured"}
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if ctx.Err() != nil {
		return sdk.Outcome{Kind: sdk.OutcomeRetryAfter, RetryAfter: retryDelay}
	}
	if _, err := os.Stat(t.suspended); err == nil {
		return sdk.Outcome{Kind: sdk.OutcomeSuspendAuth}
	}
	if t.seen[b.MessageID] {
		return sdk.Outcome{Kind: sdk.OutcomeDuplicate}
	}
	if reason := invalidReason(b); reason != "" {
		return sdk.Outcome{Kind: sdk.OutcomeRejectInvalid, Reason: reason}
	}
	line, err := encodeBatch(b)
	if err != nil {
		return sdk.Outcome{Kind: sdk.OutcomeRejectInvalid, Reason: "the batch cannot be encoded"}
	}
	if err := appendSynced(t.path, line); err != nil {
		return sdk.Outcome{Kind: sdk.OutcomeRetryAfter, RetryAfter: retryDelay}
	}
	t.seen[b.MessageID] = true
	return sdk.Outcome{Kind: sdk.OutcomeAccepted}
}

// invalidReason says why the batch cannot be stored, without quoting anything from it.
func invalidReason(b sdk.Batch) string {
	if b.MessageID == "" {
		return "the batch has no message_id"
	}
	for i, r := range b.Records {
		switch {
		case r.Sample != nil && r.Sample.Device == "":
			return fmt.Sprintf("record %d has no device", i)
		case r.Event != nil && r.Event.Device == "":
			return fmt.Sprintf("record %d has no device", i)
		case r.Sample == nil && r.Event == nil:
			return fmt.Sprintf("record %d has neither a sample nor an event", i)
		}
	}
	return ""
}

// appendSynced appends data to the file, creating it, and fsyncs before returning. When the file
// ends in a line without a newline (a write cut short by a crash or a failure), the newline is
// written first so that the torn line stays alone and the new one is whole.
func appendSynced(path string, data []byte) (err error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := f.Close(); err == nil {
			err = cerr
		}
	}()
	info, err := f.Stat()
	if err != nil {
		return err
	}
	if size := info.Size(); size > 0 {
		var last [1]byte
		if _, err := f.ReadAt(last[:], size-1); err != nil {
			return err
		}
		if last[0] != '\n' {
			data = append([]byte{'\n'}, data...)
		}
	}
	if _, err := f.Write(data); err != nil {
		return err
	}
	return f.Sync()
}

// The line format. Times are RFC 3339 in UTC, qualities and priorities are words, a value carries
// its kind next to it because JSON does not tell 1 from 1.0.

type lineBatch struct {
	MessageID string       `json:"message_id"`
	SourceSeq uint64       `json:"source_seq"`
	Records   []lineRecord `json:"records"`
	Losses    []lineLoss   `json:"losses"`
}

type lineRecord struct {
	Sample *lineSample `json:"sample,omitempty"`
	Event  *lineEvent  `json:"event,omitempty"`
}

type lineSample struct {
	Device    string    `json:"device"`
	Signal    string    `json:"signal"`
	TS        time.Time `json:"ts"`
	Value     any       `json:"value"`
	ValueType string    `json:"value_type"`
	Quality   string    `json:"quality"`
	Priority  string    `json:"priority"`
}

type lineEvent struct {
	Kind    string       `json:"kind"`
	Device  string       `json:"device"`
	TS      time.Time    `json:"ts"`
	Signals []lineSignal `json:"signals,omitempty"`
}

type lineSignal struct {
	Signal   string `json:"signal"`
	DataType string `json:"data_type"`
	Unit     string `json:"unit,omitempty"`
}

type lineLoss struct {
	Priority string    `json:"priority"`
	From     time.Time `json:"from"`
	To       time.Time `json:"to"`
	Count    uint64    `json:"count"`
	Reason   string    `json:"reason"`
}

// encodeBatch renders a batch as one line, newline included.
func encodeBatch(b sdk.Batch) ([]byte, error) {
	out := lineBatch{MessageID: b.MessageID, SourceSeq: b.SourceSeq, Records: []lineRecord{}, Losses: []lineLoss{}}
	for _, r := range b.Records {
		switch {
		case r.Sample != nil:
			s := r.Sample
			value, valueType := plainValue(s.Value)
			out.Records = append(out.Records, lineRecord{Sample: &lineSample{
				Device: s.Device, Signal: s.Signal, TS: s.TS.UTC(), Value: value, ValueType: valueType,
				Quality: word(int(s.Quality), "good", "uncertain", "bad"), Priority: word(int(s.Priority), "critical", "bulk"),
			}})
		case r.Event != nil:
			e := r.Event
			event := &lineEvent{Kind: word(int(e.Kind), "birth", "death"), Device: e.Device, TS: e.TS.UTC()}
			for _, si := range e.Signals {
				event.Signals = append(event.Signals, lineSignal{Signal: si.Signal, DataType: si.DataType, Unit: si.Unit})
			}
			out.Records = append(out.Records, lineRecord{Event: event})
		}
	}
	for _, l := range b.Losses {
		out.Losses = append(out.Losses, lineLoss{
			Priority: word(int(l.Priority), "critical", "bulk"), From: l.From.UTC(), To: l.To.UTC(), Count: l.Count, Reason: l.Reason,
		})
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf) // Encode ends the line
	enc.SetEscapeHTML(false)
	if err := enc.Encode(out); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func plainValue(v sdk.Value) (any, string) {
	switch v.Kind {
	case sdk.KindFloat:
		return v.Float, "float"
	case sdk.KindInt:
		return v.Int, "int"
	case sdk.KindBool:
		return v.Bool, "bool"
	default:
		return v.Str, "string"
	}
}

// word names the n-th (from 1) of names, or "unknown".
func word(n int, names ...string) string {
	if n < 1 || n > len(names) {
		return "unknown"
	}
	return names[n-1]
}
