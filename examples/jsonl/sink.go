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
	"path/filepath"
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

// target is a configured instance: the file it writes and the switch file.
type target struct {
	path      string
	suspended string
	file      *fileState
}

// fileState is what the sink knows of one file. Every instance configured with that file, and an
// instance reconfigured while one of its sends is still running, shares it: one lock orders
// their appends, so a line answered ACCEPTED is never overwritten by another. It lives as long
// as the process, keyed by path: a file removed, replaced (rotated) or made shorter behind the
// sink's back is noticed at the next send, and then read again (see refresh).
type fileState struct {
	mu sync.Mutex
	// seen holds the message ids in the file: those read when the file was first configured, or
	// read again after it was replaced, and those stored since.
	seen map[string]bool
	// identity is the file the last durable append wrote, and size the size it left it at; nil
	// until the first one.
	identity os.FileInfo
	size     int64
	// dirSynced is set once the parent directory has been fsynced after the file was opened.
	dirSynced bool
	// dirty is set while an append that started writing has not been fsynced; clean is the size
	// of the file before that append, which the next append truncates back to.
	dirty bool
	clean int64
}

// file is what the sink needs of the file it appends to. *os.File implements it; tests replace it
// to make a call fail.
type file interface {
	io.ReaderAt
	io.WriterAt
	Stat() (os.FileInfo, error)
	Truncate(size int64) error
	Sync() error
	Close() error
}

// openFile opens the file for reading and writing, creating it. It does not use O_APPEND: the
// sink writes at an offset it chooses, and on Windows a file opened for appending cannot be
// truncated.
func openFile(path string) (file, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	return f, nil
}

// sink implements sdk.Output.
type sink struct {
	// open, syncDir and readIDs reach the disk; tests replace them.
	open    func(path string) (file, error)
	syncDir func(dir string) error
	readIDs func(path string) (map[string]bool, error)

	mu        sync.Mutex
	instances map[string]*target
	// files is keyed by the cleaned absolute path of each file.
	files map[string]*fileState
}

func newSink() *sink {
	return &sink{open: openFile, syncDir: syncDir, readIDs: readMessageIDs, instances: map[string]*target{}, files: map[string]*fileState{}}
}

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
	state, err := s.fileState(path)
	if err != nil {
		return []sdk.ConfigError{{Path: "config.path", Message: "the file cannot be read"}}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.instances[instance] = &target{path: path, suspended: path + ".suspend", file: state}
	return nil
}

// fileState returns the state of the file at path, reading its message ids the first time the file
// is configured. Later configurations of the same file share that state, whose ids already include
// every line stored since.
func (s *sink) fileState(path string) (*fileState, error) {
	key, err := filepath.Abs(path)
	if err != nil {
		key = filepath.Clean(path)
	}
	s.mu.Lock()
	state, ok := s.files[key]
	s.mu.Unlock()
	if ok {
		return state, nil
	}
	// The file is read without the sink's lock, so that a large file does not hold the sends of
	// the other instances. Two first configurations of one file may both read it; the first to
	// come back is kept.
	seen, err := s.readIDs(path)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if state, ok := s.files[key]; ok {
		return state, nil
	}
	state = &fileState{seen: seen}
	s.files[key] = state
	return state, nil
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
	f := t.file
	f.mu.Lock()
	defer f.mu.Unlock()
	if ctx.Err() != nil {
		return sdk.Outcome{Kind: sdk.OutcomeRetryAfter, RetryAfter: retryDelay}
	}
	if _, err := os.Stat(t.suspended); err == nil {
		return sdk.Outcome{Kind: sdk.OutcomeSuspendAuth}
	}
	if err := s.refresh(t); err != nil {
		return sdk.Outcome{Kind: sdk.OutcomeRetryAfter, RetryAfter: retryDelay}
	}
	if f.seen[b.MessageID] {
		return sdk.Outcome{Kind: sdk.OutcomeDuplicate}
	}
	if reason := invalidReason(b); reason != "" {
		return sdk.Outcome{Kind: sdk.OutcomeRejectInvalid, Reason: reason}
	}
	line, err := encodeBatch(b)
	if err != nil {
		return sdk.Outcome{Kind: sdk.OutcomeRejectInvalid, Reason: "the batch cannot be encoded"}
	}
	if err := s.appendSynced(t, line); err != nil {
		return sdk.Outcome{Kind: sdk.OutcomeRetryAfter, RetryAfter: retryDelay}
	}
	f.seen[b.MessageID] = true
	return sdk.Outcome{Kind: sdk.OutcomeAccepted}
}

// refresh forgets what the sink knows of the file of t when the file is no longer the one it last
// wrote: removed, replaced (for example rotated away and created again) or shorter than the sink
// left it. Its message ids are then read again, and a truncation owed to the old file is dropped,
// so it never extends the new one. The caller holds the lock of the file's state. A file
// replaced between this check and the append is not noticed until the next send.
func (s *sink) refresh(t *target) error {
	state := t.file
	if state.identity == nil {
		return nil
	}
	info, err := os.Stat(t.path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err == nil && os.SameFile(state.identity, info) && info.Size() >= state.size {
		return nil
	}
	seen, err := s.readIDs(t.path)
	if err != nil {
		return err
	}
	state.seen, state.identity, state.size = seen, nil, 0
	state.dirSynced, state.dirty, state.clean = false, false, 0
	return nil
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

// appendSynced appends data to the file of t, creating it, and fsyncs before returning. The caller
// holds the lock of the file's state.
//
// The first append of the target also fsyncs the parent directory, so that a file the sink created
// survives a crash together with its first line. When the previous append failed after it started
// writing (a failed Write or Sync), the file is first truncated back to its size before that
// append: its line may or may not be on disk, the batch was answered RETRY_AFTER, and the agent's
// resend must not add it a second time. While the truncation fails nothing is written. When the
// file ends in a line without a newline (a write cut short by a crash), the newline is written
// first so that the torn line stays alone and the new one is whole.
func (s *sink) appendSynced(t *target, data []byte) (err error) {
	state := t.file
	f, err := s.open(t.path)
	if err != nil {
		return err
	}
	// Once Sync succeeded the line is durable, and a failed Close cannot undo it; before that the
	// append has already failed. Either way the error of Close changes nothing.
	defer func() { _ = f.Close() }()
	if !state.dirSynced {
		if err := s.syncDir(filepath.Dir(t.path)); err != nil {
			return err
		}
		state.dirSynced = true
	}
	if state.dirty {
		if err := f.Truncate(state.clean); err != nil {
			return err
		}
	}
	info, err := f.Stat()
	if err != nil {
		return err
	}
	size := info.Size()
	if size > 0 {
		var last [1]byte
		if _, err := f.ReadAt(last[:], size-1); err != nil {
			return err
		}
		if last[0] != '\n' {
			data = append([]byte{'\n'}, data...)
		}
	}
	if !state.dirty {
		state.clean = size
	}
	state.dirty = true
	if _, err := f.WriteAt(data, size); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	state.dirty = false
	state.identity = info
	state.size = size + int64(len(data))
	return nil
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
				Quality: qualityWord(s.Quality), Priority: priorityWord(s.Priority),
			}})
		case r.Event != nil:
			e := r.Event
			event := &lineEvent{Kind: eventKindWord(e.Kind), Device: e.Device, TS: e.TS.UTC()}
			for _, si := range e.Signals {
				event.Signals = append(event.Signals, lineSignal{Signal: si.Signal, DataType: si.DataType, Unit: si.Unit})
			}
			out.Records = append(out.Records, lineRecord{Event: event})
		}
	}
	for _, l := range b.Losses {
		out.Losses = append(out.Losses, lineLoss{
			Priority: priorityWord(l.Priority), From: l.From.UTC(), To: l.To.UTC(), Count: l.Count, Reason: l.Reason,
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
	case sdk.ValueKindFloat:
		return v.Float, "float"
	case sdk.ValueKindInt:
		return v.Int, "int"
	case sdk.ValueKindBool:
		return v.Bool, "bool"
	default:
		return v.Str, "string"
	}
}

// The words of the enums switch on the constants, so that they do not depend on their order.

func qualityWord(q sdk.Quality) string {
	switch q {
	case sdk.QualityGood:
		return "good"
	case sdk.QualityUncertain:
		return "uncertain"
	case sdk.QualityBad:
		return "bad"
	}
	return "unknown"
}

func priorityWord(p sdk.Priority) string {
	switch p {
	case sdk.PriorityCritical:
		return "critical"
	case sdk.PriorityBulk:
		return "bulk"
	}
	return "unknown"
}

func eventKindWord(k sdk.EventKind) string {
	switch k {
	case sdk.EventBirth:
		return "birth"
	case sdk.EventDeath:
		return "death"
	}
	return "unknown"
}
