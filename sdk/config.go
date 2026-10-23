package sdk

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"
)

// InputConfig is the section of an input as the agent sends it to Configure, with secrets
// resolved. Keys this version does not know are ignored, so a newer agent can add some.
type InputConfig struct {
	ID string
	// Connection is the free connection object of the input, as sent; nil when absent.
	Connection json.RawMessage
	Devices    []Device
}

// Device is a device of an input and the points read from it.
type Device struct {
	Device string
	Points []Point
}

// Point is one signal read from a machine.
type Point struct {
	Signal    string
	Address   string
	Type      string
	ByteOrder string
	Unit      string
	Priority  string
	Read      Read
}

// Read says how a point is read: Mode is "poll", "subscribe" or "event" (empty when the point
// has no read key). Interval is the polling interval of "poll". Subscribe holds the settings
// object of a point read by subscription, nil when there are none.
type Read struct {
	Mode      string
	Interval  time.Duration
	Subscribe json.RawMessage
}

// ProcessorConfig is the section of a processor as the agent sends it to Configure.
type ProcessorConfig struct {
	ID string
	// Observe lists the devices whose records the processor receives.
	Observe []string
	// Devices lists the derived devices and signals the processor may emit.
	Devices []DerivedDevice
	// Config is the free config object of the processor, as sent; nil when absent.
	Config json.RawMessage
}

// DerivedDevice is a device the processor derives from the observed ones.
type DerivedDevice struct {
	Device  string
	Signals []DerivedSignal
}

// DerivedSignal is a signal of a derived device.
type DerivedSignal struct {
	Signal   string
	Type     string
	Priority string
}

// OutputConfig is the section of an output as the agent sends it to Configure.
type OutputConfig struct {
	ID string
	// Config is the free config object of the output, as sent; nil when absent.
	Config json.RawMessage
}

// DecodeInput decodes the configuration an Input receives. The error is a [ConfigError] that
// names the path of the first problem and never a value.
func DecodeInput(cfg json.RawMessage) (InputConfig, error) {
	top, err := decodeObject(cfg, "")
	if err != nil {
		return InputConfig{}, err
	}
	var out InputConfig
	if out.ID, err = fieldString(top, "id", ""); err != nil {
		return InputConfig{}, err
	}
	if raw, ok := top["connection"]; ok && !isNull(raw) {
		out.Connection = append(json.RawMessage(nil), raw...)
	}
	items, err := fieldArray(top, "devices", "")
	if err != nil {
		return InputConfig{}, err
	}
	for i, item := range items {
		device, err := decodeDevice(item, indexPath("devices", i))
		if err != nil {
			return InputConfig{}, err
		}
		out.Devices = append(out.Devices, device)
	}
	return out, nil
}

func decodeDevice(raw json.RawMessage, path string) (Device, error) {
	obj, err := decodeObject(raw, path)
	if err != nil {
		return Device{}, err
	}
	var out Device
	if out.Device, err = fieldString(obj, "device", path); err != nil {
		return Device{}, err
	}
	points, err := fieldArray(obj, "points", path)
	if err != nil {
		return Device{}, err
	}
	for i, item := range points {
		point, err := decodePoint(item, indexPath(joinPath(path, "points"), i))
		if err != nil {
			return Device{}, err
		}
		out.Points = append(out.Points, point)
	}
	return out, nil
}

func decodePoint(raw json.RawMessage, path string) (Point, error) {
	obj, err := decodeObject(raw, path)
	if err != nil {
		return Point{}, err
	}
	var out Point
	for _, f := range []struct {
		key string
		dst *string
	}{
		{"signal", &out.Signal}, {"address", &out.Address}, {"type", &out.Type},
		{"byte_order", &out.ByteOrder}, {"unit", &out.Unit}, {"priority", &out.Priority},
	} {
		if *f.dst, err = fieldString(obj, f.key, path); err != nil {
			return Point{}, err
		}
	}
	if out.Read, err = decodeRead(obj["read"], joinPath(path, "read")); err != nil {
		return Point{}, err
	}
	return out, nil
}

func decodeRead(raw json.RawMessage, path string) (Read, error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || isNull(raw) {
		return Read{}, nil
	}
	switch raw[0] {
	case '"':
		var mode string
		_ = json.Unmarshal(raw, &mode)
		if mode != "subscribe" && mode != "event" {
			return Read{}, ConfigError{Path: path, Message: "must be subscribe, event or an object with poll or subscribe"}
		}
		return Read{Mode: mode}, nil
	case '{':
		obj, err := decodeObject(raw, path)
		if err != nil {
			return Read{}, err
		}
		if sub, ok := obj["subscribe"]; ok && !isNull(sub) {
			if _, err := decodeObject(sub, joinPath(path, "subscribe")); err != nil {
				return Read{}, err
			}
			return Read{Mode: "subscribe", Subscribe: append(json.RawMessage(nil), sub...)}, nil
		}
		if _, ok := obj["poll"]; !ok {
			return Read{}, ConfigError{Path: path, Message: "must have poll or subscribe"}
		}
		text, err := fieldString(obj, "poll", path)
		if err != nil {
			return Read{}, err
		}
		interval, perr := time.ParseDuration(text)
		if perr != nil || interval <= 0 {
			return Read{}, ConfigError{Path: joinPath(path, "poll"), Message: "must be a positive duration such as 500ms, 2s or 1m"}
		}
		return Read{Mode: "poll", Interval: interval}, nil
	default:
		return Read{}, ConfigError{Path: path, Message: "must be a string or an object"}
	}
}

// DecodeProcessor decodes the configuration a Processor receives. The error is a [ConfigError]
// that names the path of the first problem and never a value.
func DecodeProcessor(cfg json.RawMessage) (ProcessorConfig, error) {
	top, err := decodeObject(cfg, "")
	if err != nil {
		return ProcessorConfig{}, err
	}
	var out ProcessorConfig
	if out.ID, err = fieldString(top, "id", ""); err != nil {
		return ProcessorConfig{}, err
	}
	observed, err := fieldArray(top, "observe", "")
	if err != nil {
		return ProcessorConfig{}, err
	}
	for i, item := range observed {
		var name string
		if json.Unmarshal(item, &name) != nil || len(item) == 0 || item[0] != '"' {
			return ProcessorConfig{}, ConfigError{Path: indexPath("observe", i), Message: "must be a string"}
		}
		out.Observe = append(out.Observe, name)
	}
	devices, err := fieldArray(top, "devices", "")
	if err != nil {
		return ProcessorConfig{}, err
	}
	for i, item := range devices {
		device, err := decodeDerivedDevice(item, indexPath("devices", i))
		if err != nil {
			return ProcessorConfig{}, err
		}
		out.Devices = append(out.Devices, device)
	}
	if raw, ok := top["config"]; ok && !isNull(raw) {
		out.Config = append(json.RawMessage(nil), raw...)
	}
	return out, nil
}

func decodeDerivedDevice(raw json.RawMessage, path string) (DerivedDevice, error) {
	obj, err := decodeObject(raw, path)
	if err != nil {
		return DerivedDevice{}, err
	}
	var out DerivedDevice
	if out.Device, err = fieldString(obj, "device", path); err != nil {
		return DerivedDevice{}, err
	}
	signals, err := fieldArray(obj, "signals", path)
	if err != nil {
		return DerivedDevice{}, err
	}
	for i, item := range signals {
		spath := indexPath(joinPath(path, "signals"), i)
		sobj, err := decodeObject(item, spath)
		if err != nil {
			return DerivedDevice{}, err
		}
		var signal DerivedSignal
		for _, f := range []struct {
			key string
			dst *string
		}{{"signal", &signal.Signal}, {"type", &signal.Type}, {"priority", &signal.Priority}} {
			if *f.dst, err = fieldString(sobj, f.key, spath); err != nil {
				return DerivedDevice{}, err
			}
		}
		out.Signals = append(out.Signals, signal)
	}
	return out, nil
}

// DecodeOutput decodes the configuration an Output receives. The error is a [ConfigError] that
// names the path of the first problem and never a value.
func DecodeOutput(cfg json.RawMessage) (OutputConfig, error) {
	top, err := decodeObject(cfg, "")
	if err != nil {
		return OutputConfig{}, err
	}
	var out OutputConfig
	if out.ID, err = fieldString(top, "id", ""); err != nil {
		return OutputConfig{}, err
	}
	if raw, ok := top["config"]; ok && !isNull(raw) {
		out.Config = append(json.RawMessage(nil), raw...)
	}
	return out, nil
}

func isNull(raw json.RawMessage) bool { return string(bytes.TrimSpace(raw)) == "null" }

func joinPath(path, key string) string {
	if path == "" {
		return key
	}
	return path + "." + key
}

func indexPath(path string, i int) string { return fmt.Sprintf("%s[%d]", path, i) }

// decodeObject decodes raw as a JSON object. A null is an empty object.
func decodeObject(raw json.RawMessage, path string) (map[string]json.RawMessage, error) {
	if !json.Valid(raw) {
		return nil, ConfigError{Path: path, Message: "is not valid JSON"}
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil, ConfigError{Path: path, Message: "must be an object"}
	}
	return obj, nil
}

// fieldString reads an optional string member; absent and null give "".
func fieldString(obj map[string]json.RawMessage, key, path string) (string, error) {
	raw, ok := obj[key]
	if !ok || isNull(raw) {
		return "", nil
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return "", ConfigError{Path: joinPath(path, key), Message: "must be a string"}
	}
	return s, nil
}

// fieldArray reads an optional array member; absent and null give no items.
func fieldArray(obj map[string]json.RawMessage, key, path string) ([]json.RawMessage, error) {
	raw, ok := obj[key]
	if !ok || isNull(raw) {
		return nil, nil
	}
	var items []json.RawMessage
	if err := json.Unmarshal(raw, &items); err != nil {
		return nil, ConfigError{Path: joinPath(path, key), Message: "must be an array"}
	}
	return items, nil
}
