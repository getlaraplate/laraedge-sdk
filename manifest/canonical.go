package manifest

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
)

// maxCanonicalDepth bounds the nesting Canonical accepts.
const maxCanonicalDepth = 512

// Canonical returns the canonical form of a JSON document (decision S2): object keys sorted
// at every depth in the byte order of their UTF-8 encoding, no insignificant whitespace,
// numbers exactly as written, strings in UTF-8 with only the escapes JSON requires. It
// refuses a document that is not exactly one JSON value, that repeats a key in an object
// or that nests deeper than 512 levels.
func Canonical(raw []byte) ([]byte, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()

	var out bytes.Buffer
	if err := writeCanonical(dec, &out, 0); err != nil {
		return nil, err
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, errors.New("canonical json: data after the first value")
		}
		return nil, fmt.Errorf("canonical json: %w", err)
	}
	return out.Bytes(), nil
}

func writeCanonical(dec *json.Decoder, out *bytes.Buffer, depth int) error {
	if depth > maxCanonicalDepth {
		return errors.New("canonical json: nesting too deep")
	}
	tok, err := dec.Token()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return errors.New("canonical json: empty document")
		}
		return fmt.Errorf("canonical json: %w", err)
	}

	switch t := tok.(type) {
	case json.Delim:
		if t == '{' {
			return writeCanonicalObject(dec, out, depth)
		}
		return writeCanonicalArray(dec, out, depth)
	case string:
		writeCanonicalString(out, t)
	case json.Number:
		out.WriteString(t.String())
	case bool:
		if t {
			out.WriteString("true")
		} else {
			out.WriteString("false")
		}
	case nil:
		out.WriteString("null")
	default:
		return fmt.Errorf("canonical json: unexpected token %T", tok)
	}
	return nil
}

func writeCanonicalObject(dec *json.Decoder, out *bytes.Buffer, depth int) error {
	members := make(map[string][]byte)
	for dec.More() {
		keyTok, err := dec.Token()
		if err != nil {
			return fmt.Errorf("canonical json: %w", err)
		}
		key, ok := keyTok.(string)
		if !ok {
			return errors.New("canonical json: object key is not a string")
		}
		if _, dup := members[key]; dup {
			return errors.New("canonical json: duplicate object key")
		}
		var value bytes.Buffer
		if err := writeCanonical(dec, &value, depth+1); err != nil {
			return err
		}
		members[key] = value.Bytes()
	}
	if _, err := dec.Token(); err != nil { // the closing brace
		return fmt.Errorf("canonical json: %w", err)
	}

	keys := make([]string, 0, len(members))
	for key := range members {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	out.WriteByte('{')
	for i, key := range keys {
		if i > 0 {
			out.WriteByte(',')
		}
		writeCanonicalString(out, key)
		out.WriteByte(':')
		out.Write(members[key])
	}
	out.WriteByte('}')
	return nil
}

func writeCanonicalArray(dec *json.Decoder, out *bytes.Buffer, depth int) error {
	out.WriteByte('[')
	for first := true; dec.More(); first = false {
		if !first {
			out.WriteByte(',')
		}
		if err := writeCanonical(dec, out, depth+1); err != nil {
			return err
		}
	}
	if _, err := dec.Token(); err != nil { // the closing bracket
		return fmt.Errorf("canonical json: %w", err)
	}
	out.WriteByte(']')
	return nil
}

const hexDigits = "0123456789abcdef"

func writeCanonicalString(out *bytes.Buffer, s string) {
	out.WriteByte('"')
	for _, r := range s {
		switch {
		case r == '"':
			out.WriteString(`\"`)
		case r == '\\':
			out.WriteString(`\\`)
		case r == '\b':
			out.WriteString(`\b`)
		case r == '\f':
			out.WriteString(`\f`)
		case r == '\n':
			out.WriteString(`\n`)
		case r == '\r':
			out.WriteString(`\r`)
		case r == '\t':
			out.WriteString(`\t`)
		case r < 0x20:
			out.WriteString(`\u00`)
			out.WriteByte(hexDigits[r>>4])
			out.WriteByte(hexDigits[r&0xf])
		default:
			out.WriteString(string(r))
		}
	}
	out.WriteByte('"')
}
