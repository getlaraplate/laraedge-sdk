package manifest_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/getlaraplate/laraedge-sdk/manifest"
)

const marker = "SECRET-MARKER-9137"

const fullManifest = `{
  "name": "acme-press",
  "version": "1.2.0",
  "protocol_versions": ["v1"],
  "kinds": {
    "input": {
      "connection_schema": {
        "type": "object",
        "required": ["host"],
        "additionalProperties": false,
        "properties": {
          "host": {"type": "string", "pattern": "^[a-z.]+$"},
          "port": {"type": "integer", "minimum": 1, "maximum": 65535},
          "tls": {"enum": ["off", "on"]}
        }
      },
      "address_schema": {"type": "string", "pattern": "^db[0-9]+\\.[0-9]+$"},
      "read_modes": ["poll", "subscribe"],
      "types": ["bool", "int16", "float32"],
      "browse": true
    },
    "processor": {
      "config_schema": {"type": "object", "required": ["window"], "properties": {"window": {"type": "integer", "minimum": 1}}}
    },
    "output": {
      "config_schema": {"type": "object", "properties": {"topic": {"type": "string", "maxLength": 8}}}
    }
  }
}`

func mustManifest(t *testing.T, doc string) manifest.Manifest {
	t.Helper()
	var m manifest.Manifest
	if err := json.Unmarshal([]byte(doc), &m); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return m
}

func TestLoadRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "plugin.json")
	if err := os.WriteFile(path, []byte(fullManifest), 0o600); err != nil {
		t.Fatal(err)
	}
	m, raw, err := manifest.Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if string(raw) != fullManifest {
		t.Fatal("raw bytes are not the file content")
	}
	if m.Name != "acme-press" || m.Version != "1.2.0" || len(m.ProtocolVersions) != 1 || m.ProtocolVersions[0] != "v1" {
		t.Fatalf("header: %+v", m)
	}
	in := m.Kinds.Input
	if in == nil || !in.Browse || len(in.ReadModes) != 2 || len(in.Types) != 3 || len(in.ConnectionSchema) == 0 || len(in.AddressSchema) == 0 {
		t.Fatalf("input kind: %+v", in)
	}
	if m.Kinds.Processor == nil || m.Kinds.Output == nil {
		t.Fatal("processor or output kind missing")
	}
	if problems := m.Check(); len(problems) != 0 {
		t.Fatalf("Check: %v", problems)
	}

	out, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	want, err := manifest.Canonical([]byte(fullManifest))
	if err != nil {
		t.Fatal(err)
	}
	got, err := manifest.Canonical(out)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Fatalf("round trip differs:\n got %s\nwant %s", got, want)
	}
}

func TestLoadErrors(t *testing.T) {
	if _, _, err := manifest.Load(filepath.Join(t.TempDir(), "missing.json")); err == nil {
		t.Fatal("missing file: want an error")
	}
	path := filepath.Join(t.TempDir(), "plugin.json")
	if err := os.WriteFile(path, []byte(`{"name":`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := manifest.Load(path); err == nil {
		t.Fatal("broken json: want an error")
	}
}

func TestCheck(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(string) string
		want   string // substring of one problem; empty means no problem
	}{
		{"valid", func(s string) string { return s }, ""},
		{"bad name", func(s string) string { return strings.Replace(s, `"acme-press"`, `"Acme Press"`, 1) }, "name"},
		{"name too long", func(s string) string { return strings.Replace(s, `"acme-press"`, `"`+strings.Repeat("a", 65)+`"`, 1) }, "name"},
		{"no version", func(s string) string { return strings.Replace(s, `"version": "1.2.0",`, ``, 1) }, "version"},
		{"empty protocol versions", func(s string) string { return strings.Replace(s, `["v1"]`, `[]`, 1) }, "protocol_versions"},
		{"unknown protocol version", func(s string) string { return strings.Replace(s, `["v1"]`, `["v1","v2"]`, 1) }, "protocol_versions[1]"},
		{"unknown read mode", func(s string) string { return strings.Replace(s, `"poll", "subscribe"`, `"poll", "stream"`, 1) }, "kinds.input.read_modes[1]"},
		{"unknown type", func(s string) string { return strings.Replace(s, `"float32"`, `"decimal"`, 1) }, "kinds.input.types[2]"},
		{"broken connection schema", func(s string) string {
			return strings.Replace(s, `"type": "object",
        "required": ["host"],`, `"type": "nonsense",
        "required": ["host"],`, 1)
		}, "kinds.input.connection_schema"},
		{"broken pattern in address schema", func(s string) string { return strings.Replace(s, `^db[0-9]+\\.[0-9]+$`, `(`, 1) }, "kinds.input.address_schema"},
		{"older draft refused", func(s string) string {
			return strings.Replace(s, `"config_schema": {"type": "object", "required": ["window"]`, `"config_schema": {"$schema": "http://json-schema.org/draft-07/schema#", "type": "object", "required": ["window"]`, 1)
		}, "kinds.processor.config_schema"},
		{"draft 2020 declared is fine", func(s string) string {
			return strings.Replace(s, `"config_schema": {"type": "object", "required": ["window"]`, `"config_schema": {"$schema": "https://json-schema.org/draft/2020-12/schema", "type": "object", "required": ["window"]`, 1)
		}, ""},
		{"external ref refused", func(s string) string {
			return strings.Replace(s, `"topic": {"type": "string", "maxLength": 8}`, `"topic": {"$ref": "file:///etc/passwd"}`, 1)
		}, "kinds.output.config_schema"},
		{"http ref refused", func(s string) string {
			return strings.Replace(s, `"topic": {"type": "string", "maxLength": 8}`, `"topic": {"$ref": "http://example.com/schema.json"}`, 1)
		}, "kinds.output.config_schema"},
		{"local ref is fine", func(s string) string {
			return strings.Replace(s, `"config_schema": {"type": "object", "properties": {"topic": {"type": "string", "maxLength": 8}}}`, `"config_schema": {"$defs": {"topic": {"type": "string", "maxLength": 8}}, "type": "object", "properties": {"topic": {"$ref": "#/$defs/topic"}}}`, 1)
		}, ""},
		{"schema is not an object", func(s string) string {
			return strings.Replace(s, `"config_schema": {"type": "object", "properties": {"topic": {"type": "string", "maxLength": 8}}}`, `"config_schema": 42`, 1)
		}, "kinds.output.config_schema"},
		{"no kinds", func(s string) string {
			var doc map[string]any
			_ = json.Unmarshal([]byte(s), &doc)
			doc["kinds"] = map[string]any{}
			out, _ := json.Marshal(doc)
			return string(out)
		}, "kinds"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			doc := tt.mutate(fullManifest)
			if tt.want != "" && doc == fullManifest {
				t.Fatal("mutation did not change the manifest")
			}
			problems := mustManifest(t, doc).Check()
			if tt.want == "" {
				if len(problems) != 0 {
					t.Fatalf("want no problem, got %v", problems)
				}
				return
			}
			for _, p := range problems {
				if strings.Contains(p, tt.want) {
					return
				}
			}
			t.Fatalf("want a problem containing %q, got %v", tt.want, problems)
		})
	}
}

func TestCheckMissingSchemas(t *testing.T) {
	m := manifest.Manifest{
		Name: "x", Version: "1", ProtocolVersions: []string{"v1"},
		Kinds: manifest.Kinds{
			Input:     &manifest.InputKind{ReadModes: []string{"poll"}, Types: []string{"bool"}},
			Processor: &manifest.ConfigKind{},
		},
	}
	problems := m.Check()
	joined := strings.Join(problems, "|")
	for _, want := range []string{"kinds.input.connection_schema", "kinds.input.address_schema", "kinds.processor.config_schema"} {
		if !strings.Contains(joined, want) {
			t.Errorf("missing problem for %s in %v", want, problems)
		}
	}
}

type problemSet map[string]bool

func (s problemSet) has(path string) bool { return s[path] }

func paths(problems []manifest.Problem) problemSet {
	out := problemSet{}
	for _, p := range problems {
		out[p.Path] = true
	}
	return out
}

func assertNoValue(t *testing.T, problems []manifest.Problem) {
	t.Helper()
	for _, p := range problems {
		if strings.Contains(p.Path, marker) || strings.Contains(p.Message, marker) {
			t.Fatalf("problem leaks a value: %+v", p)
		}
	}
}

func TestValidateInput(t *testing.T) {
	m := mustManifest(t, fullManifest)

	tests := []struct {
		name    string
		section string
		want    []string // paths expected, exactly
	}{
		{"valid", `{"id":"i","driver":"plugin:acme","connection":{"host":"press.local","port":502},"devices":[{"device":"d","points":[{"signal":"s","address":"db1.0","read":{"poll":"1s"},"type":"int16"}]}]}`, nil},
		{"valid without type", `{"connection":{"host":"a"},"devices":[{"device":"d","points":[{"signal":"s","address":"db1.0","read":"subscribe"}]}]}`, nil},
		{"wrong connection field type", `{"connection":{"host":"a","port":"` + marker + `"},"devices":[]}`, []string{"connection.port"}},
		{"connection value out of range", `{"connection":{"host":"a","port":70000},"devices":[]}`, []string{"connection.port"}},
		{"connection value fails pattern", `{"connection":{"host":"` + marker + `"},"devices":[]}`, []string{"connection.host"}},
		{"connection enum", `{"connection":{"host":"a","tls":"` + marker + `"},"devices":[]}`, []string{"connection.tls"}},
		{"connection required field missing", `{"connection":{"port":1},"devices":[]}`, []string{"connection.host"}},
		{"connection absent counts as empty", `{"devices":[]}`, []string{"connection.host"}},
		{"connection unknown field", `{"connection":{"host":"a","extra":"` + marker + `"},"devices":[]}`, []string{"connection"}},
		{"connection wrong shape", `{"connection":"` + marker + `","devices":[]}`, []string{"connection"}},
		{"address fails schema", `{"connection":{"host":"a"},"devices":[{"device":"d","points":[{"signal":"a","address":"db1.0","read":"subscribe"},{"signal":"b","address":"` + marker + `","read":"subscribe"}]}]}`, []string{"devices[0].points[1].address"}},
		{"address in second device", `{"connection":{"host":"a"},"devices":[{"device":"d","points":[{"signal":"a","address":"db1.0","read":"subscribe"}]},{"device":"e","points":[{"signal":"b","address":"db2.0","read":"subscribe"},{"signal":"c","address":"` + marker + `","read":"subscribe"}]}]}`, []string{"devices[1].points[1].address"}},
		{"read mode not accepted: event", `{"connection":{"host":"a"},"devices":[{"device":"d","points":[{"signal":"a","address":"db1.0","read":"event"}]}]}`, []string{"devices[0].points[0].read"}},
		{"read mode accepted: subscribe object", `{"connection":{"host":"a"},"devices":[{"device":"d","points":[{"signal":"a","address":"db1.0","read":"subscribe"},{"signal":"b","address":"db1.1","read":{"subscribe":{}}}]}]}`, nil},
		{"type not accepted", `{"connection":{"host":"a"},"devices":[{"device":"d","points":[{"signal":"a","address":"db1.0","read":"subscribe","type":"uint64"}]}]}`, []string{"devices[0].points[0].type"}},
		{"several at once", `{"connection":{"host":"a","port":0},"devices":[{"device":"d","points":[{"signal":"a","address":"` + marker + `","read":"event","type":"string"}]}]}`, []string{"connection.port", "devices[0].points[0].address", "devices[0].points[0].read", "devices[0].points[0].type"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			problems := m.ValidateInput(json.RawMessage(tt.section))
			assertNoValue(t, problems)
			got := paths(problems)
			if len(got) != len(tt.want) {
				t.Fatalf("want paths %v, got %+v", tt.want, problems)
			}
			for _, w := range tt.want {
				if !got.has(w) {
					t.Fatalf("want path %s, got %+v", w, problems)
				}
			}
			for _, p := range problems {
				if p.Message == "" {
					t.Fatalf("empty message for %s", p.Path)
				}
			}
		})
	}
}

func TestValidateInputSectionErrors(t *testing.T) {
	m := mustManifest(t, fullManifest)
	for name, section := range map[string]string{"not json": `{"connection":` + marker, "array": `[` + marker + `]`, "scalar": `"` + marker + `"`} {
		t.Run(name, func(t *testing.T) {
			problems := m.ValidateInput(json.RawMessage(section))
			if len(problems) != 1 {
				t.Fatalf("want one problem, got %+v", problems)
			}
			assertNoValue(t, problems)
		})
	}
}

func TestValidateKindNotOffered(t *testing.T) {
	m := mustManifest(t, `{"name":"x","version":"1","protocol_versions":["v1"],"kinds":{"output":{"config_schema":{}}}}`)
	if problems := m.ValidateInput(json.RawMessage(`{}`)); len(problems) != 1 {
		t.Fatalf("input: %+v", problems)
	}
	if problems := m.ValidateProcessor(json.RawMessage(`{}`)); len(problems) != 1 {
		t.Fatalf("processor: %+v", problems)
	}
	if problems := m.ValidateOutput(json.RawMessage(`{}`)); len(problems) != 0 {
		t.Fatalf("output: %+v", problems)
	}
}

func TestValidateProcessorAndOutput(t *testing.T) {
	m := mustManifest(t, fullManifest)

	t.Run("processor valid", func(t *testing.T) {
		if problems := m.ValidateProcessor(json.RawMessage(`{"id":"p","config":{"window":5}}`)); len(problems) != 0 {
			t.Fatalf("%+v", problems)
		}
	})
	t.Run("processor wrong type", func(t *testing.T) {
		problems := m.ValidateProcessor(json.RawMessage(`{"id":"p","config":{"window":"` + marker + `"}}`))
		assertNoValue(t, problems)
		if got := paths(problems); len(got) != 1 || !got.has("config.window") {
			t.Fatalf("%+v", problems)
		}
	})
	t.Run("processor required missing and config absent", func(t *testing.T) {
		problems := m.ValidateProcessor(json.RawMessage(`{"id":"p"}`))
		if got := paths(problems); len(got) != 1 || !got.has("config.window") {
			t.Fatalf("%+v", problems)
		}
	})
	t.Run("output too long", func(t *testing.T) {
		problems := m.ValidateOutput(json.RawMessage(`{"id":"o","config":{"topic":"` + marker + `"}}`))
		assertNoValue(t, problems)
		if got := paths(problems); len(got) != 1 || !got.has("config.topic") {
			t.Fatalf("%+v", problems)
		}
	})
	t.Run("output config not an object", func(t *testing.T) {
		problems := m.ValidateOutput(json.RawMessage(`{"id":"o","config":"` + marker + `"}`))
		assertNoValue(t, problems)
		if got := paths(problems); len(got) != 1 || !got.has("config") {
			t.Fatalf("%+v", problems)
		}
	})
	t.Run("output valid", func(t *testing.T) {
		if problems := m.ValidateOutput(json.RawMessage(`{"id":"o","config":{"topic":"t"}}`)); len(problems) != 0 {
			t.Fatalf("%+v", problems)
		}
	})
}

func TestValidateWithUncompilableSchema(t *testing.T) {
	m := mustManifest(t, fullManifest)
	m.Kinds.Output.ConfigSchema = json.RawMessage(`{"type":"nonsense"}`)
	problems := m.ValidateOutput(json.RawMessage(`{"config":{}}`))
	if len(problems) != 1 || problems[0].Path != "kinds.output.config_schema" {
		t.Fatalf("%+v", problems)
	}
}

func TestValidationOrderIsDeterministic(t *testing.T) {
	m := mustManifest(t, `{"name":"x","version":"1","protocol_versions":["v1"],"kinds":{
	  "processor":{"config_schema":{"type":"object","properties":{
	    "a":{"type":"integer"},"b":{"type":"integer"},"c":{"type":"integer"},
	    "d":{"type":"integer"},"e":{"type":"integer"},"f":{"type":"integer"}}}},
	  "input":{"connection_schema":{"type":"object","properties":{
	    "a":{"type":"integer"},"b":{"type":"integer"},"c":{"type":"integer"},
	    "d":{"type":"integer"},"e":{"type":"integer"},"f":{"type":"integer"}}},
	    "address_schema":{"type":"string"},"read_modes":["poll"],"types":["bool"]}}}`)
	const bad = `{"a":"x","b":"x","c":"x","d":"x","e":"x","f":"x"}`
	processor := json.RawMessage(`{"config":` + bad + `}`)
	input := json.RawMessage(`{"connection":` + bad + `,"devices":[]}`)

	wantProcessor := m.ValidateProcessor(processor)
	wantInput := m.ValidateInput(input)
	if len(wantProcessor) != 6 || len(wantInput) != 6 {
		t.Fatalf("want 6 problems each, got %d and %d", len(wantProcessor), len(wantInput))
	}
	for i, p := range wantProcessor {
		if want := "config." + string(rune('a'+i)); p.Path != want {
			t.Fatalf("processor problem %d: path %s, want %s", i, p.Path, want)
		}
	}
	for run := 0; run < 200; run++ {
		if got := m.ValidateProcessor(processor); !reflect.DeepEqual(got, wantProcessor) {
			t.Fatalf("run %d: processor order changed:\n%+v\n%+v", run, got, wantProcessor)
		}
		if got := m.ValidateInput(input); !reflect.DeepEqual(got, wantInput) {
			t.Fatalf("run %d: input order changed:\n%+v\n%+v", run, got, wantInput)
		}
	}
}

func TestLoadRefusesAmbiguousManifests(t *testing.T) {
	tests := []struct{ name, doc string }{
		{"top-level key in other case", `{"Name":"a","version":"1","protocol_versions":["v1"],"kinds":{"output":{"config_schema":{}}}}`},
		{"upper case protocol_versions", `{"name":"a","version":"1","PROTOCOL_VERSIONS":["v1"],"kinds":{"output":{"config_schema":{}}}}`},
		{"duplicate name", `{"name":"a","name":"b","version":"1","protocol_versions":["v1"],"kinds":{"output":{"config_schema":{}}}}`},
		{"kinds key in other case", `{"name":"a","version":"1","protocol_versions":["v1"],"Kinds":{"output":{"config_schema":{}}}}`},
		{"kind name in other case", `{"name":"a","version":"1","protocol_versions":["v1"],"kinds":{"Output":{"config_schema":{}}}}`},
		{"input field in other case", `{"name":"a","version":"1","protocol_versions":["v1"],"kinds":{"input":{"Connection_Schema":{},"address_schema":{},"read_modes":["poll"],"types":["bool"]}}}`},
		{"output field in other case", `{"name":"a","version":"1","protocol_versions":["v1"],"kinds":{"output":{"CONFIG_SCHEMA":{}}}}`},
		{"processor field in other case", `{"name":"a","version":"1","protocol_versions":["v1"],"kinds":{"processor":{"Config_Schema":{}}}}`},
		{"invalid UTF-8", "{\"name\":\"a\xff\",\"version\":\"1\",\"protocol_versions\":[\"v1\"],\"kinds\":{\"output\":{\"config_schema\":{}}}}"},
		{"lone surrogate", `{"name":"a","version":"\ud800","protocol_versions":["v1"],"kinds":{"output":{"config_schema":{}}}}`},
		{"trailing data", `{"name":"a","version":"1","protocol_versions":["v1"],"kinds":{"output":{"config_schema":{}}}} {}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "plugin.json")
			if err := os.WriteFile(path, []byte(tt.doc), 0o600); err != nil {
				t.Fatal(err)
			}
			if m, _, err := manifest.Load(path); err == nil {
				t.Fatalf("want an error, got %+v", m)
			}
		})
	}
}

func TestLoadAcceptsDifferentlyNamedKeysInsideSchemas(t *testing.T) {
	doc := `{"name":"a","version":"1","protocol_versions":["v1"],"extra":1,"kinds":{"output":{"config_schema":{"type":"object","properties":{"Name":{},"name":{}}}}}}`
	path := filepath.Join(t.TempDir(), "plugin.json")
	if err := os.WriteFile(path, []byte(doc), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := manifest.Load(path); err != nil {
		t.Fatalf("Load: %v", err)
	}
}

func TestProblemPathsAreUnambiguous(t *testing.T) {
	m := mustManifest(t, `{"name":"x","version":"1","protocol_versions":["v1"],"kinds":{"processor":{"config_schema":{
	  "type":"object","properties":{
	    "a.b":{"type":"integer"},
	    "a":{"type":"object","properties":{"b":{"type":"integer"}}},
	    "x[0]":{"type":"integer"},
	    "say \"hi\"":{"type":"integer"},
	    "read-timeout":{"type":"integer"},
	    "1st":{"type":"integer"},
	    "two words":{"type":"integer"},
	    "":{"type":"integer"}}}}}}`)
	problems := m.ValidateProcessor(json.RawMessage(`{"config":{"a.b":"v","a":{"b":"v"},"x[0]":"v","say \"hi\"":"v","":"v","read-timeout":"v","1st":"v","two words":"v"}}`))
	want := []string{`config.a.b`, `config[""]`, `config["a.b"]`, `config["say \"hi\""]`, `config["x[0]"]`, `config.read-timeout`, `config.1st`, `config["two words"]`}
	got := paths(problems)
	if len(problems) != len(want) {
		t.Fatalf("want %v, got %+v", want, problems)
	}
	for _, w := range want {
		if !got.has(w) {
			t.Errorf("want path %s, got %+v", w, problems)
		}
	}
}

func TestMissingRequiredKeyPathIsQuoted(t *testing.T) {
	m := mustManifest(t, `{"name":"x","version":"1","protocol_versions":["v1"],"kinds":{"processor":{"config_schema":{
	  "type":"object","required":["a.b","plain"]}}}}`)
	got := paths(m.ValidateProcessor(json.RawMessage(`{"config":{}}`)))
	if len(got) != 2 || !got.has(`config["a.b"]`) || !got.has("config.plain") {
		t.Fatalf("%v", got)
	}
}

func TestProblemsSortIndexesNumerically(t *testing.T) {
	m := mustManifest(t, fullManifest)
	var devices []string
	for i := 0; i < 12; i++ {
		devices = append(devices, `{"device":"d","points":[{"signal":"s","address":"`+marker+`","read":"subscribe"}]}`)
	}
	section := `{"connection":{"host":"a"},"devices":[` + strings.Join(devices, ",") + `]}`
	problems := m.ValidateInput(json.RawMessage(section))
	if len(problems) != 12 {
		t.Fatalf("want 12 problems, got %+v", problems)
	}
	for i, p := range problems {
		if want := fmt.Sprintf("devices[%d].points[0].address", i); p.Path != want {
			t.Fatalf("problem %d: path %s, want %s", i, p.Path, want)
		}
	}
}

// TestDependentSchemasHasAReadableMessage pins current library behaviour: jsonschema v6 reports a
// failed dependentSchemas by the leaf of the dependent schema (here false, which has no keyword),
// never by dependentSchemas itself, so the message is the generic one.
func TestDependentSchemasHasAReadableMessage(t *testing.T) {
	m := mustManifest(t, `{"name":"x","version":"1","protocol_versions":["v1"],"kinds":{"processor":{"config_schema":{
	  "type":"object","dependentSchemas":{"credit":false}}}}}`)
	problems := m.ValidateProcessor(json.RawMessage(`{"config":{"credit":"` + marker + `"}}`))
	assertNoValue(t, problems)
	if len(problems) == 0 {
		t.Fatal("want a problem")
	}
	for _, p := range problems {
		if p.Message != "value does not satisfy the schema" {
			t.Errorf("problem %+v, want the message %q", p, "value does not satisfy the schema")
		}
	}
}

func TestValidateLocalRefAndSubscribeOnlyKind(t *testing.T) {
	m := mustManifest(t, `{"name":"x","version":"1","protocol_versions":["v1"],"kinds":{
	  "output":{"config_schema":{"$defs":{"topic":{"type":"string","maxLength":2}},"type":"object","properties":{"topic":{"$ref":"#/$defs/topic"}}}},
	  "input":{"connection_schema":{"type":"object"},"address_schema":{"type":"string"},"read_modes":["subscribe"],"types":["bool"]}}}`)

	problems := m.ValidateOutput(json.RawMessage(`{"config":{"topic":"` + marker + `"}}`))
	assertNoValue(t, problems)
	if got := paths(problems); len(got) != 1 || !got.has("config.topic") {
		t.Fatalf("local ref: %+v", problems)
	}

	const point = `{"connection":{},"devices":[{"device":"d","points":[{"signal":"s","address":"a","read":%s}]}]}`
	for read, wantProblem := range map[string]bool{
		`"poll"`: true, `{"poll":"1s"}`: true, `"event"`: true,
		`"subscribe"`: false, `{"subscribe":{}}`: false,
	} {
		problems := m.ValidateInput(json.RawMessage(fmt.Sprintf(point, read)))
		if wantProblem != (len(problems) == 1 && problems[0].Path == "devices[0].points[0].read") {
			t.Errorf("read %s: want problem %v, got %+v", read, wantProblem, problems)
		}
	}
}

func TestNoConfigValueInAnyMessage(t *testing.T) {
	m := mustManifest(t, `{"name":"x","version":"1","protocol_versions":["v1"],"kinds":{"processor":{"config_schema":{
	  "type":"object",
	  "properties":{
	    "t":{"type":"integer"},"e":{"enum":["a"]},"c":{"const":"a"},"p":{"type":"string","pattern":"^a$"},
	    "l":{"type":"string","maxLength":1},"n":{"type":"integer","maximum":1},"m":{"type":"integer","multipleOf":7},
	    "u":{"type":"array","uniqueItems":true},"i":{"type":"array","maxItems":1},"k":{"type":"object","additionalProperties":false},
	    "d":{"type":"object","dependentRequired":{"x":["y"]},"dependentSchemas":{"x":{"required":["z"]}}},
	    "f":{"not":{}}
	  }}}}}`)
	section := `{"config":{"t":"` + marker + `","e":"` + marker + `","c":"` + marker + `","p":"` + marker + `",
	  "l":"` + marker + `","n":1000,"m":8,"u":["` + marker + `","` + marker + `"],"i":["` + marker + `","` + marker + `"],
	  "k":{"extra":"` + marker + `"},"d":{"x":"` + marker + `"},"f":"` + marker + `"}}`
	problems := m.ValidateProcessor(json.RawMessage(section))
	if len(problems) < 10 {
		t.Fatalf("want many problems, got %+v", problems)
	}
	assertNoValue(t, problems)
}
