// Package manifest models and checks a plugin's plugin.json (design spec section 5): what
// kinds the plugin offers and the JSON Schemas its configuration must satisfy. It also does
// the offline validation of a configuration section against those schemas, without
// starting the plugin. Messages name a path and never a value.
package manifest

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/santhosh-tekuri/jsonschema/v6/kind"
)

// Manifest is the content of plugin.json.
type Manifest struct {
	Name             string   `json:"name"`
	Version          string   `json:"version"`
	ProtocolVersions []string `json:"protocol_versions"`
	Kinds            Kinds    `json:"kinds"`
}

// Kinds lists the component kinds a plugin offers; a nil kind is not offered.
type Kinds struct {
	Input     *InputKind  `json:"input,omitempty"`
	Processor *ConfigKind `json:"processor,omitempty"`
	Output    *ConfigKind `json:"output,omitempty"`
}

// InputKind describes an input: the schema of its free connection object, the schema of a
// point's address string, the read modes and point types it accepts, and whether it serves
// browse.
type InputKind struct {
	ConnectionSchema json.RawMessage `json:"connection_schema,omitempty"`
	AddressSchema    json.RawMessage `json:"address_schema,omitempty"`
	ReadModes        []string        `json:"read_modes"`
	Types            []string        `json:"types"`
	Browse           bool            `json:"browse"`
}

// ConfigKind describes a processor or an output: the schema of its config object.
type ConfigKind struct {
	ConfigSchema json.RawMessage `json:"config_schema,omitempty"`
}

// Problem is one offline validation finding: a path into the section and a fixed message.
// It never carries a value from the section.
type Problem struct {
	Path    string
	Message string
}

// ReadModes and PointTypes are the values the agent's configuration schema knows.
var (
	allReadModes = []string{"poll", "subscribe", "event"}
	allTypes     = []string{"bool", "int16", "uint16", "int32", "uint32", "int64", "uint64", "float32", "float64", "string"}
)

const draft2020URL = "https://json-schema.org/draft/2020-12/schema"

var namePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,63}$`)

// Load reads and decodes the manifest at path. It also returns the raw bytes, which the
// caller hashes.
func Load(path string) (Manifest, []byte, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Manifest{}, nil, fmt.Errorf("read manifest: %w", err)
	}
	var m Manifest
	if err := json.Unmarshal(raw, &m); err != nil {
		return Manifest{}, nil, fmt.Errorf("decode manifest %s: %w", path, err)
	}
	return m, raw, nil
}

// Check returns the reasons the manifest is not acceptable, empty when it is.
func (m Manifest) Check() []string {
	var problems []string
	add := func(format string, args ...any) { problems = append(problems, fmt.Sprintf(format, args...)) }

	if !namePattern.MatchString(m.Name) {
		add("name: must match %s", namePattern.String())
	}
	if m.Version == "" {
		add("version: is required")
	}
	if len(m.ProtocolVersions) == 0 {
		add("protocol_versions: must list at least one version")
	}
	for i, v := range m.ProtocolVersions {
		if v != "v1" {
			add("protocol_versions[%d]: only v1 is supported", i)
		}
	}

	if m.Kinds.Input == nil && m.Kinds.Processor == nil && m.Kinds.Output == nil {
		add("kinds: must offer at least one of input, processor, output")
	}
	if in := m.Kinds.Input; in != nil {
		checkSchema(&problems, "kinds.input.connection_schema", in.ConnectionSchema)
		checkSchema(&problems, "kinds.input.address_schema", in.AddressSchema)
		checkEnum(&problems, "kinds.input.read_modes", in.ReadModes, allReadModes)
		checkEnum(&problems, "kinds.input.types", in.Types, allTypes)
	}
	if p := m.Kinds.Processor; p != nil {
		checkSchema(&problems, "kinds.processor.config_schema", p.ConfigSchema)
	}
	if o := m.Kinds.Output; o != nil {
		checkSchema(&problems, "kinds.output.config_schema", o.ConfigSchema)
	}
	return problems
}

func checkSchema(problems *[]string, path string, raw json.RawMessage) {
	if len(bytes.TrimSpace(raw)) == 0 {
		*problems = append(*problems, path+": is required")
		return
	}
	if _, err := compileSchema(raw); err != nil {
		*problems = append(*problems, path+": "+err.Error())
	}
}

func checkEnum(problems *[]string, path string, got, allowed []string) {
	if len(got) == 0 {
		*problems = append(*problems, path+": must list at least one value")
		return
	}
	for i, v := range got {
		if !slices.Contains(allowed, v) {
			*problems = append(*problems, fmt.Sprintf("%s[%d]: unknown value, allowed: %s", path, i, strings.Join(allowed, ", ")))
		}
	}
}

// ValidateInput checks an input section against the manifest's input kind: connection
// against connection_schema, each point's address against address_schema, its read mode
// against read_modes and its type against types. It does not re-validate the agent's own
// schema, so a point without a type or with an unreadable read is not reported here.
func (m Manifest) ValidateInput(section json.RawMessage) []Problem {
	in := m.Kinds.Input
	if in == nil {
		return []Problem{{Message: "the manifest does not offer the input kind"}}
	}
	doc, problems := decodeSection(section)
	if doc == nil {
		return problems
	}

	connection, ok := doc["connection"]
	if !ok {
		connection = map[string]any{}
	}
	problems = append(problems, validateAgainst("kinds.input.connection_schema", in.ConnectionSchema, "connection", connection)...)

	var addressSchema *jsonschema.Schema
	var addressSchemaProblem *Problem
	devices, _ := doc["devices"].([]any)
	for i, d := range devices {
		device, _ := d.(map[string]any)
		points, _ := device["points"].([]any)
		for j, p := range points {
			point, _ := p.(map[string]any)
			base := fmt.Sprintf("devices[%d].points[%d]", i, j)

			if address, present := point["address"]; present {
				if addressSchema == nil && addressSchemaProblem == nil {
					sch, err := compileSchema(in.AddressSchema)
					if err != nil {
						addressSchemaProblem = &Problem{Path: "kinds.input.address_schema", Message: "schema does not compile"}
					} else {
						addressSchema = sch
					}
				}
				if addressSchemaProblem != nil {
					problems = appendUnique(problems, *addressSchemaProblem)
				} else {
					problems = append(problems, schemaProblems(addressSchema, base+".address", address)...)
				}
			}
			if mode := readMode(point["read"]); mode != "" && !slices.Contains(in.ReadModes, mode) {
				problems = append(problems, Problem{Path: base + ".read", Message: "read mode is not accepted by this plugin"})
			}
			if typ, ok := point["type"].(string); ok && !slices.Contains(in.Types, typ) {
				problems = append(problems, Problem{Path: base + ".type", Message: "type is not accepted by this plugin"})
			}
		}
	}
	return problems
}

// ValidateProcessor checks a processor section's config against config_schema.
func (m Manifest) ValidateProcessor(section json.RawMessage) []Problem {
	if m.Kinds.Processor == nil {
		return []Problem{{Message: "the manifest does not offer the processor kind"}}
	}
	return validateConfigSection("kinds.processor.config_schema", m.Kinds.Processor.ConfigSchema, section)
}

// ValidateOutput checks an output section's config against config_schema.
func (m Manifest) ValidateOutput(section json.RawMessage) []Problem {
	if m.Kinds.Output == nil {
		return []Problem{{Message: "the manifest does not offer the output kind"}}
	}
	return validateConfigSection("kinds.output.config_schema", m.Kinds.Output.ConfigSchema, section)
}

func validateConfigSection(schemaPath string, schema, section json.RawMessage) []Problem {
	doc, problems := decodeSection(section)
	if doc == nil {
		return problems
	}
	config, ok := doc["config"]
	if !ok {
		config = map[string]any{}
	}
	return validateAgainst(schemaPath, schema, "config", config)
}

func decodeSection(section json.RawMessage) (map[string]any, []Problem) {
	value, err := jsonschema.UnmarshalJSON(bytes.NewReader(section))
	if err != nil {
		return nil, []Problem{{Message: "section is not valid JSON"}}
	}
	doc, ok := value.(map[string]any)
	if !ok {
		return nil, []Problem{{Message: "section is not a JSON object"}}
	}
	return doc, nil
}

// readMode names the agent read mode a point's read value stands for, "" when it is none.
func readMode(read any) string {
	switch r := read.(type) {
	case string:
		return r
	case map[string]any:
		if _, ok := r["subscribe"]; ok {
			return "subscribe"
		}
		if _, ok := r["poll"]; ok {
			return "poll"
		}
	}
	return ""
}

func appendUnique(problems []Problem, p Problem) []Problem {
	if slices.Contains(problems, p) {
		return problems
	}
	return append(problems, p)
}

func validateAgainst(schemaPath string, schemaRaw json.RawMessage, instancePath string, instance any) []Problem {
	schema, err := compileSchema(schemaRaw)
	if err != nil {
		return []Problem{{Path: schemaPath, Message: "schema does not compile"}}
	}
	return schemaProblems(schema, instancePath, instance)
}

// schemaProblems validates instance and turns each failing keyword into a problem whose
// message is fixed text derived from the keyword. The library's own messages can embed
// instance values, so they are never used.
func schemaProblems(schema *jsonschema.Schema, instancePath string, instance any) []Problem {
	err := schema.Validate(instance)
	if err == nil {
		return nil
	}
	var verr *jsonschema.ValidationError
	if !errors.As(err, &verr) {
		return []Problem{{Path: instancePath, Message: "value does not satisfy the schema"}}
	}
	var problems []Problem
	collectLeaves(verr, func(leaf *jsonschema.ValidationError) {
		path := locationPath(instancePath, instance, leaf.InstanceLocation)
		if req, ok := leaf.ErrorKind.(*kind.Required); ok && len(req.Missing) > 0 {
			for _, name := range req.Missing {
				problems = appendUnique(problems, Problem{Path: joinKey(path, name), Message: "is required"})
			}
			return
		}
		problems = appendUnique(problems, Problem{Path: path, Message: keywordMessage(leaf.ErrorKind)})
	})
	return problems
}

func collectLeaves(e *jsonschema.ValidationError, visit func(*jsonschema.ValidationError)) {
	if len(e.Causes) == 0 {
		visit(e)
		return
	}
	for _, c := range e.Causes {
		collectLeaves(c, visit)
	}
}

// locationPath renders an instance location below prefix, writing array positions as [n]
// and object keys as .key. It follows the instance to tell the two apart.
func locationPath(prefix string, instance any, location []string) string {
	path := prefix
	current := instance
	for _, segment := range location {
		switch node := current.(type) {
		case []any:
			path += "[" + segment + "]"
			if idx, err := strconv.Atoi(segment); err == nil && idx >= 0 && idx < len(node) {
				current = node[idx]
			} else {
				current = nil
			}
		case map[string]any:
			path = joinKey(path, segment)
			current = node[segment]
		default:
			path = joinKey(path, segment)
			current = nil
		}
	}
	return path
}

func joinKey(path, key string) string {
	return path + "." + key
}

func keywordMessage(k jsonschema.ErrorKind) string {
	path := k.KeywordPath()
	if len(path) == 0 {
		return "value does not satisfy the schema"
	}
	keyword := path[len(path)-1]
	switch keyword {
	case "type":
		return "has the wrong type"
	case "enum", "const":
		return "is not an accepted value"
	case "pattern", "format":
		return "does not match the expected format"
	case "minLength", "maxLength", "minItems", "maxItems", "minProperties", "maxProperties", "minContains", "maxContains":
		return "has an unacceptable length or size"
	case "minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum", "multipleOf":
		return "is outside the accepted range"
	case "additionalProperties", "unevaluatedProperties":
		return "has a property that is not allowed"
	case "uniqueItems":
		return "has duplicate items"
	case "required", "dependentRequired", "dependencies":
		return "is missing a required property"
	}
	return "does not satisfy schema keyword " + keyword
}

type denyLoader struct{}

func (denyLoader) Load(string) (any, error) {
	return nil, errors.New("external references are not allowed")
}

// compileSchema compiles raw as a draft 2020-12 schema. It never loads anything from
// outside the manifest: a $ref to a file or a URL fails to compile. The returned error is
// fixed text.
func compileSchema(raw json.RawMessage) (*jsonschema.Schema, error) {
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
	if err != nil {
		return nil, errors.New("is not valid JSON")
	}
	switch s := doc.(type) {
	case bool:
	case map[string]any:
		if declared, ok := s["$schema"]; ok {
			if str, isStr := declared.(string); !isStr || strings.TrimSuffix(str, "#") != draft2020URL {
				return nil, errors.New("must be a JSON Schema draft 2020-12 document")
			}
		}
	default:
		return nil, errors.New("must be a JSON object or a boolean")
	}

	const location = "mem:///plugin-schema.json"
	compiler := jsonschema.NewCompiler()
	compiler.DefaultDraft(jsonschema.Draft2020)
	compiler.UseLoader(denyLoader{})
	if err := compiler.AddResource(location, doc); err != nil {
		return nil, errors.New("is not a valid JSON Schema (draft 2020-12)")
	}
	schema, err := compiler.Compile(location)
	if err != nil {
		return nil, errors.New("is not a valid JSON Schema (draft 2020-12)")
	}
	return schema, nil
}
