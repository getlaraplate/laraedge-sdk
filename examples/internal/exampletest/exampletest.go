// Package exampletest holds what the tests of the example plugins share: the check that the
// committed plugin.json is what the plugin's binary prints with --manifest, and the loading of
// plugin.json and conformance.json. It is for the tests of the examples only.
package exampletest

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/getlaraplate/laraedge-sdk/manifest"
)

// Fixtures is conformance.json: per kind the plugin offers, sections its Configure must accept
// and sections it must refuse.
type Fixtures struct {
	Input     *Section `json:"input"`
	Processor *Section `json:"processor"`
	Output    *Section `json:"output"`
}

// Section lists the valid and the invalid sections of one kind, as the agent sends them.
type Section struct {
	Valid   []json.RawMessage `json:"valid"`
	Invalid []json.RawMessage `json:"invalid"`
}

// LoadManifest loads and checks plugin.json of the package under test.
func LoadManifest(t *testing.T) manifest.Manifest {
	t.Helper()
	m, _, err := manifest.Load("plugin.json")
	if err != nil {
		t.Fatalf("load plugin.json: %v", err)
	}
	if problems := m.Check(); len(problems) > 0 {
		t.Fatalf("plugin.json is not acceptable: %v", problems)
	}
	return m
}

// LoadFixtures loads conformance.json of the package under test.
func LoadFixtures(t *testing.T) Fixtures {
	t.Helper()
	raw, err := os.ReadFile("conformance.json")
	if err != nil {
		t.Fatalf("read conformance.json: %v", err)
	}
	var f Fixtures
	if err := json.Unmarshal(raw, &f); err != nil {
		t.Fatalf("decode conformance.json: %v", err)
	}
	return f
}

// CheckCommittedManifest builds the package under test once, runs the binary with --manifest
// and fails unless the output ends with a newline and equals the committed plugin.json in
// canonical form.
func CheckCommittedManifest(t *testing.T) {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "plugin")
	if runtime.GOOS == "windows" {
		bin += ".exe"
	}
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Fatalf("go build: %v\n%s", err, out)
	}
	var stdout, stderr bytes.Buffer
	cmd := exec.Command(bin, "--manifest")
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("--manifest: %v\n%s", err, stderr.String())
	}
	if !bytes.HasSuffix(stdout.Bytes(), []byte("\n")) {
		t.Errorf("--manifest output has no trailing newline")
	}
	committed, err := os.ReadFile("plugin.json")
	if err != nil {
		t.Fatalf("read plugin.json: %v", err)
	}
	want, err := manifest.Canonical(committed)
	if err != nil {
		t.Fatalf("canonical plugin.json: %v", err)
	}
	got, err := manifest.Canonical(stdout.Bytes())
	if err != nil {
		t.Fatalf("canonical --manifest output: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("plugin.json is stale: regenerate it with `go run . --manifest > plugin.json`\ncommitted: %s\nbinary:    %s", want, got)
	}
}
