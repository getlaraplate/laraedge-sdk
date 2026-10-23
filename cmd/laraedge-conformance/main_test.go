package main

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestUsage(t *testing.T) {
	for _, args := range [][]string{nil, {"a", "b"}, {"-unknown", "dir"}} {
		var stdout, stderr bytes.Buffer
		if code := run(context.Background(), args, &stdout, &stderr); code != 2 {
			t.Errorf("args %q: exit code %d, want 2", args, code)
		}
		if !strings.Contains(stderr.String(), "usage: laraedge-conformance") ||
			!strings.Contains(stderr.String(), "<plugin dir>/<name of plugin.json>, plus .exe on Windows") {
			t.Errorf("args %q: stderr %q lacks the usage", args, stderr.String())
		}
	}
}

func TestFailingDirectoryExitsOne(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run(context.Background(), []string{t.TempDir()}, &stdout, &stderr); code != 1 {
		t.Errorf("exit code %d, want 1", code)
	}
	out := stdout.String()
	for _, want := range []string{"FAIL manifest: ", "FAIL handshake: not run: ", "conformance: FAIL (9 of 9 checks failed)\n"} {
		if !strings.Contains(out, want) {
			t.Errorf("output lacks %q:\n%s", want, out)
		}
	}
}

// TestConformantPluginExitsZero runs the command on the jsonl example: one line per check.
func TestConformantPluginExitsZero(t *testing.T) {
	dir := t.TempDir()
	binary := filepath.Join(dir, "jsonl")
	if runtime.GOOS == "windows" {
		binary += ".exe"
	}
	if out, err := exec.Command("go", "build", "-o", binary, "../../examples/jsonl").CombinedOutput(); err != nil {
		t.Fatalf("go build: %v\n%s", err, out)
	}
	for _, file := range []string{"plugin.json", "conformance.json"} {
		raw, err := os.ReadFile(filepath.Join("../../examples/jsonl", file))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, file), raw, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	var stdout, stderr bytes.Buffer
	if code := run(context.Background(), []string{dir}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit code %d, want 0:\n%s", code, stdout.String())
	}
	lines := strings.Split(strings.TrimSuffix(stdout.String(), "\n"), "\n")
	if len(lines) != 13 || lines[12] != "conformance: PASS (12 checks)" {
		t.Fatalf("output:\n%s\nwant 12 PASS lines and the summary", stdout.String())
	}
	for _, line := range lines[:12] {
		if !strings.HasPrefix(line, "PASS ") {
			t.Errorf("line %q is not a PASS line", line)
		}
	}
	if stderr.Len() != 0 {
		t.Errorf("without -v stderr holds %q", stderr.String())
	}
}
