package pluginv1_test

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	_ "github.com/getlaraplate/laraedge-sdk/pluginv1" // registers the files of the protocol
)

// updateDescriptors rewrites the golden dump instead of comparing with it:
//
//	go test ./pluginv1 -run TestDescriptorsMatchTheGolden -update-descriptors
//
// Run it only after a deliberate, compatible change to proto/ (a new field, message, value or
// method) and commit the dump together with the regenerated code.
var updateDescriptors = flag.Bool("update-descriptors", false, "rewrite testdata/descriptors.golden")

const descriptorsGolden = "testdata/descriptors.golden"

// TestDescriptorsMatchTheGolden pins the wire contract of laraplate.edge.plugin.v1: the number,
// name, cardinality, type and oneof of every field, the number of every enum value and the shape
// of every method. A renumbering regenerated into both proto/ and pluginv1/ passes the generated
// code check but fails here, because the committed dump still holds the old numbers.
func TestDescriptorsMatchTheGolden(t *testing.T) {
	got := dumpPackage(t, "laraplate.edge.plugin.v1")
	if *updateDescriptors {
		if err := os.MkdirAll(filepath.Dir(descriptorsGolden), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(descriptorsGolden, []byte(got), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(descriptorsGolden)
	if err != nil {
		t.Fatalf("%v (create it with -update-descriptors)", err)
	}
	if got == strings.ReplaceAll(string(want), "\r\n", "\n") {
		return
	}
	gotLines, wantLines := strings.Split(got, "\n"), strings.Split(string(want), "\n")
	for i := range max(len(gotLines), len(wantLines)) {
		var g, w string
		if i < len(gotLines) {
			g = gotLines[i]
		}
		if i < len(wantLines) {
			w = wantLines[i]
		}
		if g != w {
			t.Fatalf("the descriptors differ from %s at line %d:\n got: %q\nwant: %q\n"+
				"Field numbers and names are stable; regenerate the dump with -update-descriptors only for a compatible addition.",
				descriptorsGolden, i+1, g, w)
		}
	}
}

// dumpPackage renders every file of the package registered in the global registry, sorted by
// path, as stable text.
func dumpPackage(t *testing.T, pkg protoreflect.FullName) string {
	t.Helper()
	var files []protoreflect.FileDescriptor
	protoregistry.GlobalFiles.RangeFilesByPackage(pkg, func(fd protoreflect.FileDescriptor) bool {
		files = append(files, fd)
		return true
	})
	if len(files) == 0 {
		t.Fatalf("no file of package %s is registered", pkg)
	}
	slices.SortFunc(files, func(a, b protoreflect.FileDescriptor) int { return strings.Compare(a.Path(), b.Path()) })
	var b strings.Builder
	for _, fd := range files {
		fmt.Fprintf(&b, "file %s\n", fd.Path())
		dumpEnums(&b, fd.Enums())
		dumpMessages(&b, fd.Messages())
		services := fd.Services()
		for i := range services.Len() {
			sd := services.Get(i)
			fmt.Fprintf(&b, "service %s\n", sd.FullName())
			methods := sd.Methods()
			for j := range methods.Len() {
				md := methods.Get(j)
				fmt.Fprintf(&b, "  rpc %s(%s%s) returns (%s%s)\n", md.Name(),
					streamWord(md.IsStreamingClient()), md.Input().FullName(),
					streamWord(md.IsStreamingServer()), md.Output().FullName())
			}
		}
	}
	return b.String()
}

func dumpEnums(b *strings.Builder, enums protoreflect.EnumDescriptors) {
	for i := range enums.Len() {
		ed := enums.Get(i)
		fmt.Fprintf(b, "enum %s\n", ed.FullName())
		values := ed.Values()
		for j := range values.Len() {
			vd := values.Get(j)
			fmt.Fprintf(b, "  %d %s\n", vd.Number(), vd.Name())
		}
	}
}

func dumpMessages(b *strings.Builder, messages protoreflect.MessageDescriptors) {
	for i := range messages.Len() {
		md := messages.Get(i)
		fmt.Fprintf(b, "message %s\n", md.FullName())
		fields := md.Fields()
		for j := range fields.Len() {
			fd := fields.Get(j)
			line := fmt.Sprintf("  %d %s %s %s", fd.Number(), fd.Name(), fd.Cardinality(), fd.Kind())
			switch {
			case fd.Message() != nil:
				line += " " + string(fd.Message().FullName())
			case fd.Enum() != nil:
				line += " " + string(fd.Enum().FullName())
			}
			if od := fd.ContainingOneof(); od != nil && !od.IsSynthetic() {
				line += " oneof " + string(od.Name())
			}
			fmt.Fprintln(b, line)
		}
		dumpEnums(b, md.Enums())
		dumpMessages(b, md.Messages())
	}
}

func streamWord(streaming bool) string {
	if streaming {
		return "stream "
	}
	return ""
}
