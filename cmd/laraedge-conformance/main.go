// Command laraedge-conformance runs the conformance suite of package conformance on a plugin
// directory: plugin.json, conformance.json and the binary, which must be named after the
// manifest: <dir>/<name of plugin.json>, plus .exe on Windows.
//
//	laraedge-conformance [-timeout 10s] [-v] <plugin dir>
//
// It prints one line per check, PASS or FAIL with the check's name and detail, then a summary,
// and exits 0 when every check passed, 1 when one failed, 2 on a usage error. With -v the
// plugin's own output goes to stderr.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"

	"github.com/getlaraplate/laraedge-sdk/conformance"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	code := run(ctx, os.Args[1:], os.Stdout, os.Stderr)
	stop()
	os.Exit(code)
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("laraedge-conformance", flag.ContinueOnError)
	flags.SetOutput(stderr)
	timeout := flags.Duration("timeout", conformance.DefaultTimeout,
		"bound of every wait the protocol does not fix (first event, end of a stream, exits)")
	verbose := flags.Bool("v", false, "copy the plugin's output to stderr")
	flags.Usage = func() {
		_, _ = fmt.Fprintln(stderr, "usage: laraedge-conformance [-timeout 10s] [-v] <plugin dir>")
		_, _ = fmt.Fprintln(stderr, "\nThe plugin dir holds plugin.json, conformance.json and the binary, named after the")
		_, _ = fmt.Fprintln(stderr, "manifest: <plugin dir>/<name of plugin.json>, plus .exe on Windows.")
		_, _ = fmt.Fprintln(stderr)
		flags.PrintDefaults()
	}
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if flags.NArg() != 1 {
		flags.Usage()
		return 2
	}
	opts := conformance.Options{Timeout: *timeout}
	if *verbose {
		opts.Stderr = stderr
	}
	report := conformance.Run(ctx, flags.Arg(0), opts)
	failed := 0
	for _, check := range report.Checks {
		result := "PASS"
		if !check.Passed {
			result = "FAIL"
			failed++
		}
		_, _ = fmt.Fprintf(stdout, "%s %s: %s\n", result, check.Name, check.Detail)
	}
	if !report.Passed {
		_, _ = fmt.Fprintf(stdout, "conformance: FAIL (%d of %d checks failed)\n", failed, len(report.Checks))
		return 1
	}
	_, _ = fmt.Fprintf(stdout, "conformance: PASS (%d checks)\n", len(report.Checks))
	return 0
}
