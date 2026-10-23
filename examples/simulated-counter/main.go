// Command simulated-counter is an example Input plugin: a simulated machine whose points are
// counters. It reads nothing from the outside, so it runs anywhere, and it shows the whole life of
// an Input: Configure, Probe, Browse and a Run that polls on a schedule.
//
// The address of a point is counter:<n>, with n the increment of the counter per poll. Every
// poll of a point reports its counter and then says that its device answered.
//
// Build it with `go build`, print its manifest with `--manifest`. It is an example, not a
// production plugin.
package main

import (
	"github.com/getlaraplate/laraedge-sdk/sdk"
)

func main() {
	sdk.ServeInput(pluginName, pluginVersion, newCounter(), inputKind())
}
