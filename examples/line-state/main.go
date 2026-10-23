// Command line-state is an example Processor plugin: it derives the state of a production line
// from the state of its machines. The line is as bad off as its worst machine: the derived
// state is the maximum of the integer state signals of the observed devices.
//
// It shows what a Processor does: it receives the records of the devices it observes and emits
// samples of the derived device its configuration declares. It never emits births or deaths.
//
// Build it with `go build`, print its manifest with `--manifest`. It is an example, not a
// production plugin.
package main

import (
	"github.com/getlaraplate/laraedge-sdk/sdk"
)

func main() {
	sdk.ServeProcessor(pluginName, pluginVersion, newProcessor(), processorKind())
}
