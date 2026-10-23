// Command jsonl is an example Output plugin: it appends every batch it is given to a file, one
// JSON object per line. It shows what an Output must get right: idempotency (the same
// message_id twice is DUPLICATE, also across restarts), durability (each line is fsynced before
// ACCEPTED), and the other outcomes the agent understands.
//
// The configuration is {"path": "<file>"}. Delivery is suspended (SUSPEND_AUTH, as if the
// credentials were refused) while a file named <path>.suspend exists: a switch for trying the
// behaviour of the agent without a real destination.
//
// Build it with `go build`, print its manifest with `--manifest`. It is an example, not a
// production plugin.
package main

import (
	"github.com/getlaraplate/laraedge-sdk/sdk"
)

func main() {
	sdk.ServeOutput(pluginName, pluginVersion, newSink(), outputKind())
}
