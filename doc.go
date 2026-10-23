// Package laraedgesdk is the root of the laraedge plugin SDK, the Apache-2.0
// toolkit for writing plugins of the Laraplate edge agent.
//
// # Licence boundary
//
// This module is licensed under the Apache License 2.0. A plugin imports only
// this module, never code of the agent. The agent (github.com/getlaraplate/laraedge,
// AGPL-3.0) imports this module for the host side of the plugin protocol; this
// module never imports the agent. Plugins that depend only on this module may
// therefore be proprietary.
//
// The protocol is laraplate.edge.plugin.v1. The packages that carry it are
// added beneath this root: handshake, manifest, sdk and conformance.
package laraedgesdk
