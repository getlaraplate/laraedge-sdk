// Package handshake is the process boundary between the Laraplate edge agent and a plugin:
// how the agent starts a plugin and how the two agree on a protocol and a local transport.
//
// The plugin side is [Listen]: it checks the cookie, negotiates the protocol, opens a Unix
// socket or a Windows named pipe that only the current user can reach, and prints the
// handshake line on stdout:
//
//	1|<protocol>|unix|<socket path>
//	1|<protocol>|npipe|<pipe name>
//
// The host side is [Launch]: it starts the plugin without a shell and with a minimal
// environment, reads the handshake line, and returns a [Process] whose [Process.Dial] opens
// the gRPC connection. The plugin is tied to the host: on Linux it leads its own process group
// and gets a parent-death signal; on Windows it runs in a job object that kills on close.
// [Process.Kill] and the plugin's own exit end its descendants as well. When the host dies
// suddenly, Windows kills the whole job, while the Linux parent-death signal reaches the plugin
// only: the children it started survive, and the agent's systemd unit (whose cgroup is killed
// as a whole, KillMode=control-group) is the backstop. Other Unix systems give no parent-death
// guarantee and serve for development only.
package handshake

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"slices"
	"strings"
)

// Environment variables the host sets for the plugin. Secrets never travel in the
// environment.
const (
	// CookieKey carries [CookieValue]. It stops a plugin started by hand; it is not a security
	// control.
	CookieKey = "LARAPLATE_EDGE_PLUGIN_COOKIE"
	// CookieValue is the value of [CookieKey].
	CookieValue = "ea6f1c2b-laraplate-edge-plugin-v1"
	// ProtocolsKey carries the protocols the host offers, comma-separated, preferred first.
	ProtocolsKey = "LARAPLATE_EDGE_PLUGIN_PROTOCOLS"
	// SocketDirKey carries the directory of the plugin's Unix socket (Unix only).
	SocketDirKey = "LARAPLATE_EDGE_PLUGIN_SOCKET_DIR"
)

// ExitCodeNoProtocol is the exit code of a plugin that shares no protocol with its host.
const ExitCodeNoProtocol = 3

// MaxMessageSize is the largest gRPC message either side sends or receives (16 MB).
const MaxMessageSize = 16 << 20

// lineVersion is the first field of the handshake line: the version of the line's format.
const lineVersion = "1"

var (
	// ErrNotLaunched is returned by [Listen] when the plugin was not started by the agent.
	ErrNotLaunched = errors.New("this program is a Laraplate edge plugin: the Laraplate edge agent starts it, " +
		"it is not meant to be run by hand")
	// ErrNoProtocol reports that host and plugin share no protocol version. A plugin exits
	// with [ExitCodeNoProtocol]; [Launch] then returns an [ExitError] that matches it.
	ErrNoProtocol = errors.New("no protocol version in common with the host")
	// ErrSocketPathTooLong reports a Unix socket path over the platform limit.
	ErrSocketPathTooLong = errors.New("socket path too long")
)

// Listen is the plugin side of the handshake. It checks the cookie (else [ErrNotLaunched]),
// picks the first protocol offered by the host that is in supported (else [ErrNoProtocol]),
// opens the local transport and prints the handshake line on stdout. It returns the negotiated
// protocol version, such as "v1". Closing the listener removes the Unix socket.
func Listen(supported []string) (net.Listener, string, error) {
	return listen(os.Getenv, os.Stdout, supported)
}

func listen(getenv func(string) string, stdout io.Writer, supported []string) (net.Listener, string, error) {
	if getenv(CookieKey) != CookieValue {
		return nil, "", ErrNotLaunched
	}
	protocol, err := negotiate(getenv(ProtocolsKey), supported)
	if err != nil {
		return nil, "", err
	}
	listener, network, address, err := listenTransport(getenv)
	if err != nil {
		return nil, "", err
	}
	if _, err := fmt.Fprintf(stdout, "%s|%s|%s|%s\n", lineVersion, protocol, network, address); err != nil {
		_ = listener.Close()
		return nil, "", fmt.Errorf("handshake: writing the handshake line: %w", err)
	}
	return listener, protocol, nil
}

// negotiate picks the first protocol of offered (comma-separated, host preference first)
// that is in supported.
func negotiate(offered string, supported []string) (string, error) {
	var hostProtocols []string
	for entry := range strings.SplitSeq(offered, ",") {
		if entry = strings.TrimSpace(entry); entry != "" {
			hostProtocols = append(hostProtocols, entry)
		}
	}
	for _, protocol := range hostProtocols {
		if slices.Contains(supported, protocol) {
			return protocol, nil
		}
	}
	return "", fmt.Errorf("%w: the host offers [%s], the plugin supports [%s]",
		ErrNoProtocol, strings.Join(hostProtocols, ","), strings.Join(supported, ","))
}

// randomHex returns 8 random hexadecimal digits for socket and pipe names.
func randomHex() string {
	var buf [4]byte
	_, _ = rand.Read(buf[:]) // crypto/rand.Read never returns an error
	return hex.EncodeToString(buf[:])
}
