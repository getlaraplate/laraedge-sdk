package handshake

import (
	"context"
	"fmt"
	"net"
	"os"
	"regexp"

	"github.com/Microsoft/go-winio"
	"golang.org/x/sys/windows"
)

// transportNetwork is the network of the handshake line on this platform.
const transportNetwork = "npipe"

// localPipe matches a pipe of the local machine whose name has no path separator, so a
// handshake line cannot point the host at a remote pipe or, through "..", at a file.
var localPipe = regexp.MustCompile(`^\\\\\.\\pipe\\[A-Za-z0-9_-][A-Za-z0-9._-]*$`)

// listenTransport opens \\.\pipe\laraedge-<pid>-<8 random hex> with a protected DACL that
// grants access to the current user only. go-winio also rejects remote clients.
func listenTransport(func(string) string) (net.Listener, string, string, error) {
	user, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil {
		return nil, "", "", fmt.Errorf("handshake: current user: %w", err)
	}
	name := fmt.Sprintf(`\\.\pipe\laraedge-%d-%s`, os.Getpid(), randomHex())
	listener, err := winio.ListenPipe(name, &winio.PipeConfig{
		SecurityDescriptor: "D:P(A;;GA;;;" + user.User.Sid.String() + ")",
	})
	if err != nil {
		return nil, "", "", fmt.Errorf("handshake: listening on the pipe: %w", err)
	}
	return listener, transportNetwork, name, nil
}

// prepareSocketDir has nothing to prepare: named pipes need no directory.
func prepareSocketDir(string) (string, error) {
	return "", nil
}

// verifyAddress accepts a local named pipe.
func verifyAddress(_, address string) (string, error) {
	if !localPipe.MatchString(address) {
		return "", fmt.Errorf("pipe name %s is not a local named pipe", quoteStart(address))
	}
	return address, nil
}

// dialTransport connects to the plugin's pipe.
func dialTransport(ctx context.Context, address string) (net.Conn, error) {
	return winio.DialPipeContext(ctx, address)
}
