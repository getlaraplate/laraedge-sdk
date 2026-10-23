//go:build unix

package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"path/filepath"

	"github.com/getlaraplate/laraedge-sdk/handshake"
)

// listen opens the plugin's socket in the socket directory, private unless BROKEN_PERMISSIONS
// is set, with a random name unless BROKEN_FIXED_ADDRESS is set.
func listen() (net.Listener, string, string, error) {
	dir := os.Getenv(handshake.SocketDirKey)
	if dir == "" {
		return nil, "", "", fmt.Errorf("%s is not set", handshake.SocketDirKey)
	}
	var random [4]byte
	_, _ = rand.Read(random[:])
	name := "p" + hex.EncodeToString(random[:]) + ".sock"
	if defect("BROKEN_FIXED_ADDRESS") {
		name = "fixed.sock"
	}
	path := filepath.Join(dir, name)
	lis, err := net.Listen("unix", path)
	if err != nil {
		return nil, "", "", err
	}
	socketMode, dirMode := os.FileMode(0o600), os.FileMode(0o700)
	if defect("BROKEN_PERMISSIONS") {
		socketMode, dirMode = 0o666, 0o755
	}
	if err := os.Chmod(path, socketMode); err != nil {
		_ = lis.Close()
		return nil, "", "", err
	}
	if err := os.Chmod(dir, dirMode); err != nil {
		_ = lis.Close()
		return nil, "", "", err
	}
	return lis, "unix", path, nil
}
