//go:build unix

package handshake

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"syscall"
)

// transportNetwork is the network of the handshake line on this platform.
const transportNetwork = "unix"

// maxSocketPath is the longest Unix socket path: sun_path (108 bytes on Linux, 104 on darwin
// and the BSDs) less its terminating NUL.
const maxSocketPath = len(syscall.RawSockaddrUnix{}.Path) - 1

// socketPath joins dir and name and refuses a path over the platform limit.
func socketPath(dir, name string) (string, error) {
	path := filepath.Join(dir, name)
	if len(path) > maxSocketPath {
		return "", fmt.Errorf("handshake: %w: %d bytes, the platform allows at most %d (%s)",
			ErrSocketPathTooLong, len(path), maxSocketPath, path)
	}
	return path, nil
}

// listenTransport opens <socket dir>/p<8 random hex>.sock with mode 0600. The mode is set
// after the bind, so it does not depend on the umask; until then the 0700 directory keeps
// other users out.
func listenTransport(getenv func(string) string) (net.Listener, string, string, error) {
	dir := getenv(SocketDirKey)
	if dir == "" {
		return nil, "", "", fmt.Errorf("handshake: %s is not set", SocketDirKey)
	}
	path, err := socketPath(dir, "p"+randomHex()+".sock")
	if err != nil {
		return nil, "", "", err
	}
	listener, err := net.Listen("unix", path)
	if err != nil {
		return nil, "", "", fmt.Errorf("handshake: listening on the socket: %w", err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		_ = listener.Close()
		return nil, "", "", fmt.Errorf("handshake: socket permissions: %w", err)
	}
	return listener, transportNetwork, path, nil
}

// prepareSocketDir returns the absolute socket directory, private to the current user and
// without leftover sockets.
func prepareSocketDir(dir string) (string, error) {
	if dir == "" {
		return "", errors.New("handshake: LaunchConfig.SocketDir is empty")
	}
	abs, err := filepath.Abs(dir)
	if err != nil {
		return "", fmt.Errorf("handshake: socket directory: %w", err)
	}
	// Checked before anything is created: the plugin could not bind such a path.
	if _, err := socketPath(abs, "p00000000.sock"); err != nil {
		return "", err
	}
	if err := os.MkdirAll(abs, 0o700); err != nil {
		return "", fmt.Errorf("handshake: socket directory: %w", err)
	}
	info, err := os.Lstat(abs)
	if err != nil {
		return "", fmt.Errorf("handshake: socket directory: %w", err)
	}
	if info.Mode()&fs.ModeSymlink != 0 {
		return "", fmt.Errorf("handshake: socket directory %s is a symbolic link", abs)
	}
	if !info.IsDir() {
		return "", fmt.Errorf("handshake: socket directory %s is not a directory", abs)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || int(stat.Uid) != os.Getuid() {
		return "", fmt.Errorf("handshake: socket directory %s does not belong to the current user", abs)
	}
	if info.Mode().Perm() != 0o700 {
		if err := os.Chmod(abs, 0o700); err != nil {
			return "", fmt.Errorf("handshake: socket directory: %w", err)
		}
	}
	entries, err := os.ReadDir(abs)
	if err != nil {
		return "", fmt.Errorf("handshake: socket directory: %w", err)
	}
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".sock" {
			continue
		}
		if err := os.Remove(filepath.Join(abs, entry.Name())); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return "", fmt.Errorf("handshake: removing a leftover socket: %w", err)
		}
	}
	return abs, nil
}

// verifyAddress accepts a socket directly inside socketDir and returns its clean path.
func verifyAddress(socketDir, address string) (string, error) {
	if !filepath.IsAbs(address) {
		return "", fmt.Errorf("socket path %s is not absolute", quoteStart(address))
	}
	clean := filepath.Clean(address)
	if filepath.Dir(clean) != socketDir {
		return "", fmt.Errorf("socket path %s is not in the socket directory %s", quoteStart(address), socketDir)
	}
	info, err := os.Lstat(clean)
	if err != nil {
		return "", fmt.Errorf("socket path %s: %w", quoteStart(address), err)
	}
	if info.Mode().Type() != fs.ModeSocket {
		return "", fmt.Errorf("socket path %s is not a socket", quoteStart(address))
	}
	return clean, nil
}

// dialTransport connects to the plugin's socket.
func dialTransport(ctx context.Context, address string) (net.Conn, error) {
	var dialer net.Dialer
	return dialer.DialContext(ctx, "unix", address)
}
