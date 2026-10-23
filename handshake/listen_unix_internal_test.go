//go:build unix

package handshake

import (
	"bytes"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
)

func TestListenSocketIsPrivateWhateverTheUmask(t *testing.T) {
	env := fakeEnv{CookieKey: CookieValue, ProtocolsKey: "v1", SocketDirKey: shortDir(t)}
	previous := syscall.Umask(0)
	defer syscall.Umask(previous)

	var stdout bytes.Buffer
	listener, _, err := listen(env.get, &stdout, []string{"v1"})
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	path := listener.Addr().String()
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Errorf("socket mode %o with umask 0, want 600", perm)
	}
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(path); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("Close left the socket file: %v", err)
	}
}

func TestListenRefusesASocketPathTooLong(t *testing.T) {
	limit := len(syscall.RawSockaddrUnix{}.Path) - 1
	dir := filepath.Join(shortDir(t), strings.Repeat("d", limit))
	length := len(filepath.Join(dir, "p00000000.sock"))
	env := fakeEnv{CookieKey: CookieValue, ProtocolsKey: "v1", SocketDirKey: dir}

	var stdout bytes.Buffer
	listener, _, err := listen(env.get, &stdout, []string{"v1"})
	if err == nil {
		_ = listener.Close()
		t.Fatal("listen accepted a socket path over the limit")
	}
	if !errors.Is(err, ErrSocketPathTooLong) {
		t.Fatalf("err %v, want ErrSocketPathTooLong", err)
	}
	for _, number := range []int{length, limit} {
		if !strings.Contains(err.Error(), strconv.Itoa(number)) {
			t.Errorf("message %q does not name %d", err, number)
		}
	}
	if stdout.Len() != 0 {
		t.Errorf("printed %q on failure", stdout.String())
	}
}

func TestListenRequiresTheSocketDirectory(t *testing.T) {
	env := fakeEnv{CookieKey: CookieValue, ProtocolsKey: "v1"}
	var stdout bytes.Buffer
	listener, _, err := listen(env.get, &stdout, []string{"v1"})
	if err == nil {
		_ = listener.Close()
		t.Fatal("listen without a socket directory succeeded")
	}
	if !strings.Contains(err.Error(), SocketDirKey) {
		t.Errorf("message %q does not name %s", err, SocketDirKey)
	}
}
