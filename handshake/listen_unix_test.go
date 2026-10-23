//go:build unix

package handshake_test

import (
	"context"
	"errors"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"testing"

	"github.com/getlaraplate/laraedge-sdk/handshake"
)

var socketName = regexp.MustCompile(`^p[0-9a-f]{8}\.sock$`)

func TestLaunchKeepsTheSocketPrivate(t *testing.T) {
	cases := []struct {
		name    string
		prepare func(t *testing.T, base string) string
	}{
		{
			name: "missing directory is created",
			prepare: func(_ *testing.T, base string) string {
				return filepath.Join(base, "run", "p")
			},
		},
		{
			name: "existing open directory is narrowed",
			prepare: func(t *testing.T, base string) string {
				dir := filepath.Join(base, "open")
				if err := os.Mkdir(dir, 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.Chmod(dir, 0o755); err != nil {
					t.Fatal(err)
				}
				return dir
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := config(t)
			cfg.SocketDir = tc.prepare(t, cfg.SocketDir)
			process := launch(t, cfg)

			dirInfo, err := os.Lstat(cfg.SocketDir)
			if err != nil {
				t.Fatal(err)
			}
			if perm := dirInfo.Mode().Perm(); perm != 0o700 {
				t.Errorf("socket directory mode %o, want 700", perm)
			}
			if filepath.Dir(process.Address) != cfg.SocketDir {
				t.Errorf("socket %q is not in %q", process.Address, cfg.SocketDir)
			}
			if !socketName.MatchString(filepath.Base(process.Address)) {
				t.Errorf("socket name %q, want p<8 hex>.sock", filepath.Base(process.Address))
			}
			info, err := os.Lstat(process.Address)
			if err != nil {
				t.Fatal(err)
			}
			if info.Mode().Type() != fs.ModeSocket {
				t.Errorf("%q is %v, want a socket", process.Address, info.Mode().Type())
			}
			if perm := info.Mode().Perm(); perm != 0o600 {
				t.Errorf("socket mode %o, want 600", perm)
			}
			checkHealth(t, process)
		})
	}
}

func TestLaunchRefusesAnUnsafeSocketDirectory(t *testing.T) {
	cases := []struct {
		name    string
		prepare func(t *testing.T, base string) string
		want    string
	}{
		{
			name: "symlink",
			want: "symbolic link",
			prepare: func(t *testing.T, base string) string {
				target := filepath.Join(base, "target")
				if err := os.Mkdir(target, 0o700); err != nil {
					t.Fatal(err)
				}
				link := filepath.Join(base, "link")
				if err := os.Symlink(target, link); err != nil {
					t.Fatal(err)
				}
				return link
			},
		},
		{
			name: "regular file",
			want: "not a directory",
			prepare: func(t *testing.T, base string) string {
				file := filepath.Join(base, "file")
				if err := os.WriteFile(file, nil, 0o600); err != nil {
					t.Fatal(err)
				}
				return file
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := config(t)
			cfg.SocketDir = tc.prepare(t, cfg.SocketDir)
			process, err := handshake.Launch(context.Background(), cfg)
			if err == nil {
				_ = process.Kill()
				_ = process.Wait()
				t.Fatal("Launch accepted the socket directory")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("message %q does not say %q", err, tc.want)
			}
			assertNoChildren(t)
		})
	}
}

func TestLaunchRemovesStaleSockets(t *testing.T) {
	cfg := config(t)
	stale := filepath.Join(cfg.SocketDir, "pdeadbeef.sock")
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: stale, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	listener.SetUnlinkOnClose(false)
	_ = listener.Close()
	keep := filepath.Join(cfg.SocketDir, "keep.txt")
	if err := os.WriteFile(keep, nil, 0o600); err != nil {
		t.Fatal(err)
	}

	process := launch(t, cfg)
	if _, err := os.Lstat(stale); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("stale socket still present: %v", err)
	}
	if _, err := os.Lstat(keep); err != nil {
		t.Errorf("a file that is not a socket was removed: %v", err)
	}
	checkHealth(t, process)
}

func TestLaunchReportsASocketPathTooLong(t *testing.T) {
	cfg := config(t)
	limit := len(syscall.RawSockaddrUnix{}.Path) - 1
	cfg.SocketDir = filepath.Join(cfg.SocketDir, strings.Repeat("d", limit))
	length := len(filepath.Join(cfg.SocketDir, "p00000000.sock"))

	process, err := handshake.Launch(context.Background(), cfg)
	if err == nil {
		_ = process.Kill()
		_ = process.Wait()
		t.Fatal("Launch accepted a socket path over the limit")
	}
	if !errors.Is(err, handshake.ErrSocketPathTooLong) {
		t.Fatalf("err %v, want ErrSocketPathTooLong", err)
	}
	for _, number := range []int{length, limit} {
		if !strings.Contains(err.Error(), strconv.Itoa(number)) {
			t.Errorf("message %q does not name %d", err, number)
		}
	}
	if _, err := os.Lstat(cfg.SocketDir); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("the directory was created before the length check: %v", err)
	}
}

func TestLaunchRefusesAnAddressOutsideTheSocketDirectory(t *testing.T) {
	cases := []struct {
		name    string
		address func(t *testing.T, dir string) string
	}{
		{"other directory", func(_ *testing.T, _ string) string { return "/tmp/p00000000.sock" }},
		{"relative path", func(_ *testing.T, _ string) string { return "p00000000.sock" }},
		{"escapes the directory", func(_ *testing.T, dir string) string {
			return dir + "/../" + filepath.Base(dir) + "x/p00000000.sock"
		}},
		{"missing socket", func(_ *testing.T, dir string) string { return filepath.Join(dir, "p00000000.sock") }},
		{"not a socket", func(t *testing.T, dir string) string {
			file := filepath.Join(dir, "regular")
			if err := os.WriteFile(file, nil, 0o600); err != nil {
				t.Fatal(err)
			}
			return file
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := config(t)
			cfg.Env = helperEnv("HELPER_LINE=1|v1|unix|" + tc.address(t, cfg.SocketDir))
			process, err := handshake.Launch(context.Background(), cfg)
			if err == nil {
				_ = process.Kill()
				_ = process.Wait()
				t.Fatal("Launch accepted the address")
			}
			if !errors.Is(err, handshake.ErrBadHandshakeLine) {
				t.Errorf("err %v, want ErrBadHandshakeLine", err)
			}
			assertNoChildren(t)
		})
	}
}
