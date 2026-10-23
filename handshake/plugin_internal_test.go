package handshake

import (
	"bytes"
	"errors"
	"os"
	"runtime"
	"testing"
)

// fakeEnv is an environment for listen.
type fakeEnv map[string]string

func (e fakeEnv) get(key string) string { return e[key] }

// shortDir returns a short private directory: Unix socket paths have a small length limit.
func shortDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "hs")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

func TestListenChecksTheCookieAndNegotiates(t *testing.T) {
	cases := []struct {
		name      string
		cookie    string
		offered   string
		supported []string
		want      Protocol
		wantErr   error
	}{
		{"no cookie", "", "v1", []string{"v1"}, "", ErrNotLaunched},
		{"wrong cookie", "not-the-cookie", "v1", []string{"v1"}, "", ErrNotLaunched},
		{"host preference wins", CookieValue, "v2,v1", []string{"v1", "v2"}, "v2", nil},
		{"first common protocol", CookieValue, " v3 , v1 ", []string{"v1"}, "v1", nil},
		{"no common protocol", CookieValue, "v2", []string{"v1"}, "", ErrNoProtocol},
		{"host offers none", CookieValue, "", []string{"v1"}, "", ErrNoProtocol},
		{"plugin supports none", CookieValue, "v1", nil, "", ErrNoProtocol},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := fakeEnv{CookieKey: tc.cookie, ProtocolsKey: tc.offered, SocketDirKey: shortDir(t)}
			var stdout bytes.Buffer
			listener, protocol, err := listen(env.get, &stdout, tc.supported)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("err %v, want %v", err, tc.wantErr)
				}
				if listener != nil {
					t.Error("a listener was returned with the error")
				}
				if stdout.Len() != 0 {
					t.Errorf("printed %q on failure", stdout.String())
				}
				return
			}
			if err != nil {
				t.Fatalf("listen: %v", err)
			}
			defer func() { _ = listener.Close() }()
			if protocol != tc.want {
				t.Errorf("protocol %q, want %q", protocol, tc.want)
			}
			network := "unix"
			if runtime.GOOS == "windows" {
				network = "npipe"
			}
			want := "1|" + string(tc.want) + "|" + network + "|" + listener.Addr().String() + "\n"
			if got := stdout.String(); got != want {
				t.Errorf("handshake line %q, want %q", got, want)
			}
		})
	}
}
