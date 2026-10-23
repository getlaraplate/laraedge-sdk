//go:build unix

package conformance

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// transportPermissions checks that the socket at address has mode 0600 and its directory 0700.
func transportPermissions(_ context.Context, address string) (string, error) {
	var problems []string
	socket, err := os.Lstat(address)
	switch {
	case err != nil:
		problems = append(problems, err.Error())
	case socket.Mode().Type() != fs.ModeSocket:
		problems = append(problems, fmt.Sprintf("%s is not a socket", address))
	case socket.Mode().Perm() != 0o600:
		problems = append(problems, fmt.Sprintf("the socket has mode %#o, want 0600", socket.Mode().Perm()))
	}
	dir, err := os.Lstat(filepath.Dir(address))
	switch {
	case err != nil:
		problems = append(problems, err.Error())
	case !dir.IsDir():
		problems = append(problems, fmt.Sprintf("%s is not a directory", filepath.Dir(address)))
	case dir.Mode().Perm() != 0o700:
		problems = append(problems, fmt.Sprintf("the socket directory has mode %#o, want 0700", dir.Mode().Perm()))
	}
	if len(problems) > 0 {
		return "", errors.New(strings.Join(problems, "; "))
	}
	return "socket 0600, directory 0700", nil
}
