//go:build unix && !linux

package handshake_test

import (
	"testing"
	"time"
)

// watchExit has no implementation here: without a parent-death guarantee the tests that
// need it skip on this platform.
func watchExit(t *testing.T, _ int) func(time.Duration) bool {
	t.Helper()
	t.Skip("process exit watching is implemented for Linux and Windows")
	return nil
}

// assertNoChildren is checked on Linux only.
func assertNoChildren(*testing.T) {}
