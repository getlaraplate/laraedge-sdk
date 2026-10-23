package handshake_test

import (
	"testing"
	"time"

	"golang.org/x/sys/windows"
)

// watchExit opens pid now and returns a function that waits up to a timeout for the process
// to terminate.
func watchExit(t *testing.T, pid int) func(time.Duration) bool {
	t.Helper()
	handle, err := windows.OpenProcess(windows.SYNCHRONIZE, false, uint32(pid))
	if err != nil {
		t.Fatalf("OpenProcess(%d): %v", pid, err)
	}
	t.Cleanup(func() { _ = windows.CloseHandle(handle) })
	return func(timeout time.Duration) bool {
		event, err := windows.WaitForSingleObject(handle, uint32(timeout.Milliseconds()))
		return err == nil && event == windows.WAIT_OBJECT_0
	}
}

// assertNoChildren is checked on Linux only.
func assertNoChildren(*testing.T) {}
