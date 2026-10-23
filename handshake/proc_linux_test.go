package handshake_test

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// watchExit opens a pidfd on pid now and returns a function that waits up to a timeout for
// the process to terminate. A pidfd becomes readable when its process terminates.
func watchExit(t *testing.T, pid int) func(time.Duration) bool {
	t.Helper()
	fd, err := unix.PidfdOpen(pid, 0)
	if err != nil {
		t.Fatalf("pidfd_open(%d): %v", pid, err)
	}
	t.Cleanup(func() { _ = unix.Close(fd) })
	return func(timeout time.Duration) bool {
		deadline := time.Now().Add(timeout)
		for {
			remaining := time.Until(deadline)
			if remaining < 0 {
				remaining = 0
			}
			fds := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}
			n, err := unix.Poll(fds, int(remaining.Milliseconds()))
			if errors.Is(err, unix.EINTR) {
				continue
			}
			return err == nil && n == 1
		}
	}
}

// assertNoChildren fails when the test process still has a child process: a failed Launch
// must kill and reap the plugin it started.
func assertNoChildren(t *testing.T) {
	t.Helper()
	files, err := filepath.Glob("/proc/self/task/*/children")
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			continue // the thread ended meanwhile
		}
		if children := strings.TrimSpace(string(data)); children != "" {
			t.Errorf("child processes left behind: %s", children)
		}
	}
}
