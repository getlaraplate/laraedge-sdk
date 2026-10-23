package handshake

import (
	"errors"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// TestAwaitExitLeavesTheProcessToReap checks the guarantee Kill relies on: after awaitExit the
// process has exited but is not reaped, so its pid and process group id stay reserved.
func TestAwaitExitLeavesTheProcessToReap(t *testing.T) {
	path, err := exec.LookPath("true")
	if err != nil {
		t.Skip("no true command")
	}
	cmd := exec.Command(path)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	if !awaitExit(cmd.Process.Pid) {
		_ = cmd.Wait()
		t.Fatal("awaitExit could not wait for the process")
	}
	if err := syscall.Kill(cmd.Process.Pid, 0); err != nil {
		t.Errorf("the process was reaped by awaitExit: %v", err)
	}
	if err := cmd.Wait(); err != nil {
		t.Errorf("Wait after awaitExit: %v", err)
	}
}

// TestStartSurvivesTheEndOfTheCallingThread starts a child with the parent-death signal from
// a goroutine whose thread then ends: the child must not receive the signal, because the
// start ran on the launcher's thread.
func TestStartSurvivesTheEndOfTheCallingThread(t *testing.T) {
	path, err := exec.LookPath("sleep")
	if err != nil {
		t.Skip("no sleep command")
	}
	cmd := exec.Command(path, "60")
	if _, err := newProcessTree(cmd); err != nil {
		t.Fatal(err)
	}
	type result struct {
		tid int
		err error
	}
	var got result
	for attempt := 0; ; attempt++ {
		started := make(chan result, 1)
		go func() {
			// Returning while locked ends this goroutine's thread, except the main thread,
			// which the runtime keeps: retry until the goroutine runs on another thread.
			runtime.LockOSThread()
			if unix.Gettid() == os.Getpid() {
				runtime.UnlockOSThread()
				started <- result{tid: 0}
				return
			}
			started <- result{unix.Gettid(), startCommand(cmd)}
		}()
		if got = <-started; got.tid != 0 {
			break
		}
		if attempt == 100 {
			t.Skip("every attempt ran on the main thread")
		}
	}
	if got.err != nil {
		t.Fatalf("startCommand: %v", got.err)
	}
	defer func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}()
	fd, err := unix.PidfdOpen(cmd.Process.Pid, 0)
	if err != nil {
		t.Fatalf("pidfd_open: %v", err)
	}
	defer func() { _ = unix.Close(fd) }()

	// The thread's /proc entry goes away after its exit has sent any parent-death signal.
	task := "/proc/self/task/" + strconv.Itoa(got.tid)
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(task); errors.Is(err, os.ErrNotExist) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the locked thread did not end")
		}
		<-ticker.C
	}
	fds := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}
	if n, _ := unix.Poll(fds, 500); n == 1 {
		t.Fatal("the child died with the thread that asked for its start")
	}
}
