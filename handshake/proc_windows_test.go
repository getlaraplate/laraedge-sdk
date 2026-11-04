package handshake_test

import (
	"bufio"
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"

	"github.com/getlaraplate/laraedge-sdk/handshake"
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

// procSuspendThread is not wrapped by golang.org/x/sys/windows.
var procSuspendThread = windows.NewLazySystemDLL("kernel32.dll").NewProc("SuspendThread")

// isSuspended reports whether every thread of pid is suspended: the process runs no code.
// Each thread is suspended once more to read its count, then resumed.
func isSuspended(t *testing.T, pid int) bool {
	t.Helper()
	snapshot, err := windows.CreateToolhelp32Snapshot(windows.TH32CS_SNAPTHREAD, 0)
	if err != nil {
		t.Fatalf("thread snapshot: %v", err)
	}
	defer func() { _ = windows.CloseHandle(snapshot) }()
	entry := windows.ThreadEntry32{Size: uint32(unsafe.Sizeof(windows.ThreadEntry32{}))}
	threads := 0
	for err = windows.Thread32First(snapshot, &entry); err == nil; err = windows.Thread32Next(snapshot, &entry) {
		if entry.OwnerProcessID != uint32(pid) {
			continue
		}
		thread, err := windows.OpenThread(windows.THREAD_SUSPEND_RESUME, false, entry.ThreadID)
		if err != nil {
			continue // the thread ended meanwhile
		}
		previous, _, callErr := procSuspendThread.Call(uintptr(thread))
		if previous == 0xffffffff {
			_ = windows.CloseHandle(thread)
			t.Fatalf("SuspendThread: %v", callErr)
		}
		_, _ = windows.ResumeThread(thread)
		_ = windows.CloseHandle(thread)
		if previous == 0 {
			return false
		}
		threads++
	}
	if !errors.Is(err, windows.ERROR_NO_MORE_FILES) {
		t.Fatalf("thread snapshot: %v", err)
	}
	return threads > 0
}

// TestJobHoldsAChildStartedAsTheFirstAction starts the helper as Launch does, but takes the
// job assignment as a separate step: until it, the helper must not run, or the child it
// starts as its first action would be outside the job and survive Kill.
func TestJobHoldsAChildStartedAsTheFirstAction(t *testing.T) {
	stdoutRead, stdoutWrite, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stdoutRead.Close() }()
	cmd := &exec.Cmd{Path: helperPath, Args: []string{helperPath}, Env: helperEnv("HELPER_CHILD_FIRST=1"),
		Stdout: stdoutWrite}
	tree, err := handshake.NewProcessTree(cmd)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Release()
	err = cmd.Start()
	_ = stdoutWrite.Close()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}()
	lines := make(chan string, 1)
	go func() {
		line, _ := bufio.NewReader(stdoutRead).ReadString('\n')
		lines <- strings.TrimRight(line, "\r\n")
	}()

	// Before the assignment the helper is either suspended or has already started its child.
	var line string
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.Now().Add(10 * time.Second)
	for line == "" && !isSuspended(t, cmd.Process.Pid) {
		if time.Now().After(deadline) {
			t.Fatal("the helper neither stayed suspended nor started its child")
		}
		select {
		case line = <-lines:
		case <-ticker.C:
		}
	}
	if err := tree.Attach(cmd.Process); err != nil {
		t.Fatalf("Attach: %v", err)
	}
	if line == "" {
		select {
		case line = <-lines:
		case <-time.After(10 * time.Second):
			t.Fatal("the helper printed no child line after the assignment")
		}
	}
	childPid := parseChildLine(t, line)
	childExited := watchExit(t, childPid)

	if err := tree.Kill(); err != nil {
		t.Fatalf("Kill: %v", err)
	}
	if !childExited(10 * time.Second) {
		killLeftover(childPid)
		t.Fatal("the child the helper started first escaped the job and survived Kill")
	}
}
