package handshake

import (
	"os/exec"
	"syscall"
	"testing"
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
