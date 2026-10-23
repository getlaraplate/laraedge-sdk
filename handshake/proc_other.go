//go:build unix && !linux

package handshake

import (
	"errors"
	"os"
	"os/exec"
	"syscall"
)

// processTree puts the plugin in its own process group. These systems offer no parent-death
// signal: a plugin may outlive a host that dies, so they serve for development only.
type processTree struct{}

func newProcessTree(cmd *exec.Cmd) (*processTree, error) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	return &processTree{}, nil
}

func (*processTree) attach(*os.Process) error { return nil }

// kill sends SIGKILL to the plugin's process group.
func (*processTree) kill(pid int) error {
	err := syscall.Kill(-pid, syscall.SIGKILL)
	if errors.Is(err, syscall.ESRCH) {
		return os.ErrProcessDone
	}
	return err
}

func (*processTree) release() {}

// awaitExit cannot wait without reaping here.
func awaitExit(int) bool { return false }
