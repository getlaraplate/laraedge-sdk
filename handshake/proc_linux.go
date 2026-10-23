package handshake

import (
	"errors"
	"os"
	"os/exec"
	"syscall"

	"golang.org/x/sys/unix"
)

// processTree ties the plugin to the host: the plugin leads its own process group and gets
// SIGKILL when the host dies.
//
// Linux delivers the parent-death signal when the thread that started the child ends. Go
// threads outlive the host process except when a goroutine locked to its thread returns
// without unlocking; a host that does so may see a plugin killed early.
type processTree struct{}

func newProcessTree(cmd *exec.Cmd) (*processTree, error) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
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

// awaitExit blocks until the process has exited without reaping it, and reports whether it
// could.
func awaitExit(pid int) bool {
	for {
		var info unix.Siginfo
		err := unix.Waitid(unix.P_PID, pid, &info, unix.WEXITED|unix.WNOWAIT, nil)
		if errors.Is(err, unix.EINTR) {
			continue
		}
		return err == nil
	}
}
