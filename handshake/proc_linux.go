package handshake

import (
	"errors"
	"os"
	"os/exec"
	"runtime"
	"sync"
	"syscall"

	"golang.org/x/sys/unix"
)

// processTree ties the plugin to the host: the plugin leads its own process group and gets
// SIGKILL when the host dies.
//
// The parent-death signal reaches the plugin only, not the children the plugin starts: when
// the host dies suddenly they survive it. The agent's systemd unit, whose cgroup is killed as a
// whole (KillMode=control-group), is the backstop for them. While the host lives, Kill and the
// plugin's own exit end the whole process group.
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

// Linux sends the parent-death signal when the thread that started the child ends, not the
// process. A Go thread ends when a goroutine locked to it returns, so a thread that once ran a
// start could end early and kill a healthy plugin. Every start therefore runs on the
// launcher's thread, which is locked and never ends.
var (
	launcherOnce     sync.Once
	launcherRequests chan launchRequest
)

type launchRequest struct {
	cmd    *exec.Cmd
	result chan<- error
}

// startCommand starts cmd on the launcher's thread.
func startCommand(cmd *exec.Cmd) error {
	launcherOnce.Do(func() {
		launcherRequests = make(chan launchRequest)
		go runLauncher(launcherRequests)
	})
	result := make(chan error, 1)
	launcherRequests <- launchRequest{cmd: cmd, result: result}
	return <-result
}

// runLauncher locks its goroutine to its thread and serves start requests for the life of the
// process; it never unlocks and never returns.
func runLauncher(requests <-chan launchRequest) {
	runtime.LockOSThread()
	for request := range requests {
		request.result <- request.cmd.Start()
	}
}
