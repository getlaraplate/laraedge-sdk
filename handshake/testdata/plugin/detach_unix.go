//go:build unix

package main

import (
	"os/exec"
	"syscall"
)

// detach moves the child to its own process group, out of reach of a group kill.
func detach(child *exec.Cmd) {
	child.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}
