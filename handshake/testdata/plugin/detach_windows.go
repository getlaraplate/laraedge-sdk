package main

import "os/exec"

// detach does nothing on Windows: the child stays in the host's job object.
func detach(*exec.Cmd) {}
