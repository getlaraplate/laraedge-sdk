package handshake

import (
	"os"
	"os/exec"
)

// ProcessTree exposes the job object of a start to the external tests, which drive the steps
// Launch runs back to back one at a time.
type ProcessTree struct{ tree *processTree }

// NewProcessTree prepares cmd for its job, as Launch does before the start.
func NewProcessTree(cmd *exec.Cmd) (*ProcessTree, error) {
	tree, err := newProcessTree(cmd)
	if err != nil {
		return nil, err
	}
	return &ProcessTree{tree: tree}, nil
}

// Attach puts the started process in the job and lets it run.
func (t *ProcessTree) Attach(process *os.Process) error { return t.tree.attach(process) }

// Kill terminates every process of the job.
func (t *ProcessTree) Kill() error { return t.tree.kill(0) }

// Release closes the job.
func (t *ProcessTree) Release() { t.tree.release() }
