package handshake

import (
	"os"
	"os/exec"
	"unsafe"

	"golang.org/x/sys/windows"
)

// processTree is a job object that kills its processes when its last handle closes: when
// the host dies, or when the plugin is reaped (ending any descendant left behind).
//
// The plugin joins the job right after it starts; a child it started before that moment would
// stay outside the job.
type processTree struct {
	job windows.Handle
}

func newProcessTree(*exec.Cmd) (*processTree, error) {
	job, err := windows.CreateJobObject(nil, nil)
	if err != nil {
		return nil, err
	}
	info := windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION{
		BasicLimitInformation: windows.JOBOBJECT_BASIC_LIMIT_INFORMATION{
			LimitFlags: windows.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
		},
	}
	if _, err := windows.SetInformationJobObject(job, windows.JobObjectExtendedLimitInformation,
		uintptr(unsafe.Pointer(&info)), uint32(unsafe.Sizeof(info))); err != nil {
		_ = windows.CloseHandle(job)
		return nil, err
	}
	return &processTree{job: job}, nil
}

// attach puts the started plugin in the job. Its pid cannot be reused yet: the process is
// not reaped.
func (t *processTree) attach(process *os.Process) error {
	handle, err := windows.OpenProcess(windows.PROCESS_SET_QUOTA|windows.PROCESS_TERMINATE, false,
		uint32(process.Pid))
	if err != nil {
		return err
	}
	defer func() { _ = windows.CloseHandle(handle) }()
	return windows.AssignProcessToJobObject(t.job, handle)
}

// kill terminates every process of the job.
func (t *processTree) kill(int) error {
	return windows.TerminateJobObject(t.job, 1)
}

// release closes the job, which kills what is left in it.
func (t *processTree) release() {
	_ = windows.CloseHandle(t.job)
}

// awaitExit cannot wait without reaping here; Windows holds the process handle, so no pid is
// reused while the job is open.
func awaitExit(int) bool { return false }
