package handshake

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
)

// processTree is a job object that kills its processes when its last handle closes: when
// the host dies, or when the plugin is reaped (ending any descendant left behind).
//
// The plugin starts suspended and runs only once it is in the job, so every process it starts
// is in the job too. One window remains: if the host dies between Start and attach, the plugin
// stays suspended outside the job. It has run no code, so it starts nothing, but nothing kills
// it either. No Go or golang.org/x/sys API closes that window: creating the process inside the
// job takes PROC_THREAD_ATTRIBUTE_JOB_LIST, which golang.org/x/sys does not define and
// syscall.SysProcAttr cannot pass.
type processTree struct {
	job windows.Handle
}

// newProcessTree creates the job and makes cmd start suspended.
func newProcessTree(cmd *exec.Cmd) (*processTree, error) {
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
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.CreationFlags |= windows.CREATE_SUSPENDED
	return &processTree{job: job}, nil
}

// attach puts the started, still suspended plugin in the job, then lets it run. Its pid cannot
// be reused yet: the process is not reaped. On an error the plugin may still be suspended; the
// caller terminates it, which works on a suspended process.
func (t *processTree) attach(process *os.Process) error {
	handle, err := windows.OpenProcess(windows.PROCESS_SET_QUOTA|windows.PROCESS_TERMINATE, false,
		uint32(process.Pid))
	if err != nil {
		return err
	}
	defer func() { _ = windows.CloseHandle(handle) }()
	if err := windows.AssignProcessToJobObject(t.job, handle); err != nil {
		return err
	}
	return resumeProcess(uint32(process.Pid))
}

// resumeProcess resumes the threads of a process started with CREATE_SUSPENDED.
//
// os/exec closes the main thread handle that CreateProcess returns, so the thread is found
// again by enumerating the system's threads with a Toolhelp snapshot and keeping those of pid.
// A suspended new process has run no code and has one thread, its main thread. This keeps
// exec.Cmd starting the process, so Wait, Kill and the inherited output pipes behave as for
// any command; NtResumeProcess would do the same in one call, but it is an undocumented ntdll
// export that golang.org/x/sys does not wrap.
func resumeProcess(pid uint32) error {
	snapshot, err := windows.CreateToolhelp32Snapshot(windows.TH32CS_SNAPTHREAD, 0)
	if err != nil {
		return fmt.Errorf("thread snapshot: %w", err)
	}
	defer func() { _ = windows.CloseHandle(snapshot) }()
	entry := windows.ThreadEntry32{Size: uint32(unsafe.Sizeof(windows.ThreadEntry32{}))}
	resumed := 0
	for err = windows.Thread32First(snapshot, &entry); err == nil; err = windows.Thread32Next(snapshot, &entry) {
		if entry.OwnerProcessID != pid {
			continue
		}
		if err := resumeThread(entry.ThreadID); err != nil {
			return err
		}
		resumed++
	}
	if !errors.Is(err, windows.ERROR_NO_MORE_FILES) {
		return fmt.Errorf("thread snapshot: %w", err)
	}
	if resumed == 0 {
		return fmt.Errorf("no thread of process %d to resume", pid)
	}
	return nil
}

// resumeThread resumes the thread with the given id.
func resumeThread(id uint32) error {
	thread, err := windows.OpenThread(windows.THREAD_SUSPEND_RESUME, false, id)
	if err != nil {
		return fmt.Errorf("opening thread %d: %w", id, err)
	}
	defer func() { _ = windows.CloseHandle(thread) }()
	if _, err := windows.ResumeThread(thread); err != nil {
		return fmt.Errorf("resuming thread %d: %w", id, err)
	}
	return nil
}

// kill terminates every process of the job.
func (t *processTree) kill(int) error {
	return windows.TerminateJobObject(t.job, 1)
}

// release closes the job, which kills what is left in it.
func (t *processTree) release() {
	_ = windows.CloseHandle(t.job)
}

// startCommand starts cmd; only Linux needs a dedicated thread.
func startCommand(cmd *exec.Cmd) error { return cmd.Start() }

// awaitExit cannot wait without reaping here; Windows holds the process handle, so no pid is
// reused while the job is open.
func awaitExit(int) bool { return false }
