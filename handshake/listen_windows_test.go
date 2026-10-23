package handshake_test

import (
	"context"
	"regexp"
	"strconv"
	"testing"
	"time"
	"unsafe"

	"github.com/Microsoft/go-winio"
	"golang.org/x/sys/windows"
)

var pipeName = regexp.MustCompile(`^\\\\\.\\pipe\\laraedge-(\d+)-[0-9a-f]{8}$`)

// TestPipeGrantsOnlyTheCurrentUser reads the pipe's security descriptor back through a client
// handle opened with go-winio: a protected DACL with one allow entry for the current user.
func TestPipeGrantsOnlyTheCurrentUser(t *testing.T) {
	process := launch(t, config(t))
	match := pipeName.FindStringSubmatch(process.Address)
	if match == nil {
		t.Fatalf("pipe name %q, want \\\\.\\pipe\\laraedge-<pid>-<8 hex>", process.Address)
	}
	if match[1] != strconv.Itoa(process.Pid) {
		t.Errorf("pipe name carries pid %s, want %d", match[1], process.Pid)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := winio.DialPipeContext(ctx, process.Address)
	if err != nil {
		t.Fatalf("DialPipe: %v", err)
	}
	defer func() { _ = conn.Close() }()
	handle, ok := conn.(interface{ Fd() uintptr })
	if !ok {
		t.Fatalf("pipe connection %T has no handle", conn)
	}
	sd, err := windows.GetSecurityInfo(windows.Handle(handle.Fd()), windows.SE_KERNEL_OBJECT,
		windows.DACL_SECURITY_INFORMATION)
	if err != nil {
		t.Fatalf("GetSecurityInfo: %v", err)
	}
	control, _, err := sd.Control()
	if err != nil {
		t.Fatal(err)
	}
	if control&windows.SE_DACL_PROTECTED == 0 {
		t.Errorf("DACL not protected (%s)", sd)
	}
	dacl, _, err := sd.DACL()
	if err != nil {
		t.Fatal(err)
	}
	if dacl == nil || dacl.AceCount != 1 {
		t.Fatalf("DACL %s, want exactly one entry", sd)
	}
	var ace *windows.ACCESS_ALLOWED_ACE
	if err := windows.GetAce(dacl, 0, &ace); err != nil {
		t.Fatal(err)
	}
	if ace.Header.AceType != windows.ACCESS_ALLOWED_ACE_TYPE {
		t.Errorf("entry type %d, want allow (%s)", ace.Header.AceType, sd)
	}
	user, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil {
		t.Fatal(err)
	}
	sid := (*windows.SID)(unsafe.Pointer(&ace.SidStart))
	if !sid.Equals(user.User.Sid) {
		t.Errorf("entry grants %s, want the current user %s", sid, user.User.Sid)
	}
	// GENERIC_ALL is stored mapped to the pipe's specific rights.
	const fileAllAccess = 0x1F01FF
	if ace.Mask != windows.GENERIC_ALL && ace.Mask != fileAllAccess {
		t.Errorf("entry mask %#x, want full access (%s)", ace.Mask, sd)
	}
}
