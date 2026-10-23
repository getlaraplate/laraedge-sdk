package conformance

import (
	"context"
	"errors"
	"fmt"
	"unsafe"

	"github.com/Microsoft/go-winio"
	"golang.org/x/sys/windows"
)

// fileAllAccess is GENERIC_ALL as stored for a pipe: generic rights are mapped to the object's
// specific rights.
const fileAllAccess = 0x1F01FF

// transportPermissions reads the security descriptor of the pipe at address through a client
// handle: its DACL must be protected and hold one entry, allowing full access to the current
// user.
func transportPermissions(ctx context.Context, address string) (string, error) {
	conn, err := winio.DialPipeContext(ctx, address)
	if err != nil {
		return "", fmt.Errorf("opening the pipe: %w", err)
	}
	defer func() { _ = conn.Close() }()
	handle, ok := conn.(interface{ Fd() uintptr })
	if !ok {
		return "", fmt.Errorf("the pipe connection %T has no handle", conn)
	}
	sd, err := windows.GetSecurityInfo(windows.Handle(handle.Fd()), windows.SE_KERNEL_OBJECT, windows.DACL_SECURITY_INFORMATION)
	if err != nil {
		return "", fmt.Errorf("reading the pipe's security descriptor: %w", err)
	}
	control, _, err := sd.Control()
	if err != nil {
		return "", err
	}
	if control&windows.SE_DACL_PROTECTED == 0 {
		return "", fmt.Errorf("the pipe's DACL is not protected (%s)", sd)
	}
	dacl, _, err := sd.DACL()
	if err != nil {
		return "", err
	}
	if dacl == nil || dacl.AceCount != 1 {
		return "", fmt.Errorf("the pipe's DACL %s must hold exactly one entry, for the current user", sd)
	}
	var ace *windows.ACCESS_ALLOWED_ACE
	if err := windows.GetAce(dacl, 0, &ace); err != nil {
		return "", err
	}
	if ace.Header.AceType != windows.ACCESS_ALLOWED_ACE_TYPE {
		return "", fmt.Errorf("the pipe's DACL entry is not an allow entry (%s)", sd)
	}
	user, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil {
		return "", fmt.Errorf("the current user: %w", err)
	}
	sid := (*windows.SID)(unsafe.Pointer(&ace.SidStart))
	if !sid.Equals(user.User.Sid) {
		return "", fmt.Errorf("the pipe's DACL grants %s, not the current user %s", sid, user.User.Sid)
	}
	if ace.Mask != windows.GENERIC_ALL && ace.Mask != fileAllAccess {
		return "", errors.New("the pipe's DACL entry does not grant full access")
	}
	return "protected DACL granting only the current user", nil
}
