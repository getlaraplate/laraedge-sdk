package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net"
	"os"

	"github.com/Microsoft/go-winio"
	"golang.org/x/sys/windows"
)

// listen opens the plugin's named pipe, granting only the current user unless
// BROKEN_PERMISSIONS is set, with a random name unless BROKEN_FIXED_ADDRESS is set.
func listen() (net.Listener, string, string, error) {
	user, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil {
		return nil, "", "", err
	}
	descriptor := "D:P(A;;GA;;;" + user.User.Sid.String() + ")"
	if defect("BROKEN_PERMISSIONS") {
		descriptor += "(A;;GA;;;WD)"
	}
	var random [4]byte
	_, _ = rand.Read(random[:])
	name := fmt.Sprintf(`\\.\pipe\laraedge-%d-%s`, os.Getpid(), hex.EncodeToString(random[:]))
	if defect("BROKEN_FIXED_ADDRESS") {
		name = `\\.\pipe\laraedge-conformance-broken`
	}
	lis, err := winio.ListenPipe(name, &winio.PipeConfig{SecurityDescriptor: descriptor})
	if err != nil {
		return nil, "", "", err
	}
	return lis, "npipe", name, nil
}
