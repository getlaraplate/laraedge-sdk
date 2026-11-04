//go:build windows

package main

// syncDir does nothing on Windows: Go cannot fsync a directory there (os.File.Sync on a
// directory fails), and NTFS journals metadata changes such as the creation of a file.
func syncDir(string) error { return nil }
