//go:build !windows

package main

import "os"

// syncDir fsyncs a directory, so that a file created in it is still there after a crash.
func syncDir(dir string) (err error) {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := d.Close(); err == nil {
			err = cerr
		}
	}()
	return d.Sync()
}
