//go:build !linux

package storm

import "os"

func openProcStat() (*os.File, error) {
	return nil, os.ErrNotExist
}

func sampleFDs() int {
	return 0
}

func GetMaxFDs() int {
	return 0
}
