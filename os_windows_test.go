//go:build windows

package gnetplus

import (
	"syscall"
)

func SysClose(fd int) error {
	return syscall.CloseHandle(syscall.Handle(fd))
}
