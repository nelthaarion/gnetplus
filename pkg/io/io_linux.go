/*
 * Copyright (c) 2021 The Gnet Authors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io

import (
	"runtime"
	"unsafe"

	"golang.org/x/sys/unix"
)

const stackIovecs = 16

// Writev invokes non-blocking writev(2) directly via RawSyscall on Linux.
//
// Like ReadNB/WriteNB, gnetplus sockets are always O_NONBLOCK so skipping Go's
// entersyscall/exitsyscall bracket avoids scheduler overhead. Batches with at
// most stackIovecs non-empty buffers use a stack-backed [stackIovecs]unix.Iovec
// array with zero allocations; larger batches allocate a heap-backed slice.
// Both iovecs and iov are kept alive across RawSyscall so neither the Iovec
// descriptor array nor the underlying byte buffers can be collected before the
// syscall returns.
func Writev(fd int, iov [][]byte) (int, error) {
	if len(iov) == 0 {
		return 0, nil
	}
	var stack [stackIovecs]unix.Iovec
	var iovecs []unix.Iovec
	if len(iov) <= stackIovecs {
		iovecs = stack[:]
	} else {
		nonEmpty := 0
		for _, b := range iov {
			if len(b) > 0 {
				nonEmpty++
			}
		}
		if nonEmpty == 0 {
			return 0, nil
		}
		if nonEmpty <= stackIovecs {
			iovecs = stack[:]
		} else {
			iovecs = make([]unix.Iovec, nonEmpty)
		}
	}
	n := 0
	for _, b := range iov {
		if len(b) == 0 {
			continue
		}
		iovecs[n].Base = unsafe.SliceData(b)
		iovecs[n].SetLen(len(b))
		n++
	}
	if n == 0 {
		return 0, nil
	}
	r, _, e := unix.RawSyscall(unix.SYS_WRITEV, uintptr(fd), uintptr(unsafe.Pointer(&iovecs[0])), uintptr(n)) //nolint:staticcheck
	runtime.KeepAlive(iovecs)
	runtime.KeepAlive(iov)
	if e != 0 {
		return -1, errnoErr(e)
	}
	return int(r), nil
}

// Readv invokes non-blocking readv(2) directly via RawSyscall on Linux.
//
// Like Writev, batches with at most stackIovecs non-empty buffers use a
// stack-backed array (0 allocations), larger batches use a heap-backed slice,
// and both iovecs and iov are explicitly kept alive until RawSyscall completes.
func Readv(fd int, iov [][]byte) (int, error) {
	if len(iov) == 0 {
		return 0, nil
	}
	var stack [stackIovecs]unix.Iovec
	var iovecs []unix.Iovec
	if len(iov) <= stackIovecs {
		iovecs = stack[:]
	} else {
		nonEmpty := 0
		for _, b := range iov {
			if len(b) > 0 {
				nonEmpty++
			}
		}
		if nonEmpty == 0 {
			return 0, nil
		}
		if nonEmpty <= stackIovecs {
			iovecs = stack[:]
		} else {
			iovecs = make([]unix.Iovec, nonEmpty)
		}
	}
	n := 0
	for _, b := range iov {
		if len(b) == 0 {
			continue
		}
		iovecs[n].Base = unsafe.SliceData(b)
		iovecs[n].SetLen(len(b))
		n++
	}
	if n == 0 {
		return 0, nil
	}
	r, _, e := unix.RawSyscall(unix.SYS_READV, uintptr(fd), uintptr(unsafe.Pointer(&iovecs[0])), uintptr(n)) //nolint:staticcheck
	runtime.KeepAlive(iovecs)
	runtime.KeepAlive(iov)
	if e != 0 {
		return -1, errnoErr(e)
	}
	return int(r), nil
}
