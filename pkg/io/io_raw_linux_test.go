// Copyright (c) 2025 The Gnet Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build linux

package io

import (
	"bytes"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"golang.org/x/sys/unix"
)

func nonblockingPair(t *testing.T) (a, b int) {
	t.Helper()
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_NONBLOCK|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = unix.Close(fds[0]); _ = unix.Close(fds[1]) })
	return fds[0], fds[1]
}

func TestReadWriteNB(t *testing.T) {
	a, b := nonblockingPair(t)

	// nothing to read yet: EAGAIN, n == -1 like unix.Read, and comparable with unix.EAGAIN
	buf := make([]byte, 16)
	n, err := ReadNB(b, buf)
	if n != -1 || err != unix.EAGAIN {
		t.Fatalf("empty socket: n=%d err=%v, want -1 / EAGAIN", n, err)
	}

	msg := []byte("raw syscalls")
	if n, err = WriteNB(a, msg); n != len(msg) || err != nil {
		t.Fatalf("write: n=%d err=%v", n, err)
	}
	if n, err = ReadNB(b, buf); n != len(msg) || err != nil || !bytes.Equal(buf[:n], msg) {
		t.Fatalf("read: n=%d err=%v data=%q", n, err, buf[:max(n, 0)])
	}

	// zero-length operations must not touch the (nil) buffer
	if n, err = WriteNB(a, nil); n != 0 || err != nil {
		t.Fatalf("empty write: n=%d err=%v", n, err)
	}
	if n, err = ReadNB(b, nil); n != -1 && n != 0 {
		t.Fatalf("empty read: n=%d err=%v", n, err)
	}

	// peer closed: read returns 0, EOF is the caller's business
	_ = unix.Close(a)
	if n, err = ReadNB(b, buf); n != 0 || err != nil {
		t.Fatalf("after peer close: n=%d err=%v, want 0 / nil", n, err)
	}
}

func TestWriteNBErrors(t *testing.T) {
	if n, err := WriteNB(-1, []byte("x")); n != -1 || err != unix.EBADF {
		t.Fatalf("bad fd: n=%d err=%v, want -1 / EBADF", n, err)
	}
	if n, err := ReadNB(-1, make([]byte, 1)); n != -1 || err != unix.EBADF {
		t.Fatalf("bad fd: n=%d err=%v, want -1 / EBADF", n, err)
	}

	// EPIPE: write to a socket whose peer is gone (SIGPIPE is ignored by the Go runtime for
	// non-stdout descriptors, which is what the engine relies on as well)
	a, b := nonblockingPair(t)
	_ = unix.Close(b)
	if n, err := WriteNB(a, []byte("x")); n != -1 || err != unix.EPIPE {
		t.Fatalf("broken pipe: n=%d err=%v, want -1 / EPIPE", n, err)
	}
}

// A full socket buffer must report a short write or EAGAIN, never block the thread.
func TestWriteNBFullBufferDoesNotBlock(t *testing.T) {
	a, _ := nonblockingPair(t)
	chunk := make([]byte, 64*1024)
	total := 0
	for i := 0; i < 1<<14; i++ {
		n, err := WriteNB(a, chunk)
		if err == unix.EAGAIN {
			if total == 0 {
				t.Fatal("EAGAIN before any byte was accepted")
			}
			return
		}
		if err != nil {
			t.Fatalf("unexpected error after %d bytes: %v", total, err)
		}
		total += n
	}
	t.Fatalf("socket buffer never filled up after %d bytes", total)
}

// The errno boxing must not allocate on the hot EAGAIN path.
func TestReadNBNoAllocOnEAGAIN(t *testing.T) {
	_, b := nonblockingPair(t)
	buf := make([]byte, 64)
	if allocs := testing.AllocsPerRun(200, func() { _, _ = ReadNB(b, buf) }); allocs != 0 {
		t.Fatalf("ReadNB allocates %.1f times per call on EAGAIN", allocs)
	}
}

func TestWritevReadv(t *testing.T) {
	a, b := nonblockingPair(t)

	// Empty / all-zero iov slices return 0, nil.
	if n, err := Writev(a, nil); n != 0 || err != nil {
		t.Fatalf("empty Writev: n=%d err=%v", n, err)
	}
	if n, err := Writev(a, [][]byte{{}, nil}); n != 0 || err != nil {
		t.Fatalf("zero-len Writev: n=%d err=%v", n, err)
	}
	if n, err := Readv(b, [][]byte{make([]byte, 8)}); n != -1 || err != unix.EAGAIN {
		t.Fatalf("empty socket Readv: n=%d err=%v, want -1/EAGAIN", n, err)
	}

	parts := [][]byte{[]byte("hello "), nil, []byte("scatter-"), []byte("gather")}
	want := "hello scatter-gather"
	n, err := Writev(a, parts)
	if err != nil || n != len(want) {
		t.Fatalf("Writev: n=%d err=%v", n, err)
	}
	r1 := make([]byte, 6)
	r2 := make([]byte, 32)
	rn, rerr := Readv(b, [][]byte{r1, r2})
	if rerr != nil || rn != len(want) {
		t.Fatalf("Readv: n=%d err=%v", rn, rerr)
	}
	got := string(r1) + string(r2[:rn-len(r1)])
	if got != want {
		t.Fatalf("Readv data=%q, want %q", got, want)
	}

	// Overflow path (> stackIovecs buffers).
	many := make([][]byte, stackIovecs+5)
	for i := range many {
		many[i] = []byte("ab")
	}
	n, err = Writev(a, many)
	if err != nil || n != 2*len(many) {
		t.Fatalf("Writev overflow: n=%d err=%v", n, err)
	}
	rbuf := make([]byte, n)
	if rn, rerr = ReadNB(b, rbuf); rerr != nil || rn != n {
		t.Fatalf("ReadNB after Writev overflow: n=%d err=%v", rn, rerr)
	}

	// Bad fd returns -1 and EBADF.
	if n, err := Writev(-1, [][]byte{[]byte("x")}); n != -1 || err != unix.EBADF {
		t.Fatalf("bad fd Writev: n=%d err=%v", n, err)
	}
}

func TestWritevReadvNoAlloc(t *testing.T) {
	a, b := nonblockingPair(t)

	// 2-buffer stack path: 0 allocs.
	wbufs2 := [][]byte{[]byte("head"), []byte("tail")}
	rbufs2 := [][]byte{make([]byte, 4), make([]byte, 4)}
	if allocs := testing.AllocsPerRun(200, func() {
		if _, err := Writev(a, wbufs2); err != nil {
			t.Fatal(err)
		}
		if _, err := Readv(b, rbufs2); err != nil {
			t.Fatal(err)
		}
	}); allocs != 0 {
		t.Fatalf("Writev+Readv (2 bufs) allocates %.1f times per call, want 0", allocs)
	}

	// Exact stackIovecs (16) boundary: 0 allocs.
	wbufs16 := make([][]byte, stackIovecs)
	rbufs16 := make([][]byte, stackIovecs)
	for i := range wbufs16 {
		wbufs16[i] = []byte("abcd")
		rbufs16[i] = make([]byte, 4)
	}
	if allocs := testing.AllocsPerRun(200, func() {
		if _, err := Writev(a, wbufs16); err != nil {
			t.Fatal(err)
		}
		if _, err := Readv(b, rbufs16); err != nil {
			t.Fatal(err)
		}
	}); allocs != 0 {
		t.Fatalf("Writev+Readv (16 bufs) allocates %.1f times per call, want 0", allocs)
	}

	// >stackIovecs total entries with <=stackIovecs non-empty: 0 allocs.
	sparseW := make([][]byte, stackIovecs+8)
	sparseR := make([][]byte, stackIovecs+8)
	for i := 0; i < 8; i++ {
		sparseW[i*3] = []byte("xy")
		sparseR[i*3] = make([]byte, 2)
	}
	if allocs := testing.AllocsPerRun(200, func() {
		if _, err := Writev(a, sparseW); err != nil {
			t.Fatal(err)
		}
		if _, err := Readv(b, sparseR); err != nil {
			t.Fatal(err)
		}
	}); allocs != 0 {
		t.Fatalf("Writev+Readv (24 sparse, 8 non-empty) allocates %.1f times per call, want 0", allocs)
	}

	// All-empty >stackIovecs batch: 0 allocs.
	emptyMany := make([][]byte, stackIovecs+8)
	if allocs := testing.AllocsPerRun(200, func() {
		if n, err := Writev(a, emptyMany); n != 0 || err != nil {
			t.Fatalf("empty Writev: n=%d err=%v", n, err)
		}
		if n, err := Readv(b, emptyMany); n != 0 || err != nil {
			t.Fatalf("empty Readv: n=%d err=%v", n, err)
		}
	}); allocs != 0 {
		t.Fatalf("all-empty >16 Writev+Readv allocates %.1f times per call, want 0", allocs)
	}

	// EAGAIN on small batch: 0 allocs.
	if allocs := testing.AllocsPerRun(200, func() {
		if n, err := Readv(b, rbufs16); n != -1 || err != unix.EAGAIN {
			t.Fatalf("Readv EAGAIN: n=%d err=%v", n, err)
		}
	}); allocs != 0 {
		t.Fatalf("Readv EAGAIN (16 bufs) allocates %.1f times per call, want 0", allocs)
	}

	// Heap-backed >stackIovecs batch: exactly 1 alloc per Writev and 1 per Readv (no extra growth).
	wbufs24 := make([][]byte, stackIovecs+8)
	rbufs24 := make([][]byte, stackIovecs+8)
	for i := range wbufs24 {
		wbufs24[i] = []byte("abcd")
		rbufs24[i] = make([]byte, 4)
	}
	if allocs := testing.AllocsPerRun(200, func() {
		if _, err := Writev(a, wbufs24); err != nil {
			t.Fatal(err)
		}
		if _, err := Readv(b, rbufs24); err != nil {
			t.Fatal(err)
		}
	}); allocs != 2 {
		t.Fatalf("Writev+Readv (24 bufs) allocates %.1f times per pair, want 2 (1 per call)", allocs)
	}
	if allocs := testing.AllocsPerRun(200, func() {
		if n, err := Readv(b, rbufs24); n != -1 || err != unix.EAGAIN {
			t.Fatalf("Readv EAGAIN (24 bufs): n=%d err=%v", n, err)
		}
	}); allocs != 1 {
		t.Fatalf("Readv EAGAIN (24 bufs) allocates %.1f times per call, want 1", allocs)
	}
}

func TestWritevReadvEmptyAndErrors(t *testing.T) {
	a, b := nonblockingPair(t)

	// Empty inputs (nil, empty slice, zero-len slices, and >stackIovecs empty slices).
	emptyInputs := [][][]byte{
		nil,
		{},
		{{}, nil, make([]byte, 0, 16)},
		make([][]byte, stackIovecs+8),
	}
	for i, in := range emptyInputs {
		if n, err := Writev(a, in); n != 0 || err != nil {
			t.Fatalf("empty Writev[%d]: n=%d err=%v, want 0/nil", i, n, err)
		}
		if n, err := Readv(b, in); n != 0 || err != nil {
			t.Fatalf("empty Readv[%d]: n=%d err=%v, want 0/nil", i, n, err)
		}
	}

	// EBADF for both small (<=16) and overflow (>16) batches on both Readv and Writev.
	small := [][]byte{[]byte("x"), []byte("y")}
	overflow := make([][]byte, stackIovecs+8)
	for i := range overflow {
		overflow[i] = []byte("xy")
	}
	for _, tc := range []struct {
		name string
		bufs [][]byte
	}{
		{"small", small},
		{"overflow", overflow},
	} {
		if n, err := Writev(-1, tc.bufs); n != -1 || err != unix.EBADF {
			t.Fatalf("Writev(-1, %s): n=%d err=%v, want -1/EBADF", tc.name, n, err)
		}
		if n, err := Readv(-1, tc.bufs); n != -1 || err != unix.EBADF {
			t.Fatalf("Readv(-1, %s): n=%d err=%v, want -1/EBADF", tc.name, n, err)
		}
		if n, err := Readv(b, tc.bufs); n != -1 || err != unix.EAGAIN {
			t.Fatalf("Readv(empty, %s): n=%d err=%v, want -1/EAGAIN", tc.name, n, err)
		}
	}

	// Writev EAGAIN when socket buffer is full (test both small and >16 batches).
	chunk := make([]byte, 32*1024)
	wSmall := [][]byte{chunk, chunk}
	hitEAGAIN := false
	for i := 0; i < 1<<14; i++ {
		n, err := Writev(a, wSmall)
		if err == unix.EAGAIN {
			if n != -1 {
				t.Fatalf("Writev EAGAIN returned n=%d, want -1", n)
			}
			hitEAGAIN = true
			break
		}
		if err != nil {
			t.Fatalf("unexpected Writev error while filling buffer: %v", err)
		}
	}
	if !hitEAGAIN {
		t.Fatal("Writev small batch never hit EAGAIN")
	}

	// With buffer full, >16 batch must also return -1 / EAGAIN cleanly.
	wOverflow := make([][]byte, stackIovecs+8)
	for i := range wOverflow {
		wOverflow[i] = make([]byte, 64)
	}
	if n, err := Writev(a, wOverflow); n != -1 || err != unix.EAGAIN {
		t.Fatalf("Writev(full, overflow): n=%d err=%v, want -1/EAGAIN", n, err)
	}

	// Zero allocs on Writev EAGAIN for small batch.
	if allocs := testing.AllocsPerRun(100, func() {
		if n, err := Writev(a, wSmall); n != -1 || err != unix.EAGAIN {
			t.Fatalf("Writev EAGAIN: n=%d err=%v", n, err)
		}
	}); allocs != 0 {
		t.Fatalf("Writev EAGAIN (small) allocates %.1f times per call, want 0", allocs)
	}

	// Peer closed: Readv returns 0, nil (EOF) after draining; Writev returns -1, EPIPE.
	c, d := nonblockingPair(t)
	_ = unix.Close(c)
	if n, err := Readv(d, overflow); n != 0 || err != nil {
		t.Fatalf("Readv(closed peer, overflow): n=%d err=%v, want 0/nil", n, err)
	}
	if n, err := Writev(d, overflow); n != -1 || err != unix.EPIPE {
		t.Fatalf("Writev(closed peer, overflow): n=%d err=%v, want -1/EPIPE", n, err)
	}
}

func TestWritevReadvOverflowGCPressure(t *testing.T) {
	a, b := nonblockingPair(t)

	// Background goroutines continuously trigger GC and allocate garbage to maximize
	// the chance of catching any missing KeepAlive on heap-backed Iovec arrays or
	// underlying byte buffers during RawSyscall.
	var stop atomic.Bool
	var gcWG sync.WaitGroup
	for g := 0; g < 2; g++ {
		gcWG.Add(1)
		go func() {
			defer gcWG.Done()
			for !stop.Load() {
				_ = make([]byte, 4096)
				runtime.GC()
			}
		}()
	}
	defer func() {
		stop.Store(true)
		gcWG.Wait()
	}()

	// Exercise stack boundary (16), first heap size (17), and larger batches (24, 32, 64)
	// with interleaved empty/nil slices and freshly-allocated buffers.
	batchCounts := []int{stackIovecs, stackIovecs + 1, 24, 32, 64}
	for _, count := range batchCounts {
		for iter := 0; iter < 50; iter++ {
			var expected bytes.Buffer
			// Interleave an empty slice every 4th entry to test skipping on heap path too.
			wbufs := make([][]byte, 0, count+count/4)
			rbufs := make([][]byte, 0, count+count/4)
			chunkLen := 17 + (iter % 13)
			for i := 0; i < count; i++ {
				if i%4 == 0 {
					wbufs = append(wbufs, nil, []byte{})
					rbufs = append(rbufs, nil, []byte{})
				}
				buf := make([]byte, chunkLen)
				for j := range buf {
					buf[j] = byte((iter*31 + i*7 + j) & 0xff)
				}
				expected.Write(buf)
				wbufs = append(wbufs, buf)
				rbufs = append(rbufs, make([]byte, chunkLen))
			}

			total := expected.Len()
			// Write all bytes (handling any partial non-blocking writes).
			written := 0
			remW := wbufs
			for written < total {
				n, err := Writev(a, remW)
				if err == unix.EAGAIN {
					runtime.Gosched()
					continue
				}
				if err != nil {
					t.Fatalf("count=%d iter=%d Writev failed: %v", count, iter, err)
				}
				written += n
				remW = advanceIov(remW, n)
			}

			// Read all bytes back into >16 freshly-allocated scatter buffers.
			got := make([]byte, 0, total)
			read := 0
			remR := rbufs
			for read < total {
				n, err := Readv(b, remR)
				if err == unix.EAGAIN {
					runtime.Gosched()
					continue
				}
				if err != nil {
					t.Fatalf("count=%d iter=%d Readv failed: %v", count, iter, err)
				}
				if n == 0 {
					t.Fatalf("count=%d iter=%d unexpected EOF after %d/%d bytes", count, iter, read, total)
				}
				read += n
				remR = advanceIov(remR, n)
			}
			for _, rb := range rbufs {
				got = append(got, rb...)
			}
			if !bytes.Equal(got, expected.Bytes()) {
				t.Fatalf("count=%d iter=%d data mismatch (len=%d)", count, iter, total)
			}
		}
	}
}

func advanceIov(iov [][]byte, n int) [][]byte {
	for len(iov) > 0 && n > 0 {
		if len(iov[0]) <= n {
			n -= len(iov[0])
			iov = iov[1:]
		} else {
			iov[0] = iov[0][n:]
			n = 0
		}
	}
	return iov
}

func BenchmarkWritevReadv(b *testing.B) {
	for _, nBufs := range []int{2, stackIovecs, 24} {
		name := "2bufs"
		switch nBufs {
		case stackIovecs:
			name = "16bufs"
		case 24:
			name = "24bufs"
		}
		b.Run(name, func(b *testing.B) {
			fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_NONBLOCK|unix.SOCK_CLOEXEC, 0)
			if err != nil {
				b.Fatal(err)
			}
			defer unix.Close(fds[0])
			defer unix.Close(fds[1])
			wbufs := make([][]byte, nBufs)
			rbufs := make([][]byte, nBufs)
			for i := range wbufs {
				wbufs[i] = []byte("abcd")
				rbufs[i] = make([]byte, 4)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := Writev(fds[0], wbufs); err != nil {
					b.Fatal(err)
				}
				if _, err := Readv(fds[1], rbufs); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
	b.Run("ReadvEAGAIN", func(b *testing.B) {
		fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_NONBLOCK|unix.SOCK_CLOEXEC, 0)
		if err != nil {
			b.Fatal(err)
		}
		defer unix.Close(fds[0])
		defer unix.Close(fds[1])
		rbufs := [][]byte{make([]byte, 8), make([]byte, 8)}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = Readv(fds[1], rbufs)
		}
	})
}
