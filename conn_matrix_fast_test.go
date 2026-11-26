//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd

package gnetplus

import "testing"

func TestConnMatrixFastTableAndFallback(t *testing.T) {
	var cm connMatrix
	cm.init()

	// Exercise low FDs, growth past initial 64, and fallback above fastFDLimit.
	fds := []int{3, 7, 63, 64, 127, 500, fastFDLimit - 1, fastFDLimit, fastFDLimit + 42}
	conns := make([]*conn, len(fds))
	for i, fd := range fds {
		c := &conn{fd: fd}
		conns[i] = c
		if err := cm.addConn(c, 0); err != nil {
			t.Fatalf("addConn(%d): %v", fd, err)
		}
	}

	if got := cm.loadCount(); got != int32(len(fds)) {
		t.Fatalf("loadCount = %d, want %d", got, len(fds))
	}

	for i, fd := range fds {
		if got := cm.getConn(fd); got != conns[i] {
			t.Fatalf("getConn(%d) = %p, want %p", fd, got, conns[i])
		}
	}
	if cm.getConn(-1) != nil || cm.getConn(999999) != nil || cm.getConn(4) != nil {
		t.Fatal("missing or negative fd returned non-nil conn")
	}

	for _, c := range conns {
		cm.delConn(c)
		if cm.getConn(c.fd) != nil {
			t.Fatalf("getConn(%d) non-nil after delConn", c.fd)
		}
	}
	if got := cm.loadCount(); got != 0 {
		t.Fatalf("loadCount after delete = %d, want 0", got)
	}
}
