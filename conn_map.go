// Copyright (c) 2023 The Gnet Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build (darwin || dragonfly || freebsd || linux || netbsd || openbsd) && !gc_opt

package gnetplus

import (
	"sync/atomic"

	"github.com/nelthaarion/gnetplus/v2/internal/gfd"
)

// fastFDLimit caps the direct-indexed FD slice used to bypass map hashing on
// every epoll/kqueue event. Linux assigns dense small FDs starting at 3, so
// nearly every connection resolves with a single bounds-checked slice load;
// FDs at or above the cap fall back to connMap transparently.
const fastFDLimit = 65536

type connMatrix struct {
	connCount int32
	fast      []*conn
	connMap   map[int]*conn
	// seq is this matrix's own gfd duplicate-detection counter; see
	// conn_matrix.go for why it is per-matrix and not package-global.
	seq uint32
}

func (cm *connMatrix) init() {
	cm.connMap = make(map[int]*conn)
	cm.fast = make([]*conn, 64)
}

func (cm *connMatrix) iterate(f func(*conn) bool) {
	for _, c := range cm.connMap {
		if c != nil {
			if !f(c) {
				return
			}
		}
	}
}

func (cm *connMatrix) incCount(_ int, delta int32) {
	atomic.AddInt32(&cm.connCount, delta)
}

func (cm *connMatrix) loadCount() (n int32) {
	return atomic.LoadInt32(&cm.connCount)
}

// addConn registers c in the map and direct FD table. Unlike the matrix-backed
// implementation this one grows on demand, so it never runs out of room and
// never fails.
func (cm *connMatrix) addConn(c *conn, index int) error {
	c.gfd = gfd.NewGFDWithSeq(c.fd, index, 0, 0, &cm.seq)
	cm.connMap[c.fd] = c
	if fd := c.fd; fd >= 0 && fd < fastFDLimit {
		if fd >= len(cm.fast) {
			newCap := len(cm.fast)
			if newCap == 0 {
				newCap = 64
			}
			for newCap <= fd {
				newCap <<= 1
			}
			if newCap > fastFDLimit {
				newCap = fastFDLimit
			}
			grown := make([]*conn, newCap)
			copy(grown, cm.fast)
			cm.fast = grown
		}
		cm.fast[fd] = c
	}
	cm.incCount(0, 1)
	return nil
}

func (cm *connMatrix) delConn(c *conn) {
	if fd := c.fd; uint(fd) < uint(len(cm.fast)) {
		cm.fast[fd] = nil
	}
	delete(cm.connMap, c.fd)
	cm.incCount(0, -1)
}

func (cm *connMatrix) getConn(fd int) *conn {
	if uint(fd) < uint(len(cm.fast)) {
		return cm.fast[fd]
	}
	return cm.connMap[fd]
}

/*
func (cm *connMatrix) getConnByGFD(fd gfd.GFD) *conn {
	return cm.connMap[fd.Fd()]
}
*/
