//go:build !plan9
// +build !plan9

/*******************************************************************************
The MIT License (MIT)

Copyright (c) 2019 Arteev Aleksey

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
the Software, and to permit persons to whom the Software is furnished to do so,
subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*******************************************************************************/

package firebirdsql

import (
	"context"
	"errors"
	"net"
	"sync"
)

// eventScope owns sockets before handshake or subscriber registration can block.
// It is used exclusively by Events; ordinary SQL connections retain their path.
type eventScope struct {
	ctx      context.Context
	cancel   context.CancelFunc
	mu       sync.Mutex
	conns    []net.Conn
	closeErr error
	done     chan struct{}
	dial     func(context.Context, string, string) (net.Conn, error)
}

func newEventScope(parent context.Context, dial func(context.Context, string, string) (net.Conn, error)) *eventScope {
	ctx, cancel := context.WithCancel(parent)
	s := &eventScope{ctx: ctx, cancel: cancel, done: make(chan struct{}), dial: dial}
	go func() {
		defer close(s.done)
		<-ctx.Done()
		s.mu.Lock()
		defer s.mu.Unlock()
		for _, c := range s.conns {
			if err := c.Close(); err != nil {
				s.closeErr = errors.Join(s.closeErr, err)
			}
		}
		s.conns = nil
	}()
	return s
}
func (s *eventScope) close() error { s.cancel(); <-s.done; return s.closeErr }
func (s *eventScope) wire(addr, timezone, charset string) (*wireProtocol, error) {
	c, err := s.dial(s.ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	if err = s.ctx.Err(); err != nil {
		s.mu.Unlock()
		c.Close()
		return nil, err
	}
	s.conns = append(s.conns, c)
	s.mu.Unlock()
	p := &wireProtocol{buf: make([]byte, 0, BUFFER_LEN), addr: addr, timezone: timezone, charset: charset}
	p.conn, err = newWireChannel(c)
	if err != nil {
		return nil, err
	}
	p.charsetLen()
	return p, nil
}
