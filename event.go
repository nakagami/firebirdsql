//go:build !plan9

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
	"database/sql"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
)

var (
	ErrAlreadySubscribe = errors.New("already subscribe")
	ErrFbEventClosed    = errors.New("fbevent already closed")
)

const sqlPostEvent = `execute block as begin post_event '%s'; end`

type Event struct {
	Name     string
	Count    int
	ID       int32
	RemoteID int32
}
type EventHandler func(e Event)
type FbEvent struct {
	mu          sync.RWMutex
	dsn         *firebirdDsn
	conn        *sql.DB
	workers     sync.WaitGroup
	ctx         context.Context
	cancel      context.CancelFunc
	pending     sync.WaitGroup
	closed      int32
	closer      sync.Once
	closeErr    error
	subscribers []*Subscription
	dial        func(context.Context, string, string) (net.Conn, error)
	// Package-private construction boundary also permits deterministic lifecycle tests.
	prepare func(*eventScope, *firebirdDsn, []string, EventHandler, chan Event) (*Subscription, error)
}

func NewFBEvent(dsns string) (*FbEvent, error) {
	dsn, err := parseDSN(dsns)
	if err != nil {
		return nil, err
	}
	conn, err := sql.Open("firebirdsql", dsns)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &FbEvent{dsn: dsn, conn: conn, ctx: ctx, cancel: cancel, dial: (&net.Dialer{}).DialContext, prepare: newSubscription}, nil
}
func (e *FbEvent) PostEvent(name string) error {
	_, err := e.conn.Exec(fmt.Sprintf(sqlPostEvent, name))
	return err
}
func (e *FbEvent) newSubscriber(events []string, cb EventHandler, ch chan Event) (*Subscription, error) {
	e.mu.Lock()
	if e.IsClosed() {
		e.mu.Unlock()
		return nil, ErrFbEventClosed
	}
	e.pending.Add(1)
	e.mu.Unlock()
	defer e.pending.Done()
	scope := newEventScope(e.ctx, e.dial)
	s, err := e.prepare(scope, e.dsn, events, cb, ch)
	if err != nil {
		scope.close()
		if e.ctx.Err() != nil {
			return nil, e.ctx.Err()
		}
		return nil, err
	}
	e.mu.Lock()
	if e.IsClosed() {
		e.mu.Unlock()
		scope.close()
		return nil, ErrFbEventClosed
	}
	e.subscribers = append(e.subscribers, s)
	e.workers.Add(1)
	s.owner = e
	s.onDone = e.shutdownSubscriber
	s.workerDone = e.workers.Done
	s.start()
	e.mu.Unlock()
	return s, nil
}
func (e *FbEvent) Subscribe(events []string, cb EventHandler) (*Subscription, error) {
	return e.newSubscriber(events, cb, nil)
}
func (e *FbEvent) SubscribeChan(events []string, ch chan Event) (*Subscription, error) {
	return e.newSubscriber(events, nil, ch)
}
func (e *FbEvent) Subscribers() []*Subscription {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return append([]*Subscription(nil), e.subscribers...)
}
func (e *FbEvent) Count() int { e.mu.RLock(); defer e.mu.RUnlock(); return len(e.subscribers) }
func (e *FbEvent) shutdownSubscriber(s *Subscription) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for i, v := range e.subscribers {
		if v == s {
			e.subscribers = append(e.subscribers[:i], e.subscribers[i+1:]...)
			return
		}
	}
}
func (e *FbEvent) IsClosed() bool { return atomic.LoadInt32(&e.closed) == 1 }

// Close cancels construction and joins driver-owned workers, including pending
// error deliveries. Cleanup runs once; repeated public calls return ErrFbEventClosed.
func (e *FbEvent) Close() error {
	first := false
	e.closer.Do(func() {
		first = true
		e.mu.Lock()
		atomic.StoreInt32(&e.closed, 1)
		subs := append([]*Subscription(nil), e.subscribers...)
		e.mu.Unlock()
		for _, s := range subs {
			s.cancelNotices()
			s.stop(nil)
		}
		e.cancel()
		e.pending.Wait()
		for _, s := range subs {
			if err := s.finishClose(); err != nil {
				e.recordCloseError(err)
			}
		}
		e.workers.Wait()
		if err := e.conn.Close(); err != nil {
			e.recordCloseError(err)
		}
	})
	if !first {
		return ErrFbEventClosed
	}
	return e.closeErr
}
func (e *FbEvent) recordCloseError(err error) {
	e.closeErr = errors.Join(e.closeErr, err)
}
