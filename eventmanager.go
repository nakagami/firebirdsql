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
	"fmt"
	"sync"
)

type eventManager struct {
	wp         *wireProtocol
	handle     int32
	destructor sync.Once
	scope      *eventScope
	done       chan struct{}
}

func newEventManager(scope *eventScope, address string, auxHandle int32) (*eventManager, error) {
	wp, err := scope.wire(address, "", "UTF8")
	if err != nil {
		return nil, err
	}
	newManager := &eventManager{
		wp:    wp,
		scope: scope, done: make(chan struct{}),
		handle: auxHandle,
	}
	return newManager, nil
}

func (e *eventManager) wait(event *remoteEvent, eventCounts chan<- Event) <-chan error {
	chErr := make(chan error, 1)
	go func() {
		defer close(e.done)
		for {
			data, err := e.wp.recvPackets(4)
			if err != nil {
				e.wp.debugPrint("recvPackets:%v:%v", err, data)
				chErr <- err
				return
			}
			op := bytes_to_bint32(data)
			switch op {
			case op_event:
				if _, err = e.wp.recvPackets(4); err != nil { //handle
					chErr <- err
					return
				}
				b, err := e.wp.recvPackets(4)
				if err != nil {
					chErr <- err
					return
				}
				szBuf := bytes_to_bint32(b)
				// The event buffer mirrors the client's own EPB, capped at maxEpbLength;
				// a size outside that range is malformed and must not reach make().
				if szBuf < 0 || int(szBuf) > maxEpbLength {
					chErr <- fmt.Errorf("firebirdsql: event buffer size %d out of range", szBuf)
					return
				}
				buffer, err := e.wp.recvPacketsAlignment(int(szBuf))
				if err != nil {
					chErr <- err
					return
				}
				if _, err = e.wp.recvPackets(8); err != nil { //ast
					chErr <- err
					return
				}
				b, err = e.wp.recvPackets(4)
				if err != nil {
					chErr <- err
					return
				}
				eventId := bytes_to_bint32(b)
				e.wp.debugPrint("op_event:%v: event id: %v", buffer, eventId)
				counts, err := event.getEventCounts(buffer)
				if err != nil {
					e.wp.debugPrint("getEventCounts:%v", err)
					chErr <- err
					return
				}
				for _, count := range counts {
					select {
					case eventCounts <- count:
					case <-e.scope.ctx.Done():
						return
					}
				}
			default:
				e.wp.debugPrint("unknown operation:%v:%v", op, data)
			}
		}
	}()
	return chErr
}

func (e *eventManager) close() (err error) {
	e.destructor.Do(func() {
		err = e.wp.conn.Close()
	})
	return
}
