package firebirdsql

import (
	"sync"
)

type eventManager struct {
	wp         *wireProtocol
	handle     int32
	destructor sync.Once
}

func newEventManager(address string, auxHandle int32) (*eventManager, error) {
	wp, err := newWireProtocol(address, "")
	if err != nil {
		return nil, err
	}
	newManager := &eventManager{
		wp:     wp,
		handle: auxHandle,
	}
	return newManager, nil
}

func (e *eventManager) wait(event *remoteEvent, eventCounts chan<- Event) <-chan error {
	chErr := make(chan error, 1)
	go func() {
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
				e.wp.recvPackets(4) //handle
				b, _ := e.wp.recvPackets(4)
				szBuf := bytes_to_bint32(b)
				buffer, _ := e.wp.recvPacketsAlignment(int(szBuf))
				e.wp.recvPackets(8) //ast
				b, _ = e.wp.recvPackets(4)
				eventId := bytes_to_bint32(b)
				e.wp.debugPrint("op_event:%v: event id: %v", buffer, eventId)
				for _, count := range event.getEventCounts(buffer) {
					eventCounts <- count
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
