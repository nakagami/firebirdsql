//go:build !plan9

package firebirdsql

import (
	"encoding/binary"
	"fmt"
	"net"
	"net/netip"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
)

type Subscription struct {
	mu               sync.RWMutex
	revent           *remoteEvent
	auxHandle        int32
	callback         EventHandler
	chEvent          chan Event
	eventCounts      chan Event
	closed           int32
	muClose          sync.Mutex
	closes           []chan error
	closer           sync.Once
	chDoneEvent      chan *Subscription
	doneSubscription chan struct{}
	manager          *eventManager
	fc               *firebirdsqlConn
	noNotify         int32
}

func newSubscription(dsn *firebirdDsn, events []string, cb EventHandler, chEvent chan Event, chDoneEvent chan *Subscription) (*Subscription, error) {
	fc, err := attachFirebirdsqlConn(dsn)
	if err != nil {
		return nil, err
	}
	newSubscription := &Subscription{
		fc:               fc,
		callback:         cb,
		chEvent:          chEvent,
		eventCounts:      make(chan Event),
		doneSubscription: make(chan struct{}),
		chDoneEvent:      chDoneEvent,
	}
	manager, err := newSubscription.getEventManager()
	if err != nil {
		return nil, err
	}
	newSubscription.manager = manager

	remoteEvent := newRemoteEvent()
	if err := remoteEvent.queueEvents(events...); err != nil {
		return nil, err
	}

	newSubscription.revent = remoteEvent

	newSubscription.queueEvents(0)
	chErrManager := manager.wait(remoteEvent, newSubscription.eventCounts)
	go newSubscription.wait(chErrManager)

	return newSubscription, nil
}
func (s *Subscription) cancelEvents() error {
	if atomic.LoadInt32(&s.closed) == 1 {
		return nil
	}
	id := atomic.LoadInt32(&s.revent.id)
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.fc.wp.opCancelEvents(id); err != nil {
		return err
	}
	_, _, _, err := s.fc.wp.opResponse()
	if err != nil {
		return err
	}
	s.revent.cancelEvents()
	return nil
}

func (s *Subscription) queueEvents(eventID int32) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := eventID + 1
	epbData := s.revent.buildEpb()

	if err := s.fc.wp.opQueEvents(s.auxHandle, epbData, id); err != nil {
		return err
	}
	rid, _, _, err := s.fc.wp.opResponse()
	if err != nil {
		return err
	}

	atomic.StoreInt32(&s.revent.id, id)
	atomic.StoreInt32(&s.revent.rid, rid)
	return nil
}

func (s *Subscription) getEventManager() (*eventManager, error) {
	auxHandle, address, err := s.connAuxRequest()
	if err != nil {
		return nil, err
	}
	newManager, err := newEventManager(address, auxHandle)
	if err != nil {
		return nil, err
	}
	s.auxHandle = auxHandle
	return newManager, nil
}

func (s *Subscription) wait(chErrManager <-chan error) {
	for {
		select {
		case event := <-s.eventCounts:
			s.doEventCounts(event)
			s.queueEvents(event.ID)
		case <-s.doneSubscription:
			return
		case err := <-chErrManager:
			s.closeWithError(err)
		}
	}
}

func (s *Subscription) doEventCounts(e Event) {
	if s.callback != nil {
		go s.callback(e)
		return
	}
	s.chEvent <- e
}

func (s *Subscription) Unsubscribe() error {
	if s.IsClose() {
		return nil
	}
	if s.manager != nil {
		if err := s.manager.close(); err != nil {
			return err
		}
		s.manager = nil
	}
	if err := s.cancelEvents(); err != nil {
		return err
	}
	return s.Close()
}

func (s *Subscription) unsubscribeNoNotify() error {
	atomic.StoreInt32(&s.noNotify, 1)
	return s.Unsubscribe()
}

func (s *Subscription) connAuxRequest() (int32, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.fc.wp.opConnectRequest(); err != nil {
		return -1, "", err
	}
	auxHandle, _, buf, err := s.fc.wp.opResponse()
	if err != nil {
		return -1, "", err
	}
	family := bytes_to_int16(buf[0:2])
	port := binary.BigEndian.Uint16(buf[2:4])

	var addr netip.Addr
	switch family {
	case syscall.AF_INET:
		addr = netip.AddrFrom4([4]byte(buf[4:8]))
	case syscall.AF_INET6:
		if reflect.DeepEqual(buf[4:20], []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff}) {
			addr = netip.AddrFrom4([4]byte(buf[20:24]))
		} else {
			addr = netip.AddrFrom16([16]byte(buf[4:20]))
		}
	default:
		return -1, "", fmt.Errorf("unsupported  family protocol: %x", family)
	}

	var host string
	if addr.IsUnspecified() {
		// Server is bound to all interfaces (RemoteBindAddress empty), so it
		// reports 0.0.0.0 / :: which the client cannot route to. Fall back to
		// the host the primary connection used — it is reachable by definition.
		// See: https://github.com/nakagami/firebirdsql/issues/156
		host, _, err = net.SplitHostPort(s.fc.dsn.addr)
		if err != nil {
			return -1, "", err
		}
	} else {
		host = addr.String()
	}
	return auxHandle, net.JoinHostPort(host, strconv.Itoa(int(port))), nil
}

func (s *Subscription) NotifyClose(receiver chan error) {
	s.muClose.Lock()
	defer s.muClose.Unlock()
	s.closes = append(s.closes, receiver)
}

func (s *Subscription) IsClose() bool {
	if s == nil {
		return true
	}
	return atomic.LoadInt32(&s.closed) == 1
}

func (s *Subscription) Close() error {
	if s.IsClose() {
		return ErrFbEventClosed
	}
	return s.doClose(nil)
}

func (s *Subscription) closeWithError(err error) error {
	if s.IsClose() {
		return ErrFbEventClosed
	}
	return s.doClose(err)
}

func (s *Subscription) doClose(err error) (errResult error) {
	atomic.StoreInt32(&s.closed, 1)
	s.closer.Do(func() {

		close(s.doneSubscription)

		s.muClose.Lock()
		defer s.muClose.Unlock()
		if err != nil {
			for _, c := range s.closes {
				c <- err
			}
		}

		s.mu.RLock()
		errResult = s.fc.Close()
		s.mu.RUnlock()

		if atomic.LoadInt32(&s.noNotify) == 0 {
			s.chDoneEvent <- s
		}
	})
	return
}
