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
)

const (
	afInet         = 2
	afInet6Linux   = 10
	afInet6Windows = 23
	afInet6Darwin  = 30
)

type Subscription struct {
	workerDone       func()
	mu               sync.RWMutex
	revent           *remoteEvent
	auxHandle        int32
	callback         EventHandler
	chEvent          chan Event
	eventCounts      chan Event
	closed           int32
	muClose          sync.Mutex
	closes           []chan error
	owner            *FbEvent
	noticeWorkers    sync.WaitGroup
	noticeCancel     chan struct{}
	noticeCanceled   bool
	terminalErr      error
	doneSubscription chan struct{}
	manager          *eventManager
	fc               *firebirdsqlConn
	scope            *eventScope
	onDone           func(*Subscription)
}

func newSubscription(scope *eventScope, dsn *firebirdDsn, events []string, cb EventHandler, ch chan Event) (*Subscription, error) {
	revent := newRemoteEvent()
	if err := revent.queueEvents(events...); err != nil {
		return nil, err
	}
	fc, err := openFirebirdsqlConnWithWire(dsn, func(wp *wireProtocol) error {
		return wp.opAttach(dsn.dbName, dsn.user, dsn.passwd, dsn.options["role"])
	}, scope.wire)
	if err != nil {
		return nil, err
	}
	s := &Subscription{fc: fc, scope: scope, revent: revent, callback: cb, chEvent: ch, eventCounts: make(chan Event), doneSubscription: make(chan struct{}), noticeCancel: make(chan struct{})}
	s.manager, err = s.getEventManager()
	if err != nil {
		return nil, err
	}
	if err = s.queueEvents(0); err != nil {
		return nil, fmt.Errorf("initial event registration: %w", err)
	}
	return s, nil
}
func (s *Subscription) start() {
	go func() { defer s.workerDone(); s.wait(s.manager.wait(s.revent, s.eventCounts)) }()
}
func (s *Subscription) wait(chErr <-chan error) {
	defer func() {
		s.stop(nil)
		s.scope.close()
		<-s.manager.done
		if s.onDone != nil {
			s.onDone(s)
		}
		close(s.doneSubscription)
	}()
	for {
		select {
		case <-s.scope.ctx.Done():
			return
		case err := <-chErr:
			s.stop(err)
			return
		case event := <-s.eventCounts:
			if s.callback != nil {
				go s.callback(event)
			} else {
				select {
				case s.chEvent <- event:
				case <-s.scope.ctx.Done():
					return
				}
			}
			if err := s.queueEvents(event.ID); err != nil {
				s.stop(fmt.Errorf("event rearm: %w", err))
				return
			}
		}
	}
}

// stop publishes the terminal state under the same lock used by NotifyClose.
// Transport cleanup never waits for a consumer to receive a notification.
func (s *Subscription) stop(err error) bool {
	s.muClose.Lock()
	defer s.muClose.Unlock()
	if s.IsClose() {
		return false
	}
	s.terminalErr = err
	atomic.StoreInt32(&s.closed, 1)
	if err != nil {
		for _, c := range s.closes {
			s.deliverNoticeLocked(c, err)
		}
	}
	s.closes = nil
	s.scope.cancel()
	return true
}

func (s *Subscription) terminalError() error {
	s.muClose.Lock()
	defer s.muClose.Unlock()
	return s.terminalErr
}

// deliverNoticeLocked registers every delivery before shutdown can start waiting.
// The owner also joins deliveries after the subscription leaves its active list.
func (s *Subscription) deliverNoticeLocked(receiver chan error, err error) {
	if s.noticeCanceled {
		return
	}
	s.owner.mu.Lock()
	if s.owner.IsClosed() {
		s.owner.mu.Unlock()
		return
	}
	s.owner.workers.Add(1)
	s.noticeWorkers.Add(1)
	s.owner.mu.Unlock()
	go func() {
		defer s.owner.workers.Done()
		defer s.noticeWorkers.Done()
		select {
		case receiver <- err:
		case <-s.noticeCancel:
		case <-s.owner.ctx.Done():
		}
	}()
}

// NotifyClose delivers an asynchronous terminal error, including to an unbuffered
// or temporarily busy receiver. Explicit Subscription/FbEvent Close may cancel
// pending delivery; normal shutdown sends no error. Receivers must not close
// registered channels. Late observers see the same terminal error until Close.
func (s *Subscription) NotifyClose(receiver chan error) {
	s.muClose.Lock()
	defer s.muClose.Unlock()
	if s.IsClose() {
		if s.terminalErr != nil {
			s.deliverNoticeLocked(receiver, s.terminalErr)
		}
		return
	}
	if !s.noticeCanceled {
		s.closes = append(s.closes, receiver)
	}
}
func (s *Subscription) IsClose() bool { return s == nil || atomic.LoadInt32(&s.closed) == 1 }

func (s *Subscription) cancelNotices() {
	s.muClose.Lock()
	if !s.noticeCanceled {
		s.noticeCanceled = true
		close(s.noticeCancel)
	}
	s.muClose.Unlock()
}
func (s *Subscription) finishClose() error {
	s.cancelNotices()
	<-s.doneSubscription
	s.noticeWorkers.Wait()
	return s.scope.close()
}

// Unsubscribe preserves the legacy no-op result for an already closed subscription.
func (s *Subscription) Unsubscribe() error {
	err := s.Close()
	if err == ErrFbEventClosed {
		return nil
	}
	return err
}
func (s *Subscription) Close() error {
	s.cancelNotices()
	first := s.stop(nil)
	err := s.finishClose()
	if !first {
		return ErrFbEventClosed
	}
	return err
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
	newManager, err := newEventManager(s.scope, address, auxHandle)
	if err != nil {
		return nil, err
	}
	s.auxHandle = auxHandle
	return newManager, nil
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
	// buf is the server's address response and may be empty; each branch
	// bounds-checks the bytes it reads before slicing.
	if len(buf) < 4 {
		return -1, "", fmt.Errorf("firebirdsql: aux connection response too short (%d bytes)", len(buf))
	}
	family := bytes_to_int16(buf[0:2])
	port := binary.BigEndian.Uint16(buf[2:4])

	var addr netip.Addr
	switch family {
	case afInet:
		if len(buf) < 8 {
			return -1, "", fmt.Errorf("firebirdsql: aux connection IPv4 address truncated (%d bytes)", len(buf))
		}
		addr = netip.AddrFrom4([4]byte(buf[4:8]))
	case afInet6Linux, afInet6Windows, afInet6Darwin:
		if len(buf) < 24 {
			return -1, "", fmt.Errorf("firebirdsql: aux connection IPv6 address truncated (%d bytes)", len(buf))
		}
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
