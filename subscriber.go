package firebirdsql

import (
	"fmt"
	"log"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
)

type Subscription struct {
	mu sync.RWMutex

	revent      *remoteEvent
	auxHandle   int32
	callback    EventHandler
	chEvent     chan Event
	eventCounts chan Event

	closed      int32
	muClose     sync.Mutex
	closes      []chan error
	closer      sync.Once
	chDoneEvent chan *Subscription

	doneSubscription chan struct{}

	//fbeventer *FbEvent

	manager *eventManager

	fc *firebirdsqlConn

	noNotify int32
}

func newSubscription(dsn string, events []string, cb EventHandler, chEvent chan Event, chDoneEvent chan *Subscription) (*Subscription, error) {
	fc, err := newFirebirdsqlConn(dsn)
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
	if err:=remoteEvent.QueueEvents(events...);err!=nil{
		return nil,err
	}

	newSubscription.revent = remoteEvent

	newSubscription.queueEvents(0)
	chErrManager := manager.Wait(remoteEvent, newSubscription.eventCounts)
	go newSubscription.wait(chErrManager)

	return newSubscription, nil
}
func (s *Subscription) CancelEvents() {
	if atomic.LoadInt32(&s.closed) == 0 {
		return
	}
	s.mu.Lock()
	id := atomic.LoadInt32(&s.revent.id)
	s.fc.wp.opCancelEvents(id)
	_, _, _, err := s.fc.wp.opResponse()
	s.mu.Unlock()
	if err != nil {
		log.Println(err)
	}
	s.revent.CancelEvents()
}

func (e *Subscription) queueEvents(eventID int32) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	id := eventID + 1
	epbData := e.revent.buildEpb()

	e.fc.wp.opQueEvents(e.auxHandle, epbData, id)
	rid, _, _, err := e.fc.wp.opResponse()
	if err != nil {
		return err
	}

	atomic.StoreInt32(&e.revent.id,id)
	atomic.StoreInt32(&e.revent.rid,rid)
	return nil
}

func (e *Subscription) getEventManager() (*eventManager, error) {
	auxHandle, addr, port, err := e.connAuxRequest()
	if err != nil {
		return nil, err
	}
	address := addr.String() + ":" + strconv.Itoa(port)
	newManager, err := newEventManager(address, auxHandle)
	if err != nil {
		return nil, err
	}
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

func (e *Subscription) Unsubscribe() error {
	if e.IsClose() {
		return nil
	}

	e.CancelEvents()
	if e.manager != nil {
		if err := e.manager.Close(); err != nil {
			return err
		}
		e.manager = nil
	}

	return e.Close()
}

func (s *Subscription) unsubscribeNoNotify() error {
	atomic.StoreInt32(&s.noNotify,1)
	return s.Unsubscribe()
}

// returns network, address, error
func (e *Subscription) connAuxRequest() (int32, *net.IP, int, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.fc.wp.opConnectRequest()
	auxHandle, _, buf, err := e.fc.wp.opResponse()
	if err != nil {
		return -1, nil, 0, err
	}
	family := bytes_to_int16(buf[0:2])
	port := bytes_to_bint16(buf[2:4])
	ip := net.IPv4(buf[4], buf[5], buf[6], buf[7])

	if syscall.AF_INET != family {
		return -1, nil, 0, fmt.Errorf("unsupported  family protocol: %x", family)
	}

	return auxHandle, &ip, int(port), nil
}

func (e *Subscription) NotifyClose(receiver chan error) {
	e.muClose.Lock()
	defer e.muClose.Unlock()
	e.closes = append(e.closes, receiver)
}

func (e *Subscription) IsClose() bool {
	return atomic.LoadInt32(&e.closed) == 1
}

func (e *Subscription) Close() error {
	if e.IsClose() {
		return ErrClosed
	}
	return e.doClose(nil)
}

func (e *Subscription) closeWithError(err error) error {
	if e.IsClose() {
		return ErrClosed
	}
	return e.doClose(err)
}

func (e *Subscription) doClose(err error) (errResult error) {
	atomic.StoreInt32(&e.closed, 1)
	e.closer.Do(func() {

		close(e.doneSubscription)

		e.muClose.Lock()
		defer e.muClose.Unlock()
		if err != nil {
			for _, c := range e.closes {
				c <- err
			}
		}

		e.mu.RLock()
		errResult = e.fc.Close()
		e.mu.RUnlock()

		if atomic.LoadInt32(&e.noNotify)==0 {
			e.chDoneEvent <- e
		}
	})
	return
}
