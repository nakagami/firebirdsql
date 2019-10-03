package firebirdsql

import (
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

var (
	ErrAlreadySubscribe = errors.New("already subscribe")
	ErrClosed           = errors.New("fbevent already closed")
)

const (
	sqlPostEvent = `execute block as begin post_event '%s'; end`
)

type FbEvent struct {
	mu               sync.RWMutex
	dsn              string
	conn             *sql.DB
	done             chan struct{}
	closed           int32
	closer           sync.Once
	chDoneSubscriber chan *Subscription
	subscribers      []*Subscription
}

type Event struct {
	Name     string
	Count    int
	ID       int32
	RemoteID int32
}

type EventHandler func(e Event)

func NewFBEvent(dsn string) (*FbEvent, error) {
	conn, err := sql.Open("firebirdsql", dsn)
	if err != nil {
		return nil, err
	}
	fbEvent := &FbEvent{
		dsn:              dsn,
		conn:             conn,
		done:             make(chan struct{}),
		chDoneSubscriber: make(chan *Subscription),
	}
	go fbEvent.run()
	return fbEvent, nil
}

func (e *FbEvent) PostEvent(name string) error {
	_, err := e.conn.Exec(fmt.Sprintf(sqlPostEvent, name))
	if err != nil {
		return err
	}
	return nil
}

func (e *FbEvent) newSubscriber(events []string, cb EventHandler, chEvent chan Event) (*Subscription, error) {
	subscriber, err := newSubscription(e.dsn, events, cb, chEvent, e.chDoneSubscriber)
	if err != nil {
		return nil, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.subscribers = append(e.subscribers, subscriber)
	return subscriber, nil
}

func (e *FbEvent) Subscribers() []*Subscription {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.subscribers[:]
}

func (e *FbEvent) CountSubscriber() int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return len(e.subscribers)
}

func (e *FbEvent) Subscribe(events []string, cb EventHandler) (*Subscription, error) {
	return e.newSubscriber(events, cb, nil)
}

func (e *FbEvent) SubscribeChan(events []string, chEvent chan Event) (*Subscription, error) {
	return e.newSubscriber(events, nil, chEvent)
}

func (e *FbEvent) run() {
	for {
		select {
		case <-e.done:
			return
		case subscriber := <-e.chDoneSubscriber:
			e.shutdownSubscriber(subscriber)
		}
	}
}

func (e *FbEvent) shutdownSubscriber(subscriber *Subscription) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for i := range e.subscribers {
		if e.subscribers[i] == subscriber {
			last := len(e.subscribers) - 1
			e.subscribers[i] = e.subscribers[last]
			e.subscribers[last] = nil
			e.subscribers = e.subscribers[:last]
			return
		}
	}
}

func (e *FbEvent) IsClose() bool {
	return atomic.LoadInt32(&e.closed) == 1
}

func (e *FbEvent) Close() error {
	if e.IsClose() {
		return ErrClosed
	}
	return e.doClose(nil)
}

func (e *FbEvent) closeWithError(err error) error {
	if e.IsClose() {
		return ErrClosed
	}
	return e.doClose(err)
}

func (e *FbEvent) doClose(err error) (errResult error) {
	atomic.StoreInt32(&e.closed, 1)
	e.closer.Do(func() {
		e.conn.Close()
		e.mu.Lock()
		wg := &sync.WaitGroup{}
		wg.Add(len(e.subscribers))
		for i := range e.subscribers {
			go func(subscriber *Subscription) {
				defer wg.Done()
				subscriber.unsubscribeNoNotify()
			}(e.subscribers[i])
		}
		e.subscribers = make([]*Subscription, 0)
		e.mu.Unlock()
		wg.Wait()
		close(e.done)
	})
	return
}
