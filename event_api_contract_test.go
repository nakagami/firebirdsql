package firebirdsql

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func contractSubscription(t *testing.T) (*FbEvent, *Subscription, *scriptedSocket, *scriptedSocket) {
	t.Helper()
	e := lifecycleOwner(t)
	t.Cleanup(func() { e.Close() })
	main, aux := socket("main"), socket("aux")
	setupSockets(e, main, aux)
	s, err := e.SubscribeChan([]string{"probe"}, make(chan Event, 1))
	if err != nil {
		t.Fatal(err)
	}
	return e, s, main, aux
}
func receiveNotice(t *testing.T, ch <-chan error, want error) {
	t.Helper()
	select {
	case err := <-ch:
		if !errors.Is(err, want) {
			t.Fatalf("terminal error mismatch: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("notification lost")
	}
}
func TestLifecycleAPIRepeatedClose(t *testing.T) {
	e, s, main, aux := contractSubscription(t)
	notices := make(chan error, 1)
	s.NotifyClose(notices)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != ErrFbEventClosed {
		t.Fatalf("repeat subscription Close=%v", err)
	}
	if err := s.Unsubscribe(); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != ErrFbEventClosed {
		t.Fatalf("repeat owner Close=%v", err)
	}
	assertSocketsClosed(t, main, aux)
	select {
	case <-notices:
		t.Fatal("normal close notified an error")
	default:
	}
}
func TestLifecycleAPINotificationBusyConsumer(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		t.Run(map[bool]string{false: "unbuffered", true: "busy-buffer"}[buffered], func(t *testing.T) {
			_, s, main, aux := contractSubscription(t)
			ch := make(chan error)
			previous := errors.New("previous work")
			if buffered {
				ch = make(chan error, 1)
				ch <- previous
			}
			s.NotifyClose(ch)
			terminal := errors.New("synthetic terminal failure")
			s.stop(terminal)
			await(t, s.doneSubscription)
			assertSocketsClosed(t, main, aux) // reader hasn't consumed anything yet
			if buffered {
				receiveNotice(t, ch, previous)
			}
			receiveNotice(t, ch, terminal)
			if s.terminalError() != terminal {
				t.Fatal("terminal changed")
			}
			if err := s.Unsubscribe(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestLifecycleAPIClosePendingNotice(t *testing.T) {
	for _, ownerClose := range []bool{false, true} {
		e, s, main, aux := contractSubscription(t)
		for i := 0; i < 16; i++ {
			s.NotifyClose(make(chan error))
		}
		s.stop(errors.New("synthetic unread error"))
		await(t, s.doneSubscription)
		if e.Count() != 0 {
			t.Fatal("closed subscription retained in active list")
		}
		finished := make(chan struct{})
		go func() {
			defer close(finished)
			if ownerClose {
				if err := e.Close(); err != nil {
					t.Error(err)
				}
			} else if err := s.Close(); err != ErrFbEventClosed {
				t.Error(err)
			}
		}()
		await(t, finished)
		s.noticeWorkers.Wait()
		assertSocketsClosed(t, main, aux)
	}
}
func TestLifecycleAPITerminalPublication(t *testing.T) {
	// Register at the publication boundary, not after a delay. Every observer
	// must receive the same error regardless of whether it wins the close lock.
	for iteration := 0; iteration < 100; iteration++ {
		e, s, _, _ := contractSubscription(t)
		terminal := errors.New("same terminal error")
		gate := make(chan struct{})
		var wg sync.WaitGroup
		notices := make([]chan error, 16)
		for i := range notices {
			notices[i] = make(chan error)
			wg.Add(1)
			go func(c chan error) { defer wg.Done(); <-gate; s.NotifyClose(c) }(notices[i])
		}
		wg.Add(1)
		go func() { defer wg.Done(); <-gate; s.stop(terminal) }()
		close(gate)
		wg.Wait()
		if !s.IsClose() || s.terminalError() != terminal {
			t.Fatal("incoherent terminal state")
		}
		for _, c := range notices {
			receiveNotice(t, c, terminal)
		}
		if err := e.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
func TestLifecycleAPILateObserverVsOwnerClose(t *testing.T) {
	for i := 0; i < 100; i++ {
		e, s, _, _ := contractSubscription(t)
		s.stop(errors.New("terminal"))
		await(t, s.doneSubscription)
		start := make(chan struct{})
		var wg sync.WaitGroup
		for n := 0; n < 8; n++ {
			wg.Add(1)
			go func() { defer wg.Done(); <-start; s.NotifyClose(make(chan error)) }()
		}
		wg.Add(1)
		go func() { defer wg.Done(); <-start; e.Close() }()
		close(start)
		wg.Wait()
		s.noticeWorkers.Wait()
	}
}
func TestLifecycleAPINoticeConsumerCanClose(t *testing.T) {
	e, s, _, _ := contractSubscription(t)
	ch := make(chan error)
	s.NotifyClose(ch)
	done := make(chan struct{})
	go func() { defer close(done); <-ch; e.Close() }()
	s.stop(errors.New("terminal"))
	await(t, done)
}
func TestLifecycleAPIConcurrentCloseResult(t *testing.T) {
	e, s, _, _ := contractSubscription(t)
	for _, closeFn := range []func() error{s.Close, e.Close} {
		gate := make(chan struct{})
		results := make(chan error, 16)
		for i := 0; i < 16; i++ {
			go func() { <-gate; results <- closeFn() }()
		}
		close(gate)
		first := 0
		for i := 0; i < 16; i++ {
			err := <-results
			if err == nil {
				first++
			} else if err != ErrFbEventClosed {
				t.Fatal(err)
			}
		}
		if first != 1 {
			t.Fatalf("successful first calls=%d", first)
		}
	}
}
