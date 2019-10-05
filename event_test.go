package firebirdsql

import (
	"database/sql"
	"math/rand"
	"net"
	"sync"
	"testing"
	"time"
)

func TestEventsCallback(t *testing.T) {
	tempPathDB := TempFileName("test_events_")
	conn, err := sql.Open("firebirdsql_createdb", "sysdba:masterkey@localhost:3050"+tempPathDB)
	if err != nil {
		t.Fatalf("Error connecting: %v", err)
	}
	conn.Ping()
	conn.Close()

	fbevent, err := NewFBEvent("sysdba:masterkey@localhost:3050" + tempPathDB)
	if err != nil {
		t.Fatalf("Error connecting: %v", err)
	}
	defer fbevent.Close()

	doEvent := func(wg *sync.WaitGroup, wantEvents map[string]int) {
		eventsSlice := make([]string, 0, len(wantEvents))
		events := make(map[string]int, len(wantEvents))
		for event, count := range wantEvents {
			if count > 0 {
				eventsSlice = append(eventsSlice, event)
				events[event] = count
			}
		}
		waitDone := len(events)
		for len(events) > 0 {
			idx := rand.Intn(len(events))
			name := eventsSlice[idx]

			if err := fbevent.PostEvent(name); err != nil {
				for i := 0; i < waitDone; i++ {
					wg.Done()
				}
				t.Error(err)
				return
			}
			time.Sleep(time.Millisecond * 5)
			events[name]--
			if events[name] <= 0 {
				delete(events, name)
				eventsSlice = append(eventsSlice[:idx], eventsSlice[idx+1:]...)
				wg.Done()
				waitDone--
			}
		}

	}

	t.Run("callback", func(t *testing.T) {
		wg := &sync.WaitGroup{}

		wantEvents := map[string]int{
			"event_1": 12,
			"event_2": 15,
			"event_3": 23,
			"event_4": 0,
		}
		events := make([]string, 0, len(wantEvents))
		for event := range wantEvents {
			events = append(events, event)
		}
		wg.Add(3)

		muEvents := &sync.Mutex{}
		gotEvents := map[string]int{}

		subscribe, err := fbevent.Subscribe(events, func(e Event) {
			muEvents.Lock()
			gotEvents[e.Name] += e.Count
			muEvents.Unlock()
		})
		if err != nil {
			t.Error(err)
		}
		defer subscribe.Unsubscribe()

		go doEvent(wg, wantEvents)
		wg.Wait()
		time.Sleep(time.Second * 1)

		muEvents.Lock()
		for wantEvent, wantCount := range wantEvents {
			if wantCount <= 0 {
				continue
			}
			gotCount, ok := gotEvents[wantEvent]
			if !ok {
				t.Errorf("Expected %s count %d", wantEvent, wantCount)
			} else if gotCount != wantCount {
				t.Errorf("Expected %s count %d, got %d", wantEvent, wantCount, gotCount)
			}
		}
		muEvents.Unlock()
	})

	t.Run("channel", func(t *testing.T) {
		wg := &sync.WaitGroup{}

		wantEvents := map[string]int{
			"event_ch_1": 31,
			"event_ch_2": 21,
			"event_ch_3": 15,
			"event_ch_4": 0,
		}
		events := make([]string, 0, len(wantEvents))
		for event := range wantEvents {
			events = append(events, event)
		}
		wg.Add(3)

		muEvents := &sync.Mutex{}
		gotEvents := map[string]int{}
		chEvent := make(chan Event)
		subscribe, err := fbevent.SubscribeChan(events, chEvent)
		if err != nil {
			t.Error(err)

		}
		chClose := make(chan error)
		subscribe.NotifyClose(chClose)
		go func() {
			for {
				select {
				case e := <-chEvent:
					muEvents.Lock()
					gotEvents[e.Name] += e.Count
					muEvents.Unlock()

				case err := <-chClose:
					if err != nil {
						if _, ok := err.(*net.OpError); !ok {
							t.Error(err)
						}
					}
					return
				}
			}
		}()
		defer subscribe.Unsubscribe()

		go doEvent(wg, wantEvents)
		wg.Wait()
		time.Sleep(time.Second * 1)

		muEvents.Lock()
		for wantEvent, wantCount := range wantEvents {
			if wantCount <= 0 {
				continue
			}
			gotCount, ok := gotEvents[wantEvent]
			if !ok {
				t.Errorf("Expected %s count %d", wantEvent, wantCount)
			} else if gotCount != wantCount {
				t.Errorf("Expected %s count %d, got %d", wantEvent, wantCount, gotCount)
			}

		}
		muEvents.Unlock()
	})
}
