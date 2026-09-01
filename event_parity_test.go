/*******************************************************************************
The MIT License (MIT)

Copyright (c) 2026 Alexey Kovyazin

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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Phase 6 of the Jaybird test port plan (JAYBIRD_TEST_PORT_PLAN.md):
// event delivery parity — mirroring Jaybird's FBEventManagerTest coverage
// (multiple listeners, large loads, unsubscribe lifecycle).

func newTestEvent(t *testing.T, prefix string) (*FbEvent, string) {
	t.Helper()
	dbPath, dsn, err := CreateTestDatabase(prefix)
	require.NoError(t, err)
	t.Cleanup(func() { _ = removeDatabaseFile(dbPath) })
	fbe, err := NewFBEvent(dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = fbe.Close() })
	return fbe, dsn
}

func receiveWithin(ch chan Event, d time.Duration) []Event {
	var got []Event
	deadline := time.After(d)
	for {
		select {
		case e := <-ch:
			got = append(got, e)
			// keep draining briefly to collect coalesced deliveries
			select {
			case e2 := <-ch:
				got = append(got, e2)
				continue
			case <-time.After(300 * time.Millisecond):
				return got
			}
		case <-deadline:
			return got
		}
	}
}

// TestEventChanDelivery mirrors FBEventManagerTest's wait-with-counts cases:
// a channel subscriber receives posted counts for each event it registered.
func TestEventChanDelivery(t *testing.T) {
	fbe, _ := newTestEvent(t, "event_parity_")
	ch := make(chan Event, 64)
	sub, err := fbe.SubscribeChan([]string{"parity_a", "parity_b"}, ch)
	require.NoError(t, err)
	defer sub.Unsubscribe()

	time.Sleep(200 * time.Millisecond) // let the subscription settle

	for i := 0; i < 5; i++ {
		require.NoError(t, fbe.PostEvent("parity_a"))
	}
	for i := 0; i < 3; i++ {
		require.NoError(t, fbe.PostEvent("parity_b"))
	}

	counts := map[string]int{}
	deadline := time.After(5 * time.Second)
	for (counts["parity_a"] < 5 || counts["parity_b"] < 3) && len(counts) >= 0 {
		select {
		case e := <-ch:
			counts[e.Name] += e.Count
			continue
		case <-deadline:
		}
		break
	}
	require.Equal(t, 5, counts["parity_a"], "parity_a delivered count")
	require.Equal(t, 3, counts["parity_b"], "parity_b delivered count")
}

// TestEventMultipleSubscribers mirrors FBEventManagerTest's multiple-listener
// case: every subscriber receives the same posted event.
func TestEventMultipleSubscribers(t *testing.T) {
	fbe, _ := newTestEvent(t, "event_parity_multi_")
	ch1 := make(chan Event, 16)
	ch2 := make(chan Event, 16)
	sub1, err := fbe.SubscribeChan([]string{"multi_ev"}, ch1)
	require.NoError(t, err)
	defer sub1.Unsubscribe()
	sub2, err := fbe.SubscribeChan([]string{"multi_ev"}, ch2)
	require.NoError(t, err)
	defer sub2.Unsubscribe()

	time.Sleep(200 * time.Millisecond)
	require.NoError(t, fbe.PostEvent("multi_ev"))

	got1 := receiveWithin(ch1, 5*time.Second)
	got2 := receiveWithin(ch2, 5*time.Second)

	total := func(events []Event) int {
		n := 0
		for _, e := range events {
			n += e.Count
		}
		return n
	}
	require.Equal(t, 1, total(got1), "subscriber 1 must receive the event")
	require.Equal(t, 1, total(got2), "subscriber 2 must receive the event")
}

// TestEventLargeLoad mirrors FBEventManagerTest's large-load case: many rapid
// posts are all accounted for (possibly coalesced into count deltas).
func TestEventLargeLoad(t *testing.T) {
	fbe, _ := newTestEvent(t, "event_parity_load_")
	ch := make(chan Event, 64)
	sub, err := fbe.SubscribeChan([]string{"load_ev"}, ch)
	require.NoError(t, err)
	defer sub.Unsubscribe()

	time.Sleep(200 * time.Millisecond)
	const posts = 500
	for i := 0; i < posts; i++ {
		require.NoError(t, fbe.PostEvent("load_ev"))
	}

	total := 0
	deadline := time.After(10 * time.Second)
	for total < posts {
		select {
		case e := <-ch:
			total += e.Count
			continue
		case <-deadline:
		}
		break
	}
	require.Equal(t, posts, total, "all posted events must be accounted for")
}

// TestEventUnsubscribeStopsDelivery mirrors the FBEventManager lifecycle:
// after Unsubscribe no further deliveries arrive on the old channel, and
// fresh subscriptions still deliver.
func TestEventUnsubscribeStopsDelivery(t *testing.T) {
	dbPath, dsn, err := CreateTestDatabase("event_parity_unsub_")
	require.NoError(t, err)
	t.Cleanup(func() { _ = removeDatabaseFile(dbPath) })

	// A separate SQL connection posts events, independent of the FbEvent
	// connection lifecycle. (POST_EVENT is PSQL-only, so it needs the
	// execute-block wrapper.)
	poster := openTestDatabase(t, dsn)
	post := func() {
		mustExec(t, stmtCtx, poster, "execute block as begin post_event 'unsub_ev'; end")
	}

	fbe, err := NewFBEvent(dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = fbe.Close() })
	ch := make(chan Event, 16)
	sub, err := fbe.SubscribeChan([]string{"unsub_ev"}, ch)
	require.NoError(t, err)

	time.Sleep(200 * time.Millisecond)
	post() // sanity: delivery works before unsubscribing
	got := receiveWithin(ch, 5*time.Second)
	require.NotEmpty(t, got, "delivery must work before Unsubscribe")

	// After Unsubscribe the old channel stays silent. (Unsubscribe may
	// surface the connection teardown race as ErrFbEventClosed.)
	if err := sub.Unsubscribe(); err != nil && err != ErrFbEventClosed {
		require.NoError(t, err)
	}
	for i := 0; i < 5; i++ {
		post()
	}
	got = receiveWithin(ch, 1*time.Second)
	require.Empty(t, got, "no delivery expected after Unsubscribe")

	// A fresh subscription on the same database still delivers.
	fbe2, err := NewFBEvent(dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = fbe2.Close() })
	ch2 := make(chan Event, 16)
	sub2, err := fbe2.SubscribeChan([]string{"unsub_ev"}, ch2)
	require.NoError(t, err)
	defer sub2.Unsubscribe()
	time.Sleep(200 * time.Millisecond)
	post()
	got2 := receiveWithin(ch2, 5*time.Second)
	require.NotEmpty(t, got2, "new subscription must still deliver")
}
