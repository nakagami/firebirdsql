package firebirdsql

import (
	"bytes"
	"context"
	"database/sql/driver"
	"errors"
	"io"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"
)

// pingFakeConn is an in-memory net.Conn for Ping tests that need no server.
// Read serves canned bytes and never parks, Write discards, and every
// SetDeadline call is recorded in order.
type pingFakeConn struct {
	mu        sync.Mutex
	rd        *bytes.Reader
	deadlines []time.Time
}

func (f *pingFakeConn) Read(b []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.rd.Read(b)
}
func (f *pingFakeConn) Write(b []byte) (int, error) { return len(b), nil }
func (f *pingFakeConn) Close() error                { return nil }
func (f *pingFakeConn) LocalAddr() net.Addr         { return nil }
func (f *pingFakeConn) RemoteAddr() net.Addr        { return nil }
func (f *pingFakeConn) SetDeadline(t time.Time) error {
	f.mu.Lock()
	f.deadlines = append(f.deadlines, t)
	f.mu.Unlock()
	return nil
}
func (f *pingFakeConn) SetReadDeadline(t time.Time) error  { return f.SetDeadline(t) }
func (f *pingFakeConn) SetWriteDeadline(t time.Time) error { return f.SetDeadline(t) }

// snapshot returns the number of recorded SetDeadline calls and the last one.
func (f *pingFakeConn) snapshot() (int, time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.deadlines) == 0 {
		return 0, time.Time{}
	}
	return len(f.deadlines), f.deadlines[len(f.deadlines)-1]
}

// nonZeroSince counts the non-zero deadlines recorded after the first n calls.
func (f *pingFakeConn) nonZeroSince(n int) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	cnt := 0
	for _, d := range f.deadlines[n:] {
		if !d.IsZero() {
			cnt++
		}
	}
	return cnt
}

// newSilentPeerConn returns a connection whose peer accepts bytes and never
// answers, so every wire read blocks until a socket deadline fires.
func newSilentPeerConn(t *testing.T) *firebirdsqlConn {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to start silent listener: %v", err)
	}
	acceptDone := make(chan struct{})
	go func() {
		defer close(acceptDone)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				_, _ = io.Copy(io.Discard, c)
			}(conn)
		}
	}()
	t.Cleanup(func() {
		listener.Close()
		<-acceptDone
	})

	netConn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatalf("Failed to dial silent listener: %v", err)
	}
	t.Cleanup(func() { netConn.Close() })

	wc, err := newWireChannel(netConn)
	if err != nil {
		t.Fatalf("newWireChannel failed: %v", err)
	}
	return &firebirdsqlConn{
		wp: &wireProtocol{
			buf:      make([]byte, 0, BUFFER_LEN),
			conn:     wc,
			dbHandle: 1,
		},
		transactionSet: make(map[*firebirdsqlTx]struct{}),
	}
}

// pingAsync runs Ping on its own goroutine so a Ping that never returns fails
// the test instead of hanging it. The goroutine is leaked on failure.
func pingAsync(fc *firebirdsqlConn, ctx context.Context) <-chan error {
	res := make(chan error, 1)
	go func() { res <- fc.Ping(ctx) }()
	return res
}

// TestPingLeavesNoDeadlineAfterReturn: a successful Ping must not leave the
// cancellation machinery running. In the common pattern
// "ctx, cancel := WithTimeout; defer cancel(); db.PingContext(ctx)" cancel()
// runs after the connection is back in the pool; a watcher that outlived Ping
// would then set an expired deadline on it (upstream issue #304). With
// GOMAXPROCS(1) the watcher stays unscheduled until the test yields, which
// makes the race show up in about half of the rounds.
func TestPingLeavesNoDeadlineAfterReturn(t *testing.T) {
	old := runtime.GOMAXPROCS(1)
	t.Cleanup(func() { runtime.GOMAXPROCS(old) })

	const rounds = 64
	var frames acceptFrame
	for i := 0; i < rounds; i++ {
		frames.opResponseFrame(0, []byte{isc_info_ods_version, 4, 0, 13, 0, 0, 0, isc_info_end})
	}
	f := &pingFakeConn{rd: bytes.NewReader(frames.bytes())}
	wc, err := newWireChannel(f)
	if err != nil {
		t.Fatalf("newWireChannel failed: %v", err)
	}
	fc := &firebirdsqlConn{wp: &wireProtocol{buf: make([]byte, 0, BUFFER_LEN), conn: wc, dbHandle: 1}}

	failed := 0
	for i := 0; i < rounds; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		err := fc.Ping(ctx)
		if err != nil {
			cancel()
			t.Fatalf("round %d: Ping failed: %v", i, err)
		}
		n, last := f.snapshot()
		if !last.IsZero() {
			cancel()
			t.Fatalf("round %d: Ping returned with socket deadline %v still armed", i, last)
		}

		// The caller cancels once the connection is back in the pool.
		cancel()
		runtime.Gosched()
		time.Sleep(2 * time.Millisecond)
		if late := f.nonZeroSince(n); late > 0 {
			failed++
			t.Logf("round %d: %d non-zero SetDeadline call(s) after Ping returned", i, late)
		}
	}
	if failed > 0 {
		t.Fatalf("Ping left an expired deadline on the connection in %d of %d rounds", failed, rounds)
	}
}

// TestPingStaleDeadlineDoesNotHang: a connection that carries an expired
// deadline makes the Ping read fail at once. Ping must report that as
// driver.ErrBadConn for any context, including one that can never be done: it
// must not wait on ctx.Done(), which is a nil channel for a context without
// a deadline or cancel function.
func TestPingStaleDeadlineDoesNotHang(t *testing.T) {
	cases := []struct {
		name string
		ctx  func(t *testing.T) context.Context
	}{
		{"Background", func(t *testing.T) context.Context { return context.Background() }},
		{"WithCancel", func(t *testing.T) context.Context {
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			return ctx
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fc := newSilentPeerConn(t)
			if err := fc.wp.conn.SetDeadline(time.Now().Add(-time.Second)); err != nil {
				t.Fatalf("SetDeadline failed: %v", err)
			}

			res := pingAsync(fc, tc.ctx(t))
			select {
			case err := <-res:
				if err == nil {
					t.Fatal("Ping on a connection with an expired deadline returned nil")
				}
				if !errors.Is(err, driver.ErrBadConn) {
					t.Fatalf("expected driver.ErrBadConn, got %v", err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("Ping did not return within 3s on a connection with an expired deadline")
			}
		})
	}
}

// TestPingCancelledMidFlightEvictsConn: cancelling a Ping that is blocked in
// the read must return the context error wrapped in driver.ErrBadConn and mark
// the wire desynced, so database/sql discards the connection instead of
// reusing one with an unread response in flight.
func TestPingCancelledMidFlightEvictsConn(t *testing.T) {
	fc := newSilentPeerConn(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	res := pingAsync(fc, ctx)
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-res:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
		if !errors.Is(err, driver.ErrBadConn) {
			t.Fatalf("expected driver.ErrBadConn, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Ping did not return within 3s of cancellation")
	}
	if !fc.wp.desynced || fc.IsValid() {
		t.Fatalf("connection must be marked desynced after a cancelled Ping: desynced=%v IsValid=%v",
			fc.wp.desynced, fc.IsValid())
	}
}

// TestPingFailureCloseDoesNotWait: after a failed Ping the pool closes the
// connection. Close must drop the socket instead of sending rollback and
// detach into a wire that is not going to answer, each of which would wait out
// abandonReadTimeout.
func TestPingFailureCloseDoesNotWait(t *testing.T) {
	defer func(orig time.Duration) { abandonReadTimeout = orig }(abandonReadTimeout)
	abandonReadTimeout = 2 * time.Second

	cases := []struct {
		name string
		prep func(fc *firebirdsqlConn)
		ctx  func(t *testing.T) context.Context
	}{
		{"Timeout", func(*firebirdsqlConn) {}, func(t *testing.T) context.Context {
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			t.Cleanup(cancel)
			return ctx
		}},
		{"BackgroundStaleDeadline", func(fc *firebirdsqlConn) {
			_ = fc.wp.conn.SetDeadline(time.Now().Add(-time.Second))
		}, func(*testing.T) context.Context { return context.Background() }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fc := newSilentPeerConn(t)
			// One transaction left in the set, as after any real transaction:
			// Close rolls it back before it detaches.
			fc.transactionSet[&firebirdsqlTx{fc: fc}] = struct{}{}
			tc.prep(fc)

			select {
			case err := <-pingAsync(fc, tc.ctx(t)):
				if err == nil {
					t.Fatal("Ping on a silent peer returned nil")
				}
			case <-time.After(3 * time.Second):
				t.Fatal("Ping did not return within 3s")
			}

			closed := make(chan time.Duration, 1)
			go func() {
				start := time.Now()
				_ = fc.Close()
				closed <- time.Since(start)
			}()
			select {
			case d := <-closed:
				if d > time.Second {
					t.Fatalf("Close took %v after a failed Ping, want < 1s (abandonReadTimeout is %v)", d, abandonReadTimeout)
				}
				t.Logf("Close took %v", d)
			case <-time.After(3 * abandonReadTimeout):
				t.Fatalf("Close did not return within %v", 3*abandonReadTimeout)
			}
		})
	}
}
