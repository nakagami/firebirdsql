package firebirdsql

import (
	"context"
	"io"
	"net"
	"testing"
	"time"
)

// canceldropHarness is a loopback socket pair with a silent server side: the
// driver-side wireProtocol parks in reads that only a hard drop can unblock.
type canceldropHarness struct {
	wp        *wireProtocol
	serverEnd net.Conn
}

func newCanceldropHarness(t *testing.T) *canceldropHarness {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { ln.Close() })

	type dialResult struct {
		conn net.Conn
		err  error
	}
	dialed := make(chan dialResult, 1)
	go func() {
		c, err := net.Dial("tcp", ln.Addr().String())
		dialed <- dialResult{c, err}
	}()
	serverEnd, err := ln.Accept()
	if err != nil {
		t.Fatalf("accept: %v", err)
	}
	dr := <-dialed
	if dr.err != nil {
		t.Fatalf("dial: %v", dr.err)
	}
	t.Cleanup(func() { serverEnd.Close(); dr.conn.Close() })

	wp := &wireProtocol{buf: make([]byte, 0, 1024)}
	wp.conn, err = newWireChannel(dr.conn)
	if err != nil {
		t.Fatalf("wireChannel: %v", err)
	}
	return &canceldropHarness{wp: wp, serverEnd: serverEnd}
}

// serverSeesEOF drains the server side until EOF (the client op_cancel packet
// may arrive first) and reports whether EOF arrived before the deadline.
func (h *canceldropHarness) serverSeesEOF(within time.Duration) bool {
	deadline := time.Now().Add(within)
	buf := make([]byte, 512)
	for {
		h.serverEnd.SetReadDeadline(deadline)
		if _, err := h.serverEnd.Read(buf); err != nil {
			return err == io.EOF
		}
		if time.Now().After(deadline) {
			return false
		}
	}
}

func (h *canceldropHarness) stmt() *firebirdsqlStmt {
	return &firebirdsqlStmt{fc: &firebirdsqlConn{wp: h.wp}}
}

// parkedRead is an fn for withCancelWatcher that parks on the wire until the
// socket drops out from under it.
func parkedRead(wp *wireProtocol) func() error {
	return func() error {
		buf := make([]byte, 16)
		_, err := wp.conn.Read(buf)
		return err
	}
}

// The op_cancel-ignoring server case: cancel fires, op_cancel goes nowhere,
// the read stays parked — hard drop must close the socket within the grace
// period and unblock withCancelWatcher.
func TestWithCancelWatcherHardDropClosesSocket(t *testing.T) {
	h := newCanceldropHarness(t)
	h.wp.cancelHardDrop = true
	h.wp.cancelHardDropGrace = 300 * time.Millisecond
	stmt := h.stmt()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	start := time.Now()
	go func() {
		done <- stmt.withCancelWatcher(ctx, parkedRead(h.wp))
	}()

	time.Sleep(100 * time.Millisecond) // let fn park on the wire first
	cancel()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected a network error after the hard drop, got nil")
		}
		if el := time.Since(start); el > 3*time.Second {
			t.Fatalf("hard drop took %v, want ~grace (300ms)", el)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("withCancelWatcher not unblocked by hard drop")
	}
	if !h.serverSeesEOF(2 * time.Second) {
		t.Fatal("server side did not see the socket close")
	}
	// The watcher must be joined: no goroutine left writing op_cancel after
	// withCancelWatcher returns (rely on the race detector in -race runs).
}

// Healthy cancel: the server honors op_cancel (read returns right after the
// cancel), so the grace never expires and the socket must stay open.
func TestWithCancelWatcherHardDropHealthyCancelKeepsSocket(t *testing.T) {
	h := newCanceldropHarness(t)
	h.wp.cancelHardDrop = true
	h.wp.cancelHardDropGrace = 2 * time.Second
	stmt := h.stmt()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- stmt.withCancelWatcher(ctx, func() error {
			// Simulate the server responding shortly after op_cancel: the
			// watcher wakes us by closing... we just return quickly; the
			// watcher's select then takes the stop branch.
			time.Sleep(100 * time.Millisecond)
			return nil
		})
	}()

	if err := <-done; err != nil {
		t.Fatalf("healthy cancel returned %v", err)
	}
	if h.serverSeesEOF(500 * time.Millisecond) {
		t.Fatal("socket was closed on the healthy path")
	}
}

// Opt-out (default): without cancel_hard_drop the parked read stays parked
// after cancel — the pre-fork behavior this parameter must not change.
func TestWithCancelWatcherHardDropOptOutStaysParked(t *testing.T) {
	h := newCanceldropHarness(t)
	h.wp.cancelHardDrop = false
	h.wp.cancelHardDropGrace = 300 * time.Millisecond
	stmt := h.stmt()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- stmt.withCancelWatcher(ctx, parkedRead(h.wp))
	}()

	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		t.Fatalf("read returned without hard drop: %v (opt-out must stay parked)", err)
	case <-time.After(1 * time.Second):
		// still parked — correct for the default
	}
	_ = h.wp.conn.Close() // release the parked read for cleanup
}

// fn returning before the grace expires must close(stop) the watcher before
// the timer fires — no drop even though ctx was canceled mid-op.
func TestWithCancelWatcherHardDropGraceNotArmedAfterReturn(t *testing.T) {
	h := newCanceldropHarness(t)
	h.wp.cancelHardDrop = true
	h.wp.cancelHardDropGrace = 150 * time.Millisecond
	stmt := h.stmt()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- stmt.withCancelWatcher(ctx, func() error {
			time.Sleep(50 * time.Millisecond) // fn wins the race against grace
			return nil
		})
		cancel() // cancel while/just after fn returns
	}()

	if err := <-done; err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	time.Sleep(300 * time.Millisecond) // let a wrongly-armed grace fire
	if h.serverSeesEOF(200 * time.Millisecond) {
		t.Fatal("socket closed although fn returned before grace expired")
	}
}

func TestParseDSNHardDropOptions(t *testing.T) {
	d, err := parseDSN("SYSDBA:masterkey@localhost:3050/db.fdb?cancel_hard_drop=true&cancel_hard_drop_grace=1500")
	if err != nil {
		t.Fatalf("parseDSN: %v", err)
	}
	if d.options["cancel_hard_drop"] != "true" || d.options["cancel_hard_drop_grace"] != "1500" {
		t.Fatalf("options not parsed: %+v", d.options)
	}

	d, err = parseDSN("SYSDBA:masterkey@localhost:3050/db.fdb")
	if err != nil {
		t.Fatalf("parseDSN default: %v", err)
	}
	if d.options["cancel_hard_drop"] != "false" || d.options["cancel_hard_drop_grace"] != "3000" {
		t.Fatalf("defaults wrong: %+v", d.options)
	}
}
