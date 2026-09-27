/*******************************************************************************
The MIT License (MIT)

Copyright (c) 2019 Arteev Aleksey

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
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// All credentials/addresses here are synthetic. No database configuration is read.
// Run ONLY this suite with -run '^TestLifecycle' (upstream integration tests create databases).
// Timeouts below fail the test; they are not the lifecycle implementation.
func lifecycleOwner(t *testing.T) *FbEvent {
	t.Helper()
	e, err := NewFBEvent("probe:probe@192.0.2.1:3050/probe?charset=ISO8859_1&auth_plugin_name=Legacy_Auth&wire_crypt=false")
	if err != nil {
		t.Fatal(err)
	}
	return e
}
func await(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("bounded lifecycle assertion exceeded 2s")
	}
}
func closedOwner(t *testing.T, e *FbEvent) {
	t.Helper()
	start := time.Now()
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := e.Close(); err != nil {
			t.Errorf("Close error: %v", err)
		}
	}()
	await(t, done)
	t.Logf("Close=%s goroutines=%d", time.Since(start), runtime.NumGoroutine())
	if e.Count() != 0 {
		t.Fatal("subscriber survived shutdown")
	}
}
func subscribeAsync(e *FbEvent) <-chan error {
	ch := make(chan error, 1)
	go func() { _, err := e.Subscribe([]string{"probe"}, func(Event) {}); ch <- err }()
	return ch
}
func canceledResult(t *testing.T, ch <-chan error) {
	t.Helper()
	select {
	case err := <-ch:
		if !errors.Is(err, context.Canceled) && !errors.Is(err, ErrFbEventClosed) {
			t.Fatalf("expected cancellation, got %v", err)
		}
		t.Logf("Subscribe error=%v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("Subscribe still running")
	}
}

func TestLifecycleDialDrop(t *testing.T) {
	for _, n := range []int{1, 32} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			e := lifecycleOwner(t)
			entered := make(chan struct{}, n)
			var active int32
			e.dial = func(ctx context.Context, network, addr string) (net.Conn, error) {
				atomic.AddInt32(&active, 1)
				defer atomic.AddInt32(&active, -1)
				entered <- struct{}{}
				<-ctx.Done()
				return nil, ctx.Err()
			}
			results := make([]<-chan error, n)
			for i := range results {
				results[i] = subscribeAsync(e)
			}
			for range results {
				await(t, entered)
			}
			closedOwner(t, e)
			for _, ch := range results {
				canceledResult(t, ch)
			}
			if atomic.LoadInt32(&active) != 0 {
				t.Fatal("dial worker survived")
			}
		})
	}
}
func TestLifecycleReject(t *testing.T) {
	e := lifecycleOwner(t)
	defer e.Close()
	e.dial = func(context.Context, string, string) (net.Conn, error) {
		return nil, &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}
	}
	start := time.Now()
	_, err := e.Subscribe([]string{"probe"}, nil)
	if !errors.Is(err, syscall.ECONNREFUSED) {
		t.Fatal(err)
	}
	t.Logf("REJECT=%s error=%v", time.Since(start), err)
}

// scriptedSocket simulates protocol responses and silent stages without a Firebird
// process. It is NOT an OS socket and cannot prove real Firebird compatibility.
type scriptedSocket struct {
	writeFailure int32
	connectReply []byte
	readWait     chan struct{}
	responses    chan []byte
	closed       chan struct{}
	once         sync.Once
	mu           sync.Mutex
	rest         []byte
	kind         string
	silence      int32
	failQueue    int32
	queueCount   int32
	writes       chan int32
}

func socket(kind string) *scriptedSocket {
	return &scriptedSocket{responses: make(chan []byte, 32), closed: make(chan struct{}), kind: kind, writes: make(chan int32, 64)}
}
func words(v ...int32) []byte {
	b := make([]byte, 4*len(v))
	for i, n := range v {
		binary.BigEndian.PutUint32(b[4*i:], uint32(n))
	}
	return b
}
func response(buf []byte) []byte {
	b := words(op_response, 1, 0, 0, int32(len(buf)))
	b = append(b, buf...)
	for len(b)%4 != 0 {
		b = append(b, 0)
	}
	return append(b, words(1, 0, 0)...)
}
func (c *scriptedSocket) Read(b []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.rest) == 0 {
		if c.readWait != nil {
			select {
			case c.readWait <- struct{}{}:
			default:
			}
		}
		select {
		case <-c.closed:
			return 0, io.EOF
		case c.rest = <-c.responses:
		}
	}
	n := copy(b, c.rest)
	c.rest = c.rest[n:]
	return n, nil
}
func (c *scriptedSocket) Write(b []byte) (int, error) {
	select {
	case <-c.closed:
		return 0, io.ErrClosedPipe
	default:
	}
	if len(b) < 4 {
		return 0, io.ErrShortWrite
	}
	op := int32(binary.BigEndian.Uint32(b))
	c.writes <- op
	if op == atomic.LoadInt32(&c.writeFailure) {
		return 0, io.ErrClosedPipe
	}
	if op == atomic.LoadInt32(&c.silence) {
		return len(b), nil
	}
	var reply []byte
	switch op {
	case op_connect:
		reply = words(op_accept, 12, 1, 2)
		if c.connectReply != nil {
			reply = c.connectReply
		}
	case op_attach:
		reply = response(nil)
	case op_connect_request:
		reply = response([]byte{2, 0, 12, 0, 127, 0, 0, 1})
	case op_que_events:
		n := atomic.AddInt32(&c.queueCount, 1)
		if n == atomic.LoadInt32(&c.failQueue) {
			reply = words(op_reject)
		} else {
			reply = response(nil)
		}
	default:
		return 0, fmt.Errorf("unexpected synthetic opcode %d", op)
	}
	select {
	case <-c.closed:
		return 0, io.ErrClosedPipe
	case c.responses <- reply:
	}
	return len(b), nil
}
func (c *scriptedSocket) Close() error { c.once.Do(func() { close(c.closed) }); return nil }
func (c *scriptedSocket) LocalAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1}
}
func (c *scriptedSocket) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 2}
}
func (c *scriptedSocket) SetDeadline(time.Time) error      { return nil }
func (c *scriptedSocket) SetReadDeadline(time.Time) error  { return nil }
func (c *scriptedSocket) SetWriteDeadline(time.Time) error { return nil }
func setupSockets(e *FbEvent, main, aux *scriptedSocket) {
	var calls int32
	e.dial = func(ctx context.Context, _, _ string) (net.Conn, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if atomic.AddInt32(&calls, 1) == 1 {
			return main, nil
		}
		return aux, nil
	}
}
func waitOp(t *testing.T, c *scriptedSocket, op int32) {
	t.Helper()
	for {
		select {
		case v := <-c.writes:
			if v == op {
				return
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("did not reach opcode %d", op)
		}
	}
}
func assertSocketsClosed(t *testing.T, cs ...*scriptedSocket) {
	t.Helper()
	for _, c := range cs {
		select {
		case <-c.closed:
		default:
			t.Fatal("owned transport not closed")
		}
	}
	t.Logf("owned transports closed=%d/%d", len(cs), len(cs))
}
func emitEvent(aux *scriptedSocket) {
	b := buildEpbSlice([]string{"probe"}, map[string]int{"probe": 1})
	packet := words(op_event, 1, int32(len(b)))
	packet = append(packet, b...)
	for len(packet)%4 != 0 {
		packet = append(packet, 0)
	}
	packet = append(packet, words(0, 0, 1)...)
	aux.responses <- packet
}

func TestLifecycleSilentHandshakeAttach(t *testing.T) {
	for _, stage := range []int32{op_connect, op_attach, op_connect_request, op_que_events} {
		t.Run(fmt.Sprint(stage), func(t *testing.T) {
			e := lifecycleOwner(t)
			main, aux := socket("main"), socket("aux")
			main.silence = stage
			setupSockets(e, main, aux)
			result := subscribeAsync(e)
			waitOp(t, main, stage)
			closedOwner(t, e)
			canceledResult(t, result)
			assertSocketsClosed(t, main)
			if stage == op_que_events {
				assertSocketsClosed(t, aux)
			}
		})
	}
}
func TestLifecycleAuxDialCancellation(t *testing.T) {
	e := lifecycleOwner(t)
	main := socket("main")
	entered := make(chan struct{})
	var calls int32
	e.dial = func(ctx context.Context, _, _ string) (net.Conn, error) {
		if atomic.AddInt32(&calls, 1) == 1 {
			return main, nil
		}
		close(entered)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	result := subscribeAsync(e)
	await(t, entered)
	closedOwner(t, e)
	canceledResult(t, result)
	assertSocketsClosed(t, main)
}
func TestLifecycleBeforeRegistration(t *testing.T) {
	e := lifecycleOwner(t)
	main, aux := socket("main"), socket("aux")
	setupSockets(e, main, aux)
	ready := make(chan struct{})
	e.prepare = func(scope *eventScope, dsn *firebirdDsn, ev []string, cb EventHandler, ch chan Event) (*Subscription, error) {
		s, err := newSubscription(scope, dsn, ev, cb, ch)
		if err != nil {
			return nil, err
		}
		close(ready)
		<-scope.ctx.Done()
		return s, nil
	}
	result := subscribeAsync(e)
	await(t, ready)
	closedOwner(t, e)
	canceledResult(t, result)
	assertSocketsClosed(t, main, aux)
}
func TestLifecycleInitialQueueError(t *testing.T) {
	e := lifecycleOwner(t)
	defer e.Close()
	main, aux := socket("main"), socket("aux")
	main.failQueue = 1
	setupSockets(e, main, aux)
	s, err := e.Subscribe([]string{"probe"}, nil)
	if err == nil || s != nil {
		t.Fatal("initial error swallowed")
	}
	t.Logf("initial error=%v", err)
	if e.Count() != 0 {
		t.Fatal("failed registration retained")
	}
	assertSocketsClosed(t, main, aux)
}
func TestLifecycleNormalAndRearm(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprint(fail), func(t *testing.T) {
			e := lifecycleOwner(t)
			defer e.Close()
			main, aux := socket("main"), socket("aux")
			if fail {
				main.failQueue = 2
			}
			setupSockets(e, main, aux)
			delivered := make(chan Event, 1)
			s, err := e.SubscribeChan([]string{"probe"}, delivered)
			if err != nil {
				t.Fatal(err)
			}
			notices := make(chan error, 1)
			s.NotifyClose(notices)
			emitEvent(aux)
			select {
			case v := <-delivered:
				if v.Name != "probe" || v.Count != 1 {
					t.Fatalf("bad synthetic event %+v", v)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("event not delivered")
			}
			if fail {
				await(t, s.doneSubscription)
				if s.terminalError() == nil {
					t.Fatal("rearm error swallowed")
				}
				select {
				case err := <-notices:
					t.Logf("rearm error=%v", err)
				case <-time.After(2 * time.Second):
					t.Fatal("missing buffered notice")
				}
			} else {
				waitOp(t, main, op_que_events)
				if err := s.Unsubscribe(); err != nil {
					t.Fatal(err)
				}
				if err := s.Unsubscribe(); err != nil {
					t.Fatal(err)
				}
			}
			assertSocketsClosed(t, main, aux)
			if e.Count() != 0 {
				t.Fatal("Unsubscribe did not remove subscription")
			}
		})
	}
}
func TestLifecycleBlockedDeliveryAndNotice(t *testing.T) {
	e := lifecycleOwner(t)
	main, aux := socket("main"), socket("aux")
	setupSockets(e, main, aux)
	s, err := e.SubscribeChan([]string{"probe"}, make(chan Event))
	if err != nil {
		t.Fatal(err)
	}
	s.NotifyClose(make(chan error))
	emitEvent(aux)
	closedOwner(t, e)
	await(t, s.doneSubscription)
	assertSocketsClosed(t, main, aux)
}
func TestLifecycleUnreadErrorNotice(t *testing.T) {
	e := lifecycleOwner(t)
	defer e.Close()
	main, aux := socket("main"), socket("aux")
	main.failQueue = 2
	setupSockets(e, main, aux)
	s, err := e.SubscribeChan([]string{"probe"}, make(chan Event, 1))
	if err != nil {
		t.Fatal(err)
	}
	s.NotifyClose(make(chan error))
	emitEvent(aux)
	await(t, s.doneSubscription)
	if s.terminalError() == nil {
		t.Fatal("error missing")
	}
	assertSocketsClosed(t, main, aux)
}
func TestLifecycleCloseRace(t *testing.T) {
	before := runtime.NumGoroutine()
	for n := 0; n < 100; n++ {
		e := lifecycleOwner(t)
		e.dial = func(ctx context.Context, _, _ string) (net.Conn, error) { <-ctx.Done(); return nil, ctx.Err() }
		var wg sync.WaitGroup
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err := e.Subscribe([]string{"probe"}, nil)
				if err == nil {
					t.Error("unexpected success")
				}
			}()
		}
		for i := 0; i < 4; i++ {
			wg.Add(1)
			go func() { defer wg.Done(); e.Close() }()
		}
		wg.Wait()
		if e.Count() != 0 {
			t.Fatal("late registration")
		}
	}
	t.Logf("100 owners x 8 Subscribe + 4 Close; goroutines before=%d after=%d", before, runtime.NumGoroutine())
}

// Actual TCP is limited to an ephemeral loopback listener. No firewall or server
// configuration is changed. The peer intentionally withholds the handshake.
func TestLifecycleRealTCPSilentPeer(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	var liveTCP int32
	peerErr := make(chan error, 1)
	peerDone := make(chan struct{})
	accepted := make(chan struct{})
	go func() {
		defer close(peerDone)
		c, err := listener.Accept()
		if err != nil {
			return
		}
		atomic.AddInt32(&liveTCP, 1)
		c = &countedTCPConn{Conn: c, live: &liveTCP}
		defer c.Close()
		buf := make([]byte, 4096)
		if _, err = c.Read(buf); err != nil {
			return
		}
		close(accepted)
		_, copyErr := io.Copy(io.Discard, c)
		peerErr <- copyErr
	}()
	e := lifecycleOwner(t)
	e.dsn.addr = listener.Addr().String()
	realDial := e.dial
	e.dial = func(ctx context.Context, n, a string) (net.Conn, error) {
		c, err := realDial(ctx, n, a)
		if err != nil {
			return nil, err
		}
		atomic.AddInt32(&liveTCP, 1)
		return &countedTCPConn{Conn: c, live: &liveTCP}, nil
	}
	result := subscribeAsync(e)
	await(t, accepted)
	closedOwner(t, e)
	canceledResult(t, result)
	await(t, peerDone)
	if err := <-peerErr; err != nil {
		t.Fatalf("peer expected EOF: %v", err)
	}
	if live := atomic.LoadInt32(&liveTCP); live != 0 {
		t.Fatalf("owned TCP sockets still open: %d", live)
	}
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	t.Log("owned OS TCP sockets: 2 endpoints -> 0; listener: 1 -> 0")
	t.Log("OS TCP peer observed EOF; client and accepted socket closed; listener closed by defer")
}
func TestLifecycleActualDialGate(t *testing.T) {
	e := lifecycleOwner(t)
	started := make(chan struct{})
	dial := e.dial
	e.dial = func(ctx context.Context, n, a string) (net.Conn, error) { close(started); return dial(ctx, n, a) }
	result := subscribeAsync(e)
	await(t, started)
	select {
	case err := <-result:
		e.Close()
		t.Skipf("destination ended before in-flight observation: %v", err)
	case <-time.After(250 * time.Millisecond):
	}
	closedOwner(t, e)
	canceledResult(t, result)
}

func TestLifecycleAuthenticationPartial(t *testing.T) {
	for _, payload := range [][]byte{words(op_accept_data, 13, 1, 2), append(append(words(op_accept_data, 13, 1, 2, 0, 3), []byte{'S', 'r', 'p', 0}...), words(0, 0)...)} {
		e := lifecycleOwner(t)
		main, aux := socket("main"), socket("aux")
		main.connectReply = payload
		main.silence = op_cont_auth
		main.readWait = make(chan struct{}, 32)
		setupSockets(e, main, aux)
		result := subscribeAsync(e)
		waitOp(t, main, op_connect)
		// Wait until the reader consumes the partial frame and requests more bytes.
		// A first read may precede consumption of the prepared response; the second cannot.
		await(t, main.readWait)
		await(t, main.readWait)
		if len(payload) > 16 {
			waitOp(t, main, op_cont_auth)
		}
		closedOwner(t, e)
		canceledResult(t, result)
		assertSocketsClosed(t, main)
	}
}
func TestLifecycleManyRegistered(t *testing.T) {
	e := lifecycleOwner(t)
	var mu sync.Mutex
	var all []*scriptedSocket
	e.dial = func(ctx context.Context, _, _ string) (net.Conn, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		c := socket("any")
		mu.Lock()
		all = append(all, c)
		mu.Unlock()
		return c, nil
	}
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := e.SubscribeChan([]string{"probe"}, make(chan Event)); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if e.Count() != 32 {
		t.Fatalf("count=%d", e.Count())
	}
	subs := e.Subscribers()
	for _, s := range subs {
		wg.Add(1)
		go func(s *Subscription) { defer wg.Done(); s.Unsubscribe() }(s)
	}
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); e.Close() }()
	}
	wg.Wait()
	for _, s := range subs {
		await(t, s.doneSubscription)
	}
	assertSocketsClosed(t, all...)
	if e.Count() != 0 {
		t.Fatal("registered subscription remained")
	}
}

func TestLifecycleInitialQueueWriteError(t *testing.T) {
	e := lifecycleOwner(t)
	defer e.Close()
	main, aux := socket("main"), socket("aux")
	main.writeFailure = op_que_events
	setupSockets(e, main, aux)
	result := subscribeAsync(e)
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("write error swallowed")
		}
		t.Logf("initial queue write error=%v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("waiting response after failed write")
	}
	assertSocketsClosed(t, main, aux)
}
func TestLifecycleRealTCPReject(t *testing.T) {
	l, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close()
	e := lifecycleOwner(t)
	defer e.Close()
	e.dsn.addr = addr
	start := time.Now()
	_, err = e.Subscribe([]string{"probe"}, nil)
	if !errors.Is(err, syscall.ECONNREFUSED) && !(runtime.GOOS == "windows" && errors.Is(err, syscall.Errno(10061))) {
		t.Fatalf("expected refusal, got %v", err)
	}
	t.Logf("OS loopback REJECT elapsed=%s; error type=%T; no connection established", time.Since(start), err)
}

func TestLifecyclePartialEventShutdown(t *testing.T) {
	for _, frame := range [][]byte{words(op_event), words(op_event, 1), words(op_event, 1, 10)} {
		e := lifecycleOwner(t)
		main, aux := socket("main"), socket("aux")
		aux.readWait = make(chan struct{}, 32)
		setupSockets(e, main, aux)
		s, err := e.SubscribeChan([]string{"probe"}, make(chan Event))
		if err != nil {
			t.Fatal(err)
		}
		aux.responses <- frame
		await(t, aux.readWait)
		await(t, aux.readWait)
		closedOwner(t, e)
		await(t, s.doneSubscription)
		assertSocketsClosed(t, main, aux)
	}
}

type countedTCPConn struct {
	net.Conn
	live *int32
	once sync.Once
	err  error
}

func (c *countedTCPConn) Close() error {
	c.once.Do(func() { c.err = c.Conn.Close(); atomic.AddInt32(c.live, -1) })
	return c.err
}
func TestLifecycleNormalCallback(t *testing.T) {
	e := lifecycleOwner(t)
	defer e.Close()
	main, aux := socket("main"), socket("aux")
	setupSockets(e, main, aux)
	delivered := make(chan struct{})
	s, err := e.Subscribe([]string{"probe"}, func(ev Event) {
		if ev.Name != "probe" {
			t.Error("wrong callback event")
		}
		close(delivered)
	})
	if err != nil {
		t.Fatal(err)
	}
	emitEvent(aux)
	await(t, delivered)
	s.Unsubscribe()
	assertSocketsClosed(t, main, aux)
}
