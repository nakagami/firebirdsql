package firebirdsql

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakeTraceService struct {
	startErr, readErr, closeErr error
	reply                       string
	closes                      int
}

func (s *fakeTraceService) ServiceStartContext(context.Context, []byte) error { return s.startErr }
func (s *fakeTraceService) GetStringContext(context.Context) (string, bool, error) {
	return s.reply, false, s.readErr
}
func (s *fakeTraceService) WaitStringContext(context.Context) (string, error) {
	return s.reply, s.readErr
}
func (s *fakeTraceService) WaitStringsContext(context.Context, chan string) error { return s.readErr }
func (s *fakeTraceService) WaitContext(context.Context) error                     { return s.readErr }
func (s *fakeTraceService) CloseContext(context.Context) error                    { s.closes++; return s.closeErr }
func traceManagerFake(s *fakeTraceService, err error) *TraceManager {
	return &TraceManager{connBuilder: func(context.Context) (traceService, error) { return s, err }}
}

func TestTraceLifecycleStartFailures(t *testing.T) {
	failure := errors.New("injected")
	for _, tc := range []struct {
		name string
		svc  *fakeTraceService
		dial error
	}{
		{"dial", &fakeTraceService{}, failure},
		{"start", &fakeTraceService{startErr: failure}, nil},
		{"read", &fakeTraceService{readErr: failure}, nil},
		{"malformed", &fakeTraceService{reply: "arbitrary private output"}, nil},
		{"overflow", &fakeTraceService{reply: "Trace session ID 2147483648 started"}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := traceManagerFake(tc.svc, tc.dial).Start("config")
			if err == nil {
				t.Fatal("expected error")
			}
			if strings.Contains(err.Error(), "private output") {
				t.Fatal("raw response leaked")
			}
			want := 1
			if tc.dial != nil {
				want = 0
			}
			if tc.svc.closes != want {
				t.Fatalf("closes=%d want=%d", tc.svc.closes, want)
			}
			if (tc.dial != nil || tc.svc.readErr != nil || tc.svc.startErr != nil) && !errors.Is(err, failure) {
				t.Fatalf("lost error: %v", err)
			}
		})
	}
}

func TestTraceLifecycleListErrors(t *testing.T) {
	failure := errors.New("injected")
	for _, tc := range []struct {
		s    *fakeTraceService
		dial error
	}{
		{&fakeTraceService{}, failure}, {&fakeTraceService{startErr: failure}, nil},
		{&fakeTraceService{readErr: failure}, nil}, {&fakeTraceService{closeErr: failure}, nil},
	} {
		if _, err := traceManagerFake(tc.s, tc.dial).List(); !errors.Is(err, failure) {
			t.Fatalf("lost error: %v", err)
		}
	}
}

func TestTraceLifecycleControlAndClose(t *testing.T) {
	stream := &fakeTraceService{reply: "Trace session ID 42 started"}
	m := traceManagerFake(stream, nil)
	ts, err := m.Start("config")
	if err != nil {
		t.Fatal(err)
	}
	if ts.ID() != 42 || ts.State() != SessionRunning {
		t.Fatal("wrong identity/state")
	}
	aux := &fakeTraceService{reply: "Trace session ID 42 paused"}
	ts.connBuilder = traceManagerFake(aux, nil).connBuilder
	if err = ts.Pause(); err != nil {
		t.Fatal(err)
	}
	if ts.State() != SessionPaused {
		t.Fatal("not paused")
	}
	aux.reply = "Trace session ID 43 resumed"
	if err = ts.Resume(); err == nil {
		t.Fatal("accepted different session ID")
	}
	stopErr, closeErr := errors.New("stop"), errors.New("close")
	ts.connBuilder = traceManagerFake(nil, stopErr).connBuilder
	stream.closeErr = closeErr
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			e := ts.Close()
			if !errors.Is(e, stopErr) || !errors.Is(e, closeErr) {
				t.Errorf("lost combined error: %v", e)
			}
		}()
	}
	wg.Wait()
	if stream.closes != 1 {
		t.Fatalf("closes=%d", stream.closes)
	}
}

func servicePipe(t *testing.T) (*ServiceManager, net.Conn) {
	t.Helper()
	client, server := net.Pipe()
	channel, _ := newWireChannel(client)
	svc := &ServiceManager{wp: &wireProtocol{conn: channel}}
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })
	return svc, server
}

func TestServiceLifecycleCancelBlockedRead(t *testing.T) {
	svc, server := servicePipe(t)
	go io.Copy(io.Discard, server) // consume request but never send response
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	_, _, err := svc.GetStringContext(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v", err)
	}
	if _, _, err = svc.GetString(); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("reused canceled stream: %v", err)
	}
	if err = svc.Close(); err != nil {
		t.Fatal(err)
	}
	if err = svc.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestServiceLifecycleCancelUnreadConsumer(t *testing.T) {
	for _, binary := range []bool{false, true} {
		t.Run(fmt.Sprint(binary), func(t *testing.T) {
			svc, server := servicePipe(t)
			item := byte(isc_info_svc_line)
			if binary {
				item = isc_info_svc_to_eof
			}
			var f acceptFrame
			f.opResponseFrame(0, []byte{item, 1, 0, 'x', isc_info_end})
			go func() { go io.Copy(io.Discard, server); _, _ = server.Write(f.bytes()) }()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
			defer cancel()
			var err error
			if binary {
				err = svc.WaitBufferContext(ctx, make(chan []byte))
			} else {
				err = svc.WaitStringsContext(ctx, make(chan string))
			}
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("got %v", err)
			}
			if err = svc.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestServiceLifecycleCloseDuringRead(t *testing.T) {
	svc, server := servicePipe(t)
	request := make(chan struct{})
	go func() {
		b := make([]byte, 4)
		_, _ = io.ReadFull(server, b)
		close(request)
		_, _ = io.Copy(io.Discard, server)
	}()
	readDone := make(chan error, 1)
	go func() { _, _, err := svc.GetString(); readDone <- err }()
	<-request
	if _, _, err := svc.GetString(); !errors.Is(err, ErrServiceBusy) {
		t.Fatalf("second reader: %v", err)
	}
	if err := svc.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-readDone:
		if err == nil {
			t.Fatal("read succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("reader leaked")
	}
}

func TestServiceLifecycleCloseUnreadConsumer(t *testing.T) {
	svc, server := servicePipe(t)
	var f acceptFrame
	f.opResponseFrame(0, []byte{isc_info_svc_line, 1, 0, 'x', isc_info_end})
	delivered := make(chan struct{})
	go func() { go io.Copy(io.Discard, server); _, _ = server.Write(f.bytes()); close(delivered) }()
	readDone := make(chan error, 1)
	go func() { readDone <- svc.WaitStrings(make(chan string)) }()
	<-delivered
	if err := svc.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-readDone:
	case <-time.After(time.Second):
		t.Fatal("delivery leaked")
	}
}

func TestServiceLifecycleDialCancellation(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	peerClosed := make(chan struct{})
	go func() {
		c, e := listener.Accept()
		if e != nil {
			return
		}
		defer c.Close()
		_, _ = io.Copy(io.Discard, c)
		close(peerClosed)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err = NewServiceManagerContext(ctx, listener.Addr().String(), "sysdba", "masterkey", GetDefaultServiceManagerOptions())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v", err)
	}
	select {
	case <-peerClosed:
	case <-time.After(time.Second):
		t.Fatal("handshake socket leaked")
	}
}

func TestServiceChunkDecoder(t *testing.T) {
	for _, tc := range []struct {
		buf                []byte
		end, pending, fail bool
	}{
		{nil, false, false, true}, {[]byte{62}, false, false, true}, {[]byte{62, 1, 0}, false, false, true},
		{[]byte{62, 0, 0}, false, false, true}, {[]byte{62, 0, 0, 1}, true, false, false},
		{[]byte{62, 0, 0, 64, 1}, false, true, false}, {[]byte{62, 0, 0, 4, 1}, false, true, false},
		{[]byte{62, 1, 0, 'x', 2, 1}, false, false, false}, {[]byte{62, 0, 0, 99}, false, false, true},
		{[]byte{2}, false, false, true}, {[]byte{1}, true, false, false},
	} {
		_, end, pending, err := serviceChunk(tc.buf, 62)
		if (err != nil) != tc.fail || end != tc.end || pending != tc.pending {
			t.Fatalf("%v: end=%v pending=%v err=%v", tc.buf, end, pending, err)
		}
	}
	// Unsigned lengths above 32 KiB must remain positive.
	b := append([]byte{62, 0, 128}, make([]byte, 32768)...)
	data, _, _, err := serviceChunk(b, 62)
	if err != nil || len(data) != 32768 {
		t.Fatalf("unsigned length: %d %v", len(data), err)
	}
}

func FuzzServiceChunk(f *testing.F) {
	f.Add([]byte{62, 0, 0, 1})
	f.Add([]byte{63, 1, 0, 'x', 2, 1})
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, b []byte) { _, _, _, _ = serviceChunk(b, 62); _, _, _, _ = serviceChunk(b, 63) })
}

func FuzzTraceReply(f *testing.F) {
	f.Add("Trace session ID 1 started")
	f.Add("Trace session ID 9999999999999999999999 started")
	f.Fuzz(func(t *testing.T, s string) {
		id, err := parseTraceReply(s, "started", 0)
		if err == nil && id <= 0 {
			t.Fatal("invalid ID")
		}
	})
}

func TestTraceLifecycleStoppedClose(t *testing.T) {
	stream := &fakeTraceService{reply: "Trace session ID 7 started"}
	ts, err := traceManagerFake(stream, nil).Start("config")
	if err != nil {
		t.Fatal(err)
	}
	aux := &fakeTraceService{reply: "Trace session ID 7 stopped"}
	ts.connBuilder = traceManagerFake(aux, nil).connBuilder
	if err = ts.Stop(); err != nil {
		t.Fatal(err)
	}
	if err = ts.Stop(); err != nil {
		t.Fatal(err)
	}
	if err = ts.Close(); err != nil {
		t.Fatal(err)
	}
	if stream.closes != 1 || aux.closes != 1 {
		t.Fatalf("close counts: stream=%d control=%d", stream.closes, aux.closes)
	}
}

func TestServiceLifecycleCloseDeadline(t *testing.T) {
	svc, server := servicePipe(t)
	go io.Copy(io.Discard, server)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	if err := svc.CloseContext(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v", err)
	}
	if err := svc.Close(); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("lost stored close error: %v", err)
	}
}

func TestServiceLifecycleIdleStatus(t *testing.T) {
	var f acceptFrame
	for _, status := range []byte{isc_info_svc_timeout, isc_info_data_not_ready} {
		f.opResponseFrame(0, []byte{isc_info_svc_line, 0, 0, status, isc_info_end})
	}
	f.opResponseFrame(0, []byte{isc_info_svc_line, 1, 0, 'x', isc_info_end})
	svc := &ServiceManager{wp: testProtocol(f.bytes())}
	line, end, err := svc.GetString()
	if err != nil || end || line != "x" {
		t.Fatalf("line=%q end=%v err=%v", line, end, err)
	}
}

func TestServiceLifecycleEmptyRequest(t *testing.T) {
	svc := &ServiceManager{wp: testProtocol(nil)}
	if _, err := svc.GetServiceInfo(nil, nil, 1024); err == nil {
		t.Fatal("empty request accepted")
	}
}

func TestServiceLifecycleOutputLimit(t *testing.T) {
	var f acceptFrame
	payload := []byte(strings.Repeat("x", 65530))
	chunk := append([]byte{isc_info_svc_line, byte(len(payload)), byte(len(payload) >> 8)}, payload...)
	chunk = append(chunk, isc_info_end)
	for n := 0; n < MaxServiceOutputBytes+len(payload); n += len(payload) {
		f.opResponseFrame(0, chunk)
	}
	svc := &ServiceManager{wp: testProtocol(f.bytes())}
	output, err := svc.WaitString()
	if err == nil || len(output) > MaxServiceOutputBytes || len(output) == 0 {
		t.Fatalf("len=%d err=%v", len(output), err)
	}
}
