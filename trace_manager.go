package firebirdsql

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

// traceService keeps control connections separate from the streaming connection.
type traceService interface {
	ServiceStartContext(context.Context, []byte) error
	GetStringContext(context.Context) (string, bool, error)
	WaitStringContext(context.Context) (string, error)
	WaitStringsContext(context.Context, chan string) error
	WaitContext(context.Context) error
	CloseContext(context.Context) error
}

type TraceManager struct {
	connBuilder func(context.Context) (traceService, error)
}

const (
	SessionStopped = iota
	SessionRunning
	SessionPaused
	SessionStarting
	SessionStopping
	SessionFailed
)

// TraceSession owns a single stream. Control operations use separate service
// attachments, and may run concurrently with WaitStringsContext. A canceled
// stream cannot be resumed: Close the session and explicitly start a new one.
type TraceSession struct {
	connBuilder func(context.Context) (traceService, error)
	conn        traceService
	id          int32
	mu          sync.Mutex
	state       int
	control     chan struct{}
	closeOnce   sync.Once
	closeErr    error
	closed      bool
}

func NewTraceManager(addr, user, password string, options ServiceManagerOptions) (*TraceManager, error) {
	return &TraceManager{connBuilder: func(ctx context.Context) (traceService, error) {
		return NewServiceManagerContext(ctx, addr, user, password, options)
	}}, nil
}

func (t *TraceManager) Start(config string) (*TraceSession, error) {
	return t.StartContext(context.Background(), config)
}
func (t *TraceManager) StartContext(ctx context.Context, config string) (*TraceSession, error) {
	return t.StartWithNameContext(ctx, "", config)
}
func (t *TraceManager) StartWithName(name, config string) (*TraceSession, error) {
	return t.StartWithNameContext(context.Background(), name, config)
}

var traceReply = regexp.MustCompile(`^Trace session ID ([0-9]+) (started|stopped|paused|resumed)$`)

func parseTraceReply(reply, action string, expected int32) (int32, error) {
	match := traceReply.FindStringSubmatch(strings.TrimSpace(reply))
	if len(match) != 3 || match[2] != action {
		return 0, fmt.Errorf("firebirdsql: unexpected trace %s response", action)
	}
	id, err := strconv.ParseInt(match[1], 10, 32)
	if err != nil || id <= 0 || (expected != 0 && int32(id) != expected) {
		return 0, fmt.Errorf("firebirdsql: invalid trace session ID in %s response", action)
	}
	return int32(id), nil
}

func (t *TraceManager) StartWithNameContext(ctx context.Context, name, config string) (_ *TraceSession, err error) {
	// SPB strings use unsigned 16-bit lengths. Do not silently wrap them.
	if len(name) > 65535 || len(config) > 65535 {
		return nil, fmt.Errorf("firebirdsql: trace name or config exceeds 65535 bytes")
	}
	conn, err := t.connBuilder(ctx)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, conn.CloseContext(ctx))
		}
	}()
	spb := NewXPBWriterFromTag(isc_action_svc_trace_start)
	if name != "" {
		spb.PutString(isc_spb_trc_name, name)
	}
	spb.PutString(isc_spb_trc_cfg, config)
	if err = conn.ServiceStartContext(ctx, spb.Bytes()); err != nil {
		return nil, err
	}
	reply, _, err := conn.GetStringContext(ctx)
	if err != nil {
		return nil, err
	}
	id, err := parseTraceReply(reply, "started", 0)
	if err != nil {
		return nil, err
	}
	return &TraceSession{connBuilder: t.connBuilder, conn: conn, id: id, state: SessionRunning, control: make(chan struct{}, 1)}, nil
}

func (t *TraceManager) List() (string, error) { return t.ListContext(context.Background()) }
func (t *TraceManager) ListContext(ctx context.Context) (result string, err error) {
	conn, err := t.connBuilder(ctx)
	if err != nil {
		return "", err
	}
	defer func() { err = errors.Join(err, conn.CloseContext(ctx)) }()
	if err = conn.ServiceStartContext(ctx, []byte{isc_action_svc_trace_list}); err != nil {
		return "", err
	}
	return conn.WaitStringContext(ctx)
}

// ID is the server's Trace session ID, not a database attachment or SQL handle.
func (ts *TraceSession) ID() int32  { return ts.id }
func (ts *TraceSession) State() int { ts.mu.Lock(); defer ts.mu.Unlock(); return ts.state }

func (ts *TraceSession) acquire(ctx context.Context) error {
	select {
	case ts.control <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (ts *TraceSession) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return ts.CloseContext(ctx)
}

// CloseContext always releases the stream, even when stopping the server-side
// session fails. Repeated calls return the same combined stop/close error.
func (ts *TraceSession) CloseContext(ctx context.Context) error {
	ts.closeOnce.Do(func() {
		ts.mu.Lock()
		ts.closed = true
		ts.mu.Unlock()
		if err := ts.acquire(ctx); err != nil {
			ts.closeErr = err
		} else {
			ts.closeErr = ts.change(ctx, isc_action_svc_trace_stop, "stopped", SessionStopped)
			<-ts.control
		}
		ts.closeErr = errors.Join(ts.closeErr, ts.conn.CloseContext(ctx))
		ts.mu.Lock()
		if ts.closeErr != nil {
			ts.state = SessionFailed
		}
		ts.mu.Unlock()
	})
	return ts.closeErr
}

func (ts *TraceSession) Stop() error { return ts.StopContext(context.Background()) }
func (ts *TraceSession) StopContext(ctx context.Context) error {
	return ts.command(ctx, isc_action_svc_trace_stop, "stopped", SessionStopped)
}
func (ts *TraceSession) Pause() error { return ts.PauseContext(context.Background()) }
func (ts *TraceSession) PauseContext(ctx context.Context) error {
	return ts.command(ctx, isc_action_svc_trace_suspend, "paused", SessionPaused)
}
func (ts *TraceSession) Resume() error { return ts.ResumeContext(context.Background()) }
func (ts *TraceSession) ResumeContext(ctx context.Context) error {
	return ts.command(ctx, isc_action_svc_trace_resume, "resumed", SessionRunning)
}

func (ts *TraceSession) command(ctx context.Context, action byte, reply string, next int) error {
	if err := ts.acquire(ctx); err != nil {
		return err
	}
	defer func() { <-ts.control }()
	ts.mu.Lock()
	closed := ts.closed
	ts.mu.Unlock()
	if closed {
		return fmt.Errorf("firebirdsql: trace session closed")
	}
	return ts.change(ctx, action, reply, next)
}

func (ts *TraceSession) change(ctx context.Context, action byte, reply string, next int) (err error) {
	ts.mu.Lock()
	old := ts.state
	if old == SessionStopped && next == SessionStopped {
		ts.mu.Unlock()
		return nil
	}
	if (next == SessionPaused && old != SessionRunning) || (next == SessionRunning && old != SessionPaused) {
		ts.mu.Unlock()
		return fmt.Errorf("firebirdsql: invalid trace session state %d", old)
	}
	if next == SessionStopped {
		ts.state = SessionStopping
	}
	ts.mu.Unlock()
	defer func() {
		ts.mu.Lock()
		defer ts.mu.Unlock()
		if err != nil {
			ts.state = SessionFailed
		} else {
			ts.state = next
		}
	}()
	conn, err := ts.connBuilder(ctx)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, conn.CloseContext(ctx)) }()
	spb := NewXPBWriterFromTag(action)
	spb.PutInt32(isc_spb_trc_id, ts.id)
	if err = conn.ServiceStartContext(ctx, spb.Bytes()); err != nil {
		return err
	}
	result, _, err := conn.GetStringContext(ctx)
	if err != nil {
		return err
	}
	_, err = parseTraceReply(result, reply, ts.id)
	return err
}

func (ts *TraceSession) Wait() error                           { return ts.WaitContext(context.Background()) }
func (ts *TraceSession) WaitContext(ctx context.Context) error { return ts.conn.WaitContext(ctx) }
func (ts *TraceSession) WaitStrings(result chan string) error {
	return ts.WaitStringsContext(context.Background(), result)
}

// WaitStringsContext does not close result. There is at most one reader per
// session; another reader returns ErrServiceBusy. Raw Trace may contain SQL,
// arguments and results: sanitize before storing or exporting it.
func (ts *TraceSession) WaitStringsContext(ctx context.Context, result chan string) error {
	err := ts.conn.WaitStringsContext(ctx, result)
	if err != nil && !errors.Is(err, ErrServiceBusy) {
		ts.mu.Lock()
		if ts.state != SessionStopped && ts.state != SessionStopping {
			ts.state = SessionFailed
		}
		ts.mu.Unlock()
	}
	return err
}
