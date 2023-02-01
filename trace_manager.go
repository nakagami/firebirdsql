package firebirdsql

import (
	"fmt"
	"regexp"
	"strconv"
)

type TraceManager struct {
	connBuilder func() (*ServiceManager, error)
}

const (
	SessionStopped = iota
	SessionRunning
	SessionPaused
)

type TraceSession struct {
	connBuilder func() (*ServiceManager, error)
	conn        *ServiceManager
	id          int32
	state       int
}

func NewTraceManager(addr string, user string, password string, options ServiceManagerOptions) (*TraceManager, error) {
	connBuilder := func() (*ServiceManager, error) {
		return NewServiceManager(addr, user, password, options)
	}
	return &TraceManager{
		connBuilder: connBuilder,
	}, nil
}

func (t *TraceManager) Start(config string) (*TraceSession, error) {
	return t.StartWithName("", config)
}

func (t *TraceManager) StartWithName(name string, config string) (*TraceSession, error) {
	var (
		conn *ServiceManager
		id   int64
		err  error
	)
	if conn, err = t.connBuilder(); err != nil {
		return nil, err
	}

	var res string
	var spb = NewXPBWriterFromTag(isc_action_svc_trace_start)

	if len(name) > 0 {
		spb.PutString(isc_spb_trc_name, name)
	}

	spb.PutString(isc_spb_trc_cfg, config)

	if err = conn.ServiceStart(spb.Bytes()); err != nil {
		return nil, err
	}
	if res, _, err = conn.GetString(); err != nil {
		return nil, err
	}
	re := regexp.MustCompile(`Trace session ID (\d+) started`)
	match := re.FindStringSubmatch(res)
	if len(match) == 0 {
		_ = conn.Close()
		return nil, fmt.Errorf("unable to start trace session: %s", res)
	}
	if id, err = strconv.ParseInt(match[1], 10, 32); err != nil {
		return nil, err
	}

	return &TraceSession{
		connBuilder: t.connBuilder,
		conn:        conn,
		id:          int32(id),
		state:       SessionRunning,
	}, nil
}

func (t *TraceManager) List() (string, error) {
	var (
		err       error
		line, res string
		end       = false
		conn      *ServiceManager
	)
	if conn, err = t.connBuilder(); err != nil {
		return "", nil
	}
	defer func(conn *ServiceManager) {
		_ = conn.Close()
	}(conn)

	if err = conn.ServiceStart([]byte{isc_action_svc_trace_list}); err != nil {
		return "", err
	}

	for {
		if line, end, err = conn.GetString(); err != nil {
			return "", nil
		}
		if end {
			return res, nil
		}
		res += line + "\n"
	}
}

func (ts *TraceSession) Close() (err error) {
	if ts.state != SessionStopped {
		if err = ts.Stop(); err != nil {
			return err
		}
	}
	if err = ts.conn.Close(); err != nil {
		return err
	}
	ts.conn = nil
	return nil
}

func (ts *TraceSession) Stop() (err error) {
	if ts.state == SessionStopped {
		return fmt.Errorf("session already stopped")
	}
	var auxConn *ServiceManager
	if auxConn, err = ts.connBuilder(); err != nil {
		return
	}
	defer func(auxConn *ServiceManager) {
		_ = auxConn.Close()
	}(auxConn)

	var res string
	spb := NewXPBWriterFromTag(isc_action_svc_trace_stop)
	spb.PutInt32(isc_spb_trc_id, ts.id)

	if err = auxConn.ServiceStart(spb.Bytes()); err != nil {
		return err
	}
	if res, _, err = auxConn.GetString(); err != nil {
		return err
	}
	re := regexp.MustCompile(`Trace session ID (\d+) stopped`)
	match := re.FindStringSubmatch(res)
	if len(match) == 0 {
		return fmt.Errorf("unable to stop trace session: %s", res)
	}
	ts.state = SessionStopped
	return nil
}

func (ts *TraceSession) Pause() (err error) {
	if ts.state != SessionRunning {
		return fmt.Errorf("session not running")
	}

	var auxConn *ServiceManager
	if auxConn, err = ts.connBuilder(); err != nil {
		return
	}
	defer func(auxConn *ServiceManager) {
		_ = auxConn.Close()
	}(auxConn)

	var res string
	spb := NewXPBWriterFromTag(isc_action_svc_trace_suspend)
	spb.PutInt32(isc_spb_trc_id, ts.id)

	if err = auxConn.ServiceStart(spb.Bytes()); err != nil {
		return err
	}
	if res, _, err = auxConn.GetString(); err != nil {
		return err
	}
	re := regexp.MustCompile(`Trace session ID (\d+) paused`)
	match := re.FindStringSubmatch(res)
	if len(match) == 0 {
		return fmt.Errorf("unable to pause trace session: %s", res)
	}
	ts.state = SessionPaused
	return nil
}

func (ts *TraceSession) Resume() (err error) {
	if ts.state == SessionPaused {
		return fmt.Errorf("session not paused")
	}

	var auxConn *ServiceManager
	if auxConn, err = ts.connBuilder(); err != nil {
		return
	}
	defer func(auxConn *ServiceManager) {
		_ = auxConn.Close()
	}(auxConn)

	var res string
	spb := NewXPBWriterFromTag(isc_action_svc_trace_resume)
	spb.PutInt32(isc_spb_trc_id, ts.id)

	if err = auxConn.ServiceStart(spb.Bytes()); err != nil {
		return err
	}
	if res, _, err = auxConn.GetString(); err != nil {
		return err
	}
	re := regexp.MustCompile(`Trace session ID (\d+) resumed`)
	match := re.FindStringSubmatch(res)
	if len(match) == 0 {
		return fmt.Errorf("unable to resume trace session: %s", res)
	}
	ts.state = SessionRunning
	return nil
}

func (ts *TraceSession) Wait() (err error) {
	return ts.conn.Wait()
}

func (ts *TraceSession) WaitStrings(result chan string) (err error) {
	return ts.conn.WaitStrings(result)
}
