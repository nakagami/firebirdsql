/*******************************************************************************
The MIT License (MIT)

Copyright (c) 2023-2024 Artyom Smirnov <artyom_smirnov@me.com>

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
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type ServiceManager struct {
	wp        *wireProtocol
	handle    int32
	mu        sync.Mutex
	initOnce  sync.Once
	done      chan struct{}
	closeOnce sync.Once
	closeErr  error
	unusable  atomic.Bool
}

type StatisticsOptions struct {
	UserDataPages             bool
	UserIndexPages            bool
	OnlyHeaderPages           bool
	SystemRelationsAndIndexes bool
	RecordVersions            bool
	Tables                    []string
}

type StatisticsOption func(*StatisticsOptions)

type SrvDbInfo struct {
	AttachmentsCount int
	DatabaseCount    int
	Databases        []string
}

type ServiceManagerOptions struct {
	WireCrypt  bool
	AuthPlugin string
}

type ServiceManagerOption func(*ServiceManagerOptions)

func GetServiceInfoSPBPreamble() []byte {
	return []byte{isc_spb_version, isc_spb_current_version}
}

func GetDefaultStatisticsOptions() StatisticsOptions {
	return StatisticsOptions{
		UserDataPages:             true,
		UserIndexPages:            true,
		OnlyHeaderPages:           false,
		SystemRelationsAndIndexes: false,
		RecordVersions:            false,
		Tables:                    []string{},
	}
}

func WithUserDataPages() StatisticsOption {
	return func(opts *StatisticsOptions) {
		opts.UserDataPages = true
	}
}

func WithoutUserDataPages() StatisticsOption {
	return func(opts *StatisticsOptions) {
		opts.UserDataPages = false
	}
}

func WithUserIndexPages() StatisticsOption {
	return func(opts *StatisticsOptions) {
		opts.UserIndexPages = true
	}
}

func WithoutIndexPages() StatisticsOption {
	return func(opts *StatisticsOptions) {
		opts.UserIndexPages = false
	}
}

func WithOnlyHeaderPages() StatisticsOption {
	return func(opts *StatisticsOptions) {
		opts.OnlyHeaderPages = true
	}
}

func WithoutOnlyHeaderPages() StatisticsOption {
	return func(opts *StatisticsOptions) {
		opts.OnlyHeaderPages = false
	}
}

func WithSystemRelationsAndIndexes() StatisticsOption {
	return func(opts *StatisticsOptions) {
		opts.SystemRelationsAndIndexes = true
	}
}

func WithoutSystemRelationsAndIndexes() StatisticsOption {
	return func(opts *StatisticsOptions) {
		opts.SystemRelationsAndIndexes = false
	}
}

func WithRecordVersions() StatisticsOption {
	return func(opts *StatisticsOptions) {
		opts.RecordVersions = true
	}
}

func WithoutRecordVersions() StatisticsOption {
	return func(opts *StatisticsOptions) {
		opts.RecordVersions = false
	}
}

func WithTables(tables []string) StatisticsOption {
	return func(opts *StatisticsOptions) {
		opts.Tables = tables
	}
}

func NewStatisticsOptions(opts ...StatisticsOption) StatisticsOptions {
	res := GetDefaultStatisticsOptions()
	for _, opt := range opts {
		opt(&res)
	}
	return res
}

func GetDefaultServiceManagerOptions() ServiceManagerOptions {
	return ServiceManagerOptions{
		WireCrypt:  true,
		AuthPlugin: "Srp256",
	}
}

func WithWireCrypt() ServiceManagerOption {
	return func(opts *ServiceManagerOptions) {
		opts.WireCrypt = true
	}
}

func WithoutWireCrypt() ServiceManagerOption {
	return func(opts *ServiceManagerOptions) {
		opts.WireCrypt = false
	}
}

func WithAuthPlugin(authPlugin string) ServiceManagerOption {
	return func(opts *ServiceManagerOptions) {
		opts.AuthPlugin = authPlugin
	}
}

func NewServiceManagerOptions(opts ...ServiceManagerOption) ServiceManagerOptions {
	res := GetDefaultServiceManagerOptions()
	for _, opt := range opts {
		opt(&res)
	}
	return res
}

func (sm ServiceManagerOptions) WithoutWireCrypt() ServiceManagerOptions {
	sm.WireCrypt = false
	return sm
}

func (sm ServiceManagerOptions) WithWireCrypt() ServiceManagerOptions {
	sm.WireCrypt = true
	return sm
}

func (sm ServiceManagerOptions) WithAuthPlugin(authPlugin string) ServiceManagerOptions {
	sm.AuthPlugin = authPlugin
	return sm
}

func NewServiceManager(addr string, user string, password string, options ServiceManagerOptions) (*ServiceManager, error) {
	return NewServiceManagerContext(context.Background(), addr, user, password, options)
}

// NewServiceManagerContext bounds dialing, authentication and service attachment.
func NewServiceManagerContext(ctx context.Context, addr string, user string, password string, options ServiceManagerOptions) (_ *ServiceManager, err error) {
	var wp *wireProtocol
	if !strings.ContainsRune(addr, ':') {
		addr += ":3050"
	}

	wireCryptStr := "false"
	if options.WireCrypt {
		wireCryptStr = "true"
	}

	var connOptions = map[string]string{
		"auth_plugin_name": options.AuthPlugin,
		// Seed the auth-plugin allow-list with the supported plugins so the
		// server-selected plugin is enforced on the admin path too (the DSN path
		// defaults this in dsn.go). Without it the allow-list parses empty and
		// _parse_connect_response rejects every server plugin.
		"auth_plugin_list": defaultAuthPlugins,
		"wire_crypt":       wireCryptStr,
		// Seed the cipher allow-list with the default ciphers. Without this the
		// allow-list parses empty, _guess_wire_crypt negotiates no cipher, and a
		// WireCrypt=true admin connection silently downgrades to plaintext (the
		// DSN path defaults this in dsn.go; the admin path must too).
		"wire_crypt_plugin": defaultWireCryptPlugins,
	}

	// Fail fast on an invalid auth-plugin configuration before dialing, matching
	// the DSN path (dsn.go).
	if err = validateAuthPlugins(connOptions["auth_plugin_name"], connOptions["auth_plugin_list"]); err != nil {
		return nil, err
	}

	if wp, err = newWireProtocolContext(ctx, addr, "", ""); err != nil {
		return nil, err
	}
	manager := &ServiceManager{wp: wp}
	defer func() {
		if err != nil {
			_ = wp.conn.Close()
		}
	}()
	err = manager.withContext(ctx, func() error {
		clientPublic, clientSecret, e := getClientSeed()
		if e != nil {
			return e
		}
		if e = wp.opConnect("", user, password, connOptions, clientPublic); e != nil {
			return e
		}
		if e = wp._parse_connect_response(user, password, connOptions, clientPublic, clientSecret); e != nil {
			return e
		}
		if e = wp.opServiceAttach(); e != nil {
			return e
		}
		wp.dbHandle, _, _, e = wp.opResponse()
		return e
	})
	if err != nil {
		return nil, err
	}
	return manager, nil
}

// WireCipher returns the name of the wire-encryption cipher negotiated for this
// service-manager connection ("ChaCha64", "ChaCha", or "Arc4"), or an empty
// string when the connection is unencrypted (plaintext). It mirrors
// firebirdsqlConn.WireCipher and lets callers verify that an admin channel
// (backup/restore/user management) is actually encrypted.
func (svc *ServiceManager) WireCipher() string {
	return svc.wp.conn.plugin
}

// ErrServiceBusy means another operation owns this service response stream.
var ErrServiceBusy = errors.New("firebirdsql: service operation already in progress")

func (svc *ServiceManager) init() { svc.initOnce.Do(func() { svc.done = make(chan struct{}) }) }

// withContext owns the protocol until its response reader has returned. Cancellation
// closes only the socket (net.Conn supports concurrent Close), never sends detach
// concurrently with a reader, and permanently abandons this service connection.
func (svc *ServiceManager) withContext(ctx context.Context, fn func() error) error {
	svc.init()
	if err := ctx.Err(); err != nil {
		return err
	}
	if !svc.mu.TryLock() {
		return ErrServiceBusy
	}
	defer svc.mu.Unlock()
	select {
	case <-svc.done:
		return net.ErrClosed
	default:
	}
	if svc.unusable.Load() {
		return net.ErrClosed
	}
	stopped := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		svc.unusable.Store(true)
		_ = svc.wp.conn.Close()
		close(stopped)
	})
	err := fn()
	if !stop() {
		<-stopped
	}
	if ctx.Err() != nil {
		svc.unusable.Store(true)
		_ = svc.wp.conn.Close()
		return ctx.Err()
	}
	return err
}

// Close is idempotent and interrupts a blocked read or channel delivery. If a
// reader owns the wire, only the transport is closed; no detach packet is sent.
func (svc *ServiceManager) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return svc.CloseContext(ctx)
}

// CloseContext bounds graceful detach. An expired context still closes the
// transport immediately. The first Close call determines the stored result.
func (svc *ServiceManager) CloseContext(ctx context.Context) error {
	svc.init()
	svc.closeOnce.Do(func() {
		close(svc.done)
		closeSocket := func() error {
			err := svc.wp.conn.Close()
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		if !svc.mu.TryLock() {
			svc.closeErr = closeSocket()
			svc.mu.Lock()
		} else {
			if !svc.unusable.Load() && ctx.Err() == nil {
				stopped := make(chan struct{})
				stop := context.AfterFunc(ctx, func() { _ = svc.wp.conn.Close(); close(stopped) })
				svc.closeErr = svc.wp.opServiceDetach()
				if svc.closeErr == nil {
					_, _, _, svc.closeErr = svc.wp.opResponse()
				}
				if !stop() {
					<-stopped
				}
				if ctx.Err() != nil {
					svc.closeErr = ctx.Err()
				}
			}
			svc.closeErr = errors.Join(svc.closeErr, closeSocket())
		}
		svc.mu.Unlock()
	})
	return svc.closeErr
}

func (svc *ServiceManager) ServiceStart(spb []byte) error {
	return svc.ServiceStartContext(context.Background(), spb)
}

func (svc *ServiceManager) ServiceStartContext(ctx context.Context, spb []byte) error {
	return svc.withContext(ctx, func() error {
		if err := svc.wp.opServiceStart(spb); err != nil {
			return err
		}
		_, _, _, err := svc.wp.opResponse()
		return err
	})
}

func (svc *ServiceManager) ServiceAttach(spb []byte, verbose chan string) error {
	if err := svc.ServiceStart(spb); err != nil {
		return err
	}
	if verbose != nil {
		return svc.WaitStrings(verbose)
	} else {
		return svc.Wait()
	}
}

func (svc *ServiceManager) ServiceAttachBuffer(spb []byte, verbose chan []byte) error {
	if err := svc.ServiceStart(spb); err != nil {
		return err
	}
	return svc.WaitBuffer(verbose)
}

func (svc *ServiceManager) IsRunning() (bool, error) {
	res, err := svc.GetServiceInfoInt(isc_info_svc_running)
	return res > 0, err
}

func (svc *ServiceManager) Wait() error { return svc.WaitContext(context.Background()) }

func (svc *ServiceManager) WaitContext(ctx context.Context) error {
	return svc.withContext(ctx, func() error {
		for {
			buf, err := svc.getServiceInfo(GetServiceInfoSPBPreamble(), []byte{isc_info_svc_running}, BUFFER_LEN)
			if err != nil {
				return err
			}
			rdr := NewXPBReader(buf[1:])
			running := rdr.GetInt16()
			if rdr.Err() != nil {
				return rdr.Err()
			}
			if running == 0 {
				return nil
			}
			if err := svc.waitPoll(ctx); err != nil {
				return err
			}
		}
	})
}

func (svc *ServiceManager) waitPoll(ctx context.Context) error {
	timer := time.NewTimer(10 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-svc.done:
		return net.ErrClosed
	case <-timer.C:
		return nil
	}
}

// serviceChunk validates the unsigned length before exposing any service output.
// A nonempty payload without a trailer is accepted for legacy binary services.
func serviceChunk(buf []byte, item byte) (data []byte, end, pending bool, err error) {
	if len(buf) == 0 {
		return nil, false, false, fmt.Errorf("firebirdsql: empty service response")
	}
	if buf[0] == isc_info_end {
		return nil, true, false, nil
	}
	if buf[0] == isc_info_truncated {
		return nil, false, false, fmt.Errorf("firebirdsql: service response truncated")
	}
	if buf[0] != item || len(buf) < 3 {
		return nil, false, false, fmt.Errorf("firebirdsql: invalid service response")
	}
	n := int(bytes_to_uint16(buf[1:3]))
	if n > len(buf)-3 {
		return nil, false, false, fmt.Errorf("firebirdsql: invalid service payload length %d", n)
	}
	data = buf[3 : 3+n]
	tail := buf[3+n:]
	if len(tail) == 0 {
		if n != 0 {
			return data, false, false, nil
		}
		return nil, false, false, fmt.Errorf("firebirdsql: missing service status")
	}
	switch tail[0] {
	case isc_info_end:
		return data, n == 0, false, nil
	case isc_info_svc_timeout, isc_info_data_not_ready:
		return data, false, n == 0, nil
	case isc_info_truncated:
		return data, false, n == 0, nil
	default:
		return nil, false, false, fmt.Errorf("firebirdsql: unexpected service status %d", tail[0])
	}
}

func (svc *ServiceManager) WaitBuffer(stream chan []byte) error {
	return svc.WaitBufferContext(context.Background(), stream)
}

// WaitBufferContext delivers bounded chunks without closing the caller's channel.
// Cancellation abandons this service connection; it must not be reused.
func (svc *ServiceManager) WaitBufferContext(ctx context.Context, stream chan []byte) error {
	return svc.withContext(ctx, func() error {
		return svc.stream(ctx, isc_info_svc_to_eof, func(data []byte) error {
			select {
			case stream <- data:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			case <-svc.done:
				return net.ErrClosed
			}
		})
	})
}

func (svc *ServiceManager) stream(ctx context.Context, item byte, consume func([]byte) error) error {
	for {
		data, end, pending, err := svc.readChunk(item)
		if err != nil {
			return err
		}
		if len(data) != 0 {
			if err = consume(data); err != nil {
				return err
			}
		}
		if end {
			return nil
		}
		if pending {
			if err = svc.waitPoll(ctx); err != nil {
				return err
			}
		}
		if err = ctx.Err(); err != nil {
			return err
		}
	}
}

func (svc *ServiceManager) readChunk(item byte) ([]byte, bool, bool, error) {
	// Services API timeout is a length-prefixed integer, not an SPB boolean.
	spb := []byte{isc_info_svc_timeout, 4, 0, 1, 0, 0, 0}
	buf, err := svc.getServiceInfo(spb, []byte{item}, BUFFER_LEN)
	if err != nil {
		return nil, false, false, err
	}
	return serviceChunk(buf, item)
}

func (svc *ServiceManager) WaitStrings(result chan string) error {
	return svc.WaitStringsContext(context.Background(), result)
}

// WaitStringsContext keeps the legacy channel ownership contract: the caller
// closes result. Cancellation also interrupts delivery to an unread channel.
func (svc *ServiceManager) WaitStringsContext(ctx context.Context, result chan string) error {
	return svc.withContext(ctx, func() error {
		return svc.stream(ctx, isc_info_svc_line, func(data []byte) error {
			select {
			case result <- string(data):
				return nil
			case <-ctx.Done():
				return ctx.Err()
			case <-svc.done:
				return net.ErrClosed
			}
		})
	})
}

func (svc *ServiceManager) WaitString() (string, error) {
	return svc.WaitStringContext(context.Background())
}

// MaxServiceOutputBytes bounds convenience methods that collect a whole service
// response. For larger output use WaitStringsContext or WaitBufferContext.
const MaxServiceOutputBytes = 16 << 20

func (svc *ServiceManager) WaitStringContext(ctx context.Context) (string, error) {
	var result strings.Builder
	err := svc.withContext(ctx, func() error {
		return svc.stream(ctx, isc_info_svc_line, func(data []byte) error {
			if result.Len()+len(data)+1 > MaxServiceOutputBytes {
				return fmt.Errorf("firebirdsql: service output exceeds %d bytes", MaxServiceOutputBytes)
			}
			result.Write(data)
			result.WriteByte('\n')
			return nil
		})
	})
	return result.String(), err
}

func (svc *ServiceManager) GetString() (string, bool, error) {
	return svc.GetStringContext(context.Background())
}

func (svc *ServiceManager) GetStringContext(ctx context.Context) (result string, end bool, err error) {
	err = svc.withContext(ctx, func() error {
		for {
			data, finished, pending, e := svc.readChunk(isc_info_svc_line)
			if e != nil {
				return e
			}
			if !pending {
				result, end = string(data), finished
				return nil
			}
			if e = svc.waitPoll(ctx); e != nil {
				return e
			}
		}
	})
	return
}

func (svc *ServiceManager) GetServiceInfo(spb []byte, srb []byte, bufferLength int32) (buf []byte, err error) {
	err = svc.withContext(context.Background(), func() error {
		var e error
		buf, e = svc.getServiceInfo(spb, srb, bufferLength)
		return e
	})
	return
}

func (svc *ServiceManager) getServiceInfo(spb []byte, srb []byte, bufferLength int32) ([]byte, error) {
	if len(srb) == 0 {
		return nil, fmt.Errorf("firebirdsql: empty service request")
	}
	if err := svc.wp.opServiceInfo(spb, srb, bufferLength); err != nil {
		return nil, err
	}
	_, _, buf, err := svc.wp.opResponse()
	if err != nil {
		return nil, err
	}
	if len(buf) == 0 {
		return nil, fmt.Errorf("response buffer is empty")
	}
	if buf[0] != srb[0] && buf[0] != isc_info_end && buf[0] != isc_info_truncated {
		return nil, fmt.Errorf("wrong item '%d' response buffer", buf[0])
	}
	return buf, nil
}

func (svc *ServiceManager) GetServiceInfoInt(item byte) (int16, error) {
	var buf []byte
	var err error
	if buf, err = svc.GetServiceInfo(GetServiceInfoSPBPreamble(), []byte{item}, BUFFER_LEN); err != nil {
		return 0, err
	}
	rdr := NewXPBReader(buf[1:])
	return rdr.GetInt16(), rdr.Err()
}

func (svc *ServiceManager) GetServiceInfoString(item byte) (string, error) {
	var buf []byte
	var err error
	if buf, err = svc.GetServiceInfo(GetServiceInfoSPBPreamble(), []byte{item}, -1); err != nil {
		return "", err
	}
	rdr := NewXPBReader(buf[1:])
	return rdr.GetString(), rdr.Err()
}

func (svc *ServiceManager) GetServerVersionString() (string, error) {
	return svc.GetServiceInfoString(isc_info_svc_server_version)
}

func (svc *ServiceManager) GetServerVersion() (FirebirdVersion, error) {
	ver, err := svc.GetServerVersionString()
	if err != nil {
		return FirebirdVersion{}, err
	}
	parsed := ParseFirebirdVersion(ver)
	if parsed.Full == "" {
		// Turn an unrecognized banner into an error rather than silently returning a
		// zero version (see ParseFirebirdVersion) that would flip feature gates off.
		return parsed, fmt.Errorf("firebirdsql: unrecognized server version banner %q", ver)
	}
	return parsed, nil
}

func (svc *ServiceManager) GetArchitecture() (string, error) {
	return svc.GetServiceInfoString(isc_info_svc_implementation)
}

func (svc *ServiceManager) GetHomeDir() (string, error) {
	return svc.GetServiceInfoString(isc_info_svc_get_env)
}

func (svc *ServiceManager) GetSecurityDatabasePath() (string, error) {
	return svc.GetServiceInfoString(isc_info_svc_user_dbpath)
}

func (svc *ServiceManager) GetLockFileDir() (string, error) {
	return svc.GetServiceInfoString(isc_info_svc_get_env_lock)
}

func (svc *ServiceManager) GetMsgFileDir() (string, error) {
	return svc.GetServiceInfoString(isc_info_svc_get_env_msg)
}

func (svc *ServiceManager) GetSvrDbInfo() (*SrvDbInfo, error) {
	var buf []byte
	var err error

	if buf, err = svc.GetServiceInfo(GetServiceInfoSPBPreamble(), []byte{isc_info_svc_svr_db_info}, -1); err != nil {
		return &SrvDbInfo{}, err
	}

	var attachmentsCount int32 = 0
	var databasesCount int32 = 0
	var databases []string

	srb := NewXPBReader(buf)
	have, val := srb.Next()
	for ; have && val != isc_info_flag_end; have, val = srb.Next() {
		switch val {
		case isc_spb_num_att:
			attachmentsCount = srb.GetInt32()
		case isc_spb_num_db:
			databasesCount = srb.GetInt32()
		case isc_spb_dbname:
			databases = append(databases, srb.GetString())
		}
	}
	if err := srb.Err(); err != nil {
		return &SrvDbInfo{}, err
	}

	return &SrvDbInfo{int(attachmentsCount), int(databasesCount), databases}, nil
}

func (svc *ServiceManager) doGetFbLog() error {
	return svc.ServiceStart([]byte{isc_action_svc_get_fb_log})
}

func (svc *ServiceManager) GetFbLog(result chan string) error {
	if err := svc.doGetFbLog(); err != nil {
		return err
	}
	return svc.WaitStrings(result)
}

func (svc *ServiceManager) GetFbLogString() (string, error) {
	if err := svc.doGetFbLog(); err != nil {
		return "", err
	}
	return svc.WaitString()
}

func (svc *ServiceManager) doGetDbStats(database string, options StatisticsOptions) error {
	var optMask int32
	if options.OnlyHeaderPages {
		options.UserDataPages = false
		options.UserIndexPages = false
		options.SystemRelationsAndIndexes = false
		options.RecordVersions = false
	}

	if options.UserDataPages {
		optMask |= isc_spb_sts_data_pages
	}
	if options.OnlyHeaderPages {
		optMask |= isc_spb_sts_hdr_pages
	}
	if options.UserIndexPages {
		optMask |= isc_spb_sts_idx_pages
	}
	if options.SystemRelationsAndIndexes {
		optMask |= isc_spb_sts_sys_relations
	}
	if options.RecordVersions {
		optMask |= isc_spb_sts_record_versions
	}
	if options.Tables != nil && len(options.Tables) > 0 {
		optMask |= isc_spb_sts_table
	}

	spb := NewXPBWriterFromTag(isc_action_svc_db_stats)
	spb.PutString(isc_spb_dbname, database)
	spb.PutInt32(isc_spb_options, optMask)

	if options.Tables != nil && len(options.Tables) > 0 {
		spb.PutString(isc_spb_command_line, strings.Join(options.Tables, " "))
	}

	return svc.ServiceStart(spb.Bytes())
}

func (svc *ServiceManager) GetDbStats(database string, options StatisticsOptions, result chan string) error {
	if err := svc.doGetDbStats(database, options); err != nil {
		return err
	}
	return svc.WaitStrings(result)
}

func (svc *ServiceManager) GetDbStatsString(database string, options StatisticsOptions) (string, error) {
	if err := svc.doGetDbStats(database, options); err != nil {
		return "", err
	}
	return svc.WaitString()
}

func serviceAttach(connBuilder func() (*ServiceManager, error), spb []byte, verbose chan string) error {
	conn, err := connBuilder()
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	return conn.ServiceAttach(spb, verbose)
}

func serviceAttachBuffer(connBuilder func() (*ServiceManager, error), spb []byte, verbose chan []byte) error {
	conn, err := connBuilder()
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	return conn.ServiceAttachBuffer(spb, verbose)
}
