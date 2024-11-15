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
	"bytes"
	"fmt"
	"strings"
	"time"
)

type ServiceManager struct {
	wp     *wireProtocol
	handle int32
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
	var err error
	var wp *wireProtocol
	if !strings.ContainsRune(addr, ':') {
		addr += ":3050"
	}
	if wp, err = newWireProtocol(addr, "", ""); err != nil {
		return nil, err
	}

	wireCryptStr := "false"
	if options.WireCrypt {
		wireCryptStr = "true"
	}

	var connOptions = map[string]string{
		"auth_plugin_name": options.AuthPlugin,
		"wire_crypt":       wireCryptStr,
	}

	clientPublic, clientSecret := getClientSeed()
	if err = wp.opConnect("", user, password, connOptions, clientPublic); err != nil {
		return nil, err
	}

	if err = wp._parse_connect_response(user, password, connOptions, clientPublic, clientSecret); err != nil {
		return nil, err
	}

	if err = wp.opServiceAttach(); err != nil {
		return nil, err
	}

	if wp.dbHandle, _, _, err = wp.opResponse(); err != nil {
		return nil, err
	}

	manager := &ServiceManager{
		wp: wp,
	}
	return manager, nil
}

func (svc *ServiceManager) Close() (err error) {
	if err = svc.wp.opServiceDetach(); err != nil {
		svc.wp.conn.Close()
		return err
	}

	if _, _, _, err = svc.wp.opResponse(); err != nil {
		svc.wp.conn.Close()
		return err
	}

	return svc.wp.conn.Close()
}

func (svc *ServiceManager) ServiceStart(spb []byte) error {
	var err error
	if err = svc.wp.opServiceStart(spb); err != nil {
		return err
	}
	_, _, _, err = svc.wp.opResponse()
	return err
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

func (svc *ServiceManager) Wait() error {
	var (
		err     error
		running bool
	)
	for {
		if running, err = svc.IsRunning(); err != nil {
			return err
		}
		if !running {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	return nil
}

func (svc *ServiceManager) WaitBuffer(stream chan []byte) error {
	var (
		err          error
		buf          []byte
		cont               = true
		bufferLength int32 = BUFFER_LEN
	)
	for cont {
		spb := NewXPBWriterFromBytes(GetServiceInfoSPBPreamble())
		spb.PutByte(isc_info_svc_timeout, 1)
		if buf, err = svc.GetServiceInfo(spb.Bytes(), []byte{isc_info_svc_to_eof}, bufferLength); err != nil {
			return err
		}
		switch buf[0] {
		case isc_info_svc_to_eof:
			dataLen := bytes_to_int16(buf[1:3])
			if dataLen == 0 {
				if buf[3] == isc_info_svc_timeout {
					break
				} else if buf[3] != isc_info_end {
					return fmt.Errorf("unexpected end of stream")
				} else {
					cont = false
					break
				}
			}
			stream <- buf[3 : 3+dataLen]
		case isc_info_truncated:
			bufferLength *= 2
		case isc_info_end:
			cont = false
		}
	}
	return nil
}

func (svc *ServiceManager) WaitStrings(result chan string) error {
	var (
		err  error
		line string
		end  = false
	)

	for {
		if line, end, err = svc.GetString(); err != nil {
			return err
		}
		if end {
			return nil
		}
		result <- line
	}
}

func (svc *ServiceManager) WaitString() (string, error) {
	part := make(chan string)
	done := make(chan bool)
	var err error
	var result string

	go func() {
		err = svc.WaitStrings(part)
		done <- true
	}()

	var s string
	for cont := true; cont; {
		select {
		case s = <-part:
			result += s + "\n"
		case <-done:
			cont = false
			break
		default:
		}
	}

	if err != nil {
		return "", err
	}

	return result, nil
}

func (svc *ServiceManager) GetString() (result string, end bool, err error) {
	var buf []byte
	if buf, err = svc.GetServiceInfo(GetServiceInfoSPBPreamble(), []byte{isc_info_svc_line}, -1); err != nil {
		return "", false, nil
	}
	if bytes.Compare(buf[:4], []byte{isc_info_svc_line, 0, 0, isc_info_end}) == 0 {
		return "", true, nil
	}

	return NewXPBReader(buf[1:]).GetString(), false, nil
}

func (svc *ServiceManager) GetServiceInfo(spb []byte, srb []byte, bufferLength int32) ([]byte, error) {
	var buf []byte
	var err error

	if err = svc.wp.opServiceInfo(spb, srb, bufferLength); err != nil {
		return nil, err
	}

	if _, _, buf, err = svc.wp.opResponse(); err != nil {
		return nil, err
	}

	if len(buf) == 0 {
		return nil, fmt.Errorf("response buffer is empty")
	}

	if buf[0] != srb[0] {
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
	return NewXPBReader(buf[1:]).GetInt16(), nil
}

func (svc *ServiceManager) GetServiceInfoString(item byte) (string, error) {
	var buf []byte
	var err error
	if buf, err = svc.GetServiceInfo(GetServiceInfoSPBPreamble(), []byte{item}, -1); err != nil {
		return "", err
	}
	return NewXPBReader(buf[1:]).GetString(), nil
}

func (svc *ServiceManager) GetServerVersionString() (string, error) {
	return svc.GetServiceInfoString(isc_info_svc_server_version)
}

func (svc *ServiceManager) GetServerVersion() (FirebirdVersion, error) {
	if ver, err := svc.GetServerVersionString(); err == nil {
		return ParseFirebirdVersion(ver), nil
	} else {
		return FirebirdVersion{}, err
	}
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
