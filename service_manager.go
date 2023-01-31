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

type SrvDbInfo struct {
	AttachmentsCount int32
	DatabaseCount    int32
	Databases        []string
}

type ServiceManagerOptions struct {
	WireCrypt  bool
	AuthPlugin string
}

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

func GetDefaultServiceManagerOptions() ServiceManagerOptions {
	return ServiceManagerOptions{
		WireCrypt:  true,
		AuthPlugin: "Srp256",
	}
}

func NewServiceManager(addr string, user string, password string, options ServiceManagerOptions) (*ServiceManager, error) {
	var err error
	var wp *wireProtocol

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
		spb := NewXPBWriterFromBuffer(GetServiceInfoSPBPreamble())
		spb.PutByte(isc_info_svc_timeout, 1)
		if buf, err = svc.GetServiceInfo(spb.GetBuffer(), []byte{isc_info_svc_to_eof}, bufferLength); err != nil {
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

func (svc *ServiceManager) GetServerVersion() (string, error) {
	return svc.GetServiceInfoString(isc_info_svc_server_version)
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

	return &SrvDbInfo{attachmentsCount, databasesCount, databases}, nil
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

	spb := NewXPBWriterFromBuffer([]byte{isc_action_svc_db_stats})
	spb.PutString(isc_spb_dbname, database)
	spb.PutInt32(isc_spb_options, optMask)

	if options.Tables != nil && len(options.Tables) > 0 {
		spb.PutString(isc_spb_command_line, strings.Join(options.Tables, " "))
	}

	return svc.ServiceStart(spb.GetBuffer())
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
