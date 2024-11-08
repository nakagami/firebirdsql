package firebirdsql

type MaintenanceManager struct {
	connBuilder func() (*ServiceManager, error)
}

func NewMaintenanceManager(addr string, user string, password string, options ServiceManagerOptions) (*MaintenanceManager, error) {
	connBuilder := func() (*ServiceManager, error) {
		return NewServiceManager(addr, user, password, options)
	}
	return &MaintenanceManager{
		connBuilder: connBuilder,
	}, nil
}

func (mm *MaintenanceManager) setAccessMode(database string, mode byte) error {
	spb := NewXPBWriterFromTag(isc_action_svc_properties)
	spb.PutString(isc_spb_dbname, database)
	spb.PutByte(isc_spb_prp_access_mode, mode)
	return mm.attach(spb.Bytes(), nil)
}

func (mm *MaintenanceManager) SetAccessModeReadWrite(database string) error {
	return mm.setAccessMode(database, isc_spb_prp_am_readwrite)
}

func (mm *MaintenanceManager) SetAccessModeReadOnly(database string) error {
	return mm.setAccessMode(database, isc_spb_prp_am_readonly)
}

func (mm *MaintenanceManager) SetDialect(database string, dialect int) error {
	spb := NewXPBWriterFromTag(isc_action_svc_properties)
	spb.PutString(isc_spb_dbname, database)
	spb.PutInt32(isc_spb_prp_set_sql_dialect, int32(dialect))
	return mm.attach(spb.Bytes(), nil)
}

func (mm *MaintenanceManager) SetPageBuffers(database string, pageCount int) error {
	spb := NewXPBWriterFromTag(isc_action_svc_properties)
	spb.PutString(isc_spb_dbname, database)
	spb.PutInt32(isc_spb_prp_page_buffers, int32(pageCount))
	return mm.attach(spb.Bytes(), nil)
}

func (mm *MaintenanceManager) setWriteMode(database string, mode byte) error {
	spb := NewXPBWriterFromTag(isc_action_svc_properties)
	spb.PutString(isc_spb_dbname, database)
	spb.PutByte(isc_spb_prp_write_mode, mode)
	return mm.attach(spb.Bytes(), nil)
}

func (mm *MaintenanceManager) SetWriteModeAsync(database string) error {
	return mm.setWriteMode(database, isc_spb_prp_wm_async)
}

func (mm *MaintenanceManager) SetWriteModeSync(database string) error {
	return mm.setWriteMode(database, isc_spb_prp_wm_sync)
}

func (mm *MaintenanceManager) setPageFill(database string, mode byte) error {
	spb := NewXPBWriterFromTag(isc_action_svc_properties)
	spb.PutString(isc_spb_dbname, database)
	spb.PutByte(isc_spb_prp_reserve_space, mode)
	return mm.attach(spb.Bytes(), nil)
}

func (mm *MaintenanceManager) SetPageFillNoReserve(database string) error {
	return mm.setPageFill(database, isc_spb_prp_res_use_full)
}

func (mm *MaintenanceManager) SetPageFillReserve(database string) error {
	return mm.setPageFill(database, isc_spb_prp_res)
}

func (mm *MaintenanceManager) Shutdown(database string, shutdownMode ShutdownMode, timeout uint) error {
	spb := NewXPBWriterFromTag(isc_action_svc_properties)
	spb.PutString(isc_spb_dbname, database)
	spb.PutInt32(byte(shutdownMode), int32(timeout))
	return mm.attach(spb.Bytes(), nil)
}

func (mm *MaintenanceManager) Online(database string) error {
	spb := NewXPBWriterFromTag(isc_action_svc_properties)
	spb.PutString(isc_spb_dbname, database)
	spb.PutInt32(isc_spb_options, isc_spb_prp_db_online)
	return mm.attach(spb.Bytes(), nil)
}

func (mm *MaintenanceManager) NoLinger(database string) error {
	spb := NewXPBWriterFromTag(isc_action_svc_properties)
	spb.PutString(isc_spb_dbname, database)
	spb.PutInt32(isc_spb_options, isc_spb_prp_nolinger)
	return mm.attach(spb.Bytes(), nil)
}

func (mm *MaintenanceManager) ShutdownEx(database string, operationMode OperationMode, shutdownModeEx ShutdownModeEx, timeout uint) error {
	spb := NewXPBWriterFromTag(isc_action_svc_properties)
	spb.PutString(isc_spb_dbname, database)
	spb.PutByte(isc_spb_prp_shutdown_mode, byte(operationMode))
	spb.PutInt32(byte(shutdownModeEx), int32(timeout))
	return mm.attach(spb.Bytes(), nil)
}

func (mm *MaintenanceManager) OnlineEx(database string, operationMode OperationMode) error {
	spb := NewXPBWriterFromTag(isc_action_svc_properties)
	spb.PutString(isc_spb_dbname, database)
	spb.PutByte(isc_spb_prp_online_mode, byte(operationMode))
	return mm.attach(spb.Bytes(), nil)
}

func (mm *MaintenanceManager) SetSweepInterval(database string, transactions uint) error {
	spb := NewXPBWriterFromTag(isc_action_svc_properties)
	spb.PutString(isc_spb_dbname, database)
	spb.PutInt32(isc_spb_prp_sweep_interval, int32(transactions))
	return mm.attach(spb.Bytes(), nil)
}

func (mm *MaintenanceManager) Sweep(database string) error {
	spb := NewXPBWriterFromTag(isc_action_svc_repair)
	spb.PutString(isc_spb_dbname, database)
	spb.PutInt32(isc_spb_options, isc_spb_rpr_sweep_db)
	return mm.attach(spb.Bytes(), nil)
}

func (mm *MaintenanceManager) ActivateShadow(shadow string) error {
	spb := NewXPBWriterFromTag(isc_action_svc_properties)
	spb.PutString(isc_spb_dbname, shadow)
	spb.PutInt32(isc_spb_options, isc_spb_prp_activate)
	return mm.attach(spb.Bytes(), nil)
}

func (mm *MaintenanceManager) KillShadow(database string) error {
	spb := NewXPBWriterFromTag(isc_action_svc_repair)
	spb.PutString(isc_spb_dbname, database)
	spb.PutInt32(isc_spb_options, isc_spb_rpr_kill_shadows)
	return mm.attach(spb.Bytes(), nil)
}

func (mm *MaintenanceManager) Mend(database string) error {
	spb := NewXPBWriterFromTag(isc_action_svc_repair)
	spb.PutString(isc_spb_dbname, database)
	spb.PutInt32(isc_spb_options, isc_spb_rpr_mend_db)
	return mm.attach(spb.Bytes(), nil)
}

func (mm *MaintenanceManager) Validate(database string, options int) error {
	spb := NewXPBWriterFromTag(isc_action_svc_repair)
	spb.PutString(isc_spb_dbname, database)
	spb.PutInt32(isc_spb_options, isc_spb_rpr_validate_db|int32(options))
	return mm.attach(spb.Bytes(), nil)
}

func (mm *MaintenanceManager) GetLimboTransactions(database string) ([]int64, error) {
	var (
		err     error
		resChan = make(chan []byte)
		done    = make(chan bool)
		cont    = true
		buf     []byte
		tids    []int64
	)

	spb := NewXPBWriterFromTag(isc_action_svc_repair)
	spb.PutString(isc_spb_dbname, database)
	spb.PutInt32(isc_spb_options, isc_spb_rpr_list_limbo_trans)

	go func() {
		err = mm.attachBuffer(spb.Bytes(), resChan)
		done <- true
	}()

	for cont {
		select {
		case buf = <-resChan:
			srb := NewXPBReader(buf)
			var (
				have bool
				val  byte
			)
			for {
				if have, val = srb.Next(); !have {
					break
				}
				switch val {
				case isc_spb_single_tra_id:
					fallthrough
				case isc_spb_multi_tra_id:
					tids = append(tids, int64(srb.GetInt32()))
				case isc_spb_single_tra_id_64:
					fallthrough
				case isc_spb_multi_tra_id_64:
					tids = append(tids, srb.GetInt64())
				case isc_spb_tra_id:
					srb.Skip(4)
				case isc_spb_tra_id_64:
					srb.Skip(8)
				case isc_spb_tra_state:
					fallthrough
				case isc_spb_tra_advise:
					srb.Skip(1)
				case isc_spb_tra_host_site:
					fallthrough
				case isc_spb_tra_remote_site:
					fallthrough
				case isc_spb_tra_db_path:
					srb.GetString()
				}
			}
		case <-done:
			cont = false
			break
		}
	}

	return tids, err
}

func (mm *MaintenanceManager) CommitTransaction(database string, transaction int64) error {
	spb := NewXPBWriterFromTag(isc_action_svc_repair)
	spb.PutString(isc_spb_dbname, database)
	if fitsUint32(transaction) {
		spb.PutInt32(isc_spb_rpr_commit_trans, int32(transaction))
	} else {
		spb.PutInt64(isc_spb_rpr_commit_trans_64, transaction)
	}
	return mm.attach(spb.Bytes(), nil)
}

func (mm *MaintenanceManager) RollbackTransaction(database string, transaction int64) error {
	spb := NewXPBWriterFromTag(isc_action_svc_repair)
	spb.PutString(isc_spb_dbname, database)
	if fitsUint32(transaction) {
		spb.PutInt32(isc_spb_rpr_rollback_trans, int32(transaction))
	} else {
		spb.PutInt64(isc_spb_rpr_rollback_trans_64, transaction)
	}
	return mm.attach(spb.Bytes(), nil)
}

func (mm *MaintenanceManager) attach(spb []byte, verbose chan string) error {
	var (
		err  error
		conn *ServiceManager
	)
	if conn, err = mm.connBuilder(); err != nil {
		return err
	}
	defer func(conn *ServiceManager) {
		_ = conn.Close()
	}(conn)
	return conn.ServiceAttach(spb, verbose)
}

func (mm *MaintenanceManager) attachBuffer(spb []byte, verbose chan []byte) error {
	var (
		err  error
		conn *ServiceManager
	)
	if conn, err = mm.connBuilder(); err != nil {
		return err
	}
	defer func(conn *ServiceManager) {
		_ = conn.Close()
	}(conn)
	return conn.ServiceAttachBuffer(spb, verbose)
}
