package firebirdsql

type NBackupManager struct {
	connBuilder func() (*ServiceManager, error)
}

type NBackupOptions struct {
	Level            int32
	Guid             string
	NoDBTriggers     bool
	InPlaceRestore   bool
	PreserveSequence bool
}

func GetDefaultNBackupOptions() NBackupOptions {
	return NBackupOptions{
		Level:            -1,
		Guid:             "",
		NoDBTriggers:     false,
		InPlaceRestore:   false,
		PreserveSequence: false,
	}
}

func (o NBackupOptions) GetOptionsMask() int32 {
	var optionsMask int32

	if o.NoDBTriggers {
		optionsMask |= isc_spb_nbk_no_triggers
	}
	if o.InPlaceRestore {
		optionsMask |= isc_spb_nbk_inplace
	}
	if o.PreserveSequence {
		optionsMask |= isc_spb_nbk_sequence
	}

	return optionsMask
}

func NewNBackupManager(addr string, user string, password string, options ServiceManagerOptions) (*NBackupManager, error) {
	connBuilder := func() (*ServiceManager, error) {
		return NewServiceManager(addr, user, password, options)
	}
	return &NBackupManager{
		connBuilder: connBuilder,
	}, nil
}

func (bm *NBackupManager) Backup(database string, backup string, options NBackupOptions, verbose chan string) error {
	spb := NewXPBWriterFromTag(isc_action_svc_nbak)
	spb.PutString(isc_spb_dbname, database)
	spb.PutString(isc_spb_nbk_file, backup)

	level := options.Level
	if options.Level < 0 && options.Guid == "" {
		level = 0
	}
	spb.PutInt32(isc_spb_nbk_level, level)
	if options.Guid != "" {
		spb.PutString(isc_spb_nbk_guid, options.Guid)
	}

	optionsMask := options.GetOptionsMask()
	if optionsMask != 0 {
		spb.PutInt32(isc_spb_options, optionsMask)
	}

	return bm.attach(spb.GetBuffer(), verbose)
}

func (bm *NBackupManager) Restore(backup string, database string, options NBackupOptions, verbose chan string) error {
	spb := NewXPBWriterFromTag(isc_action_svc_nrest)
	spb.PutString(isc_spb_dbname, database)
	spb.PutString(isc_spb_nbk_file, backup)

	optionsMask := options.GetOptionsMask()
	if optionsMask != 0 {
		spb.PutInt32(isc_spb_options, optionsMask)
	}

	return bm.attach(spb.GetBuffer(), verbose)
}

func (bm *NBackupManager) Fixup(database string, options NBackupOptions, verbose chan string) error {
	spb := NewXPBWriterFromTag(isc_action_svc_nfix)
	spb.PutString(isc_spb_dbname, database)

	optionsMask := options.GetOptionsMask()
	if optionsMask != 0 {
		spb.PutInt32(isc_spb_options, optionsMask)
	}

	return bm.attach(spb.GetBuffer(), verbose)
}

func (bm *NBackupManager) attach(spb []byte, verbose chan string) error {
	var err error
	var conn *ServiceManager
	if conn, err = bm.connBuilder(); err != nil {
		return err
	}
	defer func(conn *ServiceManager) {
		_ = conn.Close()
	}(conn)

	return conn.ServiceAttach(spb, verbose)
}
