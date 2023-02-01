package firebirdsql

type BackupManager struct {
	connBuilder func() (*ServiceManager, error)
}

type BackupOptions struct {
	IgnoreChecksums                       bool
	IgnoreLimboTransactions               bool
	MetadataOnly                          bool
	GarbageCollect                        bool
	Transportable                         bool
	ConvertExternalTablesToInternalTables bool
	Expand                                bool
}

type RestoreOptions struct {
	Replace              bool
	Create               bool
	DeactivateIndexes    bool
	RestoreShadows       bool
	EnforceConstraints   bool
	CommitAfterEachTable bool
	UseAllPageSpace      bool
	PageSize             int32
	CacheBuffers         int32
}

func GetDefaultBackupOptions() BackupOptions {
	return BackupOptions{
		IgnoreChecksums:                       false,
		IgnoreLimboTransactions:               false,
		MetadataOnly:                          false,
		GarbageCollect:                        true,
		Transportable:                         true,
		ConvertExternalTablesToInternalTables: true,
		Expand:                                false,
	}
}

func GetDefaultRestoreOptions() RestoreOptions {
	return RestoreOptions{
		Replace:              false,
		Create:               false,
		DeactivateIndexes:    false,
		RestoreShadows:       true,
		EnforceConstraints:   true,
		CommitAfterEachTable: false,
		UseAllPageSpace:      false,
		PageSize:             0,
		CacheBuffers:         0,
	}
}

func NewBackupManager(addr string, user string, password string, options ServiceManagerOptions) (*BackupManager, error) {
	connBuilder := func() (*ServiceManager, error) {
		return NewServiceManager(addr, user, password, options)
	}
	return &BackupManager{
		connBuilder: connBuilder,
	}, nil
}

func (bm *BackupManager) Backup(database string, backup string, options BackupOptions, verbose chan string) error {
	var optionsMask int32
	var err error
	var conn *ServiceManager

	if options.IgnoreChecksums {
		optionsMask |= isc_spb_bkp_ignore_checksums
	}

	if options.IgnoreLimboTransactions {
		optionsMask |= isc_spb_bkp_ignore_limbo
	}

	if options.MetadataOnly {
		optionsMask |= isc_spb_bkp_metadata_only
	}

	if !options.GarbageCollect {
		optionsMask |= isc_spb_bkp_no_garbage_collect
	}

	if !options.Transportable {
		optionsMask |= isc_spb_bkp_non_transportable
	}

	if options.ConvertExternalTablesToInternalTables {
		optionsMask |= isc_spb_bkp_convert
	}

	if options.Expand {
		optionsMask |= isc_spb_bkp_expand
	}

	spb := NewXPBWriterFromTag(isc_action_svc_backup)
	spb.PutString(isc_spb_dbname, database)
	spb.PutString(isc_spb_bkp_file, backup)
	spb.PutInt32(isc_spb_options, optionsMask)

	if verbose != nil {
		spb.PutTag(isc_spb_verbose)
	}

	if conn, err = bm.connBuilder(); err != nil {
		return err
	}
	defer func(conn *ServiceManager) {
		_ = conn.Close()
	}(conn)

	return bm.attach(spb.Bytes(), verbose)
}

func (bm *BackupManager) Restore(backup string, database string, options RestoreOptions, verbose chan string) error {
	var optionsMask int32 = 0
	var err error
	var conn *ServiceManager

	if options.Replace {
		optionsMask |= isc_spb_res_replace
	}

	if options.Create {
		optionsMask |= isc_spb_res_create
	}

	if options.DeactivateIndexes {
		optionsMask |= isc_spb_res_deactivate_idx
	}

	if !options.RestoreShadows {
		optionsMask |= isc_spb_res_no_shadow
	}

	if !options.EnforceConstraints {
		optionsMask |= isc_spb_res_no_validity
	}

	if options.CommitAfterEachTable {
		optionsMask |= isc_spb_res_one_at_a_time
	}

	if options.UseAllPageSpace {
		optionsMask |= isc_spb_res_use_all_space
	}

	spb := NewXPBWriterFromTag(isc_action_svc_restore)
	spb.PutString(isc_spb_dbname, database)
	spb.PutString(isc_spb_bkp_file, backup)
	spb.PutInt32(isc_spb_options, optionsMask)

	if verbose != nil {
		spb.PutTag(isc_spb_verbose)
	}

	if options.PageSize > 0 {
		spb.PutInt32(isc_spb_res_page_size, options.PageSize)
	}

	if options.CacheBuffers > 0 {
		spb.PutInt32(isc_spb_res_buffers, options.PageSize)
	}

	if conn, err = bm.connBuilder(); err != nil {
		return err
	}
	defer func(conn *ServiceManager) {
		_ = conn.Close()
	}(conn)

	return bm.attach(spb.Bytes(), verbose)
}

func (bm *BackupManager) attach(spb []byte, verbose chan string) error {
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
