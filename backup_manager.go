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

type BackupOption func(*BackupOptions)

type RestoreOptions struct {
	Replace              bool
	DeactivateIndexes    bool
	RestoreShadows       bool
	EnforceConstraints   bool
	CommitAfterEachTable bool
	UseAllPageSpace      bool
	PageSize             int32
	CacheBuffers         int32
}

type RestoreOption func(*RestoreOptions)

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

func WithIgnoreChecksums() BackupOption {
	return func(opts *BackupOptions) {
		opts.IgnoreChecksums = true
	}
}

func WithoutIgnoreChecksums() BackupOption {
	return func(opts *BackupOptions) {
		opts.IgnoreChecksums = false
	}
}

func WithIgnoreLimboTransactions() BackupOption {
	return func(opts *BackupOptions) {
		opts.IgnoreLimboTransactions = true
	}
}

func WithoutIgnoreLimboTransactions() BackupOption {
	return func(opts *BackupOptions) {
		opts.IgnoreLimboTransactions = false
	}
}

func WithMetadataOnly() BackupOption {
	return func(opts *BackupOptions) {
		opts.MetadataOnly = true
	}
}

func WithoutMetadataOnly() BackupOption {
	return func(opts *BackupOptions) {
		opts.MetadataOnly = false
	}
}

func WithGarbageCollect() BackupOption {
	return func(opts *BackupOptions) {
		opts.GarbageCollect = true
	}
}

func WithoutGarbageCollect() BackupOption {
	return func(opts *BackupOptions) {
		opts.GarbageCollect = false
	}
}

func WithTransportable() BackupOption {
	return func(opts *BackupOptions) {
		opts.Transportable = true
	}
}

func WithoutTransportable() BackupOption {
	return func(opts *BackupOptions) {
		opts.Transportable = false
	}
}

func WithConvertExternalTablesToInternalTables() BackupOption {
	return func(opts *BackupOptions) {
		opts.ConvertExternalTablesToInternalTables = true
	}
}

func WithoutConvertExternalTablesToInternalTables() BackupOption {
	return func(opts *BackupOptions) {
		opts.ConvertExternalTablesToInternalTables = false
	}
}

func WithExpand() BackupOption {
	return func(opts *BackupOptions) {
		opts.Expand = true
	}
}

func WithoutExpand() BackupOption {
	return func(opts *BackupOptions) {
		opts.Expand = false
	}
}

func NewBackupOptions(opts ...BackupOption) BackupOptions {
	res := GetDefaultBackupOptions()
	for _, opt := range opts {
		opt(&res)
	}
	return res
}

func GetDefaultRestoreOptions() RestoreOptions {
	return RestoreOptions{
		Replace:              false,
		DeactivateIndexes:    false,
		RestoreShadows:       true,
		EnforceConstraints:   true,
		CommitAfterEachTable: false,
		UseAllPageSpace:      false,
		PageSize:             0,
		CacheBuffers:         0,
	}
}

func WithReplace() RestoreOption {
	return func(opts *RestoreOptions) {
		opts.Replace = true
	}
}

func WithoutReplace() RestoreOption {
	return func(opts *RestoreOptions) {
		opts.Replace = false
	}
}

func WithDeactivateIndexes() RestoreOption {
	return func(opts *RestoreOptions) {
		opts.DeactivateIndexes = true
	}
}

func WithRestoreShadows() RestoreOption {
	return func(opts *RestoreOptions) {
		opts.RestoreShadows = true
	}
}

func WithoutRestoreShadows() RestoreOption {
	return func(opts *RestoreOptions) {
		opts.RestoreShadows = false
	}
}

func WithEnforceConstraints() RestoreOption {
	return func(opts *RestoreOptions) {
		opts.EnforceConstraints = true
	}
}

func WithoutEnforceConstraints() RestoreOption {
	return func(opts *RestoreOptions) {
		opts.EnforceConstraints = false
	}
}

func WithCommitAfterEachTable() RestoreOption {
	return func(opts *RestoreOptions) {
		opts.CommitAfterEachTable = true
	}
}

func WithoutCommitAfterEachTable() RestoreOption {
	return func(opts *RestoreOptions) {
		opts.CommitAfterEachTable = false
	}
}

func WithUseAllPageSpace() RestoreOption {
	return func(opts *RestoreOptions) {
		opts.UseAllPageSpace = true
	}
}

func WithoutUseAllPageSpace() RestoreOption {
	return func(opts *RestoreOptions) {
		opts.UseAllPageSpace = false
	}
}

func WithPageSize(pageSize int32) RestoreOption {
	return func(opts *RestoreOptions) {
		opts.PageSize = pageSize
	}
}

func WithCacheBuffers(cacheBuffers int32) RestoreOption {
	return func(opts *RestoreOptions) {
		opts.CacheBuffers = cacheBuffers
	}
}

func NewRestoreOptions(opts ...RestoreOption) RestoreOptions {
	res := GetDefaultRestoreOptions()
	for _, opt := range opts {
		opt(&res)
	}
	return res
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
	} else {
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
