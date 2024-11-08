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

type NBackupOption func(*NBackupOptions)

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

func WithLevel(level int) NBackupOption {
	return func(opts *NBackupOptions) {
		opts.Level = int32(level)
	}
}

func WithGuid(guid string) NBackupOption {
	return func(opts *NBackupOptions) {
		opts.Guid = guid
	}
}

func WithoutDBTriggers() NBackupOption {
	return func(opts *NBackupOptions) {
		opts.NoDBTriggers = true
	}
}

func WithDBTriggers() NBackupOption {
	return func(opts *NBackupOptions) {
		opts.NoDBTriggers = false
	}
}

func WithInPlaceRestore() NBackupOption {
	return func(opts *NBackupOptions) {
		opts.InPlaceRestore = false
	}
}

func WithPlaceRestore() NBackupOption {
	return func(opts *NBackupOptions) {
		opts.InPlaceRestore = true
	}
}

func WithPreserveSequence() NBackupOption {
	return func(opts *NBackupOptions) {
		opts.PreserveSequence = true
	}
}

func WithoutPreserveSequence() NBackupOption {
	return func(opts *NBackupOptions) {
		opts.PreserveSequence = false
	}
}

func NewNBackupOptions(opts ...NBackupOption) NBackupOptions {
	res := GetDefaultNBackupOptions()
	for _, opt := range opts {
		opt(&res)
	}
	return res
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

	return bm.attach(spb.Bytes(), verbose)
}

func (bm *NBackupManager) Restore(backups []string, database string, options NBackupOptions, verbose chan string) error {
	spb := NewXPBWriterFromTag(isc_action_svc_nrest)
	spb.PutString(isc_spb_dbname, database)
	for _, file := range backups {
		spb.PutString(isc_spb_nbk_file, file)
	}

	optionsMask := options.GetOptionsMask()
	if optionsMask != 0 {
		spb.PutInt32(isc_spb_options, optionsMask)
	}

	return bm.attach(spb.Bytes(), verbose)
}

func (bm *NBackupManager) Fixup(database string, options NBackupOptions, verbose chan string) error {
	spb := NewXPBWriterFromTag(isc_action_svc_nfix)
	spb.PutString(isc_spb_dbname, database)

	optionsMask := options.GetOptionsMask()
	if optionsMask != 0 {
		spb.PutInt32(isc_spb_options, optionsMask)
	}

	return bm.attach(spb.Bytes(), verbose)
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
