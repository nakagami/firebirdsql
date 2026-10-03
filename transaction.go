/*******************************************************************************
The MIT License (MIT)

Copyright (c) 2013-2019 Hajime Nakagami

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

import "context"

type firebirdsqlTx struct {
	fc             *firebirdsqlConn
	isolationLevel int
	isAutocommit   bool
	transHandle    int32
	needBegin      bool
	// ctx is the BeginTx context. database/sql documents it as used until the
	// transaction is committed or rolled back, and Tx.Commit takes no context
	// of its own, so Commit is bounded by it. Nil for the connection's
	// autocommit transaction.
	ctx context.Context
}

func tpbForIsolationLevel(isolationLevel int) ([]byte, error) {
	switch isolationLevel {
	case ISOLATION_LEVEL_READ_COMMITED_LEGACY:
		return []byte{
			byte(isc_tpb_version3),
			byte(isc_tpb_write),
			byte(isc_tpb_wait),
			byte(isc_tpb_read_committed),
			byte(isc_tpb_no_rec_version),
		}, nil
	case ISOLATION_LEVEL_READ_COMMITED:
		return []byte{
			byte(isc_tpb_version3),
			byte(isc_tpb_write),
			byte(isc_tpb_wait),
			byte(isc_tpb_read_committed),
			byte(isc_tpb_rec_version),
		}, nil
	case ISOLATION_LEVEL_READ_COMMITED_NOWAIT:
		return []byte{
			byte(isc_tpb_version3),
			byte(isc_tpb_write),
			byte(isc_tpb_nowait),
			byte(isc_tpb_read_committed),
			byte(isc_tpb_rec_version),
		}, nil
	case ISOLATION_LEVEL_REPEATABLE_READ:
		return []byte{
			byte(isc_tpb_version3),
			byte(isc_tpb_write),
			byte(isc_tpb_wait),
			byte(isc_tpb_concurrency),
		}, nil
	case ISOLATION_LEVEL_SERIALIZABLE:
		return []byte{
			byte(isc_tpb_version3),
			byte(isc_tpb_write),
			byte(isc_tpb_wait),
			byte(isc_tpb_consistency),
		}, nil
	case ISOLATION_LEVEL_READ_COMMITED_RO:
		return []byte{
			byte(isc_tpb_version3),
			byte(isc_tpb_read),
			byte(isc_tpb_wait),
			byte(isc_tpb_read_committed),
			byte(isc_tpb_rec_version),
		}, nil
	case ISOLATION_LEVEL_READ_COMMITED_RO_NOWAIT:
		return []byte{
			byte(isc_tpb_version3),
			byte(isc_tpb_read),
			byte(isc_tpb_nowait),
			byte(isc_tpb_read_committed),
			byte(isc_tpb_rec_version),
		}, nil
	default:
		return nil, ErrInvalidIsolationLevel
	}
}

func (tx *firebirdsqlTx) begin() (err error) {
	tpb, err := tpbForIsolationLevel(tx.isolationLevel)
	if err != nil {
		return err
	}
	err = tx.fc.wp.opTransaction(tpb)
	if err != nil {
		return
	}
	tx.transHandle, _, _, err = tx.fc.wp.opResponse()
	if err != nil {
		return
	}
	tx.needBegin = false
	tx.fc.transactionSet[tx] = struct{}{}
	return
}

func (tx *firebirdsqlTx) commitRetainging() (err error) {
	err = tx.fc.wp.opCommitRetaining(tx.transHandle)
	if err != nil {
		return
	}
	// Teardown read: bounded so the autocommit commit-retaining cannot hang a silent-wire
	// close (it runs in freeStatement's teardown path as well as the exec hot path).
	_, _, _, err = tx.fc.wp.opResponseTimeout(abandonReadTimeout)
	tx.fc.wp.clearInlineBlobCache(tx.transHandle)
	tx.isAutocommit = tx.fc.isAutocommit
	return
}

// Commit is bounded by the BeginTx context: if it ends before the server answers,
// Commit returns the context error wrapped with driver.ErrBadConn and the
// connection is discarded. As with any connection lost mid-commit, the outcome of
// the commit on the server is then unknown.
func (tx *firebirdsqlTx) Commit() (err error) {
	ctx := tx.ctx
	tx.ctx = nil
	if ctx == nil {
		ctx = context.Background()
	}
	err = tx.fc.wp.withContextDeadline(ctx, func() error {
		if err := tx.fc.wp.opCommit(tx.transHandle); err != nil {
			return err
		}
		_, _, _, err := tx.fc.wp.opResponse()
		return err
	})
	tx.fc.wp.clearInlineBlobCache(tx.transHandle)
	tx.isAutocommit = tx.fc.isAutocommit
	tx.needBegin = true
	return
}

// Rollback keeps its fixed teardown bound instead of the BeginTx context:
// database/sql rolls back precisely when that context has ended.
func (tx *firebirdsqlTx) Rollback() (err error) {
	tx.ctx = nil
	err = tx.fc.wp.opRollback(tx.transHandle)
	if err != nil {
		return err
	}
	// Teardown read: bounded so connection.Close()'s rollback loop cannot hang on a silent
	// wire before it ever reaches opDetach.
	_, _, _, err = tx.fc.wp.opResponseTimeout(abandonReadTimeout)
	tx.fc.wp.clearInlineBlobCache(tx.transHandle)
	tx.isAutocommit = tx.fc.isAutocommit
	tx.needBegin = true
	return
}

func newFirebirdsqlTx(fc *firebirdsqlConn, isolationLevel int, isAutocommit bool, withBegin bool) (tx *firebirdsqlTx, err error) {
	tx = new(firebirdsqlTx)
	tx.fc = fc
	tx.isolationLevel = isolationLevel
	tx.isAutocommit = isAutocommit
	tx.needBegin = false

	if withBegin {
		err = tx.begin()

		if err != nil {
			return nil, err
		}

	} else {
		tx.needBegin = true
	}

	return
}
