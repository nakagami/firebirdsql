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
	tpb            []byte // TPB this wire transaction was started with
	// ctx is the BeginTx context. database/sql documents it as used until the
	// transaction is committed or rolled back, and Tx.Commit takes no context
	// of its own, so Commit is bounded by it. Nil for the connection's
	// autocommit transaction.
	ctx context.Context
}

// txScenario is a decoded IsolationLevel: everything needed to materialize
// a TPB for the transaction.
type txScenario struct {
	isolation   int // ISOLATION_LEVEL_* equivalent base
	waitMode    int // isc_tpb_wait | isc_tpb_nowait
	lockTimeout int // seconds; >0 forces WAIT and excludes isc_tpb_nowait
	ro          bool
}

// decodeDriverLevel decodes a database/sql IsolationLevel (possibly carrying a
// driver-specific encoding, see consts.go) into a txScenario. Plain values
// decode as the historical numeric union of sql.Level* and the internal
// ISOLATION_LEVEL_* constants (LevelDefault/LevelReadCommitted and
// ISOLATION_LEVEL_READ_COMMITED share values 0/1 and both map to READ
// COMMITTED rec_version; 4 is both LevelSerializable and
// ISOLATION_LEVEL_SERIALIZABLE).
func decodeDriverLevel(level int) (txScenario, bool) {
	sc := txScenario{waitMode: isc_tpb_wait}
	switch {
	case level == LevelReadCommittedNoWait:
		sc.isolation = ISOLATION_LEVEL_READ_COMMITED
		sc.waitMode = isc_tpb_nowait
		return sc, true
	case level == LevelReadCommittedRecVersion:
		sc.isolation = ISOLATION_LEVEL_READ_COMMITED
		return sc, true
	case level == LevelReadCommittedLegacy:
		sc.isolation = ISOLATION_LEVEL_READ_COMMITED_LEGACY
		return sc, true
	case level == LevelReadCommittedLegacyNoWait:
		sc.isolation = ISOLATION_LEVEL_READ_COMMITED_LEGACY_NOWAIT
		sc.waitMode = isc_tpb_nowait
		return sc, true
	case level == LevelSnapshot:
		sc.isolation = ISOLATION_LEVEL_REPEATABLE_READ
		return sc, true
	case level == LevelSnapshotNoWait:
		sc.isolation = ISOLATION_LEVEL_REPEATABLE_READ_NOWAIT
		sc.waitMode = isc_tpb_nowait
		return sc, true
	case level == LevelConsistency:
		sc.isolation = ISOLATION_LEVEL_SERIALIZABLE
		return sc, true
	case level > LevelLockTimeoutBase && level <= LevelLockTimeoutBase+maxLockTimeout:
		sc.isolation = ISOLATION_LEVEL_READ_COMMITED
		sc.lockTimeout = level - LevelLockTimeoutBase
		return sc, true
	}
	switch level {
	// Plain values decode as the union of sql.Level* and the internal
	// ISOLATION_LEVEL_* constants. Collisions are resolved to keep the
	// historical BeginTx behavior of the sql.Level* values:
	//   0: LevelDefault / LEGACY            -> RC (rec_version)  [old: RC]
	//   1: LevelReadUncommitted / RC        -> RC                [old: error]
	//   2: LevelReadCommitted / REPEATABLE  -> RC                [old: RC]
	//   3: LevelWriteCommitted / SERIALIZABLE -> consistency     [old: error]
	//   4: LevelRepeatableRead / RC_RO      -> snapshot          [old: snapshot]
	//   5: LevelSnapshot / RC_NOWAIT        -> RC nowait         [old: error]
	//   6: LevelSerializable / RC_RO_NOWAIT -> consistency       [old: consistency]
	//   7..10: internal presets (LEGACY_NOWAIT, REPEATABLE_READ_NOWAIT,
	//          REPEATABLE_READ_RO, SERIALIZABLE_RO)
	case 0, 1, 2:
		sc.isolation = ISOLATION_LEVEL_READ_COMMITED
	case 3, 6:
		sc.isolation = ISOLATION_LEVEL_SERIALIZABLE
	case 4:
		sc.isolation = ISOLATION_LEVEL_REPEATABLE_READ
	case 5:
		sc.isolation = ISOLATION_LEVEL_READ_COMMITED_NOWAIT
		sc.waitMode = isc_tpb_nowait
	case 7:
		sc.isolation = ISOLATION_LEVEL_READ_COMMITED_LEGACY_NOWAIT
		sc.waitMode = isc_tpb_nowait
	case 8:
		sc.isolation = ISOLATION_LEVEL_REPEATABLE_READ_NOWAIT
		sc.waitMode = isc_tpb_nowait
	case 9:
		sc.isolation = ISOLATION_LEVEL_REPEATABLE_READ_RO
		sc.ro = true
	case 10:
		sc.isolation = ISOLATION_LEVEL_SERIALIZABLE_RO
		sc.ro = true
	default:
		return sc, false
	}
	return sc, true
}

// tpbBytes materializes the scenario into a TPB. Element order mirrors the
// legacy presets (version3, access mode, wait group, isolation cluster).
// isc_tpb_lock_timeout is encoded length-prefixed little-endian (VAX) per
// tra.cpp; it conflicts with isc_tpb_nowait server-side, so it implies WAIT.
func (sc txScenario) tpbBytes() ([]byte, error) {
	tpb := []byte{isc_tpb_version3}
	if sc.ro {
		tpb = append(tpb, isc_tpb_read)
	} else {
		tpb = append(tpb, isc_tpb_write)
	}
	switch {
	case sc.lockTimeout > 0:
		tpb = append(tpb, isc_tpb_wait, isc_tpb_lock_timeout,
			2, byte(sc.lockTimeout), byte(sc.lockTimeout>>8))
	case sc.waitMode == isc_tpb_nowait:
		tpb = append(tpb, isc_tpb_nowait)
	default:
		tpb = append(tpb, isc_tpb_wait)
	}
	switch sc.isolation {
	case ISOLATION_LEVEL_READ_COMMITED, ISOLATION_LEVEL_READ_COMMITED_NOWAIT,
		ISOLATION_LEVEL_READ_COMMITED_RO, ISOLATION_LEVEL_READ_COMMITED_RO_NOWAIT:
		tpb = append(tpb, isc_tpb_read_committed, isc_tpb_rec_version)
	case ISOLATION_LEVEL_READ_COMMITED_LEGACY, ISOLATION_LEVEL_READ_COMMITED_LEGACY_NOWAIT:
		tpb = append(tpb, isc_tpb_read_committed, isc_tpb_no_rec_version)
	case ISOLATION_LEVEL_REPEATABLE_READ, ISOLATION_LEVEL_REPEATABLE_READ_NOWAIT,
		ISOLATION_LEVEL_REPEATABLE_READ_RO:
		tpb = append(tpb, isc_tpb_concurrency)
	case ISOLATION_LEVEL_SERIALIZABLE, ISOLATION_LEVEL_SERIALIZABLE_RO:
		tpb = append(tpb, isc_tpb_consistency)
	default:
		return nil, ErrInvalidIsolationLevel
	}
	return tpb, nil
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
		if sc, ok := decodeDriverLevel(isolationLevel); ok {
			return sc.tpbBytes()
		}
		return nil, ErrInvalidIsolationLevel
	}
}

func (tx *firebirdsqlTx) begin() (err error) {
	tpb, err := tpbForIsolationLevel(tx.isolationLevel)
	if err != nil {
		return err
	}
	return tx.beginWithTPB(tpb)
}

// beginWithTPB starts the wire transaction with an explicit TPB.
func (tx *firebirdsqlTx) beginWithTPB(tpb []byte) (err error) {
	err = tx.fc.wp.opTransaction(tpb)
	if err != nil {
		return
	}
	tx.transHandle, _, _, err = tx.fc.wp.opResponse()
	if err != nil {
		return
	}
	tx.tpb = tpb
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
