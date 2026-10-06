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

import (
	"context"
	"database/sql/driver"
	"errors"
)

type firebirdsqlTx struct {
	fc             *firebirdsqlConn
	isolationLevel int
	isAutocommit   bool
	transHandle    int32
	needBegin      bool
	// ctx is the BeginTx context. database/sql documents it as used until the
	// transaction is committed or rolled back, and Tx.Commit/Tx.Rollback take no
	// context of their own, so they are bounded by it. Nil for the connection's
	// autocommit transaction and once the transaction has ended.
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

// teardownCtx is a done context for callers that have no context of their own
// (Stmt.Close, connection.Close): endResponse then takes the bounded teardown read.
var teardownCtx = func() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}()

// endResponse reads the reply to op_commit, op_commit_retaining or op_rollback.
//
// That reply carries the transaction's server-side work (a large commit or rollback, the
// delta merge behind ALTER DATABASE END BACKUP), and a fixed timeout cannot tell a slow
// server from a dead one. So a live ctx alone bounds the read: no deadline, no limit. A
// done ctx means teardown (Stmt.Close, connection.Close, database/sql rolling back a Tx
// whose context ended), and the read keeps the fixed abandonReadTimeout bound so a silent
// wire cannot hang it.
//
// A reply read in full is authoritative: success stays success even if ctx ended
// meanwhile, and a server error is returned as is. Any other failure leaves the reply
// unread, so the wire is marked desynced and IsValid keeps the connection out of the pool.
func (tx *firebirdsqlTx) endResponse(ctx context.Context) error {
	wp := tx.fc.wp
	if contextErrOrDeadlineExceeded(ctx) != nil {
		_, _, _, err := wp.opResponseTimeout(abandonReadTimeout)
		return err
	}
	return wp.boundByContext(ctx, func() error {
		_, _, _, err := wp.opResponse()
		return wp.markUnlessReplyRead(err)
	})
}

// noRetryAfterExec takes driver.ErrBadConn out of the error of a step that runs after the
// statement executed. database/sql runs a statement again on another connection when it
// fails with ErrBadConn, unless the context is done; here that would execute it twice.
// The connection is still discarded: every such failure marks the wire desynced.
func noRetryAfterExec(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return err
	}
	return stripBadConn("commit", err)
}

// stripBadConn takes driver.ErrBadConn out of err, naming the step it failed in. The
// driver.ErrBadConn contract: it must not be returned when the server may have performed
// the operation, since database/sql then runs it again on another connection.
func stripBadConn(step string, err error) error {
	if err == nil || !errors.Is(err, driver.ErrBadConn) {
		return err
	}
	return &afterExecError{step: step, err: err}
}

// afterExecError keeps the message and the rest of the error chain (context and
// network errors, *FbError) of an error whose driver.ErrBadConn was taken out.
type afterExecError struct {
	step string
	err  error
}

func (e *afterExecError) Error() string   { return "firebirdsql: " + e.step + ": " + e.err.Error() }
func (e *afterExecError) Unwrap() []error { return withoutBadConn(e.err) }

// withoutBadConn returns the parts of err's chain that do not lead to driver.ErrBadConn.
func withoutBadConn(err error) []error {
	if err == nil || err == driver.ErrBadConn {
		return nil
	}
	if !errors.Is(err, driver.ErrBadConn) {
		return []error{err}
	}
	var out []error
	switch u := err.(type) {
	case interface{ Unwrap() []error }:
		for _, e := range u.Unwrap() {
			out = append(out, withoutBadConn(e)...)
		}
	case interface{ Unwrap() error }:
		out = withoutBadConn(u.Unwrap())
	}
	return out
}

// commitRetaining commits the autocommit transaction's work and keeps the transaction
// open. The statement paths (exec, ExecImmediate, batch) and rows.Close pass their
// context; Stmt.Close passes teardownCtx. See endResponse.
func (tx *firebirdsqlTx) commitRetaining(ctx context.Context) (err error) {
	// A desynced wire is refused before anything is sent. errConnDesynced keeps its
	// driver.ErrBadConn: the work is rolled back with the connection, so a retry runs the
	// statement once.
	if err = tx.fc.checkWire(); err != nil {
		return err
	}
	defer func() { err = noRetryAfterExec(ctx, err) }()
	if err = tx.fc.wp.opCommitRetaining(tx.transHandle); err != nil {
		return err
	}
	err = tx.endResponse(ctx)
	tx.fc.wp.clearInlineBlobCache(tx.transHandle)
	tx.isAutocommit = tx.fc.isAutocommit
	if isServerError(err) {
		return tx.rollbackRefusedCommit(ctx, err)
	}
	return err
}

// rollbackRefusedCommit rolls back a transaction whose commit the server refused (an ON
// TRANSACTION COMMIT trigger raising, for example). Firebird leaves such a transaction
// active with its work; left alone, a later commit on this connection would commit work
// the caller was told had failed. It returns the commit error, joined with the rollback's
// if that failed too.
func (tx *firebirdsqlTx) rollbackRefusedCommit(ctx context.Context, commitErr error) error {
	if err := tx.rollback(ctx); err != nil {
		return errors.Join(commitErr, err)
	}
	return commitErr
}

// retire takes the transaction out of use once a commit or rollback reply was read (not
// after commit-retaining, which keeps it open): it drops the handle's inline-blob cache
// entries, resets isAutocommit, marks it as needing a new begin and drops its BeginTx
// context. On success it also leaves the connection's transaction set.
func (tx *firebirdsqlTx) retire(err error) {
	tx.fc.wp.clearInlineBlobCache(tx.transHandle)
	tx.isAutocommit = tx.fc.isAutocommit
	tx.needBegin = true
	tx.ctx = nil
	if err == nil {
		// A transaction that failed to end stays in the set. Either its connection is
		// already unusable (the reply was lost, or the server refused the rollback; see
		// rollback) and is dropped, which makes the server release it, or connection.Close
		// rolls it back.
		delete(tx.fc.transactionSet, tx)
	}
}

// Commit is bounded by the BeginTx context (see endResponse). If that context ends
// while the reply is outstanding, Commit returns the context error wrapped with
// driver.ErrBadConn and the connection is discarded; as with any connection lost
// mid-commit, the outcome on the server is then unknown.
func (tx *firebirdsqlTx) Commit() (err error) {
	ctx := tx.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	if err = tx.fc.checkWire(); err != nil {
		return err
	}
	if cerr := contextErrOrDeadlineExceeded(ctx); cerr != nil {
		// database/sql checks the context before calling Commit, but it can end right
		// after; by then the Tx is marked done, so database/sql's own rollback no longer
		// runs. Roll back here rather than leave the transaction and its locks open on a
		// pooled connection, and report the context error as database/sql would have.
		// The rollback takes the bounded teardown read, like database/sql's own rollback
		// after the context ends.
		if err = tx.rollback(teardownCtx); err != nil {
			return errors.Join(cerr, err)
		}
		return cerr
	}
	if err = tx.fc.wp.opCommit(tx.transHandle); err != nil {
		return err
	}
	err = tx.endResponse(ctx)
	if isServerError(err) {
		return tx.rollbackRefusedCommit(ctx, err)
	}
	tx.retire(err)
	return err
}

// Rollback is bounded by the BeginTx context the same way as Commit. database/sql's
// automatic rollback after that context ends sees it done and so takes the fixed teardown
// bound; database/sql hands the driver the caller's context, not the one it cancels when
// the Tx finishes.
func (tx *firebirdsqlTx) Rollback() error {
	ctx := tx.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	return tx.rollback(ctx)
}

func (tx *firebirdsqlTx) rollback(ctx context.Context) (err error) {
	if err = tx.fc.checkWire(); err != nil {
		return err
	}
	if err = tx.fc.wp.opRollback(tx.transHandle); err != nil {
		return err
	}
	err = tx.endResponse(ctx)
	if isServerError(err) {
		// The server still holds the transaction. Reusing fc.tx would overwrite its
		// handle and leak it with its locks, so the connection is dropped instead
		// (IsValid) and the server releases it on disconnect.
		tx.fc.wp.desynced = true
	}
	tx.retire(err)
	return err
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
