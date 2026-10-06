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
	"fmt"
	"os"
	"time"
)

type firebirdsqlStmt struct {
	fc           *firebirdsqlConn
	queryString  string
	stmtHandle   int32
	resultXsqlda []xSQLVAR
	inputXsqlda  []xSQLVAR
	blr          []byte
	stmtType     int32
	activeBatch  *PreparedBatch
}

// freeStatement sends op_free_statement and reads its ack (or leaves it to the lazy
// drain). It does not commit; see commitAutocommit. DSQL_drop invalidates the handle
// whatever the outcome. On a desynced wire nothing is sent: the connection is being
// discarded, and the server frees the statement with it.
func (stmt *firebirdsqlStmt) freeStatement(mode int32) (err error) {
	if mode == DSQL_drop {
		defer func() { stmt.stmtHandle = -1 }()
		if stmt.activeBatch != nil {
			stmt.activeBatch.releaseBeforeFree()
			stmt.activeBatch = nil
		}
	}
	if err = stmt.fc.checkWire(); err != nil {
		return err
	}
	err = stmt.fc.wp.opFreeStatement(stmt.stmtHandle, mode)
	if err != nil {
		return err
	}
	if (stmt.fc.wp.acceptType & ptype_MASK) == ptype_lazy_send {
		stmt.fc.wp.lazyResponseCount++
	} else {
		// Teardown read: bound it so a silent wire can't hang rows.Close()/stmt.Close()
		// (reached automatically by database/sql's awaitDone on a mid-fetch ctx deadline).
		_, _, _, err = stmt.fc.wp.opResponseTimeout(abandonReadTimeout)
	}
	return err
}

// closeMode reports how Stmt.Close / rows.Close free this statement: DSQL_drop when
// dropping, DSQL_close for an open select cursor, ok=false when there is nothing to free.
func (stmt *firebirdsqlStmt) closeMode(drop bool) (mode int32, ok bool) {
	switch {
	case stmt.stmtHandle == -1:
		return 0, false
	case drop:
		return DSQL_drop, true
	case stmt.stmtType == isc_info_sql_stmt_select,
		stmt.stmtType == isc_info_sql_stmt_select_for_upd:
		return DSQL_close, true
	}
	return 0, false
}

// commitAutocommit commits the autocommit transaction, if one is active, reading the reply
// under ctx (see endResponse). needBegin means no transaction is active (for example
// database/sql closing a Tx's statements after Tx.Commit), so there is nothing to commit.
func (stmt *firebirdsqlStmt) commitAutocommit(ctx context.Context) error {
	if stmt.fc.tx.isAutocommit && !stmt.fc.tx.needBegin {
		return stmt.fc.tx.commitRetaining(ctx)
	}
	return nil
}

func (stmt *firebirdsqlStmt) Close() error {
	if _, ok := stmt.closeMode(true); !ok {
		return nil
	}
	err := stmt.freeStatement(DSQL_drop)
	// No context: the commit takes the bounded teardown read; it runs only for an
	// autocommit statement. Close's error reaches the caller for statements prepared on
	// a sql.Conn or in a Tx and for Raw callers; database/sql discards it for db.Prepare
	// statements, and fc.exec's deferred Close ignores it. A failed read marks the wire
	// desynced, so the connection is discarded either way.
	return closeErr(err, stmt.commitAutocommit(teardownCtx))
}

// closeErr combines the errors of a statement's free and of the autocommit commit that
// follows it. A lone error is returned as is. A commit refused because the free had
// already flagged the wire (errConnDesynced) adds nothing to the free's error.
func closeErr(freeErr, commitErr error) error {
	switch {
	case commitErr == nil:
		return freeErr
	case freeErr == nil:
		return commitErr
	case errors.Is(commitErr, errConnDesynced):
		return freeErr
	}
	return errors.Join(freeErr, commitErr)
}

func (stmt *firebirdsqlStmt) NumInput() int {
	return -1
}

// withCancelWatcher runs fn — a blocking wire *read* (opResponse, opSqlResponse,
// or opFetchResponse) — while a watcher goroutine waits on ctx and fires op_cancel
// if ctx is canceled first. The watcher is always joined before this returns, so no
// op_cancel can still be in flight when the caller resumes writing the wire on the
// main goroutine; that join is what keeps the unsynchronized send buffer
// (wireProtocol.buf, the bufio.Writer, the write cipher) race-free.
//
// fn MUST be a read: op_cancel writes p.buf, which is only safe to overlap a read
// (opResponse reads into a fresh buffer via recvPackets; wireChannel keeps read and
// write state separate). Never wrap a wire write (e.g. opFetch) with this helper.
//
// The join can't hang: callers set SetDeadline(ctx.Deadline()+3s) before calling,
// bounding the watcher's op_cancel write (and op_cancel is an 8-byte packet onto an
// already-flushed buffer, so it returns promptly even without a deadline).
func (stmt *firebirdsqlStmt) withCancelWatcher(ctx context.Context, fn func() error) error {
	if ctx.Done() == nil {
		// Context can never be canceled; skip the watcher goroutine entirely.
		return fn()
	}
	stop := make(chan struct{})
	watcherDone := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			stmt.fc.wp.opCancel(fb_cancel_raise)
		case <-stop:
		}
		close(watcherDone)
	}()
	err := fn()
	close(stop)
	<-watcherDone // join: the watcher (and any in-flight op_cancel) has finished past here
	return err
}

// awaitContextDone waits, at most a second, for ctx.Done() once
// contextErrOrDeadlineExceeded has reported ctx as over. That check can run ahead of the
// context's timer, and database/sql retries driver.ErrBadConn unless it sees the context
// done, so an ErrBadConn returned before Done is closed could run the statement again.
func awaitContextDone(ctx context.Context) {
	select {
	case <-ctx.Done():
		return
	default:
	}
	t := time.NewTimer(time.Second)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C: // a context whose Done never follows its deadline
	}
}

// disposeRead disposes of the read of a statement's reply (run under withCancelWatcher and
// enforceDeadline), once op_execute or its like went out. It returns nil only when the
// reply was a success read with ctx still live; the caller then goes on (and may commit).
//   - The OS-deadline fallback fired: send op_cancel and read the ack (cancelAndDrain).
//   - ctx ended, before or after the reply: abandon (not committed, ErrBadConn once ctx is
//     done; the wire is flagged unless the reply was a server error).
//   - Otherwise a failed read: readFailed (no ErrBadConn, the statement may have run).
//
// Failed reads are judged by markUnlessReplyRead. A success reply read after the
// deadline is flagged too, though the wire is in step: the flag is what keeps that work
// from being committed (nothing more is written; dropping the connection rolls it back).
func (stmt *firebirdsqlStmt) disposeRead(ctx context.Context, err error) error {
	if errors.Is(err, os.ErrDeadlineExceeded) {
		// The read was interrupted before the server's reply; cancel and read the
		// server's answer (its cancellation ack, or the reply itself).
		err = stmt.cancelAndDrain()
	}
	if cerr := contextErrOrDeadlineExceeded(ctx); cerr != nil {
		if err == nil { // a success reply, read after the deadline
			err = cerr
		}
		return stmt.abandon(ctx, err)
	}
	if err != nil {
		return stmt.readFailed(err)
	}
	return nil
}

// abandon disposes of a statement read around which ctx ended: a reply read after the
// deadline (err is nil or the context error), or a read that failed meanwhile. A server
// error means the statement did not complete (the server undid its work) and the wire is
// in step; otherwise the statement may have run, so the wire is marked desynced. The
// error is tagged driver.ErrBadConn after awaitContextDone, which gives the context's
// timer up to a second to close Done: database/sql then refuses to retry, and reports
// the context error.
func (stmt *firebirdsqlStmt) abandon(ctx context.Context, err error) error {
	if err == nil { // cancelAndDrain read the statement's own success reply
		err = contextErrOrDeadlineExceeded(ctx)
	}
	stmt.fc.wp.markUnlessReplyRead(err)
	awaitContextDone(ctx)
	return fmt.Errorf("%w: %w", err, driver.ErrBadConn)
}

// readFailed disposes of a statement read that failed while ctx is live, after op_execute
// went out. Without a server reply the wire is marked desynced: nothing more is written
// to it, so neither fc.exec's deferred Stmt.Close nor a later Tx.Commit can commit the
// work, and dropping the connection makes the server roll it back. The statement may have
// run, so a driver.ErrBadConn from the wire parser is taken out: database/sql would run it
// again elsewhere, repeating whatever the server does not roll back (generators,
// autonomous transactions, external calls).
func (stmt *firebirdsqlStmt) readFailed(err error) error {
	stmt.fc.wp.markUnlessReplyRead(err)
	return stripBadConn("statement reply", err)
}

func contextErrOrDeadlineExceeded(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if dl, ok := ctx.Deadline(); ok && !time.Now().Before(dl) {
		return context.DeadlineExceeded
	}
	return nil
}

// enforceDeadline mirrors ctx's deadline onto the connection at the OS socket level
// (ctx.Deadline()+3s) and returns a closure that clears it. It is the fallback that
// enforces the context deadline when Go's sysmon timer is starved by a CPU-bound query
// and the withCancelWatcher goroutine can't run. The +3s margin lets the watcher's
// op_cancel win the race when scheduling is healthy. No-op when ctx has no deadline.
// Use as:  defer stmt.enforceDeadline(ctx)()
func (stmt *firebirdsqlStmt) enforceDeadline(ctx context.Context) func() {
	dl, ok := ctx.Deadline()
	if !ok {
		return func() {}
	}
	stmt.fc.wp.conn.SetDeadline(dl.Add(3 * time.Second))
	return func() { stmt.fc.wp.conn.SetDeadline(time.Time{}) }
}

// cancelAndDrain is called when the OS-level connection deadline fires before
// the server responded (a fallback for Go timer starvation on loaded machines).
// It resets the connection deadline, sends op_cancel so the server cleans up,
// reads the resulting error response, and returns it.
func (stmt *firebirdsqlStmt) cancelAndDrain() error {
	stmt.fc.wp.conn.SetDeadline(time.Time{}) // re-enable I/O before the op_cancel write
	stmt.fc.wp.opCancel(fb_cancel_raise)
	_, _, _, err := stmt.fc.wp.opResponseTimeout(abandonReadTimeout)
	return err
}

// ensureInputXsqlda fetches bind-parameter metadata on first execute with args.
// It records the attempt by leaving inputXsqlda as a non-nil empty slice when the
// server returns no metadata, so we don't re-issue the info request on every call.
func (stmt *firebirdsqlStmt) ensureInputXsqlda(args []driver.Value) error {
	if len(args) == 0 || stmt.inputXsqlda != nil {
		return nil
	}
	xs, err := stmt.fc.wp._fetchBindXsqlda(stmt.stmtHandle)
	if err != nil {
		return err
	}
	if xs == nil {
		xs = []xSQLVAR{}
	}
	stmt.inputXsqlda = xs
	return nil
}

// prepareExecute runs the round-trips that precede op_execute (lazy begin,
// re-prepare of a freed handle, bind metadata) bounded by ctx. It returns the
// statement to execute, which is a new one when the handle had been freed.
func (stmt *firebirdsqlStmt) prepareExecute(ctx context.Context, args []driver.Value) (*firebirdsqlStmt, error) {
	err := stmt.fc.wp.withContextDeadline(ctx, func() error {
		if stmt.fc.tx.needBegin {
			if err := stmt.fc.tx.begin(); err != nil {
				return err
			}
		}
		if stmt.stmtHandle == -1 {
			s, err := newFirebirdsqlStmt(stmt.fc, stmt.queryString)
			if err != nil {
				return err
			}
			stmt = s
		}
		return stmt.ensureInputXsqlda(args)
	})
	return stmt, err
}

func (stmt *firebirdsqlStmt) exec(ctx context.Context, args []driver.Value) (result driver.Result, err error) {
	if err = stmt.fc.checkWire(); err != nil {
		return
	}
	if stmt, err = stmt.prepareExecute(ctx, args); err != nil {
		return
	}
	err = stmt.fc.wp.opExecute(stmt, args, stmt.inputXsqlda)
	if err != nil {
		return
	}

	// Fallback OS-level deadline (see enforceDeadline): unblocks the read when sysmon
	// is starved; withCancelWatcher's op_cancel still fires first when healthy.
	defer stmt.enforceDeadline(ctx)()

	err = stmt.withCancelWatcher(ctx, func() error {
		_, _, _, e := stmt.fc.wp.opResponse()
		return e
	})

	if err = stmt.disposeRead(ctx, err); err != nil {
		return result, err
	}

	err = stmt.fc.wp.opInfoSql(stmt.stmtHandle, []byte{isc_info_sql_records})
	if err != nil {
		// op_execute already ran: no ErrBadConn, or database/sql would run it again.
		return result, stmt.readFailed(err)
	}

	_, _, buf, err := stmt.fc.wp.opResponse()
	var countErr error
	if err != nil {
		// The statement already ran. The read is bounded by the still-armed enforceDeadline;
		// if that fired, cancel and read the server's answer first. A server error is count
		// metadata (RowsAffected's error, the work is committed); otherwise, once ctx is over,
		// abandon (not committed), else readFailed (wire flagged, not committed, no retry).
		if errors.Is(err, os.ErrDeadlineExceeded) {
			err = stmt.cancelAndDrain()
		}
		switch {
		case isServerError(err):
			// The statement already ran and the reply was read in full: a refused records
			// request is count metadata, like a malformed one below, not Exec's error.
			countErr, err = err, nil
		case contextErrOrDeadlineExceeded(ctx) != nil:
			return result, stmt.abandon(ctx, err)
		default:
			return result, stmt.readFailed(err)
		}
	}

	var records statementRecords
	var rowcount int64
	if countErr == nil {
		records, countErr = decodeStatementRecords(buf)
	}
	if countErr == nil {
		rowcount, countErr = records.rowsAffected(stmt.stmtType)
	}
	// Execution already succeeded. Invalid count metadata belongs to RowsAffected,
	// never to Exec's error (and must not prevent the existing autocommit).
	if countErr != nil {
		result = &firebirdsqlResultCountError{err: countErr}
	} else {
		result = &firebirdsqlResult{affectedRows: rowcount}
	}

	if stmt.fc.tx.isAutocommit {
		// Bounded by ctx alone: the reply carries the commit's server-side work.
		if cerr := stmt.fc.tx.commitRetaining(ctx); cerr != nil {
			return result, cerr
		}
	}
	return
}

func (stmt *firebirdsqlStmt) Exec(args []driver.Value) (result driver.Result, err error) {
	return stmt.exec(context.Background(), args)
}

func (stmt *firebirdsqlStmt) query(ctx context.Context, args []driver.Value) (driver.Rows, error) {
	var rows driver.Rows
	var err error
	var result []driver.Value

	if err = stmt.fc.checkWire(); err != nil {
		return nil, err
	}
	if stmt, err = stmt.prepareExecute(ctx, args); err != nil {
		return nil, err
	}

	if stmt.stmtType == isc_info_sql_stmt_exec_procedure {
		err = stmt.fc.wp.opExecute2(stmt, args, stmt.blr, stmt.inputXsqlda)
		if err != nil {
			return nil, err
		}

		defer stmt.enforceDeadline(ctx)()

		// op_sql_response and its trailing op_response are both wire reads with no
		// main-goroutine write between them, so one joined watcher covers both.
		err = stmt.withCancelWatcher(ctx, func() error {
			var e error
			if result, e = stmt.fc.wp.opSqlResponse(stmt.resultXsqlda); e != nil {
				return e
			}
			_, _, _, e = stmt.fc.wp.opResponse()
			return e
		})
		if err = stmt.disposeRead(ctx, err); err != nil {
			return nil, err
		}

		rows = newFirebirdsqlRows(ctx, stmt, result)
	} else {
		err := stmt.fc.wp.opExecute(stmt, args, stmt.inputXsqlda)
		if err != nil {
			return nil, err
		}

		defer stmt.enforceDeadline(ctx)()

		err = stmt.withCancelWatcher(ctx, func() error {
			_, _, _, e := stmt.fc.wp.opResponse()
			return e
		})

		if err = stmt.disposeRead(ctx, err); err != nil {
			return nil, err
		}

		rows = newFirebirdsqlRows(ctx, stmt, nil)
	}
	return rows, err
}

func (stmt *firebirdsqlStmt) Query(args []driver.Value) (rows driver.Rows, err error) {
	return stmt.query(context.Background(), args)
}

func newFirebirdsqlStmt(fc *firebirdsqlConn, query string) (stmt *firebirdsqlStmt, err error) {
	stmt = new(firebirdsqlStmt)
	stmt.fc = fc
	stmt.queryString = query

	err = stmt.fc.wp.opAllocateStatement()
	if err != nil {
		return nil, err
	}

	if (stmt.fc.wp.acceptType & ptype_MASK) == ptype_lazy_send {
		stmt.fc.wp.lazyResponseCount++
		stmt.stmtHandle = -1
	} else {
		stmt.stmtHandle, _, _, err = stmt.fc.wp.opResponse()
		if err != nil {
			return
		}
	}

	err = stmt.fc.wp.opPrepareStatement(stmt.stmtHandle, stmt.fc.tx.transHandle, query)
	if err != nil {
		return nil, err
	}

	if (stmt.fc.wp.acceptType&ptype_MASK) == ptype_lazy_send && stmt.fc.wp.lazyResponseCount > 0 {
		stmt.fc.wp.lazyResponseCount--
		stmt.stmtHandle, _, _, _ = stmt.fc.wp.opResponse()
	}

	_, _, buf, err := stmt.fc.wp.opResponse()
	if err != nil {
		return
	}

	stmt.stmtType, stmt.resultXsqlda, err = stmt.fc.wp.parse_xsqlda(buf, stmt.stmtHandle)
	if err != nil {
		return nil, err
	}

	stmt.blr = calcBlr(stmt.resultXsqlda)

	return
}
