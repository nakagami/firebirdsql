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
}

func (stmt *firebirdsqlStmt) freeStatement(mode int32) error {
	err := stmt.fc.wp.opFreeStatement(stmt.stmtHandle, mode)
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
	if stmt.fc.tx.isAutocommit {
		stmt.fc.tx.commitRetainging()
	}
	return err
}

func (stmt *firebirdsqlStmt) Close() error {
	if stmt.stmtHandle == -1 {
		return nil
	}
	defer func() { stmt.stmtHandle = -1 }()
	return stmt.freeStatement(DSQL_drop)
}

func (stmt *firebirdsqlStmt) closeCursor() error {
	if stmt.stmtHandle == -1 ||
		(stmt.stmtType != isc_info_sql_stmt_select &&
			stmt.stmtType != isc_info_sql_stmt_select_for_upd) {
		return nil
	}
	return stmt.freeStatement(DSQL_close)
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

func (stmt *firebirdsqlStmt) exec(ctx context.Context, args []driver.Value) (result driver.Result, err error) {
	if stmt.fc.tx.needBegin {
		if err = stmt.fc.tx.begin(); err != nil {
			return
		}
	}
	if stmt.stmtHandle == -1 {
		stmt, err = newFirebirdsqlStmt(stmt.fc, stmt.queryString)
		if err != nil {
			return
		}
	}
	if err = stmt.ensureInputXsqlda(args); err != nil {
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

	// Deadline-abandon disposition: drain the cancel ack if the OS deadline fired, then evict
	// (ErrBadConn) if the ctx deadline has passed. This block is repeated verbatim at the other
	// three blocking-read sites (exec's opInfoSql read; query's exec_procedure and select reads) —
	// keep all four in sync.
	if err != nil {
		if errors.Is(err, os.ErrDeadlineExceeded) {
			// OS deadline fired (sysmon was starved). The read was cleanly
			// interrupted before the server sent any data; send op_cancel now
			// and read the server's cancellation acknowledgement.
			err = stmt.cancelAndDrain()
		}
		if contextErrOrDeadlineExceeded(ctx) != nil {
			return result, fmt.Errorf("%w: %w", err, driver.ErrBadConn)
		}
		return
	}
	if cerr := contextErrOrDeadlineExceeded(ctx); cerr != nil {
		return result, fmt.Errorf("%w: %w", cerr, driver.ErrBadConn)
	}

	err = stmt.fc.wp.opInfoSql(stmt.stmtHandle, []byte{isc_info_sql_records})
	if err != nil {
		return
	}

	_, _, buf, err := stmt.fc.wp.opResponse()
	if err != nil {
		// Mirror the 1st-read defense (above): this opInfoSql records read is bounded by the
		// still-armed enforceDeadline, so it cannot hang, but on a ctx-deadline abandon the
		// wire is desynced — cancel/drain and evict rather than pool the poisoned conn.
		if errors.Is(err, os.ErrDeadlineExceeded) {
			err = stmt.cancelAndDrain()
		}
		if contextErrOrDeadlineExceeded(ctx) != nil {
			return result, fmt.Errorf("%w: %w", err, driver.ErrBadConn)
		}
		return
	}

	var rowcount int64
	if len(buf) >= 32 {
		if stmt.stmtType == isc_info_sql_stmt_select ||
			stmt.stmtType == isc_info_sql_stmt_select_for_upd {
			rowcount = int64(bytes_to_int32(buf[20:24]))
		} else {
			rowcount = int64(bytes_to_int32(buf[27:31]) + bytes_to_int32(buf[6:10]) + bytes_to_int32(buf[13:17]))
		}
	} else {
		rowcount = 0
	}

	result = &firebirdsqlResult{
		affectedRows: rowcount,
	}
	if stmt.fc.tx.isAutocommit {
		if cerr := stmt.fc.tx.commitRetainging(); cerr != nil {
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

	if stmt.fc.tx.needBegin {
		if err = stmt.fc.tx.begin(); err != nil {
			return nil, err
		}
	}

	if stmt.stmtHandle == -1 {
		stmt, err = newFirebirdsqlStmt(stmt.fc, stmt.queryString)
		if err != nil {
			return nil, err
		}
	}

	if err = stmt.ensureInputXsqlda(args); err != nil {
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
		if err != nil {
			if errors.Is(err, os.ErrDeadlineExceeded) {
				err = stmt.cancelAndDrain()
			}
			if contextErrOrDeadlineExceeded(ctx) != nil {
				return nil, fmt.Errorf("%w: %w", err, driver.ErrBadConn)
			}
			return nil, err
		}
		if cerr := contextErrOrDeadlineExceeded(ctx); cerr != nil {
			return nil, fmt.Errorf("%w: %w", cerr, driver.ErrBadConn)
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

		if err != nil {
			if errors.Is(err, os.ErrDeadlineExceeded) {
				err = stmt.cancelAndDrain()
			}
			if contextErrOrDeadlineExceeded(ctx) != nil {
				return nil, fmt.Errorf("%w: %w", err, driver.ErrBadConn)
			}
			return nil, err
		}
		if cerr := contextErrOrDeadlineExceeded(ctx); cerr != nil {
			return nil, fmt.Errorf("%w: %w", cerr, driver.ErrBadConn)
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
