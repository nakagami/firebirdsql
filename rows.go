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
	"io"
	"reflect"
	"strings"
)

type firebirdsqlRows struct {
	ctx              context.Context
	stmt             *firebirdsqlStmt
	currentChunk     [][]driver.Value // rows fetched in the current chunk
	currentChunkIdx  int              // index of the current row within currentChunk
	moreData         bool
	result           []driver.Value
	closeStmtOnClose bool // true for internal stmts that should be dropped on rows.Close()
	badConn          bool // Close returns driver.ErrBadConn: the ctx ended, or a read left the wire desynced
	closed           bool // Close has run: a second call must not commit again
}

func newFirebirdsqlRows(ctx context.Context, stmt *firebirdsqlStmt, result []driver.Value) *firebirdsqlRows {
	rows := new(firebirdsqlRows)
	rows.ctx = ctx
	rows.stmt = stmt
	rows.result = result
	if stmt.stmtType == isc_info_sql_stmt_select ||
		stmt.stmtType == isc_info_sql_stmt_select_for_upd {
		rows.moreData = true
	}
	return rows
}

func (rows *firebirdsqlRows) Columns() []string {
	columns := make([]string, len(rows.stmt.resultXsqlda))
	for i, x := range rows.stmt.resultXsqlda {
		columns[i] = x.aliasname
		if rows.stmt.fc.columnNameToLower {
			columns[i] = strings.ToLower(columns[i])
		}
	}
	return columns
}

func (rows *firebirdsqlRows) Close() error {
	// A query's autocommit commit happens here: for db.Query / db.QueryRow (INSERT ...
	// RETURNING, EXECUTE PROCEDURE) and for a prepared statement run with Query, including
	// one that is not a SELECT (nothing to free then, but its work still needs committing).
	// The reply is read under the query's context like exec's: live on a normal close,
	// done when database/sql closes the rows on context expiry (then the bounded teardown
	// read). After a fetch or blob read that did not end in a reply read in full the wire is
	// flagged (readFailed), so nothing more is written to it.
	if rows.closed {
		return nil
	}
	rows.closed = true
	ctx := rows.ctx
	if rows.badConn {
		ctx = teardownCtx
	}
	var err error
	if mode, ok := rows.stmt.closeMode(rows.closeStmtOnClose); ok {
		err = rows.stmt.freeStatement(mode)
	}
	err = closeErr(err, rows.stmt.commitAutocommit(ctx))
	// database/sql drives connection eviction off *this* return value (Rows.close passes
	// rowsi.Close()'s result to releaseConn -> putConn), not off Next's error. A pooled
	// conn is evicted after a context ended or a read left the wire desynced. A Tx ignores
	// this error; it goes on unless the wire is flagged (then its next call is refused).
	if rows.badConn {
		return driver.ErrBadConn
	}
	return err
}

// readFailed disposes of a failed fetch or blob read by the wire rule
// (markUnlessReplyRead): a server error was read in full and leaves the wire in step, so a
// Tx can go on; any other failure marks the wire desynced, and Close then returns
// driver.ErrBadConn.
func (rows *firebirdsqlRows) readFailed(err error) {
	wp := rows.stmt.fc.wp
	if wp.markUnlessReplyRead(err); wp.desynced {
		rows.badConn = true
	}
}

func (rows *firebirdsqlRows) Next(dest []driver.Value) (err error) {
	// contextErrOrDeadlineExceeded folds in the timer-starvation fallback: it returns the
	// context error, or DeadlineExceeded when the wall clock has passed ctx.Deadline() even
	// though ctx.Err() is still nil (Go's sysmon delayed by a CPU-bound Firebird query).
	// Nothing is in flight here, so the wire stays in step. The op_cancel has no reply, and
	// Firebird drops one that reaches an idle attachment; badConn makes Close return
	// driver.ErrBadConn, so a pooled conn is evicted while a Tx goes on. The caller sees
	// the clean context error.
	if cerr := contextErrOrDeadlineExceeded(rows.ctx); cerr != nil {
		if rows.stmt.fc.checkWire() == nil {
			rows.stmt.fc.wp.opCancel(fb_cancel_raise)
		}
		// Nothing was in flight, so the wire is still in step: a Tx can go on.
		rows.badConn = true
		return cerr
	}

	if rows.stmt.stmtType == isc_info_sql_stmt_exec_procedure {
		if rows.result != nil {
			for i, v := range rows.result {
				if rows.stmt.resultXsqlda[i].sqltype == SQL_TYPE_BLOB && v != nil {
					blobId := v.([]byte)
					var blob []byte
					blob, err = rows.stmt.fc.wp.getBlobSegments(blobId, rows.stmt.fc.tx.transHandle)
					if err != nil {
						rows.readFailed(err)
						return
					}
					dest[i] = blob
				} else {
					dest[i] = v
				}
			}
			rows.result = nil
		} else {
			err = io.EOF
		}

		return
	}

	if rows.currentChunk != nil {
		rows.currentChunkIdx++
	}

	if rows.currentChunkIdx >= len(rows.currentChunk) && rows.moreData {
		// Mirror exec/query: bound the blocking fetch with an OS-level deadline a bit
		// past the context deadline. This unblocks a starved Go timer (sysmon) and —
		// just as importantly — bounds the watcher's op_cancel write, so the
		// withCancelWatcher join below cannot hang even if that write meets TCP
		// backpressure. (Unlike the single-response exec/query paths, a fetch is a
		// stream, so we do not cancelAndDrain on timeout — the connection is left to
		// be evicted rather than risk reading misaligned bytes mid-stream.)
		defer rows.stmt.enforceDeadline(rows.ctx)()

		// opFetch is a wire *write*; run it on the main goroutine with no watcher
		// live (a goroutine-fired op_cancel would race the write). Cancellation is
		// watched only around opFetchResponse — the blocking *read* — and the
		// watcher is joined inside withCancelWatcher before control returns, so it
		// can never overlap a later main-goroutine wire write (e.g. the stray
		// op_cancel at the top of the next Next() call).
		// A fetch is new work: on a wire another statement left out of step (same Tx or
		// sql.Conn) op_fetch would read that statement's reply.
		if err = rows.stmt.fc.checkWire(); err != nil {
			rows.badConn = true
			return
		}
		err = rows.stmt.fc.wp.opFetch(rows.stmt.stmtHandle, rows.stmt.blr)
		if err == nil {
			err = rows.stmt.withCancelWatcher(rows.ctx, func() error {
				var e error
				rows.currentChunk, rows.moreData, e = rows.stmt.fc.wp.opFetchResponse(rows.stmt.stmtHandle, rows.stmt.fc.tx.transHandle, rows.stmt.resultXsqlda)
				return e
			})
		}

		if err != nil {
			// The wire rule decides the wire, not the context: a fetch the server
			// answered with a status vector (isc_cancelled after the watcher's op_cancel,
			// or any refusal) was read in full, so a Tx survives it. A deadline abandon
			// (the OS-deadline fallback), EOF or a malformed frame leaves fetch bytes on
			// the wire, and readFailed flags it so the next request cannot read them.
			rows.readFailed(err)
			if cerr := contextErrOrDeadlineExceeded(rows.ctx); cerr != nil {
				rows.badConn = true
				return cerr
			}
			return
		}
		rows.currentChunkIdx = 0
	}

	if rows.currentChunkIdx >= len(rows.currentChunk) {
		err = io.EOF
		return
	}
	row := rows.currentChunk[rows.currentChunkIdx]
	for i, v := range row {
		if rows.stmt.resultXsqlda[i].sqltype == SQL_TYPE_BLOB && v != nil {
			blobId := v.([]byte)
			var blob []byte
			blob, err = rows.stmt.fc.wp.getBlobSegments(blobId, rows.stmt.fc.tx.transHandle)
			if err != nil {
				rows.readFailed(err) // same rule as the fetch branch above
				return
			}
			if rows.stmt.resultXsqlda[i].sqlsubtype == 1 {
				charset := rows.stmt.fc.wp.charset
				if s, ok := decodeCharset(blob, charset); ok {
					dest[i] = s
				} else {
					dest[i] = string(blob)
				}
			} else {
				dest[i] = blob
			}

		} else {
			dest[i] = v
		}
	}

	return
}

func (rows *firebirdsqlRows) ColumnTypeDatabaseTypeName(index int) string {
	return rows.stmt.resultXsqlda[index].typename()
}

func (rows *firebirdsqlRows) ColumnTypeLength(index int) (length int64, ok bool) {
	return int64(rows.stmt.resultXsqlda[index].displayLength()), true
}

func (rows *firebirdsqlRows) ColumnTypeNullable(index int) (nullable bool, ok bool) {
	return rows.stmt.resultXsqlda[index].null_ok, true
}

func (rows *firebirdsqlRows) ColumnTypePrecisionScale(index int) (precision, scale int64, ok bool) {
	return int64(rows.stmt.resultXsqlda[index].displayLength()), int64(rows.stmt.resultXsqlda[index].sqlscale), rows.stmt.resultXsqlda[index].hasPrecisionScale()
}

func (rows *firebirdsqlRows) ColumnTypeScanType(index int) reflect.Type {
	return rows.stmt.resultXsqlda[index].scantype()
}
