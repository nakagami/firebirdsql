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
	badConn          bool // set when a fetch was abandoned under an expired ctx (wire desynced)
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
	var err error
	if rows.closeStmtOnClose {
		err = rows.stmt.Close()
	} else {
		err = rows.stmt.closeCursor()
	}
	// database/sql drives connection eviction off *this* return value (Rows.close passes
	// rowsi.Close()'s result to releaseConn -> putConn), not off Next's error. So when a fetch
	// was abandoned mid-stream under an expired context the wire is desynced — return
	// ErrBadConn here so the poisoned conn is evicted, not pooled (otherwise the next query
	// reads leftover fetch bytes where it expects op_response -> "Error op_response:N").
	if rows.badConn {
		return driver.ErrBadConn
	}
	return err
}

// materializeArray turns a fetched array column value (the raw 8-byte server
// array id) into an ArrayValue by fetching the slice (op_get_slice) and
// decoding its elements. Runs in Next(), after the row has been fully consumed
// from the wire — a get_slice round-trip must not run mid-row while the fetch
// response is still streaming.
func (rows *firebirdsqlRows) materializeArray(x xSQLVAR, v driver.Value) (driver.Value, error) {
	if x.arrayMeta == nil {
		return nil, fmt.Errorf("firebirdsql: array column %s: metadata unavailable", x.aliasname)
	}
	if len(v.([]byte)) != 8 {
		return nil, fmt.Errorf("firebirdsql: array column %s: short array id (%d bytes): %w", x.aliasname, len(v.([]byte)), driver.ErrBadConn)
	}
	id := bytes_to_bint64(v.([]byte))
	if id == 0 {
		return nil, nil
	}
	data, err := rows.stmt.fc.wp.opGetSlice(rows.stmt.fc.tx.transHandle, id, x.arrayMeta)
	if err != nil {
		return nil, err
	}
	elements, err := rows.stmt.fc.wp.decodeSliceElements(x.arrayMeta, data)
	if err != nil {
		return nil, err
	}
	return ArrayValue{Meta: x.arrayMeta, Elements: elements}, nil
}

func (rows *firebirdsqlRows) Next(dest []driver.Value) (err error) {
	// contextErrOrDeadlineExceeded folds in the timer-starvation fallback: it returns the
	// context error, or DeadlineExceeded when the wall clock has passed ctx.Deadline() even
	// though ctx.Err() is still nil (Go's sysmon delayed by a CPU-bound Firebird query). We
	// fire op_cancel, so the wire state is then uncertain — mark the conn bad so Close() evicts
	// it (the eviction lever; see Close). The caller still sees the clean context error.
	if cerr := contextErrOrDeadlineExceeded(rows.ctx); cerr != nil {
		rows.stmt.fc.wp.opCancel(fb_cancel_raise)
		rows.badConn = true
		return cerr
	}

	if rows.stmt.stmtType == isc_info_sql_stmt_exec_procedure {
		if rows.result != nil {
			for i, v := range rows.result {
				if rows.stmt.resultXsqlda[i].sqltype == SQL_TYPE_ARRAY && v != nil {
					dest[i], err = rows.materializeArray(rows.stmt.resultXsqlda[i], v)
					if err != nil {
						if errors.Is(err, driver.ErrBadConn) {
							rows.badConn = true
						}
						return
					}
				} else if rows.stmt.resultXsqlda[i].sqltype == SQL_TYPE_BLOB && v != nil {
					blobId := v.([]byte)
					var blob []byte
					blob, err = rows.stmt.fc.wp.getBlobSegments(blobId, rows.stmt.fc.tx.transHandle)
					if err != nil {
						if errors.Is(err, driver.ErrBadConn) {
							rows.badConn = true
						}
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
		err = rows.stmt.fc.wp.opFetch(rows.stmt.stmtHandle, rows.stmt.blr)
		if err == nil {
			err = rows.stmt.withCancelWatcher(rows.ctx, func() error {
				var e error
				rows.currentChunk, rows.moreData, e = rows.stmt.fc.wp.opFetchResponse(rows.stmt.stmtHandle, rows.stmt.fc.tx.transHandle, rows.stmt.resultXsqlda)
				return e
			})
		}

		if err != nil {
			// A ctx-deadline abandon (incl. the OS-deadline fallback firing while
			// ctx.Err() is still nil) leaves the fetch response bytes pending on the
			// wire — the conn is desynced. Mark it bad so Close() evicts it rather than
			// pooling a conn the next query would read misaligned ("Error op_response:N").
			// A genuine server/wire error keeps the wire synced, so it stays reusable.
			if cerr := contextErrOrDeadlineExceeded(rows.ctx); cerr != nil {
				rows.badConn = true // fetch abandoned mid-stream: wire desynced
				return cerr
			}
			// The fetch decode (readRow, opFetchResponse, getBlobSegments) tags a
			// malformed-length/count error with driver.ErrBadConn because it aborts
			// mid-stream with bytes left on the wire. Close() evicts off rows.badConn,
			// not this return value, so propagate the signal here or the desynced conn
			// gets pooled.
			if errors.Is(err, driver.ErrBadConn) {
				rows.badConn = true
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
		x := rows.stmt.resultXsqlda[i]
		if x.sqltype == SQL_TYPE_ARRAY && v != nil {
			dest[i], err = rows.materializeArray(x, v)
			if err != nil {
				// materializeArray tags wire-level failures with
				// driver.ErrBadConn; Close() evicts off rows.badConn.
				if errors.Is(err, driver.ErrBadConn) {
					rows.badConn = true
				}
				return
			}
		} else if x.sqltype == SQL_TYPE_BLOB && v != nil {
			blobId := v.([]byte)
			var blob []byte
			blob, err = rows.stmt.fc.wp.getBlobSegments(blobId, rows.stmt.fc.tx.transHandle)
			if err != nil {
				// Same as the fetch branch above: getBlobSegments tags its error with
				// driver.ErrBadConn, and Close() only evicts when rows.badConn is set here.
				if errors.Is(err, driver.ErrBadConn) {
					rows.badConn = true
				}
				return
			}
			if x.sqlsubtype == 1 {
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
