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
}

func newFirebirdsqlRows(ctx context.Context, stmt *firebirdsqlStmt, result []driver.Value) *firebirdsqlRows {
	rows := new(firebirdsqlRows)
	rows.ctx = ctx
	rows.stmt = stmt
	rows.result = result
	if stmt.stmtType == isc_info_sql_stmt_select {
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
	if rows.closeStmtOnClose {
		return rows.stmt.Close()
	}
	return rows.stmt.closeCursor()
}

func (rows *firebirdsqlRows) Next(dest []driver.Value) (err error) {
	if rows.ctx.Err() != nil {
		rows.stmt.fc.wp.opCancel(fb_cancel_raise)
		return rows.ctx.Err()
	}

	if rows.stmt.stmtType == isc_info_sql_stmt_exec_procedure {
		if rows.result != nil {
			for i, v := range rows.result {
				dest[i] = v
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
		// Get one chunk
		err = rows.stmt.fc.wp.opFetch(rows.stmt.stmtHandle, rows.stmt.blr)
		if err != nil {
			return err
		}
		rows.currentChunk, rows.moreData, err = rows.stmt.fc.wp.opFetchResponse(rows.stmt.stmtHandle, rows.stmt.fc.tx.transHandle, rows.stmt.resultXsqlda)
		if err != nil {
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
