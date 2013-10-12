/*******************************************************************************
The MIT License (MIT)

Copyright (c) 2013 Hajime Nakagami

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
	"fmt"
	"io"
    "container/list"
	"database/sql/driver"
)

type firebirdsqlRows struct {
    stmt *firebirdsqlStmt
    chunk *list.List
    rowElem *list.Element
    moreData bool
}

func newFirebirdsqlRows(stmt *firebirdsqlStmt) *firebirdsqlRows {
	rows :=  new(firebirdsqlRows)
    rows.stmt = stmt
    if stmt.stmtType == isc_info_sql_stmt_select {
        rows.moreData = true
    }
    return rows
}

func (rows *firebirdsqlRows) Columns() []string {
    columns := make([]string, len(rows.stmt.xsqlda))
    for i, x := range rows.stmt.xsqlda {
        columns[i] = x.aliasname
    }
	return columns
}

func (rows *firebirdsqlRows) Close() (er error) {
	fmt.Println("firebirdsqlRows.Close()")
	return
}

func (rows *firebirdsqlRows) Next(dest []driver.Value) (err error) {
    if rows.rowElem == nil && rows.moreData == false {
        // No data
		err = io.EOF
        return
    } else if rows.rowElem == nil && rows.moreData == true {
        // Get one chunk
        rows.stmt.wp.opFetch(rows.stmt.stmtHandle, rows.stmt.blr)
        rows.chunk, err = rows.stmt.wp.opFetchResponse(rows.stmt.stmtHandle, rows.stmt.xsqlda)
        rows.rowElem = rows.chunk.Front()
    } else {
        rows.rowElem = rows.rowElem.Next()
    }

    i := 0
    var row *list.List
    row = rows.rowElem.Value
    for e := row.Front(); e != nil; e = e.Next() {
        dest[i] = e.Value
        i++
    }

	return
}
