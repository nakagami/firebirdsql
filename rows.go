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
	"database/sql/driver"
	"fmt"
	"io"
)

type firebirdsqlRows struct {
    stmt *firebirdsqlStmt
	curNum int
}

func newFirebirdsqlRows(stmt *firebirdsqlStmt) *firebirdsqlRows {
	r :=  new(firebirdsqlRows)
    r.stmt = stmt
    return r
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
	fmt.Println("firebirdsqlRows.Next()")
	rows.curNum++
	if rows.curNum < 5 {
		dest[0] = rows.curNum
		dest[1] = "ABC"
	} else {
		err = io.EOF
	}

	return
}
