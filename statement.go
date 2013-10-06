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
    "errors"
    )

type firebirdsqlStmt struct {
    wp *wireProtocol
    stmtHandle int32
    tx *firebirdsqlTx
}

func (stmt *firebirdsqlStmt) Close() (err error) {
    return
}

func (stmt *firebirdsqlStmt) NumInput() int {
    return -1
}

func (stmt *firebirdsqlStmt) Exec(args []driver.Value) (driver.Result, error) {
    var err error
    err = errors.New("Not implement")
    return nil, err
}

func (stmt *firebirdsqlStmt) Query(args []driver.Value) (driver.Rows, error) {
    var err error
    err = errors.New("Not implement")
    return nil, err
}

func newFirebirdsqlStmt(fc *firebirdsqlConn, query string) (*firebirdsqlStmt, error) {
    var err error
    stmt := new(firebirdsqlStmt)
    stmt.wp = fc.wp
    stmt.tx = fc.tx
    fc.wp.opAllocateStatement()

    stmt.stmtHandle, _, _, err = fc.wp.opResponse()
    return stmt, err
}
