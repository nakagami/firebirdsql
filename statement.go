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
    )


type _result struct {
}
func (r *_result) LastInsertId() (int64, error) {
    return 0, nil
}

func (r *_result) RowsAffected() (int64, error) {
    return 0, nil
}


type firebirdsqlStmt struct {
    wp *wireProtocol
    stmtHandle int32
    tx *firebirdsqlTx
    xsqlda []xSQLVAR
    stmtType int32
}

func (stmt *firebirdsqlStmt) Close() (err error) {
    stmt.wp.opFreeStatement(stmt.stmtHandle, 2)     // DSQL_drop
    return
}

func (stmt *firebirdsqlStmt) NumInput() int {
    return -1
}

func (stmt *firebirdsqlStmt) Exec(args []driver.Value) (result driver.Result, err error) {
    stmt.wp.opExecute(stmt.stmtHandle, stmt.tx.transHandle, args)
    _, _, _, err = stmt.wp.opResponse()

    result = new(_result)
    return
}

func (stmt *firebirdsqlStmt) Query(args []driver.Value) (rows driver.Rows, err error) {
    stmt.wp.opExecute(stmt.stmtHandle, stmt.tx.transHandle, args)
    _, _, _, err = stmt.wp.opResponse()
    rows = newFirebirdsqlRows(stmt)
    return
}

func newFirebirdsqlStmt(fc *firebirdsqlConn, query string) (stmt *firebirdsqlStmt, err error) {
    stmt = new(firebirdsqlStmt)
    stmt.wp = fc.wp
    stmt.tx = fc.tx
    fc.wp.opAllocateStatement()
    stmt.stmtHandle, _, _, err = fc.wp.opResponse()
    if err != nil {
        return
    }
    fc.wp.opPrepareStatement(stmt.stmtHandle, stmt.tx.transHandle, query)
    _, _, buf, err := fc.wp.opResponse()

    stmt.stmtType, stmt.xsqlda, err = fc.wp.parse_xsqlda(buf, stmt.stmtHandle)

    return
}
