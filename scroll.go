/*******************************************************************************
The MIT License (MIT)

Copyright (c) 2026 Alexey Kovyazin

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
)

// ScrollableStmt is a prepared statement executed with a protocol-18 scrollable
// cursor. It is not a database/sql Rows implementation; obtain it through
// sql.Conn.Raw by asserting the driver connection to an interface with
// QueryScrollable.
type ScrollableStmt struct {
	stmt *firebirdsqlStmt
}

// QueryScrollable prepares and executes query with CURSOR_TYPE_SCROLLABLE.
// Requires negotiated wire protocol >= 18. Reach via sql.Conn.Raw:
//
//	var sc *firebirdsql.ScrollableStmt
//	err := sqlConn.Raw(func(dc any) error {
//	    q, ok := dc.(interface {
//	        QueryScrollable(context.Context, string, ...driver.Value) (*firebirdsql.ScrollableStmt, error)
//	    })
//	    if !ok {
//	        return fmt.Errorf("scrollable cursors not available")
//	    }
//	    var e error
//	    sc, e = q.QueryScrollable(ctx, "SELECT ... ORDER BY ...")
//	    return e
//	})
func (fc *firebirdsqlConn) QueryScrollable(ctx context.Context, query string, args ...driver.Value) (*ScrollableStmt, error) {
	if fc.wp.protocolVersion < PROTOCOL_VERSION18 {
		return nil, fmt.Errorf("firebirdsql: scrollable cursors require wire protocol 18+, got %d", fc.wp.protocolVersion)
	}
	if fc.tx.needBegin {
		if err := fc.tx.begin(); err != nil {
			return nil, err
		}
	}
	stmt, err := newFirebirdsqlStmt(fc, query)
	if err != nil {
		return nil, err
	}
	if stmt.stmtType == isc_info_sql_stmt_exec_procedure {
		_ = stmt.Close()
		return nil, errors.New("firebirdsql: scrollable cursors are not supported for EXECUTE PROCEDURE")
	}
	if err = stmt.ensureInputXsqlda(args); err != nil {
		_ = stmt.Close()
		return nil, err
	}

	prev := fc.wp.cursorFlags
	fc.wp.cursorFlags = CURSOR_TYPE_SCROLLABLE
	err = fc.wp.opExecute(stmt, args, stmt.inputXsqlda)
	fc.wp.cursorFlags = prev
	if err != nil {
		_ = stmt.Close()
		return nil, err
	}
	_, _, _, err = fc.wp.opResponse()
	if err != nil {
		_ = stmt.Close()
		return nil, err
	}
	return &ScrollableStmt{stmt: stmt}, nil
}

// Fetch fetches up to count rows using the given scroll orientation and offset.
// count must be > 0. For ScrollAbsolute / ScrollRelative, offset is the Firebird
// cursor offset; otherwise it is ignored (sent as 0).
func (s *ScrollableStmt) Fetch(orientation int32, offset int32, count int32) ([][]driver.Value, bool, error) {
	if s == nil || s.stmt == nil {
		return nil, false, driver.ErrBadConn
	}
	if count <= 0 {
		return nil, false, fmt.Errorf("firebirdsql: Fetch count must be > 0")
	}
	stmt := s.stmt
	if err := stmt.fc.wp.opFetchScroll(stmt.stmtHandle, stmt.blr, orientation, offset, count); err != nil {
		return nil, false, err
	}
	rows, more, err := stmt.fc.wp.opFetchResponse(stmt.stmtHandle, stmt.fc.tx.transHandle, stmt.resultXsqlda)
	if err != nil {
		return nil, false, err
	}
	for i := range rows {
		for j, v := range rows[i] {
			if stmt.resultXsqlda[j].sqltype == SQL_TYPE_BLOB && v != nil {
				blobId := v.([]byte)
				blob, berr := stmt.fc.wp.getBlobSegments(blobId, stmt.fc.tx.transHandle)
				if berr != nil {
					return nil, false, berr
				}
				rows[i][j] = blob
			}
		}
	}
	return rows, more, nil
}

// FetchOne is Fetch with count=1.
func (s *ScrollableStmt) FetchOne(orientation int32, offset int32) ([]driver.Value, error) {
	rows, _, err := s.Fetch(orientation, offset, 1)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, io.EOF
	}
	return rows[0], nil
}

// Columns returns result column names.
func (s *ScrollableStmt) Columns() []string {
	if s == nil || s.stmt == nil {
		return nil
	}
	cols := make([]string, len(s.stmt.resultXsqlda))
	for i, x := range s.stmt.resultXsqlda {
		cols[i] = x.aliasname
		if cols[i] == "" {
			cols[i] = x.fieldname
		}
	}
	return cols
}

// Close releases the scrollable statement.
func (s *ScrollableStmt) Close() error {
	if s == nil || s.stmt == nil {
		return nil
	}
	err := s.stmt.Close()
	s.stmt = nil
	return err
}
