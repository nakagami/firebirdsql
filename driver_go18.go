/*******************************************************************************
The MIT License (MIT)

Copyright (c) 2016 Hajime Nakagami

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

// +build go1.8

package firebirdsql

import (
	"database/sql/driver"
	"errors"

	"context"
)

func (stmt *firebirdsqlStmt) ExecContext(ctx context.Context, namedargs []driver.NamedValue) (result driver.Result, err error) {
	args := make([]driver.Value, len(namedargs))
	for i, nv := range namedargs {
		args[i] = nv.Value
	}

	return stmt.exec(ctx, args)
}

func (stmt *firebirdsqlStmt) QueryContext(ctx context.Context, namedargs []driver.NamedValue) (rows driver.Rows, err error) {
	args := make([]driver.Value, len(namedargs))
	for i, nv := range namedargs {
		args[i] = nv.Value
	}

	return stmt.query(ctx, args)
}

func (fc *firebirdsqlConn) BeginContext(ctx context.Context) (driver.Tx, error) {
	isolationLevel := 0
	readOnly := false
	return fc.begin(isolationLevel, readOnly)
}

func (fc *firebirdsqlConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	return fc.prepare(ctx, query)
}

func (fc *firebirdsqlConn) ExecContext(ctx context.Context, query string, namedargs []driver.NamedValue) (result driver.Result, err error) {
	args := make([]driver.Value, len(namedargs))
	for i, nv := range namedargs {
		args[i] = nv.Value
	}
	return fc.exec(ctx, query, args)
}

func (fc *firebirdsqlConn) Ping(ctx context.Context) error {
	if fc == nil {
		return errors.New("Connection was closed")
	}
	return nil
}

func (fc *firebirdsqlConn) QueryContext(ctx context.Context, query string, namedargs []driver.NamedValue) (rows driver.Rows, err error) {
	args := make([]driver.Value, len(namedargs))
	for i, nv := range namedargs {
		args[i] = nv.Value
	}
	return fc.query(ctx, query, args)
}
