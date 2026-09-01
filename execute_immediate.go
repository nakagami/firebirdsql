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
)

// ExecImmediate executes SQL without preparing a statement (op_execute_immediate).
// Reach via sql.Conn.Raw. Uses dialect 3 and does not send OpExecute-style trailers.
func (fc *firebirdsqlConn) ExecImmediate(ctx context.Context, sql string) error {
	if fc == nil || fc.wp == nil || fc.tx == nil {
		return driver.ErrBadConn
	}
	if fc.tx.needBegin {
		if err := fc.tx.begin(); err != nil {
			return err
		}
	}
	if err := fc.wp.opExecuteImmediate(fc.tx.transHandle, sql); err != nil {
		return err
	}

	// Borrow cancel/deadline helpers from a transient statement handle wrapper.
	tmp := &firebirdsqlStmt{fc: fc}
	defer tmp.enforceDeadline(ctx)()
	err := tmp.withCancelWatcher(ctx, func() error {
		_, _, _, e := fc.wp.opResponse()
		return e
	})
	if err != nil {
		return err
	}
	if fc.tx.isAutocommit {
		if cerr := fc.tx.commitRetainging(); cerr != nil {
			return cerr
		}
	}
	return nil
}

func (p *wireProtocol) opExecuteImmediate(transHandle int32, sql string) error {
	p.debugPrint("opExecuteImmediate")
	p.packInt(op_execute_immediate)
	p.packInt(transHandle)
	p.packInt(0) // statement
	p.packInt(3) // dialect
	p.packString(sql)
	p.packBytes(nil) // items
	p.packInt(0)     // buffer length
	_, err := p.sendPackets()
	return err
}
