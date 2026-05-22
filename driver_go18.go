/*******************************************************************************
The MIT License (MIT)

Copyright (c) 2016-2019 Hajime Nakagami

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
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"os"
	"sort"
	"time"
)

func flattenNamedValues(named []driver.NamedValue) []driver.Value {
	values := make([]driver.Value, len(named))
	for i, v := range named {
		values[i] = v.Value
	}
	return values
}

func (stmt *firebirdsqlStmt) ExecContext(ctx context.Context, namedargs []driver.NamedValue) (result driver.Result, err error) {
	sort.SliceStable(namedargs, func(i, j int) bool {
		return namedargs[i].Ordinal < namedargs[j].Ordinal
	})
	return stmt.exec(ctx, flattenNamedValues(namedargs))
}

func (stmt *firebirdsqlStmt) QueryContext(ctx context.Context, namedargs []driver.NamedValue) (rows driver.Rows, err error) {
	return stmt.query(ctx, flattenNamedValues(namedargs))
}

func (fc *firebirdsqlConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if opts.ReadOnly {
		// Preserve existing behaviour: readonly always uses READ COMMITTED RO.
		// The only extra knob we currently support here is NOWAIT.
		if (sql.IsolationLevel)(opts.Isolation) == LevelReadCommittedNoWait {
			return fc.begin(ISOLATION_LEVEL_READ_COMMITED_RO_NOWAIT)
		}
		return fc.begin(ISOLATION_LEVEL_READ_COMMITED_RO)
	}

	switch (sql.IsolationLevel)(opts.Isolation) {
	case sql.LevelDefault:
		return fc.begin(ISOLATION_LEVEL_READ_COMMITED)
	case sql.LevelReadCommitted:
		return fc.begin(ISOLATION_LEVEL_READ_COMMITED)
	case LevelReadCommittedNoWait:
		return fc.begin(ISOLATION_LEVEL_READ_COMMITED_NOWAIT)
	case sql.LevelRepeatableRead:
		return fc.begin(ISOLATION_LEVEL_REPEATABLE_READ)
	case sql.LevelSerializable:
		return fc.begin(ISOLATION_LEVEL_SERIALIZABLE)
	default:
	}
	return nil, errors.New("This isolation level is not supported.")
}

func (fc *firebirdsqlConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	return fc.prepare(ctx, query)
}

func (fc *firebirdsqlConn) ExecContext(ctx context.Context, query string, namedargs []driver.NamedValue) (result driver.Result, err error) {
	return fc.exec(ctx, query, flattenNamedValues(namedargs))
}

// isc_info_ods_version chosen over isc_info_ping for FB 2.5 compatibility (Jaybird does the same).
var pingInfoItems = []byte{isc_info_ods_version, isc_info_end}

// Ping uses op_info_database (1 round-trip) instead of a SQL query — no transaction is opened.
// Cancellation needs SetDeadline + watcher goroutine: wire path has no statement to cancel.
func (fc *firebirdsqlConn) Ping(ctx context.Context) (err error) {
	if err := ctx.Err(); err != nil {
		return err
	}

	if ctx.Done() != nil {
		completed := make(chan struct{})
		defer close(completed)

		if d, ok := ctx.Deadline(); ok {
			defer fc.wp.conn.SetDeadline(time.Time{})
			_ = fc.wp.conn.SetDeadline(d)
		}

		go func() {
			select {
			case <-ctx.Done():
				_ = fc.wp.conn.SetDeadline(time.Now())
			case <-completed:
			}
		}()
	}

	if err = fc.wp.opInfoDatabase(pingInfoItems); err != nil {
		return fmt.Errorf("ping info_database failed: %w: %w", err, driver.ErrBadConn)
	}

	if _, _, _, err = fc.wp.opResponse(); err != nil {
		if errors.Is(err, os.ErrDeadlineExceeded) {
			// ctx is the source of truth. The OS conn deadline can fire a hair
			// before the ctx timer at the same deadline instant; wait so the
			// ctx.Err() check below sees the populated cause.
			<-ctx.Done()
		}

		if cerr := ctx.Err(); cerr != nil {
			return fmt.Errorf("ping cancelled: %w: %w", cerr, driver.ErrBadConn)
		}
		return fmt.Errorf("ping response failed: %w: %w", err, driver.ErrBadConn)
	}
	return nil
}

func (fc *firebirdsqlConn) QueryContext(ctx context.Context, query string, namedargs []driver.NamedValue) (rows driver.Rows, err error) {
	return fc.query(ctx, query, flattenNamedValues(namedargs))
}

// ================== Implementation of the Connector interface ====================

type firebirdConnector struct {
	dsn *firebirdDsn
}

func (d *firebirdConnector) OpenConnector(dsns string) (driver.Connector, error) {
	dsn, err := parseDSN(dsns)
	if err != nil {
		return nil, err
	}
	return &firebirdConnector{dsn: dsn}, nil
}

func (fc *firebirdConnector) Driver() driver.Driver {
	return &firebirdsqlDriver{}
}

func (fc *firebirdConnector) Connect(ctx context.Context) (driver.Conn, error) {
	return attachFirebirdsqlConn(fc.dsn)
}
