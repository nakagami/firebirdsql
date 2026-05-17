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
	"sort"
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

// This file implements the optional pinger interface for the database/sql package
func (fc *firebirdsqlConn) Ping(ctx context.Context) (err error) {
	if fc == nil {
		return fmt.Errorf("Connection was closed: %w", driver.ErrBadConn)
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	// Keep Ping as a SQL check, but do not leave the connection's autocommit
	// transaction context open through COMMIT RETAINING.
	previousTx := fc.tx
	pingTx, err := newFirebirdsqlTx(fc, ISOLATION_LEVEL_READ_COMMITED_RO, false, true)
	if err != nil {
		return fmt.Errorf("ping begin transaction failed: %w: %w", err, driver.ErrBadConn)
	}
	fc.tx = pingTx
	committed := false
	defer func() {
		fc.tx = previousTx
		if !committed {
			_ = pingTx.Rollback()
		}
		delete(fc.transactionSet, pingTx)
	}()

	rows, err := fc.query(ctx, "SELECT 1 from rdb$database", nil)
	if err != nil {
		return fmt.Errorf("ping query failed: %w: %w", err, driver.ErrBadConn)
	}
	if err = rows.Close(); err != nil {
		return fmt.Errorf("ping rows close failed: %w: %w", err, driver.ErrBadConn)
	}
	if err = pingTx.Commit(); err != nil {
		return fmt.Errorf("ping commit failed: %w: %w", err, driver.ErrBadConn)
	}
	committed = true

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
