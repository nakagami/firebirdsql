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
	isolationLevel, err := txIsolationLevel(opts)
	if err != nil {
		return nil, err
	}
	var tx driver.Tx
	err = fc.wp.withContextDeadline(ctx, func() error {
		var err error
		tx, err = fc.begin(isolationLevel)
		return err
	})
	if err != nil {
		return nil, err
	}
	tx.(*firebirdsqlTx).ctx = ctx // bounds Commit (see firebirdsqlTx.Commit)
	return tx, nil
}

func txIsolationLevel(opts driver.TxOptions) (int, error) {
	if opts.ReadOnly {
		// Preserve existing behaviour: readonly always uses READ COMMITTED RO.
		// The only extra knob we currently support here is NOWAIT.
		if (sql.IsolationLevel)(opts.Isolation) == LevelReadCommittedNoWait {
			return ISOLATION_LEVEL_READ_COMMITED_RO_NOWAIT, nil
		}
		return ISOLATION_LEVEL_READ_COMMITED_RO, nil
	}

	switch (sql.IsolationLevel)(opts.Isolation) {
	case sql.LevelDefault:
		return ISOLATION_LEVEL_READ_COMMITED, nil
	case sql.LevelReadCommitted:
		return ISOLATION_LEVEL_READ_COMMITED, nil
	case LevelReadCommittedNoWait:
		return ISOLATION_LEVEL_READ_COMMITED_NOWAIT, nil
	case sql.LevelRepeatableRead:
		return ISOLATION_LEVEL_REPEATABLE_READ, nil
	case sql.LevelSerializable:
		return ISOLATION_LEVEL_SERIALIZABLE, nil
	default:
	}
	return 0, errors.New("This isolation level is not supported.")
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
func (fc *firebirdsqlConn) Ping(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	err := fc.wp.withContextDeadline(ctx, func() error {
		if err := fc.wp.opInfoDatabase(pingInfoItems); err != nil {
			return fmt.Errorf("ping info_database failed: %w", err)
		}
		_, _, _, err := fc.wp.opResponse()
		return err
	})
	if err == nil {
		return nil
	}
	// A failed ping always discards the connection; desynced makes Close drop
	// the socket instead of waiting out rollback and detach on a dead wire.
	fc.wp.desynced = true
	if errors.Is(err, driver.ErrBadConn) {
		return err
	}
	return fmt.Errorf("ping failed: %w: %w", err, driver.ErrBadConn)
}

func (fc *firebirdsqlConn) QueryContext(ctx context.Context, query string, namedargs []driver.NamedValue) (rows driver.Rows, err error) {
	return fc.query(ctx, query, flattenNamedValues(namedargs))
}

// ================== Implementation of the Connector interface ====================

type firebirdConnector struct {
	dsn    *firebirdDsn
	dsnErr error
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
	if fc.dsnErr != nil {
		return nil, fc.dsnErr
	}
	return attachFirebirdsqlConn(ctx, fc.dsn)
}
