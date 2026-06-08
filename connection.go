/*******************************************************************************
The MIT License (MIT)

Copyright (c) 2013-2019 Hajime Nakagami

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
	"math/big"
)

type firebirdsqlConn struct {
	wp                *wireProtocol
	tx                *firebirdsqlTx
	dsn               *firebirdDsn
	columnNameToLower bool
	isAutocommit      bool
	clientPublic      *big.Int
	clientSecret      *big.Int
	transactionSet    map[*firebirdsqlTx]struct{}
}

// WireCipher returns the name of the wire-encryption cipher negotiated for this
// connection ("ChaCha64", "ChaCha", or "Arc4"), or an empty string when the
// connection is unencrypted (plaintext). Because the concrete connection type
// is unexported, applications reach it through the database/sql layer by
// asserting sql.Conn.Raw to an interface:
//
//	conn.Raw(func(dc any) error {
//	    cipher := dc.(interface{ WireCipher() string }).WireCipher()
//	    ...
//	})
func (fc *firebirdsqlConn) WireCipher() string {
	return fc.wp.conn.plugin
}

// ============ driver.Conn implementation

func (fc *firebirdsqlConn) begin(isolationLevel int) (driver.Tx, error) {
	tx, err := newFirebirdsqlTx(fc, isolationLevel, false, true)
	fc.tx = tx
	return driver.Tx(tx), err
}

// Begin starts and returns a new transaction.
//
// Deprecated: Drivers should implement ConnBeginTx instead (or additionally).
// -> is implemented in driver_go18.go with BeginTx()
func (fc *firebirdsqlConn) Begin() (driver.Tx, error) {
	return fc.begin(ISOLATION_LEVEL_READ_COMMITED)
}

// Close invalidates and potentially stops any current
// prepared statements and transactions, marking this
// connection as no longer in use.
//
// Because the sql package maintains a free pool of
// connections and only calls Close when there's a surplus of
// idle connections, it shouldn't be necessary for drivers to
// do their own connection caching.
func (fc *firebirdsqlConn) Close() (err error) {
	for tx := range fc.transactionSet {
		tx.Rollback()
	}

	err = fc.wp.opDetach()
	if err != nil {
		return
	}
	// Teardown read: bounded so pool eviction (database/sql closing a conn flagged
	// ErrBadConn) cannot hang on a silent wire.
	_, _, _, err = fc.wp.opResponseTimeout(abandonReadTimeout)
	fc.wp.conn.Close()
	return
}

func (fc *firebirdsqlConn) prepare(ctx context.Context, query string) (driver.Stmt, error) {
	if fc.tx == nil {
		return nil, driver.ErrBadConn
	}
	if fc.tx.needBegin {
		err := fc.tx.begin()
		if err != nil {
			return nil, err
		}
	}

	return newFirebirdsqlStmt(fc, query)
}

// Prepare returns a prepared statement, bound to this connection.
func (fc *firebirdsqlConn) Prepare(query string) (driver.Stmt, error) {
	return fc.prepare(context.Background(), query)
}

// ============ driver.Tx implementation

func (fc *firebirdsqlConn) exec(ctx context.Context, query string, args []driver.Value) (result driver.Result, err error) {
	stmt, err := fc.prepare(ctx, query)
	if err != nil {
		return
	}
	defer stmt.Close()
	return stmt.(*firebirdsqlStmt).exec(ctx, args)
}

func (fc *firebirdsqlConn) Exec(query string, args []driver.Value) (result driver.Result, err error) {
	return fc.exec(context.Background(), query, args)
}

func (fc *firebirdsqlConn) query(ctx context.Context, query string, args []driver.Value) (rows driver.Rows, err error) {

	stmt, err := fc.prepare(ctx, query)
	if err != nil {
		return
	}
	rows, err = stmt.(*firebirdsqlStmt).query(ctx, args)
	if err == nil {
		rows.(*firebirdsqlRows).closeStmtOnClose = true
	}
	return
}

func (fc *firebirdsqlConn) Query(query string, args []driver.Value) (rows driver.Rows, err error) {
	return fc.query(context.Background(), query, args)
}

func openFirebirdsqlConn(dsn *firebirdDsn, dbOp func(*wireProtocol) error) (*firebirdsqlConn, error) {
	wp, err := newWireProtocol(dsn.addr, dsn.options["timezone"], dsn.options["charset"])
	if err != nil {
		return nil, err
	}
	columnNameToLower := convertToBool(dsn.options["column_name_to_lower"], false)
	clientPublic, clientSecret, err := getClientSeed()
	if err != nil {
		return nil, err
	}

	if err = wp.opConnect(dsn.dbName, dsn.user, dsn.passwd, dsn.options, clientPublic); err != nil {
		return nil, err
	}
	if err = wp._parse_connect_response(dsn.user, dsn.passwd, dsn.options, clientPublic, clientSecret); err != nil {
		return nil, err
	}
	if err = dbOp(wp); err != nil {
		return nil, err
	}
	wp.dbHandle, _, _, err = wp.opResponse()
	if err != nil {
		return nil, err
	}

	fc := &firebirdsqlConn{
		transactionSet:    make(map[*firebirdsqlTx]struct{}),
		wp:                wp,
		dsn:               dsn,
		columnNameToLower: columnNameToLower,
		isAutocommit:      true,
		clientPublic:      clientPublic,
		clientSecret:      clientSecret,
	}
	fc.tx, err = newFirebirdsqlTx(fc, ISOLATION_LEVEL_READ_COMMITED, fc.isAutocommit, false)
	if err != nil {
		return nil, err
	}
	return fc, nil
}

func attachFirebirdsqlConn(dsn *firebirdDsn) (*firebirdsqlConn, error) {
	return openFirebirdsqlConn(dsn, func(wp *wireProtocol) error {
		return wp.opAttach(dsn.dbName, dsn.user, dsn.passwd, dsn.options["role"])
	})
}

func createFirebirdsqlConn(dsn *firebirdDsn) (*firebirdsqlConn, error) {
	return openFirebirdsqlConn(dsn, func(wp *wireProtocol) error {
		return wp.opCreate(dsn.dbName, dsn.user, dsn.passwd, dsn.options["role"])
	})
}
