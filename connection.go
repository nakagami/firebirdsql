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
	addr              string
	dbName            string
	user              string
	password          string
	columnNameToLower bool
	isAutocommit      bool
	clientPublic      *big.Int
	clientSecret      *big.Int
	transactionSet    map[*firebirdsqlTx]struct{}
}

func (fc *firebirdsqlConn) begin(isolationLevel int) (driver.Tx, error) {
	tx, err := newFirebirdsqlTx(fc, isolationLevel, false)
	fc.tx = tx
	return driver.Tx(tx), err
}

func (fc *firebirdsqlConn) Begin() (driver.Tx, error) {
	return fc.begin(ISOLATION_LEVEL_READ_COMMITED)
}

func (fc *firebirdsqlConn) Close() (err error) {
	for tx, _ := range fc.transactionSet {
		tx.Rollback()
	}

	fc.wp.opDetach()
	_, _, _, err = fc.wp.opResponse()
	fc.wp.conn.Close()
	return
}

func (fc *firebirdsqlConn) prepare(ctx context.Context, query string) (driver.Stmt, error) {
	return newFirebirdsqlStmt(fc, query)
}

func (fc *firebirdsqlConn) Prepare(query string) (driver.Stmt, error) {
	return fc.prepare(context.Background(), query)
}

func (fc *firebirdsqlConn) exec(ctx context.Context, query string, args []driver.Value) (result driver.Result, err error) {

	if fc.tx.needBegin {
		err = fc.tx.begin()
		if err != nil {
			return
		}
	}

	stmt, err := fc.prepare(ctx, query)
	if err != nil {
		return
	}

	result, err = stmt.(*firebirdsqlStmt).exec(ctx, args)
	if err != nil {
		return
	}
	if fc.isAutocommit && fc.tx.isAutocommit {
		fc.tx.commitRetainging()
	}
	stmt.Close()
	return
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
	return
}

func (fc *firebirdsqlConn) Query(query string, args []driver.Value) (rows driver.Rows, err error) {
	return fc.query(context.Background(), query, args)
}

func newFirebirdsqlConn(dsn string) (fc *firebirdsqlConn, err error) {
	addr, dbName, user, password, options, err := parseDSN(dsn)

	wp, err := newWireProtocol(addr, options["timezone"])
	if err != nil {
		return
	}

	column_name_to_lower := convertToBool(options["column_name_to_lower"], false)

	clientPublic, clientSecret := getClientSeed()

	wp.opConnect(dbName, user, password, options, clientPublic)
	err = wp._parse_connect_response(user, password, options, clientPublic, clientSecret)
	if err != nil {
		return
	}
	wp.opAttach(dbName, user, password, options["role"])
	wp.dbHandle, _, _, err = wp.opResponse()
	if err != nil {
		return
	}

	fc = new(firebirdsqlConn)
	fc.transactionSet = make(map[*firebirdsqlTx]struct{})
	fc.wp = wp
	fc.addr = addr
	fc.dbName = dbName
	fc.user = user
	fc.password = password
	fc.columnNameToLower = column_name_to_lower
	fc.isAutocommit = true
	fc.tx, err = newFirebirdsqlTx(fc, ISOLATION_LEVEL_READ_COMMITED, fc.isAutocommit)
	fc.clientPublic = clientPublic
	fc.clientSecret = clientSecret

	fc.loadTimeZoneId()

	return fc, err
}

func createFirebirdsqlConn(dsn string) (fc *firebirdsqlConn, err error) {
	// Create Database
	addr, dbName, user, password, options, err := parseDSN(dsn)

	wp, err := newWireProtocol(addr, options["timezone"])
	if err != nil {
		return
	}
	column_name_to_lower := convertToBool(options["column_name_to_lower"], false)

	clientPublic, clientSecret := getClientSeed()

	wp.opConnect(dbName, user, password, options, clientPublic)
	err = wp._parse_connect_response(user, password, options, clientPublic, clientSecret)
	if err != nil {
		return
	}
	wp.opCreate(dbName, user, password, options["role"])
	wp.dbHandle, _, _, err = wp.opResponse()
	if err != nil {
		return
	}

	fc = new(firebirdsqlConn)
	fc.transactionSet = make(map[*firebirdsqlTx]struct{})
	fc.wp = wp
	fc.addr = addr
	fc.dbName = dbName
	fc.user = user
	fc.password = password
	fc.columnNameToLower = column_name_to_lower
	fc.isAutocommit = true
	fc.tx, err = newFirebirdsqlTx(fc, ISOLATION_LEVEL_READ_COMMITED, fc.isAutocommit)
	fc.clientPublic = clientPublic
	fc.clientSecret = clientSecret

	fc.loadTimeZoneId()

	return fc, err
}
