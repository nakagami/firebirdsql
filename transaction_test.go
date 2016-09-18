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

package firebirdsql

import (
	"database/sql"
	"testing"
)

func TestTransaction(t *testing.T) {
	var n int
	conn, err := sql.Open("firebirdsql_createdb", "sysdba:masterkey@localhost:3050/tmp/go_test_transaction.fdb")
	if err != nil {
		t.Fatalf("Error connecting: %v", err)
	}

	// Connection (autocommit)
	conn.Exec("CREATE TABLE test_trans (s varchar(2048))")
	conn.Close()
	conn, err = sql.Open("firebirdsql", "sysdba:masterkey@localhost:3050/tmp/go_test_transaction.fdb")
	err = conn.QueryRow("SELECT Count(*) FROM test_trans").Scan(&n)
	if n != 0 {
		t.Fatalf("Incorrect count: %v", n)
	}
	conn.Exec("INSERT INTO test_trans (s) values ('A')")
	conn, err = sql.Open("firebirdsql", "sysdba:masterkey@localhost:3050/tmp/go_test_transaction.fdb")
	err = conn.QueryRow("SELECT Count(*) FROM test_trans").Scan(&n)
	if n != 1 {
		t.Fatalf("Incorrect count: %v", n)
	}

	// Transaction
	tx, err := conn.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}

	// Rollback
	err = tx.QueryRow("SELECT Count(*) FROM test_trans").Scan(&n)
	if n != 1 {
		t.Fatalf("Incorrect count: %v", n)
	}
	_, err = tx.Exec("INSERT INTO test_trans (s) values ('B')")
	err = tx.QueryRow("SELECT Count(*) FROM test_trans").Scan(&n)
	if n != 2 {
		t.Fatalf("Incorrect count: %v", n)
	}
	tx.Rollback()
	tx, err = conn.Begin()
	err = tx.QueryRow("SELECT Count(*) FROM test_trans").Scan(&n)
	if n != 1 {
		t.Fatalf("Incorrect count: %v", n)
	}

	// Commit & Rollback
	_, err = tx.Exec("INSERT INTO test_trans (s) values ('C')")
	tx.Commit()
	tx.Rollback()
	tx, err = conn.Begin()
	err = tx.QueryRow("SELECT Count(*) FROM test_trans").Scan(&n)
	if n != 2 {
		t.Fatalf("Incorrect count: %v", n)
	}

	// Connection (autocommit)
	conn.Exec("INSERT INTO test_trans (s) values ('D')")
	conn.Close()
	conn, err = sql.Open("firebirdsql", "sysdba:masterkey@localhost:3050/tmp/go_test_transaction.fdb")
	err = tx.QueryRow("SELECT Count(*) FROM test_trans").Scan(&n)

	if n != 3 {
		t.Fatalf("Incorrect count: %v", n)
	}

	conn.Close()
}
