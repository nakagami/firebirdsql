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
	if err != nil {
		t.Fatalf("Error SELECT: %v", err)
	}
	if n != 0 {
		t.Fatalf("Incorrect count: %v", n)
	}
	conn.Exec("INSERT INTO test_trans (s) values ('A')")
	conn, err = sql.Open("firebirdsql", "sysdba:masterkey@localhost:3050/tmp/go_test_transaction.fdb")
	err = conn.QueryRow("SELECT Count(*) FROM test_trans").Scan(&n)
	if err != nil {
		t.Fatalf("Error SELECT: %v", err)
	}
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
	if err != nil {
		t.Fatalf("Error SELECT: %v", err)
	}
	if n != 1 {
		t.Fatalf("Incorrect count: %v", n)
	}
	_, err = tx.Exec("INSERT INTO test_trans (s) values ('B')")
	err = tx.QueryRow("SELECT Count(*) FROM test_trans").Scan(&n)
	if err != nil {
		t.Fatalf("Error SELECT: %v", err)
	}
	if n != 2 {
		t.Fatalf("Incorrect count: %v", n)
	}
	err = tx.Rollback()
	if err != nil {
		t.Fatalf("Error Rollback: %v", err)
	}

	tx, err = conn.Begin()
	err = tx.QueryRow("SELECT Count(*) FROM test_trans").Scan(&n)
	if err != nil {
		t.Fatalf("Error SELECT: %v", err)
	}
	if n != 1 {
		t.Fatalf("Incorrect count: %v", n)
	}

	// Commit
	_, err = tx.Exec("INSERT INTO test_trans (s) values ('C')")
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Error Commit: %v", err)
	}
	tx, err = conn.Begin()
	err = tx.QueryRow("SELECT Count(*) FROM test_trans").Scan(&n)
	if err != nil {
		t.Fatalf("Error SELECT: %v", err)
	}
	if n != 2 {
		t.Fatalf("Incorrect count: %v", n)
	}

	// without Commit (Need commit manually)
	_, err = tx.Exec("INSERT INTO test_trans (s) values ('D')")
	tx, err = conn.Begin()
	err = tx.QueryRow("SELECT Count(*) FROM test_trans").Scan(&n)
	if err != nil {
		t.Fatalf("Error SELECT: %v", err)
	}
	if n != 2 {
		t.Fatalf("Incorrect count: %v", n)
	}

	// Connection (autocommit)
	conn.Exec("INSERT INTO test_trans (s) values ('E')")
	conn.Close()
	conn, err = sql.Open("firebirdsql", "sysdba:masterkey@localhost:3050/tmp/go_test_transaction.fdb")
	err = tx.QueryRow("SELECT Count(*) FROM test_trans").Scan(&n)
	if err != nil {
		t.Fatalf("Error SELECT: %v", err)
	}
	if n != 3 {
		t.Fatalf("Incorrect count: %v", n)
	}

	conn.Close()
}

func TestIssue35(t *testing.T) {
	var n int
	conn, err := sql.Open("firebirdsql_createdb", "sysdba:masterkey@localhost:3050/tmp/go_test_issue35.fdb")

	if err != nil {
		t.Fatalf("Error connecting: %v", err)
	}

	tx, err := conn.Begin()

	if err != nil {
		t.Fatalf("Error Begin: %v", err)
	}

	err = tx.Commit()

	if err != nil {
		t.Fatalf("Error Commit: %v", err)
	}

	_, err = conn.Exec("CREATE TABLE test_issue35 (s varchar(2048))")

	if err != nil {
		t.Fatalf("Error CREATE TABLE: %v", err)
	}
	conn.Close()

	conn, err = sql.Open("firebirdsql", "sysdba:masterkey@localhost:3050/tmp/go_test_issue35.fdb")
	err = conn.QueryRow("SELECT Count(*) FROM test_issue35").Scan(&n)
	if err != nil {
		t.Fatalf("Error SELECT: %v", err)
	}
	if n != 0 {
		t.Fatalf("Incorrect count: %v", n)
	}
}

func TestIssue38(t *testing.T) {
	conn, err := sql.Open("firebirdsql_createdb", "sysdba:masterkey@localhost:3050/tmp/go_test_issue38.fdb")

	if err != nil {
		t.Fatalf("Error connecting: %v", err)
	}
	conn.Exec(`
        CREATE TABLE test_issue38 (
          id  INTEGER NOT NULL,
          key VARCHAR(64),
          value VARCHAR(64)
        )
    `)
	if err != nil {
		t.Fatalf("Error CREATE TABLE: %v", err)
	}
	conn.Close()

	conn, err = sql.Open("firebirdsql", "sysdba:masterkey@localhost:3050/tmp/go_test_issue38.fdb")
	defer conn.Close()
	tx, err := conn.Begin()

	if err != nil {
		t.Fatalf("Error Begin: %v", err)
	}

	var rowId = sql.NullInt64{}

	err = tx.QueryRow(
		"INSERT INTO test_issue38 (id, key, value) VALUES (?, ?, ?) RETURNING id", 1, "testKey", "testValue").Scan(&rowId)
	if err == nil {
		t.Fatalf("'Dynamic SQL Error' is not occuerd.")
	}
	err = tx.Rollback()
	if err != nil {
		t.Fatalf("Error Rollback: %v", err)
	}
}

func TestIssue39(t *testing.T) {
	conn, err := sql.Open("firebirdsql", "sysdba:masterkey@localhost:3050/tmp/go_test_transaction.fdb")
	defer conn.Close()
	tx, err := conn.Begin()

	if err != nil {
		t.Fatalf("Error Begin: %v", err)
	}
	var rowId = sql.NullInt64{}
	err = tx.QueryRow("select 5 / 0 from rdb$database").Scan(&rowId)
	if err == nil {
		t.Fatalf("'Dynamic SQL Error' is not occured.")
	}
	err = tx.Rollback()
	if err != nil {
		t.Fatalf("broken transaction, but error is not occured.")
	}
}
