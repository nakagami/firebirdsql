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
	"bytes"
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"
)

func TestTPBForIsolationLevelReadCommittedNoWait(t *testing.T) {
	tpb, err := tpbForIsolationLevel(ISOLATION_LEVEL_READ_COMMITED_NOWAIT)
	if err != nil {
		t.Fatalf("tpbForIsolationLevel(): %v", err)
	}
	want := []byte{
		byte(isc_tpb_version3),
		byte(isc_tpb_write),
		byte(isc_tpb_nowait),
		byte(isc_tpb_read_committed),
		byte(isc_tpb_rec_version),
	}
	if !bytes.Equal(tpb, want) {
		t.Fatalf("tpb mismatch\n got: %v\nwant: %v", tpb, want)
	}
}

func TestTPBForIsolationLevelReadCommittedRONoWait(t *testing.T) {
	tpb, err := tpbForIsolationLevel(ISOLATION_LEVEL_READ_COMMITED_RO_NOWAIT)
	if err != nil {
		t.Fatalf("tpbForIsolationLevel(): %v", err)
	}
	want := []byte{
		byte(isc_tpb_version3),
		byte(isc_tpb_read),
		byte(isc_tpb_nowait),
		byte(isc_tpb_read_committed),
		byte(isc_tpb_rec_version),
	}
	if !bytes.Equal(tpb, want) {
		t.Fatalf("tpb mismatch\n got: %v\nwant: %v", tpb, want)
	}
}

func TestTransaction(t *testing.T) {
	var n int
	test_dsn := GetTestDSN("test_transaction_")
	conn, err := sql.Open("firebirdsql_createdb", test_dsn)
	if err != nil {
		t.Fatalf("Error sql.Open(): %v", err)
	}
	conn.Exec("CREATE TABLE test_trans (s varchar(2048))")
	conn.Close()

	time.Sleep(1 * time.Second)

	conn, err = sql.Open("firebirdsql", test_dsn)
	if err != nil {
		t.Fatalf("Error sql.Open(): %v", err)
	}
	err = conn.QueryRow("SELECT Count(*) FROM test_trans").Scan(&n)
	if err != nil {
		t.Fatalf("Error SELECT: %v", err)
	}
	if n != 0 {
		t.Fatalf("Incorrect count: %v", n)
	}
	conn.Exec("INSERT INTO test_trans (s) values ('A')")
	conn.Close()

	time.Sleep(1 * time.Second)

	conn, err = sql.Open("firebirdsql", test_dsn)
	if err != nil {
		t.Fatalf("sql.Open(): %v", err)
	}
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
	if err != nil {
		t.Fatalf("Error Insert: %v", err)
	}
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
	if err != nil {
		t.Fatalf("Error Begin: %v", err)
	}

	err = tx.QueryRow("SELECT Count(*) FROM test_trans").Scan(&n)
	if err != nil {
		t.Fatalf("Error SELECT: %v", err)
	}
	if n != 2 {
		t.Fatalf("Incorrect count: %v", n)
	}

	// Connection (autocommit)
	_, err = conn.Exec("INSERT INTO test_trans (s) values ('E')")
	if err != nil {
		t.Fatalf("Error Insert: %v", err)
	}
	conn.Close()

	time.Sleep(1 * time.Second)

	conn, err = sql.Open("firebirdsql", test_dsn)
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
	test_dsn := GetTestDSN("test_issue35_")
	conn, err := sql.Open("firebirdsql_createdb", test_dsn)

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

	time.Sleep(1 * time.Second)

	conn, err = sql.Open("firebirdsql", test_dsn)
	var n int
	err = conn.QueryRow("SELECT Count(*) FROM test_issue35").Scan(&n)
	if err != nil {
		t.Fatalf("Error SELECT: %v", err)
	}
	if n != 0 {
		t.Fatalf("Incorrect count: %v", n)
	}

	conn.Close()
}

func TestIssue38(t *testing.T) {
	test_dsn := GetTestDSN("test_issue38_")
	conn, err := sql.Open("firebirdsql_createdb", test_dsn)

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

	time.Sleep(1 * time.Second)

	conn, err = sql.Open("firebirdsql", test_dsn)
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

	conn.Close()
}

func TestIssue39(t *testing.T) {
	conn, err := sql.Open("firebirdsql_createdb", GetTestDSN("test_issue39_"))
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

	conn.Close()
}

func TestIssue67(t *testing.T) {
	test_dsn := GetTestDSN("test_issue67_")
	conn, _ := sql.Open("firebirdsql_createdb", test_dsn)
	var n int
	conn.QueryRow("SELECT Count(*) FROM rdb$relations").Scan(&n)
	err := conn.Close()
	if err != nil {
		t.Fatalf("Error Close: %v", err)
	}

	conn, _ = sql.Open("firebirdsql", test_dsn)
	tx, _ := conn.Begin()
	tx.QueryRow("SELECT Count(*) FROM rdb$relations").Scan(&n)

	tx.Commit()

	err = conn.Close()
	if err != nil {
		t.Fatalf("Error Close: %v", err)
	}

}

func TestPingCommitsTransaction(t *testing.T) {
	test_dsn := GetTestDSN("test_ping_commits_transaction_")
	conn1, err := sql.Open("firebirdsql_createdb", test_dsn)
	if err != nil {
		t.Fatalf("Error connecting: %v", err)
	}
	defer conn1.Close()

	ctx := context.Background()
	pingConn, err := conn1.Conn(ctx)
	if err != nil {
		t.Fatalf("Error getting dedicated ping connection: %v", err)
	}
	defer pingConn.Close()

	if err = pingConn.PingContext(ctx); err != nil {
		t.Fatalf("Error ping: %v", err)
	}

	conn2, err := sql.Open("firebirdsql", test_dsn)
	if err != nil {
		t.Fatalf("Error connecting monitor connection: %v", err)
	}
	defer conn2.Close()

	var numberTrans int
	err = conn2.QueryRowContext(ctx, "select count(*) from mon$transactions where mon$attachment_id <> current_connection").Scan(&numberTrans)
	if err != nil {
		t.Fatalf("Error querying monitor transactions: %v", err)
	}
	if numberTrans != 0 {
		t.Fatalf("Ping left %d transaction(s)", numberTrans)
	}

	var n int
	err = pingConn.QueryRowContext(ctx, "select 1 from rdb$database").Scan(&n)
	if err != nil {
		t.Fatalf("Error querying after ping: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected 1, got %d", n)
	}
}

func TestPingContextCanceled(t *testing.T) {
	test_dsn := GetTestDSN("test_ping_context_canceled_")
	conn, err := sql.Open("firebirdsql_createdb", test_dsn)
	if err != nil {
		t.Fatalf("Error connecting: %v", err)
	}
	defer conn.Close()

	pingConn, err := conn.Conn(context.Background())
	if err != nil {
		t.Fatalf("Error getting dedicated ping connection: %v", err)
	}
	defer pingConn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = pingConn.PingContext(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestIssue89(t *testing.T) {

	var noconn1, numberTrans, numberrelations int

	//	test transaction open on connection open
	test_dsn := GetTestDSN("test_issue89_")
	conn1, _ := sql.Open("firebirdsql_createdb", test_dsn)

	conn2, _ := sql.Open("firebirdsql", test_dsn)

	conn2.QueryRow("select count(*) from mon$transactions where mon$attachment_id <> current_connection").Scan(&numberTrans)
	if numberTrans > 0 {
		t.Fatalf("Transaction open without query runned")
	}

	conn2.Close()

	//	test if are more than 1 transaction open on first query
	conn1.QueryRow("select mon$attachment_id from mon$attachments where mon$attachment_id = current_connection").Scan(&noconn1)

	conn2, _ = sql.Open("firebirdsql", test_dsn)
	conn2.QueryRow("select count(*) from mon$transactions where mon$attachment_id <> current_connection").Scan(&numberTrans)

	if numberTrans > 2 {
		t.Fatalf("More than 1 transaction open")
	}

	conn1.Close()
	conn2.Close()

	//	test autocommit when rows is closed
	conn1, _ = sql.Open("firebirdsql", test_dsn)

	rows, _ := conn1.Query("select first 3 rdb$relation_id from rdb$relations")

	rows.Next()
	rows.Next()

	rows.Close()

	conn2, _ = sql.Open("firebirdsql", test_dsn)
	conn2.QueryRow("select count(*) from mon$transactions where mon$attachment_id <> current_connection").Scan(&numberTrans)

	if numberTrans != 1 {
		t.Fatalf("Autocommit don't work")
	}

	conn1.Close()
	conn2.Close()

	//	test autocommit on prepare statement
	conn1, _ = sql.Open("firebirdsql", test_dsn)
	stmt, _ := conn1.Prepare("select count(*) from rdb$relations")
	err := stmt.QueryRow().Scan(&numberrelations)

	if err != nil {
		t.Fatalf("Error QueryRow of Prepare: %v", err)
	}

	stmt.Close()

	stmt, _ = conn1.Prepare("select count(*) from rdb$relations")

	rows, _ = stmt.Query("select first 3 rdb$relation_id from rdb$relations")

	rows.Next()
	rows.Next()

	rows.Close()

	conn2, _ = sql.Open("firebirdsql", test_dsn)
	conn2.QueryRow("select count(*) from mon$transactions where mon$attachment_id <> current_connection").Scan(&numberTrans)

	if numberTrans != 1 {
		t.Fatalf("Autocommit in prepare don't work")
	}

	//	test autocommit on prepare statement
	conn1, _ = sql.Open("firebirdsql", test_dsn)
	conn1.Exec("create table testprepareinsert (id integer)")

	stmt, _ = conn1.Prepare("insert into testprepareinsert (id) values (?)")

	stmt.Exec(1)
	/*	_, err = stmt.Exec(2)
		if err == nil {
			t.Fatalf("Autocommit in prepare don't work")
		}
	*/
	stmt.Close()

	conn2, _ = sql.Open("firebirdsql", test_dsn)
	conn2.QueryRow("select count(*) from mon$transactions where mon$attachment_id <> current_connection").Scan(&numberTrans)

	conn1, _ = sql.Open("firebirdsql", test_dsn)
	txp, _ := conn1.Begin()
	stmt, _ = txp.Prepare("insert into testprepareinsert (id) values (?)")

	for i := 1; i <= 6; i++ {
		_, err = stmt.Exec(i)
		if err != nil {
			t.Fatalf("Multiple execute of a prepared statement in same transaction don't work: %v", err)
		}
	}

	txp.Commit()
	conn2, _ = sql.Open("firebirdsql", test_dsn)
	conn2.QueryRow("select count(*) from mon$transactions where mon$attachment_id <> current_connection").Scan(&numberTrans)

	// test transaction open after a commit of another transaction
	conn1, _ = sql.Open("firebirdsql", test_dsn)
	conn2, _ = sql.Open("firebirdsql", test_dsn)

	tx, err := conn1.Begin()

	if err != nil {
		t.Fatalf("Error opening new transaction: %v", err)
	}

	tx.QueryRow("select mon$attachment_id from mon$attachments where mon$attachment_id = current_connection").Scan(&noconn1)

	tx.Commit()

	err = conn1.QueryRow("select mon$attachment_id from mon$attachments where mon$attachment_id = current_connection").Scan(&noconn1)
	if err != nil {
		t.Fatalf("Error opening new transaction after last one committed or rollback: %v", err)
	}

	conn2, _ = sql.Open("firebirdsql", test_dsn)
	conn2.QueryRow("select count(*) from mon$transactions where mon$attachment_id <> current_connection").Scan(&numberTrans)

	conn1.Close()
	conn2.Close()

}

// TestIssue136 verifies that INSERT/UPDATE/DELETE issued via a user-prepared
// sql.Stmt in autocommit mode become visible to other attachments immediately,
// not only when the sql.Stmt is closed. See github.com/nakagami/firebirdsql#136.
func TestIssue136(t *testing.T) {
	test_dsn := GetTestDSN("test_issue136_")

	setupConn, err := sql.Open("firebirdsql_createdb", test_dsn)
	if err != nil {
		t.Fatalf("createdb open: %v", err)
	}
	if _, err = setupConn.Exec("CREATE TABLE numbers (i INTEGER NOT NULL PRIMARY KEY)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	setupConn.Close()

	writeDB, err := sql.Open("firebirdsql", test_dsn)
	if err != nil {
		t.Fatalf("writeDB open: %v", err)
	}
	defer writeDB.Close()

	readDB, err := sql.Open("firebirdsql", test_dsn)
	if err != nil {
		t.Fatalf("readDB open: %v", err)
	}
	defer readDB.Close()

	const (
		rowCount      = 15
		writeSpacing  = 400 * time.Millisecond
		pollInterval  = 100 * time.Millisecond
		visibilitySLA = 2 * time.Second
	)

	// Warm both pools so the first row isn't charged for cold-start handshake latency.
	if err = writeDB.Ping(); err != nil {
		t.Fatalf("writeDB ping: %v", err)
	}
	if err = readDB.Ping(); err != nil {
		t.Fatalf("readDB ping: %v", err)
	}

	writeDone := make(chan struct{})
	writeTimes := make([]time.Time, rowCount)

	go func() {
		defer close(writeDone)
		stmt, perr := writeDB.Prepare("INSERT INTO numbers (i) VALUES (?)")
		if perr != nil {
			t.Errorf("prepare failed: %v", perr)
			return
		}
		defer stmt.Close()
		for i := 0; i < rowCount; i++ {
			if _, eerr := stmt.Exec(i); eerr != nil {
				t.Errorf("exec %d failed: %v", i, eerr)
				return
			}
			writeTimes[i] = time.Now()
			time.Sleep(writeSpacing)
		}
	}()

	observedAt := make([]time.Time, rowCount)
	deadline := time.Now().Add(time.Duration(rowCount)*writeSpacing + visibilitySLA)
	seen := 0
	for seen < rowCount && time.Now().Before(deadline) {
		var max int
		err := readDB.QueryRow("SELECT COALESCE(MAX(i), -1) FROM numbers").Scan(&max)
		if err != nil {
			t.Fatalf("poll query failed: %v", err)
		}
		now := time.Now()
		for seen <= max {
			observedAt[seen] = now
			seen++
		}
		time.Sleep(pollInterval)
	}

	<-writeDone

	if seen < rowCount {
		t.Fatalf("reader saw only %d/%d rows before deadline — writer commits not propagating", seen, rowCount)
	}
	for i := 0; i < rowCount; i++ {
		lag := observedAt[i].Sub(writeTimes[i])
		if lag > visibilitySLA {
			t.Fatalf("row %d visible after %v (> %v) — writer autocommit broken", i, lag, visibilitySLA)
		}
	}
}
