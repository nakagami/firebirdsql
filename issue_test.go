/*******************************************************************************
The MIT License (MIT)

Copyright (c) 2013-2014 Hajime Nakagami

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
	"fmt"
	"reflect"
	"testing"
)

func TestIssue2(t *testing.T) {
	conn, _ := sql.Open("firebirdsql_createdb", "sysdba:masterkey@localhost:3050/tmp/go_test_issue2.fdb")

	_, err := conn.Exec(`
        CREATE TABLE test_issue2
         (f1 integer NOT NULL,
          f2 integer,
          f3 integer NOT NULL,
          f4 integer NOT NULL,
          f5 integer NOT NULL,
          f6 integer NOT NULL,
          f7 varchar(255) NOT NULL,
          f8 varchar(255) NOT NULL,
          f9 varchar(255) NOT NULL,
          f10 varchar(255) NOT NULL,
          f11 varchar(255) NOT NULL,
          f12 varchar(255) NOT NULL,
          f13 varchar(255) NOT NULL,
          f14 varchar(255) NOT NULL,
          f15 integer,
          f16 integer,
          f17 integer,
          f18 integer,
          f19 integer,
          f20 integer,
          f21 integer,
          f22 varchar(1),
          f23 varchar(255),
          f24 integer,
          f25 varchar(64),
          f26 integer)`)
	defer conn.Close()
	if err != nil {
		t.Fatalf("Error Create Table: %v", err)
	}

	_, err = conn.Exec(`
        INSERT INTO test_issue2 VALUES
        (1, 2, 3, 4, 5, 6, '7', '8', '9', '10', '11', '12', '13', '14',
          15, 16, 17, 18, 19, 20, 21, 'A', '23', 24, '25', '26')`)
	if err != nil {
		t.Fatalf("Error Insert: %v", err)
	}

	rows, err := conn.Query("SELECT * FROM test_issue2")
	if err != nil {
		t.Fatalf("Error Query: %v", err)
	}
	for rows.Next() {
	}
}

func TestIssue3(t *testing.T) {
	conn, _ := sql.Open("firebirdsql_createdb", "sysdba:masterkey@localhost:3050/tmp/go_test_issue3.fdb")
	too_many := 401

	conn.Exec("CREATE TABLE test_issue3 (f1 integer NOT NULL)")
	defer conn.Close()
	stmt, _ := conn.Prepare("INSERT INTO test_issue3 values (?)")
	for i := 0; i < too_many; i++ {
		stmt.Exec(i + 1)
	}

	rows, _ := conn.Query("SELECT * FROM test_issue3 ORDER BY f1")
	i := 0
	var n int
	for rows.Next() {
		rows.Scan(&n)
		i++
		if i != n {
			t.Fatalf("Error %v != %v", n, i)
		}
	}
	if i != too_many {
		t.Fatalf("Can't get all %v records. only %v", too_many, i)
	}
}

func TestIssue7(t *testing.T) {
	conn, _ := sql.Open("firebirdsql_createdb", "sysdba:masterkey@localhost:3050/tmp/go_test_issue7.fdb")

	conn.Exec("CREATE TABLE test_issue7 (f1 varchar(2048))")
	defer conn.Close()
	stmt, _ := conn.Prepare("INSERT INTO test_issue7 values (?)")
	stmt.Exec(fmt.Sprintf("%2000d", 1))
}

func TestIssue9(t *testing.T) {
	conn, _ := sql.Open("firebirdsql_createdb", "sysdba:masterkey@localhost:3050/tmp/go_test_issue9.fdb")

	conn.Exec("CREATE TABLE test_issue9 (f1 smallint)")
	defer conn.Close()
	conn.Exec("INSERT INTO test_issue9 (f1) values (1)")
	var n int
	err := conn.QueryRow("SELECT f1 from test_issue9").Scan(&n)
	if err != nil || n != 1 {
		fmt.Println(err)
		t.Fatalf("Invalid short value:%v:%v", err, n)
	}
}

func TestIssue10(t *testing.T) {
	conn, _ := sql.Open("firebirdsql_createdb", "sysdba:masterkey@localhost:3050/tmp/go_test_issue10.fdb")

	conn.Exec("CREATE TABLE test_issue10 (f1 BLOB SUB_TYPE 0, f2 BLOB SUB_TYPE 1)")
	defer conn.Close()
	conn.Exec("INSERT INTO test_issue10 (f1, f2) values ('ABC', 'ABC')")

	var s string
	var b []byte
	err := conn.QueryRow("SELECT f1, f2 from test_issue10").Scan(&s, &b)
	if err != nil {
		t.Fatalf("Error in query: %v", err)
	}
	if s != "ABC" {
		t.Fatalf("Text blob: expected <%s>, got <%s>", "ABC", s)
	}
	b0 := []byte("ABC")
	if !reflect.DeepEqual(b, b0) {
		t.Fatalf("Binary blob: expected <%v>, got <%v>", b0, b)
	}
}

func TestIssue23(t *testing.T) {
	conn, _ := sql.Open("firebirdsql_createdb", "sysdba:masterkey@localhost:3050/tmp/go_test_issue23.fdb")
	conn.Exec("CREATE TABLE test_issue23 (f1 varchar(2048))")
	defer conn.Close()

	tx, err := conn.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}

	sqlStr := "INSERT INTO test_issue23 (f1) VALUES (?)"
	if _, err := tx.Exec(sqlStr, "test"); err != nil {
		tx.Rollback()
		if err != nil {
			t.Fatalf("Rollback: %v", err)
		}
	}

	if err := tx.Commit(); err != nil {
		if err != nil {
			t.Fatalf("Commit: %v", err)
		}
	}

	var name string
	sqlStr = "SELECT f1 FROM test_issue23"
	if err = conn.QueryRow(sqlStr).Scan(&name); err != nil {
		if err != nil {
			t.Fatalf("QueryRow: %v", err)
		}
	}
}
