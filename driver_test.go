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
	"time"
)

func TestBasic(t *testing.T) {
	conn, err := sql.Open("firebirdsql_createdb", "sysdba:masterkey@localhost:3050/tmp/go_test_basic.fdb")
	defer conn.Close()

	if err != nil {
		t.Fatalf("Error connecting: %v", err)
	}
	var sql string
	var n int

	sql = "SELECT Count(*) FROM rdb$relations where rdb$relation_name='FOO'"
	err = conn.QueryRow(sql).Scan(&n)
	if err != nil {
		t.Fatalf("Error QueryRow: %v", err)
	}
	if n > 0 {
		conn.Exec("DROP TABLE foo")
	}

	sql = `
        CREATE TABLE foo (
            a INTEGER NOT NULL,
            b VARCHAR(30) NOT NULL UNIQUE,
            c VARCHAR(1024),
            d DECIMAL(16,3) DEFAULT -0.123,
            e DATE DEFAULT '1967-08-11',
            f TIMESTAMP DEFAULT '1967-08-11 23:45:01',
            g TIME DEFAULT '23:45:01',
            h BLOB SUB_TYPE 1, 
            i DOUBLE PRECISION DEFAULT 0.0,
            j FLOAT DEFAULT 0.0,
            PRIMARY KEY (a),
            CONSTRAINT CHECK_A CHECK (a <> 0)
        )
    `
	conn.Exec(sql)
	_, err = conn.Exec("CREATE TABLE foo (a INTEGER)")
	if err == nil {
		t.Fatalf("Need metadata update error")
	}
	if err.Error() != "unsuccessful metadata update\nTable FOO already exists\n" {
		t.Fatalf("Bad message:%v", err.Error())
	}

	// 3 records insert
	conn.Exec("insert into foo(a, b, c, h) values (1, 'a', 'b','This is a memo')")
	conn.Exec("insert into foo(a, b, c, e, g, i, j) values (2, 'A', 'B', '1999-01-25', '00:00:01', 0.1, 0.1)")
	conn.Exec("insert into foo(a, b, c, e, g, i, j) values (3, 'X', 'Y', '2001-07-05', '00:01:02', 0.2, 0.2)")

	err = conn.QueryRow("select count(*) cnt from foo").Scan(&n)
	if err != nil {
		t.Fatalf("Error QueryRow: %v", err)
	}
	if n != 3 {
		t.Fatalf("Error bad record count: %v", n)
	}

	rows, err := conn.Query("select a, b, c, d, e, f, g, h, i, j from foo")
	var a int
	var b, c string
	var d float64
	var e time.Time
	var f time.Time
	var g time.Time
	var h []byte
	var i float64
	var j float32

	for rows.Next() {
		rows.Scan(&a, &b, &c, &d, &e, &f, &g, &h, &i, &j)
	}

	stmt, _ := conn.Prepare("select count(*) from foo where a=? and b=? and d=? and e=? and f=? and g=?")
	ep := time.Date(1967, 8, 11, 0, 0, 0, 0, time.UTC)
	fp := time.Date(1967, 8, 11, 23, 45, 1, 0, time.UTC)
	gp, err := time.Parse("15:04:05", "23:45:01")
	err = stmt.QueryRow(1, "a", -0.123, ep, fp, gp).Scan(&n)
	if err != nil {
		t.Fatalf("Error QueryRow: %v", err)
	}
	if n != 1 {
		t.Fatalf("Error bad record count: %v", n)
	}
}

func TestReturning(t *testing.T) {
	conn, _ := sql.Open("firebirdsql_createdb", "SYSDBA:masterkey@localhost:3050/tmp/go_test_returning.fdb")
	defer conn.Close()

	conn.Exec(`
        CREATE TABLE test_returning (
            f1 integer NOT NULL,
            f2 integer default 2,
            f3 varchar(20) default 'abc')`)
	conn.Close()

	conn, _ = sql.Open("firebirdsql", "SYSDBA:masterkey@localhost:3050/tmp/go_test_returning.fdb")

	for i := 0; i < 2; i++ {

		rows, err := conn.Query("INSERT INTO test_returning (f1) values (1) returning f2, f3")
		if err != nil {
			t.Fatalf("Error Insert returning : %v", err)
		}
		var f2 int
		var f3 string
		rows.Next()
		rows.Scan(&f2, &f3)
		if f2 != 2 || f3 != "abc" {
			t.Fatalf("Bad value insert returning: %v,%v", f2, f3)
		}
	}

}

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

func TestInsertBlobsWithParams(t *testing.T) {
	conn, _ := sql.Open("firebirdsql_createdb", "sysdba:masterkey@localhost:3050/tmp/go_test_insert_blobs_with_params.fdb")
	conn.Exec("CREATE TABLE test_blobs (f1 BLOB SUB_TYPE 0, f2 BLOB SUB_TYPE 1)")
	defer conn.Close()

	s0 := "Test Text"
	b0 := []byte{0, 1, 2, 3, 4}
	if _, err := conn.Exec("INSERT INTO test_blobs (f1, f2) values (?, ?)", s0, b0); err != nil {
		t.Fatalf("Error inserting blobs with params: %v", err)
	}

	var s string
	var b []byte
	err := conn.QueryRow("SELECT f1, f2 from test_blobs").Scan(&s, &b)
	if err != nil {
		t.Fatalf("Error in query: %v", err)
	}
	if s != s0 {
		t.Fatalf("Text blob: expected <%s>, got <%s>", s0, s)
	}
	if !reflect.DeepEqual(b, b0) {
		t.Fatalf("Binary blob: expected <%v>, got <%v> (%s)", b0, b, string(b))
	}
}

func TestError(t *testing.T) {
	conn, err := sql.Open("firebirdsql_createdb", "sysdba:masterkey@localhost:3050/tmp/go_test_error.fdb")
	if err != nil {
		t.Fatalf("Error connecting: %v", err)
	}
	_, err = conn.Exec("incorrect sql statement")
	if err == nil || err.Error() != "Dynamic SQL Error\nSQL error code = -104\nToken unknown - line 1, column 1\nincorrect\n" {
		t.Fatalf("Incorrect error")
	}
}

func TestRole(t *testing.T) {
	conn1, err := sql.Open("firebirdsql_createdb", "sysdba:masterkey@localhost:3050/tmp/go_test_role.fdb")
	if err != nil {
		t.Fatalf("Error creating: %v", err)
	}
	conn1.Exec("CREATE TABLE test_role (f1 integer)")
	conn1.Exec("INSERT INTO test_role (f1) values (1)")
	if err != nil {
		t.Fatalf("Error connecting: %v", err)
	}
	conn1.Exec("CREATE ROLE DRIVERROLE")
	if err != nil {
		t.Fatalf("Error creating role: %v", err)
	}
	conn1.Exec("GRANT DRIVERROLE TO DRIVERTEST")
	if err != nil {
		t.Fatalf("Error creating role: %v", err)
	}
	conn1.Exec("GRANT SELECT ON test_role TO DRIVERROLE")
	if err != nil {
		t.Fatalf("Error granting right to role: %v", err)
	}
	conn1.Close()

	conn2, err := sql.Open("firebirdsql", "drivertest:driverpw:driverrole@localhost:3050/tmp/go_test_role.fdb")
	if err != nil {
		t.Fatalf("Error connecting: %v", err)
	}

	rows, err := conn2.Query("SELECT f1 FROM test_role")
	defer conn2.Close()
	if err != nil {
		t.Fatalf("Error Query: %v", err)
	}

	for rows.Next() {
	}
}

func TestInsertTimestamp(t *testing.T) {
	const (
		sqlSchema = "CREATE TABLE TEST (VAL1 TIMESTAMP, VAL2 TIMESTAMP, VAL3 TIMESTAMP, VAL4 TIMESTAMP);"
		sqlInsert = "INSERT INTO TEST (VAL1, VAL2, VAL3, VAL4) VALUES (?, ?, ?, '2015/2/9 19:25:50.7405');"
		sqlSelect = "SELECT * FROM TEST;"
	)

	conn, err := sql.Open("firebirdsql_createdb", "sysdba:masterkey@localhost:3050/tmp/go_test_timestamp.fdb")
	if err != nil {
		t.Fatalf("Error creating: %v", err)
	}
	defer conn.Close()

	_, err = conn.Exec(sqlSchema)
	if err != nil {
		t.Fatalf("Error creating schema: %v", err)
	}

	dt1 := time.Date(2015, 2, 9, 19, 25, 50, 740500000, time.UTC)
	dt2 := "2015/2/9 19:25:50.7405"
	dt3 := "2015-2-9 19:25:50.7405"

	if _, err = conn.Exec(sqlInsert, dt1, dt2, dt3); err != nil {
		t.Fatalf("Error executing insert: %s", err)
	}

	var rt1, rt2, rt3, rt4 time.Time

	err = conn.QueryRow(sqlSelect).Scan(&rt1, &rt2, &rt3, &rt4)
	if err != nil {
		t.Fatalf("Unexpected error in select: %s", err)
	}

	if rt1 != dt1 {
		t.Errorf("Expected <%v>, got <%v>", dt1, rt1)
	}
	if rt2 != dt1 {
		t.Errorf("Expected <%v>, got <%v>", dt1, rt2)
	}
	if rt3 != dt1 {
		t.Errorf("Expected <%v>, got <%v>", dt1, rt3)
	}
	if rt4 != dt1 {
		t.Errorf("Expected <%v>, got <%v>", dt1, rt4)
	}
}

/*
func TestFB3(t *testing.T) {
	conn, err := sql.Open("firebirdsql_createdb", "sysdba:masterkey@localhost:3050/tmp/go_test_fb3.fdb")
	if err != nil {
		t.Fatalf("Error connecting: %v", err)
	}
	var sql string
	var n int

	sql = "SELECT Count(*) FROM rdb$relations where rdb$relation_name='TEST_FB3'"
	err = conn.QueryRow(sql).Scan(&n)
	if err != nil {
		t.Fatalf("Error QueryRow: %v", err)
	}
	if n > 0 {
		conn.Exec("DROP TABLE test_fb3")
	}

	sql = `
        CREATE TABLE test_fb3 (
            b BOOLEAN
        )
    `
	conn.Exec(sql)
	conn.Exec("insert into test_fb3(b) values (true)")
	conn.Exec("insert into test_fb3(b) values (false)")
    var b bool
	err = conn.QueryRow("select * from test_fb3 where b is true").Scan(&b)
	if err != nil {
		t.Fatalf("Error QueryRow: %v", err)
	}
	if b != true{
		conn.Exec("Invalid boolean value")
	}
	err = conn.QueryRow("select * from test_fb3 where b is false").Scan(&b)
	if err != nil {
		t.Fatalf("Error QueryRow: %v", err)
	}
	if b != false{
		conn.Exec("Invalid boolean value")
	}

	stmt, _ := conn.Prepare("select * from test_fb3 where b=?")
	err = stmt.QueryRow(true).Scan(&b)
	if err != nil {
		t.Fatalf("Error QueryRow: %v", err)
	}
	if b != false{
		conn.Exec("Invalid boolean value")
	}

	defer conn.Close()
}
*/
