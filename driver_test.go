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
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TempFileName(prefix string) string {
	randBytes := make([]byte, 16)
	rand.Read(randBytes)
	return filepath.Join(os.TempDir(), prefix+hex.EncodeToString(randBytes)+".fdb")
}

func TestBasic(t *testing.T) {
	temppath := TempFileName("test_basic_")
	conn, err := sql.Open("firebirdsql_createdb", "sysdba:masterkey@localhost:3050"+temppath)

	if err != nil {
		t.Fatalf("Error connecting: %v", err)
	}
	var n int

	query := "SELECT Count(*) FROM rdb$relations where rdb$relation_name='FOO'"
	err = conn.QueryRow(query).Scan(&n)
	if err != nil {
		t.Fatalf("Error QueryRow: %v", err)
	}
	if n > 0 {
		conn.Exec("DROP TABLE foo")
	}

	query = `
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
	conn.Exec(query)
	conn.Close()

	time.Sleep(1 * time.Second)

	conn, err = sql.Open("firebirdsql", "sysdba:masterkey@localhost:3050"+temppath)
	_, err = conn.Exec("CREATE TABLE foo (a INTEGER)")
	if err == nil {
		t.Fatalf("Need metadata update error")
	}
	if !strings.Contains(err.Error(), "unsuccessful metadata update\n") {
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
	columns, err := rows.Columns()
	if !reflect.DeepEqual(columns, []string{"A", "B", "C", "D", "E", "F", "G", "H", "I", "J"}) {
		t.Fatalf("Columns() mismatch: %v", columns)
	}
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
		//		fmt.Println(a, b, c, d, e, f, g, h, i, j)
	}

	stmt, _ := conn.Prepare("select count(*) from foo where a=? and b=? and d=? and e=? and f=? and g=?")
	ep := time.Date(1967, 8, 11, 0, 0, 0, 0, time.Local)
	fp := time.Date(1967, 8, 11, 23, 45, 1, 0, time.Local)
	gp, err := time.Parse("15:04:05", "23:45:01")
	err = stmt.QueryRow(1, "a", -0.123, ep, fp, gp).Scan(&n)
	if err != nil {
		t.Fatalf("Error QueryRow: %v", err)
	}
	if n != 1 {
		t.Fatalf("Error bad record count: %v", n)
	}

	conn.Close()
}

func TestReturning(t *testing.T) {
	temppath := TempFileName("test_returning_")
	conn, err := sql.Open("firebirdsql_createdb", "SYSDBA:masterkey@localhost:3050"+temppath)
	if err != nil {
		t.Fatalf("Error sql.Open() : %v", err)
	}

	conn.Exec(`
        CREATE TABLE test_returning (
            f1 integer NOT NULL,
            f2 integer default 2,
            f3 varchar(20) default 'abc')`)

	conn.Close()

	time.Sleep(1 * time.Second)

	conn, err = sql.Open("firebirdsql", "SYSDBA:masterkey@localhost:3050"+temppath)
	if err != nil {
		t.Fatalf("Error sql.Open() : %v", err)
	}

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

	conn.Close()
}

func TestInsertBlobsWithParams(t *testing.T) {
	temppath := TempFileName("test_insert_blobs_with_params")
	conn, _ := sql.Open("firebirdsql_createdb", "sysdba:masterkey@localhost:3050"+temppath)
	conn.Exec("CREATE TABLE test_blobs (f1 BLOB SUB_TYPE 0, f2 BLOB SUB_TYPE 1)")
	conn.Close()

	time.Sleep(1 * time.Second)

	conn, _ = sql.Open("firebirdsql", "sysdba:masterkey@localhost:3050"+temppath)

	s0 := "Test Text"
	b0 := []byte{0, 1, 2, 3, 4, 13, 10, 5, 6, 7}
	if _, err := conn.Exec("INSERT INTO test_blobs (f1, f2) values (?, ?)", b0, s0); err != nil {
		t.Fatalf("Error inserting blobs with params: %v", err)
	}

	var s string
	var b []byte
	err := conn.QueryRow("SELECT f1, f2 from test_blobs").Scan(&b, &s)
	if err != nil {
		t.Fatalf("Error in query: %v", err)
	}
	if s != s0 {
		t.Fatalf("Text blob: expected <%s>, got <%s>", s0, s)
	}
	if !reflect.DeepEqual(b, b0) {
		t.Fatalf("Binary blob: expected <%v>, got <%v> (%s)", b0, b, string(b))
	}

	conn.Close()
}

func TestError(t *testing.T) {
	temppath := TempFileName("test_error_")
	conn, err := sql.Open("firebirdsql_createdb", "sysdba:masterkey@localhost:3050"+temppath)
	if err != nil {
		t.Fatalf("Error connecting: %v", err)
	}
	_, err = conn.Exec("incorrect sql statement")
	if err == nil {
		t.Fatalf("Incorrect error")
	} else if err.Error() != "Dynamic SQL Error\nSQL error code = -104\nToken unknown - line 1, column 1\nincorrect\n" {
		t.Fatalf("Incorrect error: %v", err.Error())
	}
	conn.Close()
}

func TestRole(t *testing.T) {
	temppath := TempFileName("test_role_")
	conn1, err := sql.Open("firebirdsql_createdb", "sysdba:masterkey@localhost:3050"+temppath)
	if err != nil {
		t.Fatalf("Error creating: %v", err)
	}
	conn1.Exec("CREATE TABLE test_role (f1 integer)")
	conn1.Exec("INSERT INTO test_role (f1) values (1)")
	_, err = conn1.Exec("CREATE ROLE DRIVERROLE")
	if err != nil {
		t.Fatalf("Error creating role: %v", err)
	}
	conn1.Exec("GRANT DRIVERROLE TO SYSDBA")
	if err != nil {
		t.Fatalf("Error creating role: %v", err)
	}
	conn1.Exec("GRANT SELECT ON test_role TO DRIVERROLE")
	if err != nil {
		t.Fatalf("Error granting right to role: %v", err)
	}
	conn1.Close()

	time.Sleep(1 * time.Second)

	conn2, err := sql.Open("firebirdsql", "sysdba:masterkey@localhost:3050/"+temppath+"?role=driverrole")
	if err != nil {
		t.Fatalf("Error connecting: %v", err)
	}

	rows, err := conn2.Query("SELECT f1 FROM test_role")
	if err != nil {
		t.Fatalf("Error Query: %v", err)
	}

	for rows.Next() {
	}
	conn2.Close()
}

func TestInsertTimestamp(t *testing.T) {
	temppath := TempFileName("test_timestamp_")

	conn, err := sql.Open("firebirdsql_createdb", "sysdba:masterkey@localhost:3050"+temppath)
	if err != nil {
		t.Fatalf("Error creating: %v", err)
	}

	_, err = conn.Exec("CREATE TABLE TEST (VAL1 TIMESTAMP, VAL2 TIMESTAMP, VAL3 TIMESTAMP, VAL4 TIMESTAMP)")
	if err != nil {
		t.Fatalf("Error creating table: %v", err)
	}

	dt1 := time.Date(2015, 2, 9, 19, 25, 50, 740500000, time.Local)
	dt2 := "2015/2/9 19:25:50.7405"
	dt3 := "2015-2-9 19:25:50.7405"

	if _, err = conn.Exec("INSERT INTO TEST (VAL1, VAL2, VAL3, VAL4) VALUES (?, ?, ?, '2015/2/9 19:25:50.7405')", dt1, dt2, dt3); err != nil {
		t.Fatalf("Error executing insert: %s", err)
	}

	var rt1, rt2, rt3, rt4 time.Time

	err = conn.QueryRow("SELECT * FROM TEST").Scan(&rt1, &rt2, &rt3, &rt4)
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
	conn.Close()
}

/*
func TestBoolean(t *testing.T) {
	temppath := TempFileName("test_boolean_")

	conn, err := sql.Open("firebirdsql_createdb", "sysdba:masterkey@localhost:3050"+temppath)
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

	conn.Close()
}

func TestDecFloat(t *testing.T) {
	temppath := TempFileName("test_decfloat_")

	conn, err := sql.Open("firebirdsql_createdb", "sysdba:masterkey@localhost:3050"+temppath)
	if err != nil {
		t.Fatalf("Error connecting: %v", err)
	}

	sql := `
        CREATE TABLE test_decfloat (
            i integer,
            d DECIMAL(20, 2),
            df64 DECFLOAT(16),
            df128 DECFLOAT(34)
        )
    `
	conn.Exec(sql)
	conn.Exec("insert into test_decfloat(i, d, df64, df128) values (1, 0.0, 0.0, 0.0)")
	conn.Exec("insert into test_decfloat(i, d, df64, df128) values (2, 1.0, 1.0, 1.0)")
	conn.Exec("insert into test_decfloat(i, d, df64, df128) values (3, 20.0, 20.0, 20.0)")
	conn.Exec("insert into test_decfloat(i, d, df64, df128) values (4, -1.0, -1.0, -1.0)")
	conn.Exec("insert into test_decfloat(i, d, df64, df128) values (5, -20.0, -20.0, -20.0)")

	var n int
	err = conn.QueryRow("select count(*) cnt from test_decfloat").Scan(&n)
	if err != nil {
		t.Fatalf("Error QueryRow: %v", err)
	}
	if n != 5 {
		t.Fatalf("Error bad record count: %v", n)
	}

	rows, err := conn.Query("select d, df64, df128 from test_decfloat order by i")

	var d, df64, df128 decimal.Decimal
	for rows.Next() {
		rows.Scan(&d, &df64, &df128)
		fmt.Println(d, df64, df128)
	}

	conn.Close()
}
*/

func TestLegacyAuthWireCrypt(t *testing.T) {
	temppath := TempFileName("test_legacy_atuh_")
	var n int
	conn, err := sql.Open("firebirdsql_createdb", "sysdba:masterkey@localhost:3050"+temppath)
	if err != nil {
		t.Fatalf("Error connecting: %v", err)
	}
	err = conn.Ping()
	if err != nil {
		t.Fatalf("Error ping: %v", err)
	}
	conn.Close()

	time.Sleep(1 * time.Second)

	conn, err = sql.Open("firebirdsql", "sysdba:masterkey@localhost:3050"+temppath+"?auth_plugin_anme=Legacy_Auth")
	if err != nil {
		t.Fatalf("Error connecting: %v", err)
	}
	err = conn.QueryRow("SELECT Count(*) FROM rdb$relations").Scan(&n)
	if err != nil {
		t.Fatalf("Error SELECT: %v", err)
	}
	conn.Close()

	conn, err = sql.Open("firebirdsql", "sysdba:masterkey@localhost:3050"+temppath+"?wire_crypt=false")
	if err != nil {
		t.Fatalf("Error connecting: %v", err)
	}
	err = conn.QueryRow("SELECT Count(*) FROM rdb$relations").Scan(&n)
	if err != nil {
		t.Fatalf("Error SELECT: %v", err)
	}
	conn.Close()

	conn, err = sql.Open("firebirdsql", "sysdba:masterkey@localhost:3050"+temppath+"?auth_plugin_name=Legacy_Auth&wire_auth=true")
	if err != nil {
		t.Fatalf("Error connecting: %v", err)
	}
	err = conn.QueryRow("SELECT Count(*) FROM rdb$relations").Scan(&n)
	if err != nil {
		t.Fatalf("Error SELECT: %v", err)
	}
	conn.Close()

	conn, err = sql.Open("firebirdsql", "sysdba:masterkey@localhost:3050"+temppath+"?auth_plugin_name=Legacy_Auth&wire_auth=false")
	if err != nil {
		t.Fatalf("Error connecting: %v", err)
	}
	err = conn.QueryRow("SELECT Count(*) FROM rdb$relations").Scan(&n)
	if err != nil {
		t.Fatalf("Error SELECT: %v", err)
	}
	conn.Close()
}

func TestErrorConnect(t *testing.T) {
	var n int
	conn, err := sql.Open("firebirdsql", "foo:bar@something_wrong_hostname:3050/dbname")
	if err != nil {
		t.Fatalf("Error occured at sql.Open()")
	}
	err = conn.QueryRow("SELECT Count(*) FROM rdb$relations").Scan(&n)
	if err == nil {
		t.Fatalf("Error not occured")
	}

	conn.Close()
}

func TestGoIssue44(t *testing.T) {
	conn, err := sql.Open("firebirdsql", "SomethingWrongConnectionString")
	err = conn.Ping()
	if err == nil {
		t.Fatalf("Error not occured")
	}
	conn.Close()
}

func TestGoIssue45(t *testing.T) {
	temppath := TempFileName("test_issue45_")
	conn, err := sql.Open("firebirdsql_createdb", "sysdba:masterkey@localhost:3050"+temppath)
	if err != nil {
		t.Fatalf("Error occured at sql.Open()")
	}

	conn.Exec(`
        CREATE TABLE person (
            name VARCHAR(60) NOT NULL,
            created TIMESTAMP
        )
    `)
	conn.Exec(`
        insert into person (name, created)
        values ('Giovanni', null)
    `)

	conn.Close()

	time.Sleep(1 * time.Second)

	conn, err = sql.Open("firebirdsql", "sysdba:masterkey@localhost:3050"+temppath)

	// select null value
	type response struct {
		name    string
		created *time.Time
	}
	r := response{}

	err = conn.QueryRow(`
        select name, created from person
    `).Scan(&r.name, &r.created)
	if err != nil {
		t.Fatalf("Error: %v", err)
	}
	if r.created != nil {
		t.Fatalf("created is not nil")
	}

	// insert returning not null value
	err = conn.QueryRow(`
        insert into person (name, created)
        values ('Giovanni Gaspar', current_timestamp)
        returning name, created
    `).Scan(&r.name, &r.created)
	if err != nil {
		t.Fatalf("Error: %v", err)
	}
	if r.created == nil {
		t.Fatalf("created is nil")
	}

	// insert returning null value
	err = conn.QueryRow(`
        insert into person (name, created)
        values ('Nakagami', null)
        returning name, created
     `).Scan(
		&r.name, &r.created)
	if err != nil {
		t.Fatalf("Error: %v", err)
	}
	if r.created != nil {
		t.Fatalf("created is not nil")
	}

	conn.Close()
}

func TestGoIssue49(t *testing.T) {
	temppath := TempFileName("test_issue49_")
	conn, err := sql.Open("firebirdsql_createdb", "sysdba:masterkey@localhost:3050"+temppath)
	if err != nil {
		t.Fatalf("Error occured at sql.Open()")
	}
	defer conn.Close()

	sqlCreate := `
    CREATE TABLE NullTest (       
        name VARCHAR(60) NOT NULL,
        nullname VARCHAR(10),
        nullDate DATE,
        bug1 SMALLINT,
        bug2 INTEGER
    )
`
	conn.Exec(sqlCreate)

	//Worked
	sqlTest1 := `insert into NullTest (name, nullDate)values ('value', null)`
	_, err = conn.Exec(sqlTest1)
	if err != nil {
		t.Error(err)
	}
	//Worked
	sqlTest1 = `insert into NullTest (name, nullDate)values (?, ?)`
	_, err = conn.Exec(sqlTest1, "value", nil)
	if err != nil {
		t.Error(err)
	}

	//Failed
	sqlTest1 = `insert into NullTest (name, nullDate)values (?, ?)`
	_, err = conn.Exec(sqlTest1, "value", nil)
	if err != nil {
		t.Error(err)
	}

	// Failed
	sqlTest1 = `insert into NullTest (name, bug1) values ('value', ?)`
	_, err = conn.Exec(sqlTest1, nil)
	if err != nil {
		t.Error(err)
	}
	// Failed
	sqlTest1 = `insert into NullTest (name, bug1,bug2) values ('value', ?,?)`
	_, err = conn.Exec(sqlTest1, nil, nil)
	if err != nil {
		t.Error(err)
	}

	// must be failed!
	sqlTest1 = `insert into NullTest (name, bug1) values ('value', ?)`
	_, err = conn.Exec(sqlTest1)
	if err == nil {
		t.Error("Expected error!")
	}
}

func TestGoIssue53(t *testing.T) {
	timeout := time.Second * 40
	temppath := TempFileName("test_issue53_")
	conn, err := sql.Open("firebirdsql_createdb", "sysdba:masterkey@localhost:3050"+temppath)
	if err != nil {
		t.Fatalf("Error occured at sql.Open()")
	}
	defer conn.Close()

	tests := []int{
		31,
		BLOB_SEGMENT_SIZE,
		BLOB_SEGMENT_SIZE + 1,
		22*BLOB_SEGMENT_SIZE + 21,
		97*BLOB_SEGMENT_SIZE + 21,
	}

	conn.Exec(`CREATE TABLE BlobTest (bugField blob sub_type binary )`)

	for _, test := range tests {
		t.Run(fmt.Sprintf("%d", test), func(t *testing.T) {
			sqlDelete := `delete from BlobTest`
			_, err = conn.Exec(sqlDelete)
			if err != nil {
				t.Error(err)
			}

			sqlTest1 := `insert into BlobTest(bugField) values(?)`

			str := strings.Repeat("F", test)

			done := make(chan bool)
			go func(ch chan bool) {
				_, err = conn.Exec(sqlTest1, str)
				if err != nil {
					t.Error(err)
				}
				close(done)
			}(done)

			select {
			case <-done:
			case <-time.After(timeout):
				t.Fatal("Test timed out after ", timeout)
			}

			sqlget := `select bugField from BlobTest`
			rows, err := conn.Query(sqlget)
			if err != nil {
				t.Error(err)
			}

			for rows.Next() {
				var buf []byte
				err = rows.Scan(&buf)
				if err != nil {
					t.Error(err)
				}
				if len(buf) != test {
					t.Errorf("Expected size blob %d, got %d", test, len(buf))
				}
			}

			rows.Close()

		})
	}
}
func TestGoIssue65(t *testing.T) {
	/*var field01 string
	var field02 int
	var field03 int
	var field04 string
	var field05 int
	var field06 int
	var field07 string
	var field08 string
	var field09 string
	var field10 string
	var field11 string
	var field12 float32
	var field13 float32
	var field14 float32
	var field15 float32
	var field16 float32
	var field17 float32
	var field18 float32
	var field19 float32
	var field20 string
	var field21 string
	var field22 string
	var field23 string
	var field24 float32
	var field25 float32
	*/
	temppath := TempFileName("test_issue65_")
	conn, err := sql.Open("firebirdsql_createdb", "sysdba:masterkey@localhost:3050"+temppath)
	if err != nil {
		t.Fatalf("Error occured at sql.Open()")
	}
	defer conn.Close()

	conn.Exec(`CREATE TABLE FPI_MOVTO_MOVIMIENTOS
	(
	  RFCEMPRESA varchar(20) NOT NULL,
	  NOSUCURSAL integer NOT NULL,
	  TIPO integer NOT NULL,
	  SERIE varchar(5) NOT NULL,
	  NODOCTO integer NOT NULL,
	  LINEA integer NOT NULL,
	  CODART varchar(20),
	  NOMART varchar(80),
	  CLAVEPRODSERV varchar(10),
	  UNIDADCLAVE varchar(10),
	  UNIDADNOMBRE varchar(80),
	  CANT1 double precision,
	  CATN2 double precision,
	  PUNIT double precision,
	  MONTO double precision,
	  IMPTO1 double precision,
	  IMPTO2 double precision,
	  PIMPTO1 double precision,
	  PIMPTO2 double precision,
	  TIMPTO1 varchar(10),
	  TIMPTO2 varchar(10),
	  TFIMPTO1 varchar(10),
	  TFIMPTO2 varchar(10),
	  PDESCTO double precision,
	  IDESCTO double precision,
	  CONSTRAINT PXFPI_MOVTO_MOVIMIENTOS PRIMARY KEY (RFCEMPRESA,NOSUCURSAL,TIPO,SERIE,NODOCTO,LINEA)
	);`)

	//Worked
	sqlTest1 := `INSERT INTO FPI_MOVTO_MOVIMIENTOS (RFCEMPRESA, NOSUCURSAL, TIPO, SERIE, NODOCTO, LINEA, CODART, NOMART, CLAVEPRODSERV, UNIDADCLAVE, UNIDADNOMBRE, CANT1, CATN2, PUNIT, MONTO, IMPTO1, IMPTO2, PIMPTO1, PIMPTO2, TIMPTO1, TIMPTO2, TFIMPTO1, TFIMPTO2, PDESCTO, IDESCTO) VALUES ('p2', '0', '700', 'X', '1', '1', 'ART-001', 'PRUEBA DE ARTICULO', '01010101', 'ACT', 'Actividad', '10.000000', '0.000000', '2.500000', '25.000000', '4.000000', '0.000000', '16.000000', '0.000000', '002', '', 'Tasa', '', '0.000000', '0.000000');`
	_, err = conn.Exec(sqlTest1)
	if err != nil {
		t.Error(err)
	}

	sqlTest2 := "select doc.RFCEMPRESA, doc.NOSUCURSAL, doc.TIPO, doc.SERIE, doc.NODOCTO, doc.LINEA,\n" +
		"	doc.CODART, doc.NOMART, doc.CLAVEPRODSERV, doc.UNIDADCLAVE, doc.UNIDADNOMBRE, doc.CANT1,\n" +
		"	doc.CATN2, doc.PUNIT, doc.MONTO, doc.IMPTO1, doc.IMPTO2, doc.PIMPTO1, doc.PIMPTO2,\n" +
		"	doc.TIMPTO1, doc.TIMPTO2, doc.TFIMPTO1, doc.TFIMPTO2, doc.PDESCTO, doc.IDESCTO\n" +
		"from FPI_MOVTO_MOVIMIENTOS doc\n" +
		"where doc.RFCEMPRESA = 'p2' and doc.NOSUCURSAL = 0 and doc.TIPO = 700 and doc.SERIE = 'X' and doc.NODOCTO = 1 \n"
	movtos, err := conn.Query(sqlTest2)
	if err != nil {
		t.Error(err)
	}

	existData := movtos.Next()
	if existData == false {
		t.Fatalf("Expecting Data")
	} /*else {
		movtos.Scan(&field01, &field02, &field03, &field04, &field05, &field06, &field07, &field08, &field09, &field10,
			&field11, &field12, &field13, &field14, &field15, &field16, &field17, &field18, &field19, &field20, &field21,
			&field22, &field23, &field24, &field25)
		t.Fatalf(field01)
	}*/
}
