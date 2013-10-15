/*******************************************************************************
The MIT License (MIT)

Copyright (c) 2013 Hajime Nakagami

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
    "fmt"
    "testing"
    "time"
    "database/sql"
)

func TestConnect(t *testing.T) {
    conn, err := sql.Open("firebirdsql", "sysdba:masterkey@localhost:3050/tmp/go_test.fdb")
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
    // 3 records insert
    conn.Exec("insert into foo(a, b, c,h) values (1, 'a', 'b','This is a memo')")
    conn.Exec("insert into foo(a, b, c, e, g, i, j) values (2, 'A', 'B', '1999-01-25', '00:00:01', 0.1, 0.1)")
    conn.Exec("insert into foo(a, b, c, e, g, i, j) values (3, 'X', 'Y', '2001-07-05', '00:01:02', 0.2, 0.2)")

    err = conn.QueryRow("select count(*) cnt from foo").Scan(&n)
    if err != nil {
        t.Fatalf("Error QueryRow: %v", err)
    }
    if n != 3 {
        t.Fatalf("Error bad record count: %v", n)
    }

    rows, err := conn.Query("select a, b, c, d, e, f, g, i, j from foo")
    var a int
    var b, c string
    var d float64
    var e time.Time
    var f time.Time
    var g time.Time
    var i float64
    var j float32
    for rows.Next() {
        rows.Scan(&a, &b, &c, &d, &e, &f, &g, &i, &j)
        fmt.Println(a, b, c, d, e, f, g, i, j)
    }

    defer conn.Close()
}
