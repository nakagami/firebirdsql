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
    "database/sql"
)

func TestConnect(t *testing.T) {
    conn, err := sql.Open("firebirdsql", "sysdba:masterkey@localhost:3060/tmp/go_test.fdb")
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

    sql = "CREATE TABLE foo (\n"
    sql += "     a INTEGER NOT NULL,\n"
    sql += "     b VARCHAR(30) NOT NULL UNIQUE,\n"
    sql += "     c VARCHAR(1024),\n"
    sql += "     d DECIMAL(16,3) DEFAULT -0.123,\n"
    sql += "     e DATE DEFAULT '1967-08-11',\n"
    sql += "     f TIMESTAMP DEFAULT '1967-08-11 23:45:01',\n"
    sql += "     g TIME DEFAULT '23:45:01',\n"
    sql += "     h BLOB SUB_TYPE 1,\n"
    sql += "     i DOUBLE PRECISION DEFAULT 0.0,\n"
    sql += "     j FLOAT DEFAULT 0.0,\n"
    sql += "     PRIMARY KEY (a),\n"
    sql += "     CONSTRAINT CHECK_A CHECK (a <> 0)\n"
    sql += ")"
    fmt.Println(sql)
    conn.Exec(sql)


    rows, err := conn.Query("select count(*) cnt from foo")
    if err != nil {
        t.Fatalf("Error Query: %v", err)
    }
    columns, _ := rows.Columns()
    if len(columns) != 1 {
        t.Fatalf("Columns count error")
    }

    for rows.Next() {
        rows.Scan(&n)
        fmt.Println(n)
    }

    defer conn.Close()

}
