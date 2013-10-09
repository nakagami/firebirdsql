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
//    conn.Exec("create table foo (a int, var char(256))")
    rows, err := conn.Query("select count(*) cnt from foo")
    if err != nil {
        t.Fatalf("Error Query: %v", err)
    }
    columns, _ := rows.Columns()
    fmt.Println("Columns:", columns)

    var n int
    for rows.Next() {
        rows.Scan(&n)
        fmt.Println(n)
    }

    defer conn.Close()

}
