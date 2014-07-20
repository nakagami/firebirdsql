======================================
firebirdsql (Go firebird sql driver)
======================================

Firebird RDBMS http://firebirdsql.org SQL driver for Go

Requirements
-------------

* Firebird 2.1 or later

Installation
-------------

::

   $ go get github.com/cznic/mathutil
   $ go get github.com/nakagami/firebirdsql


Example
-------------

::

   package main

   import (
       "fmt"
       "database/sql"
       _ "github.com/nakagami/firebirdsql"
   )

   func main() {
       var n int
       conn, _ := sql.Open("firebirdsql", "user:password@servername/foo/bar.fdb")
       defer conn.Close()
       conn.QueryRow("SELECT Count(*) FROM rdb$relations").Scan(&n)
       fmt.Println("Relations count=", n)

   }


See also driver_test.go
