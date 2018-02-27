======================================
firebirdsql (Go firebird sql driver)
======================================

Firebird RDBMS http://firebirdsql.org SQL driver for Go

.. image:: https://travis-ci.org/nakagami/firebirdsql.svg?branch=master
    :target: https://travis-ci.org/nakagami/firebirdsql

Requirements
-------------

* Firebird 2.1 or higher
* Golang 1.7 or higher

Installation
-------------

::

   $ go get github.com/cznic/mathutil
   $ go get github.com/kardianos/osext
   $ go get github.com/nyarla/go-crypt
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

Connection string
--------------------------

::

   user:password@servername[:port_number]/database_name_or_file[?params1=value1[&param2=value2]...]


General
=========

- user: login user
- password: login password
- servername: Firebird server's host name or IP address.
- port_number: Port number. default value is 3050.
- database_name_or_file: Database path (or alias name).

Optional
=========

param1, param2... are

- role: Role name.
- auth_plugin_name: Authentication plugin name for FB3. Srp or Legacy_Auth are available. Default is Srp.
- wire_crypt: Enable wire data encryption or not. It is for FB3 server. Default is true.
