======================================
firebirdsql (Go firebird sql driver)
======================================

Go SQL driver for Firebird RDBMS http://firebirdsql.org .

Requirements
-------------

* Firebird 2.x or later (not 1.x)

Installation
-------------

::

   $ go get github.com/go-sql-driver/mysql


Usage
-------------

::

   import (
       "database/sql"
       "github.com/nakagami/firebirdsql"
   )

   conn, err := sql.Open("firebirdsql", "sysdba:masterkey@localhost:3050/tmp/go_test.fdb")

And see driver_test.go
