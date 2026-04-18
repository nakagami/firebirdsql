/*
Package firebirdsql provides a database/sql driver for Firebird RDBMS
(https://firebirdsql.org). It is a pure Go implementation using the Firebird
wire protocol — no C dependencies or cgo required.

# Drivers

Two driver names are registered:

  - "firebirdsql" — attach to an existing database.
  - "firebirdsql_createdb" — create the database if it does not exist, then attach.

# Connection String (DSN)

	user:password@host[:port]/database[?param=value&...]

The driver automatically prepends the "firebird://" scheme and parses the DSN
as a URI per RFC 3986. The default port is 3050.

# Reserved Characters and Percent-Encoding

Because the DSN is parsed as a URI, any RFC 3986 reserved character that
appears literally in the password or database path will be misinterpreted.
Affected characters and their encoded forms:

	@   →  %40      (terminates the userinfo section)
	:   →  %3A      (separates user from password in userinfo)
	#   →  %23      (starts the fragment — silently drops everything after it)
	?   →  %3F      (starts the query string)
	/   →  %2F      (path separator)
	&   →  %26      (separates query parameters)
	=   →  %3D      (separates a query key from its value)
	%   →  %25      (the escape character itself)
	+   →  %2B
	space → %20

Use [net/url.QueryEscape] to encode a password, or [net/url.PathEscape] to
encode a database path segment, when constructing DSNs programmatically.

# Examples

Plain connection to localhost:

	db, err := sql.Open("firebirdsql", "sysdba:masterkey@localhost/var/lib/firebird/mydb.fdb")

Password containing reserved characters (e.g. "p@ss:w#rd"):

	import "net/url"

	pass := url.QueryEscape("p@ss:w#rd")   // → "p%40ss%3Aw%23rd"
	dsn  := "sysdba:" + pass + "@localhost/var/lib/firebird/mydb.fdb"
	db, err := sql.Open("firebirdsql", dsn)

Windows database path:

	db, err := sql.Open("firebirdsql", "sysdba:masterkey@localhost/C:/fbdata/mydb.fdb")

See the README for the full list of optional query parameters (auth_plugin_name,
charset, role, timezone, wire_crypt, wire_compress, column_name_to_lower).
*/
package firebirdsql
