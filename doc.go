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
auth_plugin_list, charset, role, timezone, wire_crypt, wire_crypt_plugin,
wire_compress, column_name_to_lower).

Authentication is controlled by two parameters:

	auth_plugin_name  preferred authentication plugin (default "Srp256"). Must be
	                  a member of auth_plugin_list.
	auth_plugin_list  ordered, comma-separated allow-list of acceptable auth
	                  plugins (default "Srp256,Srp,Legacy_Auth"; must be a subset
	                  of the supported plugins). The plugin the server selects must
	                  be a member, otherwise the connection is refused before any
	                  credentials are sent. Omit a plugin to refuse it — e.g.
	                  "Srp256,Srp" rejects a server downgrade to Legacy_Auth, which
	                  would otherwise put a brute-forceable DES crypt(password) hash
	                  on the wire.

Wire encryption is controlled by two parameters that mirror the Firebird server's
own WireCrypt / WireCryptPlugin settings:

	wire_crypt        disabled | enabled | required  (default enabled; false/true
	                  are accepted as aliases for disabled/enabled). "enabled"
	                  encrypts when the server offers an acceptable cipher but
	                  tolerates a plaintext channel; "required" fails the
	                  connection closed on every non-encrypting handshake
	                  outcome — including a legacy plain op_accept (protocol
	                  versions <= 12) and an op_accept_data with no negotiated
	                  cipher — and refuses before any credentials are sent.
	                  Note: "enabled" tolerates an active-MITM downgrade to
	                  plaintext; on untrusted networks use "required".
	wire_crypt_plugin ordered, comma-separated allow-list of acceptable ciphers
	                  (default "ChaCha64,ChaCha,Arc4"). Order is client
	                  preference; omit a cipher to refuse it — e.g.
	                  "ChaCha64,ChaCha" rejects the weak RC4/Arc4 cipher.

The negotiated cipher can be inspected via the WireCipher method on the driver
connection (see firebirdsqlConn.WireCipher), reachable through sql.Conn.Raw.
*/
package firebirdsql
