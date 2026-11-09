/*
Package firebirdsql provides a database/sql driver for Firebird RDBMS
(https://firebirdsql.org). It is a pure Go implementation using the Firebird
wire protocol — no C dependencies or cgo required.

# Drivers

Two driver names are registered:

  - "firebirdsql" — attach to an existing database.
  - "firebirdsql_createdb" — create the database, then attach. A database that
    already exists at that path is overwritten, and this happens on every new
    connection the pool opens, not only the first one.

To create a database once, open it with "firebirdsql_createdb", call Ping, close
that *sql.DB, and then open the database with "firebirdsql":

	db, err := sql.Open("firebirdsql_createdb", dsn)
	if err != nil {
		log.Fatal(err)
	}
	if err := db.Ping(); err != nil { // creates the database
		log.Fatal(err)
	}
	db.Close()

	db, err = sql.Open("firebirdsql", dsn)

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
wire_compress, column_name_to_lower, page_size, max_inline_blob_size,
max_blob_cache_size).

The timezone parameter takes an IANA name ("Europe/Berlin") or a fixed UTC
offset. Write a fixed offset percent-encoded on the DSN: a raw '+' in a URL
query becomes a space. Example: timezone=%2B05:30.

Authentication is controlled by two parameters:

	auth_plugin_name  preferred authentication plugin (default "Srp256"). Must be
	                  a member of auth_plugin_list.
	auth_plugin_list  ordered, comma-separated allow-list of acceptable auth
	                  plugins (default "Srp256,Srp,Legacy_Auth"; must be a subset
	                  of the supported plugins). The plugin the server selects must
	                  be a member, otherwise the connection is refused before the
	                  attach request is sent. Below wire protocol 13 there are
	                  no plugins and the only login is the Legacy_Auth hash, so the
	                  connection counts as Legacy_Auth, and Firebird 2.5 can't
	                  connect with a list that omits it. Omit a plugin to refuse it
	                  — e.g. "Srp256,Srp" rejects a server downgrade to Legacy_Auth,
	                  which would otherwise put a brute-forceable DES crypt(password)
	                  hash on the wire.

Wire encryption is controlled by two parameters that mirror the Firebird server's
own WireCrypt / WireCryptPlugin settings:

	wire_crypt        disabled | enabled | required  (default enabled; false/true
	                  are accepted as aliases for disabled/enabled). "enabled"
	                  encrypts when the server offers an acceptable cipher but
	                  tolerates a plaintext channel; "required" fails the
	                  connection closed on every non-encrypting handshake
	                  outcome — including a legacy plain op_accept (protocol
	                  versions <= 12) and an op_accept_data with no negotiated
	                  cipher — and refuses before the attach request is sent. It
	                  can't protect what the first packet already carried: with
	                  auth_plugin_name=Legacy_Auth, that includes the DES password
	                  hash. Note: "enabled" tolerates an active-MITM downgrade to
	                  plaintext; on untrusted networks use "required".
	wire_crypt_plugin ordered, comma-separated allow-list of acceptable ciphers
	                  (default "ChaCha64,ChaCha,Arc4"). Order is client
	                  preference; omit a cipher to refuse it — e.g.
	                  "ChaCha64,ChaCha" rejects the weak RC4/Arc4 cipher.

What the driver sends to log in depends on the wire protocol and the plugin the
server picks. At protocol 13+ (Firebird 3.0+) that's an SRP proof, or the DES
crypt(password) hash with Legacy_Auth. Below 13 (Firebird 2.5) it's the same hash
as password_enc. It's never the password.

The hash protects the password but not the login. Only the first 8 bytes of the
password count, it can be brute-forced, and anyone who captures it can log in with
it on a server that accepts Legacy_Auth (always on Firebird 2.5; on 3.0+ only when
AuthServer includes it). It travels unencrypted, because those protocols and
Legacy_Auth have no wire encryption.

The server picks the protocol and plugin, so with default settings a fake server
or an active attacker can ask for the hash. wire_crypt=required refuses such a
connection, and so does an auth_plugin_list that omits Legacy_Auth. The service
manager can't do so: it has no required mode and always allows Legacy_Auth, so an
untrusted network isn't safe for it.

If a server moves on to Legacy_Auth partway through an SRP login, the driver
refuses and sends no hash, unlike isql. This can happen on a server with
AuthServer = Srp, Legacy_Auth, whatever wire_crypt says. There, a user who only
has a Legacy_Auth entry can't log in that way, and a wrong password or an unknown
user gets a login error that isn't always an FbError. A Legacy_Auth-only user can
set auth_plugin_name=Legacy_Auth instead, with the effect described next.

With auth_plugin_name=Legacy_Auth, the hash is in the first packet, at every
protocol, before the server has picked anything. wire_crypt=required can't stop
that.

Passwords that change a user through UserManager, and any password in SQL text,
are ordinary request data that only wire encryption protects.

The negotiated cipher can be inspected via the WireCipher method on the driver
connection (see firebirdsqlConn.WireCipher), reachable through sql.Conn.Raw.
The negotiated wire protocol version is available via ProtocolVersion().

Protocol 16+ adds batch DML (PrepareBatch via Raw; after op_batch_msg,
protocol 17 uses op_batch_sync while create/release still use op_ping).
BLOB and array parameters are rejected. Autocommit batches commit-retain on
success and roll back on per-row errors. ExecImmediate runs SQL with
op_execute_immediate (no OpExecute trailers).

Protocol 18 adds scrollable cursors (QueryScrollable via Raw). Protocol 19 adds
inline BLOB transfer controlled by max_inline_blob_size / max_blob_cache_size.
*/
package firebirdsql
