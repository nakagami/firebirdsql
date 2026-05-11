# firebirdsql (Go firebird sql driver)

Firebird RDBMS https://firebirdsql.org SQL driver for Go

## Requirements

* Firebird 2.5 or higher
* Golang 1.20 or higher

## Example

```go
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
```


See also driver_test.go

```go
   package main

      import (
       "fmt"
       "github.com/nakagami/firebirdsql"
   )

   func main() {
       dsn := "user:password@servername/foo/bar.fdb"
       events := []string{"my_event", "order_created"}
       fbEvent, _ := firebirdsql.NewFBEvent(dsn)
       defer fbEvent.Close()
       sbr, _ := fbEvent.Subscribe(events, func(event firebirdsql.Event) { //or use SubscribeChan
           fmt.Printf("event: %s, count: %d, id: %d, remote id:%d \n", event.Name, event.Count, event.ID, event.RemoteID)
       })
       defer sbr.Unsubscribe()
       go func() {
               fbEvent.PostEvent(events[0])
               fbEvent.PostEvent(events[1])
       }()
       <- make(chan struct{}) //wait
   }
```

See also _example

## Connection string

```bash
user:password@servername[:port_number]/database_name_or_file[?params1=value1[&param2=value2]...]
```

Examples:
```bash
# Basic connection
user:password@localhost/path/to/database.fdb

# With wire compression enabled (for better performance over slow networks)
user:password@localhost/path/to/database.fdb?wire_compress=true

# With multiple parameters
user:password@localhost/path/to/database.fdb?wire_crypt=true&wire_compress=true&role=admin
```

### Reserved characters

The DSN is parsed as an RFC 3986 URI (the driver prepends `firebird://` automatically).
Reserved characters in the **password** or **database path** must be percent-encoded,
otherwise they are misinterpreted as URI delimiters:

| Character | Encoded | Why it breaks things |
|-----------|---------|----------------------|
| `@`       | `%40`   | ends the userinfo section |
| `:`       | `%3A`   | splits user from password |
| `#`       | `%23`   | starts the fragment (silently dropped) |
| `?`       | `%3F`   | starts the query string |
| `/`       | `%2F`   | path separator |
| `&`       | `%26`   | separates query parameters |
| `=`       | `%3D`   | separates a query key from its value |
| `%`       | `%25`   | the escape character itself |
| space     | `%20`   | invalid in a URI |

Use [`url.QueryEscape`](https://pkg.go.dev/net/url#QueryEscape) to encode a password:

```go
import "net/url"

pass := url.QueryEscape("p@ss:w#rd")  // → "p%40ss%3Aw%23rd"
dsn  := "sysdba:" + pass + "@localhost/var/lib/firebird/mydb.fdb"
db, err := sql.Open("firebirdsql", dsn)
```

### Drivers

Two driver names are registered with `database/sql`:

- `"firebirdsql"` — attach to an existing database.
- `"firebirdsql_createdb"` — create the database if it does not exist, then attach.
  See `_examples/service_manager.go` for a usage example.

### General

- user: login user
- password: login password
- servername: Firebird server's host name or IP address.
- port_number: Port number. default value is 3050.
- database_name_or_file: Database path (or alias name).

### Optional

param1, param2... are

| Name | Description | Default | Note |
| --- | --- | --- | --- |
| auth_plugin_name | Authentication plugin name. | Srp256 | Srp256/Srp/Legacy_Auth are available. |
| column_name_to_lower | Force column name to lower | false | For "github.com/jmoiron/sqlx" |
| role | Role name | | |
| timezone | IANA time zone name (e.g. `UTC`, `Europe/Berlin`) | | Controls client-side decoding of naive DATE/TIME/TIMESTAMP and server session time zone (FB 4+). See "Time and timestamp handling" below. |
| wire_crypt | Enable wire data encryption or not. | true | For Firebird 3.0+ |
| wire_compress | Enable wire protocol compression. | false | For Firebird 3.0+ (protocol version 13+) |
| charset | Firebird Charecter Set | | |

## Time and timestamp handling

Firebird's `DATE`, `TIME`, and `TIMESTAMP` types store wall-clock components without zone information — by design. When the driver decodes such a column into a Go `time.Time`, it must attach some `*time.Location`. Resolution order:

1. If `?timezone=<IANA>` is set on the DSN, that location is used.
2. Otherwise `time.Local` is used (matching Firebird's Java driver Jaybird, which uses the JVM default).

For cross-host determinism (e.g. app servers in different time zones all reading the same rows), set `?timezone=UTC` on the DSN:

```
user:password@host/path/to/db?timezone=UTC
```

The same parameter also tells a Firebird 4+ server to evaluate `CURRENT_TIMESTAMP` and `LOCALTIMESTAMP` in that zone.

For applications that need explicit zone semantics at the schema level, use the Firebird 4+ types `TIME WITH TIME ZONE` and `TIMESTAMP WITH TIME ZONE`. Those columns carry their zone on the wire and are unaffected by the `?timezone=` fallback.

Round-trip of naive `time.Time` values is **wall-clock-preserving**, not absolute-instant-preserving: inserting a value in zone A and reading it back on a host in zone B will return identical wall-clock components (year, month, day, hour, minute, second, nanosecond), but `a.Equal(b)` may be false. This matches Firebird's underlying storage model.

## Transactions: READ COMMITTED + NOWAIT

Firebird supports `NOWAIT` transactions (lock conflicts return immediately instead of waiting).
The standard `database/sql` `sql.TxOptions` doesn't have a knob for `NOWAIT`, so this driver exposes a driver-specific isolation level constant:

```go
tx, err := db.BeginTx(ctx, &sql.TxOptions{
	Isolation: firebirdsql.LevelReadCommittedNoWait,
})
```

This maps to a transaction TPB containing `READ COMMITTED`, `RECORD VERSION`, and `NOWAIT`.

## GORM for Firebird

See https://github.com/flylink888/gorm-firebird
