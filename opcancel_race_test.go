/*******************************************************************************
The MIT License (MIT)

Copyright (c) 2013-2025 Hajime Nakagami

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
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"
)

// TestQueryContextCancelRace stresses the query/fetch cancellation path: it runs
// QueryContext repeatedly with short, jittered deadlines so context cancellation lands
// mid-fetch, iterating the result set each time. A handful of iterations use a generous
// deadline so the query also completes cleanly, exercising the watcher's no-cancel exit.
//
// It attaches through "firebirdsql" to a database created once: a firebirdsql_createdb pool
// recreates the database on every new connection, so the table would vanish after the first
// cancel evicts one. Each iteration takes its connection before the deadline starts, so the
// deadline lands on the query rather than on a reconnect.
//
// Beyond the table surviving the loop, the test asserts only "no data race / no panic". Its
// value is running under `go test -race`, where it guards against regressions of
// the op_cancel send-buffer data race: before withCancelWatcher joined the watcher
// goroutine, the watcher's op_cancel could run concurrently with a main-goroutine wire
// write (cancelAndDrain's op_cancel, the stray op_cancel at the top of rows.Next, or the
// next packet), racing the unsynchronized send buffer. Requires a live Firebird server.
func TestQueryContextCancelRace(t *testing.T) {
	db, _, _ := createTestDatabaseWithDDL(t, "test_opcancel_race_",
		"CREATE TABLE race_rows (id INTEGER NOT NULL PRIMARY KEY, payload VARCHAR(100))")

	// op_cancel-driven cancellation is effective on FB 3.0+, where this stress loop runs in
	// seconds and serves as the meaningful `go test -race` guard. FB 2.5 does not honor
	// op_cancel the same way (each cancel falls through to the ~3s OS-deadline + 10s
	// cancelAndDrain fallback) and its SuperServer wedges under the connection churn this
	// test induces, so skip it there — matching the repo's other FB-2.5 skips. The fix
	// itself is server-version-independent and is verified under -race on FB 3.0+.
	if engineMajorVersion(db) < 3 {
		t.Skip("requires Firebird 3.0+ (op_cancel cancellation semantics under load)")
	}

	// Enough rows that a fetch spans many chunks (opFetch requests 400 rows per round
	// trip), so cancellation reliably lands between fetches as well as during one.
	const nrows = 4000
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	stmt, err := tx.Prepare("INSERT INTO race_rows (id, payload) VALUES (?, ?)")
	if err != nil {
		t.Fatalf("prepare insert: %v", err)
	}
	for i := 0; i < nrows; i++ {
		if _, err = stmt.Exec(i, fmt.Sprintf("row-%05d-padding-padding-padding-padding", i)); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}
	stmt.Close()
	if err = tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// A server occasionally leaves a cancelled query unanswered until the driver's
	// fallback timeouts give up on it (about 13 s); the budget keeps a run of those
	// well inside go test's default package timeout.
	const iterations = 100
	budget := time.Now().Add(60 * time.Second)
	for i := 0; i < iterations; i++ {
		if time.Now().After(budget) {
			t.Logf("time budget used up after %d of %d iterations", i, iterations)
			break
		}
		// Mostly sub-millisecond, jittered, so cancels land at varying points in the
		// chunked fetch; every 7th iteration gets a generous deadline so the query can
		// finish and the watcher takes its clean no-cancel exit.
		var timeout time.Duration
		if i%7 == 0 {
			timeout = 5 * time.Second
		} else {
			timeout = time.Duration(150+(i*53)%900) * time.Microsecond
		}

		conn := raceConn(t, db)
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		rows, err := conn.QueryContext(ctx, "SELECT id, payload FROM race_rows ORDER BY id")
		if err == nil {
			for rows.Next() {
				var id int
				var payload string
				if scanErr := rows.Scan(&id, &payload); scanErr != nil {
					break
				}
			}
			rows.Close() // ignore error; cancellation surfaces as ctx error / ErrBadConn
		} // else the deadline fired during prepare/execute, before any rows — fine
		cancel()
		conn.Close() // returns the connection to the pool, or discards it if it went bad
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var n int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM race_rows").Scan(&n); err != nil || n != nrows {
		t.Fatalf("race_rows after the cancel loop: n=%d err=%v", n, err)
	}
}

// raceConn takes a connection outside the iteration's deadline: dialing and
// authenticating a replacement for an evicted connection takes longer than the
// sub-millisecond deadlines above, which would then expire before the query.
func raceConn(t *testing.T, db *sql.DB) *sql.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	return conn
}

// engineMajorVersion returns the Firebird engine major version (e.g. 2, 3, 5), or 0 if it
// cannot be determined. It queries ENGINE_VERSION over the existing connection rather than
// the Service Manager, which is not consistently reachable on the FB 2.5 CI container.
func engineMajorVersion(db *sql.DB) int {
	var s string
	if err := db.QueryRow("SELECT rdb$get_context('SYSTEM', 'ENGINE_VERSION') FROM rdb$database").Scan(&s); err != nil {
		return 0
	}
	var major int
	_, _ = fmt.Sscanf(s, "%d", &major)
	return major
}
