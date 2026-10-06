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
	"database/sql/driver"
	"errors"
	"net"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

// stallProxy is a transparent TCP proxy: client->server bytes always flow, but server->client
// bytes are held while "stalled". This makes the driver's OS-deadline fallback path (the rare
// Go-timer-starvation case the cancellation hardening defends against) deterministic — the
// client's conn.SetDeadline still fires through the stall while the server's response is
// withheld.
type stallProxy struct {
	ln      net.Listener
	backend string
	mu      sync.Mutex
	cond    *sync.Cond
	stalled bool

	// Opcode-triggered stall: once armed, the next client->server write that starts with
	// this opcode stalls the server->client direction for stallFor, one-shot.
	stallOnOpcode int32
	stallFor      time.Duration
	stallHits     int
}

func newStallProxy(backend string) (*stallProxy, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	p := &stallProxy{ln: ln, backend: backend}
	p.cond = sync.NewCond(&p.mu)
	go p.acceptLoop()
	return p, nil
}

func (p *stallProxy) addr() string { return p.ln.Addr().String() }
func (p *stallProxy) close()       { p.ln.Close() }

func (p *stallProxy) stall() {
	p.mu.Lock()
	p.stalled = true
	p.mu.Unlock()
}

func (p *stallProxy) release() {
	p.mu.Lock()
	p.stalled = false
	p.cond.Broadcast()
	p.mu.Unlock()
}

func (p *stallProxy) waitWhileStalled() {
	p.mu.Lock()
	for p.stalled {
		p.cond.Wait()
	}
	p.mu.Unlock()
}

// armStallOnOpcode arms a one-shot stall: the next client->server write carrying op (see
// observeClientChunk) stalls the server->client direction for d, then self-releases. The
// stall starts before that write is forwarded, so the server's reply cannot slip through.
// Opcodes are matched in cleartext: the DSN needs wire_crypt=disabled and no wire_compress.
func (p *stallProxy) armStallOnOpcode(op int32, d time.Duration) {
	p.mu.Lock()
	p.stallOnOpcode = op
	p.stallFor = d
	p.mu.Unlock()
}

// stallHitCount reports how many times an armed opcode was observed and triggered a stall.
func (p *stallProxy) stallHitCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.stallHits
}

// observeClientChunk checks a client->server write against the armed opcode (if any) and
// triggers the stall exactly once when it matches. The match is checked at exactly two offsets:
// chunk offset 0, and offset 12 immediately following a 12-byte op_free_statement packet that
// starts the chunk. It never scans arbitrary 4-byte-aligned offsets, where an opcode value can
// also appear in an unrelated handle or length word. The offset-12 case exists because the
// free-statement write is lazy (no read follows it before the caller's next write), so it can
// coalesce into the same proxy read as the very next packet, e.g. the op_commit/op_rollback
// that follows closing a transaction's statement.
func (p *stallProxy) observeClientChunk(chunk []byte) {
	p.mu.Lock()
	op := p.stallOnOpcode
	if op == 0 {
		p.mu.Unlock()
		return
	}
	matched := len(chunk) >= 4 && bytes_to_bint32(chunk[:4]) == op
	if !matched && len(chunk) >= 16 && bytes_to_bint32(chunk[:4]) == op_free_statement {
		matched = bytes_to_bint32(chunk[12:16]) == op
	}
	if !matched {
		p.mu.Unlock()
		return
	}
	p.stallOnOpcode = 0
	p.stallHits++
	p.stalled = true
	d := p.stallFor
	p.mu.Unlock()
	time.AfterFunc(d, p.release)
}

func (p *stallProxy) acceptLoop() {
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		go p.handle(c)
	}
}

func (p *stallProxy) handle(client net.Conn) {
	server, err := net.Dial("tcp", p.backend)
	if err != nil {
		client.Close()
		return
	}
	go func() { // client -> server: always flows
		buf := make([]byte, 64*1024)
		for {
			n, rerr := client.Read(buf)
			if n > 0 {
				p.observeClientChunk(buf[:n])
				if _, werr := server.Write(buf[:n]); werr != nil {
					break
				}
			}
			if rerr != nil {
				break
			}
		}
		server.Close()
	}()
	buf := make([]byte, 64*1024) // server -> client: gated by the stall
	for {
		n, rerr := server.Read(buf)
		if n > 0 {
			p.waitWhileStalled()
			if _, werr := client.Write(buf[:n]); werr != nil {
				break
			}
		}
		if rerr != nil {
			break
		}
	}
	client.Close()
}

// TestDirtyConnCancellation drives the cancellation dirty-connection fixes through a stalling
// TCP proxy in front of a live Firebird server. With the server's responses held, a
// QueryContext deadline that fires mid-fetch must:
//
//	teardown bound  not hang the *automatic* rows-close (database/sql's awaitDone ->
//	                rows.Close() -> freeStatement) on the silent wire — every teardown
//	                opResponse is now OS-deadline bounded, so the call returns from the
//	                deadlines, not from the stall release.
//	conn eviction   leave the desynced connection *evicted*, not pooled — rows.Next wraps
//	                driver.ErrBadConn on ctx expiry, so a fresh query on the same *sql.DB
//	                succeeds instead of reading the leftover fetch bytes where it expects
//	                op_response ("Error op_response:1").
//
// Gated to Firebird 3.0+ (matching TestQueryContextCancelRace): op_cancel semantics differ on
// FB 2.5 and its SuperServer wedges under this connection churn.
func TestDirtyConnCancellation(t *testing.T) {
	_, realDSN, err := CreateTestDatabase("test_dirtyconn_")
	if err != nil {
		t.Fatalf("create test database: %v", err)
	}
	time.Sleep(1 * time.Second) // match the repo's create->attach settle

	proxy, err := newStallProxy(testServerAddr())
	if err != nil {
		t.Fatalf("start proxy: %v", err)
	}
	defer proxy.close()
	proxyDSN := strings.Replace(realDSN, testServerAddr(), proxy.addr(), 1)

	db, err := sql.Open("firebirdsql", proxyDSN)
	if err != nil {
		t.Fatalf("open through proxy: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	if err := db.PingContext(context.Background()); err != nil {
		t.Fatalf("ping through proxy: %v", err)
	}
	if engineMajorVersion(db) < 3 {
		t.Skip("requires Firebird 3.0+ (op_cancel cancellation semantics; FB 2.5 SuperServer wedges under connection churn)")
	}

	// Shorten the teardown bound so the test exercises the several sequential stalled reads
	// (freeStatement, commit-retaining, rollback, detach) quickly; production default is 10s.
	// No top-level test is parallel (only subtests, which finish before their parent
	// returns), so this global override is safe and restored below.
	defer func(orig time.Duration) { abandonReadTimeout = orig }(abandonReadTimeout)
	abandonReadTimeout = 2 * time.Second

	const stallRelease = 40 * time.Second
	// A selectable execute block: rows arrive via cursor fetch, so the stall lands on the
	// fetch response (not the execute ack).
	const twoRowSelect = `execute block returns (i integer) as begin i = 1; suspend; i = 2; suspend; end`

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	rows, err := db.QueryContext(ctx, twoRowSelect)
	if err != nil {
		// Deadline occasionally fires during execute (before any rows) — that path is already
		// covered by 6f05210 and isn't what this test targets.
		t.Skipf("deadline fired during execute, not fetch: %v", err)
	}

	// Stall AFTER the execute response, BEFORE the first fetch; release well later so we can
	// distinguish "returned because the deadlines fired" from "returned only on release".
	proxy.stall()
	releaseTimer := time.AfterFunc(stallRelease, proxy.release)
	defer releaseTimer.Stop()
	defer proxy.release()

	done := make(chan error, 1)
	start := time.Now()
	go func() {
		for rows.Next() {
			var v int
			_ = rows.Scan(&v)
		}
		cerr := rows.Err()
		rows.Close() // reached automatically via awaitDone too; calling it explicitly is fine
		done <- cerr
	}()

	var iterErr error
	select {
	case iterErr = <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("rows iteration/close hung > 30s on the stalled wire: a teardown opResponse is not bounded")
	}
	elapsed := time.Since(start)

	// Must have returned from the OS deadlines/teardown bounds, not by waiting out the stall.
	if elapsed >= stallRelease-5*time.Second {
		t.Fatalf("rows iteration/close took %s — it appears to have waited for the stall release (%s); a teardown read is not bounded", elapsed, stallRelease)
	}
	// The cancelled fetch must surface an error, not a silent success.
	if iterErr == nil {
		t.Fatal("expected a context-deadline error from the cancelled fetch, got nil")
	}

	proxy.release() // also covered by defers; lets a fresh connection be established now

	// The desynced connection must have been evicted, not pooled. A fresh query on the
	// same *sql.DB must succeed — no leftover-bytes "Error op_response:1".
	pctx, pcancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer pcancel()
	var probe int
	if err := db.QueryRowContext(pctx, "SELECT 6809 FROM rdb$database").Scan(&probe); err != nil {
		t.Fatalf("reuse query after cancellation failed (poisoned conn returned to pool): %v", err)
	}
	if probe != 6809 {
		t.Fatalf("reuse query returned %d, want 6809 (conn returned misaligned data)", probe)
	}
}

// newStallProxyDB opens a *sql.DB through a fresh stallProxy in front of a fresh test
// database, with wire_crypt=disabled so the proxy can match opcodes in cleartext. It shortens
// abandonReadTimeout to 2s (restored at cleanup); the production default is 10s. It skips when
// the server rejects a plaintext attach, and on Firebird < 3.0 (same gate and reason as
// TestDirtyConnCancellation). Each ddl statement runs before the proxy is armed.
func newStallProxyDB(t *testing.T, prefix string, ddl ...string) (*sql.DB, *stallProxy) {
	t.Helper()
	file, realDSN, err := CreateTestDatabase(prefix)
	if err != nil {
		t.Fatalf("create test database: %v", err)
	}
	time.Sleep(1 * time.Second) // match the repo's create->attach settle

	proxy, err := newStallProxy(testServerAddr())
	if err != nil {
		_ = os.Remove(file)
		t.Fatalf("start proxy: %v", err)
	}
	proxyDSN := strings.Replace(realDSN, testServerAddr(), proxy.addr(), 1) + "?wire_crypt=disabled"

	db, err := sql.Open("firebirdsql", proxyDSN)
	if err != nil {
		proxy.close()
		_ = os.Remove(file)
		t.Fatalf("open through proxy: %v", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	pctx, pcancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer pcancel()
	if err := db.PingContext(pctx); err != nil {
		db.Close()
		proxy.close()
		_ = os.Remove(file)
		t.Skipf("server rejects plaintext (wire_crypt=disabled) connections: %v", err)
	}
	if engineMajorVersion(db) < 3 {
		db.Close()
		proxy.close()
		_ = os.Remove(file)
		t.Skip("requires Firebird 3.0+ (op_cancel cancellation semantics; FB 2.5 SuperServer wedges under connection churn)")
	}

	orig := abandonReadTimeout
	abandonReadTimeout = 2 * time.Second

	// t.Cleanup runs LIFO: db.Close, proxy.release, proxy.close, remove file, restore timeout.
	t.Cleanup(func() { abandonReadTimeout = orig })
	t.Cleanup(func() { _ = os.Remove(file) })
	t.Cleanup(func() { proxy.close() })
	t.Cleanup(func() { proxy.release() })
	t.Cleanup(func() { db.Close() })

	for _, q := range ddl {
		mustExec(t, context.Background(), db, q)
	}
	return db, proxy
}

func requireStallHit(t *testing.T, proxy *stallProxy) {
	t.Helper()
	if n := proxy.stallHitCount(); n != 1 {
		t.Fatalf("stallHitCount() = %d, want 1 (opcode trigger missed; the test proves nothing)", n)
	}
}

func requireProbe(t *testing.T, db *sql.DB) {
	t.Helper()
	var probe int
	if err := db.QueryRow("SELECT 6809 FROM rdb$database").Scan(&probe); err != nil {
		t.Fatalf("probe query: %v", err)
	}
	if probe != 6809 {
		t.Fatalf("probe = %d, want 6809 (conn returned misaligned data)", probe)
	}
}

// TestSlowAutocommitCommit verifies that an autocommit commit whose reply takes longer than
// abandonReadTimeout succeeds under context.Background(): the reply carries server-side work
// (e.g. the delta merge behind ALTER DATABASE END BACKUP), so only the caller's context may
// bound it. The connection stays pooled and in sync.
func TestSlowAutocommitCommit(t *testing.T) {
	db, proxy := newStallProxyDB(t, "test_slow_commit_")

	const stall = 3 * time.Second // longer than the 2s abandonReadTimeout
	proxy.armStallOnOpcode(op_commit_retaining, stall)

	start := time.Now()
	if _, err := db.ExecContext(context.Background(), "RECREATE TABLE t288 (id INTEGER)"); err != nil {
		t.Fatalf("exec with a slow commit: %v", err)
	}
	if elapsed := time.Since(start); elapsed < stall {
		t.Fatalf("exec returned after %v, before the %v stall ended", elapsed, stall)
	}
	requireStallHit(t, proxy)
	if n := db.Stats().OpenConnections; n != 1 {
		t.Fatalf("OpenConnections = %d, want 1 (conn should stay pooled)", n)
	}
	requireProbe(t, db)
}

// TestSlowQueryRowCommit is TestSlowAutocommitCommit for the rows path: for
// INSERT ... RETURNING through QueryRow the only autocommit commit runs in rows.Close, bounded
// by the query's context. The row must be committed exactly once.
func TestSlowQueryRowCommit(t *testing.T) {
	db, proxy := newStallProxyDB(t, "test_slow_qr_commit_", "CREATE TABLE t288q (id INTEGER)")

	const stall = 3 * time.Second
	// rows.Close writes op_free_statement (lazy, no read) then op_commit_retaining; stalling
	// from the free holds both replies.
	proxy.armStallOnOpcode(op_free_statement, stall)

	start := time.Now()
	var id int
	if err := db.QueryRowContext(context.Background(), "INSERT INTO t288q (id) VALUES (1) RETURNING id").Scan(&id); err != nil {
		t.Fatalf("QueryRow with a slow commit: %v", err)
	}
	if elapsed := time.Since(start); elapsed < stall {
		t.Fatalf("QueryRow returned after %v, before the %v stall ended", elapsed, stall)
	}
	if id != 1 {
		t.Fatalf("RETURNING id = %d, want 1", id)
	}
	requireStallHit(t, proxy)
	if n := db.Stats().OpenConnections; n != 1 {
		t.Fatalf("OpenConnections = %d, want 1 (conn should stay pooled)", n)
	}
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM t288q").Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Fatalf("COUNT(*) = %d, want 1", count)
	}
}

// TestQueryRowCommitContextDeadline is the rows-path twin of TestAutocommitCommitContextDeadline:
// for INSERT ... RETURNING through QueryRow the autocommit commit runs in rows.Close, and when
// its reply is still missing after the query context's deadline, rows.Close must report the
// failure (Row.Scan returns rows.Close's error) tagged ErrBadConn, so the connection is
// discarded instead of pooled with the reply unread. The row count is deliberately not
// checked: the proxy only holds the reply back, so the server did commit, and the error
// means "outcome unknown", not "rolled back".
func TestQueryRowCommitContextDeadline(t *testing.T) {
	db, proxy := newStallProxyDB(t, "test_qr_commit_deadline_", "CREATE TABLE t288r (id INTEGER)")

	// rows.Close writes op_free_statement (lazy, no read) then op_commit_retaining; stalling
	// from the free holds both replies past the failure (the commit read fails at the 1s
	// deadline; the desynced conn is then closed without further reads).
	proxy.armStallOnOpcode(op_free_statement, 10*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	var id int
	err := db.QueryRowContext(ctx, "INSERT INTO t288r (id) VALUES (1) RETURNING id").Scan(&id)
	if !errors.Is(err, driver.ErrBadConn) {
		t.Fatalf("QueryRow err = %v, want an error wrapping driver.ErrBadConn", err)
	}
	requireStallHit(t, proxy)
	if n := db.Stats().OpenConnections; n != 0 {
		t.Fatalf("OpenConnections = %d, want 0 (desynced conn must be discarded)", n)
	}

	proxy.release()
	requireProbe(t, db)
}

// TestAutocommitCommitContextDeadline verifies that a commit reply still missing when the
// caller's context deadline passes fails with the context error, and that the connection,
// whose reply is still unread, is discarded instead of pooled. database/sql cannot retry a
// done context, so the statement is not executed again.
func TestAutocommitCommitContextDeadline(t *testing.T) {
	db, proxy := newStallProxyDB(t, "test_commit_deadline_")

	// Longer than the failure path: the commit read fails at the 1s deadline, then the
	// desynced conn is refused by Stmt.Close and closed without further reads.
	proxy.armStallOnOpcode(op_commit_retaining, 8*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	_, err := db.ExecContext(ctx, "RECREATE TABLE t288d (id INTEGER)")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("exec err = %v, want context.DeadlineExceeded", err)
	}
	requireStallHit(t, proxy)
	if n := db.Stats().OpenConnections; n != 0 {
		t.Fatalf("OpenConnections = %d, want 0 (desynced conn must be discarded)", n)
	}

	proxy.release()
	requireProbe(t, db)
}

// TestTeardownCommitTimeoutEvictsConn covers the other half of the report: when the
// teardown commit (Stmt.Close after an autocommit Exec, read under the fixed bound) gives
// up, its reply is still owed on the wire. The Exec itself succeeded, so nothing tags the
// error; the connection must still be discarded rather than pooled, or the next query
// reads that late reply ("sql: no rows in result set" from a query that always has a row).
func TestTeardownCommitTimeoutEvictsConn(t *testing.T) {
	db, proxy := newStallProxyDB(t, "test_teardown_commit_")

	// fc.exec commits in stmt.exec, then its deferred Stmt.Close writes op_free_statement
	// (lazy, no read) and a second op_commit_retaining; stalling from that free holds the
	// teardown commit's reply past abandonReadTimeout (2s).
	proxy.armStallOnOpcode(op_free_statement, 4*time.Second)

	if _, err := db.ExecContext(context.Background(), "RECREATE TABLE t288e (id INTEGER)"); err != nil {
		t.Fatalf("exec: %v", err)
	}
	requireStallHit(t, proxy)
	if n := db.Stats().OpenConnections; n != 0 {
		t.Fatalf("OpenConnections = %d, want 0 (conn with an unread commit reply must be discarded)", n)
	}

	proxy.release()
	requireProbe(t, db)
}

// TestSlowPreparedQueryCommit covers a prepared statement that is not a SELECT, run with
// Query on a sql.Conn: it has no cursor to free, so its autocommit commit happens in
// rows.Close, bounded by the query's context like any other commit. A reply slower than
// abandonReadTimeout must not fail it, and the row is committed once.
func TestSlowPreparedQueryCommit(t *testing.T) {
	db, proxy := newStallProxyDB(t, "test_slow_prep_commit_", "CREATE TABLE t288p (id INTEGER)")

	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("conn: %v", err)
	}
	defer conn.Close()
	stmt, err := conn.PrepareContext(ctx, "INSERT INTO t288p (id) VALUES (?)")
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	defer stmt.Close()

	const stall = 3 * time.Second // longer than the 2s abandonReadTimeout
	proxy.armStallOnOpcode(op_commit_retaining, stall)

	start := time.Now()
	rows, err := stmt.QueryContext(ctx, 1)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	for rows.Next() {
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("rows.Close with a slow commit: %v", err)
	}
	if elapsed := time.Since(start); elapsed < stall {
		t.Fatalf("rows.Close returned after %v, before the %v stall ended (no commit there?)", elapsed, stall)
	}
	requireStallHit(t, proxy)

	var count int
	if err := conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM t288p").Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Fatalf("COUNT(*) = %d, want 1", count)
	}
}

// TestIdleCancelDoesNotLeak pins the server behaviour the statement read paths rely on:
// when the context's cancel watcher fires just after a statement already finished with a
// server error, its op_cancel reaches an idle attachment. The driver keeps such a
// connection (the reply was read in full), so that late cancel must not fail the next
// request: here, the next statement and the commit of the same Tx.
func TestIdleCancelDoesNotLeak(t *testing.T) {
	db, _, _ := createTestDatabaseWithDDL(t, "test_idle_cancel_")
	if engineMajorVersion(db) < 3 {
		t.Skip("requires Firebird 3.0+ (op_cancel semantics differ on 2.5, as in TestDirtyConnCancellation)")
	}
	db.SetMaxOpenConns(1)
	ctx := context.Background()
	mustExec(t, ctx, db, "CREATE TABLE t_idle_cancel (id INTEGER)")

	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO t_idle_cancel (id) VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	if err := conn.Raw(func(dc any) error {
		return dc.(*firebirdsqlConn).wp.opCancel(fb_cancel_raise)
	}); err != nil {
		t.Fatalf("op_cancel on the idle attachment: %v", err)
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO t_idle_cancel (id) VALUES (2)"); err != nil {
		t.Fatalf("statement after an idle op_cancel: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit after an idle op_cancel: %v", err)
	}
	var n int
	if err := conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM t_idle_cancel").Scan(&n); err != nil || n != 2 {
		t.Fatalf("COUNT(*) = %d, err = %v; want 2 rows committed", n, err)
	}
}

// TestTxSurvivesCancelledFetch: a Tx query whose context ends while the server is busy in a
// fetch. The watcher's op_cancel stops the fetch and the server answers it with
// isc_cancelled, a reply read in full, so the wire is in step and the Tx must still commit
// its earlier work.
func TestTxSurvivesCancelledFetch(t *testing.T) {
	db, _, _ := createTestDatabaseWithDDL(t, "test_fetch_cancel_")
	if engineMajorVersion(db) < 3 {
		t.Skip("requires Firebird 3.0+ (op_cancel semantics differ on 2.5, as in TestDirtyConnCancellation)")
	}
	db.SetMaxOpenConns(1)
	ctx := context.Background()
	mustExec(t, ctx, db, "CREATE TABLE t_fetch_cancel (id INTEGER)")

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, "INSERT INTO t_fetch_cancel (id) VALUES (1)"); err != nil {
		t.Fatal(err)
	}

	// One row, then a loop far longer than the context: the cursor's first fetch runs the
	// loop on the server, so the context ends while that fetch is in progress.
	const busyFetch = `execute block returns (i integer) as
		declare n bigint = 0;
		begin
			i = 1; suspend;
			while (n < 10000000000) do n = n + 1;
			i = 2; suspend;
		end`
	qctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	rows, err := tx.QueryContext(qctx, busyFetch)
	if errors.Is(err, context.DeadlineExceeded) {
		t.Skipf("deadline fired during execute, not fetch: %v", err)
	}
	if err != nil {
		t.Fatalf("QueryContext: %v", err)
	}
	start := time.Now()
	for rows.Next() {
	}
	if err := rows.Err(); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("rows.Err() = %v, want context.DeadlineExceeded", err)
	}
	rows.Close()
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("the cancelled fetch took %v; the OS deadline, not the server's cancel, ended it", elapsed)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit after a cleanly cancelled fetch: %v (the Tx was lost)", err)
	}
	var n int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM t_fetch_cancel").Scan(&n); err != nil || n != 1 {
		t.Fatalf("COUNT(*) = %d, err = %v; want the Tx's row committed", n, err)
	}
}

// TestBatchOverflowKeepsConnInStep: the server refuses an op_batch_msg once the rows no
// longer fit its batch buffer ("batch too big"). The reply to the op_ping / op_batch_sync
// sent after it must still be read, or every later request on the connection reads the
// previous one's reply (a "SELECT ... FROM rdb$database" then returns no rows).
func TestBatchOverflowKeepsConnInStep(t *testing.T) {
	db, _, _ := createTestDatabaseWithDDL(t, "test_batch_overflow_")
	requireBatchSupport(t, db)
	db.SetMaxOpenConns(1)
	ctx := context.Background()
	mustExec(t, ctx, db, "CREATE TABLE t_batch_overflow (id INTEGER, s VARCHAR(2000))")

	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	var b *PreparedBatch
	if err := conn.Raw(func(dc any) error {
		var e error
		b, e = dc.(*firebirdsqlConn).PrepareBatch(ctx,
			"INSERT INTO t_batch_overflow (id, s) VALUES (?, ?)", BatchOptions{BufferBytes: 64 * 1024})
		return e
	}); err != nil {
		t.Fatalf("PrepareBatch: %v", err)
	}
	pad := strings.Repeat("x", 1900)
	var addErr error
	for i := 0; i < 5000 && addErr == nil; i++ {
		addErr = b.Add(int64(i), pad) // flushes every 128 KiB: well past a 64 KiB buffer
	}
	var fbErr *FbError
	if !errors.As(addErr, &fbErr) || !slices.Contains(fbErr.GDSCodes, ISCBatchTooBig) {
		t.Fatalf("Add err = %v, want the server's batch-too-big refusal", addErr)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("batch Close: %v", err)
	}

	var v int
	if err := conn.QueryRowContext(ctx, "SELECT 6809 FROM rdb$database").Scan(&v); err != nil || v != 6809 {
		t.Fatalf("query after the refused batch = %d, %v; want 6809 (the connection is out of step)", v, err)
	}
	var n int
	if err := conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM t_batch_overflow").Scan(&n); err != nil || n != 0 {
		t.Fatalf("COUNT(*) = %d, %v; want 0 (the batch never ran)", n, err)
	}
}

// TestTxEndLateResponse verifies that Tx.Commit and a user-called Tx.Rollback under a
// context.Background() BeginTx wait for a reply slower than abandonReadTimeout.
func TestTxEndLateResponse(t *testing.T) {
	for _, tc := range []struct {
		name      string
		op        int32
		end       func(*sql.Tx) error
		wantCount int
	}{
		{"commit", op_commit, (*sql.Tx).Commit, 1},
		{"rollback", op_rollback, (*sql.Tx).Rollback, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, proxy := newStallProxyDB(t, "test_tx_end_late_", "CREATE TABLE t288t (id INTEGER)")

			tx, err := db.BeginTx(context.Background(), nil)
			if err != nil {
				t.Fatalf("begin: %v", err)
			}
			if _, err := tx.Exec("INSERT INTO t288t (id) VALUES (1)"); err != nil {
				t.Fatalf("insert: %v", err)
			}

			const stall = 3 * time.Second
			proxy.armStallOnOpcode(tc.op, stall)
			start := time.Now()
			if err := tc.end(tx); err != nil {
				t.Fatalf("%s with a slow reply: %v", tc.name, err)
			}
			if elapsed := time.Since(start); elapsed < stall {
				t.Fatalf("%s returned after %v, before the %v stall ended", tc.name, elapsed, stall)
			}
			requireStallHit(t, proxy)

			var count int
			if err := db.QueryRow("SELECT COUNT(*) FROM t288t").Scan(&count); err != nil {
				t.Fatalf("count: %v", err)
			}
			if count != tc.wantCount {
				t.Fatalf("COUNT(*) = %d, want %d", count, tc.wantCount)
			}
		})
	}
}

// TestTxContextCancelRollbackTeardown verifies that when the BeginTx context is canceled with
// no statement in flight, database/sql's awaitDone goroutine calls Rollback automatically;
// that call must take the fixed-bound teardown read (there is no live context left to bound
// it by) rather than wait for the stalled reply. database/sql always discards the connection
// after that rollback (no driver.SessionResetter), so OpenConnections reaching 0 within 20s
// is what proves the bound, not the eviction itself.
func TestTxContextCancelRollbackTeardown(t *testing.T) {
	db, proxy := newStallProxyDB(t, "test_tx_cancel_")

	ctx, cancel := context.WithCancel(context.Background())
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}

	proxy.armStallOnOpcode(op_rollback, 40*time.Second)

	cancel()

	deadline := time.Now().Add(20 * time.Second)
	for db.Stats().OpenConnections != 0 {
		if time.Now().After(deadline) {
			t.Fatal("awaitDone rollback is not bounded: OpenConnections did not reach 0 within 20s")
		}
		time.Sleep(100 * time.Millisecond)
	}

	if err := tx.Rollback(); !errors.Is(err, sql.ErrTxDone) {
		t.Fatalf("tx.Rollback() after awaitDone err = %v, want sql.ErrTxDone", err)
	}
	requireStallHit(t, proxy)

	proxy.release()
	requireProbe(t, db)
}
