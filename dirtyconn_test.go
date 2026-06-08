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
	"net"
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

	proxy, err := newStallProxy("localhost:3050")
	if err != nil {
		t.Fatalf("start proxy: %v", err)
	}
	defer proxy.close()
	proxyDSN := strings.Replace(realDSN, "localhost:3050", proxy.addr(), 1)

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
	// Package tests run without t.Parallel, so this global override is safe and restored below.
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
