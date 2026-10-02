/*******************************************************************************
The MIT License (MIT)

Copyright (c) 2026 Dener Rocha

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
	"strings"
	"testing"
	"time"
)

// database/sql only uses Connector.Connect(ctx) when the registered driver
// implements driver.DriverContext (#289).
var _ driver.DriverContext = (*firebirdsqlDriver)(nil)

// ctxBound is how long past the context deadline a call may take to return:
// generous for slow CI, far below the "never returns" these tests guard against.
const ctxBound = 5 * time.Second

// TestConnectHonorsContext: a peer that accepts TCP but never speaks the wire
// protocol must not hold PingContext past its context deadline. No server needed.
func TestConnectHonorsContext(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		c, err := ln.Accept()
		if err == nil {
			accepted <- c // keep it open and silent
		}
	}()
	defer func() {
		select {
		case c := <-accepted:
			c.Close()
		default:
		}
	}()

	db, err := sql.Open("firebirdsql", "user:pass@"+ln.Addr().String()+"/db.fdb")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	err = db.PingContext(ctx)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("PingContext succeeded against a silent peer")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("PingContext error = %v, want context.DeadlineExceeded", err)
	}
	if elapsed > ctxBound {
		t.Fatalf("PingContext returned after %s, want about 200ms", elapsed)
	}
}

// TestCancelBeforeConnect: an already cancelled context must not dial at all.
func TestCancelBeforeConnect(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	db, err := sql.Open("firebirdsql", "user:pass@127.0.0.1:1/db.fdb")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.PingContext(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("PingContext error = %v, want context.Canceled", err)
	}
}

// TestStalledServerHonorsContext holds the server's responses (stallProxy) at the
// round-trips that op_cancel cannot reach — the connect handshake, op_transaction
// and statement prepare — and checks each call returns at its context deadline,
// then that the pool recovers once the wire flows again.
func TestStalledServerHonorsContext(t *testing.T) {
	_, realDSN, err := CreateTestDatabase("test_ctx_deadline_")
	if err != nil {
		t.Fatalf("create test database: %v", err)
	}
	time.Sleep(1 * time.Second) // match the repo's create->attach settle

	proxy, err := newStallProxy(testServerAddr())
	if err != nil {
		t.Fatalf("start proxy: %v", err)
	}
	defer proxy.close()
	defer proxy.release()
	proxyDSN := strings.Replace(realDSN, testServerAddr(), proxy.addr(), 1)

	probe, err := sql.Open("firebirdsql", realDSN)
	if err != nil {
		t.Fatal(err)
	}
	major := engineMajorVersion(probe)
	probe.Close()
	if major < 3 {
		t.Skip("requires Firebird 3.0+ (FB 2.5 SuperServer wedges under connection churn, as in TestDirtyConnCancellation)")
	}

	// abandonReadTimeout stays at its production value on purpose: database/sql
	// closes the evicted connection in the caller's goroutine, so a Close that
	// still talked to the stalled server would push the call past ctxBound.

	expectDeadline := func(t *testing.T, what string, fn func(ctx context.Context) error) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()
		done := make(chan error, 1)
		start := time.Now()
		go func() { done <- fn(ctx) }()
		select {
		case err := <-done:
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("%s error = %v, want context.DeadlineExceeded", what, err)
			}
			if elapsed := time.Since(start); elapsed > ctxBound {
				t.Fatalf("%s returned after %s, want about 500ms", what, elapsed)
			}
		case <-time.After(30 * time.Second):
			proxy.release() // let the stuck goroutine finish before failing
			t.Fatalf("%s did not return: context deadline ignored", what)
		}
	}

	t.Run("connect", func(t *testing.T) {
		db, err := sql.Open("firebirdsql", proxyDSN)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		proxy.stall()
		expectDeadline(t, "PingContext (new connection)", db.PingContext)
		proxy.release()
		if err := db.PingContext(context.Background()); err != nil {
			t.Fatalf("ping after release: %v", err)
		}
	})

	for _, tc := range []struct {
		name string
		fn   func(ctx context.Context, db *sql.DB) error
	}{
		{"begin", func(ctx context.Context, db *sql.DB) error {
			tx, err := db.BeginTx(ctx, nil)
			if err == nil {
				tx.Rollback()
			}
			return err
		}},
		{"prepare", func(ctx context.Context, db *sql.DB) error {
			var n int
			return db.QueryRowContext(ctx, "select 1 from rdb$database").Scan(&n)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, err := sql.Open("firebirdsql", proxyDSN)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			db.SetMaxOpenConns(1)
			db.SetMaxIdleConns(1)
			if err := db.PingContext(context.Background()); err != nil {
				t.Fatalf("warm-up ping: %v", err)
			}

			proxy.stall()
			expectDeadline(t, tc.name, func(ctx context.Context) error { return tc.fn(ctx, db) })
			proxy.release()

			// The half-finished exchange must not be pooled: the next call on
			// the same *sql.DB has to work.
			var n int
			if err := db.QueryRowContext(context.Background(), "select 1 from rdb$database").Scan(&n); err != nil || n != 1 {
				t.Fatalf("query after release: n=%d err=%v", n, err)
			}
		})
	}
}
