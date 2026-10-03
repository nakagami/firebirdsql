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
	"encoding/binary"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// slowCommitProxy relays a live Firebird server and, whenever the client sends
// op_commit_retaining, holds the server's next bytes for `delay`: a busy server
// that answers late, not a dead one (#288). The DSN must use wire_crypt=false so
// the opcode is visible; XDR keeps opcodes 4-byte aligned, so the client chunk is
// scanned at aligned offsets (with lazy_send the commit can share a write with a
// deferred op_free_statement).
type slowCommitProxy struct {
	ln        net.Listener
	backend   string
	delay     time.Duration
	mu        sync.Mutex
	holdUntil time.Time
	commits   int
}

func newSlowCommitProxy(backend string, delay time.Duration) (*slowCommitProxy, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	p := &slowCommitProxy{ln: ln, backend: backend, delay: delay}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go p.handle(c)
		}
	}()
	return p, nil
}

func (p *slowCommitProxy) addr() string { return p.ln.Addr().String() }
func (p *slowCommitProxy) close()       { p.ln.Close() }

func (p *slowCommitProxy) commitCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.commits
}

func (p *slowCommitProxy) handle(client net.Conn) {
	server, err := net.Dial("tcp", p.backend)
	if err != nil {
		client.Close()
		return
	}
	go func() { // client -> server: flows, arming the hold on op_commit_retaining
		buf := make([]byte, 64*1024)
		for {
			n, rerr := client.Read(buf)
			for i := 0; i+4 <= n; i += 4 {
				if binary.BigEndian.Uint32(buf[i:i+4]) == op_commit_retaining {
					p.mu.Lock()
					p.commits++
					p.holdUntil = time.Now().Add(p.delay)
					p.mu.Unlock()
					break
				}
			}
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
	buf := make([]byte, 64*1024) // server -> client: held while a commit is "running"
	for {
		n, rerr := server.Read(buf)
		if n > 0 {
			p.mu.Lock()
			wait := time.Until(p.holdUntil)
			p.mu.Unlock()
			if wait > 0 {
				time.Sleep(wait)
			}
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

func slowCommitDB(t *testing.T, delay time.Duration) (*sql.DB, *slowCommitProxy) {
	t.Helper()
	_, realDSN, err := CreateTestDatabase("test_slow_commit_")
	if err != nil {
		t.Fatalf("create test database: %v", err)
	}
	time.Sleep(1 * time.Second)
	probe, err := sql.Open("firebirdsql", realDSN)
	if err != nil {
		t.Fatal(err)
	}
	major := engineMajorVersion(probe)
	_, _ = probe.Exec("create table slow_commit (i integer)")
	probe.Close()
	if major < 3 {
		t.Skip("requires Firebird 3.0+ (as TestDirtyConnCancellation)")
	}
	proxy, err := newSlowCommitProxy(testServerAddr(), delay)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(proxy.close)
	dsn := strings.Replace(realDSN, testServerAddr(), proxy.addr(), 1)
	if strings.Contains(dsn, "?") {
		dsn += "&wire_crypt=false"
	} else {
		dsn += "?wire_crypt=false"
	}
	db, err := sql.Open("firebirdsql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if err := db.Ping(); err != nil {
		t.Fatalf("ping through proxy: %v", err)
	}
	return db, proxy
}

// TestAutocommitSlowCommit (#288): an autocommit Exec whose commit answers after
// abandonReadTimeout must succeed when the caller's context has no deadline: the
// commit carries the statement's work, so only the caller's context may bound it.
// The teardown commit (freeStatement) keeps the fixed bound; when it gives up, the
// connection must not be pooled with the late response still on the wire (the next
// query would read it and return a wrong result).
func TestAutocommitSlowCommit(t *testing.T) {
	defer func(orig time.Duration) { abandonReadTimeout = orig }(abandonReadTimeout)
	abandonReadTimeout = 1 * time.Second
	db, proxy := slowCommitDB(t, 2*time.Second)

	before := proxy.commitCount()
	if _, err := db.ExecContext(context.Background(), "insert into slow_commit values (1)"); err != nil {
		t.Fatalf("autocommit insert with a slow commit: %v", err)
	}
	if proxy.commitCount() == before {
		t.Fatal("the proxy never saw op_commit_retaining: test did not exercise the commit path")
	}

	var engine string
	if err := db.QueryRowContext(context.Background(),
		"select rdb$get_context('SYSTEM', 'ENGINE_VERSION') from rdb$database").Scan(&engine); err != nil || engine == "" {
		t.Fatalf("query after a slow commit: engine=%q err=%v (desynced connection reused?)", engine, err)
	}
	var n int
	if err := db.QueryRow("select count(*) from slow_commit").Scan(&n); err != nil || n != 1 {
		t.Fatalf("committed rows: n=%d err=%v", n, err)
	}
}

// TestAutocommitCommitHonorsContext: with a deadline, the hot-path commit read is
// bounded by it and the half-read connection is not reused.
func TestAutocommitCommitHonorsContext(t *testing.T) {
	defer func(orig time.Duration) { abandonReadTimeout = orig }(abandonReadTimeout)
	abandonReadTimeout = 1 * time.Second
	db, _ := slowCommitDB(t, 6*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	start := time.Now()
	_, err := db.ExecContext(ctx, "insert into slow_commit values (1)")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Exec error = %v, want context.DeadlineExceeded", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("Exec returned after %s, want about 1s", elapsed)
	}

	time.Sleep(6 * time.Second) // let the held response through
	var engine string
	if err := db.QueryRowContext(context.Background(),
		"select rdb$get_context('SYSTEM', 'ENGINE_VERSION') from rdb$database").Scan(&engine); err != nil || engine == "" {
		t.Fatalf("query after the abandoned commit: engine=%q err=%v (desynced connection reused?)", engine, err)
	}
}
