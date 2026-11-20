/*******************************************************************************
The MIT License (MIT)

Copyright (c) 2026 Hajime Nakagami

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
	"errors"
	"net"
	"sync"
	"testing"
)

func TestNewConnectorRejectsMalformedDSN(t *testing.T) {
	if _, err := NewConnector("localhost/db.fdb"); !errors.Is(err, ErrDsnUserUnknown) {
		t.Fatalf("NewConnector() error = %v, want %v", err, ErrDsnUserUnknown)
	}
}

func TestNewConnectorDialError(t *testing.T) {
	refused := errors.New("refused by the test dialer")
	connector, err := NewConnector("user:pass@db.invalid:3050/db.fdb", WithDialer(func(context.Context, string, string) (net.Conn, error) {
		return nil, refused
	}))
	if err != nil {
		t.Fatalf("NewConnector() error = %v", err)
	}
	if _, err := connector.Connect(context.Background()); !errors.Is(err, refused) {
		t.Fatalf("Connect() error = %v, want %v", err, refused)
	}
}

func TestNewConnectorWithDialer(t *testing.T) {
	_, dsn, err := CreateTestDatabase("test_connector_dialer_")
	if err != nil {
		t.Skipf("cannot create a test database on %s: %v", testServerAddr(), err)
	}
	var mu sync.Mutex
	var addrs []string
	connector, err := NewConnector(dsn, WithDialer(func(ctx context.Context, network, addr string) (net.Conn, error) {
		mu.Lock()
		addrs = append(addrs, network+" "+addr)
		mu.Unlock()
		return (&net.Dialer{}).DialContext(ctx, network, addr)
	}))
	if err != nil {
		t.Fatalf("NewConnector() error = %v", err)
	}
	db := sql.OpenDB(connector)
	defer db.Close()
	var n int
	if err := db.QueryRow("SELECT 1 FROM rdb$database").Scan(&n); err != nil {
		t.Fatalf("query through the dialer: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if want := "tcp " + testServerAddr(); len(addrs) != 1 || addrs[0] != want {
		t.Fatalf("dialed %v, want [%s]", addrs, want)
	}
}
