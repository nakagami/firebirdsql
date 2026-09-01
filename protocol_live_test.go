/*******************************************************************************
The MIT License (MIT)

Copyright (c) 2026 Alexey Kovyazin

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
	"net"
	"os"
	"runtime"
	"testing"
	"time"
)

// GetEmployeeLiveDSN returns a DSN for the read-only EMPLOYEE sample database
// used by protocol 18/19 live tests. EMPLOYEE.FDB is looked up in the package
// directory (the repository root during go test) or via the
// FIREBIRD_EMPLOYEE_DB environment variable; the server address defaults to
// localhost:3055 and can be overridden with FIREBIRD_EMPLOYEE_ADDR. Tests
// skip when the database or server is absent.
func GetEmployeeLiveDSN() string {
	dbPath := os.Getenv("FIREBIRD_EMPLOYEE_DB")
	if dbPath == "" {
		dbPath = "EMPLOYEE.FDB"
	}
	if runtime.GOOS == "windows" {
		dbPath = "/" + dbPath
	}
	addr := os.Getenv("FIREBIRD_EMPLOYEE_ADDR")
	if addr == "" {
		addr = "localhost:3055"
	}
	return GetTestUser() + ":" + GetTestPassword() + "@" + addr + dbPath
}

func GetEmployeeLiveDSNWithOptions(opts string) string {
	dsn := GetEmployeeLiveDSN()
	if opts == "" {
		return dsn
	}
	if opts[0] != '?' {
		opts = "?" + opts
	}
	return dsn + opts
}

func employeeAddr() string {
	if addr := os.Getenv("FIREBIRD_EMPLOYEE_ADDR"); addr != "" {
		return addr
	}
	return "localhost:3055"
}

func openEmployeeLive(t *testing.T, opts string) *sql.DB {
	t.Helper()
	conn, err := net.DialTimeout("tcp", employeeAddr(), 2*time.Second)
	if err != nil {
		t.Skipf("EMPLOYEE live server unreachable on %s: %v", employeeAddr(), err)
	}
	_ = conn.Close()

	db, err := sql.Open("firebirdsql", GetEmployeeLiveDSNWithOptions(opts))
	if err != nil {
		t.Skipf("open EMPLOYEE DSN: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		t.Skipf("ping EMPLOYEE: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func employeeProtocolVersion(t *testing.T, db *sql.DB) int {
	t.Helper()
	sqlConn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("Conn: %v", err)
	}
	defer sqlConn.Close()
	var ver int
	err = sqlConn.Raw(func(dc any) error {
		p, ok := dc.(interface{ ProtocolVersion() int })
		if !ok {
			return driver.ErrSkip
		}
		ver = p.ProtocolVersion()
		return nil
	})
	if err != nil {
		t.Fatalf("ProtocolVersion: %v", err)
	}
	return ver
}

func TestLiveEmployeeProtocolSmoke(t *testing.T) {
	db := openEmployeeLive(t, "")
	ver := employeeProtocolVersion(t, db)
	t.Logf("negotiated protocol version %d", ver)
	if ver < PROTOCOL_VERSION13 {
		t.Skipf("protocol %d: server predates SQLSTATE/auth-framework support", ver)
	}
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM EMPLOYEE`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n <= 0 {
		t.Fatalf("expected EMPLOYEE rows, got %d", n)
	}
}

func TestLiveEmployeeInlineBlobs(t *testing.T) {
	db := openEmployeeLive(t, "max_inline_blob_size=65536")
	ver := employeeProtocolVersion(t, db)
	if ver < PROTOCOL_VERSION19 {
		t.Skipf("inline blobs require protocol 19+, got %d", ver)
	}

	rows, err := db.Query(`SELECT FIRST 5 PROJ_DESC FROM PROJECT WHERE PROJ_DESC IS NOT NULL`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var desc []byte
		if err := rows.Scan(&desc); err != nil {
			t.Fatal(err)
		}
		if len(desc) == 0 {
			t.Fatal("expected non-empty PROJ_DESC")
		}
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if count == 0 {
		t.Fatal("no PROJECT rows with PROJ_DESC")
	}

	// Disabled inline blobs should still read successfully.
	db2 := openEmployeeLive(t, "max_inline_blob_size=0")
	var desc []byte
	if err := db2.QueryRow(`SELECT FIRST 1 PROJ_DESC FROM PROJECT WHERE PROJ_DESC IS NOT NULL`).Scan(&desc); err != nil {
		t.Fatal(err)
	}
	if len(desc) == 0 {
		t.Fatal("expected blob with inline size disabled")
	}
}

func TestLiveEmployeeScrollable(t *testing.T) {
	db := openEmployeeLive(t, "")
	ver := employeeProtocolVersion(t, db)
	if ver < PROTOCOL_VERSION18 {
		t.Skipf("scrollable cursors require protocol 18+, got %d", ver)
	}

	sqlConn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer sqlConn.Close()

	err = sqlConn.Raw(func(dc any) error {
		q, ok := dc.(interface {
			QueryScrollable(context.Context, string, ...driver.Value) (*ScrollableStmt, error)
		})
		if !ok {
			t.Fatal("QueryScrollable not available on driver conn")
		}
		sc, err := q.QueryScrollable(context.Background(),
			`SELECT EMP_NO FROM EMPLOYEE ORDER BY EMP_NO`)
		if err != nil {
			return err
		}
		defer sc.Close()

		first, err := sc.FetchOne(ScrollFirst, 0)
		if err != nil {
			return err
		}
		next, err := sc.FetchOne(ScrollNext, 0)
		if err != nil {
			return err
		}
		last, err := sc.FetchOne(ScrollLast, 0)
		if err != nil {
			return err
		}
		prior, err := sc.FetchOne(ScrollPrior, 0)
		if err != nil {
			return err
		}
		t.Logf("first=%v next=%v last=%v prior=%v", first[0], next[0], last[0], prior[0])
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
