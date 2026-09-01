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
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func openWritableTestDB(t *testing.T, prefix string) *sql.DB {
	t.Helper()
	port := 3050
	if c, err := net.DialTimeout("tcp", "127.0.0.1:3050", 2*time.Second); err == nil {
		_ = c.Close()
	} else if c, err := net.DialTimeout("tcp", "127.0.0.1:3055", 2*time.Second); err == nil {
		_ = c.Close()
		port = 3055
	} else {
		t.Skipf("Firebird writable server unreachable on 127.0.0.1:3050/3055")
	}

	dbPath := GetTestDatabase(prefix)
	if runtime.GOOS == "windows" {
		dbPath = "/" + filepath.ToSlash(dbPath)
	}
	dsn := fmt.Sprintf("%s:%s@127.0.0.1:%d%s", GetTestUser(), GetTestPassword(), port, dbPath)
	db, err := sql.Open("firebirdsql_createdb", dsn)
	if err != nil {
		t.Skipf("open createdb: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		t.Skipf("ping writable db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	db.SetMaxOpenConns(1)
	return db
}

func writableProtocolVersion(t *testing.T, db *sql.DB) int {
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

func TestExecImmediateLive(t *testing.T) {
	db := openWritableTestDB(t, "test_exec_imm_")
	sqlConn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer sqlConn.Close()

	err = sqlConn.Raw(func(dc any) error {
		fc, ok := dc.(interface {
			ExecImmediate(context.Context, string) error
		})
		if !ok {
			return fmt.Errorf("ExecImmediate not available")
		}
		ctx := context.Background()
		if err := fc.ExecImmediate(ctx, "recreate table EXEC_IMM_SMOKE (ID int not null primary key)"); err != nil {
			return err
		}
		return fc.ExecImmediate(ctx, "drop table EXEC_IMM_SMOKE")
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestPreparedBatchLiveInsert(t *testing.T) {
	db := openWritableTestDB(t, "test_batch_")
	ver := writableProtocolVersion(t, db)
	if ver < 16 {
		t.Skipf("batch requires protocol >= 16, got %d", ver)
	}

	_, err := db.Exec(`recreate table BATCH_SMOKE (ID int not null primary key, NOTE varchar(64))`)
	if err != nil {
		t.Fatal(err)
	}

	sqlConn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer sqlConn.Close()

	ctx := context.Background()
	err = sqlConn.Raw(func(dc any) error {
		fc, ok := dc.(interface {
			PrepareBatch(context.Context, string, BatchOptions) (*PreparedBatch, error)
		})
		if !ok {
			return fmt.Errorf("PrepareBatch not available")
		}
		b, err := fc.PrepareBatch(ctx, "insert into BATCH_SMOKE (ID, NOTE) values (?, ?)", BatchOptions{})
		if err != nil {
			return err
		}
		defer b.Close()
		for i := 1; i <= 10; i++ {
			if err := b.Add(int64(i), fmt.Sprintf("n-%d", i)); err != nil {
				return err
			}
		}
		res, err := b.Exec(ctx)
		if err != nil {
			return err
		}
		if res.Affected < 10 {
			return fmt.Errorf("affected %d", res.Affected)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	var n int
	if err := sqlConn.QueryRowContext(ctx, "select count(*) from BATCH_SMOKE").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 10 {
		t.Fatalf("count %d", n)
	}
}

func TestPreparedBatchRejectsBlobColumn(t *testing.T) {
	db := openWritableTestDB(t, "test_batch_blob_")
	ver := writableProtocolVersion(t, db)
	if ver < 16 {
		t.Skipf("batch requires protocol >= 16, got %d", ver)
	}
	_, err := db.Exec(`recreate table BATCH_BLOB (ID int not null primary key, DATA blob)`)
	if err != nil {
		t.Fatal(err)
	}
	sqlConn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer sqlConn.Close()

	err = sqlConn.Raw(func(dc any) error {
		fc := dc.(interface {
			PrepareBatch(context.Context, string, BatchOptions) (*PreparedBatch, error)
		})
		_, err := fc.PrepareBatch(context.Background(), "insert into BATCH_BLOB (ID, DATA) values (?, ?)", BatchOptions{})
		if err == nil {
			return fmt.Errorf("expected blob rejection")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestCompareBatchVsExecInsert100k compares batch vs per-row Exec for 100k inserts.
// Opt-in: FIREBIRDSQL_BENCH=1 go test -run TestCompareBatchVsExecInsert100k -timeout 15m
func TestCompareBatchVsExecInsert100k(t *testing.T) {
	if os.Getenv("FIREBIRDSQL_BENCH") != "1" {
		t.Skip("set FIREBIRDSQL_BENCH=1 to run 100k batch vs exec compare")
	}
	db := openWritableTestDB(t, "test_batch_bench_")
	ver := writableProtocolVersion(t, db)
	if ver < 16 {
		t.Skipf("batch requires protocol >= 16, got %d", ver)
	}

	const n = 100000
	ctx := context.Background()

	setup := func() {
		t.Helper()
		if _, err := db.Exec(`recreate table BATCH_BENCH (ID int not null primary key, NOTE varchar(64))`); err != nil {
			t.Fatal(err)
		}
	}

	countRows := func(conn *sql.Conn) int {
		t.Helper()
		var c int
		var err error
		if conn != nil {
			err = conn.QueryRowContext(ctx, "select count(*) from BATCH_BENCH").Scan(&c)
		} else {
			err = db.QueryRow("select count(*) from BATCH_BENCH").Scan(&c)
		}
		if err != nil {
			t.Fatal(err)
		}
		return c
	}

	setup()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	stmt, err := tx.Prepare("insert into BATCH_BENCH (ID, NOTE) values (?, ?)")
	if err != nil {
		t.Fatal(err)
	}
	t0 := time.Now()
	for i := 1; i <= n; i++ {
		if _, err := stmt.Exec(i, fmt.Sprintf("n-%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	_ = stmt.Close()
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	execDur := time.Since(t0)
	if countRows(nil) != n {
		t.Fatalf("exec path count")
	}

	setup()
	sqlConn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}

	tx2, err := sqlConn.BeginTx(ctx, nil)
	if err != nil {
		sqlConn.Close()
		t.Fatal(err)
	}

	t1 := time.Now()
	err = sqlConn.Raw(func(dc any) error {
		fc := dc.(interface {
			PrepareBatch(context.Context, string, BatchOptions) (*PreparedBatch, error)
		})
		// 100k * ~74-byte classic messages ≈ 7 MiB; request a larger server buffer
		// so one Exec can hold the full set (Flush only bounds client RAM).
		b, err := fc.PrepareBatch(ctx, "insert into BATCH_BENCH (ID, NOTE) values (?, ?)", BatchOptions{
			BufferBytes: 32 * 1024 * 1024,
		})
		if err != nil {
			return err
		}
		defer b.Close()
		for i := 1; i <= n; i++ {
			if err := b.Add(int64(i), fmt.Sprintf("n-%d", i)); err != nil {
				return err
			}
			if b.PendingBytes() >= 128*1024 {
				if err := b.Flush(ctx); err != nil {
					return err
				}
			}
		}
		_, err = b.Exec(ctx)
		return err
	})
	if err != nil {
		_ = tx2.Rollback()
		_ = sqlConn.Close()
		t.Fatal(err)
	}
	if err := tx2.Commit(); err != nil {
		_ = sqlConn.Close()
		t.Fatal(err)
	}
	batchDur := time.Since(t1)
	if countRows(sqlConn) != n {
		_ = sqlConn.Close()
		t.Fatalf("batch path count")
	}
	_ = sqlConn.Close()

	speedup := float64(execDur) / float64(batchDur)
	t.Logf("batch=%s exec=%s speedup=%.2fx batch_rps=%.0f exec_rps=%.0f",
		batchDur, execDur, speedup,
		float64(n)/batchDur.Seconds(),
		float64(n)/execDur.Seconds())
}
