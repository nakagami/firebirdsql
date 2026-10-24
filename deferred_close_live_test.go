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
	"bufio"
	"context"
	"database/sql"
	"io"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// Deferred cursor close.
//
// Closing a cursor (op_free_statement with DSQL_close) needs no answer before
// the next request can go out, so on a lazy-send port type the driver queues
// the packet and writes it in front of the next request, as fbclient and
// Jaybird do. The server's response to it is skipped when the next response
// is read. These tests count the packets a query costs and check that
// everything that can follow a close (another query, a drop, a commit, a
// blob, a batch, a connection close) still works.

// countingWriter counts the writes to the network: the driver buffers each
// request and flushes it in one write, so a write is a packet.
type countingWriter struct {
	io.Writer
	n atomic.Int64
}

func (w *countingWriter) Write(b []byte) (int, error) {
	w.n.Add(1)
	return w.Writer.Write(b)
}

// countWrites puts a countingWriter under the connection's write buffer.
func countWrites(t *testing.T, conn *sql.Conn) *countingWriter {
	t.Helper()
	cw := &countingWriter{}
	require.NoError(t, conn.Raw(func(dc any) error {
		fc := dc.(*firebirdsqlConn)
		if (fc.wp.acceptType & ptype_MASK) != ptype_lazy_send {
			t.Skip("the server did not accept the lazy-send port type")
		}
		cw.Writer = fc.wp.conn.conn
		fc.wp.conn.writer = bufio.NewWriter(cw)
		return nil
	}))
	return cw
}

// TestDeferredCursorClosePacketCount counts the packets a query on a
// prepared statement costs. In a transaction it is two (execute, fetch): the
// cursor close is written with the next request. In autocommit mode it is
// three (execute, fetch, commit retaining), the close riding with the
// commit. Without deferral each is one more.
func TestDeferredCursorClosePacketCount(t *testing.T) {
	_, dsn, _ := createTestDatabaseWithDDL(t, "defclose_count_",
		`CREATE TABLE T (ID INTEGER NOT NULL PRIMARY KEY, V VARCHAR(20))`,
		`INSERT INTO T VALUES (1, 'one')`)
	db := openTestDatabase(t, dsn)
	db.SetMaxOpenConns(1)
	ctx := context.Background()

	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()
	writes := countWrites(t, conn)

	// perQuery runs n queries on st and returns the packets each one cost
	perQuery := func(st interface {
		QueryRowContext(context.Context, ...any) *sql.Row
	}) float64 {
		const n = 50
		var v string
		require.NoError(t, st.QueryRowContext(ctx, 1).Scan(&v)) // warm up
		before := writes.n.Load()
		for i := 0; i < n; i++ {
			require.NoError(t, st.QueryRowContext(ctx, 1).Scan(&v))
			require.Equal(t, "one", v)
		}
		return float64(writes.n.Load()-before) / n
	}

	tx, err := conn.BeginTx(ctx, nil)
	require.NoError(t, err)
	st, err := tx.PrepareContext(ctx, "SELECT V FROM T WHERE ID = ?")
	require.NoError(t, err)
	inTx := perQuery(st)
	t.Logf("in a transaction: %.2f packets per query", inTx)
	require.NoError(t, st.Close())
	require.NoError(t, tx.Commit())
	require.Equal(t, 2.0, inTx, "execute and fetch; the cursor close should not be a packet of its own")

	st, err = conn.PrepareContext(ctx, "SELECT V FROM T WHERE ID = ?")
	require.NoError(t, err)
	auto := perQuery(st)
	t.Logf("autocommit: %.2f packets per query", auto)
	require.NoError(t, st.Close())
	require.Equal(t, 3.0, auto, "execute, fetch and commit retaining; the cursor close should ride with the commit")
}

// TestDeferredCursorCloseFollowedBy runs everything that can follow a cursor
// close on the same connection, so a skipped response is never mistaken for
// the response of the next request.
func TestDeferredCursorCloseFollowedBy(t *testing.T) {
	_, dsn, _ := createTestDatabaseWithDDL(t, "defclose_next_",
		`CREATE TABLE T (ID INTEGER NOT NULL PRIMARY KEY, V VARCHAR(20), B BLOB SUB_TYPE TEXT)`,
		`INSERT INTO T VALUES (1, 'one', 'blob one')`,
		`INSERT INTO T VALUES (2, 'two', NULL)`)
	db := openTestDatabase(t, dsn)
	db.SetMaxOpenConns(1)
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()

	query := func(q string, args ...any) []string {
		rows, err := conn.QueryContext(ctx, q, args...)
		require.NoError(t, err)
		var out []string
		for rows.Next() {
			var s sql.NullString
			require.NoError(t, rows.Scan(&s))
			out = append(out, s.String)
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close()) // queues the close
		return out
	}

	// close followed by another query, repeatedly
	for i := 0; i < 20; i++ {
		require.Equal(t, []string{"one", "two"}, query("SELECT V FROM T ORDER BY ID"))
	}
	// close followed by a query that reads a blob (blob ops in the middle)
	require.Equal(t, []string{"blob one", ""}, query("SELECT B FROM T ORDER BY ID"))
	// close followed by a statement error: the error belongs to the new statement
	_, err = conn.QueryContext(ctx, "SELECT V FROM NO_SUCH_TABLE")
	require.Error(t, err)
	require.Contains(t, err.Error(), "NO_SUCH_TABLE")
	require.Equal(t, []string{"one", "two"}, query("SELECT V FROM T ORDER BY ID"))
	// close followed by an insert and a commit in a transaction
	tx, err := conn.BeginTx(ctx, nil)
	require.NoError(t, err)
	rows, err := tx.QueryContext(ctx, "SELECT V FROM T WHERE ID = 1")
	require.NoError(t, err)
	require.True(t, rows.Next())
	require.NoError(t, rows.Close())
	_, err = tx.ExecContext(ctx, "INSERT INTO T (ID, V) VALUES (3, 'three')")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	require.Equal(t, []string{"one", "two", "three"}, query("SELECT V FROM T ORDER BY ID"))
	// close followed by a rollback
	tx, err = conn.BeginTx(ctx, nil)
	require.NoError(t, err)
	rows, err = tx.QueryContext(ctx, "SELECT V FROM T WHERE ID = 1")
	require.NoError(t, err)
	require.NoError(t, rows.Close())
	require.NoError(t, tx.Rollback())
	// close followed by a drop of the same statement, and of another
	st, err := conn.PrepareContext(ctx, "SELECT V FROM T WHERE ID = ?")
	require.NoError(t, err)
	var v string
	require.NoError(t, st.QueryRowContext(ctx, 2).Scan(&v))
	require.Equal(t, "two", v)
	require.NoError(t, st.Close())
	require.Equal(t, []string{"three"}, query("SELECT V FROM T WHERE ID = 3"))
	// close followed by a prepare
	st2, err := conn.PrepareContext(ctx, "SELECT COUNT(*) FROM T")
	require.NoError(t, err)
	var cnt int
	require.NoError(t, st2.QueryRowContext(ctx).Scan(&cnt))
	require.Equal(t, 3, cnt)
	require.NoError(t, st2.Close())
	// close followed by a ping
	require.NoError(t, conn.PingContext(ctx))
	require.Equal(t, []string{"one"}, query("SELECT V FROM T WHERE ID = 1"))
	// close followed by the connection being closed: nothing else is sent
	require.NoError(t, conn.Close())
	db2 := openTestDatabase(t, dsn)
	require.Equal(t, 3, func() int {
		var n int
		require.NoError(t, db2.QueryRow("SELECT COUNT(*) FROM T").Scan(&n))
		return n
	}())
}

// TestDeferredCursorCloseConcurrent runs queries from many goroutines over a
// pool, so deferred closes interleave with every kind of request.
func TestDeferredCursorCloseConcurrent(t *testing.T) {
	_, dsn, _ := createTestDatabaseWithDDL(t, "defclose_conc_",
		`CREATE TABLE T (ID INTEGER NOT NULL PRIMARY KEY, V VARCHAR(20))`,
		`EXECUTE BLOCK AS DECLARE I INTEGER = 1; BEGIN WHILE (I <= 100) DO BEGIN INSERT INTO T VALUES (:I, 'v'); I = I + 1; END END`)
	db := openTestDatabase(t, dsn)
	db.SetMaxOpenConns(4)
	ctx := context.Background()
	var wg sync.WaitGroup
	errs := make(chan error, 64)
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				rows, err := db.QueryContext(ctx, "SELECT ID FROM T WHERE ID > ? ORDER BY ID ROWS 5", i%90)
				if err != nil {
					errs <- err
					return
				}
				n := 0
				for rows.Next() {
					var id int
					if err := rows.Scan(&id); err != nil {
						errs <- err
						return
					}
					n++
				}
				rows.Close()
				if n != 5 {
					errs <- io.ErrUnexpectedEOF
					return
				}
				if i%10 == 0 {
					if _, err := db.ExecContext(ctx, "UPDATE T SET V = ? WHERE ID = ?", "w", g+1); err != nil {
						errs <- err
						return
					}
				}
			}
		}(g)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
}
