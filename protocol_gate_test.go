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
	"database/sql/driver"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Phase 8 of the Jaybird test port plan (JAYBIRD_TEST_PORT_PLAN.md):
// protocol-version feature gating. Jaybird re-runs its suites per wire
// protocol version (V10DatabaseTest → … → V19StatementTest); this driver has
// a single code path, so the parity equivalent is to negotiate one protocol
// and gate each wire feature on the minimum protocol version that carries it
// (V12 cancel, V16 batch, V18 scrollable cursors, V19 inline blobs).

// TestNegotiatedProtocolVersion mirrors Jaybird's WireDatabaseConnectionTest
// identify() checks: the negotiated protocol must be inside the advertised
// 10–19 range and consistent with the server generation.
func TestNegotiatedProtocolVersion(t *testing.T) {
	db, _, _ := createTestDatabaseWithDDL(t, "gate_negotiate_")
	ver := negotiatedProtocol(t, db)
	t.Logf("negotiated protocol %d against %s", ver, testServerAddr())
	require.GreaterOrEqual(t, ver, 10)
	require.LessOrEqual(t, ver, 19)

	// A protocol-19 server must be Firebird 5+; older servers cannot speak it.
	v := testServerVersion(t)
	if ver >= PROTOCOL_VERSION19 {
		require.True(t, v.EqualOrGreater(5, 0),
			"protocol %d negotiated from %q, needs Firebird 5+", ver, v.Raw)
	}
}

// TestFeatureProtocolGating exercises each wire feature behind the minimum
// protocol version that introduced it (Jaybird's RequireProtocolExtension
// pattern): a feature test must skip — never fail — on older servers.
func TestFeatureProtocolGating(t *testing.T) {
	db, _, _ := createTestDatabaseWithDDL(t, "gate_features_",
		`CREATE TABLE GATE_T (ID INTEGER PRIMARY KEY, V VARCHAR(20))`)

	t.Run("batch (protocol 16+)", func(t *testing.T) {
		requireProtocol(t, db, PROTOCOL_VERSION16)
		sqlConn, err := db.Conn(stmtCtx)
		require.NoError(t, err)
		defer sqlConn.Close()
		err = sqlConn.Raw(func(dc any) error {
			b, err := dc.(*firebirdsqlConn).PrepareBatch(stmtCtx, "INSERT INTO GATE_T (ID, V) VALUES (?, ?)", BatchOptions{})
			if err != nil {
				return err
			}
			defer b.Close()
			if err := b.Add(int64(100), "batched"); err != nil {
				return err
			}
			res, err := b.Exec(stmtCtx)
			if err != nil {
				return err
			}
			if res.Affected < 1 {
				return errBatchAffectedMismatch
			}
			return nil
		})
		require.NoError(t, err)
	})

	t.Run("scrollable cursors (protocol 18+)", func(t *testing.T) {
		requireProtocol(t, db, PROTOCOL_VERSION18)
		mustExec(t, stmtCtx, db, `CREATE TABLE GATE_SCROLL (ID INTEGER PRIMARY KEY)`)
		for id := 1; id <= 5; id++ {
			mustExec(t, stmtCtx, db, "INSERT INTO GATE_SCROLL VALUES (?)", id)
		}
		sqlConn, err := db.Conn(stmtCtx)
		require.NoError(t, err)
		defer sqlConn.Close()

		// Each step depends on the cursor position left by the previous one,
		// and every orientation is used once, so a wrong orientation or
		// offset on the wire lands on a different row.
		steps := []struct {
			name        string
			orientation int32
			offset      int32
			want        int64
		}{
			{"first", ScrollFirst, 0, 1},
			{"absolute 3", ScrollAbsolute, 3, 3},
			{"relative -1", ScrollRelative, -1, 2},
			{"last", ScrollLast, 0, 5},
			{"prior", ScrollPrior, 0, 4},
			{"next", ScrollNext, 0, 5},
		}
		err = sqlConn.Raw(func(dc any) error {
			sc, err := dc.(*firebirdsqlConn).QueryScrollable(stmtCtx, "SELECT ID FROM GATE_SCROLL ORDER BY ID")
			if err != nil {
				return err
			}
			defer sc.Close()
			for i, st := range steps {
				row, err := sc.FetchOne(st.orientation, st.offset)
				require.NoError(t, err, "step %d (%s)", i+1, st.name)
				require.Equal(t, []driver.Value{st.want}, row, "step %d (%s)", i+1, st.name)
			}
			_, err = sc.FetchOne(ScrollNext, 0)
			require.ErrorIs(t, err, io.EOF, "next past the end")
			return sc.Close()
		})
		require.NoError(t, err)
	})
}

// TestCancelOperationParity consolidates Jaybird's V12 cancelOperation and
// operation-monitor coverage: a context cancellation aborts a long-running
// statement and the connection pool stays usable afterwards.
func TestCancelOperationParity(t *testing.T) {
	db, _, _ := createTestDatabaseWithDDL(t, "gate_cancel_",
		`CREATE TABLE GATE_CANCEL (ID INTEGER PRIMARY KEY)`)

	ctx, cancel := context.WithTimeout(stmtCtx, 500*time.Millisecond)
	defer cancel()
	_, err := db.ExecContext(ctx, longQueryNonSelectable)
	require.Error(t, err, "long-running statement must be cancelled by the context")

	// The pool remains usable after the cancellation.
	mustExec(t, stmtCtx, db, "INSERT INTO GATE_CANCEL VALUES (1)")
	var n int
	require.NoError(t, db.QueryRow("SELECT COUNT(*) FROM GATE_CANCEL").Scan(&n))
	require.Equal(t, 1, n)
}
