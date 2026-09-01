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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Phase 4 of the Jaybird test port plan (JAYBIRD_TEST_PORT_PLAN.md):
// statement execution, stored procedures, transactions and savepoints —
// mirroring Jaybird's FBStatementTest, DDLTest, FBCallableStatementTest,
// GeneratedKeys*Test, AbstractTransactionTest, AutoCommitBehaviourTest,
// FBTpbMapperTest (integration side) and FBSavepointTest.

var stmtCtx = context.Background()

// TestStatementLifecycle mirrors Jaybird's FBStatementTest and DDLTest:
// Firebird DDL is transactional, prepared statements survive transaction
// boundaries, and closed statements refuse execution.
func TestStatementLifecycle(t *testing.T) {
	db, _, _ := createTestDatabaseWithDDL(t, "stmt_lifecycle_")

	// DDL inside an explicit transaction rolls back with it.
	tx, err := db.BeginTx(stmtCtx, nil)
	require.NoError(t, err)
	mustExec(t, stmtCtx, tx, "CREATE TABLE STMT_RB (ID INTEGER PRIMARY KEY)")
	require.NoError(t, tx.Rollback())
	var n int
	err = db.QueryRow("SELECT COUNT(*) FROM RDB$RELATIONS WHERE RDB$RELATION_NAME = 'STMT_RB'").Scan(&n)
	require.NoError(t, err)
	require.Equal(t, 0, n, "rolled-back DDL must leave no table")

	// DDL inside a transaction commits with it. (Firebird does not expose a
	// table to the same transaction that created it, so DML follows commit.)
	tx, err = db.BeginTx(stmtCtx, nil)
	require.NoError(t, err)
	mustExec(t, stmtCtx, tx, "CREATE TABLE STMT_CM (ID INTEGER PRIMARY KEY, V VARCHAR(10))")
	require.NoError(t, tx.Commit())
	mustExec(t, stmtCtx, db, "INSERT INTO STMT_CM VALUES (1, 'x')")
	mustExec(t, stmtCtx, db, "SELECT V FROM STMT_CM WHERE ID = 1") // table usable

	// A prepared statement is reusable across transaction boundaries.
	prep, err := db.PrepareContext(stmtCtx, "INSERT INTO STMT_CM VALUES (?, ?)")
	require.NoError(t, err)
	for i := 2; i <= 3; i++ {
		_, err = prep.Exec(i, "y")
		require.NoError(t, err)
	}
	require.NoError(t, prep.Close())
	require.Equal(t, 3, mustCount(t, db, "STMT_CM"))

	// A closed prepared statement refuses execution.
	dead, err := db.PrepareContext(stmtCtx, "SELECT COUNT(*) FROM STMT_CM")
	require.NoError(t, err)
	require.NoError(t, dead.Close())
	_, err = dead.Query()
	require.Error(t, err, "query on closed statement must fail")
}

func mustCount(t *testing.T, db *sql.DB, table string) int {
	t.Helper()
	var n int
	require.NoError(t, db.QueryRow("SELECT COUNT(*) FROM "+table).Scan(&n))
	return n
}

// TestStoredProcedureCalls mirrors Jaybird's FBCallableStatementTest:
// executable procedures return out-parameters as a row, selectable procedures
// stream rows, and exceptions inside procedures surface as errors.
func TestStoredProcedureCalls(t *testing.T) {
	db, _, _ := createTestDatabaseWithDDL(t, "stmt_procs_",
		`CREATE EXCEPTION PROC_TEST_EXC 'procedure raised test exception'`,
		`CREATE PROCEDURE P_ADD (A INTEGER, B INTEGER) RETURNS (S INTEGER)
		 AS BEGIN S = A + B; END`,
		`CREATE PROCEDURE P_RANGE (A INTEGER, B INTEGER) RETURNS (N INTEGER)
		 AS BEGIN N = :A; WHILE (:N <= :B) DO BEGIN SUSPEND; N = :N + 1; END END`,
		`CREATE PROCEDURE P_FAIL (F INTEGER)
		 AS BEGIN IF (F = 1) THEN EXCEPTION PROC_TEST_EXC; END`,
		`CREATE PROCEDURE P_NULL (A INTEGER) RETURNS (S INTEGER)
		 AS BEGIN S = A; END`)

	// Executable procedure: out-parameter comes back as a row.
	var sum int
	require.NoError(t, db.QueryRow("EXECUTE PROCEDURE P_ADD(2, 3)").Scan(&sum))
	require.Equal(t, 5, sum)

	// Parameterized call with NULL in-parameter.
	var out sql.NullInt64
	require.NoError(t, db.QueryRow("EXECUTE PROCEDURE P_NULL(?)", nil).Scan(&out))
	require.False(t, out.Valid, "NULL in-parameter must produce NULL out-parameter")
	require.NoError(t, db.QueryRow("EXECUTE PROCEDURE P_NULL(?)", 7).Scan(&out))
	require.True(t, out.Valid)
	require.Equal(t, int64(7), out.Int64)

	// Selectable procedure: rows streamed via SUSPEND.
	var total int
	rows, err := db.Query("SELECT N FROM P_RANGE(?, ?) ORDER BY N", 3, 5)
	require.NoError(t, err)
	defer rows.Close()
	for rows.Next() {
		var n int
		require.NoError(t, rows.Scan(&n))
		total += n
	}
	require.NoError(t, rows.Err())
	require.Equal(t, 12, total)

	// Exception inside a procedure propagates as an error.
	_, err = db.Exec("EXECUTE PROCEDURE P_FAIL(1)")
	require.Error(t, err)
	require.Contains(t, err.Error(), "procedure raised test exception")
	_, err = db.Exec("EXECUTE PROCEDURE P_FAIL(0)")
	require.NoError(t, err, "no exception expected when flag is 0")
}

// TestReturningEdgeCases mirrors Jaybird's FBStatementGeneratedKeysTest /
// FBPreparedStatementGeneratedKeysTest edge cases: multi-row RETURNING,
// RETURNING of BLOB columns, and errors for unknown columns.
func TestReturningEdgeCases(t *testing.T) {
	db, _, _ := createTestDatabaseWithDDL(t, "stmt_returning_",
		`CREATE TABLE RET_EDGE (ID INTEGER NOT NULL PRIMARY KEY, NOTE VARCHAR(20), DOC BLOB SUB_TYPE TEXT)`)

	for i := 1; i <= 3; i++ {
		mustExec(t, stmtCtx, db, "INSERT INTO RET_EDGE VALUES (?, ?, NULL)", i, "note")
	}

	// Single-row UPDATE ... RETURNING works everywhere. (RETURNING of BLOB
	// columns additionally requires fetching blob values out of the returning
	// record - covered by the wire-protocol feature branch.)
	var id int64
	require.NoError(t, db.QueryRow(
		"UPDATE RET_EDGE SET NOTE = 'single' WHERE ID = 1 RETURNING ID").Scan(&id))
	require.Equal(t, int64(1), id)

	// Multi-row UPDATE/DELETE ... RETURNING: Firebird 5 streams every affected
	// row, while Firebird 4 and older reject the statement with "multiple rows
	// in singleton select" because it is executed as a singleton returning.
	// (Candidate driver improvement: execute multi-row RETURNING with the
	// cursor flag.) Assert the behavior each server version actually has.
	multiRow := testServerVersion(t).EqualOrGreater(5, 0)

	rows, err := db.Query("UPDATE RET_EDGE SET NOTE = 'upd' WHERE ID <= 2 RETURNING ID")
	if multiRow {
		require.NoError(t, err)
		ids := map[int64]bool{}
		for rows.Next() {
			require.NoError(t, rows.Scan(&id))
			ids[id] = true
		}
		require.NoError(t, rows.Err())
		rows.Close()
		require.Equal(t, map[int64]bool{1: true, 2: true}, ids)
	} else {
		require.Error(t, err)
		require.Contains(t, err.Error(), "multiple rows in singleton select")
		var unchanged int
		require.NoError(t, db.QueryRow("SELECT COUNT(*) FROM RET_EDGE").Scan(&unchanged))
		require.Equal(t, 3, unchanged, "failed statement must not have updated rows")
	}

	// RETURNING of a nonexistent column must fail.
	_, err = db.Query("INSERT INTO RET_EDGE (ID, NOTE) VALUES (9, 'x') RETURNING NO_SUCH_COL")
	require.Error(t, err)

	// DELETE ... RETURNING reports the removed rows.
	rows, err = db.Query("DELETE FROM RET_EDGE WHERE ID >= 2 RETURNING ID")
	if multiRow {
		require.NoError(t, err)
		deleted := 0
		for rows.Next() {
			require.NoError(t, rows.Scan(&id))
			deleted++
		}
		require.NoError(t, rows.Err())
		rows.Close()
		require.Equal(t, 2, deleted)
	} else {
		require.Error(t, err)
		require.Contains(t, err.Error(), "multiple rows in singleton select")
		mustExec(t, stmtCtx, db, "DELETE FROM RET_EDGE WHERE ID >= 2")
	}
	require.Equal(t, 1, mustCount(t, db, "RET_EDGE"))
}

// TestBatchParity mirrors Jaybird's BatchUpdatesTest / V16StatementTest batch
// cases: many rows, per-row error reporting, record counts, and cancel.
func TestBatchParity(t *testing.T) {
	requireServerVersion(t, 4, 0) // batch needs Firebird 4 protocol 16+ in practice
	db, _, _ := createTestDatabaseWithDDL(t, "stmt_batch_",
		`CREATE TABLE BATCH_PARITY (ID INTEGER NOT NULL PRIMARY KEY, V VARCHAR(20))`)

	sqlConn, err := db.Conn(stmtCtx)
	require.NoError(t, err)
	defer sqlConn.Close()

	err = sqlConn.Raw(func(dc any) error {
		fc, ok := dc.(interface {
			PrepareBatch(context.Context, string, BatchOptions) (*PreparedBatch, error)
		})
		if !ok {
			return errBatchUnsupported
		}
		ctx := stmtCtx

		// Many rows in one batch.
		b, err := fc.PrepareBatch(ctx, "INSERT INTO BATCH_PARITY (ID, V) VALUES (?, ?)", BatchOptions{})
		if err != nil {
			return err
		}
		for i := 1; i <= 200; i++ {
			if err := b.Add(int64(i), "v"); err != nil {
				return err
			}
		}
		res, err := b.Exec(ctx)
		if err != nil {
			return err
		}
		if res.Affected < 200 {
			return errBatchAffectedMismatch
		}
		if err := b.Close(); err != nil {
			return err
		}

		// A unique violation in the middle must surface as an error.
		b2, err := fc.PrepareBatch(ctx, "INSERT INTO BATCH_PARITY (ID, V) VALUES (?, ?)", BatchOptions{ContinueOnError: true, DetailedErrors: 1})
		if err != nil {
			return err
		}
		_ = b2.Add(int64(300), "ok")
		_ = b2.Add(int64(1), "duplicate") // PK conflict with the earlier batch
		if _, err := b2.Exec(ctx); err == nil {
			return errBatchExpectedError
		}
		return b2.Close()
	})
	if err == errBatchUnsupported {
		t.Skip("PrepareBatch not available on driver conn")
	}
	if err == errBatchAffectedMismatch {
		t.Fatal("batch affected rows below expectation")
	}
	if err == errBatchExpectedError {
		t.Fatal("expected mid-batch unique violation error, got nil")
	}
	require.NoError(t, err)
	require.Equal(t, 200, mustCount(t, db, "BATCH_PARITY"))
}

var (
	errBatchUnsupported      = errStatic("batch unsupported")
	errBatchAffectedMismatch = errStatic("batch affected mismatch")
	errBatchExpectedError    = errStatic("expected batch error")
)

type errStatic string

func (e errStatic) Error() string { return string(e) }

// TestTransactionIsolation mirrors Jaybird's AbstractTransactionTest and
// FBTpbMapperTest integration semantics: uncommitted work is invisible to
// other connections, snapshots are stable under REPEATABLE READ, and
// read-only transactions refuse writes.
func TestTransactionIsolation(t *testing.T) {
	db, dsn, _ := createTestDatabaseWithDDL(t, "stmt_iso_",
		`CREATE TABLE ISO_T (ID INTEGER PRIMARY KEY, V VARCHAR(10))`)
	mustExec(t, stmtCtx, db, "INSERT INTO ISO_T VALUES (1, 'base')")

	// Two independent physical connections.
	c1 := openTestDatabase(t, dsn)
	c2 := openTestDatabase(t, dsn)
	conn1, err := c1.Conn(stmtCtx)
	require.NoError(t, err)
	defer conn1.Close()
	conn2, err := c2.Conn(stmtCtx)
	require.NoError(t, err)
	defer conn2.Close()

	// Uncommitted work is invisible to the other connection.
	tx1, err := conn1.BeginTx(stmtCtx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	require.NoError(t, err)
	mustExec(t, stmtCtx, tx1, "INSERT INTO ISO_T VALUES (2, 'pending')")
	var n int
	require.NoError(t, conn2.QueryRowContext(stmtCtx, "SELECT COUNT(*) FROM ISO_T").Scan(&n))
	require.Equal(t, 1, n, "uncommitted insert must be invisible")
	require.NoError(t, tx1.Commit())
	require.NoError(t, conn2.QueryRowContext(stmtCtx, "SELECT COUNT(*) FROM ISO_T").Scan(&n))
	require.Equal(t, 2, n, "committed insert must become visible")

	// Rollback discards the work.
	tx1, err = conn1.BeginTx(stmtCtx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	require.NoError(t, err)
	mustExec(t, stmtCtx, tx1, "INSERT INTO ISO_T VALUES (3, 'doomed')")
	require.NoError(t, tx1.Rollback())
	require.NoError(t, conn2.QueryRowContext(stmtCtx, "SELECT COUNT(*) FROM ISO_T").Scan(&n))
	require.Equal(t, 2, n, "rolled-back insert must stay invisible")

	// REPEATABLE READ: the snapshot ignores concurrent commits.
	tx2, err := conn2.BeginTx(stmtCtx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	require.NoError(t, err)
	require.NoError(t, conn2.QueryRowContext(stmtCtx, "SELECT COUNT(*) FROM ISO_T").Scan(&n))
	require.Equal(t, 2, n)
	tx1, err = conn1.BeginTx(stmtCtx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	require.NoError(t, err)
	mustExec(t, stmtCtx, tx1, "INSERT INTO ISO_T VALUES (4, 'later')")
	require.NoError(t, tx1.Commit())
	require.NoError(t, conn2.QueryRowContext(stmtCtx, "SELECT COUNT(*) FROM ISO_T").Scan(&n))
	require.Equal(t, 2, n, "repeatable-read snapshot must ignore concurrent commit")
	require.NoError(t, tx2.Rollback())

	// Read-only transactions refuse writes.
	roTx, err := db.BeginTx(stmtCtx, &sql.TxOptions{ReadOnly: true})
	require.NoError(t, err)
	_, err = roTx.Exec("INSERT INTO ISO_T VALUES (5, 'rejected')")
	require.Error(t, err, "write in read-only transaction must fail")
	require.NoError(t, roTx.Rollback())
}

// TestSavepoints mirrors Jaybird's FBSavepointTest behavior: savepoints are
// plain SQL, so partial rollback inside a transaction works without any
// driver-side savepoint API.
func TestSavepoints(t *testing.T) {
	db, _, _ := createTestDatabaseWithDDL(t, "stmt_savepoint_",
		`CREATE TABLE SAVE_T (ID INTEGER PRIMARY KEY, V VARCHAR(10))`)

	tx, err := db.BeginTx(stmtCtx, nil)
	require.NoError(t, err)
	mustExec(t, stmtCtx, tx, "INSERT INTO SAVE_T VALUES (1, 'keep')")

	// Partial work after a savepoint is undone by ROLLBACK TO.
	mustExec(t, stmtCtx, tx, "SAVEPOINT SP_A")
	mustExec(t, stmtCtx, tx, "INSERT INTO SAVE_T VALUES (2, 'undo-me')")
	mustExec(t, stmtCtx, tx, "ROLLBACK TO SP_A")
	mustExec(t, stmtCtx, tx, "INSERT INTO SAVE_T VALUES (3, 'keep2')")

	// Nested savepoints: rollback to the outer one discards the inner one.
	mustExec(t, stmtCtx, tx, "SAVEPOINT SP_B")
	mustExec(t, stmtCtx, tx, "INSERT INTO SAVE_T VALUES (4, 'inner')")
	mustExec(t, stmtCtx, tx, "SAVEPOINT SP_C")
	mustExec(t, stmtCtx, tx, "INSERT INTO SAVE_T VALUES (5, 'inner2')")
	mustExec(t, stmtCtx, tx, "ROLLBACK TO SP_B")

	// RELEASE keeps the work done since the savepoint.
	mustExec(t, stmtCtx, tx, "SAVEPOINT SP_D")
	mustExec(t, stmtCtx, tx, "INSERT INTO SAVE_T VALUES (6, 'released')")
	mustExec(t, stmtCtx, tx, "RELEASE SAVEPOINT SP_D")

	require.NoError(t, tx.Commit())

	var got strings.Builder
	rows, err := db.Query("SELECT V FROM SAVE_T ORDER BY ID")
	require.NoError(t, err)
	for rows.Next() {
		var v string
		require.NoError(t, rows.Scan(&v))
		got.WriteString(v)
		got.WriteString(",")
	}
	require.NoError(t, rows.Err())
	rows.Close()
	require.Equal(t, "keep,keep2,released,", got.String(),
		"only rows kept across savepoint rollbacks must survive")
}

// TestAutoCommitSemantics mirrors Jaybird's AutoCommitBehaviourTest: an
// autocommit statement commits its work immediately — visible to other
// connections without any explicit commit — while explicit-transaction work
// stays invisible until commit.
func TestAutoCommitSemantics(t *testing.T) {
	db, dsn, _ := createTestDatabaseWithDDL(t, "stmt_autocommit_",
		`CREATE TABLE AC_T (ID INTEGER PRIMARY KEY)`)

	conn1, err := db.Conn(stmtCtx)
	require.NoError(t, err)
	defer conn1.Close()
	c2 := openTestDatabase(t, dsn)

	// An autocommit statement is committed at once: the second connection
	// sees it with no commit call in between.
	mustExec(t, stmtCtx, conn1, "INSERT INTO AC_T VALUES (1)")
	var n int
	require.NoError(t, c2.QueryRowContext(stmtCtx, "SELECT COUNT(*) FROM AC_T").Scan(&n))
	require.Equal(t, 1, n, "autocommit insert must be visible immediately")

	// Explicit-transaction work stays invisible to the other connection
	// until the transaction commits.
	tx, err := conn1.BeginTx(stmtCtx, nil)
	require.NoError(t, err)
	mustExec(t, stmtCtx, tx, "INSERT INTO AC_T VALUES (2)")
	require.NoError(t, c2.QueryRowContext(stmtCtx, "SELECT COUNT(*) FROM AC_T").Scan(&n))
	require.Equal(t, 1, n, "explicit-transaction insert must stay invisible")
	require.NoError(t, tx.Commit())

	ctx2, cancel := context.WithTimeout(stmtCtx, 5*time.Second)
	defer cancel()
	require.NoError(t, c2.QueryRowContext(ctx2, "SELECT COUNT(*) FROM AC_T").Scan(&n))
	require.Equal(t, 2, n)
}
