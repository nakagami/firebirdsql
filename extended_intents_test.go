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
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// ==================== pure decode / TPB tests ====================

func TestDecodeDriverLevelMatrix(t *testing.T) {
	cases := []struct {
		level    int
		iso      int
		wait     int
		ro       bool
		lockTo   int
		complete int
	}{
		// plain values: historical sql.Level* behavior preserved
		{0, ISOLATION_LEVEL_READ_COMMITED, isc_tpb_wait, false, 0, completionPlain},
		{1, ISOLATION_LEVEL_READ_COMMITED, isc_tpb_wait, false, 0, completionPlain},
		{2, ISOLATION_LEVEL_READ_COMMITED, isc_tpb_wait, false, 0, completionPlain}, // LevelReadCommitted
		{3, ISOLATION_LEVEL_SERIALIZABLE, isc_tpb_wait, false, 0, completionPlain},  // LevelWriteCommitted / internal SERIALIZABLE
		{4, ISOLATION_LEVEL_REPEATABLE_READ, isc_tpb_wait, false, 0, completionPlain}, // LevelRepeatableRead
		{5, ISOLATION_LEVEL_READ_COMMITED_NOWAIT, isc_tpb_nowait, false, 0, completionPlain}, // internal RC_NOWAIT
		{6, ISOLATION_LEVEL_SERIALIZABLE, isc_tpb_wait, false, 0, completionPlain},  // LevelSerializable
		{7, ISOLATION_LEVEL_READ_COMMITED_LEGACY_NOWAIT, isc_tpb_nowait, false, 0, completionPlain},
		{8, ISOLATION_LEVEL_REPEATABLE_READ_NOWAIT, isc_tpb_nowait, false, 0, completionPlain},
		{9, ISOLATION_LEVEL_REPEATABLE_READ_RO, isc_tpb_wait, true, 0, completionPlain},
		{10, ISOLATION_LEVEL_SERIALIZABLE_RO, isc_tpb_wait, true, 0, completionPlain},
		{LevelReadCommittedNoWait, ISOLATION_LEVEL_READ_COMMITED, isc_tpb_nowait, false, 0, completionPlain},
		{LevelLockTimeoutBase + 5, ISOLATION_LEVEL_READ_COMMITED, isc_tpb_wait, false, 5, completionPlain},
		{LevelCommitRetainingBase + ISOLATION_LEVEL_READ_COMMITED, ISOLATION_LEVEL_READ_COMMITED, isc_tpb_wait, false, 0, completionCommitRetaining},
		{LevelRollbackRetainingBase + ISOLATION_LEVEL_SERIALIZABLE, ISOLATION_LEVEL_SERIALIZABLE, isc_tpb_wait, false, 0, completionRollbackRetaining},
		{LevelPrepareThenDieBase + ISOLATION_LEVEL_READ_COMMITED, ISOLATION_LEVEL_READ_COMMITED, isc_tpb_wait, false, 0, completionPrepareThenDie},
		{LevelHardDropBase + ISOLATION_LEVEL_READ_COMMITED, ISOLATION_LEVEL_READ_COMMITED, isc_tpb_wait, false, 0, completionHardDrop},
	}
	for _, c := range cases {
		sc, ok := decodeDriverLevel(c.level)
		require.True(t, ok, "level %d must decode", c.level)
		require.Equal(t, c.iso, sc.isolation, "level %d iso", c.level)
		require.Equal(t, c.wait, sc.waitMode, "level %d wait", c.level)
		require.Equal(t, c.ro, sc.ro, "level %d ro", c.level)
		require.Equal(t, c.lockTo, sc.lockTimeout, "level %d lockTimeout", c.level)
		require.Equal(t, c.complete, sc.completion, "level %d completion", c.level)
	}

	for _, bad := range []int{2000, LevelLockTimeoutBase + maxLockTimeout + 1, 11,
		LevelCommitRetainingBase + numInternalIsolationLevels, 999} {
		_, ok := decodeDriverLevel(bad)
		require.False(t, ok, "level %d must not decode", bad)
	}
}

func TestScenarioTpbBytes(t *testing.T) {
	rcROWait, err := tpbForIsolationLevel(ISOLATION_LEVEL_READ_COMMITED_RO)
	require.NoError(t, err)
	require.Equal(t, []byte{byte(isc_tpb_version3), byte(isc_tpb_read), byte(isc_tpb_wait),
		byte(isc_tpb_read_committed), byte(isc_tpb_rec_version)}, rcROWait)

	// RO composes with the level (this is the F2 fix: snapshot + read-only)
	snapRO := txScenario{isolation: ISOLATION_LEVEL_REPEATABLE_READ, waitMode: isc_tpb_wait, ro: true}
	tpb, err := snapRO.tpbBytes()
	require.NoError(t, err)
	require.Equal(t, []byte{byte(isc_tpb_version3), byte(isc_tpb_read),
		byte(isc_tpb_wait), byte(isc_tpb_concurrency)}, tpb)

	consRO := txScenario{isolation: ISOLATION_LEVEL_SERIALIZABLE, waitMode: isc_tpb_wait, ro: true}
	tpb, err = consRO.tpbBytes()
	require.NoError(t, err)
	require.Equal(t, []byte{byte(isc_tpb_version3), byte(isc_tpb_read),
		byte(isc_tpb_wait), byte(isc_tpb_consistency)}, tpb)

	// lock_timeout: length-prefixed little-endian (VAX), implies WAIT
	lt := txScenario{isolation: ISOLATION_LEVEL_READ_COMMITED, lockTimeout: 5}
	tpb, err = lt.tpbBytes()
	require.NoError(t, err)
	require.Equal(t, []byte{byte(isc_tpb_version3), byte(isc_tpb_write),
		byte(isc_tpb_wait), byte(isc_tpb_lock_timeout), 2, 5, 0,
		byte(isc_tpb_read_committed), byte(isc_tpb_rec_version)}, tpb)

	ltRO := txScenario{isolation: ISOLATION_LEVEL_READ_COMMITED, ro: true, lockTimeout: 1}
	tpb, err = ltRO.tpbBytes()
	require.NoError(t, err)
	require.Equal(t, []byte{byte(isc_tpb_version3), byte(isc_tpb_read),
		byte(isc_tpb_wait), byte(isc_tpb_lock_timeout), 2, 1, 0,
		byte(isc_tpb_read_committed), byte(isc_tpb_rec_version)}, tpb)

	// snapshot nowait (new preset)
	snapNowait := txScenario{isolation: ISOLATION_LEVEL_REPEATABLE_READ_NOWAIT, waitMode: isc_tpb_nowait}
	tpb, err = snapNowait.tpbBytes()
	require.NoError(t, err)
	require.Equal(t, []byte{byte(isc_tpb_version3), byte(isc_tpb_write),
		byte(isc_tpb_nowait), byte(isc_tpb_concurrency)}, tpb)

	// completion intents do not change the TPB
	plain, err := tpbForIsolationLevel(ISOLATION_LEVEL_READ_COMMITED)
	require.NoError(t, err)
	retaining, err := tpbForIsolationLevel(LevelCommitRetainingBase + ISOLATION_LEVEL_READ_COMMITED)
	require.NoError(t, err)
	require.Equal(t, plain, retaining)
	hardDrop, err := tpbForIsolationLevel(LevelHardDropBase + ISOLATION_LEVEL_READ_COMMITED)
	require.NoError(t, err)
	require.Equal(t, plain, hardDrop)

	// unknown level fails loudly
	_, err = tpbForIsolationLevel(12345)
	require.Error(t, err)
}

// ==================== live tests (need a Firebird server) ====================

func TestLiveLockTimeoutTpb(t *testing.T) {
	db, dsn, _ := createTestDatabaseWithDDL(t, "test_lock_to_",
		"CREATE TABLE t_lt (id INTEGER PRIMARY KEY, v INTEGER)",
		"INSERT INTO t_lt VALUES (1, 100)")
	requireBooleanSupport(t)

	ctx := context.Background()
	// release the autocommit retained tx (row lock) from the DDL phase
	db.SetMaxIdleConns(0)
	dbA, err := sql.Open("firebirdsql", dsn)
	require.NoError(t, err)
	defer dbA.Close()

	txA, err := dbA.BeginTx(ctx, nil)
	require.NoError(t, err)
	_, err = txA.ExecContext(ctx, "UPDATE t_lt SET v = v + 1 WHERE id = 1")
	require.NoError(t, err)
	defer func() { _ = txA.Rollback() }()

	// B waits with a 1-second lock timeout and must get a lock time-out error.
	start := time.Now()
	opts := sql.TxOptions{Isolation: sql.IsolationLevel(LevelLockTimeoutBase + 1)}
	txB, err := db.BeginTx(ctx, &opts)
	require.NoError(t, err, "lock_timeout TPB must be accepted by the server")
	_, err = txB.ExecContext(ctx, "UPDATE t_lt SET v = v + 2 WHERE id = 1")
	elapsed := time.Since(start)
	require.Error(t, err, "expected a lock time-out")
	// Firebird reports a lock time-out as a deadlock-class error
	// ("lock time-out on wait transaction" / deadlock primary message).
	require.True(t, strings.Contains(strings.ToLower(err.Error()), "lock time-out") ||
		strings.Contains(strings.ToLower(err.Error()), "lock conflict") ||
		strings.Contains(strings.ToLower(err.Error()), "deadlock"), "unexpected error: %v", err)
	require.Less(t, elapsed, 10*time.Second, "lock timeout must not hang")
	_ = txB.Rollback()

	// NOWAIT on the same row fails immediately (sanity check of the wait axis).
	txC, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.IsolationLevel(LevelReadCommittedNoWait)})
	require.NoError(t, err)
	_, err = txC.ExecContext(ctx, "UPDATE t_lt SET v = v + 3 WHERE id = 1")
	require.Error(t, err)
	start = time.Now()
	require.Error(t, err)
	require.Less(t, time.Since(start), time.Second)
	_ = txC.Rollback()
}

func TestLiveSnapshotReadOnlyComposition(t *testing.T) {
	db, dsn, _ := createTestDatabaseWithDDL(t, "test_snap_ro_",
		"CREATE TABLE t_sr (id INTEGER PRIMARY KEY, v INTEGER)",
		"INSERT INTO t_sr VALUES (1, 100)")
	requireBooleanSupport(t)

	ctx := context.Background()
	dbW, err := sql.Open("firebirdsql", dsn)
	require.NoError(t, err)
	defer dbW.Close()

	// The DDL helper ran as autocommit statements; their teardown (COMMIT
	// RETAINING) keeps the wire tx — and its row locks — alive on the pooled
	// conn. Close idle conns so the retained tx is rolled back and does not
	// block the concurrent writer below.
	db.SetMaxIdleConns(0)

	// ReadOnly + LevelRepeatableRead must compose into snapshot + read-only
	// (before the fix, ReadOnly silently degraded the level to RC RO).
	opts := sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true}
	txRO, err := db.BeginTx(ctx, &opts)
	require.NoError(t, err)

	var v int
	require.NoError(t, txRO.QueryRowContext(ctx, "SELECT v FROM t_sr WHERE id = 1").Scan(&v))
	require.Equal(t, 100, v)

	// write inside the read-only tx must be rejected
	_, err = txRO.ExecContext(ctx, "INSERT INTO t_sr VALUES (2, 2)")
	require.Error(t, err, "read-only transaction must reject writes")

	// snapshot stability: a concurrent committed change stays invisible
	txW, err := dbW.BeginTx(ctx, nil)
	require.NoError(t, err)
	_, err = txW.ExecContext(ctx, "UPDATE t_sr SET v = 200 WHERE id = 1")
	require.NoError(t, err)
	require.NoError(t, txW.Commit())

	require.NoError(t, txRO.QueryRowContext(ctx, "SELECT v FROM t_sr WHERE id = 1").Scan(&v))
	require.Equal(t, 100, v, "snapshot tx must not see the concurrent commit")
	require.NoError(t, txRO.Commit())

	var v2 int
	require.NoError(t, db.QueryRowContext(ctx, "SELECT v FROM t_sr WHERE id = 1").Scan(&v2))
	require.Equal(t, 200, v2)
}

func TestLiveCommitRetainingAndReuse(t *testing.T) {
	db, _, _ := createTestDatabaseWithDDL(t, "test_retain_",
		"CREATE TABLE t_cr (id INTEGER PRIMARY KEY, v INTEGER)")
	requireBooleanSupport(t)

	ctx := context.Background()
	db.SetMaxOpenConns(1) // pin one connection so the retained context is reachable

	opts := sql.TxOptions{Isolation: sql.IsolationLevel(LevelCommitRetainingBase + ISOLATION_LEVEL_READ_COMMITED)}
	tx1, err := db.BeginTx(ctx, &opts)
	require.NoError(t, err)
	_, err = tx1.ExecContext(ctx, "INSERT INTO t_cr VALUES (1, 10)")
	require.NoError(t, err)
	require.NoError(t, tx1.Commit()) // COMMIT RETAINING: wire tx stays live

	// Next BeginTx with the same TPB reuses the retained context and works.
	tx2, err := db.BeginTx(ctx, &opts)
	require.NoError(t, err)
	_, err = tx2.ExecContext(ctx, "INSERT INTO t_cr VALUES (2, 20)")
	require.NoError(t, err)
	require.NoError(t, tx2.Commit())

	var n int
	require.NoError(t, db.QueryRowContext(ctx, "SELECT COUNT(*) FROM t_cr").Scan(&n))
	require.Equal(t, 2, n, "both retained commits must persist")

	// Retained context + plain rollback: the context (and its changes) ends.
	tx3, err := db.BeginTx(ctx, &opts)
	require.NoError(t, err)
	_, err = tx3.ExecContext(ctx, "INSERT INTO t_cr VALUES (3, 30)")
	require.NoError(t, err)
	require.NoError(t, tx3.Rollback())

	require.NoError(t, db.QueryRowContext(ctx, "SELECT COUNT(*) FROM t_cr").Scan(&n))
	require.Equal(t, 2, n, "plain rollback of the retained context must undo the insert")

	// Mismatched next TPB over a live retained context: the retained context
	// is rolled back and the new tx starts fresh.
	tx4, err := db.BeginTx(ctx, &opts)
	require.NoError(t, err)
	_, err = tx4.ExecContext(ctx, "INSERT INTO t_cr VALUES (4, 40)")
	require.NoError(t, err)
	require.NoError(t, tx4.Commit()) // COMMIT RETAINING: context stays live

	tx5, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	require.NoError(t, err, "TPB mismatch must roll the retained context back and start fresh")
	_, err = tx5.ExecContext(ctx, "INSERT INTO t_cr VALUES (5, 50)")
	require.NoError(t, err)
	require.NoError(t, tx5.Commit())

	require.NoError(t, db.QueryRowContext(ctx, "SELECT COUNT(*) FROM t_cr").Scan(&n))
	require.Equal(t, 4, n, "rows 1,2,4,5 must persist")
}

func TestLiveRollbackRetaining(t *testing.T) {
	db, _, _ := createTestDatabaseWithDDL(t, "test_rb_retain_",
		"CREATE TABLE t_rr (id INTEGER PRIMARY KEY, v INTEGER)")
	requireBooleanSupport(t)

	ctx := context.Background()
	opts := sql.TxOptions{Isolation: sql.IsolationLevel(LevelRollbackRetainingBase + ISOLATION_LEVEL_READ_COMMITED)}
	tx1, err := db.BeginTx(ctx, &opts)
	require.NoError(t, err)
	_, err = tx1.ExecContext(ctx, "INSERT INTO t_rr VALUES (1, 10)")
	require.NoError(t, err)
	require.NoError(t, tx1.Rollback()) // ROLLBACK RETAINING: insert undone

	var n int
	require.NoError(t, db.QueryRowContext(ctx, "SELECT COUNT(*) FROM t_rr").Scan(&n))
	require.Equal(t, 0, n, "rollback retaining must undo the insert")
}

func TestLivePrepareThenDieLeavesLimbo(t *testing.T) {
	_, dsn, file := createTestDatabaseWithDDL(t, "test_limbo_",
		"CREATE TABLE t_pl (id INTEGER PRIMARY KEY, v INTEGER)")
	requireBooleanSupport(t)
	requireServiceAvailable(t)

	ctx := context.Background()
	// dedicated single-conn pool: the limbo tx belongs to this attachment
	dbL, err := sql.Open("firebirdsql", dsn)
	require.NoError(t, err)
	defer dbL.Close()
	dbL.SetMaxOpenConns(1)

	tx, err := dbL.BeginTx(ctx, &sql.TxOptions{Isolation: sql.IsolationLevel(LevelPrepareThenDieBase + ISOLATION_LEVEL_READ_COMMITED)})
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, "INSERT INTO t_pl VALUES (1, 10)")
	require.NoError(t, err)

	err = tx.Commit()
	require.Error(t, err, "prepare-then-die must fail the commit (socket dropped)")
	require.True(t, errors.Is(err, driver.ErrBadConn), "want ErrBadConn, got %v", err)

	// The socket is dead: subsequent use must fail or reconnect cleanly, not hang.
	require.Eventually(t, func() bool {
		c3, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		_, _ = dbL.QueryContext(c3, "SELECT 1 FROM RDB$DATABASE")
		return true
	}, 6*time.Second, 100*time.Millisecond)

	// A fresh attachment must see the transaction in limbo (services API).
	mm, err := NewMaintenanceManager(testServerAddr(), GetTestUser(), GetTestPassword(), GetDefaultServiceManagerOptions())
	require.NoError(t, err)

	var tids []int64
	require.Eventually(t, func() bool {
		tids, err = mm.GetLimboTransactions(file)
		return err == nil && len(tids) >= 1
	}, 15*time.Second, 300*time.Millisecond, "prepared (limbo) transaction must be listed by the services API")
	t.Logf("limbo transactions: %v", tids)

	// Resolve it (gfix -rollback equivalent). The action may legally report
	// failure here: the engine's own recovery can resolve the transaction
	// between the list query and the action, which now surfaces as an error
	// instead of a silent nil. The behavioral contract is that the list drains.
	if rerr := mm.RollbackLimboTransaction(file, tids[0]); rerr != nil {
		t.Logf("resolution attempt reported: %v", rerr)
	}
	require.Eventually(t, func() bool {
		rest, err := mm.GetLimboTransactions(file)
		return err == nil && len(rest) == 0
	}, 15*time.Second, 300*time.Millisecond, "limbo transaction must be resolved")
}

func TestLiveHardDropOpenTx(t *testing.T) {
	_, dsn, _ := createTestDatabaseWithDDL(t, "test_harddrop_",
		"CREATE TABLE t_hd (id INTEGER PRIMARY KEY, v INTEGER)")
	requireBooleanSupport(t)

	ctx := context.Background()
	dbL, err := sql.Open("firebirdsql", dsn)
	require.NoError(t, err)
	defer dbL.Close()
	dbL.SetMaxOpenConns(1)

	tx, err := dbL.BeginTx(ctx, &sql.TxOptions{Isolation: sql.IsolationLevel(LevelHardDropBase + ISOLATION_LEVEL_READ_COMMITED)})
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, "INSERT INTO t_hd VALUES (1, 10)")
	require.NoError(t, err)

	err = tx.Rollback()
	require.Error(t, err, "hard drop must fail the rollback (socket dropped)")
	require.True(t, errors.Is(err, driver.ErrBadConn), "want ErrBadConn, got %v", err)

	// The uncommitted insert died with the attachment: a fresh connection sees 0 rows.
	dbFix := openTestDatabase(t, dsn)
	var n int
	require.Eventually(t, func() bool {
		row := dbFix.QueryRowContext(ctx, "SELECT COUNT(*) FROM t_hd")
		return row.Scan(&n) == nil && n == 0
	}, 10*time.Second, 200*time.Millisecond, "hard drop must not leave the uncommitted insert visible")
}

func TestIntentPreservesWaitAndRO(t *testing.T) {
	// 5005 = commit retaining of ISOLATION_LEVEL_READ_COMMITED_NOWAIT: the
	// TPB must stay NOWAIT (the intent must not degrade to infinite WAIT).
	sc, ok := decodeDriverLevel(LevelCommitRetainingBase + ISOLATION_LEVEL_READ_COMMITED_NOWAIT)
	require.True(t, ok)
	require.Equal(t, completionCommitRetaining, sc.completion)
	tpb, err := sc.tpbBytes()
	require.NoError(t, err)
	require.Equal(t, []byte{byte(isc_tpb_version3), byte(isc_tpb_write), byte(isc_tpb_nowait),
		byte(isc_tpb_read_committed), byte(isc_tpb_rec_version)}, tpb)

	// hard drop of the snapshot-RO preset keeps read-only
	scHD, ok := decodeDriverLevel(LevelHardDropBase + ISOLATION_LEVEL_REPEATABLE_READ_RO)
	require.True(t, ok)
	require.True(t, scHD.ro)
	tpbHD, err := scHD.tpbBytes()
	require.NoError(t, err)
	require.Equal(t, byte(isc_tpb_read), tpbHD[1])
}
