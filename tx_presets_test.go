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

// ==================== pure decode / TPB tests ====================

func TestDecodeDriverLevelMatrix(t *testing.T) {
	cases := []struct {
		level  int
		iso    int
		wait   int
		ro     bool
		lockTo int
	}{
		// plain values: historical sql.Level* behavior preserved
		{0, ISOLATION_LEVEL_READ_COMMITED, isc_tpb_wait, false, 0},
		{1, ISOLATION_LEVEL_READ_COMMITED, isc_tpb_wait, false, 0},
		{2, ISOLATION_LEVEL_READ_COMMITED, isc_tpb_wait, false, 0},                    // LevelReadCommitted
		{3, ISOLATION_LEVEL_SERIALIZABLE, isc_tpb_wait, false, 0},                     // LevelWriteCommitted / internal SERIALIZABLE
		{4, ISOLATION_LEVEL_REPEATABLE_READ, isc_tpb_wait, false, 0},                  // LevelRepeatableRead
		{5, ISOLATION_LEVEL_READ_COMMITED_NOWAIT, isc_tpb_nowait, false, 0},           // internal RC_NOWAIT
		{6, ISOLATION_LEVEL_SERIALIZABLE, isc_tpb_wait, false, 0},                     // LevelSerializable
		{7, ISOLATION_LEVEL_READ_COMMITED_LEGACY_NOWAIT, isc_tpb_nowait, false, 0},    // internal preset
		{8, ISOLATION_LEVEL_REPEATABLE_READ_NOWAIT, isc_tpb_nowait, false, 0},         // internal preset
		{9, ISOLATION_LEVEL_REPEATABLE_READ_RO, isc_tpb_wait, true, 0},                // internal preset
		{10, ISOLATION_LEVEL_SERIALIZABLE_RO, isc_tpb_wait, true, 0},                  // internal preset
		{LevelReadCommittedNoWait, ISOLATION_LEVEL_READ_COMMITED, isc_tpb_nowait, false, 0},
		{LevelReadCommittedLegacy, ISOLATION_LEVEL_READ_COMMITED_LEGACY, isc_tpb_wait, false, 0},
		{LevelSnapshot, ISOLATION_LEVEL_REPEATABLE_READ, isc_tpb_wait, false, 0},
		{LevelSnapshotNoWait, ISOLATION_LEVEL_REPEATABLE_READ_NOWAIT, isc_tpb_nowait, false, 0},
		{LevelConsistency, ISOLATION_LEVEL_SERIALIZABLE, isc_tpb_wait, false, 0},
		{LevelLockTimeoutBase + 5, ISOLATION_LEVEL_READ_COMMITED, isc_tpb_wait, false, 5},
	}
	for _, c := range cases {
		sc, ok := decodeDriverLevel(c.level)
		require.True(t, ok, "level %d must decode", c.level)
		require.Equal(t, c.iso, sc.isolation, "level %d iso", c.level)
		require.Equal(t, c.wait, sc.waitMode, "level %d wait", c.level)
		require.Equal(t, c.ro, sc.ro, "level %d ro", c.level)
		require.Equal(t, c.lockTo, sc.lockTimeout, "level %d lockTimeout", c.level)
	}

	for _, bad := range []int{LevelLockTimeoutBase, LevelLockTimeoutBase + maxLockTimeout + 1, 11, 999, 12345} {
		_, ok := decodeDriverLevel(bad)
		require.False(t, ok, "level %d must not decode", bad)
	}
}

func TestScenarioTpbBytes(t *testing.T) {
	rcROWait, err := tpbForIsolationLevel(ISOLATION_LEVEL_READ_COMMITED_RO)
	require.NoError(t, err)
	require.Equal(t, []byte{byte(isc_tpb_version3), byte(isc_tpb_read), byte(isc_tpb_wait),
		byte(isc_tpb_read_committed), byte(isc_tpb_rec_version)}, rcROWait)

	// RO composes with the level (snapshot + read-only)
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

	// the legacy internal table and the decoder agree on the shared constants
	legacy, err := tpbForIsolationLevel(ISOLATION_LEVEL_READ_COMMITED_LEGACY)
	require.NoError(t, err)
	decoded, ok := decodeDriverLevel(LevelReadCommittedLegacy)
	require.True(t, ok)
	legacyDecoded, err := decoded.tpbBytes()
	require.NoError(t, err)
	require.Equal(t, legacy, legacyDecoded)

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
