package firebirdsql

import (
	"context"
	"database/sql"
	"testing"
	"time"
)

// TestLiveProbeResolveLimboOutput captures what the services repair actions
// actually print in both phases of a limbo window: while the dead attachment
// still holds the prepared transaction (failure phase) and after the engine
// sweeps it. The line-presence rule in resolveLimbo must rest on observed
// output, not guesses.
func TestLiveProbeResolveLimboOutput(t *testing.T) {
	_, dsn, file := createTestDatabaseWithDDL(t, "test_limboprobe_",
		"CREATE TABLE t_lop (id INTEGER PRIMARY KEY, v INTEGER)")
	requireBooleanSupport(t)
	requireServiceAvailable(t)

	ctx := context.Background()
	dbL, err := sql.Open("firebirdsql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer dbL.Close()
	dbL.SetMaxOpenConns(1)

	tx, err := dbL.BeginTx(ctx, &sql.TxOptions{Isolation: sql.IsolationLevel(LevelPrepareThenDieBase + ISOLATION_LEVEL_READ_COMMITED)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO t_lop VALUES (1, 10)"); err != nil {
		t.Fatal(err)
	}
	err = tx.Commit()
	if err == nil {
		t.Fatal("prepare-then-die must fail the commit (socket dropped)")
	}

	mm, err := NewMaintenanceManager(testServerAddr(), GetTestUser(), GetTestPassword(), GetDefaultServiceManagerOptions())
	if err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(60 * time.Second)
	for attempt := 1; time.Now().Before(deadline); attempt++ {
		tids, lerr := mm.GetLimboTransactions(file)
		if lerr != nil {
			t.Fatalf("GetLimboTransactions: %v", lerr)
		}
		if len(tids) == 0 {
			t.Logf("attempt %d: limbo list drained — probe complete", attempt)
			return
		}
		tid := tids[0]
		var lines []string
		var aerr error
		lines, aerr = mm.resolveLimboVerbose(file, isc_spb_rpr_commit_trans_64, tid)
		t.Logf("attempt %2d commit   tid=%d err=%v lines=%q", attempt, tid, aerr, lines)
		lines, aerr = mm.resolveLimboVerbose(file, isc_spb_rpr_rollback_trans_64, tid)
		t.Logf("attempt %2d rollback tid=%d err=%v lines=%q", attempt, tid, aerr, lines)
		lines, aerr = mm.resolveLimboVerbose(file, isc_spb_rpr_recover_two_phase_64, tid)
		t.Logf("attempt %2d 2phase   tid=%d err=%v lines=%q", attempt, tid, aerr, lines)
		time.Sleep(500 * time.Millisecond)
	}
	t.Log("deadline hit — logging last observed state above")
}

// TestLiveResolveLimboActionFailureSurfaces is the deterministic regression
// for the line-presence rule: a repair action aimed at a transaction id that
// does not exist in limbo must surface an error, not a silent nil.
func TestLiveResolveLimboActionFailureSurfaces(t *testing.T) {
	_, _, file := createTestDatabaseWithDDL(t, "test_limbosilent_",
		"CREATE TABLE t_los (id INTEGER PRIMARY KEY, v INTEGER)")
	requireBooleanSupport(t)
	requireServiceAvailable(t)

	mm, err := NewMaintenanceManager(testServerAddr(), GetTestUser(), GetTestPassword(), GetDefaultServiceManagerOptions())
	if err != nil {
		t.Fatal(err)
	}

	err = mm.CommitLimboTransaction(file, 987654321)
	if err == nil {
		t.Fatal("committing a nonexistent limbo transaction must return an error, got nil")
	}
	t.Logf("commit: %v", err)

	err = mm.RollbackLimboTransaction(file, 987654321)
	if err == nil {
		t.Fatal("rolling back a nonexistent limbo transaction must return an error, got nil")
	}
	t.Logf("rollback: %v", err)
}
