package firebirdsql

import (
	"context"
	"database/sql"
	"testing"
	"time"
)

// Live proof of cancel_hard_drop (needs a local Firebird 3.0+): a statement
// parked in a server-side lock wait does not honor op_cancel raise, so only
// the socket close can return the call to the client. The OS-deadline
// fallback would return at ctx+3s; the drop with grace=500ms must beat it.
func TestLiveCancelHardDropUnblocksLockWait(t *testing.T) {
	db, dsn, _ := createTestDatabaseWithDDL(t, "canceldrop",
		"CREATE TABLE CD_T (id integer primary key, v integer)",
		"INSERT INTO CD_T values (1, 0)",
	)
	if engineMajorVersion(db) < 3 {
		t.Skip("requires Firebird 3.0+ (op_cancel semantics)")
	}

	// The holder pins the row lock in an uncommitted WAIT transaction.
	holder, err := sql.Open("firebirdsql", dsn)
	if err != nil {
		t.Fatalf("open holder: %v", err)
	}
	defer holder.Close()
	if engineMajorVersion(holder) < 3 {
		t.Skip("requires Firebird 3.0+")
	}
	htx, err := holder.Begin()
	if err != nil {
		t.Fatalf("holder begin: %v", err)
	}
	defer htx.Rollback()
	if _, err := htx.Exec("UPDATE CD_T set v = 1 where id = 1"); err != nil {
		t.Fatalf("holder update: %v", err)
	}

	// The victim: one pooled connection, hard-drop armed.
	hardDSN := dsn + "?cancel_hard_drop=true&cancel_hard_drop_grace=500"
	victim, err := sql.Open("firebirdsql", hardDSN)
	if err != nil {
		t.Fatalf("open victim: %v", err)
	}
	defer victim.Close()
	victim.SetMaxOpenConns(1)
	victim.SetMaxIdleConns(1)
	if err := victim.Ping(); err != nil {
		t.Fatalf("victim ping: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	start := time.Now()
	_, execErr := victim.ExecContext(ctx, "UPDATE CD_T set v = 2 where id = 1")
	elapsed := time.Since(start)

	if execErr == nil {
		t.Fatal("expected an error from the canceled lock wait, got nil")
	}
	// The drop must land at ~ctx+grace (1.5s), well before the OS-deadline
	// fallback (ctx+3s = 4s). A return slower than 3s means the drop did not
	// do the work.
	if elapsed > 3*time.Second {
		t.Fatalf("canceled lock wait returned after %v; hard drop did not beat the OS deadline", elapsed)
	}

	// database/sql discards the dropped connection: the next command on the
	// same pool must open a fresh connection and succeed.
	if err := victim.Ping(); err != nil {
		t.Fatalf("ping after hard drop: %v", err)
	}
}
