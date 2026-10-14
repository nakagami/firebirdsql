package firebirdsql

import (
	"context"
	"testing"
	"time"
)

// Live: a TIMESTAMP WITH TIME ZONE bound from a +05:30 Go zone is stored with
// the zone — the server's own text rendering names it, and the value reads
// back with the zone intact.
func TestTimestampTzFixedOffsetLive(t *testing.T) {
	db, _, file := createTestDatabaseWithDDL(t, "tstz_off_",
		"CREATE TABLE t_tstz_off (id INTEGER, v TIMESTAMP WITH TIME ZONE)")
	defer func() { _ = removeDatabaseFile(file) }()
	requireBatchSupport(t, db)
	ctx := context.Background()

	z := time.FixedZone("+05:30", 5*3600+30*60)
	in := time.Date(2026, 7, 15, 12, 34, 56, 789000000, z)
	mustExec(t, ctx, db, "INSERT INTO t_tstz_off (id, v) VALUES (1, ?)", in)

	var text string
	if err := db.QueryRowContext(ctx,
		"SELECT CAST(v AS VARCHAR(64)) FROM t_tstz_off WHERE id = 1").Scan(&text); err != nil {
		t.Fatalf("select text: %v", err)
	}
	if want := "2026-07-15 12:34:56.7890 +05:30"; text != want {
		t.Errorf("server text = %q, want %q (the zone must be stored, not flattened to UTC)", text, want)
	}

	var out time.Time
	if err := db.QueryRowContext(ctx, "SELECT v FROM t_tstz_off WHERE id = 1").Scan(&out); err != nil {
		t.Fatalf("select v: %v", err)
	}
	if !out.Equal(in) {
		t.Errorf("read back = %v (%v), want %v", out.Format(time.RFC3339Nano), out.Location().String(), in.Format(time.RFC3339Nano))
	}
	if out.Location().String() != "+05:30" {
		t.Errorf("read back zone = %q, want +05:30", out.Location().String())
	}
}
