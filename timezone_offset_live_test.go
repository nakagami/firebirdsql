package firebirdsql

import (
	"context"
	"testing"
	"time"
)

// A live test. The server stores a TIMESTAMP WITH TIME ZONE value with the
// zone of the Go fixed zone. The test compares with the literal of the
// server and with EXTRACT, not with the text. The value reads back with the
// zone. The offsets +05:30 and -03:30 cover the two signs and the sign of
// the minutes.
func TestTimestampTzFixedOffsetLive(t *testing.T) {
	requireTimeZoneSupport(t)

	db, _, _ := createTestDatabaseWithDDL(t, "tstz_off_",
		"CREATE TABLE t_tstz_off (id INTEGER, v TIMESTAMP WITH TIME ZONE)")
	ctx := context.Background()

	plus := time.FixedZone("+05:30", 5*3600+30*60)
	in := time.Date(2026, 7, 15, 12, 34, 56, 789000000, plus)
	mustExec(t, ctx, db, "INSERT INTO t_tstz_off (id, v) VALUES (1, ?)", in)

	minus := time.FixedZone("-03:30", -(3*3600 + 30*60))
	inNeg := time.Date(2026, 7, 15, 12, 34, 56, 789000000, minus)
	mustExec(t, ctx, db, "INSERT INTO t_tstz_off (id, v) VALUES (2, ?)", inNeg)

	var equalsLiteral bool
	if err := db.QueryRowContext(ctx,
		"SELECT v = CAST('2026-07-15 12:34:56.7890 +05:30' AS TIMESTAMP WITH TIME ZONE) FROM t_tstz_off WHERE id = 1",
	).Scan(&equalsLiteral); err != nil {
		t.Fatalf("select literal compare: %v", err)
	}
	if !equalsLiteral {
		t.Errorf("stored +05:30 value is not equal to the server's own '+05:30' literal: the zone must be stored, not flattened to UTC")
	}

	var hour, minute int
	if err := db.QueryRowContext(ctx,
		"SELECT EXTRACT(TIMEZONE_HOUR FROM v), EXTRACT(TIMEZONE_MINUTE FROM v) FROM t_tstz_off WHERE id = 1",
	).Scan(&hour, &minute); err != nil {
		t.Fatalf("select extract +05:30: %v", err)
	}
	if hour != 5 || minute != 30 {
		t.Errorf("EXTRACT(TIMEZONE_HOUR, TIMEZONE_MINUTE) for +05:30 = %d, %d; want 5, 30", hour, minute)
	}

	if err := db.QueryRowContext(ctx,
		"SELECT EXTRACT(TIMEZONE_HOUR FROM v), EXTRACT(TIMEZONE_MINUTE FROM v) FROM t_tstz_off WHERE id = 2",
	).Scan(&hour, &minute); err != nil {
		t.Fatalf("select extract -03:30: %v", err)
	}
	if hour != -3 || minute != -30 {
		t.Errorf("EXTRACT(TIMEZONE_HOUR, TIMEZONE_MINUTE) for -03:30 = %d, %d; want -3, -30", hour, minute)
	}

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
