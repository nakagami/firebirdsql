package firebirdsql

import (
	"context"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// TIME WITH TIME ZONE wire encoding: the UTC offset must be resolved on the
// 2020-01-01 base date (the Firebird/Jaybird/fbx convention), not on today's
// date or on Go's year-0 default.
// ---------------------------------------------------------------------------

// encodeTimeTz is the wire payload _timeToBlr produces: UTC time-of-day ticks
// plus the trailing timezone bytes.
func encodeTimeTz(t *testing.T, v time.Time) (ticks int32, tzID int) {
	t.Helper()
	blr, payload := _timeToBlr(v, PROTOCOL_VERSION16, "")
	if len(blr) != 1 || blr[0] != 28 {
		t.Fatalf("blr = %v, want blr_text 28 (TIME WITH TIME ZONE)", blr)
	}
	if len(payload) != 8 {
		t.Fatalf("payload length = %d, want 8", len(payload))
	}
	return bytes_to_bint32(payload[:4]), int(bytes_to_buint16(payload[6:8]))
}

// A DST-zone time must encode with the base date's standard offset: 12:00
// America/New_York in July (EDT, -4 today) is 17:00 UTC, not 16:00, because
// the server resolves TIME WITH TIME ZONE on 2020-01-01 (EST, -5).
func TestEncodeTimeTzDSTUsesBaseDateOffset(t *testing.T) {
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skipf("IANA tzdata unavailable: %v", err)
	}

	ticks, tzID := encodeTimeTz(t, time.Date(2026, 7, 15, 12, 0, 0, 0, ny))
	if want := int32(17 * 3600 * 10000); ticks != want {
		t.Errorf("DST-season 12:00 America/New_York: UTC ticks = %d, want %d (17:00)", ticks, want)
	}
	if tzID != 65361 { // Firebird id of America/New_York
		t.Errorf("tzID = %d, want 65361 (America/New_York)", tzID)
	}

	// The date part of the input is irrelevant for a date-less type: a winter
	// value encodes to the same wire time.
	ticks2, _ := encodeTimeTz(t, time.Date(2026, 1, 15, 12, 0, 0, 0, ny))
	if ticks2 != int32(17*3600*10000) {
		t.Errorf("winter 12:00 America/New_York: UTC ticks = %d, want %d", ticks2, int32(17*3600*10000))
	}
}

func TestEncodeTimeTzUTCAndFractions(t *testing.T) {
	ticks, tzID := encodeTimeTz(t, time.Date(2026, 7, 15, 23, 59, 59, 999900000, time.UTC))
	if want := int32((23*3600+59*60+59)*10000 + 9999); ticks != want {
		t.Errorf("UTC 23:59:59.9999: ticks = %d, want %d", ticks, want)
	}
	if tzID != 64909 { // Firebird id of UTC
		t.Errorf("tzID = %d, want 64909 (UTC)", tzID)
	}
}

// Encode/decode round trip through the wire bytes: wall clock and offset come
// back on the base date, symmetrically on both sides.
func TestTimeTzWireRoundTrip(t *testing.T) {
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skipf("IANA tzdata unavailable: %v", err)
	}
	in := time.Date(2026, 7, 15, 12, 0, 0, 0, ny)

	blr, payload := _timeToBlr(in, PROTOCOL_VERSION16, "")
	_ = blr
	out := (&xSQLVAR{}).parseTimeTz(payload)

	if out.Hour() != 12 || out.Minute() != 0 || out.Second() != 0 {
		t.Errorf("round trip wall clock = %02d:%02d:%02d, want 12:00:00", out.Hour(), out.Minute(), out.Second())
	}
	_, offset := out.Zone()
	if offset != -5*3600 {
		t.Errorf("round trip offset = %d, want %d (EST on the base date)", offset, -5*3600)
	}
	if out.Nanosecond() != 0 {
		t.Errorf("round trip nanoseconds = %d, want 0", out.Nanosecond())
	}
}

// A live check against the server's own convention: what the driver binds for
// 12:00 America/New_York must be byte-equivalent to what the server makes of
// the literal '12:00:00 America/New_York'.
func TestTimeTzDSTMatchesServerLiteral(t *testing.T) {
	db, _, file := createTestDatabaseWithDDL(t, "timtz_dst_",
		"CREATE TABLE t_timtz_dst (id INTEGER, v TIME WITH TIME ZONE)")
	defer func() { _ = removeDatabaseFile(file) }()
	requireBatchSupport(t, db)
	ctx := context.Background()

	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skipf("IANA tzdata unavailable: %v", err)
	}
	in := time.Date(2026, 7, 15, 12, 0, 0, 0, ny) // EDT today, EST on the base date
	mustExec(t, ctx, db, "INSERT INTO t_timtz_dst (id, v) VALUES (1, ?)", in)

	var stored, literal string
	if err := db.QueryRowContext(ctx,
		"SELECT CAST(v AS VARCHAR(64)), CAST(CAST('12:00:00 America/New_York' AS TIME WITH TIME ZONE) AS VARCHAR(64)) FROM t_timtz_dst WHERE id = 1",
	).Scan(&stored, &literal); err != nil {
		t.Fatalf("select: %v", err)
	}
	if stored != literal {
		t.Errorf("stored = %q, server literal = %q; the driver must match the server's own resolution of the same wall clock", stored, literal)
	}
	if want := "12:00:00.0000 America/New_York"; stored != want {
		t.Errorf("stored = %q, want %q", stored, want)
	}

	var out time.Time
	if err := db.QueryRowContext(ctx, "SELECT v FROM t_timtz_dst WHERE id = 1").Scan(&out); err != nil {
		t.Fatalf("select v: %v", err)
	}
	if out.Hour() != 12 {
		t.Errorf("read back hour = %d, want 12", out.Hour())
	}
	if _, offset := out.Zone(); offset != -5*3600 {
		t.Errorf("read back offset = %d, want %d (EST on the base date)", offset, -5*3600)
	}
}
