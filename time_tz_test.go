package firebirdsql

import (
	"context"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// TIME WITH TIME ZONE wire encoding. The offset must be resolved on the
// 2020-01-01 base date (the Firebird/Jaybird/fbx convention). It must not be
// resolved on today's date, or on the Go year-0 default.
// ---------------------------------------------------------------------------

// encodeTimeTz returns the wire payload that _timeToBlr produces: the UTC
// time-of-day ticks and the trailing timezone bytes.
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

// The server resolves a TIME WITH TIME ZONE value on the 2020-01-01 base
// date (EST, -5). A time of 12:00 America/New_York in July has the EDT
// offset -4 today. The encoded value is 17:00 UTC, not 16:00 UTC.
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

	// A TIME WITH TIME ZONE value has no date part. A winter value encodes
	// to the same wire time.
	ticks2, _ := encodeTimeTz(t, time.Date(2026, 1, 15, 12, 0, 0, 0, ny))
	if ticks2 != int32(17*3600*10000) {
		t.Errorf("winter 12:00 America/New_York: UTC ticks = %d, want %d", ticks2, int32(17*3600*10000))
	}
}

// The rules of these zones changed after the base date. America/Whitehorse
// changed to permanent UTC-7 on 2020-03-08. Asia/Almaty changed from +06 to
// +05 on 2024-03-01. The offset today is different from the offset on
// 2020-01-01 in all seasons. These tests pin the base date all year.
func TestEncodeTimeTzPost2020RuleChange(t *testing.T) {
	wh, err := time.LoadLocation("America/Whitehorse")
	if err != nil {
		t.Skipf("IANA tzdata unavailable: %v", err)
	}
	ticks, tzID := encodeTimeTz(t, time.Date(2026, 7, 15, 12, 0, 0, 0, wh))
	if want := int32(20 * 3600 * 10000); ticks != want {
		t.Errorf("12:00 America/Whitehorse: UTC ticks = %d, want %d (20:00, the 2020-01-01 UTC-8 offset)", ticks, want)
	}
	if tzID != int(getTimezoneIDByName("America/Whitehorse")) {
		t.Errorf("tzID = %d, want %d (America/Whitehorse)", tzID, getTimezoneIDByName("America/Whitehorse"))
	}

	alm, err := time.LoadLocation("Asia/Almaty")
	if err != nil {
		t.Skipf("IANA tzdata unavailable: %v", err)
	}
	ticks, _ = encodeTimeTz(t, time.Date(2026, 7, 15, 12, 0, 0, 0, alm))
	if want := int32(6 * 3600 * 10000); ticks != want {
		t.Errorf("12:00 Asia/Almaty: UTC ticks = %d, want %d (06:00, the 2020-01-01 +06 offset)", ticks, want)
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

// The driver resolves the ticks in the zone that the label names, not in
// the zone of the value. Only a DST zone makes the two differ in a test: a
// fixed zone has the same offset on every date, and the anchors of today,
// of the base date, and of the date of the value then all agree. So the
// values come from Europe/Berlin, and the test calls _convert_time_tz with
// the labels UTC and Asia/Kathmandu. The July value has the CEST offset
// (+2): the instant is 10:00 UTC. The January value has the CET offset
// (+1): the instant is 11:00 UTC. An anchor on today, or in the zone of the
// value on the base date, gives a different hour in at least one case.
func TestEncodeTimeTzLabelDiffersFromValueZone(t *testing.T) {
	berlin, err := time.LoadLocation("Europe/Berlin")
	if err != nil {
		t.Skipf("IANA tzdata unavailable: %v", err)
	}
	utcID := getTimezoneIDByName("UTC")
	kathmanduID := getTimezoneIDByName("Asia/Kathmandu")
	// The guard keeps the test honest: a map miss would give label 0. Id 0
	// is a valid displacement id (-23:59), and the tick checks would then
	// still pass, with the wrong label.
	if utcID == 0 || kathmanduID == 0 {
		t.Fatalf("UTC or Asia/Kathmandu is not in the driver's timezone map (%d, %d)", utcID, kathmanduID)
	}
	for _, c := range []struct {
		in      time.Time
		label   int32
		wantUTC int // hour on the wire
	}{
		{time.Date(2026, 7, 15, 12, 0, 0, 0, berlin), utcID, 10},       // CEST
		{time.Date(2026, 1, 15, 12, 0, 0, 0, berlin), utcID, 11},       // CET
		{time.Date(2026, 7, 15, 12, 0, 0, 0, berlin), kathmanduID, 10}, // 15:45 Kathmandu
	} {
		p := _convert_time_tz(c.in, c.label)
		if got, want := bytes_to_bint32(p[:4]), int32(c.wantUTC*3600*10000); got != want {
			t.Errorf("%v label %d: UTC ticks = %d, want %d", c.in, c.label, got, want)
		}
	}
}

// An encode/decode round trip through the wire bytes. The wall clock and
// the offset come back on the base date. The two sides are symmetric.
func TestTimeTzWireRoundTrip(t *testing.T) {
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skipf("IANA tzdata unavailable: %v", err)
	}
	in := time.Date(2026, 7, 15, 12, 0, 0, 0, ny)

	_, payload := _timeToBlr(in, PROTOCOL_VERSION16, "")
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

// A decode-encode round trip must keep the instant and the offset. The
// decoder returns a year-0 time in a fixed zone with an abbreviation name
// (EST for America/New_York). The re-encode must not apply the year-0 LMT
// offset of that zone. The zone id can be different: EST has its own
// Firebird id. The test compares the ticks and the offsets, not the full
// payloads.
func TestEncodeTimeTzRebindDecoded(t *testing.T) {
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skipf("IANA tzdata unavailable: %v", err)
	}
	in := time.Date(2026, 7, 15, 12, 0, 0, 0, ny)

	_, payload := _timeToBlr(in, PROTOCOL_VERSION16, "")
	out := (&xSQLVAR{}).parseTimeTz(payload)
	_, rebound := _timeToBlr(out, PROTOCOL_VERSION16, "")

	if got, want := bytes_to_bint32(rebound[:4]), bytes_to_bint32(payload[:4]); got != want {
		t.Errorf("re-bound UTC ticks = %d, want %d", got, want)
	}
	for name, p := range map[string][]byte{"original": payload, "re-bound": rebound} {
		_, off := time.Date(2020, 1, 1, 12, 0, 0, 0, _parseTimezoneID(p[6:8])).Zone()
		if off != -5*3600 {
			t.Errorf("%s payload zone offset = %d, want %d (the base-date offset of America/New_York)", name, off, -5*3600)
		}
	}
}

// A live check against the convention of the server. The value that the
// driver binds for 12:00 America/New_York must be equal to the value of the
// literal '12:00:00 America/New_York'. The test compares values (equality,
// EXTRACT), not text.
func TestTimeTzDSTMatchesServerLiteral(t *testing.T) {
	requireTimeZoneSupport(t)

	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skipf("IANA tzdata unavailable: %v", err)
	}
	db, _, _ := createTestDatabaseWithDDL(t, "timtz_dst_",
		"CREATE TABLE t_timtz_dst (id INTEGER, v TIME WITH TIME ZONE)")
	ctx := context.Background()

	in := time.Date(2026, 7, 15, 12, 0, 0, 0, ny) // EDT today, EST on the base date
	mustExec(t, ctx, db, "INSERT INTO t_timtz_dst (id, v) VALUES (1, ?)", in)

	var stored string
	var equalsLiteral bool
	if err := db.QueryRowContext(ctx,
		"SELECT CAST(v AS VARCHAR(64)), v = CAST('12:00:00 America/New_York' AS TIME WITH TIME ZONE) FROM t_timtz_dst WHERE id = 1",
	).Scan(&stored, &equalsLiteral); err != nil {
		t.Fatalf("select: %v", err)
	}
	if !equalsLiteral {
		t.Errorf("stored = %q: the driver must match the server's own resolution of the same wall clock", stored)
	}
	if want := "12:00:00.0000 America/New_York"; stored != want {
		t.Errorf("stored = %q, want %q", stored, want)
	}

	var hour, minute int
	if err := db.QueryRowContext(ctx,
		"SELECT EXTRACT(HOUR FROM v), EXTRACT(MINUTE FROM v) FROM t_timtz_dst WHERE id = 1",
	).Scan(&hour, &minute); err != nil {
		t.Fatalf("select extract: %v", err)
	}
	if hour != 12 || minute != 0 {
		t.Errorf("EXTRACT(HOUR, MINUTE) = %d, %d, want 12, 0", hour, minute)
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
