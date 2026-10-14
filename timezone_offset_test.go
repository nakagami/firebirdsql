package firebirdsql

import (
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Firebird GMT-displacement zones: fixed UTC offsets encoded as zone ids
// (offset minutes + 1439), in both directions, and fixed-offset Go zones
// (time.FixedZone / "+05:30") round tripping without being flattened to UTC.
// ---------------------------------------------------------------------------

func TestParseUTCOffset(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want int
		ok   bool
	}{
		{"half hour", "+05:30", 5*3600 + 30*60, true},
		{"negative", "-08:00", -8 * 3600, true},
		{"quarter", "+05:45", 5*3600 + 45*60, true},
		{"compact", "+0530", 5*3600 + 30*60, true},
		{"hours only", "+05", 5 * 3600, true},
		{"zero", "+00:00", 0, true},
		{"no sign", "05:30", 0, false},
		{"zone name", "Europe/Moscow", 0, false},
		{"empty", "", 0, false},
		{"minutes overflow", "+05:75", 0, false},
		{"hours overflow", "+24:00", 0, false},
		{"garbage", "+ab:cd", 0, false},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := parseUTCOffset(tt.in)
			if ok != tt.ok || (ok && got != tt.want) {
				t.Errorf("parseUTCOffset(%q) = %d, %v; want %d, %v", tt.in, got, ok, tt.want, tt.ok)
			}
		})
	}
}

func TestOffsetTimezoneID(t *testing.T) {
	cases := []struct {
		name    string
		seconds int
		want    int32
		ok      bool
	}{
		{"+05:30", 5*3600 + 30*60, 1769, true},
		{"-08:00", -8 * 3600, 959, true},
		{"+00:00", 0, 1439, true},
		{"range edge +23:59", 23*3600 + 59*60, 2878, true},
		{"range edge -23:59", -(23*3600 + 59*60), 0, true},
		{"out of range", 24 * 3600, 0, false},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := offsetTimezoneID(tt.seconds)
			if ok != tt.ok || (ok && got != tt.want) {
				t.Errorf("offsetTimezoneID(%d) = %d, %v; want %d, %v", tt.seconds, got, ok, tt.want, tt.ok)
			}
		})
	}
}

// resolveTimezone must hand Go fixed zones to the displacement id, not fall
// back to UTC.
func TestResolveTimezoneFixedOffset(t *testing.T) {
	if id := resolveTimezone("+05:30", ""); id != 1769 {
		t.Errorf("resolveTimezone(+05:30) = %d, want 1769", id)
	}
	if id := resolveTimezone("Local", "+05:30"); id != 1769 {
		t.Errorf("resolveTimezone(Local, +05:30) = %d, want 1769 (session offset)", id)
	}
	// Named zones keep priority over a session offset.
	if id := resolveTimezone("America/New_York", "+05:30"); id != 65361 {
		t.Errorf("resolveTimezone(America/New_York, +05:30) = %d, want 65361", id)
	}
	if id := resolveTimezone("Europe/Moscow", ""); id != 65064 {
		t.Errorf("resolveTimezone(Europe/Moscow) = %d, want 65064", id)
	}
}

// A TIMESTAMP WITH TIME ZONE bound from a fixed-offset Go zone must carry the
// displacement id on the wire — the server stores the zone, not UTC.
func TestEncodeTimestampTzFixedOffset(t *testing.T) {
	z := time.FixedZone("+05:30", 5*3600+30*60)
	_, payload := _timestampToBlr(time.Date(2026, 7, 15, 12, 34, 56, 0, z), PROTOCOL_VERSION16, "")
	if got := int(bytes_to_buint16(payload[10:12])); got != 1769 {
		t.Errorf("wire zone id = %d, want 1769 (+05:30 displacement)", got)
	}
}

// Decoding a displacement id must produce a fixed zone with the offset and its
// Firebird name, not UTC.
func TestParseTimezoneIDDisplacement(t *testing.T) {
	raw := []byte{0x06, 0xE9} // 1769 = +05:30
	loc := _parseTimezoneID(raw)
	if got := loc.String(); got != "+05:30" {
		t.Errorf("zone name = %q, want +05:30", got)
	}
	_, offset := time.Now().In(loc).Zone()
	if offset != 5*3600+30*60 {
		t.Errorf("zone offset = %d, want %d", offset, 5*3600+30*60)
	}

	loc = _parseTimezoneID([]byte{0x03, 0xBF}) // 959 = -08:00
	if got := loc.String(); got != "-08:00" {
		t.Errorf("zone name = %q, want -08:00", got)
	}
}

// Full encode/decode round trip: the zone survives both directions.
func TestTimestampTzFixedOffsetWireRoundTrip(t *testing.T) {
	z := time.FixedZone("+05:30", 5*3600+30*60)
	in := time.Date(2026, 7, 15, 12, 34, 56, 789000000, z)

	_, payload := _timestampToBlr(in, PROTOCOL_VERSION16, "")
	out := (&xSQLVAR{}).parseTimestampTz(payload)

	if !out.Equal(in) {
		t.Errorf("round trip = %v, want %v", out.Format(time.RFC3339Nano), in.Format(time.RFC3339Nano))
	}
	if out.Location().String() != "+05:30" {
		t.Errorf("round trip zone = %q, want +05:30", out.Location().String())
	}
	if out.Nanosecond() != 789000000 {
		t.Errorf("round trip nanoseconds = %d, want 789000000", out.Nanosecond())
	}
}

// A TIME WITH TIME ZONE in a fixed-offset zone round trips the same way.
func TestTimeTzFixedOffsetWireRoundTrip(t *testing.T) {
	z := time.FixedZone("-08:00", -8*3600)
	in := time.Date(2026, 7, 15, 9, 15, 0, 0, z)

	_, payload := _timeToBlr(in, PROTOCOL_VERSION16, "")
	out := (&xSQLVAR{}).parseTimeTz(payload)

	if out.Hour() != 9 || out.Minute() != 15 {
		t.Errorf("round trip wall clock = %02d:%02d, want 09:15", out.Hour(), out.Minute())
	}
	if _, offset := out.Zone(); offset != -8*3600 {
		t.Errorf("round trip offset = %d, want %d", offset, -8*3600)
	}
}

// Session timezone given as an offset string resolves plain (date-bearing)
// columns to that fixed zone instead of silently decoding as UTC.
func TestParseTimestampOffsetSessionTimezone(t *testing.T) {
	year, month, day := 2026, 7, 15
	raw := append(_convert_date(time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)), _convert_time(time.Date(0, 1, 1, 10, 0, 0, 0, time.UTC))...)
	out := (&xSQLVAR{}).parseTimestamp(raw, "+05:30")
	if out.Hour() != 10 || out.Location().String() != "+05:30" {
		t.Errorf("parseTimestamp with +05:30 session zone = %v (%v), want 10:00 +05:30", out.Format("15:04"), out.Location().String())
	}
}
