package firebirdsql

import (
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Firebird GMT-displacement zones. Fixed UTC offsets are encoded as zone ids
// (offset minutes + 1439) in the two directions. Go fixed zones
// (time.FixedZone, "+05:30") round trip and do not become UTC.
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
		{"negative with minutes", "-03:30", -(3*3600 + 30*60), true},
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
		// The inputs below passed through the earlier versions of the parser.
		// strconv.Atoi is not strict after the removal of the colons. Short
		// inputs produced a slice past the end. The result was a panic, not
		// only a wrong offset.
		{"truncated +5:", "+5:", 0, false},
		{"truncated +0:", "+0:", 0, false},
		{"colon first +:5", "+:5", 0, false},
		{"double sign +-5", "+-5", 0, false},
		{"three digits +053", "+053", 0, false},
		{"signed minutes +05:-5", "+05:-5", 0, false},
		{"one digit +5", "+5", 0, false},
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

// FuzzParseUTCOffset checks the parser rules with changed inputs. An
// accepted offset is a whole number of minutes in the wire range ±23:59.
// No input gives a panic. Run with -fuzz=FuzzParseUTCOffset for inputs
// beyond the seed corpus.
func FuzzParseUTCOffset(f *testing.F) {
	for _, seed := range []string{"+05:30", "-08:00", "+0530", "+05", "+00:00", "+5:", "+-5", "+053", "+05:-5", "+:5", "", "UTC", "+25:00", "-14:00"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, s string) {
		secs, ok := parseUTCOffset(s)
		if !ok {
			return
		}
		if secs < -(23*3600+59*60) || secs > 23*3600+59*60 {
			t.Errorf("parseUTCOffset(%q) = %d: outside the ±23:59 wire range", s, secs)
		}
		if secs%60 != 0 {
			t.Errorf("parseUTCOffset(%q) = %d: not a whole number of minutes", s, secs)
		}
	})
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
		{"-03:30", -(3*3600 + 30*60), 1229, true},
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

// The driver stores only the offsets that Firebird can read back as text:
// whole minutes in -14:00..+14:00, the syntax of its literals. A wider
// offset decodes without a problem (the wire id can hold ±23:59), but the
// driver must not encode it.
func TestEncodableOffsetID(t *testing.T) {
	cases := []struct {
		name    string
		seconds int
		want    int32
		ok      bool
	}{
		{"+05:30", 5*3600 + 30*60, 1769, true},
		{"-03:30", -(3*3600 + 30*60), 1229, true},
		{"edge +14:00", 14 * 3600, 2279, true},
		{"edge -14:00", -14 * 3600, 599, true},
		{"past edge +14:01", 14*3600 + 60, 0, false},
		{"past edge -14:01", -(14*3600 + 60), 0, false},
		{"wire-only +23:59", 23*3600 + 59*60, 0, false},
		{"sub-minute +05:30:30", 5*3600 + 30*60 + 30, 0, false},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := encodableOffsetID(tt.seconds)
			if ok != tt.ok || (ok && got != tt.want) {
				t.Errorf("encodableOffsetID(%d) = %d, %v; want %d, %v", tt.seconds, got, ok, tt.want, tt.ok)
			}
		})
	}
}

// resolveTimezone gives a Go fixed zone the displacement id. It does not
// fall back to UTC.
func TestResolveTimezoneFixedOffset(t *testing.T) {
	if id := resolveTimezone("+05:30", ""); id != 1769 {
		t.Errorf("resolveTimezone(+05:30) = %d, want 1769", id)
	}
	if id := resolveTimezone("Local", "+05:30"); id != 1769 {
		t.Errorf("resolveTimezone(Local, +05:30) = %d, want 1769 (session offset)", id)
	}
	// A named zone comes before a session offset.
	if id := resolveTimezone("America/New_York", "+05:30"); id != 65361 {
		t.Errorf("resolveTimezone(America/New_York, +05:30) = %d, want 65361", id)
	}
	if id := resolveTimezone("Europe/Moscow", ""); id != 65064 {
		t.Errorf("resolveTimezone(Europe/Moscow) = %d, want 65064", id)
	}
	// Offsets beyond ±14:00 do not get a displacement id. The server can not
	// read them back as text. The result is the session zone, or UTC.
	if id := resolveTimezone("+15:00", ""); id != getTimezoneIDByName("UTC") {
		t.Errorf("resolveTimezone(+15:00) = %d, want UTC", id)
	}
	if id := resolveTimezone("Local", "+15:00"); id != getTimezoneIDByName("UTC") {
		t.Errorf("resolveTimezone(Local, +15:00) = %d, want UTC", id)
	}
}

// resolveTimezoneFor gives the value the label of its own zone. A named
// IANA zone comes first. The offset of the value comes next (time.Parse
// leaves the zone of an RFC 3339 value unnamed). The session default comes
// last.
func TestResolveTimezoneFor(t *testing.T) {
	parsed, err := time.Parse(time.RFC3339, "2026-07-15T12:00:00+05:30")
	if err != nil {
		t.Fatal(err)
	}
	if name := parsed.Location().String(); name != "" {
		t.Fatalf("premise: time.Parse leaves the zone unnamed, got %q", name)
	}
	if id := resolveTimezoneFor(parsed, ""); id != 1769 {
		t.Errorf("resolveTimezoneFor(RFC3339 +05:30, no session zone) = %d, want 1769", id)
	}
	if id := resolveTimezoneFor(parsed, "America/New_York"); id != 1769 {
		t.Errorf("resolveTimezoneFor(RFC3339 +05:30, session America/New_York) = %d, want 1769 (the value's own offset wins)", id)
	}

	wide, err := time.Parse(time.RFC3339, "2026-07-15T12:00:00+17:00")
	if err != nil {
		t.Fatal(err)
	}
	if id := resolveTimezoneFor(wide, "America/New_York"); id != 65361 {
		t.Errorf("resolveTimezoneFor(RFC3339 +17:00, session America/New_York) = %d, want 65361 (outside ±14:00 falls back to the session zone)", id)
	}

	// time.Local does not go through the offset step. The zone of the host
	// is not a choice of the application. The session default, or UTC,
	// applies.
	if id := resolveTimezoneFor(time.Now().In(time.Local), "+05:30"); id != 1769 {
		t.Errorf("resolveTimezoneFor(time.Local, +05:30) = %d, want 1769 (session zone)", id)
	}
	if id := resolveTimezoneFor(time.Now().In(time.Local), ""); id != getTimezoneIDByName("UTC") {
		t.Errorf("resolveTimezoneFor(time.Local, no session zone) = %d, want UTC", id)
	}

	// A named IANA zone still resolves by name. This is also true in a DST
	// period, when the current offset of the zone would also be encodable.
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skipf("IANA tzdata unavailable: %v", err)
	}
	if id := resolveTimezoneFor(time.Date(2026, 7, 15, 12, 0, 0, 0, ny), ""); id != 65361 {
		t.Errorf("resolveTimezoneFor(America/New_York) = %d, want 65361 (name wins over the -04:00 offset)", id)
	}
}

// A TIMESTAMP WITH TIME ZONE value from a fixed-offset Go zone must carry
// the displacement id on the wire. The server stores the zone, not UTC.
func TestEncodeTimestampTzFixedOffset(t *testing.T) {
	z := time.FixedZone("+05:30", 5*3600+30*60)
	_, payload := _timestampToBlr(time.Date(2026, 7, 15, 12, 34, 56, 0, z), PROTOCOL_VERSION16, "")
	if got := int(bytes_to_buint16(payload[10:12])); got != 1769 {
		t.Errorf("wire zone id = %d, want 1769 (+05:30 displacement)", got)
	}
}

// A fixed zone with an empty name (the result of time.Parse) must also go
// on the wire as its displacement id. It must not become UTC.
func TestEncodeTimestampTzUnnamedFixedZone(t *testing.T) {
	parsed, err := time.Parse(time.RFC3339, "2026-07-15T12:34:56-03:30")
	if err != nil {
		t.Fatal(err)
	}
	_, payload := _timestampToBlr(parsed, PROTOCOL_VERSION16, "")
	if got := int(bytes_to_buint16(payload[10:12])); got != 1229 {
		t.Errorf("wire zone id = %d, want 1229 (-03:30 displacement)", got)
	}
}

// A displacement id decodes to a fixed zone with the offset and the
// Firebird name, not to UTC. The full wire range decodes. The first id
// past the range is a named-zone id. It gives UTC when the map has no
// match.
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

	loc = _parseTimezoneID([]byte{0x04, 0xCD}) // 1229 = -03:30
	_, offset = time.Now().In(loc).Zone()
	if loc.String() != "-03:30" || offset != -(3*3600+30*60) {
		t.Errorf("zone = %q, offset %d; want -03:30, %d", loc.String(), offset, -(3*3600 + 30*60))
	}

	loc = _parseTimezoneID([]byte{0x0B, 0x3E}) // 2878 = +23:59, last displacement id
	_, offset = time.Now().In(loc).Zone()
	if loc.String() != "+23:59" || offset != 23*3600+59*60 {
		t.Errorf("zone = %q, offset %d; want +23:59, %d", loc.String(), offset, 23*3600+59*60)
	}

	loc = _parseTimezoneID([]byte{0x0B, 0x3F}) // 2879: first id past the displacement range
	if got := loc.String(); got != "UTC" {
		t.Errorf("zone = %q, want UTC (no displacement past ±23:59)", got)
	}
}

// An encode/decode round trip through the wire bytes. The zone stays
// correct in the two directions.
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

	// A negative offset with minutes. An error in the sign of the minutes
	// must show in the test result.
	z = time.FixedZone("-03:30", -(3*3600 + 30*60))
	in = time.Date(2026, 7, 15, 9, 15, 0, 0, z)
	_, payload = _timestampToBlr(in, PROTOCOL_VERSION16, "")
	out = (&xSQLVAR{}).parseTimestampTz(payload)
	if !out.Equal(in) || out.Location().String() != "-03:30" {
		t.Errorf("round trip = %v (%v), want %v (-03:30)", out.Format(time.RFC3339Nano), out.Location().String(), in.Format(time.RFC3339Nano))
	}
}

// A TIME WITH TIME ZONE value in a fixed-offset zone does the same round
// trip.
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

	z = time.FixedZone("-03:30", -(3*3600 + 30*60))
	in = time.Date(2026, 7, 15, 9, 15, 0, 0, z)
	_, payload = _timeToBlr(in, PROTOCOL_VERSION16, "")
	out = (&xSQLVAR{}).parseTimeTz(payload)
	if out.Hour() != 9 || out.Minute() != 15 {
		t.Errorf("round trip wall clock = %02d:%02d, want 09:15", out.Hour(), out.Minute())
	}
	if _, offset := out.Zone(); offset != -(3*3600 + 30*60) {
		t.Errorf("round trip offset = %d, want %d", offset, -(3*3600 + 30*60))
	}
}

// A session timezone as an offset string gives plain columns (with a date)
// that fixed zone. The columns do not silently decode as UTC.
func TestParseTimestampOffsetSessionTimezone(t *testing.T) {
	year, month, day := 2026, 7, 15
	raw := append(_convert_date(time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)), _convert_time(time.Date(0, 1, 1, 10, 0, 0, 0, time.UTC))...)
	out := (&xSQLVAR{}).parseTimestamp(raw, "+05:30")
	if out.Hour() != 10 || out.Location().String() != "+05:30" {
		t.Errorf("parseTimestamp with +05:30 session zone = %v (%v), want 10:00 +05:30", out.Format("15:04"), out.Location().String())
	}
}

// An unknown session zone must not give nil. time.Date panics on a nil
// location, and the panic stops the statement in progress. Unknown names
// give UTC. Offset names get the spelling of the Firebird zone names.
func TestLocationByNameNeverNil(t *testing.T) {
	for name, wantName := range map[string]string{
		"America/New_York": "America/New_York",
		"+05:30":           "+05:30",
		"+0530":            "+05:30", // the Firebird spelling
		"No/Such/Zone":     "UTC",
		"+5:":              "UTC",
		"":                 "UTC",
	} {
		loc := locationByName(name)
		if loc == nil {
			t.Errorf("locationByName(%q) = nil, must never be nil", name)
			continue
		}
		if loc.String() != wantName {
			t.Errorf("locationByName(%q) = %q, want %q", name, loc.String(), wantName)
		}
	}
	if _, offset := time.Now().In(locationByName("+0530")).Zone(); offset != 5*3600+30*60 {
		t.Errorf("locationByName(+0530) offset = %d, want %d", offset, 5*3600+30*60)
	}
}
