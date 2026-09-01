/*******************************************************************************
The MIT License (MIT)

Copyright (c) 2019 Hajime Nakagami

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

import "testing"

func TestTimezoneRoundTrip(t *testing.T) {
	names := []string{
		"GMT",
		"UTC",
		"Africa/Johannesburg",
		"America/Argentina/Buenos_Aires",
		"America/Mexico_City",
		"America/New_York",
		"America/Sao_Paulo",
		"America/Toronto",
		"Asia/Jakarta",
		"Asia/Kolkata",
		"Asia/Riyadh",
		"Asia/Seoul",
		"Asia/Shanghai",
		"Asia/Tokyo",
		"Australia/Sydney",
		"Europe/Berlin",
		"Europe/Istanbul",
		"Europe/London",
		"Europe/Moscow",
		"Europe/Paris",
		"Europe/Rome",
	}
	for _, name := range names {
		id := getTimezoneIDByName(name)
		if id == 0 {
			t.Errorf("getTimezoneIDByName(%q) = 0, want non-zero", name)
			continue
		}
		got := getTimezoneNameByID(int(id))
		if got != name {
			t.Errorf("getTimezoneNameByID(getTimezoneIDByName(%q)) = %q, want %q", name, got, name)
		}
	}
}

// TestTimezoneLegacyAliases mirrors Jaybird's TimeZoneByNameMappingTest legacy
// alias coverage: the 3-letter ICU aliases live at the very top of the id
// space and must round-trip.
func TestTimezoneLegacyAliases(t *testing.T) {
	for _, name := range []string{"GMT", "ACT", "AET", "AGT", "ART", "AST"} {
		id := getTimezoneIDByName(name)
		if id == 0 {
			t.Errorf("getTimezoneIDByName(%q) = 0, want non-zero", name)
			continue
		}
		if got := getTimezoneNameByID(int(id)); got != name {
			t.Errorf("getTimezoneNameByID(%d) = %q, want %q", id, got, name)
		}
	}
	if id := getTimezoneIDByName("GMT"); id != 65535 {
		t.Errorf("GMT id = %d, want 65535 (top of the Firebird id space)", id)
	}
}

// TestTimezoneMapInvalidEntries mirrors Jaybird's TimeZoneMappingTest edge
// cases: unknown names and out-of-range ids must yield zero values, never a
// wrong zone and never a panic.
func TestTimezoneMapInvalidEntries(t *testing.T) {
	for _, name := range []string{"", "Not/AZone", "America/", "gmt", "UTC "} {
		if id := getTimezoneIDByName(name); id != 0 {
			t.Errorf("getTimezoneIDByName(%q) = %d, want 0", name, id)
		}
	}
	for _, id := range []int{-1, 0, 1, 65536, 1 << 30} {
		if name := getTimezoneNameByID(id); name != "" {
			t.Errorf("getTimezoneNameByID(%d) = %q, want empty", id, name)
		}
	}
}
