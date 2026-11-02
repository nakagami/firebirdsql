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

import (
	"strconv"
	"time"
)

// Firebird gives a fixed UTC offset the zone id (offset minutes + 1439) on
// the wire. The range is -23:59..+23:59. Ids above this range are named
// zones. See getTimezoneNameByID.
const maxDisplacementOffsetMinutes = 1439

// The driver stores offsets in the range -14:00..+14:00 only. The offset
// syntax of Firebird accepts no other offsets. A wider offset gets a
// displacement id, but the server can not read that value back as text
// (CAST, exports, INSERT ... SELECT). Wider offsets keep the old behavior:
// the session zone, or UTC.
const maxEncodeOffsetMinutes = 14 * 60

// offsetTimezoneID converts a fixed UTC offset in seconds to the Firebird
// GMT-displacement zone id (offset minutes + 1439). ok is false if the
// offset is outside the displacement range.
func offsetTimezoneID(offsetSeconds int) (int32, bool) {
	minutes := offsetSeconds / 60
	if minutes < -maxDisplacementOffsetMinutes || minutes > maxDisplacementOffsetMinutes {
		return 0, false
	}
	return int32(minutes + maxDisplacementOffsetMinutes), true
}

// encodableOffsetID is offsetTimezoneID with the limits of the Firebird
// offset syntax: whole minutes in -14:00..+14:00. A value outside this range
// stores, but a later read as text does not give the same value. ok is false
// for these values. The caller then uses the session zone, or UTC, as before.
func encodableOffsetID(offsetSeconds int) (int32, bool) {
	if offsetSeconds%60 != 0 {
		return 0, false
	}
	if m := offsetSeconds / 60; m < -maxEncodeOffsetMinutes || m > maxEncodeOffsetMinutes {
		return 0, false
	}
	return offsetTimezoneID(offsetSeconds)
}

// parseUTCOffset parses a fixed UTC offset into seconds. The accepted shapes
// are a sign and exactly HH, or HHMM, or HH:MM, in ASCII digits: "+05:30",
// "-08:00", "+0530", "+05". Other input is not an offset. ok is false for
// other input. The rules are strict because strconv.Atoi accepts "+-5" and
// "+05:-5" after the removal of the colons. A short input must not produce a
// slice past its end.
func parseUTCOffset(name string) (int, bool) {
	if len(name) < 3 || (name[0] != '+' && name[0] != '-') {
		return 0, false
	}
	d := name[1:]
	var hh, mm string
	switch {
	case len(d) == 2:
		hh = d
	case len(d) == 4:
		hh, mm = d[:2], d[2:]
	case len(d) == 5 && d[2] == ':':
		hh, mm = d[:2], d[3:]
	default:
		return 0, false
	}
	num := func(s string) (int, bool) {
		n := 0
		for i := 0; i < len(s); i++ {
			if s[i] < '0' || s[i] > '9' {
				return 0, false
			}
			n = n*10 + int(s[i]-'0')
		}
		return n, true
	}
	hours, ok := num(hh)
	if !ok {
		return 0, false
	}
	minutes, ok := num(mm)
	if !ok || hours > 23 || minutes > 59 {
		return 0, false
	}
	sign := 1
	if name[0] == '-' {
		sign = -1
	}
	return sign * (hours*3600 + minutes*60), true
}

// formatUTCOffset writes a fixed offset in the format of the Firebird
// displacement zone names ("+05:30", "-08:00").
func formatUTCOffset(offsetSeconds int) string {
	sign := "+"
	if offsetSeconds < 0 {
		sign = "-"
		offsetSeconds = -offsetSeconds
	}
	h, m := offsetSeconds/3600, offsetSeconds%3600/60
	return sign + strconv.Itoa(h/10) + strconv.Itoa(h%10) + ":" + strconv.Itoa(m/10) + strconv.Itoa(m%10)
}

// offsetTimezoneLocation returns the Go location for a Firebird GMT
// displacement zone id (offset minutes + 1439). The location is a fixed zone
// with the offset and the Firebird zone name.
func offsetTimezoneLocation(id int) *time.Location {
	offsetSeconds := (id - maxDisplacementOffsetMinutes) * 60
	return time.FixedZone(formatUTCOffset(offsetSeconds), offsetSeconds)
}

// resolveTimezoneFor returns the Firebird timezone ID for the value t.
// The steps are:
//  1. The zone of t is in the map by name (an IANA zone). Return its ID.
//  2. The zone of t is not time.Local and has a fixed offset in the range
//     ±14:00. This is a Go FixedZone with the name "+05:30", or with an
//     empty name, as time.Parse leaves it. Return the GMT-displacement zone
//     ID. The zone of the value is known. It wins over the session default.
//  3. Other cases go to resolveTimezone by name: the session zone if it is
//     set (this covers time.Local), else UTC.
func resolveTimezoneFor(t time.Time, sessionTimezone string) int32 {
	loc := t.Location()
	name := loc.String()
	if id := getTimezoneIDByName(name); id != 0 {
		return id
	}
	if loc != time.Local {
		_, offset := t.Zone()
		if id, ok := encodableOffsetID(offset); ok {
			return id
		}
	}
	return resolveTimezone(name, sessionTimezone)
}

// resolveTimezone returns the Firebird timezone ID for the name locName.
// The steps are:
//  1. locName is in the map. Return its ID.
//  2. locName is a fixed UTC offset ("+05:30", as Go FixedZone names it).
//     Return the GMT-displacement zone ID.
//  3. sessionTimezone is not empty and is in the map, or is itself an
//     offset. Return its ID. This covers "Local" when the DSN has
//     ?timezone= set.
//  4. Return UTC. The instant always stays correct, because the driver
//     encodes the values in UTC.
func resolveTimezone(locName, sessionTimezone string) int32 {
	if id := getTimezoneIDByName(locName); id != 0 {
		return id
	}
	if secs, ok := parseUTCOffset(locName); ok {
		if id, ok := encodableOffsetID(secs); ok {
			return id
		}
	}
	if sessionTimezone != "" {
		if id := getTimezoneIDByName(sessionTimezone); id != 0 {
			return id
		}
		if secs, ok := parseUTCOffset(sessionTimezone); ok {
			if id, ok := encodableOffsetID(secs); ok {
				return id
			}
		}
	}
	return getTimezoneIDByName("UTC")
}
