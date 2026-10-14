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
	"strings"
	"time"
)

// maxDisplacementOffsetMinutes bounds Firebird's GMT displacement zones: the
// wire encodes a fixed UTC offset as zone id = offset minutes + 1439, covering
// -23:59..+23:59. Ids above that are named zones; see getTimezoneNameByID.
const maxDisplacementOffsetMinutes = 1439

// offsetTimezoneID converts a fixed UTC offset in seconds to Firebird's
// GMT-displacement zone id (offset minutes + 1439). ok=false when the offset
// falls outside the displacement range.
func offsetTimezoneID(offsetSeconds int) (int32, bool) {
	minutes := offsetSeconds / 60
	if minutes < -maxDisplacementOffsetMinutes || minutes > maxDisplacementOffsetMinutes {
		return 0, false
	}
	return int32(minutes + maxDisplacementOffsetMinutes), true
}

// parseUTCOffset parses a fixed UTC offset in the shapes Go uses for fixed
// zones ("+05:30", "-08:00", "+0530", "+05") into seconds. Anything else is
// not an offset: ok=false.
func parseUTCOffset(name string) (int, bool) {
	if len(name) < 3 || (name[0] != '+' && name[0] != '-') {
		return 0, false
	}
	sign := 1
	if name[0] == '-' {
		sign = -1
	}
	rest := strings.ReplaceAll(name[1:], ":", "")
	if len(rest) > 4 {
		return 0, false
	}
	hours, err := strconv.Atoi(rest[:2])
	if err != nil {
		return 0, false
	}
	minutes := 0
	if len(rest) > 2 {
		if minutes, err = strconv.Atoi(rest[2:]); err != nil {
			return 0, false
		}
	}
	if hours > 23 || minutes > 59 {
		return 0, false
	}
	return sign * (hours*3600 + minutes*60), true
}

// formatUTCOffset renders a fixed offset the way Firebird names displacement
// zones ("+05:30", "-08:00").
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
// displacement zone id (offset minutes + 1439): a fixed zone carrying the
// offset under Firebird's own zone name.
func offsetTimezoneLocation(id int) *time.Location {
	offsetSeconds := (id - maxDisplacementOffsetMinutes) * 60
	return time.FixedZone(formatUTCOffset(offsetSeconds), offsetSeconds)
}

// resolveTimezone returns the Firebird timezone ID for locName.
// Resolution order:
//  1. locName is directly in the map — return its ID
//  2. locName is a fixed UTC offset ("+05:30", as Go FixedZone names it) —
//     return the GMT-displacement zone ID
//  3. sessionTimezone is non-empty and in the map (or itself an offset) —
//     return its ID (covers "Local" when DSN ?timezone= is set)
//  4. Fall back to UTC (the instant is always preserved since values are encoded in UTC)
func resolveTimezone(locName, sessionTimezone string) int32 {
	if id := getTimezoneIDByName(locName); id != 0 {
		return id
	}
	if secs, ok := parseUTCOffset(locName); ok {
		if id, ok := offsetTimezoneID(secs); ok {
			return id
		}
	}
	if sessionTimezone != "" {
		if id := getTimezoneIDByName(sessionTimezone); id != 0 {
			return id
		}
		if secs, ok := parseUTCOffset(sessionTimezone); ok {
			if id, ok := offsetTimezoneID(secs); ok {
				return id
			}
		}
	}
	return getTimezoneIDByName("UTC")
}
