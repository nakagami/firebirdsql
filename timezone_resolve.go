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

// resolveTimezone returns the Firebird timezone ID for locName.
// Resolution order:
//  1. locName is directly in the map — return its ID
//  2. sessionTimezone is non-empty and in the map — return its ID (covers "Local" when DSN ?timezone= is set)
//  3. Fall back to UTC (the instant is always preserved since values are encoded in UTC)
func resolveTimezone(locName, sessionTimezone string) int32 {
	if id := getTimezoneIDByName(locName); id != 0 {
		return id
	}
	if sessionTimezone != "" {
		if id := getTimezoneIDByName(sessionTimezone); id != 0 {
			return id
		}
	}
	return getTimezoneIDByName("UTC")
}
