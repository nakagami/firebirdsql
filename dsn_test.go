/*******************************************************************************
The MIT License (MIT)

Copyright (c) 2013-2020 Hajime Nakagami

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
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN
AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*******************************************************************************/

package firebirdsql

import (
	"errors"
	"testing"
)

// TestParseDSNValidPaths pins the parsing results for the shapes that must
// not change (single-segment paths lose the leading slash, multi-segment
// POSIX paths keep it, Windows drive paths lose it).
func TestParseDSNValidPaths(t *testing.T) {
	cases := []struct {
		dsn    string
		dbName string
	}{
		{"user:password@localhost:3050/dbname", "dbname"},
		{"user:password@localhost/dbname", "dbname"},
		{"user:password@localhost:3050/C:/db.fdb", "C:/db.fdb"},
		{"user:password@localhost:3050/path/to/db.fdb", "/path/to/db.fdb"},
	}
	for _, c := range cases {
		dsn, err := parseDSN(c.dsn)
		if err != nil {
			t.Errorf("parseDSN(%q): %v", c.dsn, err)
			continue
		}
		if dsn.dbName != c.dbName {
			t.Errorf("parseDSN(%q).dbName = %q, want %q", c.dsn, dsn.dbName, c.dbName)
		}
	}
}

// TestParseDSNEmptyPathNoPanic: DSNs with an empty (or "/"-only) database
// path historically panicked on unguarded dsn.dbName slicing ("slice bounds
// out of range", via database/sql before any connection attempt). The
// slicing is bounds-checked now, and the remaining behavior must be a clean
// ErrDsnDbNameUnknown at parse time instead of an obscure attach-time
// failure from a "" database name.
func TestParseDSNEmptyPathNoPanic(t *testing.T) {
	cases := []string{
		"user:password@localhost:3050/?charset=UTF8", // url.Parse => Path "/"
		"user:password@localhost:3050/",              // Path "/"
		"user:password@localhost:3050",               // Path ""
		"user:password@localhost",                    // Path ""
		"user:password@localhost:3050//",             // slashes only
	}
	for _, dsn := range cases {
		_, err := parseDSN(dsn)
		if !errors.Is(err, ErrDsnDbNameUnknown) {
			t.Errorf("parseDSN(%q) error = %v, want ErrDsnDbNameUnknown", dsn, err)
		}
	}
}
