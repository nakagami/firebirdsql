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

// TestParseDSNValidPaths covers the shapes that must keep parsing exactly as
// before (guarding the slicing must not change valid results).
func TestParseDSNValidPaths(t *testing.T) {
	cases := []struct {
		dsn    string
		dbName string
	}{
		{"user:password@localhost:3050/dbname", "dbname"},
		{"user:password@localhost/dbname", "dbname"},
		{"user:password@localhost:3050/C:/db.fdb", "C:/db.fdb"},
		{"user:password@localhost:3050/path/to/db.fdb", "/path/to/db.fdb"}, // multi-segment POSIX keeps the leading slash (existing behavior)
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

// TestParseDSNEmptyPathNoPanic: a DSN with an empty (or "/"-only) database
// path used to panic with "slice bounds out of range" in the unguarded
// dsn.dbName[1:] / dsn.dbName[2:] slicing (found via database/sql:
// sql.Open + Query panic before any connection attempt). It must return a
// clean error instead.
func TestParseDSNEmptyPathNoPanic(t *testing.T) {
	cases := []string{
		"user:password@localhost:3050/?charset=UTF8", // Path="/" + query
		"user:password@localhost:3050/",              // Path="/"
		"user:password@localhost:3050",               // no path at all
		"user:password@localhost",                    // bare host
		"user:password@localhost:3050//",             // slashes only
	}
	for _, dsn := range cases {
		func() {
			// parseDSN used to panic here; recover so the failure message
			// names the offending DSN instead of killing the test binary.
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("parseDSN(%q) panicked: %v", dsn, r)
				}
			}()
			_, err := parseDSN(dsn)
			if !errors.Is(err, ErrDsnDbNameUnknown) {
				t.Errorf("parseDSN(%q) error = %v, want ErrDsnDbNameUnknown", dsn, err)
			}
		}()
	}
}
