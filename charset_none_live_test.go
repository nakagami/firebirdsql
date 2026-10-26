/*******************************************************************************
The MIT License (MIT)

Copyright (c) 2026 Hajime Nakagami

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
	"context"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Live tests for text columns the server does not transliterate to the
// connection charset (CHARACTER SET NONE, the default in a database created
// without DEFAULT CHARACTER SET).
//
// Firebird 6 sizes the output message of such a column in characters of the
// connection charset: a VARCHAR(40) CHARACTER SET NONE described with
// sqllen 40 and declared as blr_varying(40) holds only 40/4 = 10 characters
// under a UTF8 connection, so a longer value failed with
//
//	arithmetic exception, numeric overflow, or string truncation
//	string right truncation
//	expected length 10, actual 15
//
// Firebird 2.5–5.0 treat the same buffer as 40 bytes. These tests pass on all
// versions; before the fix the UTF8 cases failed on Firebird 6.

// TestCharsetNoneColumnsFetch fetches NONE columns at and below their declared
// length over every kind of connection charset.
func TestCharsetNoneColumnsFetch(t *testing.T) {
	full40 := strings.Repeat("x", 40)
	for _, charset := range []string{"UTF8", "UNICODE_FSS", "SJIS_0208", "WIN1252"} {
		t.Run(charset, func(t *testing.T) {
			_, dsn, _ := createTestDatabaseWithDDL(t, "cs_none_fetch_",
				`CREATE TABLE CS_NONE (
					ID  INTEGER NOT NULL PRIMARY KEY,
					V40 VARCHAR(40) CHARACTER SET NONE,
					C11 CHAR(11) CHARACTER SET NONE,
					U20 VARCHAR(20) CHARACTER SET UTF8,
					N   INTEGER)`)
			db := openTestDatabase(t, dsn+"?charset="+charset)
			ctx := context.Background()

			rows := []struct {
				id  int
				v40 string
				c11 string
			}{
				{1, "science-fiction", "PRIMARY KEY"}, // 15 > 40/4 and 11 > 11/4
				{2, full40, "UNIQUE"},                 // full length; CHAR padding
				{3, "", ""},                           // empty
				{4, "abc", "x"},                       // short
			}
			for _, r := range rows {
				// literals: the input path is not what this test is about
				mustExec(t, ctx, db, "INSERT INTO CS_NONE (ID, V40, C11, U20, N) VALUES ("+
					strconv.Itoa(r.id)+", '"+r.v40+"', '"+r.c11+"', 'tail', "+strconv.Itoa(r.id*10)+")")
			}

			got, err := db.QueryContext(ctx, "SELECT ID, V40, C11, U20, N FROM CS_NONE ORDER BY ID")
			require.NoError(t, err)
			defer got.Close()
			i := 0
			for got.Next() {
				var id, n int
				var v40, c11, u20 string
				require.NoError(t, got.Scan(&id, &v40, &c11, &u20, &n))
				want := rows[i]
				require.Equal(t, want.id, id)
				require.Equal(t, want.v40, v40)
				require.Equal(t, want.c11, c11, "CHAR is returned with trailing blanks trimmed")
				// columns after a widened one must still be read from the right offset
				require.Equal(t, "tail", u20)
				require.Equal(t, want.id*10, n)
				i++
			}
			require.NoError(t, got.Err())
			require.Equal(t, len(rows), i)

			// a NONE value used as a parameter (input message) keeps working
			var id int
			require.NoError(t, db.QueryRowContext(ctx,
				"SELECT ID FROM CS_NONE WHERE V40 = ?", "science-fiction").Scan(&id))
			require.Equal(t, 1, id)
		})
	}
}

// TestCharsetNoneColumnMetadata checks that the widened message buffer does not
// leak into column metadata: ColumnTypes still reports the described length.
func TestCharsetNoneColumnMetadata(t *testing.T) {
	_, dsn, _ := createTestDatabaseWithDDL(t, "cs_none_meta_",
		`CREATE TABLE CS_NONE_META (V40 VARCHAR(40) CHARACTER SET NONE, C11 CHAR(11) CHARACTER SET NONE)`)
	db := openTestDatabase(t, dsn+"?charset=UTF8")

	rows, err := db.Query("SELECT V40, C11 FROM CS_NONE_META")
	require.NoError(t, err)
	defer rows.Close()
	cts, err := rows.ColumnTypes()
	require.NoError(t, err)
	l0, ok := cts[0].Length()
	require.True(t, ok)
	l1, ok := cts[1].Length()
	require.True(t, ok)
	require.Equal(t, int64(40), l0)
	require.Equal(t, int64(11), l1)
}

// TestCharsetNoneSystemTables reads RDB$RELATION_CONSTRAINTS.RDB$CONSTRAINT_TYPE,
// a CHAR(11) CHARACTER SET NONE system column: on Firebird 6 over UTF8 it
// failed with "expected length 2, actual 11" for every PRIMARY KEY/FOREIGN KEY
// row, which breaks any schema introspection.
func TestCharsetNoneSystemTables(t *testing.T) {
	_, dsn, _ := createTestDatabaseWithDDL(t, "cs_none_sys_",
		`CREATE TABLE CS_NONE_PARENT (ID INTEGER NOT NULL PRIMARY KEY)`,
		`CREATE TABLE CS_NONE_CHILD (ID INTEGER NOT NULL PRIMARY KEY,
			PARENT_ID INTEGER REFERENCES CS_NONE_PARENT (ID))`)
	db := openTestDatabase(t, dsn+"?charset=UTF8")

	rows, err := db.Query(`SELECT TRIM(RDB$RELATION_NAME), RDB$CONSTRAINT_TYPE
		FROM RDB$RELATION_CONSTRAINTS
		WHERE RDB$RELATION_NAME IN ('CS_NONE_PARENT', 'CS_NONE_CHILD')
		  AND RDB$CONSTRAINT_TYPE <> 'NOT NULL'
		ORDER BY 1, 2`)
	require.NoError(t, err)
	defer rows.Close()
	var got []string
	for rows.Next() {
		var rel, typ string
		require.NoError(t, rows.Scan(&rel, &typ))
		got = append(got, rel+" "+typ)
	}
	require.NoError(t, rows.Err())
	require.Equal(t, []string{
		"CS_NONE_CHILD FOREIGN KEY",
		"CS_NONE_CHILD PRIMARY KEY",
		"CS_NONE_PARENT PRIMARY KEY",
	}, got)
}

// TestCharsetNoneLongColumn covers a column whose widened buffer would not fit
// the 16-bit BLR length: VARCHAR(20000) NONE × 4 > 65535. The buffer is capped
// and a value that fits the cap is still returned intact.
func TestCharsetNoneLongColumn(t *testing.T) {
	_, dsn, _ := createTestDatabaseWithDDL(t, "cs_none_long_",
		`CREATE TABLE CS_NONE_LONG (V VARCHAR(20000) CHARACTER SET NONE)`)
	db := openTestDatabase(t, dsn+"?charset=UTF8")
	ctx := context.Background()

	value := strings.Repeat("0123456789", 1500) // 15000 bytes: fits 65532/4
	mustExec(t, ctx, db, "INSERT INTO CS_NONE_LONG (V) VALUES (?)", value)
	var got string
	require.NoError(t, db.QueryRowContext(ctx, "SELECT V FROM CS_NONE_LONG").Scan(&got))
	require.Equal(t, value, got)
}

// TestCharsetNoneWideRow fetches a row of six VARCHAR(8000) NONE columns: their
// widened buffers add up to a 192 KB message under UTF8, which Firebird 3+
// accepts. (Firebird 2.5 would reject it with "SQLDA error", so widening is
// applied on protocol 13+ only.)
func TestCharsetNoneWideRow(t *testing.T) {
	cols := make([]string, 6)
	for i := range cols {
		cols[i] = "V" + strconv.Itoa(i) + " VARCHAR(8000) CHARACTER SET NONE"
	}
	_, dsn, _ := createTestDatabaseWithDDL(t, "cs_none_wide_",
		"CREATE TABLE CS_NONE_WIDE ("+strings.Join(cols, ", ")+")")
	db := openTestDatabase(t, dsn+"?charset=UTF8")
	ctx := context.Background()

	v := strings.Repeat("a", 8000)
	mustExec(t, ctx, db, "INSERT INTO CS_NONE_WIDE VALUES (?, ?, ?, ?, ?, ?)", v, v, v, v, v, v)
	got := make([]string, 6)
	dest := make([]any, 6)
	for i := range got {
		dest[i] = &got[i]
	}
	require.NoError(t, db.QueryRowContext(ctx, "SELECT * FROM CS_NONE_WIDE").Scan(dest...))
	for i := range got {
		require.Equal(t, v, got[i], "column %d", i)
	}
}
