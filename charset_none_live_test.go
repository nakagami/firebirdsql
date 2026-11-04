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
// without DEFAULT CHARACTER SET, and OCTETS).
//
// Firebird 6 sizes and validates a charset-less blr_text/blr_varying output
// item in the connection charset: a VARCHAR(40) CHARACTER SET NONE described
// with sqllen 40 and declared as blr_varying(40) held only 40/4 = 10 characters
// under a UTF8 connection, so a longer value failed with
//
//	arithmetic exception, numeric overflow, or string truncation
//	string right truncation
//	expected length 10, actual 15
//
// Declaring the described charset with blr_text2/blr_varying2 (as fbclient
// does) tells the server not to size or validate the item in the connection
// charset. These tests pass on all versions; before the fix the UTF8 cases
// failed on Firebird 6.

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
				// columns after a NONE column must still be read from the right offset
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

// TestCharsetNoneColumnMetadata checks that ColumnTypes reports the described length.
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

// TestCharsetNoneLongColumn covers long NONE columns (including VARCHAR(30000)
// over UNICODE_FSS and full 20000-byte values over UTF8).
func TestCharsetNoneLongColumn(t *testing.T) {
	_, dsn, _ := createTestDatabaseWithDDL(t, "cs_none_long_",
		`CREATE TABLE CS_NONE_LONG (V20K VARCHAR(20000) CHARACTER SET NONE, V30K VARCHAR(30000) CHARACTER SET NONE)`)
	db := openTestDatabase(t, dsn+"?charset=UTF8")
	ctx := context.Background()

	value := strings.Repeat("0123456789", 2000) // full 20000 bytes
	mustExec(t, ctx, db, "INSERT INTO CS_NONE_LONG (V20K, V30K) VALUES (?, 'abc')", value)
	var got string
	require.NoError(t, db.QueryRowContext(ctx, "SELECT V20K FROM CS_NONE_LONG").Scan(&got))
	require.Equal(t, value, got)

	dbFSS := openTestDatabase(t, dsn+"?charset=UNICODE_FSS")
	require.NoError(t, dbFSS.QueryRowContext(ctx, "SELECT V30K FROM CS_NONE_LONG").Scan(&got))
	require.Equal(t, "abc", got)
}

// TestCharsetNoneWideRow fetches a row of six VARCHAR(8000) NONE columns in one row.
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

// TestCharsetOctetsAndNonUTF8None verifies that OCTETS columns (including binary
// UUIDs and values longer than sqllen/4) and NONE columns holding non-UTF8 bytes
// (such as Latin-1 x'E9') are fetched intact over a UTF8 connection.
func TestCharsetOctetsAndNonUTF8None(t *testing.T) {
	_, dsn, _ := createTestDatabaseWithDDL(t, "cs_octets_none_",
		`CREATE TABLE CS_OCTETS_NONE (
			V_OCT VARCHAR(40) CHARACTER SET OCTETS,
			C_OCT CHAR(16) CHARACTER SET OCTETS,
			V_NONE VARCHAR(10) CHARACTER SET NONE)`)
	db := openTestDatabase(t, dsn+"?charset=UTF8")
	ctx := context.Background()

	mustExec(t, ctx, db,
		"INSERT INTO CS_OCTETS_NONE (V_OCT, C_OCT, V_NONE) VALUES ('science-fiction', x'0102030405060708090A0B0C0D0E0F10', x'636166E9')")

	var vOct, cOct []byte
	var vNone string
	require.NoError(t, db.QueryRowContext(ctx,
		"SELECT V_OCT, C_OCT, V_NONE FROM CS_OCTETS_NONE").Scan(&vOct, &cOct, &vNone))
	require.Equal(t, []byte("science-fiction"), vOct)
	require.Equal(t, []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}, cOct)
	require.Equal(t, "caf\xe9", vNone)
}
