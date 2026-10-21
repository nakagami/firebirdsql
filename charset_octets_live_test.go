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
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// Binary data: CHARACTER SET OCTETS columns and []byte parameters.
//
// Firebird 6 treats a text item declared without a charset (blr_text,
// blr_varying) as text in the connection charset, and validates it. Bytes
// that are not valid UTF8 then fail with "Malformed string", both when an
// OCTETS column is read and when a []byte parameter is sent (a []byte below
// 32 KB goes in the message as text, larger ones as a BLOB). Firebird 2.5 to
// 5.0 copy the bytes as they are.
//
// The driver now declares OCTETS columns and []byte parameters with
// CHARACTER SET OCTETS (blr_text2 / blr_varying2), as Jaybird does, which
// every version accepts.

// bin holds bytes that are not valid UTF8, with a NUL and a 0xFF.
var bin = []byte{0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, 0x00, 0xFF, 0xFE, 0xC3}

// TestOctetsColumns reads and filters CHAR/VARCHAR CHARACTER SET OCTETS
// columns over UTF8 and single-byte connections.
func TestOctetsColumns(t *testing.T) {
	for _, charset := range []string{"UTF8", "WIN1252"} {
		t.Run(charset, func(t *testing.T) {
			_, dsn, _ := createTestDatabaseWithDDL(t, "octets_cols_",
				`CREATE TABLE OCT (
					ID  INTEGER NOT NULL PRIMARY KEY,
					V20 VARCHAR(20) CHARACTER SET OCTETS,
					C12 CHAR(12) CHARACTER SET OCTETS,
					U20 VARCHAR(20) CHARACTER SET UTF8)`,
				`INSERT INTO OCT VALUES (1, x'89504E470D0A1A0A00FFFEC3', x'89504E470D0A1A0A00FFFEC3', 'tail')`,
				`INSERT INTO OCT VALUES (2, x'', x'00', 'empty')`,
				`INSERT INTO OCT VALUES (3, NULL, NULL, 'null')`)
			db := openTestDatabase(t, dsn+"?charset="+charset)
			ctx := context.Background()

			rows, err := db.QueryContext(ctx, "SELECT ID, V20, C12, U20 FROM OCT ORDER BY ID")
			require.NoError(t, err)
			defer rows.Close()
			var got []struct {
				v, c []byte
				u    string
			}
			for rows.Next() {
				var id int
				var v, c []byte
				var u string
				require.NoError(t, rows.Scan(&id, &v, &c, &u))
				got = append(got, struct {
					v, c []byte
					u    string
				}{v, c, u})
			}
			require.NoError(t, rows.Err())
			require.Len(t, got, 3)
			require.Equal(t, bin, got[0].v)
			require.Equal(t, bin, got[0].c, "CHAR OCTETS keeps every byte, including the NUL and 0xFF")
			require.Equal(t, "tail", got[0].u, "the column after an OCTETS one is read from the right offset")
			require.Empty(t, got[1].v)
			require.Equal(t, bytes.Repeat([]byte{0}, 12), got[1].c, "CHAR OCTETS is padded with NULs, which are kept")
			require.Nil(t, got[2].v)
			require.Nil(t, got[2].c)

			// []byte as a parameter: filter, insert, update
			var id int
			require.NoError(t, db.QueryRowContext(ctx, "SELECT ID FROM OCT WHERE V20 = ?", bin).Scan(&id))
			require.Equal(t, 1, id)
			mustExec(t, ctx, db, "INSERT INTO OCT (ID, V20, C12, U20) VALUES (4, ?, ?, ?)", bin, bin, "ins")
			mustExec(t, ctx, db, "UPDATE OCT SET V20 = ? WHERE ID = 2", []byte{0xFF, 0x00})
			var v, c []byte
			require.NoError(t, db.QueryRowContext(ctx, "SELECT V20, C12 FROM OCT WHERE ID = 4").Scan(&v, &c))
			require.Equal(t, bin, v)
			require.Equal(t, bin, c)
			require.NoError(t, db.QueryRowContext(ctx, "SELECT V20 FROM OCT WHERE ID = 2").Scan(&v))
			require.Equal(t, []byte{0xFF, 0x00}, v)
		})
	}
}

// TestBytesParameterToBlob sends []byte parameters of several sizes to a
// BLOB SUB_TYPE BINARY column: below MAX_CHAR_LENGTH they travel in the
// message, above it as a BLOB.
func TestBytesParameterToBlob(t *testing.T) {
	_, dsn, _ := createTestDatabaseWithDDL(t, "bytes_blob_",
		`CREATE TABLE BB (ID INTEGER NOT NULL PRIMARY KEY, B BLOB SUB_TYPE BINARY)`)
	db := openTestDatabase(t, dsn+"?charset=UTF8")
	ctx := context.Background()

	big := make([]byte, 40000)
	for i := range big {
		big[i] = byte(0x80 + i%64)
	}
	cases := map[int][]byte{1: bin, 3: {0}, 4: bytes.Repeat(bin, 2000), 5: big}
	for id, b := range cases {
		mustExec(t, ctx, db, "INSERT INTO BB (ID, B) VALUES (?, ?)", id, b)
	}
	for id, want := range cases {
		var got []byte
		require.NoError(t, db.QueryRowContext(ctx, "SELECT B FROM BB WHERE ID = ?", id).Scan(&got))
		require.Equal(t, want, got, "id %d", id)
	}
	// a []byte to a text column keeps working
	_, dsn, _ = createTestDatabaseWithDDL(t, "bytes_text_",
		`CREATE TABLE BT (T VARCHAR(20))`)
	db = openTestDatabase(t, dsn+"?charset=UTF8")
	mustExec(t, ctx, db, "INSERT INTO BT (T) VALUES (?)", []byte("héllo"))
	var s string
	require.NoError(t, db.QueryRowContext(ctx, "SELECT T FROM BT").Scan(&s))
	require.Equal(t, "héllo", s)
}
