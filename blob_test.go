/*******************************************************************************
The MIT License (MIT)

Copyright (c) 2026 Alexey Kovyazin

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
	"database/sql"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Phase 5 of the Jaybird test port plan (JAYBIRD_TEST_PORT_PLAN.md):
// BLOB/CLOB lifecycle — mirroring Jaybird's FBBlobTest, FBBlobAccessTest,
// FBBlobAutocommitTest, FBBlobStreamTest, FBClobTest and InlineBlobTest.

// TestBlobSegmentBoundaries covers blob sizes around the 32K segment boundary
// and the multi-segment path (Jaybird FBBlobStreamTest segment cases).
func TestBlobSegmentBoundaries(t *testing.T) {
	db, _, _ := createTestDatabaseWithDDL(t, "blob_seg_",
		`CREATE TABLE BLOB_SEG (ID INTEGER PRIMARY KEY, DATA BLOB SUB_TYPE BINARY)`)

	sizes := []int{1, 2, 1024, 32767, 32768, 32769, 65535, 65536, 65537, 100000}
	for i, size := range sizes {
		data := randomBytes(size)
		mustExec(t, stmtCtx, db, "INSERT INTO BLOB_SEG (ID, DATA) VALUES (?, ?)", i+1, data)

		var got []byte
		require.NoError(t, db.QueryRow("SELECT DATA FROM BLOB_SEG WHERE ID = ?", i+1).Scan(&got),
			"size %d", size)
		require.True(t, bytes.Equal(data, got), "size %d: got %d bytes, want %d", size, len(got), size)
	}
}

// TestBlobEdgeCases mirrors Jaybird's FBBlobTest / FBBlobAutocommitTest edge
// cases: NULL vs empty, visibility across connections and transactions, and
// parameter reuse.
func TestBlobEdgeCases(t *testing.T) {
	db, dsn, _ := createTestDatabaseWithDDL(t, "blob_edge_",
		`CREATE TABLE BLOB_EDGE (ID INTEGER PRIMARY KEY, BIN BLOB SUB_TYPE BINARY, TXT BLOB SUB_TYPE TEXT)`)

	// Empty string/blob and NULL are distinct states for text blobs; an
	// empty []byte parameter is written as NULL (documented current
	// contract — Jaybird round-trips an empty binary blob instead).
	mustExec(t, stmtCtx, db, "INSERT INTO BLOB_EDGE (ID, BIN, TXT) VALUES (1, X'', ?)", "")
	mustExec(t, stmtCtx, db, "INSERT INTO BLOB_EDGE (ID, BIN, TXT) VALUES (2, ?, ?)", nil, nil)
	mustExec(t, stmtCtx, db, "INSERT INTO BLOB_EDGE (ID, BIN, TXT) VALUES (3, ?, ?)",
		[]byte{0xDE, 0xAD}, "text 🎉")
	mustExec(t, stmtCtx, db, "INSERT INTO BLOB_EDGE (ID, BIN) VALUES (4, ?)", []byte{})

	var bin []byte
	var txt, binTxt sql.NullString

	// Scanning blob columns into sql.NullString distinguishes SQL NULL from
	// an empty value (an empty []byte dest would alias to nil either way).
	require.NoError(t, db.QueryRow("SELECT BIN, TXT FROM BLOB_EDGE WHERE ID = 1").Scan(&binTxt, &txt))
	require.True(t, binTxt.Valid, "empty binary blob (X'') must not read back as NULL")
	require.Len(t, binTxt.String, 0)
	require.True(t, txt.Valid, "empty text blob must not read back as NULL")
	require.Equal(t, "", txt.String)

	require.NoError(t, db.QueryRow("SELECT BIN, TXT FROM BLOB_EDGE WHERE ID = 2").Scan(&bin, &txt))
	require.Nil(t, bin)
	require.False(t, txt.Valid)

	require.NoError(t, db.QueryRow("SELECT BIN, TXT FROM BLOB_EDGE WHERE ID = 3").Scan(&bin, &txt))
	require.Equal(t, []byte{0xDE, 0xAD}, bin)
	require.Equal(t, "text 🎉", txt.String)

	// An empty []byte parameter stores as SQL NULL on Firebird 5 (protocol
	// 19) and as an empty blob on Firebird 4 and older — either way it must
	// never come back as actual content.
	require.NoError(t, db.QueryRow("SELECT BIN FROM BLOB_EDGE WHERE ID = 4").Scan(&bin))
	require.True(t, bin == nil || len(bin) == 0,
		"empty []byte parameter must not produce content, got %d bytes", len(bin))

	// The same parameter can be reused for several inserts.
	data := randomBytes(5000)
	for i := 10; i < 13; i++ {
		mustExec(t, stmtCtx, db, "INSERT INTO BLOB_EDGE (ID, BIN) VALUES (?, ?)", i, data)
	}
	for i := 10; i < 13; i++ {
		require.NoError(t, db.QueryRow("SELECT BIN FROM BLOB_EDGE WHERE ID = ?", i).Scan(&bin))
		require.True(t, bytes.Equal(data, bin), "row %d mismatch", i)
	}

	// A blob inserted in a rolled-back transaction is gone; a committed one
	// is visible from a second connection.
	tx, err := db.BeginTx(stmtCtx, nil)
	require.NoError(t, err)
	mustExec(t, stmtCtx, tx, "INSERT INTO BLOB_EDGE (ID, BIN) VALUES (20, ?)", []byte("doomed"))
	require.NoError(t, tx.Rollback())
	err = db.QueryRow("SELECT BIN FROM BLOB_EDGE WHERE ID = 20").Scan(&bin)
	require.Error(t, err, "rolled-back blob row must be gone")

	c2 := openTestDatabase(t, dsn)
	require.NoError(t, c2.QueryRow("SELECT BIN FROM BLOB_EDGE WHERE ID = 3").Scan(&bin))
	require.Equal(t, []byte{0xDE, 0xAD}, bin)
}

// TestClobLargeText mirrors Jaybird's FBClobTest: large multi-segment text
// with unicode round-trips as a string.
func TestClobLargeText(t *testing.T) {
	db, _, _ := createTestDatabaseWithDDL(t, "clob_large_",
		`CREATE TABLE CLOB_LARGE (ID INTEGER PRIMARY KEY, DOC BLOB SUB_TYPE TEXT)`)

	parts := make([]string, 0, 2000)
	for i := 0; i < 2000; i++ {
		parts = append(parts, "ligne de texte numéro × δ 🎉")
	}
	doc := strings.Join(parts, "\n")
	require.Greater(t, len(doc), 65536, "text must span multiple segments")

	mustExec(t, stmtCtx, db, "INSERT INTO CLOB_LARGE (ID, DOC) VALUES (1, ?)", doc)
	var got string
	require.NoError(t, db.QueryRow("SELECT DOC FROM CLOB_LARGE WHERE ID = 1").Scan(&got))
	require.Equal(t, doc, got)
}

// TestInlineBlobTempDB generalizes the EMPLOYEE-based inline blob checks
// (Jaybird V19StatementTest) to a throwaway database: small blobs are served
// from the inline-blob cache when the DSN enables it, and everything still
// works when it is disabled.
func TestInlineBlobTempDB(t *testing.T) {
	// Inline blobs require Firebird 5 (protocol 19); gate on the server
	// version before attaching with the inline-blob DPB options, which older
	// servers may reject.
	requireServerVersion(t, 5, 0)
	dbPath, dsn, err := CreateTestDatabase("blob_inline_")
	require.NoError(t, err)
	defer func() { _ = removeDatabaseFile(dbPath) }()

	seed := func(db *sql.DB) {
		mustExec(t, stmtCtx, db, `CREATE TABLE INLINE_T (ID INTEGER PRIMARY KEY, DOC BLOB SUB_TYPE TEXT)`)
		mustExec(t, stmtCtx, db, "INSERT INTO INLINE_T (ID, DOC) VALUES (1, ?)",
			strings.Repeat("inline-doc-", 400)) // ~4.4 KB, below the inline threshold
	}

	// Inline blobs enabled.
	inlineDB := openTestDatabase(t, dsn+"?max_inline_blob_size=65536")
	seed(inlineDB)
	requireInlineBlobs(t, inlineDB)
	var got string
	require.NoError(t, inlineDB.QueryRow("SELECT DOC FROM INLINE_T WHERE ID = 1").Scan(&got))
	require.Equal(t, strings.Repeat("inline-doc-", 400), got)

	// Disabled: the same read must work through segment fetching.
	plainDB := openTestDatabase(t, dsn+"?max_inline_blob_size=0")
	require.NoError(t, plainDB.QueryRow("SELECT DOC FROM INLINE_T WHERE ID = 1").Scan(&got))
	require.Equal(t, strings.Repeat("inline-doc-", 400), got)

	// A blob larger than the inline threshold still round-trips.
	big := strings.Repeat("B", 200000)
	mustExec(t, stmtCtx, inlineDB, "INSERT INTO INLINE_T (ID, DOC) VALUES (2, ?)", big)
	require.NoError(t, inlineDB.QueryRow("SELECT DOC FROM INLINE_T WHERE ID = 2").Scan(&got))
	require.Equal(t, big, got)
}
