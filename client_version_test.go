/*******************************************************************************
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
	"database/sql"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// Tests for the client identification DPB items sent on attach
// (isc_dpb_client_version, isc_dpb_os_user, isc_dpb_host_name): they populate
// the MON$ATTACHMENTS monitoring table (MON$CLIENT_VERSION on Firebird 3+).

// monClientVersionColumnExists reports whether MON$ATTACHMENTS has a
// MON$CLIENT_VERSION column (Firebird 3+; Firebird 2.5 does not).
func monClientVersionColumnExists(t *testing.T, db *sql.DB) bool {
	t.Helper()
	var n int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM RDB$RELATION_FIELDS
		WHERE RDB$RELATION_NAME = 'MON$ATTACHMENTS' AND RDB$FIELD_NAME = 'MON$CLIENT_VERSION'`).Scan(&n))
	return n > 0
}

// openClientVersionDB creates a throwaway database and returns an open
// connection for it with the given DSN options appended.
func openClientVersionDB(t *testing.T, opts string) (*sql.DB, string) {
	t.Helper()
	dbPath, dsn, err := CreateTestDatabase("client_version_")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.Remove(dbPath) })
	if opts != "" {
		dsn += "?" + opts
	}
	db, err := sql.Open("firebirdsql", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	require.NoError(t, db.Ping())
	return db, dsn
}

// TestClientVersionCustom verifies that the client_version connection option
// is sent in the attach DPB and shows up in MON$ATTACHMENTS.MON$CLIENT_VERSION.
func TestClientVersionCustom(t *testing.T) {
	db, _ := openClientVersionDB(t, "client_version=my-app/1.2.3")

	if !monClientVersionColumnExists(t, db) {
		t.Skip("MON$ATTACHMENTS.MON$CLIENT_VERSION not available on this server")
	}

	var got string
	require.NoError(t, db.QueryRow(
		"SELECT MON$CLIENT_VERSION FROM MON$ATTACHMENTS WHERE MON$ATTACHMENT_ID = CURRENT_CONNECTION").Scan(&got))
	require.Equal(t, "my-app/1.2.3", got)
}

// TestClientVersionDefault verifies that a connection without the
// client_version option still identifies itself (driver default) instead of
// leaving MON$CLIENT_VERSION empty.
func TestClientVersionDefault(t *testing.T) {
	db, _ := openClientVersionDB(t, "")

	if !monClientVersionColumnExists(t, db) {
		t.Skip("MON$ATTACHMENTS.MON$CLIENT_VERSION not available on this server")
	}

	var got *string
	require.NoError(t, db.QueryRow(
		"SELECT MON$CLIENT_VERSION FROM MON$ATTACHMENTS WHERE MON$ATTACHMENT_ID = CURRENT_CONNECTION").Scan(&got))
	require.NotNil(t, got, "client version must not be NULL when the driver sends it")
	require.NotEmpty(t, *got, "client version must not be empty")
	require.Contains(t, *got, "firebirdsql-go", "default client version should identify the driver")
}

// TestClientInfoOptionalItemsAccepted verifies that attachments carrying the
// optional isc_dpb_os_user / isc_dpb_host_name DPB items are accepted by the
// server on every supported version.
func TestClientInfoOptionalItemsAccepted(t *testing.T) {
	db, _ := openClientVersionDB(t, "os_user=1000&host_name=fbx-test-host")

	var n int
	require.NoError(t, db.QueryRow("SELECT COUNT(*) FROM RDB$DATABASE").Scan(&n))
	require.Equal(t, 1, n)
}
