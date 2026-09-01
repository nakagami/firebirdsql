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
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Self-tests for the test harness itself (Phase 1 of the Jaybird test port
// plan, JAYBIRD_TEST_PORT_PLAN.md). They need a live server just like the
// feature tests do, so harness breakage is caught before feature tests rely
// on it.

func TestTestUtilServerVersion(t *testing.T) {
	v := testServerVersion(t)
	t.Logf("server %s reports version %q (major=%d minor=%d patch=%d)",
		testServerAddr(), v.Raw, v.Major, v.Minor, v.Patch)
	require.True(t, v.Major > 0, "expected a parsed version, got %q", v.Raw)
}

func TestTestUtilCreateDatabaseWithDDL(t *testing.T) {
	db, _, _ := createTestDatabaseWithDDL(t, "testutil_ddl_",
		`CREATE TABLE HARNESS_TEST (ID INTEGER PRIMARY KEY, NAME VARCHAR(100))`,
		`INSERT INTO HARNESS_TEST VALUES (1, 'one')`,
		`INSERT INTO HARNESS_TEST VALUES (2, 'two')`,
	)
	var n int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM HARNESS_TEST`).Scan(&n))
	require.Equal(t, 2, n)
}

func TestTestUtilMustExecRowsAffected(t *testing.T) {
	db, _, _ := createTestDatabaseWithDDL(t, "testutil_exec_",
		`CREATE TABLE HARNESS_EXEC (ID INTEGER PRIMARY KEY)`)
	res := mustExec(t, context.Background(), db, `INSERT INTO HARNESS_EXEC VALUES (?)`, 1)
	affected, err := res.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(1), affected)
}

func TestTestUtilErrorMatchers(t *testing.T) {
	db, _, _ := createTestDatabaseWithDDL(t, "testutil_err_",
		`CREATE TABLE HARNESS_ERR (ID INTEGER PRIMARY KEY)`)
	mustExec(t, context.Background(), db, `INSERT INTO HARNESS_ERR VALUES (1)`)
	_, err := db.Exec(`INSERT INTO HARNESS_ERR VALUES (1)`)
	require.Error(t, err)
	requireGDSError(t, err, ISCUniqueKeyViolation)
	requireSQLCode(t, err, -803)
	requireSQLState(t, err, "23000")
}

func TestTestUtilProtocolGate(t *testing.T) {
	db, _, _ := createTestDatabaseWithDDL(t, "testutil_proto_")
	ver := negotiatedProtocol(t, db)
	t.Logf("negotiated protocol version %d against %s", ver, testServerAddr())
	require.GreaterOrEqual(t, ver, 10)
	// A capability gate that the local server may or may not satisfy: this
	// must never fail, only pass or skip.
	requireProtocol(t, db, PROTOCOL_VERSION13)
	requireScrollableCursors(t, db)
}

func TestTestUtilVersionGates(t *testing.T) {
	db, _, _ := createTestDatabaseWithDDL(t, "testutil_gate_")
	requireBooleanSupport(t)   // FB 3.0+
	requireProtocol(t, db, 10) // every supported server
	requireTimeZoneSupport(t)  // FB 4.0+, usually skipped on FB 2.5/3
}

func TestTestUtilUserFixture(t *testing.T) {
	requireServerVersion(t, 3, 0) // SQL user management (DROP USER)
	const testUsername = "TESTUTIL_USER"
	createTestUserFixture(t, testUsername, "testutil_pw")

	_, dsn, _ := createTestDatabaseWithDDL(t, "testutil_user_",
		`CREATE TABLE HARNESS_USER_CHECK (ID INTEGER)`,
		`GRANT SELECT ON HARNESS_USER_CHECK TO `+testUsername)

	// The fixture user must be able to authenticate and read the granted table.
	userDSN := strings.Replace(dsn, GetTestUser()+":"+GetTestPassword()+"@", testUsername+":testutil_pw@", 1)
	userDB := openTestDatabase(t, userDSN)
	var n int
	require.NoError(t, userDB.QueryRow(`SELECT COUNT(*) FROM HARNESS_USER_CHECK`).Scan(&n))
	require.Equal(t, 0, n)
}
