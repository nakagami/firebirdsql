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
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Test harness shared by the Jaybird-parity feature tests. It mirrors the
// shared infrastructure of the Jaybird driver (FBTestProperties,
// UsesDatabaseExtension, RequireFeatureExtension / RequireProtocolExtension,
// DatabaseUserExtension and SQLExceptionMatchers) so that a feature test can
// gate on a server capability with a single skip line.

// testServerAddr returns the "host:port" of the Firebird server used by the
// live tests. Defaults to localhost:3050; override with
// FIREBIRD_TEST_SERVER_ADDR (e.g. "localhost:3055").
func testServerAddr() string {
	if addr := os.Getenv("FIREBIRD_TEST_SERVER_ADDR"); addr != "" {
		return addr
	}
	return "localhost:3050"
}

// errProtocolVersionUnavailable marks driver conns that do not expose a
// ProtocolVersion accessor (feature-gating then skips).
var errProtocolVersionUnavailable = errors.New("ProtocolVersion not available on driver conn")

var (
	testServerVersionOnce  sync.Once
	testServerVersionValue FirebirdVersion
	testServerVersionErr   error
	serviceAttachAvailable bool
)

// attachServiceWithTimeout performs one Services API attach with a hard
// timeout: some server configurations (e.g. Firebird Classic on Windows) do
// not support the Services API and simply never answer, which would hang the
// suite without the deadline.
func attachServiceWithTimeout() (*ServiceManager, error) {
	type result struct {
		sm  *ServiceManager
		err error
	}
	ch := make(chan result, 1)
	go func() {
		sm, err := NewServiceManager(testServerAddr(), GetTestUser(), GetTestPassword(), GetDefaultServiceManagerOptions())
		ch <- result{sm, err}
	}()
	select {
	case r := <-ch:
		return r.sm, r.err
	case <-time.After(15 * time.Second):
		return nil, fmt.Errorf("service attach timed out after 15s (Services API unavailable on %s?)", testServerAddr())
	}
}

// probeServiceAttach resolves the server version and service availability
// once per test process.
func probeServiceAttach() {
	if raw := os.Getenv("FIREBIRD_TEST_SERVER_VERSION"); raw != "" {
		testServerVersionValue = parseTestVersion(raw)
		if testServerVersionValue.Major > 0 {
			serviceAttachAvailable = true
			return
		}
		testServerVersionErr = fmt.Errorf("invalid FIREBIRD_TEST_SERVER_VERSION %q", raw)
		return
	}
	sm, err := attachServiceWithTimeout()
	if err != nil {
		testServerVersionErr = err
		return
	}
	defer sm.Close()
	testServerVersionValue, testServerVersionErr = sm.GetServerVersion()
	serviceAttachAvailable = testServerVersionErr == nil && testServerVersionValue.Major > 0
}

// testServerVersion returns the server version, resolved once per test
// process. Set FIREBIRD_TEST_SERVER_VERSION (e.g. "4.0.2") to pin the version
// instead of querying the Services API. Tests skip when the version cannot be
// determined (e.g. no reachable server for the Services API).
func testServerVersion(t *testing.T) FirebirdVersion {
	t.Helper()
	testServerVersionOnce.Do(probeServiceAttach)
	if testServerVersionErr != nil {
		t.Skipf("cannot determine server version: %v", testServerVersionErr)
	}
	return testServerVersionValue
}

// requireServiceAvailable skips the test when the Services API is unusable on
// the target server (attach error or the Classic-on-Windows hang). The probe
// runs once per test process.
func requireServiceAvailable(t *testing.T) {
	t.Helper()
	testServerVersionOnce.Do(probeServiceAttach)
	if testServerVersionErr != nil {
		t.Skipf("Services API unavailable on %s: %v", testServerAddr(), testServerVersionErr)
	}
	if !serviceAttachAvailable {
		t.Skipf("Services API unavailable on %s", testServerAddr())
	}
}

func parseTestVersion(raw string) FirebirdVersion {
	v := FirebirdVersion{Raw: raw}
	parts := strings.Split(raw, ".")
	if len(parts) > 0 {
		v.Major, _ = strconv.Atoi(strings.TrimSpace(parts[0]))
	}
	if len(parts) > 1 {
		v.Minor, _ = strconv.Atoi(strings.TrimSpace(parts[1]))
	}
	if len(parts) > 2 {
		v.Patch, _ = strconv.Atoi(strings.TrimSpace(parts[2]))
	}
	return v
}

// requireServerVersion skips the test unless the server is major.minor or
// higher (mirror of Jaybird's RequireFeatureExtension / FbAssumptions).
func requireServerVersion(t *testing.T, major, minor int) {
	t.Helper()
	v := testServerVersion(t)
	if !v.EqualOrGreater(major, minor) {
		t.Skipf("requires Firebird %d.%d+, server is %q", major, minor, v.Raw)
	}
}

// Server-feature gates for the Firebird datatypes and wire features.
func requireBooleanSupport(t *testing.T)   { requireServerVersion(t, 3, 0) }
func requireDecFloatSupport(t *testing.T)  { requireServerVersion(t, 4, 0) }
func requireInt128Support(t *testing.T)    { requireServerVersion(t, 4, 0) }
func requireTimeZoneSupport(t *testing.T)  { requireServerVersion(t, 4, 0) }
func requireDecimal38Support(t *testing.T) { requireServerVersion(t, 4, 0) }

// negotiatedProtocol returns the negotiated wire protocol version of one of
// db's pooled connections.
func negotiatedProtocol(t *testing.T, db *sql.DB) int {
	t.Helper()
	sqlConn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer sqlConn.Close()
	var ver int
	err = sqlConn.Raw(func(dc any) error {
		p, ok := dc.(interface{ ProtocolVersion() int })
		if !ok {
			// Driver builds without the ProtocolVersion accessor cannot be
			// feature-gated; the tests using this helper skip.
			return errProtocolVersionUnavailable
		}
		ver = p.ProtocolVersion()
		return nil
	})
	if err == errProtocolVersionUnavailable {
		t.Skipf("negotiated protocol unknown: driver conn does not expose ProtocolVersion()")
	}
	require.NoError(t, err)
	return ver
}

// requireProtocol skips the test unless the negotiated wire protocol version
// is minVersion or higher (mirror of Jaybird's RequireProtocolExtension).
func requireProtocol(t *testing.T, db *sql.DB, minVersion int) {
	t.Helper()
	ver := negotiatedProtocol(t, db)
	if ver < minVersion {
		t.Skipf("requires wire protocol %d+, negotiated %d", minVersion, ver)
	}
}

func requireBatchSupport(t *testing.T, db *sql.DB)      { requireProtocol(t, db, PROTOCOL_VERSION16) }
func requireScrollableCursors(t *testing.T, db *sql.DB) { requireProtocol(t, db, PROTOCOL_VERSION18) }
func requireInlineBlobs(t *testing.T, db *sql.DB)       { requireProtocol(t, db, PROTOCOL_VERSION19) }

// openTestDatabase opens dsn, pings it and closes it at test cleanup.
func openTestDatabase(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	db, err := sql.Open("firebirdsql", dsn)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, db.PingContext(ctx))
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// createTestDatabaseWithDDL creates a throwaway database (mirror of Jaybird's
// UsesDatabaseExtension + DdlHelper), runs each ddl statement on it and
// removes the database file at test cleanup. Returns the opened database, its
// sysdba DSN and the database file path (for tests that need a second DSN).
func createTestDatabaseWithDDL(t *testing.T, prefix string, ddl ...string) (db *sql.DB, dsn string, file string) {
	t.Helper()
	file, dsn, err := CreateTestDatabase(prefix)
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.Remove(file) })
	db = openTestDatabase(t, dsn)
	for _, stmt := range ddl {
		mustExec(t, context.Background(), db, stmt)
	}
	return db, dsn, file
}

// removeDatabaseFile deletes a throwaway database file at test cleanup. It
// complements DROP DATABASE-less cleanup: the test suite assumes a local
// server, so the file created via the server is removable locally.
func removeDatabaseFile(file string) error {
	return os.Remove(file)
}

// execContexter is satisfied by *sql.DB, *sql.Tx, *sql.Conn — anything a test
// may execute a statement on.
type execContexter interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

func mustExec(t *testing.T, ctx context.Context, db execContexter, query string, args ...any) sql.Result {
	t.Helper()
	res, err := db.ExecContext(ctx, query, args...)
	require.NoError(t, err, "exec %q", query)
	return res
}

// createTestUserFixture creates a user through the Services API and drops it
// again at test cleanup (mirror of Jaybird's DatabaseUserExtension). Drops a
// leftover user with the same name first.
func createTestUserFixture(t *testing.T, username, password string) {
	t.Helper()
	newUserManager := func() *UserManager {
		um, err := NewUserManager(testServerAddr(), GetTestUser(), GetTestPassword(), GetDefaultServiceManagerOptions(), NewUserManagerOptions())
		require.NoError(t, err)
		return um
	}
	um := newUserManager()
	defer um.Close()
	_ = um.DeleteUser(NewUser(WithUsername(username))) // tolerate "not found" for leftover drops
	require.NoError(t, um.AddUser(NewUser(WithUsername(username), WithPassword(password))))
	t.Cleanup(func() {
		cleanupUm := newUserManager()
		defer cleanupUm.Close()
		_ = cleanupUm.DeleteUser(NewUser(WithUsername(username)))
	})
}

// fbErrorFrom asserts err is an *FbError and returns it.
func fbErrorFrom(t *testing.T, err error) *FbError {
	t.Helper()
	require.Error(t, err)
	var fbErr *FbError
	require.True(t, errors.As(err, &fbErr), "expected *FbError, got %T: %v", err, err)
	return fbErr
}

// requireSQLState asserts err is an *FbError with the given SQLSTATE
// (mirror of Jaybird's SQLExceptionMatchers).
func requireSQLState(t *testing.T, err error, wantSQLState string) {
	t.Helper()
	fbErr := fbErrorFrom(t, err)
	require.Equal(t, wantSQLState, fbErr.SQLState, "SQLSTATE mismatch for: %v", err)
}

// requireSQLCode asserts err is an *FbError with the given SQLCODE.
func requireSQLCode(t *testing.T, err error, wantSQLCode int32) {
	t.Helper()
	fbErr := fbErrorFrom(t, err)
	require.Equal(t, wantSQLCode, fbErr.SQLCode, "SQLCODE mismatch for: %v", err)
}

// requireGDSError asserts the status vector of err contains all given GDS
// codes (e.g. requireGDSError(t, err, ISCUniqueKeyViolation)).
func requireGDSError(t *testing.T, err error, wantGDSCodes ...int) {
	t.Helper()
	fbErr := fbErrorFrom(t, err)
	for _, code := range wantGDSCodes {
		require.Contains(t, fbErr.GDSCodes, code, "status vector %v missing GDS code %d for: %v", fbErr.GDSCodes, code, err)
	}
}
