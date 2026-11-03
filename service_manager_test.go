/*******************************************************************************
The MIT License (MIT)

Copyright (c) 2023-2024 Artyom Smirnov <artyom_smirnov@me.com>

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
	"errors"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"slices"
	"testing"
)

func TestServiceManager_Info(t *testing.T) {
	requireServiceAvailable(t)
	dbPath := GetTestDatabase("test_service_manager_info_")
	conn, err := sql.Open("firebirdsql_createdb", GetTestDSNFromDatabase(dbPath))
	require.NoError(t, err, "sql.Open")
	require.NotNil(t, conn, "sql.Open")
	err = conn.Ping()
	require.NoError(t, err, "DB.Ping")
	var s string
	conn.QueryRow("SELECT rdb$get_context('SYSTEM', 'ENGINE_VERSION') from rdb$database").Scan(&s)

	sm, err := NewServiceManager(testServerAddr(), GetTestUser(), GetTestPassword(), GetDefaultServiceManagerOptions())
	require.NoError(t, err, "NewServiceManager")
	require.NotNil(t, sm, "NewServiceManager")
	defer sm.Close()

	version, err := sm.GetServerVersion()
	assert.NoError(t, err, "GetServerVersion")
	assert.Equal(t, s, fmt.Sprintf("%d.%d.%d", version.Major, version.Minor, version.Patch))

	s, err = sm.GetArchitecture()
	assert.NoError(t, err, "GetArchitecture")
	assert.NotEmpty(t, s, "GetArchitecture")

	s, err = sm.GetHomeDir()
	assert.NoError(t, err, "GetHomeDir")
	assert.NotEmpty(t, s, "GetHomeDir")

	s, err = sm.GetLockFileDir()
	assert.NoError(t, err, "GetLockFileDir")
	assert.NotEmpty(t, s, "GetLockFileDir")

	s, err = sm.GetSecurityDatabasePath()
	assert.NoError(t, err, "GetSecurityDatabasePath")
	assert.NotEmpty(t, s, "GetSecurityDatabasePath")

	// GetSvrDbInfo must succeed, but attachment path listings are
	// server-configuration dependent (HQbird FB2.5/4/5 instances do not
	// report the temp database path), so only log what comes back.
	dbInfo, err := sm.GetSvrDbInfo()
	assert.NoError(t, err, "GetSvrDbInfo")
	if err == nil {
		t.Logf("GetSvrDbInfo: %d databases reported", dbInfo.DatabaseCount)
		for _, db := range dbInfo.Databases {
			if db == dbPath {
				t.Logf("test database listed in GetSvrDbInfo")
			}
		}
	}

	s, err = sm.GetFbLogString()
	assert.NoError(t, err, "GetFbLogString")
	assert.NotEmpty(t, s, "GetFbLogString")

	s, err = sm.GetDbStatsString(dbPath, NewStatisticsOptions(WithOnlyHeaderPages()))
	assert.NoError(t, err, "GetDbStatsString")
	assert.NotEmpty(t, s, "GetDbStatsString")
}

func TestServiceManagerOptions(t *testing.T) {
	requireServiceAvailable(t)
	opts := NewServiceManagerOptions()
	assert.Equal(t, ServiceManagerOptions{WireCrypt: true, AuthPlugin: "Srp256"}, opts)
	opts = NewServiceManagerOptions(WithoutWireCrypt(), WithAuthPlugin("LegacyAuth"))
	assert.Equal(t, ServiceManagerOptions{WireCrypt: false, AuthPlugin: "LegacyAuth"}, opts)
}

// TestServiceManagerWireCryptDefault is the regression guard for the admin-path
// wire-crypt downgrade: NewServiceManager must seed the cipher allow-list so a
// default (WireCrypt=true) connection actually negotiates encryption instead of
// silently falling back to plaintext. On a wire-crypt-capable server (FB 3.0+)
// the negotiated cipher must be non-empty; FB <3.0 has no wire crypt and stays
// plaintext, so the assertion is gated on the live server version.
func TestServiceManagerWireCryptDefault(t *testing.T) {
	requireServiceAvailable(t)
	major := get_firebird_major_version(t)

	sm, err := NewServiceManager(testServerAddr(), GetTestUser(), GetTestPassword(), GetDefaultServiceManagerOptions())
	require.NoError(t, err, "NewServiceManager")
	require.NotNil(t, sm, "NewServiceManager")
	defer sm.Close()

	if major < 3 {
		require.Empty(t, sm.WireCipher(), "FB <3.0 has no wire crypt; connection must be plaintext")
		return
	}
	require.NotEmpty(t, sm.WireCipher(), "default service-manager connection must be encrypted on a wire-crypt-capable server")
}

// TestServiceManagerWireCryptFalse attaches without wire crypt, where the
// handshake leaves the SRP proof to the attach's SPB. The only accepted refusal
// is a server that requires encryption: any other error is a failure, so a
// proof the server cannot read never turns into a skip.
func TestServiceManagerWireCryptFalse(t *testing.T) {
	requireServiceAvailable(t)
	major := get_firebird_major_version(t)

	sm, err := NewServiceManager(testServerAddr(), GetTestUser(), GetTestPassword(), GetDefaultServiceManagerOptions().WithoutWireCrypt())
	var fbErr *FbError
	if errors.As(err, &fbErr) && slices.Contains(fbErr.GDSCodes, ISCWirecryptIncompatible) {
		t.Skipf("server requires wire encryption: %v", err)
	}
	require.NoError(t, err, "NewServiceManager")
	defer sm.Close()

	require.Empty(t, sm.WireCipher(), "connection without wire crypt must be plaintext")
	if major < 3 {
		require.Empty(t, sm.wp.authData, "FB <3.0 has no deferred auth data")
	} else {
		require.NotEmpty(t, sm.wp.authData, "FB 3.0+ must defer the SRP proof to the attach")
	}

	version, err := sm.GetServerVersion()
	require.NoError(t, err, "GetServerVersion")
	require.Equal(t, major, version.Major)
}
