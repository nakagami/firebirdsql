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
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestServiceManager_Info(t *testing.T) {
	dbPath := GetTestDatabase("test_service_manager_info_")
	conn, err := sql.Open("firebirdsql_createdb", GetTestDSNFromDatabase(dbPath))
	require.NoError(t, err, "sql.Open")
	require.NotNil(t, conn, "sql.Open")
	err = conn.Ping()
	require.NoError(t, err, "DB.Ping")
	var s string
	conn.QueryRow("SELECT rdb$get_context('SYSTEM', 'ENGINE_VERSION') from rdb$database").Scan(&s)

	sm, err := NewServiceManager("localhost:3050", GetTestUser(), GetTestPassword(), GetDefaultServiceManagerOptions())
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

	dbInfo, err := sm.GetSvrDbInfo()
	assert.NotZero(t, dbInfo.DatabaseCount)
	found := false
	for _, db := range dbInfo.Databases {
		if db == dbPath {
			found = true
			break
		}
	}
	assert.True(t, found, "database found in GetSvrDbInfo")

	s, err = sm.GetFbLogString()
	assert.NoError(t, err, "GetFbLogString")
	assert.NotEmpty(t, s, "GetFbLogString")

	s, err = sm.GetDbStatsString(dbPath, NewStatisticsOptions(WithOnlyHeaderPages()))
	assert.NoError(t, err, "GetDbStatsString")
	assert.NotEmpty(t, s, "GetDbStatsString")
}

func TestServiceManagerOptions(t *testing.T) {
	opts := NewServiceManagerOptions()
	assert.Equal(t, ServiceManagerOptions{WireCrypt: true, AuthPlugin: "Srp256"}, opts)
	opts = NewServiceManagerOptions(WithoutWireCrypt(), WithAuthPlugin("LegacyAuth"))
	assert.Equal(t, ServiceManagerOptions{WireCrypt: false, AuthPlugin: "LegacyAuth"}, opts)
}
