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

	opt := GetDefaultStatisticsOptions()
	opt.OnlyHeaderPages = true
	s, err = sm.GetDbStatsString(dbPath, opt)
	assert.NoError(t, err, "GetDbStatsString")
	assert.NotEmpty(t, s, "GetDbStatsString")
}
