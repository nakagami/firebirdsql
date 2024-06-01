package firebirdsql

import (
	"database/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestBackupManager(t *testing.T) {
	dbPathOrig := GetTestDatabase("test_backup_manager_orig_")
	dbBackup := GetTestBackup("test_backup_manager_")
	dbPathRest := GetTestDatabase("test_backup_manager_rest_")
	conn, err := sql.Open("firebirdsql_createdb", GetTestDSNFromDatabase(dbPathOrig))
	require.NoError(t, err, "sql.Open")
	require.NotNil(t, conn, "sql.Open")
	defer conn.Close()
	_, err = conn.Exec("create table test(a int)")
	require.NoError(t, err, "Exec")
	_, err = conn.Exec("insert into test values(123)")
	require.NoError(t, err, "Exec")

	bm, err := NewBackupManager("localhost", GetTestUser(), GetTestPassword(), GetDefaultServiceManagerOptions())
	require.NoError(t, err, "NewBackupManager")
	require.NotNil(t, bm, "NewBackupManager")

	err = bm.Backup(dbPathOrig, dbBackup, GetDefaultBackupOptions(), nil)
	require.NoError(t, err, "Backup")

	err = bm.Restore(dbBackup, dbPathRest, GetDefaultRestoreOptions(), nil)
	require.NoError(t, err, "Restore")

	conn, err = sql.Open("firebirdsql", GetTestDSNFromDatabase(dbPathRest))
	require.NoError(t, err, "sql.Open")
	require.NotNil(t, conn, "sql.Open")

	rows, err := conn.Query("select * from test")
	require.NoError(t, err, "Query")
	require.NotNil(t, rows, "Query")
	require.True(t, rows.Next(), "Next")
	var res int
	require.NoError(t, rows.Scan(&res), "Scan")
	assert.Equal(t, 123, res, "result in restored database should be same as in original")
	rows.Close()
	conn.Close()

	err = bm.Restore(dbBackup, dbPathRest, GetDefaultRestoreOptions(), nil)
	assert.ErrorContains(t, err, "already exists.  To replace it, use the -REP switch")

	opt := GetDefaultRestoreOptions()
	opt.Replace = true
	err = bm.Restore(dbBackup, dbPathRest, opt, nil)
	require.NoError(t, err, "Restore")
}
