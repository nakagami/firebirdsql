package firebirdsql

import (
	"database/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNBackupManagerSingleLevel(t *testing.T) {
	dbPathOrig := GetTestDatabase("test_nbackup_manager_orig_")
	dbBackup := GetTestBackup("test_nbackup_manager_")
	dbPathRest := GetTestDatabase("test_nbackup_manager_rest_")
	conn, err := sql.Open("firebirdsql_createdb", GetTestDSNFromDatabase(dbPathOrig))
	require.NoError(t, err, "sql.Open")
	require.NotNil(t, conn, "sql.Open")
	defer conn.Close()
	_, err = conn.Exec("create table test(a int)")
	require.NoError(t, err, "Exec")
	_, err = conn.Exec("insert into test values(123)")
	require.NoError(t, err, "Exec")

	bm, err := NewNBackupManager("localhost", GetTestUser(), GetTestPassword(), GetDefaultServiceManagerOptions())
	require.NoError(t, err, "NewBackupManager")
	require.NotNil(t, bm, "NewBackupManager")

	err = bm.Backup(dbPathOrig, dbBackup, GetDefaultNBackupOptions(), nil)
	require.NoError(t, err, "Backup")

	err = bm.Restore([]string{dbBackup}, dbPathRest, GetDefaultNBackupOptions(), nil)
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
}

func TestNBackupManagerFixup(t *testing.T) {
	if get_firebird_major_version() < 4 {
		t.Skip("fixup in Service Manager API supported since 4.0")
	}

	dbPathOrig := GetTestDatabase("test_nbackup_manager_orig_")
	dbBackup := GetTestBackup("test_nbackup_manager_")
	conn, err := sql.Open("firebirdsql_createdb", GetTestDSNFromDatabase(dbPathOrig))
	require.NoError(t, err, "sql.Open")
	require.NotNil(t, conn, "sql.Open")
	defer conn.Close()
	_, err = conn.Exec("create table test(a int)")
	require.NoError(t, err, "Exec")
	_, err = conn.Exec("insert into test values(123)")
	require.NoError(t, err, "Exec")

	bm, err := NewNBackupManager("localhost", GetTestUser(), GetTestPassword(), GetDefaultServiceManagerOptions())
	require.NoError(t, err, "NewBackupManager")
	require.NotNil(t, bm, "NewBackupManager")

	err = bm.Backup(dbPathOrig, dbBackup, GetDefaultNBackupOptions(), nil)
	require.NoError(t, err, "Backup")

	err = bm.Fixup(dbBackup, GetDefaultNBackupOptions(), nil)
	require.NoError(t, err, "Fixup")

	conn, err = sql.Open("firebirdsql", GetTestDSNFromDatabase(dbBackup))
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
}

func TestNBackupManagerIncremental(t *testing.T) {
	dbPathOrig := GetTestDatabase("test_nbackup_manager_orig_")
	dbBackup0 := GetTestBackup("test_nbackup_manager_")
	dbBackup1 := GetTestBackup("test_nbackup_manager_")
	dbPathRest := GetTestDatabase("test_nbackup_manager_rest_")
	conn, err := sql.Open("firebirdsql_createdb", GetTestDSNFromDatabase(dbPathOrig))
	require.NoError(t, err, "sql.Open")
	require.NotNil(t, conn, "sql.Open")
	defer conn.Close()
	_, err = conn.Exec("create table test(a int)")
	require.NoError(t, err, "Exec")
	_, err = conn.Exec("insert into test values(123)")
	require.NoError(t, err, "Exec")

	bm, err := NewNBackupManager("localhost", GetTestUser(), GetTestPassword(), GetDefaultServiceManagerOptions())
	require.NoError(t, err, "NewBackupManager")
	require.NotNil(t, bm, "NewBackupManager")

	opt := GetDefaultNBackupOptions()
	opt.Level = 0
	err = bm.Backup(dbPathOrig, dbBackup0, opt, nil)
	require.NoError(t, err, "Backup Level 0")

	_, err = conn.Exec("insert into test values(456)")
	require.NoError(t, err, "Exec")

	opt.Level = 1
	err = bm.Backup(dbPathOrig, dbBackup1, opt, nil)
	require.NoError(t, err, "Backup Level 1")

	err = bm.Restore([]string{dbBackup0, dbBackup1}, dbPathRest, opt, nil)
	require.NoError(t, err, "Restore to Level 1")

	conn, err = sql.Open("firebirdsql", GetTestDSNFromDatabase(dbPathRest))
	require.NoError(t, err, "sql.Open")
	require.NotNil(t, conn, "sql.Open")

	rows, err := conn.Query("select * from test")
	require.NoError(t, err, "Query")
	require.NotNil(t, rows, "Query")

	var res int
	require.True(t, rows.Next(), "Next")
	require.NoError(t, rows.Scan(&res), "Scan")
	assert.Equal(t, 123, res, "result in restored database should be same as in original")
	require.True(t, rows.Next(), "Next")
	require.NoError(t, rows.Scan(&res), "Scan")
	assert.Equal(t, 456, res, "result in restored database should be same as in original")

	rows.Close()
	conn.Close()
}

func TestNBackupOptions(t *testing.T) {
	opts := NewNBackupOptions()
	assert.Equal(t, int32(-1), opts.Level)
	assert.Equal(t, "", opts.Guid)
	assert.Equal(t, int32(0), opts.GetOptionsMask())
	opts = NewNBackupOptions(WithLevel(1), WithGuid("abc"), WithDBTriggers(), WithPlaceRestore(), WithPreserveSequence())
	assert.Equal(t, int32(1), opts.Level)
	assert.Equal(t, "abc", opts.Guid)
	assert.Equal(t, int32(6), opts.GetOptionsMask())
}
