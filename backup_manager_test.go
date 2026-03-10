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

func TestBackupOptions(t *testing.T) {
	opts := NewBackupOptions()
	assert.Equal(t, BackupOptions{IgnoreChecksums: false, IgnoreLimboTransactions: false, MetadataOnly: false, GarbageCollect: true, Transportable: true, ConvertExternalTablesToInternalTables: true, Expand: false, Zip: false, ParallelWorkers: 0}, opts)
	opts = NewBackupOptions(WithIgnoreChecksums(), WithIgnoreLimboTransactions(), WithMetadataOnly(), WithoutGarbageCollect(), WithoutTransportable(), WithoutConvertExternalTablesToInternalTables(), WithExpand(), WithZip(), WithBackupParallelWorkers(4))
	assert.Equal(t, BackupOptions{IgnoreChecksums: true, IgnoreLimboTransactions: true, MetadataOnly: true, GarbageCollect: false, Transportable: false, ConvertExternalTablesToInternalTables: false, Expand: true, Zip: true, ParallelWorkers: 4}, opts)
	opts = NewBackupOptions(WithBackupParallelWorkers(4), WithoutBackupParallelWorkers())
	assert.Equal(t, BackupOptions{IgnoreChecksums: false, IgnoreLimboTransactions: false, MetadataOnly: false, GarbageCollect: true, Transportable: true, ConvertExternalTablesToInternalTables: true, Expand: false, Zip: false, ParallelWorkers: 0}, opts)
}

func TestRestoreOptions(t *testing.T) {
	opts := NewRestoreOptions()
	assert.Equal(t, RestoreOptions{Replace: false, DeactivateIndexes: false, RestoreShadows: true, EnforceConstraints: true, CommitAfterEachTable: false, UseAllPageSpace: false, PageSize: 0, CacheBuffers: 0, ParallelWorkers: 0}, opts)
	opts = NewRestoreOptions(WithReplace(), WithDeactivateIndexes(), WithoutRestoreShadows(), WithoutEnforceConstraints(), WithCommitAfterEachTable(), WithUseAllPageSpace(), WithPageSize(8192), WithCacheBuffers(1024), WithRestoreParallelWorkers(8))
	assert.Equal(t, RestoreOptions{Replace: true, DeactivateIndexes: true, RestoreShadows: false, EnforceConstraints: false, CommitAfterEachTable: true, UseAllPageSpace: true, PageSize: 8192, CacheBuffers: 1024, ParallelWorkers: 8}, opts)
	opts = NewRestoreOptions(WithRestoreParallelWorkers(8), WithoutRestoreParallelWorkers())
	assert.Equal(t, RestoreOptions{Replace: false, DeactivateIndexes: false, RestoreShadows: true, EnforceConstraints: true, CommitAfterEachTable: false, UseAllPageSpace: false, PageSize: 0, CacheBuffers: 0, ParallelWorkers: 0}, opts)
}
