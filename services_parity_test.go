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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Phase 7 of the Jaybird test port plan (JAYBIRD_TEST_PORT_PLAN.md):
// Services API option coverage — mirroring Jaybird's FBBackupManagerTest,
// FBMaintenanceManagerTest, FBStatisticsManagerTest and the streamed-blob
// backup regression (JaybirdBlobBackupProblemTest).

// newBackupManagerForTest attaches a BackupManager to the test server.
func newBackupManagerForTest(t *testing.T) *BackupManager {
	t.Helper()
	bm, err := NewBackupManager(testServerAddr(), GetTestUser(), GetTestPassword(), GetDefaultServiceManagerOptions())
	require.NoError(t, err)
	return bm
}

// TestBackupOptionsMatrix mirrors Jaybird's FBBackupManagerTest option matrix:
// metadata-only backups carry structure but no data, and WithReplace allows
// restoring over an existing database.
func TestBackupOptionsMatrix(t *testing.T) {
	requireServiceAvailable(t)
	dbPath, dsn, err := CreateTestDatabase("svc_backup_opt_")
	require.NoError(t, err)
	t.Cleanup(func() { _ = removeDatabaseFile(dbPath) })
	db := openTestDatabase(t, dsn)
	mustExec(t, stmtCtx, db, "CREATE TABLE SVC_T (ID INTEGER PRIMARY KEY, V VARCHAR(20))")
	for i := 1; i <= 5; i++ {
		mustExec(t, stmtCtx, db, "INSERT INTO SVC_T VALUES (?, ?)", i, "row")
	}

	bm := newBackupManagerForTest(t)
	backupFile := GetTestBackup("svc_backup_opt_")
	t.Cleanup(func() { _ = removeDatabaseFile(backupFile) })

	// Metadata-only backup: restores structure without rows.
	metaBackup := GetTestBackup("svc_backup_meta_")
	t.Cleanup(func() { _ = removeDatabaseFile(metaBackup) })
	require.NoError(t, bm.Backup(dbPath, metaBackup, NewBackupOptions(WithMetadataOnly()), nil))

	restored := GetTestDatabase("svc_backup_meta_rest_")
	t.Cleanup(func() { _ = removeDatabaseFile(restored) })
	require.NoError(t, bm.Restore(metaBackup, restored, GetDefaultRestoreOptions(), nil))

	metaDB := openTestDatabase(t, GetTestDSNFromDatabase(restored))
	var metaRows int64
	require.NoError(t, metaDB.QueryRow("SELECT COUNT(*) FROM SVC_T").Scan(&metaRows))
	require.Equal(t, int64(0), metaRows, "metadata-only restore must keep no rows")
	var tableCount int
	require.NoError(t, metaDB.QueryRow(
		"SELECT COUNT(*) FROM RDB$RELATIONS WHERE RDB$RELATION_NAME = 'SVC_T'").Scan(&tableCount))
	require.Equal(t, 1, tableCount, "metadata-only restore must keep the table")

	// Full backup, then restore over an existing database with WithReplace.
	fullBackup := GetTestBackup("svc_backup_full_")
	t.Cleanup(func() { _ = removeDatabaseFile(fullBackup) })
	require.NoError(t, bm.Backup(dbPath, fullBackup, GetDefaultBackupOptions(), nil))

	// First restore creates the target; a second one must replace it.
	first := GetTestDatabase("svc_backup_full_rest_")
	t.Cleanup(func() { _ = removeDatabaseFile(first) })
	require.NoError(t, bm.Restore(fullBackup, first, GetDefaultRestoreOptions(), nil))
	require.NoError(t, bm.Restore(fullBackup, first, NewRestoreOptions(WithReplace()), nil),
		"WithReplace must allow restoring over an existing database")

	restDB := openTestDatabase(t, GetTestDSNFromDatabase(first))
	require.Equal(t, 5, mustCount(t, restDB, "SVC_T"))

	// Restoring over an existing database without WithReplace must fail.
	noReplaceErr := bm.Restore(fullBackup, first, GetDefaultRestoreOptions(), nil)
	require.Error(t, noReplaceErr, "restore without WithReplace over an existing DB must fail")
}

// TestBackupRestoreBlobIntegrity mirrors Jaybird's JaybirdBlobBackupProblemTest:
// a database holding a large streamed blob must survive a backup/restore round
// trip with its content intact (gbak "segment buffer length shorter" regression).
func TestBackupRestoreBlobIntegrity(t *testing.T) {
	requireServiceAvailable(t)
	dbPath, dsn, err := CreateTestDatabase("svc_backup_blob_")
	require.NoError(t, err)
	t.Cleanup(func() { _ = removeDatabaseFile(dbPath) })
	db := openTestDatabase(t, dsn)
	mustExec(t, stmtCtx, db, "CREATE TABLE BLOB_DOC (ID INTEGER PRIMARY KEY, DOC BLOB SUB_TYPE TEXT)")
	doc := strings.Repeat("streamed blob segment ", 30000) // ~660 KB
	mustExec(t, stmtCtx, db, "INSERT INTO BLOB_DOC (ID, DOC) VALUES (1, ?)", doc)

	bm := newBackupManagerForTest(t)
	backupFile := GetTestBackup("svc_backup_blob_")
	t.Cleanup(func() { _ = removeDatabaseFile(backupFile) })
	require.NoError(t, bm.Backup(dbPath, backupFile, GetDefaultBackupOptions(), nil))

	restored := GetTestDatabase("svc_backup_blob_rest_")
	t.Cleanup(func() { _ = removeDatabaseFile(restored) })
	require.NoError(t, bm.Restore(backupFile, restored, GetDefaultRestoreOptions(), nil))

	restDB := openTestDatabase(t, GetTestDSNFromDatabase(restored))
	var got string
	require.NoError(t, restDB.QueryRow("SELECT DOC FROM BLOB_DOC WHERE ID = 1").Scan(&got))
	require.Equal(t, doc, got, "blob content must survive backup/restore")
}

// TestStatisticsManagerReports mirrors Jaybird's FBStatisticsManagerTest:
// header-page and table-scoped statistics reports return meaningful content.
func TestStatisticsManagerReports(t *testing.T) {
	requireServiceAvailable(t)
	dbPath, dsn, err := CreateTestDatabase("svc_stats_")
	require.NoError(t, err)
	t.Cleanup(func() { _ = removeDatabaseFile(dbPath) })
	db := openTestDatabase(t, dsn)
	mustExec(t, stmtCtx, db, "CREATE TABLE STATS_T (ID INTEGER PRIMARY KEY)")
	for i := 1; i <= 10; i++ {
		mustExec(t, stmtCtx, db, "INSERT INTO STATS_T VALUES (?)", i)
	}

	sm, err := NewServiceManager(testServerAddr(), GetTestUser(), GetTestPassword(), GetDefaultServiceManagerOptions())
	require.NoError(t, err)
	defer sm.Close()

	// Header-page report: names the header page and reports page sizes.
	header, err := sm.GetDbStatsString(dbPath, NewStatisticsOptions(WithOnlyHeaderPages()))
	require.NoError(t, err)
	require.NotEmpty(t, header)
	require.Contains(t, strings.ToLower(header), "header", "header-page report should mention the header page")

	// Table-scoped report: mentions the requested table.
	tableStats, err := sm.GetDbStatsString(dbPath, NewStatisticsOptions(WithTables([]string{"STATS_T"})))
	require.NoError(t, err)
	require.NotEmpty(t, tableStats)
	require.Contains(t, strings.ToUpper(tableStats), "STATS_T", "table-scoped report should mention the table")

	// Record-version statistics (user data/index pages) produce a report too.
	rvStats, err := sm.GetDbStatsString(dbPath, NewStatisticsOptions(
		WithUserDataPages(), WithUserIndexPages(), WithRecordVersions()))
	require.NoError(t, err)
	require.NotEmpty(t, rvStats)
}

// TestMaintenanceAccessModeParity mirrors Jaybird's FBMaintenanceManagerTest
// access-mode coverage with behavioral verification: a read-only database
// refuses writes until it is switched back to read-write. The database must
// be unattached while the access mode changes.
func TestMaintenanceAccessModeParity(t *testing.T) {
	requireServiceAvailable(t)
	dbPath, dsn, err := CreateTestDatabase("svc_access_mode_")
	require.NoError(t, err)
	t.Cleanup(func() { _ = removeDatabaseFile(dbPath) })
	db := openTestDatabase(t, dsn)
	mustExec(t, stmtCtx, db, "CREATE TABLE AM_T (ID INTEGER PRIMARY KEY)")
	require.NoError(t, db.Close()) // release the attachment: mode change needs exclusive access

	mm, err := NewMaintenanceManager(testServerAddr(), GetTestUser(), GetTestPassword(), GetDefaultServiceManagerOptions())
	require.NoError(t, err)

	require.NoError(t, mm.SetAccessModeReadOnly(dbPath))

	// A fresh attachment sees the read-only mode and refuses writes.
	roDB, err := sql.Open("firebirdsql", dsn)
	require.NoError(t, err)
	_, writeErr := roDB.ExecContext(stmtCtx, "INSERT INTO AM_T VALUES (1)")
	require.Error(t, writeErr, "insert into a read-only database must fail")
	require.NoError(t, roDB.Close())

	// Back to read-write, the same statement succeeds.
	require.NoError(t, mm.SetAccessModeReadWrite(dbPath))
	rwDB := openTestDatabase(t, dsn)
	mustExec(t, stmtCtx, rwDB, "INSERT INTO AM_T VALUES (1)")
	require.Equal(t, 1, mustCount(t, rwDB, "AM_T"))
}

// TestServiceManagerSweepParity mirrors Jaybird's Sweep coverage: a sweep on
// a healthy database completes without error.
func TestServiceManagerSweepParity(t *testing.T) {
	requireServiceAvailable(t)
	dbPath, dsn, err := CreateTestDatabase("svc_sweep_")
	require.NoError(t, err)
	t.Cleanup(func() { _ = removeDatabaseFile(dbPath) })
	db := openTestDatabase(t, dsn)
	mustExec(t, stmtCtx, db, "CREATE TABLE SWEEP_T (ID INTEGER PRIMARY KEY)")
	mustExec(t, stmtCtx, db, "INSERT INTO SWEEP_T VALUES (1)")

	mm, err := NewMaintenanceManager(testServerAddr(), GetTestUser(), GetTestPassword(), GetDefaultServiceManagerOptions())
	require.NoError(t, err)
	require.NoError(t, mm.Sweep(dbPath))

	// The database is fully usable after the sweep.
	var n int
	require.NoError(t, db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM SWEEP_T").Scan(&n))
	require.Equal(t, 1, n)
}
