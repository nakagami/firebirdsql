package firebirdsql

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"path"
	"regexp"
	"testing"
)

func cleanFirebirdLog(t *testing.T) {
	m, err := NewServiceManager("localhost:3050", GetTestUser(), GetTestPassword(), GetDefaultServiceManagerOptions())
	require.NoError(t, err)
	defer m.Close()
	logFile, err := m.GetHomeDir()
	logFile = path.Join(logFile, "firebird.log")
	require.NoError(t, err)
	require.NoError(t, os.Truncate(logFile, 0))
}

func getFirebirdLog(t *testing.T) string {
	m, err := NewServiceManager("localhost:3050", GetTestUser(), GetTestPassword(), GetDefaultServiceManagerOptions())
	require.NoError(t, err)
	defer m.Close()
	log, err := m.GetFbLogString()
	require.NoError(t, err)
	log = regexp.MustCompile(`(Database).*`).ReplaceAllString(log, "$1 xxxxx")
	log = regexp.MustCompile(`\w+\s+\w+\s+\w+\s+\d+\s+\d+:\d+:\d+\s+\d+`).ReplaceAllString(log, "")
	log = regexp.MustCompile(`(?m)^\s+`).ReplaceAllString(log, "")
	log = regexp.MustCompile(`(?m)\s+$`).ReplaceAllString(log, "")
	log = regexp.MustCompile(`(OIT|OAT|OST|Next) \d+`).ReplaceAllString(log, "$1 xxx")
	log = regexp.MustCompile(`\d+ (workers|errors|warnings|fixed)`).ReplaceAllString(log, "x $1")
	log = regexp.MustCompile(`(time) \d+\.\d+`).ReplaceAllString(log, "$1 x.xxx")
	return log
}

func grabStringOutput(run func() error, resChan chan string) (string, error) {
	done := make(chan bool)
	var result string
	var err error

	go func() {
		err = run()
		done <- true
	}()

	for loop, s := true, ""; loop; {
		select {
		case <-done:
			loop = false
			break
		case s = <-resChan:
			result += s + "\n"
		}
	}
	return result, err
}

func TestServiceManager_Sweep(t *testing.T) {
	m, err := NewMaintenanceManager("localhost:3050", GetTestUser(), GetTestPassword(), GetDefaultServiceManagerOptions())
	require.NoError(t, err)
	require.NotNil(t, m)
	cleanFirebirdLog(t)
	err = m.Sweep("employee")
	assert.NoError(t, err)
	log := getFirebirdLog(t)
	fmt.Println(log)
	assert.Equal(t, `Sweep is started by SYSDBA
Database xxxxx
OIT xxx, OAT xxx, OST xxx, Next xxx
Sweep is finished
Database xxxxx
OIT xxx, OAT xxx, OST xxx, Next xxx`, log)
}

func TestServiceManager_Validate(t *testing.T) {
	m, err := NewMaintenanceManager("localhost:3050", GetTestUser(), GetTestPassword(), GetDefaultServiceManagerOptions())
	require.NoError(t, err)
	require.NotNil(t, m)

	cleanFirebirdLog(t)
	err = m.Validate("employee", isc_spb_rpr_check_db)
	assert.NoError(t, err)
	log := getFirebirdLog(t)
	assert.Equal(t, `Database xxxxx
Validation started
Database xxxxx
Validation finished: x errors, x warnings, x fixed`, log)

	cleanFirebirdLog(t)
	err = m.Validate("employee", isc_spb_rpr_full)
	assert.NoError(t, err)
	log = getFirebirdLog(t)
	assert.Equal(t, `Database xxxxx
Validation started
Database xxxxx
Validation finished: x errors, x warnings, x fixed`, log)
}

func TestServiceManager_Mend(t *testing.T) {
	m, err := NewMaintenanceManager("localhost:3050", GetTestUser(), GetTestPassword(), GetDefaultServiceManagerOptions())
	require.NoError(t, err)
	require.NotNil(t, m)

	cleanFirebirdLog(t)
	err = m.Mend("employee")
	assert.NoError(t, err)
	log := getFirebirdLog(t)
	assert.Equal(t, `Database xxxxx
Validation started
Database xxxxx
Validation finished: x errors, x warnings, x fixed`, log)
}

func TestServiceManager_ListLimboTransactions(t *testing.T) {
	m, err := NewMaintenanceManager("localhost:3050", GetTestUser(), GetTestPassword(), GetDefaultServiceManagerOptions())
	require.NoError(t, err)
	require.NotNil(t, m)
	_, err = m.GetLimboTransactions("employee")
	assert.NoError(t, err)
}

func TestServiceManager_CommitTransaction(t *testing.T) {
	m, err := NewMaintenanceManager("localhost:3050", GetTestUser(), GetTestPassword(), GetDefaultServiceManagerOptions())
	require.NoError(t, err)
	require.NotNil(t, m)
	err = m.CommitTransaction("employee", 1)
	assert.EqualError(t, err, `failed to reconnect to a transaction in database employee
transaction is not in limbo
transaction 1 is committed
`)
}

func TestServiceManager_RollbackTransaction(t *testing.T) {
	m, err := NewMaintenanceManager("localhost:3050", GetTestUser(), GetTestPassword(), GetDefaultServiceManagerOptions())
	require.NoError(t, err)
	require.NotNil(t, m)
	err = m.RollbackTransaction("employee", 1)
	assert.EqualError(t, err, `failed to reconnect to a transaction in database employee
transaction is not in limbo
transaction 1 is committed
`)
}

func TestServiceManager_SetDatabaseMode(t *testing.T) {
	m, err := NewMaintenanceManager("localhost:3050", GetTestUser(), GetTestPassword(), GetDefaultServiceManagerOptions())
	require.NoError(t, err)
	require.NotNil(t, m)
	err = m.SetAccessModeReadOnly("employee")
	assert.NoError(t, err)
	err = m.SetAccessModeReadWrite("employee")
	assert.NoError(t, err)
}

func TestServiceManager_SetDatabaseDialect(t *testing.T) {
	m, err := NewMaintenanceManager("localhost:3050", GetTestUser(), GetTestPassword(), GetDefaultServiceManagerOptions())
	require.NoError(t, err)
	require.NotNil(t, m)
	err = m.SetDialect("employee", 1)
	assert.NoError(t, err)
	err = m.SetDialect("employee", 3)
	assert.NoError(t, err)
	err = m.SetDialect("employee", 10)
	assert.Error(t, err)
}

func TestServiceManager_SetPageBuffers(t *testing.T) {
	m, err := NewMaintenanceManager("localhost:3050", GetTestUser(), GetTestPassword(), GetDefaultServiceManagerOptions())
	require.NoError(t, err)
	require.NotNil(t, m)
	err = m.SetPageBuffers("employee", 0)
	assert.NoError(t, err)
	err = m.SetPageBuffers("employee", 30)
	assert.Error(t, err)
	err = m.SetPageBuffers("employee", 100)
	assert.NoError(t, err)
}

func TestServiceManager_SetWriteMode(t *testing.T) {
	m, err := NewMaintenanceManager("localhost:3050", GetTestUser(), GetTestPassword(), GetDefaultServiceManagerOptions())
	require.NoError(t, err)
	require.NotNil(t, m)
	err = m.SetWriteModeAsync("employee")
	assert.NoError(t, err)
	err = m.SetWriteModeSync("employee")
	assert.NoError(t, err)
}

func TestServiceManager_SetPageFill(t *testing.T) {
	m, err := NewMaintenanceManager("localhost:3050", GetTestUser(), GetTestPassword(), GetDefaultServiceManagerOptions())
	require.NoError(t, err)
	require.NotNil(t, m)
	err = m.SetPageFillNoReserve("employee")
	assert.NoError(t, err)
	err = m.SetPageFillReserve("employee")
	assert.NoError(t, err)
}

func TestServiceManager_DatabaseShutdown(t *testing.T) {
	m, err := NewMaintenanceManager("localhost:3050", GetTestUser(), GetTestPassword(), GetDefaultServiceManagerOptions())
	require.NoError(t, err)
	require.NotNil(t, m)

	for _, mode := range []ShutdownMode{ShutdownModeDenyNewAttachments, ShutdownModeDenyNewTransactions, ShutdownModeForce} {
		err = m.Shutdown("employee", mode, 0)
		assert.NoError(t, err)
		err = m.Online("employee")
		assert.NoError(t, err)
	}
}

func TestServiceManager_DatabaseShutdownEx(t *testing.T) {
	m, err := NewMaintenanceManager("localhost:3050", GetTestUser(), GetTestPassword(), GetDefaultServiceManagerOptions())
	require.NoError(t, err)
	require.NotNil(t, m)

	err = m.ShutdownEx("employee", OperationModeFull, ShutdownModeExForce, 0)
	assert.NoError(t, err)
	err = m.OnlineEx("employee", OperationModeNormal)
	assert.NoError(t, err)
}

func TestServiceManager_SetSweepInterval(t *testing.T) {
	m, err := NewMaintenanceManager("localhost:3050", GetTestUser(), GetTestPassword(), GetDefaultServiceManagerOptions())
	require.NoError(t, err)
	require.NotNil(t, m)

	err = m.SetSweepInterval("employee", 20000)
	assert.NoError(t, err)
}

func TestServiceManager_NoLinger(t *testing.T) {
	m, err := NewMaintenanceManager("localhost:3050", GetTestUser(), GetTestPassword(), GetDefaultServiceManagerOptions())
	require.NoError(t, err)
	require.NotNil(t, m)

	err = m.NoLinger("employee")
	assert.NoError(t, err)
}
