package firebirdsql

import (
	"database/sql"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestExecStatementLeakOnError guards against a regression where
// (*firebirdsqlConn).exec returns without calling stmt.Close() when
// stmt.exec() reports an error. Without the close, the prepared
// statement stays allocated on the server until the whole attachment
// is detached.
//
// Reproduction strategy: trigger N duplicate-key INSERTs against a
// UNIQUE column. Each Exec fails with FbError. After that, count how
// many prepared statements referencing our table are still alive on
// the server via the MON$STATEMENTS monitoring table.
func TestExecStatementLeakOnError(t *testing.T) {
	file, dsn, err := CreateTestDatabase("test_exec_leak_")
	require.NoError(t, err)
	defer os.Remove(file)

	conn, err := sql.Open("firebirdsql", dsn)
	require.NoError(t, err)
	defer conn.Close()

	// pin a single physical attachment so every Exec reuses the same
	// underlying conn. Otherwise, leaked statements could be reaped when
	// the pool closes their conn, masking the leak.
	conn.SetMaxOpenConns(1)
	conn.SetMaxIdleConns(1)

	_, err = conn.Exec("CREATE TABLE leak_test (id INTEGER NOT NULL PRIMARY KEY)")
	require.NoError(t, err)
	_, err = conn.Exec("INSERT INTO leak_test VALUES (1)")
	require.NoError(t, err)

	const N = 30
	for i := 0; i < N; i++ {
		_, err := conn.Exec("INSERT INTO leak_test VALUES (1)") // duplicate PK
		require.Error(t, err, "iter %d: expected duplicate-key error", i)
	}

	// count prepared statements still allocated for the current
	// attachment whose SQL text references leak_test. The MON$STATEMENTS
	// query itself is excluded.
	var leaked int
	err = conn.QueryRow(`
		SELECT COUNT(*) FROM MON$STATEMENTS
		WHERE MON$ATTACHMENT_ID = CURRENT_CONNECTION
		  AND MON$SQL_TEXT CONTAINING 'leak_test'
		  AND MON$SQL_TEXT NOT CONTAINING 'MON$STATEMENTS'
	`).Scan(&leaked)
	require.NoError(t, err)

	t.Logf("Leaked prepared statements after %d failing Exec()s: %d", N, leaked)
	assert.Equal(t, 0, leaked,
		"Exec() leaks the prepared statement handle when the statement returns an error")
}
