package firebirdsql

import (
	"database/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestUserManager(t *testing.T) {
	dbPath := GetTestDatabase("test_user_manager_")
	conn, err := sql.Open("firebirdsql_createdb", GetTestDSNFromDatabase(dbPath))
	require.NoError(t, err)
	require.NotNil(t, conn)
	defer conn.Close()
	err = conn.Ping()
	require.NoError(t, err)

	um, err := NewUserManager("localhost:3050", GetTestUser(), GetTestPassword(), GetDefaultUserManagerOptions())
	require.NoError(t, err)
	require.NotNil(t, um)
	defer um.Close()

	users, err := um.GetUsers()
	assert.NoError(t, err)

	haveSysdba := false
	haveTest := false
	for _, user := range users {
		if *user.Username == "SYSDBA" {
			haveSysdba = true
		}
		if *user.Username == "TEST" {
			haveTest = true
		}
	}
	assert.True(t, haveSysdba)
	assert.False(t, haveTest)

	err = um.AddUser(NewUser("test").WithPassword("test").WithFirstName("xxx"))
	assert.NoError(t, err)

	defer func() {
		err = um.DeleteUser(NewUser("test"))
		assert.NoError(t, err)
	}()

	conn, err = sql.Open("firebirdsql", GetTestDSNFromDatabaseUserPassword(dbPath, "test", "test"))
	require.NoError(t, err)
	require.NotNil(t, conn)
	assert.NoError(t, conn.Ping())
	conn.Close()

	err = um.ModifyUser(NewUser("test").WithLastName("testlastname").WithPassword("zzz").WithUserId(1))
	assert.NoError(t, err)

	conn, err = sql.Open("firebirdsql", GetTestDSNFromDatabaseUserPassword(dbPath, "test", "zzz"))
	require.NoError(t, err)
	require.NotNil(t, conn)
	assert.NoError(t, conn.Ping())
	conn.Close()

	users, err = um.GetUsers()
	assert.NoError(t, err)

	haveSysdba = false
	haveTest = false
	for _, user := range users {
		if *user.Username == "SYSDBA" {
			haveSysdba = true
		}
		if *user.Username == "TEST" {
			haveTest = true
			assert.NotNil(t, user.LastName)
			assert.Equal(t, "testlastname", *user.LastName)
		}
	}
	assert.True(t, haveSysdba)
	assert.True(t, haveTest)
}
