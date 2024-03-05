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
	require.NoError(t, err, "NewUserManager")
	require.NotNil(t, um, "NewUserManager")
	defer um.Close()

	users, err := um.GetUsers()
	assert.NoError(t, err, "GetUsers")

	haveSysdba := false
	haveTest := false
	for _, user := range users {
		if *user.Username == "SYSDBA" {
			haveSysdba = true
		}
		if *user.Username == "TEST" {
			haveTest = true
			assert.False(t, *user.Admin, "admin flag")
		}
	}
	assert.True(t, haveSysdba, "sysdba found")
	assert.False(t, haveTest, "test user not found")

	err = um.AddUser(NewUser("test").WithPassword("test").WithFirstName("xxx"))
	assert.NoError(t, err, "AddUser")

	defer func() {
		err = um.DeleteUser(NewUser("test"))
		assert.NoError(t, err, "DeleteUser")
	}()

	conn, err = sql.Open("firebirdsql", GetTestDSNFromDatabaseUserPassword(dbPath, "test", "test"))
	require.NoError(t, err)
	require.NotNil(t, conn)
	assert.NoError(t, conn.Ping())
	conn.Close()

	err = um.ModifyUser(NewUser("test").WithLastName("testlastname").WithPassword("zzz").WithUserId(1).WithAdmin(true))
	assert.NoError(t, err, "ModifyUser")

	conn, err = sql.Open("firebirdsql", GetTestDSNFromDatabaseUserPassword(dbPath, "test", "zzz"))
	require.NoError(t, err)
	require.NotNil(t, conn)
	assert.NoError(t, conn.Ping())
	conn.Close()

	users, err = um.GetUsers()
	assert.NoError(t, err, "GetUsers")

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
			assert.True(t, *user.Admin, "admin flag")
		}
	}
	assert.True(t, haveSysdba, "sysdba found")
	assert.True(t, haveTest, "test user found")

	assert.NoError(t, um.SetAdminRoleMapping())
	assert.NoError(t, um.DropAdminRoleMapping())
}
