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

	um, err := NewUserManager("localhost:3050", GetTestUser(), GetTestPassword(), NewServiceManagerOptions(), NewUserManagerOptions())
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

	err = um.AddUser(NewUser(WithUsername("test"), WithPassword("test"), WithFirstName("xxx")))
	assert.NoError(t, err, "AddUser")

	defer func() {
		err = um.DeleteUser(NewUser(WithUsername("test")))
		assert.NoError(t, err, "DeleteUser")
	}()

	conn, err = sql.Open("firebirdsql", GetTestDSNFromDatabaseUserPassword(dbPath, "test", "test"))
	require.NoError(t, err)
	require.NotNil(t, conn)
	assert.NoError(t, conn.Ping())
	conn.Close()

	err = um.ModifyUser(NewUser(WithUsername("test"), WithLastName("testlastname"), WithPassword("zzz"), WithUserId(1), WithAdmin()))
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

func TestUserManagerOptions(t *testing.T) {
	opts := NewUserManagerOptions()
	assert.Equal(t, UserManagerOptions{SecurityDB: ""}, opts)
	opts = NewUserManagerOptions(WithSecurityDB("secdb"))
	assert.Equal(t, UserManagerOptions{SecurityDB: "secdb"}, opts)
}

func TestUserOptions(t *testing.T) {
	user := NewUser()
	assert.Equal(t, User{Username: nil, Password: nil, FirstName: nil, MiddleName: nil, LastName: nil, UserId: -1, GroupId: -1, Admin: nil}, user)
	user = NewUser(WithUsername("test"), WithPassword("pwd"), WithFirstName("qqq"), WithMiddleName("www"), WithLastName("eee"), WithUserId(100), WithGroupId(200), WithAdmin())
	assert.Equal(t, "test", *user.Username)
	assert.Equal(t, "pwd", *user.Password)
	assert.Equal(t, "qqq", *user.FirstName)
	assert.Equal(t, "www", *user.MiddleName)
	assert.Equal(t, "eee", *user.LastName)
	assert.Equal(t, int32(100), user.UserId)
	assert.Equal(t, int32(200), user.GroupId)
	require.NotNil(t, user.Admin)
	assert.True(t, *user.Admin)
	user = NewUser(WithoutAdmin())
	require.NotNil(t, user.Admin)
	assert.False(t, *user.Admin)
}
