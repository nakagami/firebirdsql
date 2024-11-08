/*******************************************************************************
The MIT License (MIT)

Copyright (c) 2023-2024 Artyom Smirnov <artyom_smirnov@me.com>

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

package main

import (
	"database/sql"
	"fmt"
	"github.com/nakagami/firebirdsql"
	"os"
)

func main() {
	// Base service manager tools
	sm, err := firebirdsql.NewServiceManager("localhost:3050", "sysdba", "test", firebirdsql.NewServiceManagerOptions())
	defer sm.Close()
	if err != nil {
		panic(err)
	}
	// Extract some server info
	version, err := sm.GetServerVersion()
	if err != nil {
		panic(err)
	}
	homeDir, err := sm.GetHomeDir()
	if err != nil {
		panic(err)
	}
	fmt.Printf("Server version: %s, installed in %s\n", version.Raw, homeDir)

	// User management
	um, err := firebirdsql.NewUserManager("localhost:3050", "sysdba", "test", firebirdsql.NewServiceManagerOptions(), firebirdsql.NewUserManagerOptions())
	defer um.Close()
	if err != nil {
		panic(err)
	}

	// Create new user
	_ = um.AddUser(firebirdsql.NewUser(firebirdsql.WithUsername("testuser"), firebirdsql.WithPassword("testpass"), firebirdsql.WithAdmin()))
	// Obtain existing users
	users, err := um.GetUsers()
	if err != nil {
		panic(err)
	}
	for _, user := range users {
		fmt.Printf("User: %s, admin role: %v\n", *user.Username, *user.Admin)
	}
	// Drop user
	_ = um.DeleteUser(firebirdsql.NewUser(firebirdsql.WithUsername("testuser")))

	// Backup manager
	conn, err := sql.Open("firebirdsql_createdb", "sysdba:test@localhost/tmp/gofirebirdsqltest.fdb")
	defer conn.Close()
	if err != nil || conn == nil {
		panic(err)
	}
	err = conn.Ping()
	if err != nil {
		panic(err)
	}

	// Backup manager (gbak utility)
	bm, err := firebirdsql.NewBackupManager("localhost:3050", "sysdba", "test", firebirdsql.NewServiceManagerOptions())
	// no need to close BackupManager, because it opens connection during Backup/Restore and closes it automatically
	if err != nil {
		panic(err)
	}

	// Create backup with gbak using logging from server to client
	fmt.Println("\ngback:\n")
	done := make(chan bool)
	resChan := make(chan string)
	go func() {
		err = bm.Backup("/tmp/gofirebirdsqltest.fdb", "/tmp/gofirebirdsqltest.fbk", firebirdsql.NewBackupOptions(), resChan)
		if err != nil {
			panic(err)
		}
		done <- true
	}()

	cont := true
	var s string
	for cont {
		select {
		case s = <-resChan:
			fmt.Println(s)
		case <-done:
			cont = false
			break
		}
	}

	// NBackup manager (nbackup utility)
	nbk, err := firebirdsql.NewNBackupManager("localhost:3050", "sysdba", "test", firebirdsql.NewServiceManagerOptions())
	// no need to close NBackupManager, because it opens connection during Backup/Restore and closes it automatically
	if err != nil {
		panic(err)
	}

	// Create zero level backup with nbackup using logging from server to client
	os.Remove("/tmp/gofirebirdsqltest.nbk")
	fmt.Println("\nNBackup:\n")
	go func() {
		err = nbk.Backup("/tmp/gofirebirdsqltest.fdb", "/tmp/gofirebirdsqltest.nbk", firebirdsql.NewNBackupOptions(), resChan)
		if err != nil {
			panic(err)
		}
		done <- true
	}()

	cont = true
	for cont {
		select {
		case s = <-resChan:
			fmt.Println(s)
		case <-done:
			cont = false
			break
		}
	}
}
