/*******************************************************************************
The MIT License (MIT)

Copyright (c) 2013-2014 Hajime Nakagami

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
	"errors"
	"testing"
)

func TestDSNParse(t *testing.T) {
	var testDSNs = []struct {
		dsn            string
		addr           string
		dbName         string
		user           string
		passwd         string
		role           string
		authPluginName string
		wireCrypt      bool
	}{
		{"user:password@localhost:3000/dbname", "localhost:3000", "dbname", "user", "password", "", "Srp", true},
		{"user:password@localhost/dbname", "localhost:3050", "dbname", "user", "password", "", "Srp", true},
		{"user:password@localhost/dir/dbname", "localhost:3050", "/dir/dbname", "user", "password", "", "Srp", true},
		{"user:password@localhost/c:\\fbdata\\database.fdb", "localhost:3050", "c:\\fbdata\\database.fdb", "user", "password", "", "Srp", true},
		{"user:password@localhost/dbname?role=role", "localhost:3050", "dbname", "user", "password", "role", "Srp", true},
	}

	for _, d := range testDSNs {
		addr, dbName, user, passwd, role, authPluginName, wireCrypt, err := parseDSN(d.dsn)
		if addr != d.addr || dbName != d.dbName || user != d.user || passwd != d.passwd || role != d.role || authPluginName != d.authPluginName || wireCrypt != d.wireCrypt {
			err = errors.New("parse DSN fail:" + d.dsn)
		}
		if err != nil {
			t.Error(err.Error())
		}
	}
}
