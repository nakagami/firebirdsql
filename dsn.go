/*******************************************************************************
The MIT License (MIT)

Copyright (c) 2013-2020 Hajime Nakagami

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
	"net"
	"net/url"
	"strings"
)

type firebirdDsn struct {
	addr    string
	dbName  string
	user    string
	passwd  string
	options map[string]string
}

var ErrDsnUserUnknown = errors.New("User unknown")
var ErrDsnDbNameUnknown = errors.New("Database name unknown")

func newFirebirdDsn() *firebirdDsn {
	return &firebirdDsn{options: make(map[string]string)}
}

func parseDSN(dsns string) (*firebirdDsn, error) {

	dsn := newFirebirdDsn()

	if !strings.HasPrefix(dsns, "firebird://") {
		dsns = "firebird://" + dsns
	}
	u, err := url.Parse(dsns)
	if err != nil {
		return nil, err
	}
	if u.User == nil {
		return nil, ErrDsnUserUnknown
	}
	dsn.user = u.User.Username()
	dsn.passwd, _ = u.User.Password()
	dsn.addr = u.Host
	if _, _, err := net.SplitHostPort(dsn.addr); err != nil {
		// No port suffix (SplitHostPort also rejects bracketed IPv6 without a
		// port, where a naive strings.ContainsRune(addr, ':') would).
		dsn.addr += ":3050"
	}
	dsn.dbName = u.Path
	if len(dsn.dbName) > 0 && !strings.ContainsRune(dsn.dbName[1:], '/') {
		dsn.dbName = dsn.dbName[1:]
	}

	//Windows Path
	if len(dsn.dbName) >= 2 && strings.ContainsRune(dsn.dbName[2:], ':') {
		dsn.dbName = dsn.dbName[1:]
	}
	if strings.TrimLeft(dsn.dbName, "/") == "" {
		// Empty (or "/"-only) database path: nothing to attach to. Fail
		// here with a diagnosable error instead of an obscure attach-time
		// failure later.
		return nil, ErrDsnDbNameUnknown
	}

	m, _ := url.ParseQuery(u.RawQuery)

	var default_options = map[string]string{
		"auth_plugin_name":     "Srp256",
		"auth_plugin_list":     defaultAuthPlugins,
		"charset":              "UTF8",
		"client_version":       "",
		"column_name_to_lower": "false",
		"host_name":            "",
		"os_user":              "",
		"role":                 "",
		"timezone":             "",
		"wire_crypt":           "true",
		"wire_crypt_plugin":    defaultWireCryptPlugins,
		"wire_compress":        "false",
		"max_inline_blob_size": "65536",
		"max_blob_cache_size":  "10485760",
	}

	for k, v := range default_options {
		values, ok := m[k]
		if ok {
			dsn.options[k] = values[0]
		} else {
			dsn.options[k] = v
		}
	}

	// Aliases for protocol-19 blob options (fbx-compatible short names).
	if values, ok := m["inline_blob_size"]; ok && len(values) > 0 {
		dsn.options["max_inline_blob_size"] = values[0]
	}
	if values, ok := m["blob_cache_size"]; ok && len(values) > 0 {
		dsn.options["max_blob_cache_size"] = values[0]
	}

	// Fail fast on an invalid wire_crypt policy before dialing.
	if _, err := parseWireCryptMode(dsn.options["wire_crypt"]); err != nil {
		return nil, err
	}

	// Fail fast on an invalid auth-plugin configuration before dialing: the
	// allow-list must be a subset of the supported plugins and contain the
	// preferred auth_plugin_name.
	if err := validateAuthPlugins(dsn.options["auth_plugin_name"], dsn.options["auth_plugin_list"]); err != nil {
		return nil, err
	}

	return dsn, nil
}
