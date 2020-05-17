package firebirdsql

import (
	"errors"
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
	if !strings.ContainsRune(dsn.addr, ':') {
		dsn.addr += ":3050"
	}
	dsn.dbName = u.Path
	if !strings.ContainsRune(dsn.dbName[1:], '/') {
		dsn.dbName = dsn.dbName[1:]
	}

	//Windows Path
	if strings.ContainsRune(dsn.dbName[2:], ':') {
		dsn.dbName = dsn.dbName[1:]
	}

	m, _ := url.ParseQuery(u.RawQuery)

	var default_options = map[string]string{
		"auth_plugin_name":     "Srp",
		"charset":              "UTF8",
		"column_name_to_lower": "false",
		"role":                 "",
		"timezone":             "",
		"wire_crypt":           "true",
	}

	for k, v := range default_options {
		values, ok := m[k]
		if ok {
			dsn.options[k] = values[0]
		} else {
			dsn.options[k] = v
		}
	}

	return dsn, nil
}
