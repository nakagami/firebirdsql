/*******************************************************************************
The MIT License (MIT)

Copyright (c) 2013-2019 Hajime Nakagami

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
	"bytes"
	"testing"
)

func TestIsAuthPluginAllowed(t *testing.T) {
	cases := []struct {
		plugin string
		list   string
		want   bool
	}{
		{"Legacy_Auth", "Srp256,Srp", false}, // the downgrade we must refuse
		{"Srp", "Srp256,Srp", true},
		{"Srp256", "Srp256,Srp,Legacy_Auth", true},
		{"Legacy_Auth", "Srp256,Srp,Legacy_Auth", true}, // explicit opt-in
		{"Srp256", "", false},
		{"", "Srp256,Srp", false},
	}
	for _, c := range cases {
		if got := isAuthPluginAllowed(c.plugin, c.list); got != c.want {
			t.Errorf("isAuthPluginAllowed(%q,%q)=%v, want %v", c.plugin, c.list, got, c.want)
		}
	}
}

func TestValidateAuthPlugins(t *testing.T) {
	cases := []struct {
		name    string
		list    string
		wantErr bool
	}{
		{"Srp256", "Srp256,Srp,Legacy_Auth", false},
		{"Srp256", "Srp256,Srp", false},
		{"Legacy_Auth", "Srp256,Srp,Legacy_Auth", false},
		{"Srp256", "Srp256,FooBar", true},   // unsupported plugin in list
		{"Legacy_Auth", "Srp256,Srp", true}, // preferred not in list
		{"Srp256", "", true},                // empty list
	}
	for _, c := range cases {
		err := validateAuthPlugins(c.name, c.list)
		if (err != nil) != c.wantErr {
			t.Errorf("validateAuthPlugins(%q,%q) err=%v, wantErr=%v", c.name, c.list, err, c.wantErr)
		}
	}
}

// TestUidAdvertisesAuthPluginList verifies that uid() advertises the configured
// allow-list verbatim and, in particular, does not leak Legacy_Auth onto the wire
// when it has been excluded from auth_plugin_list.
func TestUidAdvertisesAuthPluginList(t *testing.T) {
	clientPublic, _, err := getClientSeed()
	if err != nil {
		t.Fatalf("getClientSeed: %v", err)
	}
	p := &wireProtocol{}

	hardened := p.uid("sysdba", "masterkey", "Srp256", "Srp256,Srp", true, clientPublic)
	if !bytes.Contains(hardened, []byte("Srp256,Srp")) {
		t.Errorf("uid() did not advertise the configured auth_plugin_list")
	}
	if bytes.Contains(hardened, []byte("Legacy_Auth")) {
		t.Errorf("uid() advertised Legacy_Auth though it was excluded from auth_plugin_list")
	}

	full := p.uid("sysdba", "masterkey", "Srp256", defaultAuthPlugins, true, clientPublic)
	if !bytes.Contains(full, []byte("Legacy_Auth")) {
		t.Errorf("uid() did not advertise Legacy_Auth though it was in auth_plugin_list")
	}
}

// TestDSNAuthPluginList covers the auth_plugin_list default, an override, and the
// fail-fast validation wired into parseDSN.
func TestDSNAuthPluginList(t *testing.T) {
	dsn, err := parseDSN("user:password@localhost:3050/dbname")
	if err != nil {
		t.Fatalf("parseDSN default: %v", err)
	}
	if dsn.options["auth_plugin_list"] != defaultAuthPlugins {
		t.Errorf("default auth_plugin_list=%q, want %q", dsn.options["auth_plugin_list"], defaultAuthPlugins)
	}

	dsn, err = parseDSN("user:password@localhost:3050/dbname?auth_plugin_list=Srp256,Srp")
	if err != nil {
		t.Fatalf("parseDSN override: %v", err)
	}
	if dsn.options["auth_plugin_list"] != "Srp256,Srp" {
		t.Errorf("override auth_plugin_list=%q, want %q", dsn.options["auth_plugin_list"], "Srp256,Srp")
	}

	// Unsupported plugin in the list must fail fast.
	if _, err = parseDSN("user:password@localhost:3050/dbname?auth_plugin_list=Srp256,Bogus"); err == nil {
		t.Errorf("parseDSN accepted an unsupported auth plugin; want error")
	}
	// Preferred plugin not in the list must fail fast.
	if _, err = parseDSN("user:password@localhost:3050/dbname?auth_plugin_name=Legacy_Auth&auth_plugin_list=Srp256,Srp"); err == nil {
		t.Errorf("parseDSN accepted a preferred plugin not in auth_plugin_list; want error")
	}
}
