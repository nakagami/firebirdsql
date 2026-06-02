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
	"fmt"
	"slices"
)

// defaultAuthPlugins is the set of authentication plugins this driver implements,
// strongest first. It is the default value of the auth_plugin_list DSN parameter
// and the supported-set reference that any configured auth_plugin_list is
// validated against (see validateAuthPlugins). Mirrors defaultWireCryptPlugins.
const defaultAuthPlugins = "Srp256,Srp,Legacy_Auth"

// isAuthPluginAllowed reports whether plugin is a member of the client allow-list
// authPluginList (an auth_plugin_list DSN value). It is the core defense against
// an auth downgrade: a server-selected plugin the client never sanctioned (e.g. a
// server forcing Legacy_Auth on an Srp256 client) is rejected before any auth
// blob is computed.
func isAuthPluginAllowed(plugin, authPluginList string) bool {
	return slices.Contains(splitList(authPluginList), plugin)
}

// validateAuthPlugins checks a connection's auth-plugin configuration before the
// handshake (fail fast). Every entry of authPluginList must be a plugin the
// driver actually implements (defaultAuthPlugins), and the preferred
// authPluginName must itself be a member of authPluginList. An empty list is
// rejected — unlike wire_crypt_plugin, where an empty list legitimately means
// "refuse all ciphers", an empty auth list leaves no way to authenticate.
// Mirrors the fail-fast style of parseWireCryptMode.
func validateAuthPlugins(authPluginName, authPluginList string) error {
	supported := splitList(defaultAuthPlugins)
	list := splitList(authPluginList)
	if len(list) == 0 {
		return fmt.Errorf("firebirdsql: auth_plugin_list is empty; want a comma-separated subset of %q", defaultAuthPlugins)
	}
	for _, p := range list {
		if !slices.Contains(supported, p) {
			return fmt.Errorf("firebirdsql: unsupported auth plugin %q in auth_plugin_list (supported: %q)", p, defaultAuthPlugins)
		}
	}
	if !slices.Contains(list, authPluginName) {
		return fmt.Errorf("firebirdsql: auth_plugin_name %q is not in auth_plugin_list %q", authPluginName, authPluginList)
	}
	return nil
}
