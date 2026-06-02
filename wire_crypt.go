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
	"errors"
	"fmt"
	"strings"
)

// wireCryptMode is the tri-state policy for wire encryption, mirroring the
// Firebird server-side WireCrypt setting (Disabled/Enabled/Required).
type wireCryptMode int

const (
	// wireCryptDisabled never encrypts the wire.
	wireCryptDisabled wireCryptMode = iota
	// wireCryptEnabled encrypts when the server offers an acceptable cipher,
	// but tolerates a plaintext channel when it does not (best-effort). This
	// is the historical behavior of wire_crypt=true.
	wireCryptEnabled
	// wireCryptRequired fails the connection closed unless wire encryption is
	// actually established.
	wireCryptRequired
)

// defaultWireCryptPlugins is the default ordered client allow-list of acceptable
// wire-encryption ciphers (strongest first). It is the single source of truth
// shared by every connection-construction site — the database/sql DSN defaults
// (dsn.go) and the Service Manager admin path (service_manager.go) — so the two
// paths cannot drift. An empty allow-list deliberately means "refuse all
// ciphers"; callers that want encryption-by-default must seed this value.
const defaultWireCryptPlugins = "ChaCha64,ChaCha,Arc4"

// parseWireCryptMode maps a wire_crypt DSN value to a wireCryptMode. The value
// is a tri-state with backward-compatible boolean aliases, matched
// case-insensitively:
//
//	"false" / "0" / "disabled" -> wireCryptDisabled
//	"" / "true" / "1" / "enabled" -> wireCryptEnabled
//	"required"                  -> wireCryptRequired
func parseWireCryptMode(s string) (wireCryptMode, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "false", "0", "disabled":
		return wireCryptDisabled, nil
	case "", "true", "1", "enabled":
		return wireCryptEnabled, nil
	case "required":
		return wireCryptRequired, nil
	default:
		return wireCryptEnabled, fmt.Errorf("firebirdsql: invalid wire_crypt value %q (want disabled/enabled/required, or the boolean aliases false/true)", s)
	}
}

// errWireCryptRequired is returned when wire_crypt=required but the handshake
// could not establish wire encryption (no acceptable cipher was negotiated on
// any path — including the legacy plain op_accept, where no cipher is possible).
var errWireCryptRequired = errors.New("firebirdsql: wire_crypt=required but no wire encryption was established; set wire_crypt=enabled to allow a plaintext fallback")

// wireCryptResolve evaluates the wire-crypt policy against the handshake
// negotiation outcome. encrypt reports whether op_crypt must be sent; err is
// errWireCryptRequired when the policy is "required" but no cipher can be
// established (fail closed).
//
// This is the single source of truth for the policy and MUST be consulted on
// every handshake outcome — including the legacy op_accept path, where the
// server never offers a cipher (encPlugin == "" and hasSessionKey == false).
// nonce is deliberately not an input: Arc4 negotiates with a nil nonce, so the
// decision must not depend on it.
func wireCryptResolve(mode wireCryptMode, encPlugin string, hasSessionKey bool) (encrypt bool, err error) {
	encrypt = encPlugin != "" && mode != wireCryptDisabled && hasSessionKey
	if !encrypt && mode == wireCryptRequired {
		err = errWireCryptRequired
	}
	return
}
