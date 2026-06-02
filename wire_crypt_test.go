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
	"testing"
)

func TestParseWireCryptMode(t *testing.T) {
	cases := []struct {
		in      string
		want    wireCryptMode
		wantErr bool
	}{
		{"false", wireCryptDisabled, false},
		{"0", wireCryptDisabled, false},
		{"disabled", wireCryptDisabled, false},
		{"DISABLED", wireCryptDisabled, false},
		{"", wireCryptEnabled, false},
		{"true", wireCryptEnabled, false},
		{"1", wireCryptEnabled, false},
		{"enabled", wireCryptEnabled, false},
		{" Enabled ", wireCryptEnabled, false},
		{"required", wireCryptRequired, false},
		{"Required", wireCryptRequired, false},
		{"bogus", wireCryptEnabled, true},
	}
	for _, c := range cases {
		got, err := parseWireCryptMode(c.in)
		if (err != nil) != c.wantErr {
			t.Errorf("parseWireCryptMode(%q) err=%v, wantErr=%v", c.in, err, c.wantErr)
		}
		if got != c.want {
			t.Errorf("parseWireCryptMode(%q)=%v, want %v", c.in, got, c.want)
		}
	}
}

// tlv encodes a single tag/length/value triple in the format _guess_wire_crypt
// parses (1-byte tag, 1-byte length, value).
func tlv(tag byte, val []byte) []byte {
	return append([]byte{tag, byte(len(val))}, val...)
}

// cryptBuf builds a synthetic crypt-negotiation buffer: tag 1 carries the
// space-separated available plugin list, tag 3 carries each plugin nonce.
func cryptBuf(available string, nonces ...[]byte) []byte {
	buf := tlv(1, []byte(available))
	for _, n := range nonces {
		buf = append(buf, tlv(3, n)...)
	}
	return buf
}

func chaCha64Nonce() []byte { return append([]byte("ChaCha64\x00"), make([]byte, 8)...) }
func chaChaNonce() []byte   { return append([]byte("ChaCha\x00"), make([]byte, 12)...) }

func TestGuessWireCryptAllowList(t *testing.T) {
	p := &wireProtocol{}
	all := cryptBuf("ChaCha64 ChaCha Arc4", chaCha64Nonce(), chaChaNonce())
	arc4Only := cryptBuf("Arc4")

	cases := []struct {
		name    string
		buf     []byte
		clients []string
		want    string
	}{
		{"default list picks strongest", all, []string{"ChaCha64", "ChaCha", "Arc4"}, "ChaCha64"},
		{"client order respected", all, []string{"ChaCha", "ChaCha64", "Arc4"}, "ChaCha"},
		{"arc4 accepted when allowed and only option", arc4Only, []string{"ChaCha64", "ChaCha", "Arc4"}, "Arc4"},
		{"arc4 refused when not in allow-list", arc4Only, []string{"ChaCha64", "ChaCha"}, ""},
		{"empty allow-list refuses everything", all, []string{}, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, _ := p._guess_wire_crypt(c.buf, c.clients)
			if got != c.want {
				t.Errorf("_guess_wire_crypt=%q, want %q", got, c.want)
			}
		})
	}
}

func TestWireCryptResolve(t *testing.T) {
	cases := []struct {
		name          string
		mode          wireCryptMode
		encPlugin     string
		hasSessionKey bool
		wantEncrypt   bool
		wantErr       bool
	}{
		// The core regression: "required" must fail closed when no cipher was
		// negotiated — this is the legacy op_accept / op_accept_data downgrade
		// path, where encPlugin stays "" and there is no session key.
		{"required, no cipher (op_accept downgrade)", wireCryptRequired, "", false, false, true},
		{"required, no cipher even with session key", wireCryptRequired, "", true, false, true},
		{"required, cipher but no session key", wireCryptRequired, "ChaCha64", false, false, true},
		{"required, cipher and session key", wireCryptRequired, "ChaCha64", true, true, false},
		{"required, arc4 resolves (nil-nonce path)", wireCryptRequired, "Arc4", true, true, false},
		{"enabled tolerates plaintext when no cipher", wireCryptEnabled, "", false, false, false},
		{"enabled, cipher and session key", wireCryptEnabled, "ChaCha64", true, true, false},
		{"disabled never encrypts even when offered", wireCryptDisabled, "ChaCha64", true, false, false},
		{"disabled, no cipher", wireCryptDisabled, "", false, false, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			encrypt, err := wireCryptResolve(c.mode, c.encPlugin, c.hasSessionKey)
			if encrypt != c.wantEncrypt {
				t.Errorf("wireCryptResolve(%v, %q, %v) encrypt=%v, want %v", c.mode, c.encPlugin, c.hasSessionKey, encrypt, c.wantEncrypt)
			}
			if (err != nil) != c.wantErr {
				t.Errorf("wireCryptResolve(%v, %q, %v) err=%v, wantErr=%v", c.mode, c.encPlugin, c.hasSessionKey, err, c.wantErr)
			}
			if c.wantErr && !errors.Is(err, errWireCryptRequired) {
				t.Errorf("wireCryptResolve(%v, %q, %v) err=%v, want errWireCryptRequired", c.mode, c.encPlugin, c.hasSessionKey, err)
			}
		})
	}
}

func TestWireCipherAccessor(t *testing.T) {
	fc := &firebirdsqlConn{wp: &wireProtocol{conn: wireChannel{plugin: "ChaCha64"}}}
	if got := fc.WireCipher(); got != "ChaCha64" {
		t.Errorf("WireCipher()=%q, want ChaCha64", got)
	}
	plain := &firebirdsqlConn{wp: &wireProtocol{conn: wireChannel{}}}
	if got := plain.WireCipher(); got != "" {
		t.Errorf("WireCipher()=%q, want empty for plaintext", got)
	}
}
