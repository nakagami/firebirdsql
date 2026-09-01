//go:build !plan9

package firebirdsql

import "testing"

// These fuzz targets exercise the wire-parse paths that consume
// server-supplied lengths/counts. The goal is the absence of panics: every
// guarded parser must return an error (or a zero value) on arbitrary input,
// never crash. CI runs the seed corpus on every `go test`.

func FuzzParseStatusVector(f *testing.F) {
	var sb statusBuf
	sb.gds(335544665)
	sb.str("INTEG_1")
	sb.sqlState("23000")
	sb.end()
	f.Add(sb.bytes())
	f.Add([]byte{})
	f.Add([]byte{0, 0, 0, 4, 0xFF, 0xFF, 0xFF, 0xFF}) // isc_arg_string, negative length

	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = testProtocol(data)._parse_status_vector()
	})
}

func FuzzParseOpResponse(f *testing.F) {
	var ok acceptFrame
	ok.int32(1)
	ok.buf.Write(make([]byte, 8))
	ok.blob([]byte("hello"))
	ok.int32(isc_arg_end)
	f.Add(ok.bytes())
	f.Add([]byte{})
	f.Add([]byte{0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0xFF, 0xFF, 0xFF, 0xFF}) // huge buf_len

	f.Fuzz(func(t *testing.T, data []byte) {
		_, _, _, _ = testProtocol(data)._parse_op_response()
	})
}

func FuzzXPBReader(f *testing.F) {
	f.Add([]byte{4, 0, 't', 'e', 's', 't'})
	f.Add([]byte{})
	f.Add([]byte{0xFF, 0xFF})

	f.Fuzz(func(t *testing.T, data []byte) {
		r := NewXPBReader(data)
		// Drive the operation sequence from the input itself so the fuzzer
		// explores interactions between read kinds, not a fixed drain order.
		for i := 0; i < len(data) && !r.End(); i++ {
			switch data[i] % 6 {
			case 0:
				r.Next()
			case 1:
				r.GetInt16()
			case 2:
				r.GetInt32()
			case 3:
				r.GetInt64()
			case 4:
				r.GetString()
			case 5:
				r.Skip(int(data[i]))
			}
		}
		_ = r.Err()
	})
}

func FuzzGuessWireCrypt(f *testing.F) {
	f.Add([]byte{1, 4, 'A', 'r', 'c', '4'})
	f.Add([]byte{})
	f.Add([]byte{3, 0xFF}) // tag with a length running past the buffer

	plugins := splitList(defaultWireCryptPlugins)
	f.Fuzz(func(t *testing.T, data []byte) {
		(&wireProtocol{})._guess_wire_crypt(data, plugins)
	})
}

func FuzzParseFirebirdVersion(f *testing.F) {
	f.Add("LI-V3.0.11.33703 Firebird 3.0")
	f.Add("")
	f.Add("garbage")

	f.Fuzz(func(t *testing.T, s string) {
		ParseFirebirdVersion(s)
	})
}

func FuzzGetEventCounts(f *testing.F) {
	f.Add([]byte{byte(EPB_version1), 3, 'e', 'v', 't', 1, 0, 0, 0})
	f.Add([]byte{})
	f.Add([]byte{byte(EPB_version1), 0xFF, 'x'})

	f.Fuzz(func(t *testing.T, data []byte) {
		e := newRemoteEvent()
		_ = e.queueEvents("evt")
		_, _ = e.getEventCounts(data)
	})
}

// FuzzParseDSN (Phase 9 of the Jaybird test port plan): parseDSN consumes
// attacker-controlled connection strings; malformed input must return an
// error, never panic.
func FuzzParseDSN(f *testing.F) {
	f.Add("user:password@localhost:3050/db.fdb")
	f.Add("firebird://user:pass@[2001:db8::1]/db.fdb?wire_crypt=required")
	f.Add("user:pass@localhost/db.fdb?charset=UTF8&wire_crypt=bogus")
	f.Add(":@/:")
	f.Add("firebird://")

	f.Fuzz(func(t *testing.T, s string) {
		_, _ = parseDSN(s)
	})
}
