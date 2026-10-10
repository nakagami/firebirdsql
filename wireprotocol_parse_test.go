package firebirdsql

import (
	"bytes"
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"io"
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// Unit tests for wire-protocol parsing of server-supplied lengths.
// No live Firebird server required.
// ---------------------------------------------------------------------------

func TestParseStatusVector_BadStringLength(t *testing.T) {
	cases := []struct {
		name   string
		length int32
	}{
		{"negative", -1},
		{"over cap", maxWirePayload + 1},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			var sb statusBuf
			sb.gds(335544665)
			sb.tag(isc_arg_string)
			sb.int32(tt.length) // claimed string length; no payload follows

			_, err := testProtocol(sb.bytes())._parse_status_vector()
			if err == nil {
				t.Fatal("expected error for malformed string length, got nil")
			}
			if !strings.Contains(err.Error(), "out of range") {
				t.Errorf("unexpected error: %v", err)
			}
			// The wire is desynced; the conn must be evicted from the pool.
			if !errors.Is(err, driver.ErrBadConn) {
				t.Errorf("error should report a bad connection, got: %v", err)
			}
		})
	}
}

func TestParseStatusVector_TruncatedString(t *testing.T) {
	var sb statusBuf
	sb.gds(335544665)
	sb.tag(isc_arg_string)
	sb.int32(100) // claims 100 bytes but the stream ends here

	_, err := testProtocol(sb.bytes())._parse_status_vector()
	if err == nil {
		t.Fatal("expected error for truncated string payload, got nil")
	}
}

func TestParseOpResponse_BadBufferLength(t *testing.T) {
	cases := []struct {
		name   string
		length int32
	}{
		{"negative", -1},
		{"over cap", maxWirePayload + 1},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			var f acceptFrame
			f.int32(7)                   // object handle
			f.buf.Write(make([]byte, 8)) // object id
			f.int32(tt.length)           // claimed buffer length; nothing follows

			_, _, _, err := testProtocol(f.bytes())._parse_op_response()
			if err == nil {
				t.Fatal("expected error for malformed buffer length, got nil")
			}
			if !strings.Contains(err.Error(), "out of range") {
				t.Errorf("unexpected error: %v", err)
			}
			if !errors.Is(err, driver.ErrBadConn) {
				t.Errorf("error should report a bad connection, got: %v", err)
			}
		})
	}
}

func TestOpFetchResponse_ValidRows(t *testing.T) {
	var f acceptFrame
	f.int32(op_fetch_response)
	f.int32(0)                         // status: cursor still open
	f.int32(1)                         // one row in this chunk
	f.buf.Write([]byte{0x00, 0, 0, 0}) // null bitmap (V13+), aligned
	f.buf.Write([]byte{42, 0, 0, 0})   // SQL_LONG value, little-endian
	f.int32(op_fetch_response)
	f.int32(100) // status: end of cursor
	f.int32(0)

	p := testProtocol(f.bytes())
	p.protocolVersion = PROTOCOL_VERSION13
	rows, more, err := p.opFetchResponse(1, 1, []xSQLVAR{{sqltype: SQL_TYPE_LONG}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("rows: got %d, want 1", len(rows))
	}
	if more {
		t.Error("more: got true, want false")
	}
}

func TestOpFetchResponse_NegativeRowCount(t *testing.T) {
	var f acceptFrame
	f.int32(op_fetch_response)
	f.int32(0)  // status
	f.int32(-1) // claimed row count

	_, _, err := testProtocol(f.bytes()).opFetchResponse(1, 1, nil)
	if err == nil {
		t.Fatal("expected error for negative row count, got nil")
	}
	if !strings.Contains(err.Error(), "out of range") {
		t.Errorf("unexpected error: %v", err)
	}
	if !errors.Is(err, driver.ErrBadConn) {
		t.Errorf("error should report a bad connection, got: %v", err)
	}
}

func TestOpFetchResponse_HugeRowCount(t *testing.T) {
	// An absurd claimed count must not drive the pre-allocation; the loop is
	// data-driven, so the truncated stream surfaces as a clean read error.
	var f acceptFrame
	f.int32(op_fetch_response)
	f.int32(0)
	f.int32(0x7FFFFFFF)

	p := testProtocol(f.bytes())
	p.protocolVersion = PROTOCOL_VERSION13
	_, _, err := p.opFetchResponse(1, 1, []xSQLVAR{{sqltype: SQL_TYPE_LONG}})
	if err == nil {
		t.Fatal("expected error for truncated stream, got nil")
	}
}

func TestReadRow_BadColumnLength(t *testing.T) {
	cases := []struct {
		name string
		ln   int32
	}{
		{"negative", -5},
		{"over cap", maxWirePayload + 1},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			var f acceptFrame
			f.buf.Write([]byte{0x00, 0, 0, 0}) // null bitmap: column present
			f.int32(tt.ln)                     // claimed varying-column length

			p := testProtocol(f.bytes())
			p.protocolVersion = PROTOCOL_VERSION13
			_, err := p.readRow([]xSQLVAR{{sqltype: SQL_TYPE_VARYING}})
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !strings.Contains(err.Error(), "out of range") {
				t.Errorf("unexpected error: %v", err)
			}
			if !errors.Is(err, driver.ErrBadConn) {
				t.Errorf("error should report a bad connection, got: %v", err)
			}
		})
	}
}

// acceptFrame builds the byte stream of a connect-response packet
// (op_accept_data and friends) as the server would send it.
type acceptFrame struct{ buf bytes.Buffer }

func (f *acceptFrame) int32(v int32) { _ = binary.Write(&f.buf, binary.BigEndian, v) }

// blob writes a 4-byte length-prefixed field with 4-byte alignment padding.
func (f *acceptFrame) blob(b []byte) {
	f.int32(int32(len(b)))
	f.buf.Write(b)
	if pad := (4 - len(b)%4) % 4; pad > 0 {
		f.buf.Write(make([]byte, pad))
	}
}

// acceptHeader writes the opcode plus the 12-byte version/architecture/type block.
func (f *acceptFrame) acceptHeader(opcode int32) {
	f.int32(opcode)
	f.int32(PROTOCOL_VERSION13) // version byte ends up in b[3]
	f.int32(1)                  // accept architecture
	f.int32(0)                  // accept type (no compression flag)
}

func (f *acceptFrame) bytes() []byte { return f.buf.Bytes() }

func testConnectOptions() map[string]string {
	return map[string]string{
		"auth_plugin_name":  "Srp256",
		"auth_plugin_list":  defaultAuthPlugins,
		"wire_crypt":        "true",
		"wire_crypt_plugin": defaultWireCryptPlugins,
	}
}

// srpServerData builds the auth-data payload of an op_accept_data response:
// 2-byte little-endian salt length, salt, 2-byte key length, hex-encoded key.
func srpServerData(saltLen int, keyHexLen int) []byte {
	data := []byte{byte(saltLen & 0xFF), byte(saltLen >> 8)}
	salt := bytes.Repeat([]byte{0x01}, saltLen)
	data = append(data, salt...)
	data = append(data, byte(keyHexLen&0xFF), byte(keyHexLen>>8))
	data = append(data, bytes.Repeat([]byte{'a'}, keyHexLen)...)
	return data
}

func parseConnectResponse(t *testing.T, frame []byte) error {
	t.Helper()
	p := testProtocol(frame)
	clientPublic, clientSecret, err := getClientSeed()
	if err != nil {
		t.Fatalf("getClientSeed: %v", err)
	}
	return p._parse_connect_response("SYSDBA", "masterkey", testConnectOptions(), clientPublic, clientSecret)
}

// opResponseFrame appends one op_response packet: object handle, object id,
// length-prefixed data buffer, and a status vector with the given entries
// (empty = success).
func (f *acceptFrame) opResponseFrame(handle int32, data []byte, statusVector ...int32) {
	f.int32(op_response)
	f.int32(handle)
	f.buf.Write(make([]byte, 8)) // object id
	f.blob(data)
	for _, v := range statusVector {
		f.int32(v)
	}
	f.int32(isc_arg_end)
}

func TestGetBlobSegments_Valid(t *testing.T) {
	var f acceptFrame
	f.opResponseFrame(1, nil) // op_open_blob2 response: blob handle 1
	// Final op_get_segment response (handle 2 = blob end) legitimately carries
	// several length-prefixed segments concatenated in one buffer.
	f.opResponseFrame(2, []byte{3, 0, 'a', 'b', 'c', 2, 0, 'd', 'e'})
	f.opResponseFrame(0, nil) // op_close_blob response

	blob, err := testProtocol(f.bytes()).getBlobSegments(make([]byte, 8), 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(blob) != "abcde" {
		t.Errorf("blob: got %q, want %q", blob, "abcde")
	}
}

func TestGetBlobSegments_MalformedSegment(t *testing.T) {
	cases := []struct {
		name    string
		payload []byte
		wantSub string
	}{
		{"length past end", []byte{5, 0, 'a'}, "exceeds response"},
		{"truncated header", []byte{7}, "truncated"},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			var f acceptFrame
			f.opResponseFrame(1, nil)
			f.opResponseFrame(2, tt.payload)

			p := testProtocol(f.bytes())
			sentinel := []byte{0xAA, 0xBB}
			p.buf = sentinel

			_, err := p.getBlobSegments(make([]byte, 8), 1)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !strings.Contains(err.Error(), tt.wantSub) {
				t.Errorf("unexpected error: %v", err)
			}
			if !errors.Is(err, driver.ErrBadConn) {
				t.Errorf("error should report a bad connection, got: %v", err)
			}
			// The suspended send buffer must be restored on every error path.
			if !bytes.Equal(p.buf, sentinel) {
				t.Errorf("wire buffer not restored: %v", p.buf)
			}
		})
	}
}

func TestGetBlobSegments_ServerErrorMidStream(t *testing.T) {
	var f acceptFrame
	f.opResponseFrame(1, nil)
	// op_get_segment response whose status vector carries a server error;
	// previously the loop ignored it and re-issued op_get_segment forever.
	f.opResponseFrame(0, nil, isc_arg_gds, 335544336 /* deadlock */)

	p := testProtocol(f.bytes())
	sentinel := []byte{0xCC}
	p.buf = sentinel

	_, err := p.getBlobSegments(make([]byte, 8), 1)
	var fbErr *FbError
	if !errors.As(err, &fbErr) {
		t.Fatalf("want *FbError, got %T: %v", err, err)
	}
	// The error frame was fully parsed, so the connection stays usable.
	if errors.Is(err, driver.ErrBadConn) {
		t.Errorf("clean server error must not poison the connection: %v", err)
	}
	if !bytes.Equal(p.buf, sentinel) {
		t.Errorf("wire buffer not restored: %v", p.buf)
	}
}

func TestParseConnectResponse_ValidSRPAcceptData(t *testing.T) {
	var f acceptFrame
	f.acceptHeader(op_accept_data)
	f.blob(srpServerData(32, 256)) // 292-byte blob: the shape real servers send
	f.blob([]byte("Srp256"))
	f.int32(0) // not yet authenticated
	f.blob(nil)

	if err := parseConnectResponse(t, f.bytes()); err != nil {
		t.Fatalf("legitimate accept frame rejected: %v", err)
	}
}

func TestParseConnectResponse_NegativeBlobLength(t *testing.T) {
	var f acceptFrame
	f.acceptHeader(op_accept_data)
	f.int32(-1) // server claims a negative auth-data length

	err := parseConnectResponse(t, f.bytes())
	if err == nil {
		t.Fatal("expected error for negative blob length, got nil")
	}
	if !strings.Contains(err.Error(), "out of range") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestParseConnectResponse_OversizedBlobLength(t *testing.T) {
	var f acceptFrame
	f.acceptHeader(op_accept_data)
	f.int32(maxWirePayload + 1) // absurd server-claimed length must not be allocated

	err := parseConnectResponse(t, f.bytes())
	if err == nil {
		t.Fatal("expected error for oversized blob length, got nil")
	}
	if !strings.Contains(err.Error(), "out of range") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestParseConnectResponse_TruncatedAcceptFields(t *testing.T) {
	var f acceptFrame
	f.acceptHeader(op_accept_data)
	// Stream ends before the first length word: the read error must propagate
	// instead of being dropped.
	if err := parseConnectResponse(t, f.bytes()); err == nil {
		t.Fatal("expected error for truncated accept frame, got nil")
	}
}

// The connection closes before the server's answer is complete: a server
// that accepted the socket and died, or a proxy in front of a server that is
// down. The read error must reach the caller as it is, so that a dropped
// connection (io.EOF, a net.Error) can be told from a protocol violation.
func TestParseConnectResponse_ConnectionClosed(t *testing.T) {
	truncatedAccept := func() []byte {
		var f acceptFrame
		f.int32(op_accept)
		f.int32(PROTOCOL_VERSION13) // 4 of the 12 header bytes
		return f.bytes()
	}()
	afterDummy := func() []byte {
		var f acceptFrame
		f.int32(op_dummy)
		return f.bytes()
	}()
	cases := []struct {
		name  string
		frame []byte
	}{
		{"no answer", nil},
		{"after op_dummy", afterDummy},
		{"truncated op_accept header", truncatedAccept},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			err := parseConnectResponse(t, tt.frame)
			if !errors.Is(err, io.EOF) {
				t.Fatalf("want io.EOF, got %v", err)
			}
		})
	}
}

func TestParseConnectResponse_SRPEmptyServerKey(t *testing.T) {
	// Salt present but no key material after it: the key hex decodes to a
	// zero public key, which must flow through the client proof computation
	// without panicking (the zero-skip scan in pad is bounded).
	var f acceptFrame
	f.acceptHeader(op_accept_data)
	f.blob(srpServerData(32, 0))
	f.blob([]byte("Srp256"))
	f.int32(0)
	f.blob(nil)

	if err := parseConnectResponse(t, f.bytes()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestParseConnectResponse_SRPDataTooShort(t *testing.T) {
	var f acceptFrame
	f.acceptHeader(op_accept_data)
	f.blob([]byte{0x20}) // 1 byte: shorter than the salt-length field itself
	f.blob([]byte("Srp256"))
	f.int32(0)
	f.blob(nil)

	err := parseConnectResponse(t, f.bytes())
	if err == nil {
		t.Fatal("expected error for short SRP data, got nil")
	}
	if !strings.Contains(err.Error(), "too short") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestParseConnectResponse_SRPSaltLengthInvalid(t *testing.T) {
	cases := []struct {
		name string
		data []byte
	}{
		{"negative salt length", []byte{0xFF, 0xFF}},
		{"salt length past end", []byte{100, 0, 'x', 'x'}},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			var f acceptFrame
			f.acceptHeader(op_accept_data)
			f.blob(tt.data)
			f.blob([]byte("Srp256"))
			f.int32(0)
			f.blob(nil)

			err := parseConnectResponse(t, f.bytes())
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !strings.Contains(err.Error(), "salt length") {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}
