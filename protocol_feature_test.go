/*******************************************************************************
The MIT License (MIT)

Copyright (c) 2026 Alexey Kovyazin

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
	"bufio"
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/binary"
	"testing"
)

func TestAppendExecuteTrailers(t *testing.T) {
	cases := []struct {
		name    string
		version int32
		flags   int32
		inline  int32
		wantLen int // trailer bytes after the classic execute body
	}{
		{"v15", 15, 0, 0, 0},
		{"v16", PROTOCOL_VERSION16, 0, 0, 4},
		{"v18", PROTOCOL_VERSION18, CURSOR_TYPE_SCROLLABLE, 0, 8},
		{"v19", PROTOCOL_VERSION19, 0, 65536, 12},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			p := &wireProtocol{protocolVersion: tt.version, cursorFlags: tt.flags, maxInlineBlobSize: tt.inline}
			before := len(p.buf)
			p.appendExecuteTrailers()
			got := p.buf[before:]
			if len(got) != tt.wantLen {
				t.Fatalf("trailer len=%d, want %d (%x)", len(got), tt.wantLen, got)
			}
			off := 0
			if tt.version >= PROTOCOL_VERSION16 {
				if binary.BigEndian.Uint32(got[off:off+4]) != 0 {
					t.Fatalf("timeout field=%x, want 0", got[off:off+4])
				}
				off += 4
			}
			if tt.version >= PROTOCOL_VERSION18 {
				if int32(binary.BigEndian.Uint32(got[off:off+4])) != tt.flags {
					t.Fatalf("cursor_flags=%x, want %d", got[off:off+4], tt.flags)
				}
				off += 4
			}
			if tt.version >= PROTOCOL_VERSION19 {
				if int32(binary.BigEndian.Uint32(got[off:off+4])) != tt.inline {
					t.Fatalf("inline_blob_size=%x, want %d", got[off:off+4], tt.inline)
				}
			}
		})
	}
}

func TestOpFetchScrollPacket(t *testing.T) {
	p := testProtocol(nil)
	var out bytes.Buffer
	p.conn.writer = bufio.NewWriter(&out)
	// Every field differs from the others, so that any swap changes the bytes.
	if err := p.opFetchScroll(7, []byte{1, 2, 3}, ScrollAbsolute, -5, 9); err != nil {
		t.Fatal(err)
	}
	want := words(
		op_fetch_scroll,
		7,          // statement handle
		3,          // BLR length
		0x01020300, // BLR plus one XDR pad byte
		0,          // message number
		9,          // count
		ScrollAbsolute,
		-5, // offset
	)
	if !bytes.Equal(out.Bytes(), want) {
		t.Fatalf("op_fetch_scroll packet=%x, want %x", out.Bytes(), want)
	}
	if len(p.buf) != 0 {
		t.Fatalf("send buffer not reset: %x", p.buf)
	}
}

// The Raw-extension signatures documented for sql.Conn.Raw callers, who can
// only reach them through an anonymous interface. A change must break the build.
var _ interface {
	QueryScrollable(context.Context, string, ...driver.Value) (*ScrollableStmt, error)
	PrepareBatch(context.Context, string, BatchOptions) (*PreparedBatch, error)
	ExecImmediate(context.Context, string) error
	WireCipher() string
	ProtocolVersion() int
} = (*firebirdsqlConn)(nil)

func TestDecodeInlineSegmentBuffer(t *testing.T) {
	raw := []byte{3, 0, 'a', 'b', 'c', 2, 0, 'd', 'e'}
	got, err := decodeInlineSegmentBuffer(append([]byte(nil), raw...))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "abcde" {
		t.Fatalf("got %q", got)
	}
}

func TestInlineBlobCache(t *testing.T) {
	c := newInlineBlobCache(100)
	if !c.add(1, 42, []byte("hello")) {
		t.Fatal("add failed")
	}
	data, ok := c.getAndRemove(1, 42)
	if !ok || string(data) != "hello" {
		t.Fatalf("get=%q ok=%v", data, ok)
	}
	if _, ok := c.getAndRemove(1, 42); ok {
		t.Fatal("expected miss after remove")
	}
	c.add(1, 1, []byte("x"))
	c.clearTx(1)
	if _, ok := c.getAndRemove(1, 1); ok {
		t.Fatal("expected miss after clearTx")
	}
}

func TestConsumeInlineBlobBeforeFetchResponse(t *testing.T) {
	var f acceptFrame
	// op_inline_blob
	f.int32(op_inline_blob)
	f.int32(9) // tx
	_ = binary.Write(&f.buf, binary.BigEndian, int64(0x0102030405060708))
	f.blob(nil) // info
	seg := []byte{4, 0, 't', 'e', 's', 't'}
	f.blob(seg) // data with segment headers
	// op_fetch_response: status=100 (end), count=0
	f.int32(op_fetch_response)
	f.int32(100)
	f.int32(0)

	p := testProtocol(f.bytes())
	p.inlineBlobCache = newInlineBlobCache(1024)
	rows, more, err := p.opFetchResponse(1, 9, nil)
	if err != nil {
		t.Fatal(err)
	}
	if more {
		t.Fatal("expected no more data")
	}
	if len(rows) != 0 {
		t.Fatalf("rows=%d", len(rows))
	}
	data, ok := p.inlineBlobCache.getAndRemove(9, 0x0102030405060708)
	if !ok || string(data) != "test" {
		t.Fatalf("cached=%q ok=%v", data, ok)
	}
}

func TestConsumeInlineBlobBeforeSqlResponse(t *testing.T) {
	var f acceptFrame
	f.int32(op_inline_blob)
	f.int32(3)
	_ = binary.Write(&f.buf, binary.BigEndian, int64(99))
	f.blob(nil)
	f.blob([]byte{1, 0, 'Z'})
	f.int32(op_sql_response)
	f.int32(0) // count 0

	p := testProtocol(f.bytes())
	p.inlineBlobCache = newInlineBlobCache(1024)
	row, err := p.opSqlResponse(nil)
	if err != nil {
		t.Fatal(err)
	}
	if row != nil {
		t.Fatalf("expected nil row, got %v", row)
	}
	data, ok := p.inlineBlobCache.getAndRemove(3, 99)
	if !ok || string(data) != "Z" {
		t.Fatalf("cached=%q ok=%v", data, ok)
	}
}

// TestInlineBlobEmptyPayloadIsNotNil: a zero-length inline BLOB is an empty
// value, not NULL; database/sql keeps nil and []byte{} apart.
func TestInlineBlobEmptyPayloadIsNotNil(t *testing.T) {
	var f acceptFrame
	f.int32(op_inline_blob)
	f.int32(9) // tx
	_ = binary.Write(&f.buf, binary.BigEndian, int64(0x0102030405060708))
	f.blob(nil) // info
	f.blob(nil) // data: zero length
	f.int32(op_fetch_response)
	f.int32(100)
	f.int32(0)

	p := testProtocol(f.bytes())
	p.inlineBlobCache = newInlineBlobCache(1024)
	if _, _, err := p.opFetchResponse(1, 9, nil); err != nil {
		t.Fatal(err)
	}
	data, err := p.getBlobSegments(bint64_to_bytes(0x0102030405060708), 9)
	if err != nil {
		t.Fatal(err)
	}
	if data == nil {
		t.Fatal("data == nil, want empty non-nil slice")
	}
	if len(data) != 0 {
		t.Fatalf("len(data)=%d, want 0", len(data))
	}
}

// TestInlineBlobEmptyPayloadIsNotNilSqlResponse is the same check on the
// op_sql_response path used by EXECUTE PROCEDURE.
func TestInlineBlobEmptyPayloadIsNotNilSqlResponse(t *testing.T) {
	var f acceptFrame
	f.int32(op_inline_blob)
	f.int32(3)
	_ = binary.Write(&f.buf, binary.BigEndian, int64(99))
	f.blob(nil)
	f.blob(nil)
	f.int32(op_sql_response)
	f.int32(0) // count 0

	p := testProtocol(f.bytes())
	p.inlineBlobCache = newInlineBlobCache(1024)
	if _, err := p.opSqlResponse(nil); err != nil {
		t.Fatal(err)
	}
	data, err := p.getBlobSegments(bint64_to_bytes(99), 3)
	if err != nil {
		t.Fatal(err)
	}
	if data == nil {
		t.Fatal("data == nil, want empty non-nil slice")
	}
	if len(data) != 0 {
		t.Fatalf("len(data)=%d, want 0", len(data))
	}
}

func TestGetBlobSegmentsUsesInlineCache(t *testing.T) {
	p := testProtocol(nil)
	p.inlineBlobCache = newInlineBlobCache(1024)
	id := bint64_to_bytes(55)
	p.inlineBlobCache.add(2, 55, []byte("cached-blob"))
	got, err := p.getBlobSegments(id, 2)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "cached-blob" {
		t.Fatalf("got %q", got)
	}
}

func TestParseDSNInlineBlobOptions(t *testing.T) {
	dsn, err := parseDSN("sysdba:masterkey@localhost/db?max_inline_blob_size=100&max_blob_cache_size=200")
	if err != nil {
		t.Fatal(err)
	}
	if dsn.options["max_inline_blob_size"] != "100" || dsn.options["max_blob_cache_size"] != "200" {
		t.Fatalf("options=%v", dsn.options)
	}
	dsn2, err := parseDSN("sysdba:masterkey@localhost/db?inline_blob_size=7&blob_cache_size=9")
	if err != nil {
		t.Fatal(err)
	}
	if dsn2.options["max_inline_blob_size"] != "7" || dsn2.options["max_blob_cache_size"] != "9" {
		t.Fatalf("alias options=%v", dsn2.options)
	}
}

func TestAppendInlineBlobDPB(t *testing.T) {
	p := &wireProtocol{protocolVersion: PROTOCOL_VERSION19, maxInlineBlobSize: 10, maxBlobCacheSize: 20}
	dpb := p.appendInlineBlobDPB([]byte{isc_dpb_version1})
	if !bytes.Contains(dpb, []byte{isc_dpb_max_inline_blob_size, 4}) {
		t.Fatalf("missing max_inline_blob_size tag in %x", dpb)
	}
	p.protocolVersion = PROTOCOL_VERSION18
	dpb2 := p.appendInlineBlobDPB([]byte{isc_dpb_version1})
	if bytes.Contains(dpb2, []byte{isc_dpb_max_inline_blob_size}) {
		t.Fatal("should not append DPB items below protocol 19")
	}
}

// dpbItems walks an isc_dpb_version1 buffer (tag, one-byte length, value) and
// returns the value of every item by tag.
func dpbItems(t *testing.T, dpb []byte) map[byte][]byte {
	t.Helper()
	if len(dpb) == 0 || dpb[0] != isc_dpb_version1 {
		t.Fatalf("DPB does not start with isc_dpb_version1: %x", dpb)
	}
	items := make(map[byte][]byte)
	for i := 1; i < len(dpb); {
		if i+2 > len(dpb) || i+2+int(dpb[i+1]) > len(dpb) {
			t.Fatalf("truncated DPB item at offset %d: %x", i, dpb)
		}
		tag, n := dpb[i], int(dpb[i+1])
		items[tag] = dpb[i+2 : i+2+n]
		i += 2 + n
	}
	return items
}

func TestCreateDPBPageSize(t *testing.T) {
	for _, tc := range []struct {
		name     string
		pageSize int32
		want     []byte // nil: no isc_dpb_page_size item
	}{
		{"server default", 0, nil},
		{"4096", 4096, []byte{0x00, 0x10, 0x00, 0x00}},
		{"16384", 16384, []byte{0x00, 0x40, 0x00, 0x00}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Protocol 19, auth data and a time zone add items after the
			// page size, so the walk covers the whole DPB.
			p := &wireProtocol{
				charset:           "UTF8",
				pageSize:          tc.pageSize,
				authData:          []byte{0xab, 0xcd},
				timezone:          "Europe/Berlin",
				protocolVersion:   PROTOCOL_VERSION19,
				maxInlineBlobSize: 65536,
				maxBlobCacheSize:  10485760,
			}
			items := dpbItems(t, p.createDPB("sysdba", "masterkey", ""))
			got, ok := items[isc_dpb_page_size]
			if tc.want == nil {
				if ok {
					t.Fatalf("isc_dpb_page_size = %x, want no item", got)
				}
			} else if !bytes.Equal(got, tc.want) {
				t.Fatalf("isc_dpb_page_size = %x (present %v), want %x", got, ok, tc.want)
			}
			for _, tag := range []byte{isc_dpb_user_name, isc_dpb_overwrite, isc_dpb_utf8_filename,
				isc_dpb_specific_auth_data, isc_dpb_session_time_zone, isc_dpb_max_blob_cache_size} {
				if _, ok := items[tag]; !ok {
					t.Errorf("DPB item %d missing", tag)
				}
			}
		})
	}
}

// TestCreateDPBUnchangedWithPageSize pins the op_create DPB for an explicit
// page size to the bytes sent before the page size became optional.
func TestCreateDPBUnchangedWithPageSize(t *testing.T) {
	p := &wireProtocol{charset: "UTF8", pageSize: 4096}
	want := bytes.Join([][]byte{
		{isc_dpb_version1},
		{isc_dpb_set_db_charset, 4}, []byte("UTF8"),
		{isc_dpb_lc_ctype, 4}, []byte("UTF8"),
		{isc_dpb_user_name, 6}, []byte("SYSDBA"),
		{isc_dpb_password, 9}, []byte("masterkey"),
		{isc_dpb_sql_role_name, 0},
		{isc_dpb_sql_dialect, 4}, int32_to_bytes(3),
		{isc_dpb_force_write, 4}, bint32_to_bytes(1),
		{isc_dpb_overwrite, 4}, bint32_to_bytes(1),
		{isc_dpb_page_size, 4}, int32_to_bytes(4096),
		{isc_dpb_utf8_filename, 1, 1},
	}, nil)
	if got := p.createDPB("sysdba", "masterkey", ""); !bytes.Equal(got, want) {
		t.Fatalf("createDPB =\n%x\nwant\n%x", got, want)
	}
}
