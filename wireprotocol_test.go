package firebirdsql

import (
	"encoding/binary"
	"encoding/hex"
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// Unit tests for _parse_select_items and parse_xsqlda's describe-vars
// parsing of server-supplied lengths and column indices. No live Firebird
// server required.
// ---------------------------------------------------------------------------

// descVarsBuf builds a minimal describe-vars buffer:
// sqlda_seq=1, sql_type=sqltype, describe_end, isc_info_end.
func descVarsBuf(sqltype int32) []byte {
	buf := []byte{
		isc_info_sql_sqlda_seq, 0x04, 0x00, 0x01, 0x00, 0x00, 0x00, // seq=1
		isc_info_sql_type, 0x04, 0x00, // type tag + ln=4
	}
	buf = append(buf, int32_to_bytes(sqltype)...)
	return append(buf, isc_info_sql_describe_end, isc_info_end)
}

func TestParseSelectItems(t *testing.T) {
	wp := &wireProtocol{}

	cases := []struct {
		name         string
		buf          []byte
		slots        int
		wantIndex    int
		wantErr      string // substring; empty means no error
		checkSqltype int    // if > 0, assert xsqlda[0].sqltype == this value
		checkFn      func(*testing.T, []xSQLVAR)
	}{
		{
			name:      "empty buffer",
			buf:       []byte{},
			slots:     1,
			wantIndex: -1,
			wantErr:   "",
		},
		{
			name:      "only isc_info_end",
			buf:       []byte{isc_info_end},
			slots:     1,
			wantIndex: -1,
			wantErr:   "",
		},
		{
			name:      "truncated mid-item (tag with no length bytes)",
			buf:       []byte{isc_info_sql_sqlda_seq},
			slots:     1,
			wantIndex: -1,
			wantErr:   "truncated",
		},
		{
			name: "oversized length via unsigned overflow",
			// sqlda_seq tag, length bytes 0xFF 0xFF = uint16(65535), then isc_info_end:
			// far more than the one byte actually remaining in the buffer.
			buf:       []byte{isc_info_sql_sqlda_seq, 0xFF, 0xFF, isc_info_end},
			slots:     1,
			wantIndex: -1,
			wantErr:   "invalid describe-vars length",
		},
		{
			name: "unknown tag with valid length+payload is skipped, not rejected",
			// sqlda_seq=1 to set a valid index, then unknown tag 0xFE, ln=4;
			// a newer Firebird version may add item types this driver
			// predates, so an unrecognized tag must not abort the describe.
			buf: func() []byte {
				b := []byte{isc_info_sql_sqlda_seq, 0x04, 0x00}
				b = append(b, int32_to_bytes(1)...)
				return append(b, 0xFE, 0x04, 0x00, 0xAA, 0xBB, 0xCC, 0xDD, isc_info_end)
			}(),
			slots:     1,
			wantIndex: -1,
			wantErr:   "",
		},
		{
			name: "typed tag before sqlda_seq (index invalid guard)",
			// sql_type tag before any sqlda_seq → index still 0
			buf:       append([]byte{isc_info_sql_type, 0x04, 0x00}, append(int32_to_bytes(496), isc_info_end)...),
			slots:     1,
			wantIndex: -1,
			wantErr:   "invalid index",
		},
		{
			name:      "sqlda_seq out of range",
			buf:       append([]byte{isc_info_sql_sqlda_seq, 0x04, 0x00}, append(int32_to_bytes(5), isc_info_end)...),
			slots:     1,
			wantIndex: -1,
			wantErr:   "out of range",
		},
		{
			name:         "valid minimal input",
			buf:          descVarsBuf(496),
			slots:        1,
			wantIndex:    -1,
			wantErr:      "",
			checkSqltype: 496,
		},
		{
			name:      "isc_info_truncated returns next index",
			buf:       append([]byte{isc_info_sql_sqlda_seq, 0x04, 0x00}, append(int32_to_bytes(2), isc_info_truncated)...),
			slots:     2,
			wantIndex: 2,
			wantErr:   "",
		},
		{
			name: "multi-column sqlda_seq advances index",
			buf: func() []byte {
				b := []byte{
					isc_info_sql_sqlda_seq, 0x04, 0x00,
				}
				b = append(b, int32_to_bytes(1)...)
				b = append(b, isc_info_sql_type, 0x04, 0x00)
				b = append(b, int32_to_bytes(496)...)
				b = append(b, isc_info_sql_sqlda_seq, 0x04, 0x00)
				b = append(b, int32_to_bytes(2)...)
				b = append(b, isc_info_sql_type, 0x04, 0x00)
				b = append(b, int32_to_bytes(452)...)
				b = append(b, isc_info_sql_describe_end, isc_info_end)
				return b
			}(),
			slots:     2,
			wantIndex: -1,
			wantErr:   "",
			checkFn: func(t *testing.T, xsqlda []xSQLVAR) {
				if xsqlda[0].sqltype != 496 {
					t.Errorf("col1 sqltype: got %d, want 496", xsqlda[0].sqltype)
				}
				if xsqlda[1].sqltype != 452 {
					t.Errorf("col2 sqltype: got %d, want 452", xsqlda[1].sqltype)
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			xsqlda := make([]xSQLVAR, tc.slots)
			gotIndex, gotErr := wp._parse_select_items(tc.buf, xsqlda)

			if gotIndex != tc.wantIndex {
				t.Errorf("index: got %d, want %d", gotIndex, tc.wantIndex)
			}
			if tc.wantErr == "" {
				if gotErr != nil {
					t.Errorf("unexpected error: %v", gotErr)
				}
			} else {
				if gotErr == nil {
					t.Errorf("expected error containing %q, got nil", tc.wantErr)
				} else if !strings.Contains(gotErr.Error(), tc.wantErr) {
					t.Errorf("error %q does not contain %q", gotErr.Error(), tc.wantErr)
				}
			}
			if tc.checkSqltype > 0 && xsqlda[0].sqltype != tc.checkSqltype {
				t.Errorf("xsqlda[0].sqltype: got %d, want %d", xsqlda[0].sqltype, tc.checkSqltype)
			}
			if tc.checkFn != nil {
				tc.checkFn(t, xsqlda)
			}
		})
	}
}

func FuzzParseSelectItems(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{isc_info_end})
	f.Add([]byte{isc_info_sql_sqlda_seq, 0xFF, 0xFF, isc_info_end})
	f.Add([]byte{0xFE, 0x04, 0x00, 0xAA, 0xBB, 0xCC, 0xDD, isc_info_end})
	f.Add(descVarsBuf(496))
	// short sqlda_seq payload (ln=2 < 4)
	f.Add([]byte{isc_info_sql_sqlda_seq, 0x02, 0x00, 0xAA, 0xBB, isc_info_end})
	// string tag before any sqlda_seq (index=0 guard)
	f.Add([]byte{isc_info_sql_field, 0x03, 0x00, 'f', 'o', 'o', isc_info_end})

	f.Fuzz(func(t *testing.T, buf []byte) {
		wp := &wireProtocol{}
		xsqlda := make([]xSQLVAR, 4)
		idx, err := wp._parse_select_items(buf, xsqlda)
		if err == nil {
			if idx != -1 && (idx < 0 || idx > len(xsqlda)) {
				t.Fatalf("returned index %d out of bounds for slots=%d", idx, len(xsqlda))
			}
		}
	})
}

// parseXsqldaFrame builds the byte stream parse_xsqlda reads directly (it is
// called with the raw info-response buffer, not through opResponse/testProtocol).
func parseXsqldaFrame(colLen int32, items []byte) []byte {
	buf := []byte{
		isc_info_sql_stmt_type, 0x04, 0x00, 0x01, 0x00, 0x00, 0x00, // stmt_type=1
		isc_info_sql_select, isc_info_sql_describe_vars,
		0x04, 0x00, // ln=4
	}
	buf = append(buf, int32_to_bytes(colLen)...)
	buf = append(buf, items...)
	return buf
}

func TestParseXsqlda_BadColumnCount(t *testing.T) {
	wp := &wireProtocol{}
	cases := []struct {
		name   string
		colLen int32
	}{
		{"negative", -1},
		{"over cap", 65536},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			buf := parseXsqldaFrame(tt.colLen, []byte{isc_info_end})
			_, _, err := wp.parse_xsqlda(buf, 1)
			if err == nil {
				t.Fatal("expected error for malformed column count, got nil")
			}
			if !strings.Contains(err.Error(), "invalid select describe column count") {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestParseXsqlda_Valid(t *testing.T) {
	wp := &wireProtocol{}
	buf := parseXsqldaFrame(1, descVarsBuf(496))
	stmtType, xsqlda, err := wp.parse_xsqlda(buf, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stmtType != 1 {
		t.Errorf("stmtType: got %d, want 1", stmtType)
	}
	if len(xsqlda) != 1 || xsqlda[0].sqltype != 496 {
		t.Errorf("xsqlda: got %+v, want one column with sqltype 496", xsqlda)
	}
}

// TestParseXsqlda_TruncatedOrMalformed exercises the panic vectors a hostile
// or truncated Prepare response can hit: the outer header scan indexes
// buf[i+1]/buf[i+2] once the leading tag byte matches, and both the
// stmt_type and select-describe-vars branches slice buf[i:i+ln] using a
// server-controlled length before the byte count is known to be safe.
//
// Uses testProtocol (a real, discard-backed conn) rather than a bare
// &wireProtocol{}: a well-formed sqlda_seq followed by isc_info_truncated
// makes parse_xsqlda take its "more describe vars" continuation branch,
// which sends a real opInfoSql request — that's a legitimate protocol path
// requiring a connection, not a parser bug, and a bare wireProtocol's nil
// conn would nil-deref there instead of exercising the guards under test.
func TestParseXsqlda_TruncatedOrMalformed(t *testing.T) {
	cases := []struct {
		name string
		buf  []byte
	}{
		{"single stmt_type tag byte, no lookahead", []byte{isc_info_sql_stmt_type}},
		{"single select tag byte, no lookahead", []byte{isc_info_sql_select}},
		{"stmt_type tag with unexpected length header bytes falls through cleanly",
			[]byte{isc_info_sql_stmt_type, 0x05, 0x00}},
		{"stmt_type payload shorter than the buffer holds",
			[]byte{isc_info_sql_stmt_type, 0x04, 0x00, 0x01, 0x00}},
		{"select tag with missing second byte", []byte{isc_info_sql_select}},
		{"select tag with wrong second byte falls through cleanly",
			[]byte{isc_info_sql_select, 0xFF}},
		{"select describe-vars length truncated before the 2-byte field",
			[]byte{isc_info_sql_select, isc_info_sql_describe_vars, 0x04}},
		{"select describe-vars length reads negative as signed int16",
			[]byte{isc_info_sql_select, isc_info_sql_describe_vars, 0x00, 0x80}},
		{"select describe-vars col_len payload past end of buffer",
			[]byte{isc_info_sql_select, isc_info_sql_describe_vars, 0x04, 0x00, 0x01, 0x00}},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("parse_xsqlda panicked on %q: %v", tt.name, r)
				}
			}()
			wp := testProtocol(nil)
			_, _, _ = wp.parse_xsqlda(tt.buf, 1)
		})
	}
}

func FuzzParseXsqlda(f *testing.F) {
	f.Add(parseXsqldaFrame(1, descVarsBuf(496)))
	f.Add([]byte{isc_info_sql_stmt_type})
	f.Add([]byte{isc_info_sql_select})
	f.Add([]byte{isc_info_sql_select, isc_info_sql_describe_vars, 0x00, 0x80})
	f.Fuzz(func(t *testing.T, buf []byte) {
		// See TestParseXsqlda_TruncatedOrMalformed: a well-formed
		// sqlda_seq + isc_info_truncated can legitimately drive the
		// "more describe vars" continuation branch, which needs a
		// connection (testProtocol) rather than a bare wireProtocol.
		wp := testProtocol(nil)
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("parse_xsqlda panicked: %v", r)
			}
		}()
		_, _, _ = wp.parse_xsqlda(buf, 1)
	})
}

// TestAdvertisedProtocolRange mirrors Jaybird's ProtocolCollectionTest: the
// connect packet must offer protocol descriptors 10–19, one per version in
// ascending order with strictly increasing weights, arch type Generic (1) and
// min version 0; pflag_compress may appear in the max field of the 13+ records
// only when wire compression is enabled.
func TestAdvertisedProtocolRange(t *testing.T) {
	const pflagCompress = 0x100
	for _, wireCompress := range []bool{false, true} {
		records := advertisedProtocols(wireCompress)
		if len(records) != 10 {
			t.Fatalf("wireCompress=%v: %d protocol records, want 10", wireCompress, len(records))
		}
		prevWeight := uint32(0)
		for i, rec := range records {
			b, err := hex.DecodeString(rec)
			if err != nil || len(b) != 20 {
				t.Fatalf("record %d: bad hex record %q (len %d, err %v)", i, rec, len(b), err)
			}
			ver := binary.BigEndian.Uint32(b[0:4])
			arch := binary.BigEndian.Uint32(b[4:8])
			minV := binary.BigEndian.Uint32(b[8:12])
			maxV := binary.BigEndian.Uint32(b[12:16])
			weight := binary.BigEndian.Uint32(b[16:20])
			// Protocols 11+ carry Firebird's FB_PROTOCOL flag: 0x8000 set in
			// the low half and sign-extended into the upper half on the wire.
			wantVer := uint32(10 + i)
			if wantVer >= 11 {
				wantVer = 0xFFFF8000 | wantVer
			}
			if ver != wantVer {
				t.Errorf("record %d: protocol version %#x, want %#x", i, ver, wantVer)
			}
			if arch != 1 {
				t.Errorf("record %d (v%d): arch type %d, want 1 (Generic)", i, ver, arch)
			}
			if minV != 0 {
				t.Errorf("record %d (v%d): min version %d, want 0", i, ver, minV)
			}
			if weight <= prevWeight {
				t.Errorf("record %d (v%d): weight %d must exceed previous %d", i, ver, weight, prevWeight)
			}
			prevWeight = weight
			hasFlag := maxV&pflagCompress != 0
			if wireCompress && 10+i >= 13 && !hasFlag {
				t.Errorf("record %d (v%d): pflag_compress missing with wire_compress=true", i, 10+i)
			}
			if !wireCompress && hasFlag {
				t.Errorf("record %d (v%d): pflag_compress set with wire_compress=false", i, 10+i)
			}
		}
	}
}
