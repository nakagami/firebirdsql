package firebirdsql

import (
	"strings"
	"testing"
)

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
			name: "negative length via signed int16 overflow",
			// sqlda_seq tag, length bytes 0xFF 0xFF = int16(-1), then isc_info_end
			buf:       []byte{isc_info_sql_sqlda_seq, 0xFF, 0xFF, isc_info_end},
			slots:     1,
			wantIndex: -1,
			wantErr:   "invalid describe-vars length",
		},
		{
			name: "unknown tag with valid length+payload",
			// sqlda_seq=1 to set a valid index, then unknown tag 0xFE, ln=4
			buf: func() []byte {
				b := []byte{isc_info_sql_sqlda_seq, 0x04, 0x00}
				b = append(b, int32_to_bytes(1)...)
				return append(b, 0xFE, 0x04, 0x00, 0xAA, 0xBB, 0xCC, 0xDD, isc_info_end)
			}(),
			slots:     1,
			wantIndex: -1,
			wantErr:   "unknown describe-vars item 0xfe",
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
			name:      "isc_info_truncated returns non-zero index",
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
