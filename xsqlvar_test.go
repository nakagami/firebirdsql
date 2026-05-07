package firebirdsql

import (
	"bytes"
	"testing"
)

func TestCalcBlr(t *testing.T) {
	tests := []struct {
		name  string
		input []xSQLVAR
		want  []byte
	}{
		{
			name:  "SQL_TYPE_NULL single column",
			input: []xSQLVAR{{sqltype: SQL_TYPE_NULL}},
			// header(6) + [blr_text,0,0](3) + [blr_short,0](2) + [blr_end,blr_eoc](2) = 13
			want: []byte{5, 2, 4, 0, 2, 0, 14, 0, 0, 7, 0, 255, 76},
		},
		{
			name: "SQL_TYPE_NULL then SQL_TYPE_LONG — alignment check",
			input: []xSQLVAR{
				{sqltype: SQL_TYPE_NULL},
				{sqltype: SQL_TYPE_LONG, sqlscale: 0},
			},
			// header(6) + [14,0,0,7,0](5) + [8,0,7,0](4) + [255,76](2) = 17
			want: []byte{5, 2, 4, 0, 4, 0, 14, 0, 0, 7, 0, 8, 0, 7, 0, 255, 76},
		},
		{
			name:  "SQL_TYPE_VARYING pins existing behavior",
			input: []xSQLVAR{{sqltype: SQL_TYPE_VARYING, sqllen: 100}},
			// header(6) + [blr_varying,100,0](3) + [blr_short,0](2) + [255,76](2) = 13
			want: []byte{5, 2, 4, 0, 2, 0, 37, 100, 0, 7, 0, 255, 76},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := calcBlr(tc.input)
			if !bytes.Equal(got, tc.want) {
				t.Errorf("calcBlr mismatch\n got:  %v\n want: %v", got, tc.want)
			}
		})
	}
}
