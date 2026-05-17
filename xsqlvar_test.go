package firebirdsql

import (
	"bytes"
	"database/sql/driver"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestParamsToBlrNil(t *testing.T) {
	p := &wireProtocol{}
	blr, v := p.paramsToBlr(0, []driver.Value{nil}, PROTOCOL_VERSION13, nil)

	// BLR identical to calcBlr output for SQL_TYPE_NULL: both paths emit {blr_text, 0, 0}
	wantBlr := []byte{5, 2, 4, 0, 2, 0, 14, 0, 0, 7, 0, 255, 76}
	// V13 value payload: null bitmap only (bit 0 set), padded to 4 bytes
	wantV := []byte{1, 0, 0, 0}

	if !bytes.Equal(blr, wantBlr) {
		t.Errorf("BLR mismatch\n got:  %v\n want: %v", blr, wantBlr)
	}
	if !bytes.Equal(v, wantV) {
		t.Errorf("value mismatch\n got:  %v\n want: %v", v, wantV)
	}
}

func TestScaledIntValue(t *testing.T) {
	tests := []struct {
		name     string
		sqlscale int
		input    int64
		want     interface{}
	}{
		{"zero scale", 0, 42, int64(42)},
		{"positive scale 2", 2, 5, int64(500)},
		{"positive scale 3", 3, 7, int64(7000)},
		{"negative scale -3", -3, 1234, "1.234"},
		{"negative scale -2", -2, 50, "0.5"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			x := &xSQLVAR{sqlscale: tt.sqlscale}
			got := x.scaledIntValue(tt.input)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestScantypePositiveScale(t *testing.T) {
	wantInt64 := reflect.TypeOf(int64(0))
	wantString := reflect.TypeOf("")

	tests := []struct {
		name     string
		sqltype  int
		sqlscale int
		want     reflect.Type
	}{
		{"SHORT scale 0", SQL_TYPE_SHORT, 0, wantInt64},
		{"SHORT scale +2", SQL_TYPE_SHORT, 2, wantInt64},
		{"SHORT scale -3", SQL_TYPE_SHORT, -3, wantString},
		{"LONG scale 0", SQL_TYPE_LONG, 0, wantInt64},
		{"LONG scale +1", SQL_TYPE_LONG, 1, wantInt64},
		{"LONG scale -2", SQL_TYPE_LONG, -2, wantString},
		{"INT64 scale 0", SQL_TYPE_INT64, 0, wantInt64},
		{"INT64 scale +3", SQL_TYPE_INT64, 3, wantInt64},
		{"INT64 scale -4", SQL_TYPE_INT64, -4, wantString},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			x := &xSQLVAR{sqltype: tt.sqltype, sqlscale: tt.sqlscale}
			assert.Equal(t, tt.want, x.scantype())
		})
	}
}

func TestValuePositiveScale(t *testing.T) {
	tests := []struct {
		name     string
		sqltype  int
		sqlscale int
		rawValue []byte
		want     interface{}
	}{
		{
			"SHORT scale +2 value 5",
			SQL_TYPE_SHORT, 2,
			bint32_to_bytes(5),
			int64(500),
		},
		{
			"LONG scale +2 value 7",
			SQL_TYPE_LONG, 2,
			bint32_to_bytes(7),
			int64(700),
		},
		{
			"INT64 scale +1 value 3",
			SQL_TYPE_INT64, 1,
			bint64_to_bytes(3),
			int64(30),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			x := &xSQLVAR{sqltype: tt.sqltype, sqlscale: tt.sqlscale}
			got, err := x.value(tt.rawValue, "", "")
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
