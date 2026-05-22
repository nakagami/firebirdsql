package firebirdsql

import (
	"bytes"
	"database/sql/driver"
	"math/big"
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
		{"negative scale -2", -2, 50, "0.50"},
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

func TestScaledIntValueTrailingZeros(t *testing.T) {
	tests := []struct {
		name     string
		sqlscale int
		input    int64
		want     interface{}
	}{
		{"12.00 as NUMERIC(9,2)", -2, 1200, "12.00"},
		{"0.50 as NUMERIC(9,2)", -2, 50, "0.50"},
		{"-12.00 as NUMERIC(9,2)", -2, -1200, "-12.00"},
		{"123.450 as NUMERIC(18,3)", -3, 123450, "123.450"},
		{"0.0050 as NUMERIC(18,4)", -4, 50, "0.0050"},
		{"42 as INTEGER (zero-scale pass-through)", 0, 42, int64(42)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			x := &xSQLVAR{sqlscale: tt.sqlscale}
			got := x.scaledIntValue(tt.input)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestValueInt128(t *testing.T) {
	ff16 := bytes.Repeat([]byte{0xFF}, 16)
	zero16 := bytes.Repeat([]byte{0x00}, 16)

	one := append(bytes.Repeat([]byte{0x00}, 15), 0x01)
	maxPos := append([]byte{0x7F}, bytes.Repeat([]byte{0xFF}, 15)...)
	minNeg := append([]byte{0x80}, bytes.Repeat([]byte{0x00}, 15)...)
	nearMinNeg := append(append([]byte{0x80}, bytes.Repeat([]byte{0x00}, 14)...), 0x01)
	pos256 := append(append(bytes.Repeat([]byte{0x00}, 14), 0x01), 0x00)
	neg256 := append(bytes.Repeat([]byte{0xFF}, 15), 0x00)

	tests := []struct {
		name     string
		rawValue []byte
		want     string
	}{
		{"zero", zero16, "0"},
		{"one", one, "1"},
		{"minus one", ff16, "-1"},
		{"max positive 2^127-1", maxPos, "170141183460469231731687303715884105727"},
		{"min negative -2^127", minNeg, "-170141183460469231731687303715884105728"},
		{"near-min negative -(2^127-1)", nearMinNeg, "-170141183460469231731687303715884105727"},
		{"small positive 256", pos256, "256"},
		{"small negative -256", neg256, "-256"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			x := &xSQLVAR{sqltype: SQL_TYPE_INT128}
			got, err := x.value(tt.rawValue, "", "")
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

// int128Bytes encodes n as a 16-byte big-endian two's-complement value, the
// on-wire representation Firebird sends for SQL_TYPE_INT128.
func int128Bytes(n *big.Int) []byte {
	out := make([]byte, 16)
	if n.Sign() >= 0 {
		b := n.Bytes()
		copy(out[16-len(b):], b)
		return out
	}
	bias := new(big.Int).Lsh(big.NewInt(1), 128)
	b := new(big.Int).Add(n, bias).Bytes()
	copy(out[16-len(b):], b)
	return out
}

func TestValueInt128WithScale(t *testing.T) {
	maxPos := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 127), big.NewInt(1))
	minNeg := new(big.Int).Neg(new(big.Int).Lsh(big.NewInt(1), 127))

	tests := []struct {
		name     string
		value    *big.Int
		sqlscale int
		want     string
	}{
		// Zero scale — current behavior must be preserved (TestInt128/TestNegativeInt128 path).
		{"scale 0, +1", big.NewInt(1), 0, "1"},
		{"scale 0, -1", big.NewInt(-1), 0, "-1"},

		// Negative scale, formatPlain branch (exponent <= 0 && adjexp >= -6).
		// Trailing zeros are preserved per IEEE 754 cohort quantum, matching #268.
		{"scale -5, +12345000 → 123.45000", big.NewInt(12345000), -5, "123.45000"},
		{"scale -5, -12345000 → -123.45000", big.NewInt(-12345000), -5, "-123.45000"},
		{"scale -2, +99 → 0.99", big.NewInt(99), -2, "0.99"},
		{"scale -2, -99 → -0.99", big.NewInt(-99), -2, "-0.99"},

		// Zero coefficient at negative scale — IEEE 754 cohort quantum:
		// "0.00000" preserves the column's scale information, matching
		// Jaybird / Python firebird-driver / decNumber.
		{"scale -5, 0 (cohort quantum)", big.NewInt(0), -5, "0.00000"},

		// Positive scale — multiply by 10^scale via big.Int (overflow-safe).
		{"scale +2, +5 → 500", big.NewInt(5), 2, "500"},
		{"scale +2, -5 → -500", big.NewInt(-5), 2, "-500"},

		// Large positive scale on near-max INT128 — proves big.Int.Exp +
		// big.Int.Mul renders a 49-digit string without overflow,
		// scientific fallback, or precision loss (the failure modes #18
		// documents for the SHORT/LONG/INT64 helper).
		{"scale +10, 2^127-1 (49-digit string)", maxPos, 10, "1701411834604692317316873037158841057270000000000"},

		// Boundary: 2^127 - 1 with scale -38, the canonical NUMERIC(38, 38) max.
		// digits len = 39, exponent = -38, adjexp = 0 → formatPlain → "1." + 38 frac digits.
		{"scale -38, 2^127-1", maxPos, -38, "1.70141183460469231731687303715884105727"},
		// Boundary: -2^127 with scale -38 (NUMERIC(38, 38) min).
		{"scale -38, -2^127", minNeg, -38, "-1.70141183460469231731687303715884105728"},

		// formatPlain → formatScientific crossover: adjexp < -6 forces scientific.
		// digits len = 5, exponent = -20, adjexp = -16 → formatScientific → "1.2345E-16".
		{"scale -20, +12345 (scientific)", big.NewInt(12345), -20, "1.2345E-16"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			x := &xSQLVAR{sqltype: SQL_TYPE_INT128, sqlscale: tt.sqlscale}
			got, err := x.value(int128Bytes(tt.value), "", "")
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
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
