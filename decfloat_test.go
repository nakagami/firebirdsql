package firebirdsql

import (
	"math/big"
	"testing"
)

func TestFormatDecimalGDA(t *testing.T) {
	cases := []struct {
		name        string
		coefficient *big.Int
		exponent    int32
		negative    bool
		special     string
		want        string
	}{
		// Plain (fixed-point) notation
		{"zero", big.NewInt(0), 0, false, "", "0"},
		{"zero-with-frac-exp", big.NewInt(0), -2, false, "", "0.00"},
		{"zero-with-pos-exp", big.NewInt(0), 3, false, "", "0E+3"},
		{"zero-at-boundary", big.NewInt(0), -6, false, "", "0.000000"},
		{"zero-past-boundary-sci", big.NewInt(0), -7, false, "", "0E-7"},
		{"zero-far-negative-sci", big.NewInt(0), -100, false, "", "0E-100"},
		{"trailing-zero-1.20", big.NewInt(120), -1, false, "", "12.0"},
		{"trailing-zeros-1.200", big.NewInt(1200), -3, false, "", "1.200"},
		{"adjexp-boundary-minus-6", big.NewInt(1), -6, false, "", "0.000001"},
		{"adjexp-just-fits-frac", big.NewInt(123), -5, false, "", "0.00123"},
		{"simple-fraction", big.NewInt(12345), -2, false, "", "123.45"},
		{"negative-finite", big.NewInt(12345), -2, true, "", "-123.45"},
		{"adjexp-boundary-minus-6-multidigit", big.NewInt(123), -8, false, "", "0.00000123"},

		// Scientific notation
		{"positive-exp-scientific", big.NewInt(12), 2, false, "", "1.2E+3"},
		{"adjexp-boundary-minus-7", big.NewInt(1), -7, false, "", "1E-7"},
		{"multidigit-mantissa-sci", big.NewInt(123), -9, false, "", "1.23E-7"},
		{"single-digit-mantissa-sci", big.NewInt(5), -20, false, "", "5E-20"},
		{"large-positive-exp", big.NewInt(1), 6111, false, "", "1E+6111"},

		// Specials
		{"NaN", nil, 0, false, "NaN", "NaN"},
		{"negative-NaN", nil, 0, true, "NaN", "-NaN"},
		{"Infinity", nil, 0, false, "Infinity", "Infinity"},
		{"negative-Infinity", nil, 0, true, "Infinity", "-Infinity"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := formatDecimalGDA(tc.coefficient, tc.exponent, tc.negative, tc.special)
			if got != tc.want {
				t.Errorf("formatDecimalGDA(%v, %d, %v, %q) = %q, want %q",
					tc.coefficient, tc.exponent, tc.negative, tc.special, got, tc.want)
			}
		})
	}
}

// TestDecimal64NoPanicOnSpecials exercises all four NaN/Inf branches of
// decimal64ToString. Before the GDA refactor these called
// decimal.NewFromFloat(math.NaN/Inf), which panics; users scanning a
// legitimate Firebird DECFLOAT(16) NaN or Infinity would crash the goroutine.
//
// d64 byte 0 layout: bit7=sign, bits6..2=cf (5 bits), bits1..0=exp high.
// cf=0x1F → NaN, cf=0x1E → Infinity.
func TestDecimal64NoPanicOnSpecials(t *testing.T) {
	cases := []struct {
		name string
		in   []byte
		want string
	}{
		{"d64-positive-NaN", []byte{0x7C, 0, 0, 0, 0, 0, 0, 0}, "NaN"},
		{"d64-negative-NaN", []byte{0xFC, 0, 0, 0, 0, 0, 0, 0}, "-NaN"},
		{"d64-positive-Inf", []byte{0x78, 0, 0, 0, 0, 0, 0, 0}, "Infinity"},
		{"d64-negative-Inf", []byte{0xF8, 0, 0, 0, 0, 0, 0, 0}, "-Infinity"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := decimal64ToString(tc.in)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

// TestDecimal128NoPanicOnSpecials — same coverage for the 16-byte decimal128
// decoder. The top 5 bits of cf live in b[0] bits 2..6 (after the sign bit).
func TestDecimal128NoPanicOnSpecials(t *testing.T) {
	cases := []struct {
		name string
		in   []byte
		want string
	}{
		{"d128-positive-NaN", append([]byte{0x7C}, make([]byte, 15)...), "NaN"},
		{"d128-negative-NaN", append([]byte{0xFC}, make([]byte, 15)...), "-NaN"},
		{"d128-positive-Inf", append([]byte{0x78}, make([]byte, 15)...), "Infinity"},
		{"d128-negative-Inf", append([]byte{0xF8}, make([]byte, 15)...), "-Infinity"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := decimal128ToString(tc.in)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

// TestDecimal128CfPrecedenceFix verifies that DECFLOAT(34) values whose
// biased exponent has bit 8 or 9 set decode correctly. The combination
// field is packed across b[0], b[1], b[2] high bits: b[1] contributes cf
// bits 9..2, so widening it through uint32 before the <<2 shift preserves
// bits 8 and 9 of the biased exponent.
//
// In decimal128 the significand is 34 digits: the prefix in the cf field
// is the leading digit, the DPD encodes the 33 trailing digits. Vectors
// below put the value's digits in DPD (prefix=0) for clean canonical
// output, except the high-prefix vector which exercises the prefix∈{8,9}
// case arm and accepts the 34-digit cohort form.
//
// All vectors have b[1] with bits 6 and 7 set (0xFB or 0xFF) — the
// strongest exercise of the precedence fix.
func TestDecimal128CfPrecedenceFix(t *testing.T) {
	// d33 is the rightmost-DPD-declet content extended to byte 15 (the
	// low-order DPD byte). All other DPD bytes stay zero.
	const eight = byte(0x08) // DPD declet encoding for digit 8 (b[3]=1,b[0]=0 → d0=8)
	cases := []struct {
		name string
		in   []byte
		want string
	}{
		// 1E-50 — prefix=0, DPD trailing digit=1. biased exp 6126 (0x17EE),
		// cf = 0x87EE; case (cf & 0x18000) == 0x08000.
		{"1E-50", []byte{0x21, 0xFB, 0x80, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x01}, "1E-50"},
		// Negative variant — exercises the sign bit alongside the cf fix.
		{"-1E-50", []byte{0xA1, 0xFB, 0x80, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x01}, "-1E-50"},
		// 8E-50 — prefix=0, DPD trailing digit=8. Same cf as 1E-50
		// (0x87EE); exercises the DPD high-digit path (d0=8 uses the
		// b[3]=1 branch of dpdToInt).
		{"8E-50", []byte{0x21, 0xFB, 0x80, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, eight}, "8E-50"},
		// 1E+6111 — prefix=0, DPD trailing digit=1, biased exp 12287
		// (0x2FFF max). cf = 0x10FFF; case (cf & 0x18000) == 0x10000.
		{"1E+6111", []byte{0x43, 0xFF, 0xC0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x01}, "1E+6111"},
		// High-prefix arm — prefix=8, DPD=0, biased exp 6126. cf = 0x1A7EE;
		// case (cf & 0x1e000) == 0x1a000. Cohort is 8×10^33 × 10^-50 = 8E-17,
		// emitted in canonical 34-digit form.
		{"prefix8_biasedexp_6126", []byte{0x69, 0xFB, 0x80, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, "8.000000000000000000000000000000000E-17"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := decimal128ToString(tc.in)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}
