/*******************************************************************************
The MIT License (MIT)

Copyright (c) 2019 Hajime Nakagami

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
	"fmt"
	"math/big"
	"strconv"
	"strings"
)

var (
	decimal64DPDMask  = bigIntFromHexString("3ffffffffffff")
	decimal128DPDMask = bigIntFromHexString("3fffffffffffffffffffffffffff")
)

func dpdBitToInt(dpd uint, mask uint) int {
	if (dpd & mask) != 0 {
		return 1
	}
	return 0
}

func dpdToInt(dpd uint) (int64, error) {
	// Convert DPD encodined value to int (0-999)
	// dpd: DPD encoded value. 10bit unsigned int

	b := make([]int, 10)
	b[9] = dpdBitToInt(dpd, 0x0200)
	b[8] = dpdBitToInt(dpd, 0x0100)
	b[7] = dpdBitToInt(dpd, 0x0080)
	b[6] = dpdBitToInt(dpd, 0x0040)
	b[5] = dpdBitToInt(dpd, 0x0020)
	b[4] = dpdBitToInt(dpd, 0x0010)
	b[3] = dpdBitToInt(dpd, 0x0008)
	b[2] = dpdBitToInt(dpd, 0x0004)
	b[1] = dpdBitToInt(dpd, 0x0002)
	b[0] = dpdBitToInt(dpd, 0x0001)

	d := make([]int, 3)
	if b[3] == 0 {
		d[2] = b[9]*4 + b[8]*2 + b[7]
		d[1] = b[6]*4 + b[5]*2 + b[4]
		d[0] = b[2]*4 + b[1]*2 + b[0]
	} else if b[3] == 1 && b[2] == 0 && b[1] == 0 {
		d[2] = b[9]*4 + b[8]*2 + b[7]
		d[1] = b[6]*4 + b[5]*2 + b[4]
		d[0] = 8 + b[0]
	} else if b[3] == 1 && b[2] == 0 && b[1] == 1 {
		d[2] = b[9]*4 + b[8]*2 + b[7]
		d[1] = 8 + b[4]
		d[0] = b[6]*4 + b[5]*2 + b[0]
	} else if b[3] == 1 && b[2] == 1 && b[1] == 0 {
		d[2] = 8 + b[7]
		d[1] = b[6]*4 + b[5]*2 + b[4]
		d[0] = b[9]*4 + b[8]*2 + b[0]
	} else if b[6] == 0 && b[5] == 0 && b[3] == 1 && b[2] == 1 && b[1] == 1 {
		d[2] = 8 + b[7]
		d[1] = 8 + b[4]
		d[0] = b[9]*4 + b[8]*2 + b[0]
	} else if b[6] == 0 && b[5] == 1 && b[3] == 1 && b[2] == 1 && b[1] == 1 {
		d[2] = 8 + b[7]
		d[1] = b[9]*4 + b[8]*2 + b[4]
		d[0] = 8 + b[0]
	} else if b[6] == 1 && b[5] == 0 && b[3] == 1 && b[2] == 1 && b[1] == 1 {
		d[2] = b[9]*4 + b[8]*2 + b[7]
		d[1] = 8 + b[4]
		d[0] = 8 + b[0]
	} else if b[6] == 1 && b[5] == 1 && b[3] == 1 && b[2] == 1 && b[1] == 1 {
		d[2] = 8 + b[7]
		d[1] = 8 + b[4]
		d[0] = 8 + b[0]
	} else {
		return 0, fmt.Errorf("invalid DPD encoding: %d", dpd)
	}

	return int64(d[2])*100 + int64(d[1])*10 + int64(d[0]), nil
}

func calcSignificand(prefix int64, dpdBits *big.Int, numBits int) (*big.Int, error) {
	// prefix: High bits integer value
	// dpdBits: dpd encoded bits
	// numBits: bit length of dpd_bits
	// https://en.wikipedia.org/wiki/Decimal128_floating-point_format#Densely_packed_decimal_significand_field
	numSegments := numBits / 10
	segments := make([]uint, numSegments)
	bi1024 := big.NewInt(1024)

	for i := 0; i < numSegments; i++ {
		var work big.Int
		work.Add(&work, dpdBits)
		segments[numSegments-i-1] = uint(work.Mod(&work, bi1024).Int64())
		dpdBits.Rsh(dpdBits, 10)
	}

	v := big.NewInt(prefix)
	bi1000 := big.NewInt(1000)
	for _, dpd := range segments {
		n, err := dpdToInt(dpd)
		if err != nil {
			return nil, err
		}
		v.Mul(v, bi1000)
		v.Add(v, big.NewInt(n))
	}

	return v, nil
}

func decimal128ToSignDigitsExponent(b []byte) (coefficient *big.Int, exponent int32, negative bool, special string, err error) {
	// https://en.wikipedia.org/wiki/Decimal128_floating-point_format
	negative = (b[0] & 0x80) == 0x80
	cf := (uint32(b[0]&0x7f) << 10) + (uint32(b[1]) << 2) + uint32(b[2]>>6)
	if (cf & 0x1F000) == 0x1F000 {
		special = "NaN"
		return
	}
	if (cf & 0x1F000) == 0x1E000 {
		special = "Infinity"
		return
	}

	var prefix int64
	switch {
	case (cf & 0x18000) == 0x00000:
		exponent = int32(cf & 0x00fff)
		prefix = int64((cf >> 12) & 0x07)
	case (cf & 0x18000) == 0x08000:
		exponent = int32(0x1000 + (cf & 0x00fff))
		prefix = int64((cf >> 12) & 0x07)
	case (cf & 0x18000) == 0x10000:
		exponent = int32(0x2000 + (cf & 0x00fff))
		prefix = int64((cf >> 12) & 0x07)
	case (cf & 0x1e000) == 0x18000:
		exponent = int32(cf & 0x00fff)
		prefix = int64(8 + (cf>>12)&0x01)
	case (cf & 0x1e000) == 0x1a000:
		exponent = int32(0x1000 + (cf & 0x00fff))
		prefix = int64(8 + (cf>>12)&0x01)
	case (cf & 0x1e000) == 0x1c000:
		exponent = int32(0x2000 + (cf & 0x00fff))
		prefix = int64(8 + (cf>>12)&0x01)
	default:
		err = fmt.Errorf("decimal128 combination field error: cf=0x%x", cf)
		return
	}
	exponent -= 6176

	dpdBits := bytesToBigInt(b)
	dpdBits.And(dpdBits, decimal128DPDMask)
	coefficient, err = calcSignificand(prefix, dpdBits, 110)
	return
}

func decimalFixedToString(b []byte, scale int32) (string, error) {
	coefficient, _, negative, special, err := decimal128ToSignDigitsExponent(b)
	if err != nil {
		return "", err
	}
	return formatDecimalGDA(coefficient, scale, negative, special), nil
}

func decimal64ToString(b []byte) (string, error) {
	// https://en.wikipedia.org/wiki/Decimal64_floating-point_format
	negative := (b[0] & 0x80) == 0x80
	cf := (uint32(b[0]) >> 2) & 0x1f
	exponent := ((int32(b[0]) & 3) << 6) + ((int32(b[1]) >> 2) & 0x3f)

	if cf == 0x1f {
		return formatDecimalGDA(nil, 0, negative, "NaN"), nil
	}
	if cf == 0x1e {
		return formatDecimalGDA(nil, 0, negative, "Infinity"), nil
	}

	var prefix int64
	switch {
	case (cf & 0x18) == 0x00:
		prefix = int64(cf & 0x07)
	case (cf & 0x18) == 0x08:
		exponent = 0x100 + exponent
		prefix = int64(cf & 0x07)
	case (cf & 0x18) == 0x10:
		exponent = 0x200 + exponent
		prefix = int64(cf & 0x07)
	case (cf & 0x1e) == 0x18:
		prefix = int64(8 + cf&1)
	case (cf & 0x1e) == 0x1a:
		exponent = 0x100 + exponent
		prefix = int64(8 + cf&1)
	case (cf & 0x1e) == 0x1c:
		exponent = 0x200 + exponent
		prefix = int64(8 + cf&1)
	default:
		return "", fmt.Errorf("decimal64 combination field error: cf=0x%x", cf)
	}

	dpdBits := bytesToBigInt(b)
	dpdBits.And(dpdBits, decimal64DPDMask)
	coefficient, err := calcSignificand(prefix, dpdBits, 50)
	if err != nil {
		return "", err
	}
	exponent -= 398
	return formatDecimalGDA(coefficient, exponent, negative, ""), nil
}

func decimal128ToString(b []byte) (string, error) {
	coefficient, exponent, negative, special, err := decimal128ToSignDigitsExponent(b)
	if err != nil {
		return "", err
	}
	return formatDecimalGDA(coefficient, exponent, negative, special), nil
}

// formatDecimalGDA renders an IEEE 754 / GDA "to-scientific-string" canonical
// form for a decimal number (coefficient * 10^exponent, signed). Matches
// fbclient (decNumber), Jaybird, and Python firebird-driver output.
//
// special != "" short-circuits numeric formatting. Allowed: "NaN",
// "Infinity". The sign prefix is prepended for negative specials.
//
// Reference: http://speleotrove.com/decimal/daconvs.html#reftostr
func formatDecimalGDA(coefficient *big.Int, exponent int32, negative bool, special string) string {
	var body string
	if special != "" {
		body = special
	} else {
		// big.Int.String() returns "0" (length 1) for a zero coefficient, so the
		// standard adjexp formula naturally collapses to adjexp = exponent for
		// zero — matching the GDA spec's adjusted-exponent rule with no special
		// case required.
		digits := coefficient.String()
		adjexp := exponent + int32(len(digits)) - 1
		if exponent <= 0 && adjexp >= -6 {
			body = formatPlain(digits, exponent)
		} else {
			body = formatScientific(digits, adjexp)
		}
	}
	if negative {
		return "-" + body
	}
	return body
}

// formatPlain — exponent <= 0 && adjusted >= -6. Insert a decimal point so
// that exactly (-exponent) digits follow it. Trailing zeros in `digits` are
// preserved (IEEE 754 cohort quantum).
func formatPlain(digits string, exponent int32) string {
	if exponent == 0 {
		return digits
	}
	frac := int(-exponent)
	if len(digits) > frac {
		return digits[:len(digits)-frac] + "." + digits[len(digits)-frac:]
	}
	return "0." + strings.Repeat("0", frac-len(digits)) + digits
}

// formatScientific — exponent > 0 OR adjusted < -6. Emit d.dddE[+-]adjexp
// with a single digit before the point (point omitted for single-digit
// mantissa). Sign on the exponent is always present.
func formatScientific(digits string, adjexp int32) string {
	var mant string
	if len(digits) == 1 {
		mant = digits
	} else {
		mant = digits[:1] + "." + digits[1:]
	}
	sign := "+"
	if adjexp < 0 {
		sign = "-"
		adjexp = -adjexp
	}
	return mant + "E" + sign + strconv.Itoa(int(adjexp))
}
