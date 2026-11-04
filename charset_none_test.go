/*******************************************************************************
The MIT License (MIT)

Copyright (c) 2026 Hajime Nakagami

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
	"testing"

	"github.com/stretchr/testify/require"
)

// Unit tests (no server) for calcOutputBlr and blrCharset. The live counterpart
// is charset_none_live_test.go.

func TestBlrCharset(t *testing.T) {
	cases := []struct {
		name    string
		subtype int
		wantCS  byte
		wantOK  bool
	}{
		{"NONE (0)", 0, 0, true},
		{"OCTETS (1)", 1, 1, true},
		{"ASCII (2)", 2, 2, true},
		{"UNICODE_FSS (3)", 3, 3, true},
		{"UTF8 (4)", 4, 4, true},
		{"WIN1252 (53)", 53, 53, true},
		{"collation in high byte is ignored", 4 | 3<<8, 4, true},
		{"CS_dynamic (127) uses plain form", charsetDynamic, 0, false},
		{"CS_dynamic with collation uses plain form", charsetDynamic | 2<<8, 0, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			x := xSQLVAR{sqltype: SQL_TYPE_VARYING, sqlsubtype: c.subtype, sqllen: 40}
			cs, ok := x.blrCharset()
			require.Equal(t, c.wantOK, ok)
			require.Equal(t, c.wantCS, cs)
		})
	}
}

// TestCalcOutputBlrDeclaresCharset checks that calcOutputBlr emits
// blr_text2/blr_varying2 with the low-byte charset id and collation 0, while
// keeping plain blr_text/blr_varying for CS_dynamic (127), SQL_TYPE_NULL, and
// input-side calcBlr.
func TestCalcOutputBlrDeclaresCharset(t *testing.T) {
	x := []xSQLVAR{
		{sqltype: SQL_TYPE_VARYING, sqlsubtype: 0, sqllen: 40},              // NONE
		{sqltype: SQL_TYPE_TEXT, sqlsubtype: 0 | 3<<8, sqllen: 11},          // NONE with collation in high byte
		{sqltype: SQL_TYPE_VARYING, sqlsubtype: 4, sqllen: 80},              // UTF8
		{sqltype: SQL_TYPE_TEXT, sqlsubtype: 1, sqllen: 16},                 // OCTETS
		{sqltype: SQL_TYPE_VARYING, sqlsubtype: charsetDynamic, sqllen: 32}, // CS_dynamic -> plain blr_varying
		{sqltype: SQL_TYPE_NULL},                                            // untyped NULL -> plain blr_text(0)
	}
	outBlr := calcOutputBlr(x)
	require.Equal(t, []byte{
		5, 2, 4, 0, 12, 0, // blr_version5, blr_begin, blr_message 0, 12 items
		38, 0, 0, 40, 0, 7, 0, // blr_varying2(cs=0, coll=0, len=40), blr_short 0
		15, 0, 0, 11, 0, 7, 0, // blr_text2(cs=0, coll=0, len=11), blr_short 0
		38, 4, 0, 80, 0, 7, 0, // blr_varying2(cs=4, coll=0, len=80), blr_short 0
		15, 1, 0, 16, 0, 7, 0, // blr_text2(cs=1, coll=0, len=16), blr_short 0
		37, 32, 0, 7, 0, // blr_varying(len=32), blr_short 0
		14, 0, 0, 7, 0, // blr_text(len=0), blr_short 0
		255, 76, // blr_end, blr_eoc
	}, outBlr)

	// Input-side calcBlr keeps plain blr_text/blr_varying for all items.
	inBlr := calcBlr(x[:3])
	require.Equal(t, []byte{
		5, 2, 4, 0, 6, 0,
		37, 40, 0, 7, 0,
		14, 11, 0, 7, 0,
		37, 80, 0, 7, 0,
		255, 76,
	}, inBlr)

	// sqllen and ioLength are unchanged.
	require.Equal(t, 11, x[1].ioLength())
	require.Equal(t, -1, x[0].ioLength())
}

func TestConnectionCharsetByteLen(t *testing.T) {
	for charset, want := range map[string]int{
		"UTF8": 4, "GB18030": 4, "UNICODE_FSS": 3,
		"SJIS_0208": 2, "EUCJ_0208": 2, "KSC_5601": 2, "BIG_5": 2, "GB_2312": 2, "GBK": 2, "CP943C": 2,
		"NONE": 1, "WIN1252": 1, "WIN1251": 1, "ISO8859_1": 1, "KOI8R": 1, "KOI8U": 1,
	} {
		p := &wireProtocol{charset: charset}
		p.charsetLen()
		require.Equal(t, want, p.charsetByteLen, charset)
	}
}
