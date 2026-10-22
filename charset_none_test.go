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

// Unit tests (no server) for widenUntransliteratedText. The live counterpart
// is charset_none_live_test.go.

func TestWidenUntransliteratedText(t *testing.T) {
	const (
		none    = 0
		octets  = 1
		ascii   = 2
		fss     = 3
		utf8    = 4
		sjis    = 5
		win1252 = 53
	)
	cases := []struct {
		name      string
		sqltype   int
		subtype   int // charset id, collation id in the high byte
		sqllen    int
		connWidth int
		want      int // message length
	}{
		{"NONE varchar under UTF8", SQL_TYPE_VARYING, none, 40, 4, 160},
		{"NONE char under UTF8", SQL_TYPE_TEXT, none, 11, 4, 44},
		{"NONE varchar under UNICODE_FSS", SQL_TYPE_VARYING, none, 40, 3, 120},
		{"NONE varchar under SJIS_0208", SQL_TYPE_VARYING, none, 40, 2, 80},
		{"NONE varchar under WIN1252", SQL_TYPE_VARYING, none, 40, 1, 40},
		{"ASCII is untransliterated too", SQL_TYPE_VARYING, ascii, 10, 4, 40},
		{"collation in the high byte is ignored", SQL_TYPE_VARYING, none | 3<<8, 10, 4, 40},
		{"transliterated column: described as UTF8", SQL_TYPE_VARYING, utf8, 160, 4, 160},
		{"UTF8 column under SJIS: already wider", SQL_TYPE_VARYING, utf8, 160, 2, 160},
		{"UNICODE_FSS column under UTF8", SQL_TYPE_VARYING, fss, 30, 4, 40},
		{"SJIS column under UTF8", SQL_TYPE_TEXT, sjis, 20, 4, 40},
		{"single-byte column under UTF8 (not transliterated)", SQL_TYPE_VARYING, win1252, 10, 4, 40},
		{"OCTETS is binary: never widened", SQL_TYPE_VARYING, octets, 16, 4, 16},
		{"OCTETS char", SQL_TYPE_TEXT, octets, 16, 4, 16},
		{"empty column", SQL_TYPE_VARYING, none, 0, 4, 0},
		{"capped to the 16-bit BLR length", SQL_TYPE_VARYING, none, 20000, 4, 65532},
		{"cap is a multiple of the char width", SQL_TYPE_VARYING, none, 30000, 3, 65535},
		{"cap never shrinks a column", SQL_TYPE_VARYING, none, 32765, 4, 65532},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			x := []xSQLVAR{{sqltype: c.sqltype, sqlsubtype: c.subtype, sqllen: c.sqllen}}
			widenUntransliteratedText(x, c.connWidth)
			require.Equal(t, c.want, x[0].messageLength())
			require.Equal(t, c.sqllen, x[0].sqllen, "the described length is kept")
			require.Equal(t, c.sqllen, x[0].displayLength(), "metadata reports the described length")
		})
	}
}

func TestWidenUntransliteratedTextLeavesOtherTypes(t *testing.T) {
	x := []xSQLVAR{
		{sqltype: SQL_TYPE_LONG, sqllen: 4},
		{sqltype: SQL_TYPE_BLOB, sqlsubtype: 1, sqllen: 8},
		{sqltype: SQL_TYPE_TIMESTAMP, sqllen: 8},
	}
	widenUntransliteratedText(x, 4)
	for _, v := range x {
		require.Zero(t, v.msglen)
	}
}

// TestCalcBlrUsesMessageLength checks the bytes sent to the server: the BLR
// item of a widened column and the number of bytes read back for CHAR.
func TestCalcBlrUsesMessageLength(t *testing.T) {
	x := []xSQLVAR{
		{sqltype: SQL_TYPE_VARYING, sqlsubtype: 0, sqllen: 40}, // NONE
		{sqltype: SQL_TYPE_TEXT, sqlsubtype: 0, sqllen: 11},    // NONE
		{sqltype: SQL_TYPE_VARYING, sqlsubtype: 4, sqllen: 80}, // UTF8
	}
	widenUntransliteratedText(x, 4)
	blr := calcBlr(x)
	require.Equal(t, []byte{
		5, 2, 4, 0, 6, 0, // blr_version5, blr_begin, blr_message 0, 6 items
		37, 160, 0, 7, 0, // blr_varying(160), blr_short 0 (null indicator)
		14, 44, 0, 7, 0, // blr_text(44), blr_short 0
		37, 80, 0, 7, 0, // blr_varying(80), blr_short 0
		255, 76, // blr_end, blr_eoc
	}, blr)
	require.Equal(t, 44, x[1].ioLength(), "CHAR is read with its message length")
	require.Equal(t, -1, x[0].ioLength(), "VARCHAR is read with its wire length prefix")
}

func TestCharsetBytesPerChar(t *testing.T) {
	// RDB$CHARACTER_SETS rows with RDB$BYTES_PER_CHARACTER > 1 (Firebird 2.5-6.0)
	want := map[int]int{3: 3, 4: 4, 5: 2, 6: 2, 44: 2, 56: 2, 57: 2, 67: 2, 68: 2, 69: 4}
	for id := 0; id < 256; id++ {
		w, ok := want[id]
		if !ok {
			w = 1
		}
		require.Equal(t, w, charsetBytesPerChar(id), "charset id %d", id)
	}
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
