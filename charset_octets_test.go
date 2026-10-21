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

// Unit tests for the BLR of binary items: no server needed.

func TestCalcBlrDeclaresOctets(t *testing.T) {
	xs := []xSQLVAR{
		{sqltype: SQL_TYPE_VARYING, sqlsubtype: charsetIDOctets, sqllen: 20},
		{sqltype: SQL_TYPE_TEXT, sqlsubtype: charsetIDOctets | 3<<8, sqllen: 12}, // collation bits ignored
		{sqltype: SQL_TYPE_VARYING, sqlsubtype: 4, sqllen: 80},                   // UTF8: unchanged
	}
	require.Equal(t, []byte{
		5, 2, 4, 0, 6, 0, // blr_version5, blr_begin, blr_message 0, 6 items
		38, 1, 0, 20, 0, 7, 0, // blr_varying2 OCTETS(20), blr_short 0
		15, 1, 0, 12, 0, 7, 0, // blr_text2 OCTETS(12), blr_short 0
		37, 80, 0, 7, 0, // blr_varying(80), blr_short 0
		255, 76, // blr_end, blr_eoc
	}, calcBlr(xs))
}

func TestOctetsToBlr(t *testing.T) {
	blr, v := _octetsToBlr([]byte{0xFF, 0x00, 0xC3})
	require.Equal(t, []byte{15, 1, 0, 3, 0}, blr, "blr_text2, charset OCTETS, length 3")
	require.Equal(t, []byte{0xFF, 0x00, 0xC3, 0}, v, "padded to 4 bytes")
	blr, v = _octetsToBlr(nil)
	require.Equal(t, []byte{15, 1, 0, 0, 0}, blr)
	require.Empty(t, v)
}
