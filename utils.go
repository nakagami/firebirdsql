/*******************************************************************************
The MIT License (MIT)

Copyright (c) 2013 Hajime Nakagami

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
    "bytes"
)

func xdrBytes(bs []byte) []byte {
    // XDR encoding bytes
    n := len(bs)
    padding := 0
    if n % 4 != 0 {
        padding = 4 - n % 4
    }
    buf := make([]byte, 4 + n + padding)
    buf[0] = byte(n >> 24 & 0xFF)
    buf[1] = byte(n >> 16 & 0xFF)
    buf[2] = byte(n >> 8 & 0xFF)
    buf[3] = byte(n & 0xFF)
    for i, b := range bs {
        buf[4+i]=b
    }
    return buf
}

func xdrString(s string) []byte {
    // XDR encoding string
    bs := bytes.NewBufferString(s).Bytes()
    return xdrBytes(bs)
}

def calc_blr(xsqlda []xSQLVAR):
    // Calculate  BLR from XSQLVAR array.
    ln := len(xsqlda) *2
    blr, err := make([]byte, ln + 6)
    blr[0] = 5
    blr[1] = 2
    blr[2] = 4
    blr[3] = 0
    blr[4] = byte(ln & 255)
    blr[5] = byte(ln >> 8)
    n = 6

    for _, x range xsqlda {
        sqltype = x.sqltype
        sqlscale = x.sqlscale
        if sqlscale < 0 {
            sql += 256
        }
        switch sqltype {
        case SQL_TYPE_VARYING:
            blr[n] = 37
            blr[n+1] = x.sqllen & 255
            blr[n+2] = x.sqllen >> 8
            n += 3
        case SQL_TYPE_TEXT:
            blr[n] = 14
            blr[n+1] = x.sqllen & 255
            blr[n+2] = x.sqllen >> 8
            n += 3
        case SQL_TYPE_LONG:
            blr[n] = 8
            blr[n+1] = x.sqlscale
            n += 2
        case SQL_TYPE_SHORT:
            blr[n] = 7
            blr[n+1] = x.sqlscale
            n += 2
        case SQL_TYPE_INT64:
            blr[n] = 16
            blr[n+1] = x.sqlscale
            n += 2
        case SQL_TYPE_QUAD:
            blr[n] = 9
            blr[n+1] = x.sqlscale
            n += 2
        case SQL_TYPE_BLOB:
            blr[n] = 9
            blr[n+1] = 0
            n += 2
        case SQL_TYPE_ARRAY:
            blr[n] = 9
            blr[n+1] = 0
            n += 2
        case SQL_TYPE_DOUBLE:
            blr[n] = 27
            n += 1
        case SQL_TYPE_FLOAT:
            blr[n] = 10
            n += 1
        case SQL_TYPE_D_FLOAT:
            blr[n] = 11
            n += 1
        case SQL_TYPE_DATE:
            blr[n] = 12
            n += 1
        case SQL_TYPE_TIME:
            blr[n] = 13
            n += 1
        case SQL_TYPE_TIMESTAMP:
            blr[n] = 35
            n += 1
        case SQL_TYPE_BOOLEAN:
            blr[n] = 23
            n += 1
        }
        // [blr_short, 0]
        blr[n] = 7
        blr[n+1] = 0
        n += 2
    }
    // [blr_end, blr_eoc]
    blr[n] = 255
    blr[n+1] = 76

    return blr

