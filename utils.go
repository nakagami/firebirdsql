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
    var sqltype2blr = map[int][]byte {
        SQL_TYPE_DOUBLE: {27},
        SQL_TYPE_FLOAT: {10},
        SQL_TYPE_D_FLOAT: {11},
        SQL_TYPE_DATE: {12},
        SQL_TYPE_TIME: {13},
        SQL_TYPE_TIMESTAMP: {35},
        SQL_TYPE_BLOB: {9, 0},
        SQL_TYPE_ARRAY: {9, 0},
        SQL_TYPE_BOOLEAN: {23},
        }


    ln := len(xsqlda) *2
    blr := {5, 2, 4, 0, byte(ln & 255), byte(ln >> 8)}
    for _, x range xsqlda {
        sqltype = x.sqltype
        if sqltype == SQL_TYPE_VARYING:
            blr += [37, x.sqllen & 255, x.sqllen >> 8]
        elif sqltype == SQL_TYPE_TEXT:
            blr += [14, x.sqllen & 255, x.sqllen >> 8]
        elif sqltype == SQL_TYPE_LONG:
            blr += [8, x.sqlscale]
        elif sqltype == SQL_TYPE_SHORT:
            blr += [7, x.sqlscale]
        elif sqltype == SQL_TYPE_INT64:
            blr += [16, x.sqlscale]
        elif sqltype == SQL_TYPE_QUAD:
            blr += [9, x.sqlscale]
        else:
            blr += sqltype2blr[sqltype]
        blr += [7, 0]   # [blr_short, 0]
    }
    blr += [255, 76]    # [blr_end, blr_eoc]

    # x.sqlscale value shoud be negative, so b convert to range(0, 256)
    return bytes(256 + b if b < 0 else b for b in blr)

