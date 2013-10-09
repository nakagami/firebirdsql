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
    "strings"
    "encoding/binary"
    "container/list"
    "database/sql/driver"
)

func str_to_bytes(s string) []byte {
    return bytes.NewBufferString(s).Bytes()
}

func int32_to_bytes(i32 int32) []byte {
    bs := []byte {
        byte(i32 & 0xFF),
        byte(i32 >> 8 & 0xFF),
        byte(i32 >> 16 & 0xFF),
        byte(i32 >> 24 & 0xFF),
    }
    return bs
}

func bint32_to_bytes(i32 int32) []byte {
    bs := []byte {
        byte(i32 >> 24 & 0xFF),
        byte(i32 >> 16 & 0xFF),
        byte(i32 >> 8 & 0xFF),
        byte(i32 & 0xFF),
    }
    return bs
}

func int16_to_bytes(i16 int16) []byte {
    bs := []byte {
        byte(i16 & 0xFF),
        byte(i16 >> 8 & 0xFF),
    }
    return bs
}
func bytes_to_str(b []byte) string {
    return bytes.NewBuffer(b).String()
}

func bytes_to_bint32(b []byte) int32 {
    var i32 int32
    buffer := bytes.NewBuffer(b)
    binary.Read(buffer, binary.BigEndian, &i32)
    return i32
}

func bytes_to_int32(b []byte) int32 {
    var i32 int32
    buffer := bytes.NewBuffer(b)
    binary.Read(buffer, binary.LittleEndian, &i32)
    return i32
}

func bytes_to_bint16(b []byte) int16 {
    var i int16
    buffer := bytes.NewBuffer(b)
    binary.Read(buffer, binary.BigEndian, &i)
    return i
}

func bytes_to_int16(b []byte) int16 {
    var i int16
    buffer := bytes.NewBuffer(b)
    binary.Read(buffer, binary.LittleEndian, &i)
    return i
}

func bytes_to_bint64(b []byte) int64 {
    var i int64
    buffer := bytes.NewBuffer(b)
    binary.Read(buffer, binary.BigEndian, &i)
    return i
}

func bytes_to_int64(b []byte) int64 {
    var i int64
    buffer := bytes.NewBuffer(b)
    binary.Read(buffer, binary.LittleEndian, &i)
    return i
}

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

func flattenBytes(l *list.List) []byte {
    n := 0
    for e := l.Front(); e != nil; e = e.Next() {
        n += len((e.Value).([]byte))
    }

    bs := make([]byte, n)

    n = 0
    for e := l.Front(); e != nil; e = e.Next() {
        for i, b := range (e.Value).([]byte) {
            bs[n+i] = b
        }
        n += len((e.Value).([]byte))
    }

    return bs
}

func paramsToBlr(params []driver.Value) ([]byte, []byte) {
    // Convert parameter array to BLR and values format.
    var v, blr []byte

    ln := len(params) * 2
    blrList := list.New()
    valuesList := list.New()
    blrList.PushBack([]byte {5, 2, 4, 0, byte(ln&255), byte(ln>>8)})

    for _, p := range params {
        switch f := p.(type) {
        case string:
            v = str_to_bytes(f)
            nbytes := len(v)
            pad_length := ((4-nbytes) & 3)
            padding := make([]byte, pad_length)
            v = bytes.Join([][]byte{
                v,
                padding,
                []byte{0, 0, 0, 0},
            }, nil)
            blr = []byte{14, byte(nbytes&255), byte(nbytes>>8)}
        case int:
            v = bytes.Join([][]byte{
                int32_to_bytes(int32(f)),
                []byte{0, 0, 0, 0},
            }, nil)
            blr = []byte{8, 0}
/*
        case float32:
            if t == float:
                p = decimal.Decimal(str(p))
            (sign, digits, exponent) = p.as_tuple()
            v = 0
            ln = len(digits)
            for i in range(ln):
                v += digits[i] * (10 ** (ln -i-1))
            if sign:
                v *= -1
            v = bint_to_bytes(v, 8)
            if exponent < 0:
                exponent += 256
            blr += bytes([16, exponent])
        case time.Time: // Date
            v = convert_date(p)
            blr += bytes([12])
        case time.Time  // Time
            v = convert_time(p)
            blr += bytes([13])
        case time.Time  // timestamp
            v = convert_timestamp(p)
            blr += bytes([35])
*/
        case bool:
            if f {
                v = []byte{1, 0, 0, 0, 0}
            } else {
                v = []byte{0, 0, 0, 0, 0}
            }
            blr = []byte{23}
        case nil:
            v = []byte{0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 0x32, 0x8c}
            blr = []byte{9, 0}
        }
        valuesList.PushBack(v)
        blrList.PushBack(blr)
        blrList.PushBack([]byte{7, 0})
    }
    blrList.PushBack([]byte{255, 76})   // [blr_end, blr_eoc]

    blr = flattenBytes(blrList)
    v = flattenBytes(valuesList)

    return blr, v
}

func split1(src string, delm string) (string, string) {
    for i := 0; i< len(src); i++ {
        if src[i:i+1] == delm {
            s1 := src[0:i]
            s2 := src[i+1:]
            return s1, s2
        }
    }
    return src, ""
}

func parseDSN(dsn string) (addr string, dbName string, user string, passwd string, err error) {
    s1, s2 := split1(dsn, "@")
    user, passwd = split1(s1, ":")
    addr, dbName = split1(s2, "/")
    if !strings.ContainsRune(addr, ':') {
        addr += ":3050"
    }
    if strings.ContainsRune(dbName, '/') {
        dbName = "/" + dbName
    }

    return
}
