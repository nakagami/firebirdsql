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
    "encoding/binary"
)

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

func bytes_to_bint64(b []byte) int64 {
    var i int64
    buffer := bytes.NewBuffer(b)
    binary.Read(buffer, binary.BigEndian, &i)
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

