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
    "time"
    "bytes"
)

const (
    SQL_TYPE_TEXT = 452
    SQL_TYPE_VARYING = 448
    SQL_TYPE_SHORT = 500
    SQL_TYPE_LONG = 496
    SQL_TYPE_FLOAT = 482
    SQL_TYPE_DOUBLE = 480
    SQL_TYPE_D_FLOAT = 530
    SQL_TYPE_TIMESTAMP = 510
    SQL_TYPE_BLOB = 520
    SQL_TYPE_ARRAY = 540
    SQL_TYPE_QUAD = 550
    SQL_TYPE_TIME = 560
    SQL_TYPE_DATE = 570
    SQL_TYPE_INT64 = 580
    SQL_TYPE_BOOLEAN = 32764
    SQL_TYPE_NULL = 32766
)

var xsqlvarTypeLength = map[int]int {
    SQL_TYPE_VARYING: -1,
    SQL_TYPE_SHORT: 4,
    SQL_TYPE_LONG: 4,
    SQL_TYPE_FLOAT: 4,
    SQL_TYPE_TIME: 4,
    SQL_TYPE_DATE: 4,
    SQL_TYPE_DOUBLE: 8,
    SQL_TYPE_TIMESTAMP: 8,
    SQL_TYPE_BLOB: 8,
    SQL_TYPE_ARRAY: 8,
    SQL_TYPE_QUAD: 8,
    SQL_TYPE_INT64: 8,
    SQL_TYPE_BOOLEAN: 1,
}

var xsqlvarTypeDisplayLength = map[int]int {
    SQL_TYPE_VARYING: -1,
    SQL_TYPE_SHORT: 6,
    SQL_TYPE_LONG: 11,
    SQL_TYPE_FLOAT: 17,
    SQL_TYPE_TIME: 11,
    SQL_TYPE_DATE: 10,
    SQL_TYPE_DOUBLE: 17,
    SQL_TYPE_TIMESTAMP: 22,
    SQL_TYPE_BLOB: 0,
    SQL_TYPE_ARRAY: -1,
    SQL_TYPE_QUAD: 20,
    SQL_TYPE_INT64: 20,
    SQL_TYPE_BOOLEAN: 5,
}

type xSQLVAR struct {
    sqltype int
    sqlscale int
    sqlsubtype int
    sqllen int
    null_ok bool
    fieldname string
    relname string
    ownname string
    aliasname string
}


func (x *xSQLVAR) ioLength() int {
    if x.sqltype == SQL_TYPE_TEXT {
        return x.sqllen
    } else {
        return xsqlvarTypeLength[x.sqltype]
    }
}

func (x *xSQLVAR) displayLenght() int {
    if x.sqltype == SQL_TYPE_TEXT {
        return x.sqllen
    } else {
        return xsqlvarTypeDisplayLength[x.sqltype]
    }
}

func (x *xSQLVAR) _parseDate(raw_value []byte) time.Time {
    nday := int(bytes_to_int32(raw_value)) + 678882
    century := (4 * nday -1) / 146097
    nday = 4 * nday - 1 - 146097 * century
    day := nday / 4

    nday = (4 * day + 3) / 1461
    day = 4 * day + 3 - 1461 * nday
    day = (day + 4) / 4

    month := (5 * day -3) / 153
    day = 5 * day - 3 - 153 * month
    day = (day + 5) / 5
    year := 100 * century + nday
    if month < 10 {
        month += 3
    } else {
        month -= 9
        year += 1
    }
    return time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
}

/*
func (x *xSQLVAR) _parseTime(raw_value []byte) time.Time {
        n := int(bytes_to_bint32(raw_value))
        s := n / 10000
        m := s / 60
        h := m / 60
        m = m % 60
        s = s % 60
        return time.Time(h, m, s, (n % 10000) * 100)
}
*/

func (x *xSQLVAR) value(raw_value []byte) interface{} {
    switch x.sqltype {
    case SQL_TYPE_TEXT:
        if x.sqlsubtype == 1 {          // OCTETS
            return raw_value
        } else {
            return bytes.NewBuffer(raw_value).String()
        }
    case SQL_TYPE_VARYING:
        if x.sqlsubtype == 1 {       // OCTETS
            return raw_value
        } else {
            return bytes.NewBuffer(raw_value).String()
        }
    case SQL_TYPE_SHORT:
        return bytes_to_int16(raw_value)
    case SQL_TYPE_LONG:
        return bytes_to_int32(raw_value)
        // return bytes_to_int32(raw_value) ** x.sqlscale
    case SQL_TYPE_INT64:
        return bytes_to_int64(raw_value)
        // return bytes_to_int64(raw_value) ** x.sqlscale
    case SQL_TYPE_DATE:
        return x._parseDate(raw_value)
//    case SQL_TYPE_TIME:
//        return x._parseTime(raw_value)
//    case SQL_TYPE_TIMESTAMP:
//        yyyy, mm, dd = self._parse_date(raw_value[:4])
//        h, m, s, ms = self._parse_time(raw_value[4:])
//        return datetime.datetime(yyyy, mm, dd, h, m, s, ms)
//    case SQL_TYPE_FLOAT:
//        return struct.unpack('!f', raw_value)[0]
//    case SQL_TYPE_DOUBLE:
//        return struct.unpack('!d', raw_value)[0]
    case SQL_TYPE_BOOLEAN:
        return raw_value[0] != 0
    }
    return raw_value
}

func calcBlr(xsqlda []xSQLVAR) []byte {
    // Calculate  BLR from XSQLVAR array.
    ln := len(xsqlda) *2
    blr := make([]byte, (ln*2) + 8)
    blr[0] = 5
    blr[1] = 2
    blr[2] = 4
    blr[3] = 0
    blr[4] = byte(ln & 255)
    blr[5] = byte(ln >> 8)
    n := 6

    for _, x := range xsqlda {
        sqlscale := x.sqlscale
        if sqlscale < 0 {
            sqlscale += 256
        }
        switch x.sqltype {
        case SQL_TYPE_VARYING:
            blr[n] = 37
            blr[n+1] = byte(x.sqllen & 255)
            blr[n+2] = byte(x.sqllen >> 8)
            n += 3
        case SQL_TYPE_TEXT:
            blr[n] = 14
            blr[n+1] = byte(x.sqllen & 255)
            blr[n+2] = byte(x.sqllen >> 8)
            n += 3
        case SQL_TYPE_LONG:
            blr[n] = 8
            blr[n+1] = byte(sqlscale)
            n += 2
        case SQL_TYPE_SHORT:
            blr[n] = 7
            blr[n+1] = byte(sqlscale)
            n += 2
        case SQL_TYPE_INT64:
            blr[n] = 16
            blr[n+1] = byte(sqlscale)
            n += 2
        case SQL_TYPE_QUAD:
            blr[n] = 9
            blr[n+1] = byte(sqlscale)
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
    n += 2

    return blr[:n]
}

