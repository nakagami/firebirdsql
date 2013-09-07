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
    "date"
    "time"
)


var xsqlvarTypeLength = map[int]int32 {
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

var xsqlvarDisplayLength = map[int]int32 {
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
    var sqltype int32
    var sqlscale int32
    var sqlsubtype int32
    var sqllen int32
    var null_ok bool
    var fieldname string
    var relname string
    var ownname string
    var aliasname string
}

func NewXSQLVAR () *xSQLVAR {
    x := new(xSQLVAR)
    return x
}

func (x *xSQLVAR) ioLength() int32 {
    if x.sqltype == SQL_TYPE_TEXT:
        return x.sqllen
    else:
        return xsqlvarTypeLength[x.sqltype]
}

func (x *xSQLVAR) displayLenght() int 32 {
    sqltype = self.sqltype
    if sqltype == SQL_TYPE_TEXT:
        return self.sqllen
    else:
        return self.type_display_length[sqltype]
}

func (x *xSQLVAR) _parseDate(raw_value []byte) date {
    nday = bytes_to_bint(raw_value) + 678882
    century = (4 * nday -1) / 146097
    nday = 4 * nday - 1 - 146097 * century
    day = nday / 4

    nday = (4 * day + 3) / 1461
    day = 4 * day + 3 - 1461 * nday
    day = (day + 4) / 4

    month = (5 * day -3) / 153
    day = 5 * day - 3 - 153 * month
    day = (day + 5) / 5
    year = 100 * century + nday
    if month < 10:
        month += 3
    else:
        month -= 9
        year += 1
    return year, month, day
}

func (x *xSQLVAR) _parseTime(raw_value []byte) time {
        n = bytes_to_bint(raw_value)
        s = n // 10000
        m = s // 60
        h = m // 60
        m = m % 60
        s = s % 60
        return (h, m, s, (n % 10000) * 100)
}

func (x *xSQLVAR) value(raw_value) interface{} {
    switch x.sqltype {
    case SQL_TYPE_TEXT:
        if x.sqlsubtype == 1 {     # OCTETS
            return raw_value
        } else {
            return self.bytes_to_str(raw_value)
        }
    case SQL_TYPE_VARYING:
        if self.sqlsubtype == 1 {     # OCTETS
            return raw_value
        } else {
            return self.bytes_to_str(raw_value)
        }
    case SQL_TYPE_SHORT:
        // TODO:
    case SQL_TYPE_LONG:
        // TODO:
    case SQL_TYPE_INT64:
        n = bytes_to_bint(raw_value)
        if self.sqlscale:
            return decimal.Decimal(str(n) + 'e' + str(self.sqlscale))
        else:
            return n
    case SQL_TYPE_DATE:
        return x._parseDate(raw_value)
    case SQL_TYPE_TIME:
        return x._parseTime(raw_value)
    case SQL_TYPE_TIMESTAMP:
        yyyy, mm, dd = self._parse_date(raw_value[:4])
        h, m, s, ms = self._parse_time(raw_value[4:])
        return datetime.datetime(yyyy, mm, dd, h, m, s, ms)
//    case SQL_TYPE_FLOAT:
//        return struct.unpack('!f', raw_value)[0]
//    case SQL_TYPE_DOUBLE:
//        return struct.unpack('!d', raw_value)[0]
    case SQL_TYPE_BOOLEAN:
        return raw_value[0] != 0
    }
    return raw_value
}

