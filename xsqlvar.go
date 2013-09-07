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

class XSQLVAR:
    type_length = {
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

    type_display_length = {
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

    def __init__(self, bytes_to_str):
        self.bytes_to_str = bytes_to_str
        self.sqltype = None
        self.sqlscale = None
        self.sqlsubtype = None
        self.sqllen = None
        self.null_ok = None
        self.fieldname = ''
        self.relname = ''
        self.ownname = ''
        self.aliasname = ''

    def io_length(self):
        sqltype = self.sqltype
        if sqltype == SQL_TYPE_TEXT:
            return self.sqllen
        else:
            return self.type_length[sqltype]

    def display_length(self):
        sqltype = self.sqltype
        if sqltype == SQL_TYPE_TEXT:
            return self.sqllen
        else:
            return self.type_display_length[sqltype]

    def precision(self):
        return None

    def __str__(self):
        s  = '[' + str(self.sqltype) + ',' + str(self.sqlscale) + ',' \
                + str(self.sqlsubtype) + ',' + str(self.sqllen)  + ',' \
                + str(self.null_ok) + ',' + self.fieldname + ',' \
                + self.relname + ',' + self.ownname + ',' \
                + self.aliasname + ']'
        return s

    def _parse_date(self, raw_value):
        "Convert raw data to datetime.date"
        nday = bytes_to_bint(raw_value) + 678882
        century = (4 * nday -1) // 146097
        nday = 4 * nday - 1 - 146097 * century
        day = nday // 4

        nday = (4 * day + 3) // 1461
        day = 4 * day + 3 - 1461 * nday
        day = (day + 4) // 4

        month = (5 * day -3) // 153
        day = 5 * day - 3 - 153 * month
        day = (day + 5) // 5
        year = 100 * century + nday
        if month < 10:
            month += 3
        else:
            month -= 9
            year += 1
        return year, month, day

    def _parse_time(self, raw_value):
        "Convert raw data to datetime.time"
        n = bytes_to_bint(raw_value)
        s = n // 10000
        m = s // 60
        h = m // 60
        m = m % 60
        s = s % 60
        return (h, m, s, (n % 10000) * 100)

    def value(self, raw_value):
        if self.sqltype == SQL_TYPE_TEXT:
            if self.sqlsubtype == 1:      # OCTETS
                return raw_value
            else:
                return self.bytes_to_str(raw_value)
        elif self.sqltype == SQL_TYPE_VARYING:
            if self.sqlsubtype == 1:      # OCTETS
                return raw_value
            else:
                return self.bytes_to_str(raw_value)
        elif self.sqltype in (SQL_TYPE_SHORT, SQL_TYPE_LONG, SQL_TYPE_INT64):
            n = bytes_to_bint(raw_value)
            if self.sqlscale:
                return decimal.Decimal(str(n) + 'e' + str(self.sqlscale))
            else:
                return n
        elif self.sqltype == SQL_TYPE_DATE:
            yyyy, mm, dd = self._parse_date(raw_value)
            return datetime.date(yyyy, mm, dd)
        elif self.sqltype == SQL_TYPE_TIME:
            h, m, s, ms = self._parse_time(raw_value)
            return datetime.time(h, m, s, ms)
        elif self.sqltype == SQL_TYPE_TIMESTAMP:
            yyyy, mm, dd = self._parse_date(raw_value[:4])
            h, m, s, ms = self._parse_time(raw_value[4:])
            return datetime.datetime(yyyy, mm, dd, h, m, s, ms)
        elif self.sqltype == SQL_TYPE_FLOAT:
            return struct.unpack('!f', raw_value)[0]
        elif self.sqltype == SQL_TYPE_DOUBLE:
            return struct.unpack('!d', raw_value)[0]
        elif self.sqltype == SQL_TYPE_BOOLEAN:
            return True if raw_value[0] else False
        else:
            return raw_value

