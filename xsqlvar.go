/*******************************************************************************
The MIT License (MIT)

Copyright (c) 2013-2016 Hajime Nakagami

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
	"math"
	//	"math/big"
	"reflect"
	"time"
)

const (
	SQL_TYPE_TEXT      = 452
	SQL_TYPE_VARYING   = 448
	SQL_TYPE_SHORT     = 500
	SQL_TYPE_LONG      = 496
	SQL_TYPE_FLOAT     = 482
	SQL_TYPE_DOUBLE    = 480
	SQL_TYPE_D_FLOAT   = 530
	SQL_TYPE_TIMESTAMP = 510
	SQL_TYPE_BLOB      = 520
	SQL_TYPE_ARRAY     = 540
	SQL_TYPE_QUAD      = 550
	SQL_TYPE_TIME      = 560
	SQL_TYPE_DATE      = 570
	SQL_TYPE_INT64     = 580
	SQL_TYPE_BOOLEAN   = 32764
	SQL_TYPE_NULL      = 32766
)

var xsqlvarTypeLength = map[int]int{
	SQL_TYPE_VARYING:   -1,
	SQL_TYPE_SHORT:     4,
	SQL_TYPE_LONG:      4,
	SQL_TYPE_FLOAT:     4,
	SQL_TYPE_TIME:      4,
	SQL_TYPE_DATE:      4,
	SQL_TYPE_DOUBLE:    8,
	SQL_TYPE_TIMESTAMP: 8,
	SQL_TYPE_BLOB:      8,
	SQL_TYPE_ARRAY:     8,
	SQL_TYPE_QUAD:      8,
	SQL_TYPE_INT64:     8,
	SQL_TYPE_BOOLEAN:   1,
}

var xsqlvarTypeDisplayLength = map[int]int{
	SQL_TYPE_VARYING:   -1,
	SQL_TYPE_SHORT:     6,
	SQL_TYPE_LONG:      11,
	SQL_TYPE_FLOAT:     17,
	SQL_TYPE_TIME:      11,
	SQL_TYPE_DATE:      10,
	SQL_TYPE_DOUBLE:    17,
	SQL_TYPE_TIMESTAMP: 22,
	SQL_TYPE_BLOB:      0,
	SQL_TYPE_ARRAY:     -1,
	SQL_TYPE_QUAD:      20,
	SQL_TYPE_INT64:     20,
	SQL_TYPE_BOOLEAN:   5,
}

var xsqlvarTypeName = map[int]string{
	SQL_TYPE_VARYING:   "VARYING",
	SQL_TYPE_SHORT:     "SHORT",
	SQL_TYPE_LONG:      "LONG",
	SQL_TYPE_FLOAT:     "FLOAT",
	SQL_TYPE_TIME:      "TIME",
	SQL_TYPE_DATE:      "DATE",
	SQL_TYPE_DOUBLE:    "DOUBLE",
	SQL_TYPE_TIMESTAMP: "TIMESTAMP",
	SQL_TYPE_BLOB:      "BLOB",
	SQL_TYPE_ARRAY:     "ARRAY",
	SQL_TYPE_QUAD:      "QUAD",
	SQL_TYPE_INT64:     "INT64",
	SQL_TYPE_BOOLEAN:   "BOOLEAN",
}

type xSQLVAR struct {
	sqltype    int
	sqlscale   int
	sqlsubtype int
	sqllen     int
	null_ok    bool
	fieldname  string
	relname    string
	ownname    string
	aliasname  string
}

func (x *xSQLVAR) ioLength() int {
	if x.sqltype == SQL_TYPE_TEXT {
		return x.sqllen
	} else {
		return xsqlvarTypeLength[x.sqltype]
	}
}

func (x *xSQLVAR) displayLength() int {
	if x.sqltype == SQL_TYPE_TEXT {
		return x.sqllen
	} else {
		return xsqlvarTypeDisplayLength[x.sqltype]
	}
}

func (x *xSQLVAR) nullable() bool {
	return x.null_ok
}

func (x *xSQLVAR) scale() int {
	return x.sqlscale
}

func (x *xSQLVAR) hasPrecisionScale() bool {
	return (x.sqltype == SQL_TYPE_SHORT || x.sqltype == SQL_TYPE_LONG || x.sqltype == SQL_TYPE_QUAD || x.sqltype == SQL_TYPE_INT64) && x.sqlscale != 0
}

func (x *xSQLVAR) typename() string {
	return xsqlvarTypeName[x.sqltype]
}

func (x *xSQLVAR) scantype() reflect.Type {
	switch x.sqltype {
	case SQL_TYPE_TEXT:
		return reflect.TypeOf("")
	case SQL_TYPE_VARYING:
		return reflect.TypeOf("")
	case SQL_TYPE_SHORT:
		if x.sqlscale != 0 {
			//return reflect.TypeOf(big.NewRat(0, 1))
			return reflect.TypeOf(float64(0))
		}
		return reflect.TypeOf(int16(0))
	case SQL_TYPE_LONG:
		if x.sqlscale != 0 {
			//return reflect.TypeOf(big.NewRat(0, 1))
			return reflect.TypeOf(float64(0))
		}
		return reflect.TypeOf(int32(0))
	case SQL_TYPE_INT64:
		if x.sqlscale != 0 {
			//return reflect.TypeOf(big.NewRat(0, 1))
			return reflect.TypeOf(float64(0))
		}
		return reflect.TypeOf(int64(0))
	case SQL_TYPE_DATE:
		return reflect.TypeOf(time.Time{})
	case SQL_TYPE_TIME:
		return reflect.TypeOf(time.Time{})
	case SQL_TYPE_TIMESTAMP:
		return reflect.TypeOf(time.Time{})
	case SQL_TYPE_FLOAT:
		return reflect.TypeOf(float32(0))
	case SQL_TYPE_DOUBLE:
		return reflect.TypeOf(float64(0))
	case SQL_TYPE_BOOLEAN:
		return reflect.TypeOf(false)
	case SQL_TYPE_BLOB:
		return reflect.TypeOf([]byte{})
	}
	return reflect.TypeOf(nil)
}

func (x *xSQLVAR) _parseDate(raw_value []byte) (int, int, int) {
	nday := int(bytes_to_bint32(raw_value)) + 678882
	century := (4*nday - 1) / 146097
	nday = 4*nday - 1 - 146097*century
	day := nday / 4

	nday = (4*day + 3) / 1461
	day = 4*day + 3 - 1461*nday
	day = (day + 4) / 4

	month := (5*day - 3) / 153
	day = 5*day - 3 - 153*month
	day = (day + 5) / 5
	year := 100*century + nday
	if month < 10 {
		month += 3
	} else {
		month -= 9
		year += 1
	}
	return year, month, day
}

func (x *xSQLVAR) _parseTime(raw_value []byte) (int, int, int, int) {
	n := int(bytes_to_bint32(raw_value))
	s := n / 10000
	m := s / 60
	h := m / 60
	m = m % 60
	s = s % 60
	return h, m, s, (n % 10000) * 100000
}

func (x *xSQLVAR) parseDate(raw_value []byte) time.Time {
	year, month, day := x._parseDate(raw_value)
	return time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
}

func (x *xSQLVAR) parseTime(raw_value []byte) time.Time {
	h, m, s, n := x._parseTime(raw_value)
	return time.Date(0, time.Month(1), 1, h, m, s, n, time.UTC)
}

func (x *xSQLVAR) parseTimestamp(raw_value []byte) time.Time {
	year, month, day := x._parseDate(raw_value[:4])
	h, m, s, n := x._parseTime(raw_value[4:])
	return time.Date(year, time.Month(month), day, h, m, s, n, time.UTC)
}

func (x *xSQLVAR) value(raw_value []byte) (v interface{}, err error) {
	switch x.sqltype {
	case SQL_TYPE_TEXT:
		if x.sqlsubtype == 1 { // OCTETS
			v = raw_value
		} else {
			v = bytes.NewBuffer(raw_value).String()
		}
	case SQL_TYPE_VARYING:
		if x.sqlsubtype == 1 { // OCTETS
			v = raw_value
		} else {
			v = bytes.NewBuffer(raw_value).String()
		}
	case SQL_TYPE_SHORT:
		i16 := int16(bytes_to_bint32(raw_value))
		if x.sqlscale > 0 {
			v = int64(i16) * int64(math.Pow10(x.sqlscale))
		} else if x.sqlscale < 0 {
			//v = big.NewRat(int64(i16), int64(math.Pow10(x.sqlscale*-1)))
			v = float64(i16) / float64(math.Pow10(x.sqlscale*-1))
		} else {
			v = i16
		}
	case SQL_TYPE_LONG:
		i32 := bytes_to_bint32(raw_value)
		if x.sqlscale > 0 {
			v = int64(i32) * int64(math.Pow10(x.sqlscale))
		} else if x.sqlscale < 0 {
			//v = big.NewRat(int64(i32), int64(math.Pow10(x.sqlscale*-1)))
			v = float64(i32) / float64(math.Pow10(x.sqlscale*-1))
		} else {
			v = i32
		}
	case SQL_TYPE_INT64:
		i64 := bytes_to_bint64(raw_value)
		if x.sqlscale > 0 {
			v = i64 * int64(math.Pow10(x.sqlscale))
		} else if x.sqlscale < 0 {
			//v = big.NewRat(i64, int64(math.Pow10(x.sqlscale*-1)))
			v = float64(i64) / float64(math.Pow10(x.sqlscale*-1))
		} else {
			v = i64
		}
	case SQL_TYPE_DATE:
		v = x.parseDate(raw_value)
	case SQL_TYPE_TIME:
		v = x.parseTime(raw_value)
	case SQL_TYPE_TIMESTAMP:
		v = x.parseTimestamp(raw_value)
	case SQL_TYPE_FLOAT:
		var f32 float32
		b := bytes.NewReader(raw_value)
		err = binary.Read(b, binary.BigEndian, &f32)
		v = f32
	case SQL_TYPE_DOUBLE:
		b := bytes.NewReader(raw_value)
		var f64 float64
		err = binary.Read(b, binary.BigEndian, &f64)
		v = f64
	case SQL_TYPE_BOOLEAN:
		v = raw_value[0] != 0
	case SQL_TYPE_BLOB:
		v = raw_value
	}
	return
}
