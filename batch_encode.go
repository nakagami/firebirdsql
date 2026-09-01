/*******************************************************************************
The MIT License (MIT)

Copyright (c) 2026 Alexey Kovyazin

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
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"time"
)

func calculateBatchMessageLength(xsqlda []xSQLVAR) int {
	length := 0
	for _, x := range xsqlda {
		fieldLength := x.sqllen
		align := 1
		switch x.sqltype {
		case SQL_TYPE_TEXT, SQL_TYPE_NULL, SQL_TYPE_BOOLEAN:
			align = 1
		case SQL_TYPE_SHORT:
			align = 2
		case SQL_TYPE_VARYING:
			align = 2
			fieldLength += 2
		case SQL_TYPE_FLOAT, SQL_TYPE_LONG, SQL_TYPE_DATE, SQL_TYPE_TIME, SQL_TYPE_TIMESTAMP,
			SQL_TYPE_BLOB, SQL_TYPE_ARRAY, SQL_TYPE_QUAD, SQL_TYPE_TIMESTAMP_TZ, SQL_TYPE_TIME_TZ:
			align = 4
		case SQL_TYPE_DOUBLE, SQL_TYPE_D_FLOAT, SQL_TYPE_INT64, SQL_TYPE_DEC64, SQL_TYPE_DEC128, SQL_TYPE_INT128:
			align = 8
		default:
			align = 4
		}
		if align > 1 {
			length = (length + align - 1) & ^(align - 1)
		}
		length += fieldLength
		length = ((length + 1) & ^1) + 2
	}
	return length
}

func (p *wireProtocol) encodeBatchRow(xsqlda []xSQLVAR, args []driver.Value) ([]byte, error) {
	if len(args) != len(xsqlda) {
		return nil, fmt.Errorf("firebirdsql: expected %d batch args, got %d", len(xsqlda), len(args))
	}
	n := (len(xsqlda) + 7) / 8
	if n%4 != 0 {
		n += 4 - n%4
	}
	nullBytes := make([]byte, n)
	fieldBufs := make([][]byte, len(args))
	for i, arg := range args {
		if arg == nil {
			nullBytes[i/8] |= 1 << (uint(i) % 8)
			continue
		}
		b, err := p.encodeBatchField(xsqlda[i], arg)
		if err != nil {
			return nil, err
		}
		fieldBufs[i] = b
	}

	var out bytes.Buffer
	out.Write(nullBytes)
	for i, x := range xsqlda {
		if nullBytes[i/8]&(1<<(uint(i)%8)) != 0 {
			continue
		}
		if x.sqltype == SQL_TYPE_NULL {
			continue
		}
		fb := fieldBufs[i]
		switch x.sqltype {
		case SQL_TYPE_VARYING:
			out.Write(bint32_to_bytes(int32(len(fb))))
			out.Write(fb)
			pad := (4 - len(fb)) & 3
			if pad > 0 {
				out.Write(bytes.Repeat([]byte{0x20}, pad))
			}
		case SQL_TYPE_TEXT:
			need := x.sqllen
			if len(fb) > need {
				fb = fb[:need]
			}
			out.Write(fb)
			if len(fb) < need {
				out.Write(bytes.Repeat([]byte{0x20}, need-len(fb)))
			}
			pad := (4 - need) & 3
			if pad > 0 {
				out.Write(bytes.Repeat([]byte{0x20}, pad))
			}
		case SQL_TYPE_BOOLEAN:
			out.Write(fb)
			pad := (4 - len(fb)) & 3
			if pad > 0 {
				out.Write(make([]byte, pad))
			}
		default:
			out.Write(fb)
		}
	}
	return out.Bytes(), nil
}

func (p *wireProtocol) encodeBatchField(x xSQLVAR, arg driver.Value) ([]byte, error) {
	switch x.sqltype {
	case SQL_TYPE_SHORT, SQL_TYPE_LONG:
		v, err := toInt64(arg)
		if err != nil {
			return nil, err
		}
		return bint32_to_bytes(int32(v)), nil
	case SQL_TYPE_INT64, SQL_TYPE_QUAD:
		v, err := toInt64(arg)
		if err != nil {
			return nil, err
		}
		return bint64_to_bytes(v), nil
	case SQL_TYPE_FLOAT:
		f, err := toFloat64(arg)
		if err != nil {
			return nil, err
		}
		buf := make([]byte, 4)
		binary.BigEndian.PutUint32(buf, math.Float32bits(float32(f)))
		return buf, nil
	case SQL_TYPE_DOUBLE:
		f, err := toFloat64(arg)
		if err != nil {
			return nil, err
		}
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, math.Float64bits(f))
		return buf, nil
	case SQL_TYPE_BOOLEAN:
		b, err := toBool(arg)
		if err != nil {
			return nil, err
		}
		if b {
			return []byte{1}, nil
		}
		return []byte{0}, nil
	case SQL_TYPE_TEXT, SQL_TYPE_VARYING:
		s, err := toString(arg)
		if err != nil {
			return nil, err
		}
		s = p.encodeString(s)
		b := []byte(s)
		if x.sqllen > 0 && len(b) > x.sqllen {
			b = b[:x.sqllen]
		}
		return b, nil
	case SQL_TYPE_DATE:
		t, err := toTime(arg)
		if err != nil {
			return nil, err
		}
		return _convert_date(t), nil
	case SQL_TYPE_TIME:
		t, err := toTime(arg)
		if err != nil {
			return nil, err
		}
		return _convert_time(t), nil
	case SQL_TYPE_TIMESTAMP:
		t, err := toTime(arg)
		if err != nil {
			return nil, err
		}
		return _convert_timestamp(t), nil
	case SQL_TYPE_BLOB, SQL_TYPE_ARRAY:
		return nil, fmt.Errorf("firebirdsql: batch does not support blob/array parameters")
	default:
		s, err := toString(arg)
		if err != nil {
			return nil, err
		}
		s = p.encodeString(s)
		return []byte(s), nil
	}
}

func toInt64(v driver.Value) (int64, error) {
	switch t := v.(type) {
	case int:
		return int64(t), nil
	case int8:
		return int64(t), nil
	case int16:
		return int64(t), nil
	case int32:
		return int64(t), nil
	case int64:
		return t, nil
	case uint:
		return int64(t), nil
	case uint8:
		return int64(t), nil
	case uint16:
		return int64(t), nil
	case uint32:
		return int64(t), nil
	case uint64:
		return int64(t), nil
	case float64:
		return int64(t), nil
	case float32:
		return int64(t), nil
	case string:
		return strconv.ParseInt(t, 10, 64)
	case []byte:
		return strconv.ParseInt(string(t), 10, 64)
	default:
		rv := reflect.ValueOf(v)
		switch rv.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return rv.Int(), nil
		}
		return 0, fmt.Errorf("cannot convert %T to int64", v)
	}
}

func toFloat64(v driver.Value) (float64, error) {
	switch t := v.(type) {
	case float32:
		return float64(t), nil
	case float64:
		return t, nil
	case int:
		return float64(t), nil
	case int32:
		return float64(t), nil
	case int64:
		return float64(t), nil
	case string:
		return strconv.ParseFloat(t, 64)
	default:
		return 0, fmt.Errorf("cannot convert %T to float64", v)
	}
}

func toBool(v driver.Value) (bool, error) {
	switch t := v.(type) {
	case bool:
		return t, nil
	case int:
		return t != 0, nil
	case int64:
		return t != 0, nil
	default:
		return false, fmt.Errorf("cannot convert %T to bool", v)
	}
}

func toString(v driver.Value) (string, error) {
	switch t := v.(type) {
	case string:
		return t, nil
	case []byte:
		return string(t), nil
	case fmt.Stringer:
		return t.String(), nil
	default:
		return fmt.Sprintf("%v", v), nil
	}
}

func toTime(v driver.Value) (time.Time, error) {
	switch t := v.(type) {
	case time.Time:
		return t, nil
	default:
		return time.Time{}, fmt.Errorf("cannot convert %T to time.Time", v)
	}
}
