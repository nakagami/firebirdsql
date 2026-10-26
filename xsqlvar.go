/*******************************************************************************
The MIT License (MIT)

Copyright (c) 2013-2020 Hajime Nakagami

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
	"math/big"
	"reflect"
	"strings"
	"time"
)

const (
	SQL_TYPE_TEXT         = 452
	SQL_TYPE_VARYING      = 448
	SQL_TYPE_SHORT        = 500
	SQL_TYPE_LONG         = 496
	SQL_TYPE_FLOAT        = 482
	SQL_TYPE_DOUBLE       = 480
	SQL_TYPE_D_FLOAT      = 530
	SQL_TYPE_TIMESTAMP    = 510
	SQL_TYPE_BLOB         = 520
	SQL_TYPE_ARRAY        = 540
	SQL_TYPE_QUAD         = 550
	SQL_TYPE_TIME         = 560
	SQL_TYPE_DATE         = 570
	SQL_TYPE_INT64        = 580
	SQL_TYPE_INT128       = 32752
	SQL_TYPE_TIMESTAMP_TZ = 32754
	SQL_TYPE_TIME_TZ      = 32756
	SQL_TYPE_DEC_FIXED    = 32758
	SQL_TYPE_DEC64        = 32760
	SQL_TYPE_DEC128       = 32762
	SQL_TYPE_BOOLEAN      = 32764
	SQL_TYPE_NULL         = 32766
)

var xsqlvarTypeLength = map[int]int{
	SQL_TYPE_TEXT:         -1,
	SQL_TYPE_VARYING:      -1,
	SQL_TYPE_SHORT:        4,
	SQL_TYPE_LONG:         4,
	SQL_TYPE_FLOAT:        4,
	SQL_TYPE_TIME:         4,
	SQL_TYPE_DATE:         4,
	SQL_TYPE_DOUBLE:       8,
	SQL_TYPE_TIMESTAMP:    8,
	SQL_TYPE_BLOB:         8,
	SQL_TYPE_ARRAY:        8,
	SQL_TYPE_QUAD:         8,
	SQL_TYPE_INT64:        8,
	SQL_TYPE_INT128:       16,
	SQL_TYPE_TIMESTAMP_TZ: 12,
	SQL_TYPE_TIME_TZ:      8,
	SQL_TYPE_DEC64:        8,
	SQL_TYPE_DEC128:       16,
	SQL_TYPE_DEC_FIXED:    16,
	SQL_TYPE_BOOLEAN:      1,
}

var xsqlvarTypeDisplayLength = map[int]int{
	SQL_TYPE_TEXT:         -1,
	SQL_TYPE_VARYING:      -1,
	SQL_TYPE_SHORT:        6,
	SQL_TYPE_LONG:         11,
	SQL_TYPE_FLOAT:        17,
	SQL_TYPE_TIME:         11,
	SQL_TYPE_DATE:         10,
	SQL_TYPE_DOUBLE:       17,
	SQL_TYPE_TIMESTAMP:    22,
	SQL_TYPE_BLOB:         0,
	SQL_TYPE_ARRAY:        -1,
	SQL_TYPE_QUAD:         20,
	SQL_TYPE_INT64:        20,
	SQL_TYPE_INT128:       20,
	SQL_TYPE_TIMESTAMP_TZ: 28,
	SQL_TYPE_TIME_TZ:      17,
	SQL_TYPE_DEC64:        16,
	SQL_TYPE_DEC128:       34,
	SQL_TYPE_DEC_FIXED:    34,
	SQL_TYPE_BOOLEAN:      5,
}

var xsqlvarTypeName = map[int]string{
	SQL_TYPE_TEXT:         "TEXT",
	SQL_TYPE_VARYING:      "VARYING",
	SQL_TYPE_SHORT:        "SHORT",
	SQL_TYPE_LONG:         "LONG",
	SQL_TYPE_FLOAT:        "FLOAT",
	SQL_TYPE_TIME:         "TIME",
	SQL_TYPE_DATE:         "DATE",
	SQL_TYPE_DOUBLE:       "DOUBLE",
	SQL_TYPE_TIMESTAMP:    "TIMESTAMP",
	SQL_TYPE_BLOB:         "BLOB",
	SQL_TYPE_ARRAY:        "ARRAY",
	SQL_TYPE_QUAD:         "QUAD",
	SQL_TYPE_INT64:        "INT64",
	SQL_TYPE_INT128:       "INT128",
	SQL_TYPE_TIMESTAMP_TZ: "TIMESTAMP WITH TIMEZONE",
	SQL_TYPE_TIME_TZ:      "TIME WITH TIMEZONE",
	SQL_TYPE_DEC64:        "DECFLOAT(16)",
	SQL_TYPE_DEC128:       "DECFLOAT(34)",
	SQL_TYPE_DEC_FIXED:    "DECFIXED",
	SQL_TYPE_BOOLEAN:      "BOOLEAN",
}

type xSQLVAR struct {
	sqltype    int
	sqlscale   int
	sqlsubtype int
	sqllen     int
	// msglen is the byte length declared for a text column in the output
	// message BLR, when it differs from sqllen (see widenUntransliteratedText).
	// Zero means sqllen. Metadata (displayLength, ColumnTypeLength) keeps
	// using sqllen.
	msglen    int
	null_ok   bool
	fieldname string
	relname   string
	ownname   string
	aliasname string

	// arrayMeta carries the resolved system-table metadata for array-typed
	// vars (SQL_TYPE_ARRAY); nil when the column is not an array or the
	// metadata could not be resolved. See array_meta.go.
	arrayMeta *ArrayMeta
}

// messageLength is the byte length of a text column in the message buffer.
func (x *xSQLVAR) messageLength() int {
	if x.msglen > 0 {
		return x.msglen
	}
	return x.sqllen
}

func (x *xSQLVAR) ioLength() int {
	if x.sqltype == SQL_TYPE_TEXT {
		return x.messageLength()
	}
	return xsqlvarTypeLength[x.sqltype]
}

func (x *xSQLVAR) displayLength() int {
	if x.sqltype == SQL_TYPE_TEXT || x.sqltype == SQL_TYPE_VARYING {
		return x.sqllen
	}
	return xsqlvarTypeDisplayLength[x.sqltype]
}

func (x *xSQLVAR) hasPrecisionScale() bool {
	return (x.sqltype == SQL_TYPE_SHORT || x.sqltype == SQL_TYPE_LONG || x.sqltype == SQL_TYPE_QUAD || x.sqltype == SQL_TYPE_INT64 || x.sqltype == SQL_TYPE_INT128 || x.sqltype == SQL_TYPE_DEC64 || x.sqltype == SQL_TYPE_DEC128 || x.sqltype == SQL_TYPE_DEC_FIXED) && x.sqlscale != 0
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
	case SQL_TYPE_SHORT, SQL_TYPE_LONG, SQL_TYPE_INT64:
		if x.sqlscale != 0 {
			return reflect.TypeOf("")
		}
		return reflect.TypeOf(int64(0))
	case SQL_TYPE_INT128:
		return reflect.TypeOf("")
	case SQL_TYPE_DATE:
		return reflect.TypeOf(time.Time{})
	case SQL_TYPE_TIME:
		return reflect.TypeOf(time.Time{})
	case SQL_TYPE_TIMESTAMP:
		return reflect.TypeOf(time.Time{})
	case SQL_TYPE_FLOAT:
		return reflect.TypeOf(float64(0))
	case SQL_TYPE_DOUBLE:
		return reflect.TypeOf(float64(0))
	case SQL_TYPE_BOOLEAN:
		return reflect.TypeOf(false)
	case SQL_TYPE_BLOB:
		return reflect.TypeOf([]byte{})
	case SQL_TYPE_ARRAY:
		return reflect.TypeOf(ArrayValue{})
	case SQL_TYPE_TIMESTAMP_TZ:
		return reflect.TypeOf(time.Time{})
	case SQL_TYPE_TIME_TZ:
		return reflect.TypeOf(time.Time{})
	case SQL_TYPE_DEC64:
		return reflect.TypeOf("")
	case SQL_TYPE_DEC128:
		return reflect.TypeOf("")
	case SQL_TYPE_DEC_FIXED:
		return reflect.TypeOf("")
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
		year++
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

// locationByName converts a session timezone name to a Go location. Fixed
// UTC offsets ("+05:30", "-08:00", "+0530", "+05") are also accepted; IANA
// LoadLocation does not accept them. A name with no match gives time.UTC.
// The return value is never nil. A nil location makes the next time.Date
// call panic, and the panic stops the request in progress.
func locationByName(name string) *time.Location {
	if tz, err := time.LoadLocation(name); err == nil {
		return tz
	}
	if secs, ok := parseUTCOffset(name); ok {
		return time.FixedZone(formatUTCOffset(secs), secs)
	}
	return time.UTC
}

func (x *xSQLVAR) parseDate(raw_value []byte, timezone string) time.Time {
	tz := time.Local
	if timezone != "" {
		tz = locationByName(timezone)
	}
	year, month, day := x._parseDate(raw_value)
	return time.Date(year, time.Month(month), day, 0, 0, 0, 0, tz)
}

func (x *xSQLVAR) parseTime(raw_value []byte, timezone string) time.Time {
	tz := time.Local
	if timezone != "" {
		tz = locationByName(timezone)
	}
	h, m, s, n := x._parseTime(raw_value)
	now := time.Now()
	zone, offset := time.Date(now.Year(), now.Month(), now.Day(), h, m, s, n, tz).Zone()
	return time.Date(0, time.Month(1), 1, h, m, s, n, time.FixedZone(zone, offset))
}

func (x *xSQLVAR) parseTimestamp(raw_value []byte, timezone string) time.Time {
	tz := time.Local
	if timezone != "" {
		tz = locationByName(timezone)
	}

	year, month, day := x._parseDate(raw_value[:4])
	h, m, s, n := x._parseTime(raw_value[4:8])
	return time.Date(year, time.Month(month), day, h, m, s, n, tz)
}

// _parseTimezoneID resolves the 2-byte Firebird timezone id carried in a
// WITH TIME ZONE wire value. Ids in the GMT-displacement range become fixed
// zones carrying the offset (Firebird names them "+05:30", "-08:00", ...);
// named ids resolve through the zone map. Unresolvable ids fall back to UTC.
func _parseTimezoneID(idRaw []byte) *time.Location {
	id := int(bytes_to_buint16(idRaw))
	if id <= maxDisplacementOffsetMinutes*2 {
		return offsetTimezoneLocation(id)
	}
	name := getTimezoneNameByID(id)
	if name != "" {
		if tz, err := time.LoadLocation(name); err == nil {
			return tz
		}
	}
	return time.UTC
}

// timeTzBaseDate is the date Firebird uses to resolve the UTC offset of a
// date-less TIME WITH TIME ZONE value (mirrors Jaybird's TIME_TZ_BASE_DATE).
var timeTzBaseDate = time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)

func (x *xSQLVAR) parseTimeTz(raw_value []byte) time.Time {
	// Wire layout: UTC time-of-day (4) + timezone id (2) + displacement (2).
	h, m, s, n := x._parseTime(raw_value[:4])
	loc := _parseTimezoneID(raw_value[6:8])
	t := time.Date(timeTzBaseDate.Year(), timeTzBaseDate.Month(), timeTzBaseDate.Day(), h, m, s, n, time.UTC).In(loc)
	zone, offset := t.Zone()
	return time.Date(0, time.Month(1), 1, t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), time.FixedZone(zone, offset))
}

func (x *xSQLVAR) parseTimestampTz(raw_value []byte) time.Time {
	// Wire layout: UTC timestamp (8) + timezone id (2) + displacement (2).
	year, month, day := x._parseDate(raw_value[:4])
	h, m, s, n := x._parseTime(raw_value[4:8])
	loc := _parseTimezoneID(raw_value[10:12])
	return time.Date(year, time.Month(month), day, h, m, s, n, time.UTC).In(loc)
}

func (x *xSQLVAR) parseString(raw_value []byte, charset string) interface{} {
	if x.sqlsubtype == 1 || charset == "OCTETS" {
		return raw_value
	}
	if charset == "UNICODE_FSS" || charset == "UTF8" {
		return string(raw_value)
	}
	if v, ok := decodeCharset(raw_value, charset); ok {
		return v
	}
	return raw_value
}

// maxDecimalScale bounds sqlscale before it drives big.Int.Exp(10, sqlscale).
// sqlscale comes off the wire in the describe response (see _parse_select_items'
// isc_info_sql_scale case); Firebird's legitimate NUMERIC/DECIMAL scale never
// exceeds 38 (FB4+; 18 older), so a larger value is rejected, not clamped.
const maxDecimalScale = 38

func (x *xSQLVAR) scaledIntValue(i int64) (interface{}, error) {
	switch {
	case x.sqlscale > 0:
		if x.sqlscale > maxDecimalScale {
			// Surfaces from inside readRow after the column's raw bytes were consumed but
			// before the rest of the row is read — the conn is desynced, so mark it bad.
			return nil, fmt.Errorf("firebirdsql: NUMERIC/DECIMAL scale %d out of range: %w", x.sqlscale, driver.ErrBadConn)
		}
		// Plain integer matches isql; formatDecimalGDA would emit "5E+2".
		mul := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(x.sqlscale)), nil)
		return new(big.Int).Mul(big.NewInt(i), mul).String(), nil
	case x.sqlscale < 0:
		return formatDecimalGDA(new(big.Int).Abs(big.NewInt(i)), int32(x.sqlscale), i < 0, ""), nil
	default:
		return i, nil
	}
}

func (x *xSQLVAR) value(raw_value []byte, timezone string, charset string) (v interface{}, err error) {
	switch x.sqltype {
	case SQL_TYPE_TEXT:
		if x.sqlsubtype == 1 || charset == "None" { // OCTETS
			v = raw_value
		} else {
			v = x.parseString(raw_value, charset)
			switch v.(type) {
			case string:
				v = strings.TrimRight(v.(string), " ")
			case []uint8:
				v = strings.TrimRight(string(v.([]uint8)), " ")
			}
		}
	case SQL_TYPE_VARYING:
		if x.sqlsubtype == 1 { // OCTETS
			v = raw_value
		} else {
			v = x.parseString(raw_value, charset)
		}
	case SQL_TYPE_SHORT:
		v, err = x.scaledIntValue(int64(int16(bytes_to_bint32(raw_value))))
	case SQL_TYPE_LONG:
		v, err = x.scaledIntValue(int64(bytes_to_bint32(raw_value)))
	case SQL_TYPE_INT64:
		v, err = x.scaledIntValue(bytes_to_bint64(raw_value))
	case SQL_TYPE_INT128:
		n := new(big.Int).SetBytes(raw_value)
		if raw_value[0]&0x80 != 0 {
			n.Sub(n, new(big.Int).Lsh(big.NewInt(1), 128))
		}
		switch {
		case x.sqlscale < 0:
			v = formatDecimalGDA(new(big.Int).Abs(n), int32(x.sqlscale), n.Sign() < 0, "")
		case x.sqlscale > 0:
			if x.sqlscale > maxDecimalScale {
				// Same desync hazard as scaledIntValue's identical guard above.
				err = fmt.Errorf("firebirdsql: NUMERIC/DECIMAL scale %d out of range: %w", x.sqlscale, driver.ErrBadConn)
				break
			}
			mul := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(x.sqlscale)), nil)
			v = new(big.Int).Mul(n, mul).String()
		default:
			v = n.String()
		}
	case SQL_TYPE_DATE:
		v = x.parseDate(raw_value, timezone)
	case SQL_TYPE_TIME:
		v = x.parseTime(raw_value, timezone)
	case SQL_TYPE_TIMESTAMP:
		v = x.parseTimestamp(raw_value, timezone)
	case SQL_TYPE_TIME_TZ:
		v = x.parseTimeTz(raw_value)
	case SQL_TYPE_TIMESTAMP_TZ:
		v = x.parseTimestampTz(raw_value)
	case SQL_TYPE_FLOAT:
		var f32 float32
		b := bytes.NewReader(raw_value)
		err = binary.Read(b, binary.BigEndian, &f32)
		v = float64(f32)
	case SQL_TYPE_DOUBLE:
		b := bytes.NewReader(raw_value)
		var f64 float64
		err = binary.Read(b, binary.BigEndian, &f64)
		v = f64
	case SQL_TYPE_BOOLEAN:
		v = raw_value[0] != 0
	case SQL_TYPE_BLOB:
		v = raw_value
	case SQL_TYPE_ARRAY:
		// Raw 8-byte array id; materialized into an ArrayValue by
		// firebirdsqlRows.Next (a get_slice round-trip must not run
		// mid-row while the fetch response is still streaming).
		v = raw_value
	case SQL_TYPE_DEC_FIXED:
		v, err = decimalFixedToString(raw_value, int32(x.sqlscale))
	case SQL_TYPE_DEC64:
		v, err = decimal64ToString(raw_value)
	case SQL_TYPE_DEC128:
		v, err = decimal128ToString(raw_value)
	}
	return
}

// widenUntransliteratedText sizes the output message buffer of text columns
// that the server returns in their own charset instead of the connection
// charset.
//
// Under a multi-byte connection charset (UTF8, UNICODE_FSS, SJIS_0208, ...)
// the server transliterates text columns and describes them in the
// connection charset: a WIN1252 VARCHAR(40) is described as sqllen 160 under
// UTF8. Columns in CHARACTER SET NONE (and OCTETS) are not transliterated and
// are described with their byte length: a NONE VARCHAR(40) is described as
// sqllen 40.
//
// The output message declares text items as blr_text/blr_varying, which carry
// no charset. Firebird 2.5 to 5.0 treat such an item's length as bytes.
// Firebird 6 treats it as characters of the connection charset, so a 40-byte
// item holds only 40/4 = 10 UTF8 characters and fetching a longer NONE value
// fails with "string right truncation, expected length 10, actual 15". The
// same happens to system columns such as RDB$CONSTRAINT_TYPE (CHAR(11)
// CHARACTER SET NONE): "expected length 2, actual 11".
//
// Declaring charLength * connBytesPerChar bytes satisfies Firebird 6, and is
// harmless on Firebird 3 to 5: VARYING values carry their own length, and the
// blank padding of a wider CHAR buffer is trimmed like any CHAR padding.
// The widened length is capped to fit the 16-bit BLR length. It is applied on
// protocol 13+ only (Firebird 3+): Firebird 2.5 is not affected, and rejects
// messages over 64 KB with "SQLDA error", which widening can produce.
// node-firebird applies the same widening (its issue #422).
//
// OCTETS columns are binary, so they are left alone: they are never
// transliterated and their byte length is exact.
func widenUntransliteratedText(xsqlda []xSQLVAR, connBytesPerChar int) {
	if connBytesPerChar <= 1 {
		return
	}
	maxLen := 0xFFFF / connBytesPerChar * connBytesPerChar
	for i := range xsqlda {
		x := &xsqlda[i]
		if x.sqltype != SQL_TYPE_TEXT && x.sqltype != SQL_TYPE_VARYING {
			continue
		}
		charsetID := x.sqlsubtype & 0xFF
		if charsetID == charsetIDOctets {
			continue
		}
		colBytesPerChar := charsetBytesPerChar(charsetID)
		if colBytesPerChar >= connBytesPerChar {
			continue // transliterated, or already as wide as the connection charset
		}
		chars := x.sqllen / colBytesPerChar
		widened := chars * connBytesPerChar
		if widened > maxLen {
			widened = maxLen
		}
		if widened > x.sqllen {
			x.msglen = widened
		}
	}
}

func calcBlr(xsqlda []xSQLVAR) []byte {
	// Calculate  BLR from XSQLVAR array.
	ln := len(xsqlda) * 2
	blr := make([]byte, (ln*6)+8)
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
		case SQL_TYPE_VARYING, SQL_TYPE_TEXT:
			if x.sqlsubtype&0xFF == charsetIDOctets {
				// blr_varying2 / blr_text2 with CHARACTER SET OCTETS: the item
				// is binary, so the server copies the bytes without treating
				// them as text in the connection charset (Firebird 6 validates
				// charset-less items as such and rejects invalid UTF8)
				if x.sqltype == SQL_TYPE_VARYING {
					blr[n] = 38
				} else {
					blr[n] = 15
				}
				blr[n+1] = charsetIDOctets
				blr[n+2] = 0
				blr[n+3] = byte(x.sqllen & 255)
				blr[n+4] = byte(x.sqllen >> 8)
				n += 5
				break
			}
			if x.sqltype == SQL_TYPE_VARYING {
				blr[n] = 37
			} else {
				blr[n] = 14
			}
			blr[n+1] = byte(x.messageLength() & 255)
			blr[n+2] = byte(x.messageLength() >> 8)
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
		case SQL_TYPE_INT128:
			blr[n] = 26
			blr[n+1] = byte(sqlscale)
			n += 2
		case SQL_TYPE_QUAD:
			blr[n] = 9
			blr[n+1] = byte(sqlscale)
			n += 2
		case SQL_TYPE_DEC_FIXED: // OBSOLATED
			blr[n] = 26
			blr[n+1] = byte(sqlscale)
			n += 2
		case SQL_TYPE_DOUBLE:
			blr[n] = 27
			n++
		case SQL_TYPE_FLOAT:
			blr[n] = 10
			n++
		case SQL_TYPE_D_FLOAT:
			blr[n] = 11
			n++
		case SQL_TYPE_DATE:
			blr[n] = 12
			n++
		case SQL_TYPE_TIME:
			blr[n] = 13
			n++
		case SQL_TYPE_TIMESTAMP:
			blr[n] = 35
			n++
		case SQL_TYPE_BLOB:
			blr[n] = 9
			blr[n+1] = 0
			n += 2
		case SQL_TYPE_ARRAY:
			blr[n] = 9
			blr[n+1] = 0
			n += 2
		case SQL_TYPE_BOOLEAN:
			blr[n] = 23
			n++
		case SQL_TYPE_NULL:
			blr[n] = 14 // blr_text
			blr[n+1] = 0
			blr[n+2] = 0
			n += 3
		case SQL_TYPE_DEC64:
			blr[n] = 24
			n++
		case SQL_TYPE_DEC128:
			blr[n] = 25
			n++
		case SQL_TYPE_TIME_TZ:
			blr[n] = 28
			n++
		case SQL_TYPE_TIMESTAMP_TZ:
			blr[n] = 29
			n++
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
