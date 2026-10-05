/*******************************************************************************
The MIT License (MIT)

Copyright (c) 2026 IBSurgeon

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
	"context"
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
)

// arrayColumnKey identifies an array column for metadata lookup: relation and
// field name as returned by the describe (upper-cased, trimmed).
type arrayColumnKey struct {
	table string
	field string
}

// resolveArrayMeta resolves system-table metadata for every array-typed var
// in xsqlda (result or bind descriptors) and attaches it to xSQLVAR.arrayMeta.
// Vars whose relation/field names are unknown (expressions, procedure outputs)
// are left unresolved and fail later with a clear error if actually used.
func resolveArrayMeta(fc *firebirdsqlConn, xsqlda []xSQLVAR) error {
	if fc.resolvingArrayMeta {
		// Internal metadata queries never contain array columns; guard against
		// pathological recursion anyway.
		return nil
	}

	var pairs []arrayColumnKey
	seen := map[arrayColumnKey]bool{}
	for i := range xsqlda {
		x := &xsqlda[i]
		x.arrayMeta = nil
		if x.sqltype != SQL_TYPE_ARRAY || x.relname == "" || x.fieldname == "" {
			continue
		}
		k := arrayColumnKey{strings.ToUpper(strings.TrimSpace(x.relname)), strings.ToUpper(strings.TrimSpace(x.fieldname))}
		if !seen[k] {
			seen[k] = true
			pairs = append(pairs, k)
		}
	}
	if len(pairs) == 0 {
		return nil
	}

	fc.resolvingArrayMeta = true
	defer func() { fc.resolvingArrayMeta = false }()

	metas, dimensions, err := fc.queryArrayMeta(pairs)
	if err != nil {
		return fmt.Errorf("firebirdsql: array metadata lookup failed: %w", err)
	}

	for _, am := range metas {
		am.Dimensions = dimensions[am.fieldSource]
	}

	for i := range xsqlda {
		x := &xsqlda[i]
		if x.sqltype != SQL_TYPE_ARRAY || x.relname == "" || x.fieldname == "" {
			continue
		}
		k := arrayColumnKey{strings.ToUpper(strings.TrimSpace(x.relname)), strings.ToUpper(strings.TrimSpace(x.fieldname))}
		x.arrayMeta = metas[k]
	}
	return nil
}

// queryArrayMeta resolves element type/scale/length/subtype from
// RDB$RELATION_FIELDS x RDB$FIELDS and per-source-field dimensions from
// RDB$FIELD_DIMENSIONS. Port of fbx getArrayMetadata + getArrayDimensions +
// getArrayInfo (non-schema dialect; FB6/RDB schema support deferred).
func (fc *firebirdsqlConn) queryArrayMeta(pairs []arrayColumnKey) (map[arrayColumnKey]*ArrayMeta, map[string][]ArrayDimension, error) {
	tables := make([]string, 0, len(pairs))
	fields := make([]string, 0, len(pairs))
	for _, p := range pairs {
		tables = append(tables, quoteMetaLiteral(p.table))
		fields = append(fields, quoteMetaLiteral(p.field))
	}

	query := "SELECT Y.RDB$FIELD_TYPE, Y.RDB$FIELD_SCALE, Y.RDB$FIELD_LENGTH, " +
		"CASE WHEN Y.RDB$FIELD_TYPE IN (" + strconv.Itoa(blr_type_text) + "," + strconv.Itoa(blr_type_varying) + ")" +
		" THEN Y.RDB$CHARACTER_SET_ID ELSE Y.RDB$FIELD_SUB_TYPE END, " +
		"CASE WHEN Y.RDB$FIELD_TYPE IN (" + strconv.Itoa(blr_type_text) + "," + strconv.Itoa(blr_type_varying) + ")" +
		" THEN Y.RDB$CHARACTER_LENGTH ELSE Y.RDB$FIELD_LENGTH END, " +
		"X.RDB$FIELD_SOURCE, X.RDB$RELATION_NAME, X.RDB$FIELD_NAME, Y.RDB$FIELD_LENGTH " +
		"FROM RDB$RELATION_FIELDS X, RDB$FIELDS Y " +
		"WHERE X.RDB$FIELD_SOURCE = Y.RDB$FIELD_NAME " +
		"AND X.RDB$RELATION_NAME IN (" + strings.Join(tables, ",") + ") " +
		"AND X.RDB$FIELD_NAME IN (" + strings.Join(fields, ",") + ")"

	rows, err := fc.internalQuery(query)
	if err != nil {
		return nil, nil, err
	}

	metas := map[arrayColumnKey]*ArrayMeta{}
	dimensions := map[string][]ArrayDimension{}
	var sources []string
	for _, row := range rows {
		if len(row) < 9 {
			return nil, nil, fmt.Errorf("short array metadata row (%d columns)", len(row))
		}
		blr, err := rowAsInt64(row[0])
		if err != nil {
			return nil, nil, fmt.Errorf("array element type: %w", err)
		}
		scale, err := rowAsInt64(row[1])
		if err != nil {
			return nil, nil, fmt.Errorf("array element scale: %w", err)
		}
		fieldBytes, err := rowAsInt64(row[2])
		if err != nil {
			return nil, nil, fmt.Errorf("array element length: %w", err)
		}
		sub := int64(0)
		if row[3] != nil {
			sub, err = rowAsInt64(row[3])
			if err != nil {
				return nil, nil, fmt.Errorf("array element sub-type: %w", err)
			}
		}
		// Element geometry in characters for text types (see the query above).
		length := fieldBytes
		if row[4] != nil {
			if charLen, err := rowAsInt64(row[4]); err == nil && charLen > 0 {
				length = charLen
			}
		}
		source := rowAsString(row[5])
		rel := rowAsString(row[6])
		field := rowAsString(row[7])

		typeID, err := arrayTypeFromBlr(blr)
		if err != nil {
			return nil, nil, err
		}
		am := &ArrayMeta{
			TableName:   rel,
			FieldName:   field,
			TypeID:      typeID,
			BlrTypeID:   int(blr),
			Scale:       int32(scale),
			Length:      uint32(length),
			FieldBytes:  uint32(fieldBytes),
			SubType:     int32(sub),
			fieldSource: source,
		}
		// NOTE: text element lengths are in CHARACTERS (RDB$CHARACTER_LENGTH),
		// which is the geometry the SDL slice transfer expects; RDB$FIELD_LENGTH
		// would be bytes in the column charset (CHAR(4) CHARACTER SET UTF8
		// reports 16).
		metas[arrayColumnKey{strings.ToUpper(rel), strings.ToUpper(field)}] = am
		if _, dup := dimensions[source]; !dup {
			sources = append(sources, source)
		}
	}

	if len(sources) > 0 {
		quoted := make([]string, 0, len(sources))
		for _, s := range sources {
			quoted = append(quoted, quoteMetaLiteral(s))
		}
		dimQuery := "SELECT X.RDB$LOWER_BOUND, X.RDB$UPPER_BOUND, X.RDB$FIELD_NAME " +
			"FROM RDB$FIELD_DIMENSIONS X " +
			"WHERE X.RDB$FIELD_NAME IN (" + strings.Join(quoted, ",") + ") " +
			"ORDER BY X.RDB$FIELD_NAME, X.RDB$DIMENSION"
		dimRows, err := fc.internalQuery(dimQuery)
		if err != nil {
			return nil, nil, err
		}
		for _, row := range dimRows {
			if len(row) < 3 {
				return nil, nil, fmt.Errorf("short array dimension row (%d columns)", len(row))
			}
			lower, err := rowAsInt64(row[0])
			if err != nil {
				return nil, nil, fmt.Errorf("array lower bound: %w", err)
			}
			upper, err := rowAsInt64(row[1])
			if err != nil {
				return nil, nil, fmt.Errorf("array upper bound: %w", err)
			}
			source := rowAsString(row[2])
			if upper < lower {
				return nil, nil, fmt.Errorf("array field %s has inverted bounds [%d,%d]", source, lower, upper)
			}
			dimensions[source] = append(dimensions[source], ArrayDimension{LowerBound: int(lower), Length: int(upper - lower + 1)})
		}
	}
	return metas, dimensions, nil
}

// internalQuery runs a metadata SELECT on this connection through the regular
// statement machinery and collects all rows. Must not be called while another
// result set is being consumed on the same connection (the wire is sequential);
// resolveArrayMeta runs at prepare time only, which satisfies that.
func (fc *firebirdsqlConn) internalQuery(query string) ([][]driver.Value, error) {
	stmt, err := newFirebirdsqlStmt(fc, query)
	if err != nil {
		return nil, err
	}
	defer func() { _ = stmt.Close() }()

	rows, err := stmt.query(context.Background(), nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	dest := make([]driver.Value, len(stmt.resultXsqlda))
	var out [][]driver.Value
	for {
		if err = rows.Next(dest); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		row := make([]driver.Value, len(dest))
		copy(row, dest)
		out = append(out, row)
	}
	return out, nil
}

func quoteMetaLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

func rowAsInt64(v driver.Value) (int64, error) {
	i, ok := v.(int64)
	if !ok {
		return 0, fmt.Errorf("expected integer, got %T", v)
	}
	return i, nil
}

func rowAsString(v driver.Value) string {
	switch s := v.(type) {
	case string:
		return strings.TrimSpace(s)
	case []byte:
		return strings.TrimSpace(string(s))
	}
	return ""
}

// encodeSliceElements packs flat element values into the wire slice layout.
func (p *wireProtocol) encodeSliceElements(meta *ArrayMeta, elements []any) ([]byte, error) {
	if declared := allElementsCount(meta.Dimensions); declared > 0 && len(elements) > declared {
		return nil, fmt.Errorf("array argument has %d elements but the column holds %d", len(elements), declared)
	}
	buf := make([]byte, 0, len(elements)*8)
	for i, e := range elements {
		b, err := p.encodeArrayElement(meta, e)
		if err != nil {
			return nil, fmt.Errorf("element %d: %w", i, err)
		}
		buf = append(buf, b...)
	}
	return buf, nil
}

// encodeArrayElement packs one element per the slice layout: fixed types big-
// endian, SHORT 4 bytes, BOOLEAN 1 value byte + 3 padding, TEXT space-padded to
// the element length and aligned to 4, VARCHAR 4-byte big-endian length prefix
// + data aligned to 4.
func (p *wireProtocol) encodeArrayElement(meta *ArrayMeta, e any) ([]byte, error) {
	var b8 [8]byte
	switch meta.TypeID {
	case SQL_TYPE_SHORT:
		v, err := toScaledInt64(e, meta.Scale)
		if err != nil {
			return nil, err
		}
		if v < math.MinInt16 || v > math.MaxInt16 {
			return nil, fmt.Errorf("value %d out of SMALLINT range", v)
		}
		binary.BigEndian.PutUint32(b8[:4], uint32(int32(v)))
		return b8[:4], nil
	case SQL_TYPE_LONG:
		v, err := toScaledInt64(e, meta.Scale)
		if err != nil {
			return nil, err
		}
		if v < math.MinInt32 || v > math.MaxInt32 {
			return nil, fmt.Errorf("value %d out of INTEGER range", v)
		}
		binary.BigEndian.PutUint32(b8[:4], uint32(int32(v)))
		return b8[:4], nil
	case SQL_TYPE_INT64, SQL_TYPE_QUAD:
		v, err := toScaledInt64(e, meta.Scale)
		if err != nil {
			return nil, err
		}
		binary.BigEndian.PutUint64(b8[:8], uint64(v))
		return b8[:8], nil
	case SQL_TYPE_FLOAT:
		f, err := toFloat64(e)
		if err != nil {
			return nil, err
		}
		binary.BigEndian.PutUint32(b8[:4], math.Float32bits(float32(f)))
		return b8[:4], nil
	case SQL_TYPE_DOUBLE:
		f, err := toFloat64(e)
		if err != nil {
			return nil, err
		}
		binary.BigEndian.PutUint64(b8[:8], math.Float64bits(f))
		return b8[:8], nil
	case SQL_TYPE_BOOLEAN:
		v, ok := e.(bool)
		if !ok {
			return nil, fmt.Errorf("cannot encode %T as BOOLEAN element", e)
		}
		if v {
			b8[0] = 1
		}
		return b8[:4], nil
	case SQL_TYPE_TEXT:
		s, err := toString(e)
		if err != nil {
			return nil, err
		}
		enc := []byte(p.encodeString(s))
		if len(enc) > int(meta.Length) {
			return nil, fmt.Errorf("value %q too long for CHAR(%d) array element", s, meta.Length)
		}
		out := make([]byte, roundUp4(int(meta.Length)))
		for i := range out {
			out[i] = ' '
		}
		copy(out, enc)
		return out, nil
	case SQL_TYPE_VARYING:
		s, err := toString(e)
		if err != nil {
			return nil, err
		}
		enc := []byte(p.encodeString(s))
		if len(enc) > int(meta.Length) {
			return nil, fmt.Errorf("value %q too long for VARCHAR(%d) array element", s, meta.Length)
		}
		out := make([]byte, 4+roundUp4(len(enc)))
		binary.BigEndian.PutUint32(out[:4], uint32(len(enc)))
		copy(out[4:], enc)
		return out, nil
	case SQL_TYPE_DATE:
		t, err := toTime(e)
		if err != nil {
			return nil, err
		}
		_, v := _dateToBlr(t)
		return v, nil
	case SQL_TYPE_TIME:
		t, err := toTime(e)
		if err != nil {
			return nil, err
		}
		_, v := _timeToBlrNoTZ(t)
		return v, nil
	case SQL_TYPE_TIMESTAMP:
		t, err := toTime(e)
		if err != nil {
			return nil, err
		}
		_, v := _timestampToBlrNoTZ(t)
		return v, nil
	case SQL_TYPE_TIME_TZ, SQL_TYPE_TIMESTAMP_TZ:
		// TIME/TIMESTAMP WITH TIME ZONE elements need their own wire form
		// (time/timestamp + 4-byte zone word, different from the row layout).
		return nil, fmt.Errorf("array elements of type %s are not supported yet", (&xSQLVAR{sqltype: meta.TypeID}).typename())
	default:
		return nil, fmt.Errorf("unsupported array element type %s", (&xSQLVAR{sqltype: meta.TypeID}).typename())
	}
}

// sliceWireElementSize returns the wire bytes of one element in the element-
// wise slice transfer (xdr_datum forms), and whether the element is varying
// (4-byte length prefix + data). Every element's wire form is 4-byte aligned:
// SHORT widens to 4, BOOLEAN is 1 value byte + 3 padding, TEXT is the full
// column-charset storage slot + alignment, VARCHAR is a 4-byte XDR length +
// data + alignment.
func sliceWireElementSize(meta *ArrayMeta) (int, bool) {
	switch meta.TypeID {
	case SQL_TYPE_INT64, SQL_TYPE_DOUBLE, SQL_TYPE_TIMESTAMP:
		return 8, false
	case SQL_TYPE_VARYING:
		return int(meta.FieldBytes), true
	case SQL_TYPE_TEXT:
		return roundUp4(int(meta.FieldBytes)), false
	default:
		return 4, false
	}
}

// toScaledInt64 converts an element value to its stored integer form for
// scaled types (NUMERIC/DECIMAL): a "10.50" string with scale -2 stores 1050.
func toScaledInt64(e any, scale int32) (int64, error) {
	if scale == 0 {
		return toInt64(e)
	}
	f, err := toFloat64(e)
	if err != nil {
		return 0, err
	}
	stored := f * math.Pow10(int(-scale))
	return int64(math.Round(stored)), nil
}

// putSliceWireElementSize returns the wire bytes of one encoded element on
// the put side (text elements are sized by the character Length there).
func putSliceWireElementSize(meta *ArrayMeta) int {
	if meta.TypeID == SQL_TYPE_TEXT {
		return roundUp4(int(meta.Length))
	}
	size, varying := sliceWireElementSize(meta)
	if varying {
		return 4 // zero length prefix
	}
	return size
}

// padSliceWire extends the element-wise wire buffer with zero elements so it
// holds the declared element count: the server reads appLen/stride elements
// regardless of how many were actually supplied, and missing ones must be
// present as zeros (for VARCHAR, a zero length prefix).
func padSliceWire(meta *ArrayMeta, data []byte, actual int) []byte {
	declared := allElementsCount(meta.Dimensions)
	missing := declared - actual
	if missing <= 0 {
		return data
	}
	per := putSliceWireElementSize(meta)
	out := make([]byte, len(data)+missing*per)
	copy(out, data)
	return out
}

// decodeSliceElements unpacks a fetched element-wise slice buffer into decoded
// element values. Short (partially stored) arrays yield fewer elements than
// the declared shape; fillArraySetter re-derives the actual dimensions.
func (p *wireProtocol) decodeSliceElements(meta *ArrayMeta, data []byte) ([]any, error) {
	if len(data) == 0 {
		return []any{}, nil
	}
	switch meta.TypeID {
	case SQL_TYPE_TIME_TZ, SQL_TYPE_TIMESTAMP_TZ:
		return nil, fmt.Errorf("array elements of type %s are not supported yet", (&xSQLVAR{sqltype: meta.TypeID}).typename())
	}
	count, err := sliceElementCount(meta, data)
	if err != nil {
		return nil, err
	}
	elemLen, varying := sliceWireElementSize(meta)

	elem := xSQLVAR{sqltype: meta.TypeID, sqlscale: int(meta.Scale), sqlsubtype: int(meta.SubType), sqllen: int(meta.Length)}
	out := make([]any, 0, count)
	pos := 0
	for i := 0; i < count; i++ {
		var raw []byte
		if varying {
			if len(data)-pos < 4 {
				return nil, fmt.Errorf("array body truncated at element %d", i)
			}
			l := int(binary.BigEndian.Uint32(data[pos : pos+4]))
			pos += 4
			if l < 0 || pos+l > len(data) {
				return nil, fmt.Errorf("array element %d length %d exceeds remaining %d bytes", i, l, len(data)-pos)
			}
			raw = data[pos : pos+l]
			pos += l
		} else {
			if pos+elemLen > len(data) {
				return nil, fmt.Errorf("array body truncated at element %d", i)
			}
			raw = data[pos : pos+elemLen]
			pos += elemLen
			if meta.TypeID == SQL_TYPE_BOOLEAN {
				raw = raw[:1] // value byte; drop the 3 padding bytes
			}
			// TEXT keeps the full storage slot: parseString + the caller's
			// space trimming (in xSQLVAR.value) handle the padding.
		}
		v, err := elem.value(raw, p.timezone, p.charset)
		if err != nil {
			return nil, fmt.Errorf("element %d: %w", i, err)
		}
		if meta.TypeID == SQL_TYPE_TEXT {
			// Never-written elements are zero-filled by the server; strip the
			// NUL padding after the scalar space-trim so they read as empty.
			if s, ok := v.(string); ok {
				v = strings.TrimRight(s, "\x00")
			}
		}
		out = append(out, v)
	}
	return out, nil
}

// sliceElementCount counts the elements actually present in an element-wise
// slice buffer.
func sliceElementCount(meta *ArrayMeta, data []byte) (int, error) {
	elemLen, varying := sliceWireElementSize(meta)
	if varying {
		count := 0
		for len(data) > 0 {
			if len(data) < 4 {
				return 0, fmt.Errorf("array varying element length prefix truncated")
			}
			l := int(binary.BigEndian.Uint32(data[:4])) + 4
			if l > len(data) {
				return 0, fmt.Errorf("array varying element length exceeds remaining bytes")
			}
			data = data[l:]
			count++
		}
		return count, nil
	}
	if elemLen < 1 {
		return 0, fmt.Errorf("array element length must not be less than 1")
	}
	return len(data) / elemLen, nil
}

// (batch_encode.go provides the shared toInt64/toFloat64/toString/toTime/toBool
// element value converters used above.)
