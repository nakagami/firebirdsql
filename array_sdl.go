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
	"encoding/binary"
	"fmt"
)

// arrayTypeFromBlr maps the element BLR type code stored in RDB$FIELD_TYPE to
// the DSQL SQL_TYPE_* value used by the rest of the driver.
func arrayTypeFromBlr(blr int64) (int, error) {
	switch blr {
	case blr_type_varying, blr_type_varying2:
		return SQL_TYPE_VARYING, nil
	case blr_type_text, blr_type_text2, blr_type_cstring, blr_type_cstring2:
		return SQL_TYPE_TEXT, nil
	case 7: // blr_short
		return SQL_TYPE_SHORT, nil
	case 8: // blr_long
		return SQL_TYPE_LONG, nil
	case 9: // blr_quad
		return SQL_TYPE_QUAD, nil
	case blr_type_int64:
		return SQL_TYPE_INT64, nil
	case blr_type_double:
		return SQL_TYPE_DOUBLE, nil
	case 10: // blr_float
		return SQL_TYPE_FLOAT, nil
	case 11: // blr_d_float
		return SQL_TYPE_D_FLOAT, nil
	case 12: // blr_sql_date
		return SQL_TYPE_DATE, nil
	case 13: // blr_sql_time
		return SQL_TYPE_TIME, nil
	case blr_type_time_tz:
		return SQL_TYPE_TIME_TZ, nil
	case blr_type_timestamp:
		return SQL_TYPE_TIMESTAMP, nil
	case blr_type_ts_tz:
		return SQL_TYPE_TIMESTAMP_TZ, nil
	case blr_type_bool:
		return SQL_TYPE_BOOLEAN, nil
	case blr_type_dec64:
		return SQL_TYPE_DEC64, nil
	case blr_type_dec128:
		return SQL_TYPE_DEC128, nil
	case blr_type_int128:
		return SQL_TYPE_INT128, nil
	default:
		return 0, fmt.Errorf("firebirdsql: unknown array element blr type %d", blr)
	}
}

// charsetIDMaxBytes maps Firebird character-set ids to the maximum bytes per
// character of that set. Ids not listed here (collation-specific / multi-byte
// sets beyond the common ones) default to 1, which only means the element
// length rescaling for foreign-charset connections is skipped.
var charsetIDMaxBytes = map[int64]int{
	0: 1, // NONE
	1: 1, // OCTETS
	2: 1, // ASCII
	3: 3, // UNICODE_FSS
	4: 4, // UTF8
	5: 2, // SJIS_0208
	6: 2, // EUCJ_0208
}

// arrayElementStride returns the element size of the array's application
// buffer layout on PUT (the dsc_length the server derives from the SDL, whose
// text length word carries characters): SHORT takes 2 bytes, BOOLEAN 1,
// VARCHAR a 2-byte prefix + characters. The element COUNT on the wire is
// p_slc_length divided by this stride.
func arrayElementStride(meta *ArrayMeta) int {
	switch meta.TypeID {
	case SQL_TYPE_SHORT:
		return 2
	case SQL_TYPE_LONG, SQL_TYPE_FLOAT, SQL_TYPE_DATE, SQL_TYPE_TIME:
		return 4
	case SQL_TYPE_INT64, SQL_TYPE_DOUBLE, SQL_TYPE_TIMESTAMP:
		return 8
	case SQL_TYPE_BOOLEAN:
		return 1
	case SQL_TYPE_TEXT:
		return int(meta.Length)
	case SQL_TYPE_VARYING:
		return 2 + int(meta.Length)
	default:
		return int(meta.Length)
	}
}

// arrayGetElementStride is like arrayElementStride for the GET direction,
// where the SDL length word carries the column-charset byte size
// (ArrayMeta.FieldBytes) for text types — the geometry the server uses when
// materializing stored elements for transfer.
func arrayGetElementStride(meta *ArrayMeta) int {
	switch meta.TypeID {
	case SQL_TYPE_TEXT:
		return int(meta.FieldBytes)
	case SQL_TYPE_VARYING:
		return 2 + int(meta.FieldBytes)
	default:
		return arrayElementStride(meta)
	}
}

// arraySliceAppLength returns the application-buffer byte length of a whole
// slice on PUT: elements × stride. Both p_slc_length fields carry this value;
// the slice data itself is transferred element-wise in wire encoding (see
// encodeSliceElements) and is a multiple of 4 bytes.
func arraySliceAppLength(meta *ArrayMeta, dims []ArrayDimension) int {
	elements := allElementsCount(dims)
	if elements == 0 {
		return 0
	}
	return elements * arrayElementStride(meta)
}

// arraySliceGetLength is the PUT-equivalent capacity declared on GET.
func arraySliceGetLength(meta *ArrayMeta, dims []ArrayDimension) int {
	elements := allElementsCount(dims)
	if elements == 0 {
		return 0
	}
	return elements * arrayGetElementStride(meta)
}

func roundUp4(n int) int {
	if r := n % 4; r != 0 {
		return n + 4 - r
	}
	return n
}

// generateSDL builds the slice descriptor addressing meta's column with the
// given dimensions. Port of fbx fbconn/array.go generateSDL: element BLR header
// (with scale / length payloads), relation + field name, per-dimension loops
// (sdl_do1 for 1-based lower bounds, sdl_do2 + explicit bound otherwise) and
// the sdl_element/scalar/variable footer.
// Grammar verified against the Firebird server's SDL parser (src/common/sdl.cpp).
//
// For text element types the SDL length word differs by direction: on put the
// server validates element content against the character length; on get it
// materializes stored elements into a buffer sized by the column-charset byte
// length (verified empirically against Firebird 5).
func generateSDL(meta *ArrayMeta, dims []ArrayDimension, forGet bool) []byte {
	sdl := make([]byte, 0, 64)
	sdl = append(sdl, isc_sdl_version1, isc_sdl_struct, 1, byte(meta.BlrTypeID))

	switch meta.BlrTypeID {
	case 7, 8, 9, blr_type_int64: // blr_short, blr_long, blr_quad, blr_int64
		sdl = append(sdl, byte(meta.Scale))
	case blr_type_text, blr_type_text2, blr_type_cstring, blr_type_cstring2, blr_type_varying:
		textLen := meta.Length
		if forGet {
			textLen = meta.FieldBytes
		}
		var lenBytes [2]byte
		binary.LittleEndian.PutUint16(lenBytes[:], uint16(textLen))
		sdl = append(sdl, lenBytes[:]...)
	}

	sdl = append(sdl, isc_sdl_relation, byte(len(meta.TableName)))
	sdl = append(sdl, meta.TableName...)
	sdl = append(sdl, isc_sdl_field, byte(len(meta.FieldName)))
	sdl = append(sdl, meta.FieldName...)

	for i, d := range dims {
		if d.LowerBound == 1 {
			sdl = append(sdl, isc_sdl_do1, byte(i))
		} else {
			sdl = append(sdl, isc_sdl_do2, byte(i))
			sdl = appendDimensionBound(sdl, d.LowerBound)
		}
		sdl = appendDimensionBound(sdl, d.LowerBound+d.Length-1)
	}

	sdl = append(sdl, isc_sdl_element, 1, isc_sdl_scalar, 0, byte(len(dims)))
	for i := range dims {
		sdl = append(sdl, isc_sdl_variable, byte(i))
	}
	return append(sdl, isc_sdl_eoc)
}

// appendDimensionBound encodes a dimension bound in the smallest integer form
// the SDL grammar accepts (tiny / short / long, little-endian payloads).
func appendDimensionBound(sdl []byte, v int) []byte {
	switch {
	case -128 <= v && v <= 127:
		return append(sdl, isc_sdl_tiny_integer, byte(v))
	case -32768 <= v && v <= 32767:
		var b [2]byte
		binary.LittleEndian.PutUint16(b[:], uint16(v))
		return append(append(sdl, isc_sdl_short_integer), b[:]...)
	default:
		var b [4]byte
		binary.LittleEndian.PutUint32(b[:], uint32(v))
		return append(append(sdl, isc_sdl_long_integer), b[:]...)
	}
}
