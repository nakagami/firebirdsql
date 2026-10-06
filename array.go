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

// Firebird array column support: reading (op_get_slice) and writing
// (op_put_slice) of array columns/parameters with typed Go values.
//
// database/sql surface:
//
//	rows.Scan(&firebirdsql.FlatArray[int64]{})            // 1-D flat target
//	rows.Scan(&firebirdsql.Array[int64]{})                // keeps dimensions
//	db.Exec("... VALUES (?)", firebirdsql.FlatArray[int64]{1, 2, 3})
//	db.Exec("... VALUES (?)", []int64{1, 2, 3})           // plain slices work too
//
// Supported element Go types: int16, int32, int64, float32, float64, string,
// time.Time. NULL elements are not representable in Firebird arrays and are
// rejected on encode. Scan targets must use the generic wrappers (database/sql
// has no conversion from array columns to plain slices).

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"reflect"
	"time"
)

// ArrayDimension describes one axis of an array: its lower bound (Firebird
// arrays need not start at 1) and the number of elements along the axis.
type ArrayDimension struct {
	LowerBound int
	Length     int
}

// ArrayMeta is the resolved metadata of an array column: element type and
// layout, plus the dimensions declared in the schema. The driver attaches it
// to every array-typed column and parameter at prepare time; ArrayValue.Meta
// carries it to scan targets.
type ArrayMeta struct {
	TableName  string
	FieldName  string
	TypeID     int // element type, one of the SQL_TYPE_* constants
	BlrTypeID  int // element type as stored in RDB$FIELD_TYPE (BLR code)
	Scale      int32
	SubType    int32  // character-set id for text elements, else field sub-type
	Length     uint32 // element length in characters for text (transfer size on put)
	FieldBytes uint32 // element storage length in column-charset bytes (transfer size on get)
	Dimensions []ArrayDimension

	// fieldSource is the underlying RDB$FIELDS domain name, the key into
	// RDB$FIELD_DIMENSIONS. Kept from metadata resolution; not meaningful
	// outside the driver.
	fieldSource string
}

// ArrayValue is the driver.Value bridge for array columns and parameters. On
// the read path the driver hands it to sql.Scanner targets as-is; on the write
// path a driver.Valuer may return it directly. Elements holds the decoded
// element values (int64, float64, string, []byte, time.Time or bool); a nil
// Elements slice represents a SQL NULL array. On write the column metadata
// resolved at prepare time is authoritative; value-supplied Meta is ignored.
type ArrayValue struct {
	Meta     *ArrayMeta
	Elements []any
}

// IsNull reports whether the value represents a SQL NULL array.
func (av ArrayValue) IsNull() bool {
	return av.Elements == nil
}

// FlatArray is a 1-D array value backed by a plain Go slice. As a scan target
// it receives the present elements in wire order (for a partially stored array
// the missing tail is dropped); as a parameter the slice length defines the
// filled prefix of the declared array.
type FlatArray[T any] []T

// Array is an array value that keeps its dimensions, so multi-dimensional
// arrays survive a round trip.
type Array[T any] struct {
	Elements []T
	Dims     []ArrayDimension
	Valid    bool
}

// Value implements driver.Valuer: the flat elements as an ArrayValue, or nil
// for a NULL array.
func (a FlatArray[T]) Value() (driver.Value, error) {
	if a == nil {
		return nil, nil
	}
	return ArrayValue{Elements: elementsToAny(a)}, nil
}

// Value implements driver.Valuer: the dimensioned elements as an ArrayValue,
// or nil for a NULL array.
func (a Array[T]) Value() (driver.Value, error) {
	if !a.Valid {
		return nil, nil
	}
	return ArrayValue{Elements: elementsToAny(a.Elements)}, nil
}

func elementsToAny[T any](elements []T) []any {
	out := make([]any, len(elements))
	for i, e := range elements {
		out[i] = any(e)
	}
	return out
}

// Scan implements sql.Scanner. src must carry an ArrayValue from this driver
// (or be nil for SQL NULL).
func (a *FlatArray[T]) Scan(src any) error {
	av, ok := src.(ArrayValue)
	if !ok {
		if src == nil {
			*a = nil
			return nil
		}
		return fmt.Errorf("firebirdsql: cannot scan array column from %T", src)
	}
	if av.IsNull() {
		*a = nil
		return nil
	}
	if av.Meta == nil {
		return fmt.Errorf("firebirdsql: cannot scan array column: metadata unavailable")
	}
	return fillArraySetter((*flatArraySetter[T])(a), av.Meta, av.Elements)
}

// Scan implements sql.Scanner. src must carry an ArrayValue from this driver
// (or be nil for SQL NULL).
func (a *Array[T]) Scan(src any) error {
	av, ok := src.(ArrayValue)
	if !ok {
		if src == nil {
			*a = Array[T]{}
			return nil
		}
		return fmt.Errorf("firebirdsql: cannot scan array column from %T", src)
	}
	if av.IsNull() {
		*a = Array[T]{}
		return nil
	}
	if av.Meta == nil {
		return fmt.Errorf("firebirdsql: cannot scan array column: metadata unavailable")
	}
	if err := fillArraySetter((*genericArraySetter[T])(a), av.Meta, av.Elements); err != nil {
		return err
	}
	a.Valid = true
	return nil
}

// arrayGetter abstracts a Go value as an array source: the driver asks for the
// value's own dimensions and then reads elements by linear index.
type arrayGetter interface {
	Index(i int) any
	Dimensions(lowers []int) ([]ArrayDimension, error)
}

// arraySetter abstracts a scan target: SetDimensions sizes the value, then
// ScanIndex yields per-element scan targets.
type arraySetter interface {
	SetDimensions(dimensions []ArrayDimension) error
	ScanIndex(i int) any
}

func lowersOf(dims []ArrayDimension) []int {
	lowers := make([]int, len(dims))
	for i, d := range dims {
		lowers[i] = d.LowerBound
	}
	return lowers
}

// flatArraySetter adapts FlatArray[T] to the arraySetter/arrayGetter shape.
type flatArraySetter[T any] []T

func (a flatArraySetter[T]) Index(i int) any { return a[i] }

func (a flatArraySetter[T]) Dimensions(lowers []int) ([]ArrayDimension, error) {
	if a == nil {
		return nil, nil
	}
	lower := 1
	if len(lowers) > 0 {
		lower = lowers[0]
	}
	return []ArrayDimension{{LowerBound: lower, Length: len(a)}}, nil
}

func (a *flatArraySetter[T]) SetDimensions(dimensions []ArrayDimension) error {
	if dimensions == nil {
		*a = nil
		return nil
	}
	*a = make(flatArraySetter[T], allElementsCount(dimensions))
	return nil
}

func (a *flatArraySetter[T]) ScanIndex(i int) any { return &(*a)[i] }

// genericArraySetter adapts Array[T] to the arraySetter/arrayGetter shape.
type genericArraySetter[T any] Array[T]

func (a genericArraySetter[T]) Index(i int) any { return a.Elements[i] }

func (a genericArraySetter[T]) Dimensions(lowers []int) ([]ArrayDimension, error) {
	return a.Dims, nil
}

func (a *genericArraySetter[T]) SetDimensions(dimensions []ArrayDimension) error {
	if dimensions == nil {
		*a = genericArraySetter[T]{}
		return nil
	}
	*a = genericArraySetter[T](Array[T]{Elements: make([]T, allElementsCount(dimensions)), Dims: dimensions})
	return nil
}

func (a *genericArraySetter[T]) ScanIndex(i int) any { return &a.Elements[i] }

// anySliceArrayGetter exposes an arbitrary single-dimensional slice or array
// (element types beyond the dedicated FlatArray conversions) as an arrayGetter.
type anySliceArrayGetter struct{ slice reflect.Value }

func (a anySliceArrayGetter) Index(i int) any { return a.slice.Index(i).Interface() }

func (a anySliceArrayGetter) Dimensions(lowers []int) ([]ArrayDimension, error) {
	if a.slice.IsNil() {
		return nil, nil
	}
	lower := 1
	if len(lowers) > 0 {
		lower = lowers[0]
	}
	return []ArrayDimension{{LowerBound: lower, Length: a.slice.Len()}}, nil
}

// anyMultiDimSliceGetter exposes a rectangular multi-dimensional slice as an
// arrayGetter. Dimensions() reports one axis per nesting level; Index(i)
// addresses elements by linear index within that shape.
type anyMultiDimSliceGetter struct {
	slice      reflect.Value
	dimensions []ArrayDimension
}

func (a *anyMultiDimSliceGetter) Index(i int) any {
	if len(a.dimensions) <= 1 {
		return a.slice.Index(i).Interface()
	}
	indexes := make([]int, len(a.dimensions))
	for j := len(a.dimensions) - 1; j >= 0; j-- {
		indexes[j] = i % a.dimensions[j].Length
		i /= a.dimensions[j].Length
	}
	v := a.slice
	for _, si := range indexes {
		v = v.Index(si)
	}
	return v.Interface()
}

func (a *anyMultiDimSliceGetter) Dimensions(lowers []int) ([]ArrayDimension, error) {
	if a.slice.IsNil() {
		return nil, nil
	}
	// lowers may be empty when the value is converted before the column
	// metadata is known (CheckNamedValue path); default to 1-based axes then.
	lowerAt := func(depth int) int {
		if depth < len(lowers) {
			return lowers[depth]
		}
		return 1
	}
	a.dimensions = a.dimensions[:0]
	s := a.slice
	depth := 0
	if s.Len() > 0 {
		for {
			if len(lowers) > 0 && depth == len(lowers) {
				return nil, fmt.Errorf("firebirdsql: array argument has more dimensions than the column")
			}
			a.dimensions = append(a.dimensions, ArrayDimension{LowerBound: lowerAt(depth), Length: s.Len()})
			if s.Len() == 0 {
				break
			}
			s = s.Index(0)
			depth++
			if s.Kind() != reflect.Slice {
				break
			}
		}
	} else {
		t := s.Type()
		for t.Kind() == reflect.Slice {
			if len(lowers) > 0 && depth == len(lowers) {
				return nil, fmt.Errorf("firebirdsql: array argument has more dimensions than the column")
			}
			a.dimensions = append(a.dimensions, ArrayDimension{LowerBound: lowerAt(depth), Length: 0})
			t = t.Elem()
			depth++
		}
	}
	return a.dimensions, nil
}

// allElementsCount returns the number of elements a shape holds, or 0 for
// empty/invalid shapes and on int overflow.
func allElementsCount(dimensions []ArrayDimension) int {
	if len(dimensions) == 0 {
		return 0
	}
	const maxInt = int(^uint(0) >> 1)
	count := 1
	for _, d := range dimensions {
		if d.Length <= 0 {
			return 0
		}
		if count > maxInt/d.Length {
			return 0
		}
		count *= d.Length
	}
	return count
}

// getArrayEncodeType wraps a Go value as an arrayGetter. Plain 1-D slices of
// the supported element types convert to the FlatArray adapter automatically;
// other slices and arrays go through the reflect adapters; ragged nested
// slices are rejected.
func getArrayEncodeType(value any) (arrayGetter, error) {
	v := reflect.ValueOf(value)
	if !v.IsValid() {
		return nil, fmt.Errorf("firebirdsql: unsupported array argument type %T", value)
	}
	if v.Kind() == reflect.Ptr && v.IsNil() {
		return nil, fmt.Errorf("firebirdsql: cannot encode array from nil %T", value)
	}

	switch target := value.(type) {
	case []int16:
		return (*flatArraySetter[int16])(&target), nil
	case []int32:
		return (*flatArraySetter[int32])(&target), nil
	case []int64:
		return (*flatArraySetter[int64])(&target), nil
	case []float32:
		return (*flatArraySetter[float32])(&target), nil
	case []float64:
		return (*flatArraySetter[float64])(&target), nil
	case []string:
		return (*flatArraySetter[string])(&target), nil
	case []time.Time:
		return (*flatArraySetter[time.Time])(&target), nil
	}

	switch v.Kind() {
	case reflect.Slice:
		if v.Type().Elem().Kind() == reflect.Slice {
			if isRagged(v) {
				return nil, fmt.Errorf("firebirdsql: cannot encode a ragged multi-dimensional array")
			}
			return &anyMultiDimSliceGetter{slice: v}, nil
		}
		return anySliceArrayGetter{slice: v}, nil
	case reflect.Array:
		return anySliceArrayGetter{slice: v}, nil
	}
	return nil, fmt.Errorf("firebirdsql: unsupported array argument type %T", value)
}

func isRagged(slice reflect.Value) bool {
	if slice.Type().Elem().Kind() != reflect.Slice {
		return false
	}
	var lens []int
	return isSliceRagged(slice, 0, &lens)
}

func isSliceRagged(slice reflect.Value, lvl int, lens *[]int) bool {
	if slice.Type().Elem().Kind() != reflect.Slice {
		return false
	}
	innerLen := 0
	for i := 0; i < slice.Len(); i++ {
		if i == 0 {
			innerLen = slice.Index(i).Len()
			if len(*lens)-1 >= lvl {
				if (*lens)[lvl] != innerLen {
					return true
				}
			} else {
				*lens = append(*lens, innerLen)
			}
		} else if slice.Index(i).Len() != innerLen {
			return true
		}
		if isSliceRagged(slice.Index(i), lvl+1, lens) {
			return true
		}
	}
	return false
}

// fillArraySetter sizes setter to the actual dimensions of the fetched array
// and fills the present elements in wire order. Partially stored 1-D arrays
// (fewer elements than the declared shape) shrink the trailing elements;
// truncated multi-dimensional arrays are rejected — the server always
// materializes full arrays, so this indicates a protocol anomaly.
func fillArraySetter(setter arraySetter, meta *ArrayMeta, elements []any) error {
	if elements == nil {
		return setter.SetDimensions(nil)
	}
	if len(elements) == 0 {
		return setter.SetDimensions([]ArrayDimension{})
	}
	if declared := allElementsCount(meta.Dimensions); len(meta.Dimensions) > 1 && len(elements) != declared {
		return fmt.Errorf("firebirdsql: truncated multi-dimensional array: %d of %d elements", len(elements), declared)
	}
	actualDims, err := shrinkDimensions(meta.Dimensions, len(elements))
	if err != nil {
		return err
	}
	if err := setter.SetDimensions(actualDims); err != nil {
		return err
	}
	idx := 0
	for _, r := range presentIndexRanges(meta.Dimensions, actualDims) {
		for i := r.start; i <= r.end && idx < len(elements); i++ {
			if err := scanArrayElement(setter.ScanIndex(idx), elements[i]); err != nil {
				return fmt.Errorf("firebirdsql: array element %d: %w", idx, err)
			}
			idx++
		}
	}
	return nil
}

// elementsFromGetter extracts the flat element values and the value's own
// dimensions, rejecting NULL elements and arrays longer than the column. The
// declared shape comes from the column metadata; it bounds the argument but is
// not required to be filled completely (short values pad the tail).
func elementsFromGetter(getter arrayGetter, declared []ArrayDimension) ([]any, []ArrayDimension, error) {
	dims, err := getter.Dimensions(lowersOf(declared))
	if err != nil {
		return nil, nil, err
	}
	if dims == nil {
		return nil, nil, nil // SQL NULL array
	}
	argCount := allElementsCount(dims)
	// The declared shape may be unknown at value-conversion time (plain slice
	// args pass through CheckNamedValue before the column is known); the bound
	// is enforced later in paramsToBlr against the resolved metadata.
	if len(declared) > 0 {
		if fieldCount := allElementsCount(declared); fieldCount < argCount {
			return nil, nil, fmt.Errorf("firebirdsql: array argument has %d elements but the column holds %d", argCount, fieldCount)
		}
	}
	elements := make([]any, argCount)
	for i := 0; i < argCount; i++ {
		e := getter.Index(i)
		if isNilValue(e) {
			return nil, nil, fmt.Errorf("firebirdsql: cannot encode NULL array element at index %d", i)
		}
		elements[i] = e
	}
	return elements, dims, nil
}

func isNilValue(v any) bool {
	if v == nil {
		return true
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Ptr, reflect.Map, reflect.Slice, reflect.Interface, reflect.Func, reflect.Chan:
		return rv.IsNil()
	}
	return false
}

// scanArrayElement converts one decoded element value into a typed scan
// target. Sources are the driver.Value shapes produced by decodeSliceElements.
func scanArrayElement(dst any, src any) error {
	switch d := dst.(type) {
	case *int16:
		v, err := toInt64(src)
		if err != nil {
			return err
		}
		if v < -32768 || v > 32767 {
			return fmt.Errorf("value %d out of int16 range", v)
		}
		*d = int16(v)
	case *int32:
		v, err := toInt64(src)
		if err != nil {
			return err
		}
		if v < -2147483648 || v > 2147483647 {
			return fmt.Errorf("value %d out of int32 range", v)
		}
		*d = int32(v)
	case *int64:
		v, err := toInt64(src)
		if err != nil {
			return err
		}
		*d = v
	case *float32:
		v, err := toFloat64(src)
		if err != nil {
			return err
		}
		*d = float32(v)
	case *float64:
		v, err := toFloat64(src)
		if err != nil {
			return err
		}
		*d = v
	case *string:
		s, err := toString(src)
		if err != nil {
			return err
		}
		*d = s
	case *time.Time:
		t, ok := src.(time.Time)
		if !ok {
			return fmt.Errorf("cannot scan %T into time.Time", src)
		}
		*d = t
	case *bool:
		b, ok := src.(bool)
		if !ok {
			return fmt.Errorf("cannot scan %T into bool", src)
		}
		*d = b
	default:
		return fmt.Errorf("unsupported element target %T", dst)
	}
	return nil
}

// shrinkDimensions computes the dimensions of an actually-stored (short)
// array: the declared shape minus the missing tail elements, distributed over
// the axes trailing-element-first. Port of fbx getActualArrayDimensions.
func shrinkDimensions(declared []ArrayDimension, actual int) ([]ArrayDimension, error) {
	missing := allElementsCount(declared) - actual
	if missing < 0 {
		return nil, fmt.Errorf("firebirdsql: array holds %d elements but the column declares %d", actual, allElementsCount(declared))
	}
	out := make([]ArrayDimension, len(declared))
	for i := range declared {
		tail := elementsInTrailingAxes(declared[i:])
		if tail <= 0 {
			return nil, fmt.Errorf("firebirdsql: array dimension %d is not positive", i)
		}
		out[i] = ArrayDimension{
			LowerBound: declared[i].LowerBound,
			Length:     declared[i].Length - missing/tail,
		}
		missing %= tail
	}
	return out, nil
}

// elementsInTrailingAxes returns the element count of all axes after the
// first one in dims (the divisor for per-axis shrink math).
func elementsInTrailingAxes(dims []ArrayDimension) int {
	total := 1
	for i := 1; i < len(dims); i++ {
		total *= dims[i].Length
	}
	return total
}

type indexRange struct {
	start, end int
}

// presentIndexRanges maps the linear indices of the actually-present elements
// within the declared element order: a shrunk last axis leaves padding gaps
// between the present runs. Port of fbx getActualSlice.
func presentIndexRanges(declared, actual []ArrayDimension) []indexRange {
	var out []indexRange
	walkPresentRanges(&out, declared, actual, 0)
	return out
}

func walkPresentRanges(out *[]indexRange, declared, actual []ArrayDimension, pos int) int {
	switch {
	case len(declared) > 1:
		for i := 0; i < actual[0].Length; i++ {
			pos = walkPresentRanges(out, declared[1:], actual[1:], pos)
		}
		pos += elementsInTrailingAxes(declared) * (declared[0].Length - actual[0].Length)
	case len(declared) == 1:
		*out = append(*out, indexRange{start: pos, end: pos + actual[0].Length - 1})
		pos += declared[0].Length
	}
	return pos
}

// Compile-time interface checks.
var (
	_ sql.Scanner   = (*FlatArray[int64])(nil)
	_ driver.Valuer = FlatArray[int64]{}
	_ sql.Scanner   = (*Array[int64])(nil)
	_ driver.Valuer = Array[int64]{}
	_ arrayGetter   = flatArraySetter[int64]{}
	_ arrayGetter   = &anySliceArrayGetter{}
	_ arrayGetter   = &anyMultiDimSliceGetter{}
	_ arraySetter   = (*flatArraySetter[int64])(nil)
	_ arraySetter   = (*genericArraySetter[int64])(nil)
)
