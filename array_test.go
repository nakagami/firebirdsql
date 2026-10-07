package firebirdsql

import (
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Server-free tests for the array support: SDL generation, slice length
// math, element codec layout, and the dimension shrink logic. The wire-level
// behavior is covered by array_live_test.go.

func TestGenerateSDLInt1D(t *testing.T) {
	meta := &ArrayMeta{
		TableName: "T", FieldName: "INTS",
		TypeID: SQL_TYPE_LONG, BlrTypeID: 8, Scale: 0, Length: 4, FieldBytes: 4,
		Dimensions: []ArrayDimension{{LowerBound: 1, Length: 5}},
	}
	sdl := generateSDL(meta, meta.Dimensions, false)
	// version1, struct, 1, blr_long, scale 0, relation "T", field "INTS",
	// do1 dim0 (lower=1), tiny bound 5, element, 1, scalar, 0, 1 dim, variable 0, eoc
	want := []byte{
		1, 6, 1, 8, 0,
		2, 1, 'T',
		4, 4, 'I', 'N', 'T', 'S',
		35, 0, 9, 5,
		36, 1, 8, 0, 1, 7, 0,
		255,
	}
	require.Equal(t, want, sdl)
}

func TestGenerateSDLLowerBoundNonOne(t *testing.T) {
	meta := &ArrayMeta{
		TableName: "T", FieldName: "G",
		TypeID: SQL_TYPE_INT64, BlrTypeID: 16, Scale: 0, Length: 8, FieldBytes: 8,
		Dimensions: []ArrayDimension{{LowerBound: 0, Length: 3}},
	}
	sdl := generateSDL(meta, meta.Dimensions, false)
	// lower bound 0 ⇒ sdl_do2 + tiny bound 0; upper bound 2 ⇒ tiny
	want := []byte{
		1, 6, 1, 16, 0,
		2, 1, 'T',
		4, 1, 'G',
		34, 0, 9, 0, 9, 2,
		36, 1, 8, 0, 1, 7, 0,
		255,
	}
	require.Equal(t, want, sdl)
}

func TestGenerateSDLVaryingLengthWord(t *testing.T) {
	meta := &ArrayMeta{
		TableName: "T", FieldName: "S",
		TypeID: SQL_TYPE_VARYING, BlrTypeID: 37, Length: 10, FieldBytes: 40,
		Dimensions: []ArrayDimension{{LowerBound: 1, Length: 4}},
	}
	putSDL := generateSDL(meta, meta.Dimensions, false)
	getSDL := generateSDL(meta, meta.Dimensions, true)
	// put carries the character length (10), get the column byte length (40);
	// both little-endian right after the blr_varying byte
	require.EqualValues(t, 10, int(putSDL[4])|int(putSDL[5])<<8)
	require.EqualValues(t, 40, int(getSDL[4])|int(getSDL[5])<<8)
}

func TestArrayElementStride(t *testing.T) {
	cases := []struct {
		typeID  int
		length  uint32
		put     int
		get     int
		wire    int
		varying bool
	}{
		{SQL_TYPE_SHORT, 2, 2, 2, 4, false},
		{SQL_TYPE_LONG, 4, 4, 4, 4, false},
		{SQL_TYPE_INT64, 8, 8, 8, 8, false},
		{SQL_TYPE_FLOAT, 4, 4, 4, 4, false},
		{SQL_TYPE_DOUBLE, 8, 8, 8, 8, false},
		{SQL_TYPE_BOOLEAN, 1, 1, 1, 4, false},
		{SQL_TYPE_DATE, 4, 4, 4, 4, false},
		{SQL_TYPE_TIME, 4, 4, 4, 4, false},
		{SQL_TYPE_TIMESTAMP, 8, 8, 8, 8, false},
		{SQL_TYPE_TEXT, 4, 4, 4, 4, false},
		{SQL_TYPE_VARYING, 10, 12, 42, 40, true},
		{SQL_TYPE_DEC64, 8, 8, 8, 8, false},
		{SQL_TYPE_DEC128, 16, 16, 16, 16, false},
		{SQL_TYPE_INT128, 16, 16, 16, 16, false},
		{SQL_TYPE_DEC_FIXED, 16, 16, 16, 16, false},
	}
	for _, c := range cases {
		meta := &ArrayMeta{TypeID: c.typeID, Length: c.length, FieldBytes: c.length}
		if c.typeID == SQL_TYPE_VARYING {
			meta.FieldBytes = 40
		}
		require.Equal(t, c.put, arrayElementStride(meta), "put stride type %d", c.typeID)
		require.Equal(t, c.get, arrayGetElementStride(meta), "get stride type %d", c.typeID)
		size, varying := sliceWireElementSize(meta)
		if c.typeID == SQL_TYPE_TEXT {
			size = roundUp4(int(meta.FieldBytes))
		}
		require.Equal(t, c.wire, size, "wire size type %d", c.typeID)
		require.Equal(t, c.varying, varying, "varying flag type %d", c.typeID)
	}
}

func TestArraySliceAppLength(t *testing.T) {
	meta := &ArrayMeta{TypeID: SQL_TYPE_LONG, Length: 4, FieldBytes: 4}
	dims := []ArrayDimension{{LowerBound: 1, Length: 5}}
	require.Equal(t, 20, arraySliceAppLength(meta, dims))
	require.Equal(t, 20, arraySliceGetLength(meta, dims))

	vmeta := &ArrayMeta{TypeID: SQL_TYPE_VARYING, Length: 10, FieldBytes: 40}
	require.Equal(t, 60, arraySliceAppLength(vmeta, dims))  // 5 × (2 + 10)
	require.Equal(t, 210, arraySliceGetLength(vmeta, dims)) // 5 × (2 + 40)
}

func TestEncodeDecodeSliceElementsRoundTrip(t *testing.T) {
	p := &wireProtocol{charset: "UTF8", timezone: "UTC"}
	meta := &ArrayMeta{TypeID: SQL_TYPE_INT64, BlrTypeID: 16, Length: 8, FieldBytes: 8}
	elements := []any{int64(1), int64(-2), int64(900000000000)}
	data, err := p.encodeSliceElements(meta, elements)
	require.NoError(t, err)
	require.Len(t, data, 24)
	decoded, err := p.decodeSliceElements(meta, data)
	require.NoError(t, err)
	require.Equal(t, []any{int64(1), int64(-2), int64(900000000000)}, decoded)
}

func TestEncodeSliceVaryingLayout(t *testing.T) {
	p := &wireProtocol{charset: "UTF8"}
	meta := &ArrayMeta{TypeID: SQL_TYPE_VARYING, BlrTypeID: 37, Length: 10, FieldBytes: 40}
	data, err := p.encodeSliceElements(meta, []any{"ab", ""})
	require.NoError(t, err)
	// each element: 4-byte BE length + data padded to 4
	require.Equal(t, []byte{0, 0, 0, 2, 'a', 'b', 0, 0, 0, 0, 0, 0}, data)
}

func TestEncodeSliceTextLayout(t *testing.T) {
	p := &wireProtocol{charset: "UTF8"}
	meta := &ArrayMeta{TypeID: SQL_TYPE_TEXT, BlrTypeID: 14, Length: 6, FieldBytes: 6}
	data, err := p.encodeSliceElements(meta, []any{"ab"})
	require.NoError(t, err)
	// space padded to Length (6) then aligned to 4 ⇒ 8 bytes
	require.Equal(t, []byte{'a', 'b', ' ', ' ', ' ', ' ', ' ', ' '}, data)
}

func TestEncodeSliceBooleanLayout(t *testing.T) {
	p := &wireProtocol{charset: "UTF8"}
	meta := &ArrayMeta{TypeID: SQL_TYPE_BOOLEAN, BlrTypeID: 23, Length: 1, FieldBytes: 1}
	data, err := p.encodeSliceElements(meta, []any{true, false})
	require.NoError(t, err)
	require.Equal(t, []byte{1, 0, 0, 0, 0, 0, 0, 0}, data)
}

func TestEncodeSliceTooLongElement(t *testing.T) {
	p := &wireProtocol{charset: "UTF8"}
	meta := &ArrayMeta{TypeID: SQL_TYPE_VARYING, BlrTypeID: 37, Length: 4, FieldBytes: 4,
		Dimensions: []ArrayDimension{{LowerBound: 1, Length: 2}}}
	_, err := p.encodeSliceElements(meta, []any{"toolong"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "too long")
}

func TestEncodeScaledNumeric(t *testing.T) {
	p := &wireProtocol{charset: "UTF8"}
	meta := &ArrayMeta{TypeID: SQL_TYPE_INT64, BlrTypeID: 16, Scale: -2, Length: 8, FieldBytes: 8,
		Dimensions: []ArrayDimension{{LowerBound: 1, Length: 2}}}
	data, err := p.encodeSliceElements(meta, []any{"10.50", 2})
	require.NoError(t, err)
	decoded, err := p.decodeSliceElements(meta, data)
	require.NoError(t, err)
	require.Equal(t, []any{"10.50", "2.00"}, decoded) // scaled decode via formatDecimalGDA
}

func TestShrinkDimensions(t *testing.T) {
	declared := []ArrayDimension{{LowerBound: 1, Length: 5}}
	out, err := shrinkDimensions(declared, 3)
	require.NoError(t, err)
	require.Equal(t, []ArrayDimension{{LowerBound: 1, Length: 3}}, out)

	declared = []ArrayDimension{{LowerBound: 0, Length: 3}, {LowerBound: 0, Length: 3}}
	out, err = shrinkDimensions(declared, 9)
	require.NoError(t, err)
	require.Equal(t, declared, out)

	_, err = shrinkDimensions(declared, 10)
	require.Error(t, err)
}

func TestPresentIndexRanges(t *testing.T) {
	declared := []ArrayDimension{{LowerBound: 0, Length: 3}}
	actual := []ArrayDimension{{LowerBound: 0, Length: 2}}
	require.Equal(t, []indexRange{{0, 1}}, presentIndexRanges(declared, actual))
}

func TestFlatArrayValuerScanner(t *testing.T) {
	meta := &ArrayMeta{
		TypeID:     SQL_TYPE_LONG,
		Dimensions: []ArrayDimension{{LowerBound: 1, Length: 3}},
	}
	v, err := FlatArray[int64]{1, 2, 3}.Value()
	require.NoError(t, err)
	av, ok := v.(ArrayValue)
	require.True(t, ok)
	require.Equal(t, []any{int64(1), int64(2), int64(3)}, av.Elements)
	av.Meta = &ArrayMeta{TypeID: SQL_TYPE_LONG, Dimensions: []ArrayDimension{{LowerBound: 1, Length: 3}}}

	var out FlatArray[int64]
	require.NoError(t, out.Scan(av))
	require.Equal(t, FlatArray[int64]{1, 2, 3}, out)

	require.NoError(t, out.Scan(nil))
	require.Nil(t, out)

	var empty FlatArray[int64]
	require.NoError(t, empty.Scan(ArrayValue{Meta: meta, Elements: []any{}}))
	require.Empty(t, empty)
}

func TestArrayGenericScanner(t *testing.T) {
	meta := &ArrayMeta{
		TypeID:     SQL_TYPE_LONG,
		Dimensions: []ArrayDimension{{LowerBound: 0, Length: 2}, {LowerBound: 0, Length: 2}},
	}
	// 2×2 array, fully stored
	elements := []any{int64(1), int64(2), int64(3), int64(4)}
	var out Array[int64]
	require.NoError(t, out.Scan(ArrayValue{Meta: meta, Elements: elements}))
	require.True(t, out.Valid)
	require.Equal(t, meta.Dimensions, out.Dims)
	require.Equal(t, []int64{1, 2, 3, 4}, out.Elements)

	// truncated multi-dimensional arrays are rejected
	err := out.Scan(ArrayValue{Meta: meta, Elements: elements[:3]})
	require.Error(t, err)
	require.Contains(t, err.Error(), "truncated")

	require.NoError(t, out.Scan(nil))
	require.False(t, out.Valid)
}

func TestArrayValueNull(t *testing.T) {
	v, err := FlatArray[int64](nil).Value()
	require.NoError(t, err)
	require.Nil(t, v)

	var dst FlatArray[int64]
	require.NoError(t, dst.Scan(ArrayValue{})) // nil Elements = NULL
	require.Nil(t, dst)
}

func TestGetArrayEncodeTypePlainSlices(t *testing.T) {
	g, err := getArrayEncodeType([]int64{1})
	require.NoError(t, err)
	dims, err := g.Dimensions(nil)
	require.NoError(t, err)
	require.Equal(t, []ArrayDimension{{LowerBound: 1, Length: 1}}, dims)

	_, err = getArrayEncodeType(42)
	require.Error(t, err)

	_, err = getArrayEncodeType([][]int64{{1}, {1, 2}})
	require.Error(t, err)
	require.Contains(t, err.Error(), "ragged")
}

func TestEncodeRejectsNULLElements(t *testing.T) {
	g, err := getArrayEncodeType([]*int64{nil})
	require.NoError(t, err)
	_, _, err = elementsFromGetter(g, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "NULL array element")
}

func TestTimezoneElementUnsupported(t *testing.T) {
	p := &wireProtocol{charset: "UTF8"}
	meta := &ArrayMeta{TypeID: SQL_TYPE_TIME_TZ, Length: 8, FieldBytes: 8}
	_, err := p.encodeSliceElements(meta, []any{time.Now()})
	require.Error(t, err)
	require.Contains(t, err.Error(), "not supported")
}

func TestGenerateSDLInt128(t *testing.T) {
	meta := &ArrayMeta{
		TableName: "T", FieldName: "I128",
		TypeID: SQL_TYPE_INT128, BlrTypeID: blr_type_int128, Scale: -2, Length: 16, FieldBytes: 16,
		Dimensions: []ArrayDimension{{LowerBound: 1, Length: 3}},
	}
	sdl := generateSDL(meta, meta.Dimensions, false)
	// version1, struct, 1, blr_int128, scale -2 (byte 254), relation "T", field "I128",
	// do1 dim0 (lower=1), tiny bound 3, element, 1, scalar, 0, 1 dim, variable 0, eoc
	want := []byte{
		1, 6, 1, blr_type_int128, 254,
		2, 1, 'T',
		4, 4, 'I', '1', '2', '8',
		35, 0, 9, 3,
		36, 1, 8, 0, 1, 7, 0,
		255,
	}
	require.Equal(t, want, sdl)
}

func TestDecodeSliceElementsINT128(t *testing.T) {
	p := &wireProtocol{charset: "UTF8"}
	meta := &ArrayMeta{TypeID: SQL_TYPE_INT128, BlrTypeID: blr_type_int128, Length: 16, FieldBytes: 16}
	// Two 16-byte elements: 12345, -1
	raw1 := int128Bytes(big.NewInt(12345))
	raw2 := int128Bytes(big.NewInt(-1))
	data := append(raw1, raw2...)
	decoded, err := p.decodeSliceElements(meta, data)
	require.NoError(t, err)
	require.Equal(t, []any{"12345", "-1"}, decoded)
}

func TestDecodeSliceElementsDEC64(t *testing.T) {
	p := &wireProtocol{charset: "UTF8"}
	meta := &ArrayMeta{TypeID: SQL_TYPE_DEC64, BlrTypeID: blr_type_dec64, Length: 8, FieldBytes: 8}
	// Two 8-byte elements: NaN, Infinity
	raw1 := []byte{0x7C, 0, 0, 0, 0, 0, 0, 0}
	raw2 := []byte{0x78, 0, 0, 0, 0, 0, 0, 0}
	data := append(raw1, raw2...)
	decoded, err := p.decodeSliceElements(meta, data)
	require.NoError(t, err)
	require.Equal(t, []any{"NaN", "Infinity"}, decoded)
}

func TestDecodeSliceElementsDEC128(t *testing.T) {
	p := &wireProtocol{charset: "UTF8"}
	meta := &ArrayMeta{TypeID: SQL_TYPE_DEC128, BlrTypeID: blr_type_dec128, Length: 16, FieldBytes: 16}
	// Two 16-byte elements: NaN, Infinity
	raw1 := append([]byte{0x7C}, make([]byte, 15)...)
	raw2 := append([]byte{0x78}, make([]byte, 15)...)
	data := append(raw1, raw2...)
	decoded, err := p.decodeSliceElements(meta, data)
	require.NoError(t, err)
	require.Equal(t, []any{"NaN", "Infinity"}, decoded)
}

func TestEncodeDecodeSliceINT128RoundTrip(t *testing.T) {
	p := &wireProtocol{charset: "UTF8"}
	meta := &ArrayMeta{TypeID: SQL_TYPE_INT128, BlrTypeID: blr_type_int128, Length: 16, FieldBytes: 16}
	elements := []any{big.NewInt(1234567890123456), "-9876543210987654", int64(42)}
	data, err := p.encodeSliceElements(meta, elements)
	require.NoError(t, err)
	require.Len(t, data, 48) // 3 elements × 16 bytes
	decoded, err := p.decodeSliceElements(meta, data)
	require.NoError(t, err)
	require.Equal(t, []any{"1234567890123456", "-9876543210987654", "42"}, decoded)

	// Test scaled INT128
	scaledMeta := &ArrayMeta{TypeID: SQL_TYPE_INT128, BlrTypeID: blr_type_int128, Scale: -2, Length: 16, FieldBytes: 16}
	scaledElements := []any{"123.45", "-0.99", int64(10)}
	data, err = p.encodeSliceElements(scaledMeta, scaledElements)
	require.NoError(t, err)
	require.Len(t, data, 48)
	decoded, err = p.decodeSliceElements(scaledMeta, data)
	require.NoError(t, err)
	require.Equal(t, []any{"123.45", "-0.99", "10.00"}, decoded)
}


