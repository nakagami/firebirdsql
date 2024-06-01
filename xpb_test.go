package firebirdsql

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNewXPBReader(t *testing.T) {
	assert.NotNil(t, NewXPBReader(nil))
	assert.NotNil(t, NewXPBReader([]byte{1, 2, 3}))
}

func TestXPBReaderEnd(t *testing.T) {
	xpb := NewXPBReader(nil)
	require.NotNil(t, xpb)
	assert.True(t, xpb.End())
	xpb = NewXPBReader([]byte{1, 2, 3})
	require.NotNil(t, xpb)
	assert.False(t, xpb.End())
}

func TestXPBReader_GetInt16(t *testing.T) {
	xpb := NewXPBReader([]byte{2, 2})
	require.NotNil(t, xpb)
	assert.Equal(t, int16(514), xpb.GetInt16())
	require.True(t, xpb.End())
}

func TestXPBReader_GetInt32(t *testing.T) {
	xpb := NewXPBReader([]byte{1, 2, 3, 4})
	require.NotNil(t, xpb)
	assert.Equal(t, int32(67305985), xpb.GetInt32())
	require.True(t, xpb.End())
}

func TestXPBReader_GetString(t *testing.T) {
	xpb := NewXPBReader([]byte{4, 0, 't', 'e', 's', 't'})
	require.NotNil(t, xpb)
	assert.Equal(t, "test", xpb.GetString())
	require.True(t, xpb.End())
}

func TestXPBReader_GetMixed(t *testing.T) {
	xpb := NewXPBReader([]byte{4, 0, 't', 'e', 's', 't', 1, 1, 2, 2, 2, 2})
	require.NotNil(t, xpb)
	assert.Equal(t, "test", xpb.GetString())
	assert.Equal(t, int16(257), xpb.GetInt16())
	assert.Equal(t, int32(33686018), xpb.GetInt32())
	require.True(t, xpb.End())
}

func TestXPBReader_Next(t *testing.T) {
	xpb := NewXPBReader([]byte{1, 2, 4, 0, 't', 'e', 's', 't'})
	require.NotNil(t, xpb)
	have, val := xpb.Next()
	assert.True(t, have)
	assert.Equal(t, byte(1), val)
	have, val = xpb.Next()
	assert.True(t, have)
	assert.Equal(t, byte(2), val)
	assert.Equal(t, "test", xpb.GetString())
	have, _ = xpb.Next()
	assert.False(t, have)
	require.True(t, xpb.End())
}

func TestXPBReader_Get(t *testing.T) {
	xpb := NewXPBReader([]byte{3, 4})
	require.NotNil(t, xpb)
	v1 := xpb.Get()
	_, v2 := xpb.Next()
	assert.Equal(t, byte(3), v1)
	assert.Equal(t, byte(3), v2)
	v1 = xpb.Get()
	_, v2 = xpb.Next()
	assert.Equal(t, byte(4), v1)
	assert.Equal(t, byte(4), v2)
}

func TestXPBReader_Reset(t *testing.T) {
	xpb := NewXPBReader([]byte{4, 0, 't', 'e', 's', 't'})
	require.NotNil(t, xpb)
	assert.Equal(t, "test", xpb.GetString())
	require.True(t, xpb.End())
	xpb.Reset()
	require.False(t, xpb.End())
	assert.Equal(t, "test", xpb.GetString())
}

func TestNewXPBWriter(t *testing.T) {
	w := NewXPBWriter()
	require.NotNil(t, w)
	assert.Equal(t, []byte{}, w.Bytes())
}

func TestNewXPBWriterFromTag(t *testing.T) {
	w := NewXPBWriterFromTag(1)
	require.NotNil(t, w)
	assert.Equal(t, []byte{1}, w.Bytes())
}

func TestNewXPBWriterFromBytes(t *testing.T) {
	w := NewXPBWriterFromBytes([]byte{1, 2, 3})
	require.NotNil(t, w)
	assert.Equal(t, []byte{1, 2, 3}, w.Bytes())
}

func TestXPBWriter_PutTag(t *testing.T) {
	w := NewXPBWriter()
	require.NotNil(t, w)
	w.PutTag(5)
	assert.Equal(t, []byte{5}, w.Bytes())
}

func TestXPBWriter_PutByte(t *testing.T) {
	w := NewXPBWriter()
	require.NotNil(t, w)
	w.PutByte(5, 6)
	assert.Equal(t, []byte{5, 6}, w.Bytes())
}

func TestXPBWriter_PutInt16(t *testing.T) {
	w := NewXPBWriter()
	require.NotNil(t, w)
	w.PutInt16(7, 20000)
	assert.Equal(t, []byte{7, 0x20, 0x4e}, w.Bytes())
}

func TestXPBWriter_PutInt32(t *testing.T) {
	w := NewXPBWriter()
	require.NotNil(t, w)
	w.PutInt32(7, 200000000)
	assert.Equal(t, []byte{7, 0x0, 0xc2, 0xeb, 0xb}, w.Bytes())
}

func TestXPBWriter_PutString(t *testing.T) {
	w := NewXPBWriter()
	require.NotNil(t, w)
	w.PutString(8, "test")
	assert.Equal(t, []byte{8, 4, 0, 't', 'e', 's', 't'}, w.Bytes())
}

func TestXPBWriter_PutMixed(t *testing.T) {
	w := NewXPBWriter()
	require.NotNil(t, w)
	w.PutTag(1).PutByte(2, 3).PutInt16(4, 5).PutInt32(6, 7).PutString(8, "test").PutTag(9)
	assert.Equal(t, []byte{1, 2, 3, 4, 5, 0, 6, 7, 0, 0, 0, 8, 4, 0, 't', 'e', 's', 't', 9}, w.Bytes())
}

func TestXPBWriter_Reset(t *testing.T) {
	w := NewXPBWriter()
	require.NotNil(t, w)
	w.PutString(1, "test")
	assert.Equal(t, []byte{1, 4, 0, 't', 'e', 's', 't'}, w.Bytes())
	w.Reset()
	assert.Equal(t, []byte{}, w.Bytes())
}
