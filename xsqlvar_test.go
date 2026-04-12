package firebirdsql

import (
	"encoding/binary"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestScaledIntValue(t *testing.T) {
	tests := []struct {
		name     string
		sqlscale int
		input    int64
		want     interface{}
	}{
		{"zero scale", 0, 42, int64(42)},
		{"positive scale 2", 2, 5, int64(500)},
		{"positive scale 3", 3, 7, int64(7000)},
		{"negative scale -3", -3, 1234, "1.234"},
		{"negative scale -2", -2, 50, "0.5"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			x := &xSQLVAR{sqlscale: tt.sqlscale}
			got := x.scaledIntValue(tt.input)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestScantypePositiveScale(t *testing.T) {
	wantInt64 := reflect.TypeOf(int64(0))
	wantString := reflect.TypeOf("")

	tests := []struct {
		name     string
		sqltype  int
		sqlscale int
		want     reflect.Type
	}{
		{"SHORT scale 0", SQL_TYPE_SHORT, 0, wantInt64},
		{"SHORT scale +2", SQL_TYPE_SHORT, 2, wantInt64},
		{"SHORT scale -3", SQL_TYPE_SHORT, -3, wantString},
		{"LONG scale 0", SQL_TYPE_LONG, 0, wantInt64},
		{"LONG scale +1", SQL_TYPE_LONG, 1, wantInt64},
		{"LONG scale -2", SQL_TYPE_LONG, -2, wantString},
		{"INT64 scale 0", SQL_TYPE_INT64, 0, wantInt64},
		{"INT64 scale +3", SQL_TYPE_INT64, 3, wantInt64},
		{"INT64 scale -4", SQL_TYPE_INT64, -4, wantString},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			x := &xSQLVAR{sqltype: tt.sqltype, sqlscale: tt.sqlscale}
			assert.Equal(t, tt.want, x.scantype())
		})
	}
}

func TestValuePositiveScale(t *testing.T) {
	tests := []struct {
		name     string
		sqltype  int
		sqlscale int
		rawValue []byte
		want     interface{}
	}{
		{
			"SHORT scale +2 value 5",
			SQL_TYPE_SHORT, 2,
			bigEndianInt32(5),
			int64(500),
		},
		{
			"LONG scale +2 value 7",
			SQL_TYPE_LONG, 2,
			bigEndianInt32(7),
			int64(700),
		},
		{
			"INT64 scale +1 value 3",
			SQL_TYPE_INT64, 1,
			bigEndianInt64(3),
			int64(30),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			x := &xSQLVAR{sqltype: tt.sqltype, sqlscale: tt.sqlscale}
			got, err := x.value(tt.rawValue, "", "")
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func bigEndianInt32(v int32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, uint32(v))
	return b
}

func bigEndianInt64(v int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(v))
	return b
}
