//go:build go1.27

package firebirdsql

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestColumnScanIntegerCompatibility(t *testing.T) {
	values := []driver.Value{
		nil, int64(0), int64(-1), int64(1),
		int64(math.MinInt32) - 1, int64(math.MinInt32), int64(math.MinInt32) + 1,
		int64(math.MaxInt32) - 1, int64(math.MaxInt32), int64(math.MaxInt32) + 1,
		int64(math.MinInt64), int64(math.MaxInt64),
		float64(42), float64(1.5), "42", "2147483648", "not an integer", []byte("-42"),
	}
	destinations := []struct {
		name string
		make func() any
	}{
		{"int", func() any { v := int(17); return &v }},
		{"int32", func() any { v := int32(17); return &v }},
		{"nil_int", func() any { return (*int)(nil) }},
		{"nil_int32", func() any { return (*int32)(nil) }},
		{"null_int", func() any { return &sql.Null[int]{V: 17, Valid: true} }},
		{"null_int32", func() any { return &sql.Null[int32]{V: 17, Valid: true} }},
	}
	for _, destination := range destinations {
		for _, value := range values {
			t.Run(fmt.Sprintf("%s/%T=%v", destination.name, value, value), func(t *testing.T) {
				got, want := destination.make(), destination.make()
				rows := &firebirdsqlRows{scanRow: []driver.Value{value}}
				wantErr := sql.ConvertAssign(driver.ScanContext{}, want, value)
				var gotErr error
				require.NotPanics(t, func() { gotErr = rows.ScanColumn(driver.ScanContext{}, 0, got) })
				if wantErr != nil {
					require.EqualError(t, gotErr, wantErr.Error())
				} else {
					require.NoError(t, gotErr)
				}
				// Failed conversions must also preserve the destination's value.
				require.True(t, reflect.DeepEqual(got, want), "got %v, want %v", got, want)
			})
		}
	}
	t.Run("native_int_width", func(t *testing.T) {
		rows := &firebirdsqlRows{scanRow: []driver.Value{int64(math.MaxInt32) + 1}}
		var got int
		err := rows.ScanColumn(driver.ScanContext{}, 0, &got)
		if strconv.IntSize == 32 {
			require.Error(t, err)
			require.Zero(t, got)
		} else {
			require.NoError(t, err)
			require.Equal(t, int64(math.MaxInt32)+1, int64(got))
		}
	})
}

type columnScanIntegerScanner struct {
	value int64
	calls int
	err   error
}

func (s *columnScanIntegerScanner) Scan(value any) error {
	s.calls++
	s.value = value.(int64)
	return s.err
}

func TestColumnScanIntegerCustomScanner(t *testing.T) {
	rows := &firebirdsqlRows{scanRow: []driver.Value{int64(math.MaxInt64)}}
	wantErr := errors.New("custom scanner rejected value")
	dest := &columnScanIntegerScanner{err: wantErr}
	require.ErrorIs(t, rows.ScanColumn(driver.ScanContext{}, 0, dest), wantErr)
	require.Equal(t, 1, dest.calls)
	require.Equal(t, int64(math.MaxInt64), dest.value)
}

func TestColumnScanInvalidIndex(t *testing.T) {
	for _, values := range [][]driver.Value{nil, {int64(42)}} {
		for _, index := range []int{-1, len(values), math.MaxInt32} {
			t.Run(fmt.Sprintf("columns=%d/index=%d", len(values), index), func(t *testing.T) {
				rows := &firebirdsqlRows{scanRow: values}
				var dest int
				var err error
				require.NotPanics(t, func() { err = rows.ScanColumn(driver.ScanContext{}, index, &dest) })
				require.Error(t, err)
			})
		}
	}
}

// Mix common destination types so the benchmark includes int/int32 conversion
// and standard fallbacks, rather than measuring only exact int64 assignments.
func BenchmarkColumnScanMixed(b *testing.B) {
	data := make([][]driver.Value, 1000)
	for i := range data {
		data[i] = []driver.Value{
			int64(i), int64(i + 1), int64(i + 2), float64(i) + 0.5,
			i%2 == 0, "row", []byte{1, 2, 3}, nil,
		}
		if i%2 == 0 {
			data[i][7] = "present"
		}
	}
	for _, legacy := range []bool{true, false} {
		b.Run(fmt.Sprintf("legacy=%t", legacy), func(b *testing.B) {
			db := sql.OpenDB(&scanConnector{legacy: legacy, data: data})
			defer db.Close()
			var i int
			var i32 int32
			var i64 int64
			var f float64
			var flag bool
			var text string
			var binary []byte
			var nullable sql.Null[string]
			dest := []any{&i, &i32, &i64, &f, &flag, &text, &binary, &nullable}
			b.ReportAllocs()
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				rows, err := db.Query("unused")
				if err != nil {
					b.Fatal(err)
				}
				for rows.Next() {
					if err := rows.Scan(dest...); err != nil {
						b.Fatal(err)
					}
				}
				if err := rows.Err(); err != nil {
					b.Fatal(err)
				}
				rows.Close()
			}
		})
	}
}
