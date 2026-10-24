//go:build go1.27

package firebirdsql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Hiding the optional interface lets both database/sql paths run on the same
// toolchain and input. Fetching is excluded to isolate the scanning overhead.
type legacyScanRows struct{ driver.Rows }
type columnScanRows struct{ *firebirdsqlRows }

func (r *columnScanRows) Close() error { return nil }

type scanConnector struct {
	data   [][]driver.Value
	legacy bool
}

func (c *scanConnector) Driver() driver.Driver { return &firebirdsqlDriver{} }
func (c *scanConnector) Connect(context.Context) (driver.Conn, error) {
	return &scanConn{connector: c}, nil
}

type scanConn struct{ connector *scanConnector }

func (c *scanConn) Close() error                        { return nil }
func (c *scanConn) Begin() (driver.Tx, error)           { return nil, errors.New("unused") }
func (c *scanConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("unused") }
func (c *scanConn) QueryContext(ctx context.Context, _ string, _ []driver.NamedValue) (driver.Rows, error) {
	xsqlda := make([]xSQLVAR, len(c.connector.data[0]))
	for i := range xsqlda {
		xsqlda[i].aliasname = fmt.Sprintf("C%d", i)
	}
	r := &columnScanRows{&firebirdsqlRows{
		ctx:             ctx,
		stmt:            &firebirdsqlStmt{fc: &firebirdsqlConn{}, resultXsqlda: xsqlda},
		currentChunk:    c.connector.data,
		currentChunkIdx: -1,
	}}
	if c.connector.legacy {
		return legacyScanRows{r}, nil
	}
	return r, nil
}

func TestColumnScanConversions(t *testing.T) {
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	for _, legacy := range []bool{true, false} {
		t.Run(fmt.Sprintf("legacy=%t", legacy), func(t *testing.T) {
			db := sql.OpenDB(&scanConnector{legacy: legacy, data: [][]driver.Value{
				{int64(42), float64(1.5), true, "hello", []byte{1, 2}, now, nil},
				{int64(43), float64(2.5), false, "world", []byte{3, 4}, now, "present"},
			}})
			defer db.Close()
			rows, err := db.Query("unused")
			require.NoError(t, err)
			defer rows.Close()
			for n := 0; n < 2; n++ {
				require.True(t, rows.Next())
				var i int
				var f float64
				var b bool
				var s string
				var bytes []byte
				var timestamp time.Time
				var nullable sql.Null[string]
				require.NoError(t, rows.Scan(&i, &f, &b, &s, &bytes, &timestamp, &nullable))
				require.Equal(t, 42+n, i)
				require.Equal(t, 1.5+float64(n), f)
				require.Equal(t, n == 0, b)
				require.Equal(t, []string{"hello", "world"}[n], s)
				require.Equal(t, []byte{byte(1 + 2*n), byte(2 + 2*n)}, bytes)
				require.Equal(t, now, timestamp)
				require.Equal(t, sql.Null[string]{V: []string{"", "present"}[n], Valid: n == 1}, nullable)
				bytes[0] = 99 // []byte destinations must own their data.
				var raw [7]sql.RawBytes
				require.NoError(t, rows.Scan(&raw[0], &raw[1], &raw[2], &raw[3], &raw[4], &raw[5], &raw[6]))
				require.Equal(t, fmt.Sprint(42+n), string(raw[0]))
				require.Equal(t, []byte{byte(1 + 2*n), byte(2 + 2*n)}, []byte(raw[4]))
				require.Equal(t, now.Format(time.RFC3339Nano), string(raw[5]))
			}
			require.False(t, rows.Next())
			require.NoError(t, rows.Err())
		})
	}
}

func TestColumnScanErrors(t *testing.T) {
	for _, legacy := range []bool{true, false} {
		t.Run(fmt.Sprintf("legacy=%t", legacy), func(t *testing.T) {
			db := sql.OpenDB(&scanConnector{legacy: legacy, data: [][]driver.Value{{"1.5"}, {nil}}})
			defer db.Close()
			rows, err := db.Query("unused")
			require.NoError(t, err)
			defer rows.Close()
			var i int
			require.ErrorContains(t, rows.Scan(&i), "Scan called without calling Next")
			require.True(t, rows.Next())
			require.ErrorContains(t, rows.Scan(), "expected 1 destination arguments")
			require.EqualError(t, rows.Scan(&i), `sql: Scan error on column index 0, name "C0": converting driver.Value type string ("1.5") to a int: invalid syntax`)
			var nilDest *string
			require.ErrorContains(t, rows.Scan(nilDest), "destination pointer is nil")
			var retry string
			require.NoError(t, rows.Scan(&retry))
			require.Equal(t, "1.5", retry)
			require.True(t, rows.Next())
			require.ErrorContains(t, rows.Scan(&i), "converting NULL to int is unsupported")
			require.False(t, rows.Next())
			require.NoError(t, rows.Err())
		})
	}
}

func TestColumnScanScalarCompatibility(t *testing.T) {
	values := []driver.Value{nil, int64(42), int64(256), float64(1.5), true, "42", "1.5", []byte("42")}
	destinations := []func() any{
		func() any { return new(int64) },
		func() any { return new(float64) },
		func() any { return new(bool) },
		func() any { return new(int) },
		func() any { return new(uint8) },
		func() any { return new(string) },
		func() any { return new(any) },
		func() any { return new(*int64) },
		func() any { return new(sql.Null[int64]) },
		func() any { return new(sql.Null[float64]) },
		func() any { return new(sql.Null[bool]) },
		func() any { return (*int64)(nil) },
		func() any { return (*float64)(nil) },
		func() any { return (*bool)(nil) },
	}
	// Compare panic behavior too: some typed nil destinations are not checked
	// by database/sql. Match the toolchain's behavior without hard-coding it.
	invoke := func(scan func() error) (err error, panicValue any) {
		defer func() { panicValue = recover() }()
		err = scan()
		return
	}
	for _, value := range values {
		for _, destination := range destinations {
			got, want := destination(), destination()
			t.Run(fmt.Sprintf("%T=%v/to=%T", value, value, got), func(t *testing.T) {
				rows := &firebirdsqlRows{scanRow: []driver.Value{value}}
				wantErr, wantPanic := invoke(func() error { return sql.ConvertAssign(driver.ScanContext{}, want, value) })
				gotErr, gotPanic := invoke(func() error { return rows.ScanColumn(driver.ScanContext{}, 0, got) })
				require.Equal(t, fmt.Sprint(wantPanic), fmt.Sprint(gotPanic))
				if wantErr != nil {
					require.EqualError(t, gotErr, wantErr.Error())
				} else {
					require.NoError(t, gotErr)
					require.True(t, reflect.DeepEqual(got, want), "got %v, want %v", got, want)
				}
			})
		}
	}
}

func TestColumnScanProcedure(t *testing.T) {
	rows := newFirebirdsqlRows(context.Background(), &firebirdsqlStmt{
		stmtType:     isc_info_sql_stmt_exec_procedure,
		resultXsqlda: []xSQLVAR{{sqltype: SQL_TYPE_LONG}},
	}, []driver.Value{int64(42)})
	require.NoError(t, rows.NextRow())
	var got int
	require.NoError(t, rows.ScanColumn(driver.ScanContext{}, 0, &got))
	require.Equal(t, 42, got)
	require.NoError(t, rows.ScanColumn(driver.ScanContext{}, 0, &got))
	require.ErrorIs(t, rows.NextRow(), io.EOF)
	require.Nil(t, rows.scanRow)
}

// Materialized large values must live in a reusable row, rather than being
// retained in every consumed row of the fetched chunk.
func TestColumnScanMaterializationBuffer(t *testing.T) {
	for _, sqlType := range []int{SQL_TYPE_BLOB, SQL_TYPE_ARRAY} {
		t.Run(fmt.Sprint(sqlType), func(t *testing.T) {
			rows := &firebirdsqlRows{
				ctx:             context.Background(),
				stmt:            &firebirdsqlStmt{resultXsqlda: []xSQLVAR{{sqltype: sqlType}}},
				currentChunk:    [][]driver.Value{{nil}, {nil}},
				currentChunkIdx: -1,
			}
			require.NoError(t, rows.NextRow())
			first := &rows.scanRow[0]
			require.NotSame(t, &rows.currentChunk[0][0], first)
			rows.scanRow[0] = []byte("materialized")
			require.Nil(t, rows.currentChunk[0][0])
			require.NoError(t, rows.NextRow())
			require.Same(t, first, &rows.scanRow[0])
			require.Nil(t, rows.scanRow[0])
			require.ErrorIs(t, rows.NextRow(), io.EOF)
			require.Nil(t, rows.scanRow)
		})
	}
}

func BenchmarkColumnScan(b *testing.B) {
	for _, width := range []int{1, 8, 32} {
		data := make([][]driver.Value, 1000)
		for i := range data {
			data[i] = make([]driver.Value, width)
			for j := range data[i] {
				data[i][j] = int64(i + j)
			}
		}
		for _, legacy := range []bool{true, false} {
			b.Run(fmt.Sprintf("columns=%d/legacy=%t", width, legacy), func(b *testing.B) {
				db := sql.OpenDB(&scanConnector{legacy: legacy, data: data})
				defer db.Close()
				values := make([]int64, width)
				dest := make([]any, width)
				for i := range dest {
					dest[i] = &values[i]
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
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
}
