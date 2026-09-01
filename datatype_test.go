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
	"context"
	"database/sql/driver"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Phase 3 of the Jaybird test port plan (JAYBIRD_TEST_PORT_PLAN.md):
// datatype round-trip matrices, mirroring Jaybird's BooleanSupportTest,
// DecfloatSupportTest, Int128SupportTest, DecimalPrecision38SupportTest,
// TimeWithTimeZoneSupportTest, TimestampWithTimeZoneSupportTest,
// SessionTimeZoneTest, FBEncodingsTest, FBPreparedStatementUTF8Test and the
// per-type boundary coverage of its field/* suites.

var dtypeCtx = context.Background()

// dtypeWallClock asserts got carries exactly want's wall-clock fields. Used
// for the naive DATE/TIME/TIMESTAMP types, which the driver round-trips as
// wall-clock values independent of session zone.
func dtypeWallClock(t *testing.T, want, got time.Time) {
	t.Helper()
	if got.Year() != want.Year() || got.Month() != want.Month() || got.Day() != want.Day() ||
		got.Hour() != want.Hour() || got.Minute() != want.Minute() || got.Second() != want.Second() ||
		got.Nanosecond() != want.Nanosecond() {
		t.Fatalf("wall clock mismatch: want %v, got %v", want, got)
	}
}

type dtypeCase struct {
	name  string
	gate  func(*testing.T) // nil = run on every server version
	col   string           // column DDL after the type name
	binds []driver.Value
	check func(t *testing.T, bind driver.Value, got any)
}

func TestDatatypeMatrix(t *testing.T) {
	db, _, _ := createTestDatabaseWithDDL(t, "dtype_matrix_")

	cases := []dtypeCase{
		{
			name:  "SMALLINT boundaries",
			col:   "SMALLINT",
			binds: []driver.Value{int64(-32768), int64(32767), int64(0), nil},
			check: func(t *testing.T, bind driver.Value, got any) {
				if bind == nil {
					require.Nil(t, got)
					return
				}
				require.Equal(t, bind, got)
			},
		},
		{
			name:  "INTEGER boundaries",
			col:   "INTEGER",
			binds: []driver.Value{int64(math.MinInt32), int64(math.MaxInt32), int64(0), nil},
			check: func(t *testing.T, bind driver.Value, got any) {
				require.Equal(t, bind, got)
			},
		},
		{
			name:  "BIGINT boundaries",
			col:   "BIGINT",
			binds: []driver.Value{int64(math.MinInt64), int64(math.MaxInt64), int64(0), nil},
			check: func(t *testing.T, bind driver.Value, got any) {
				require.Equal(t, bind, got)
			},
		},
		{
			name: "FLOAT round trip",
			col:  "FLOAT",
			binds: []driver.Value{
				float64(float32(1.5)),
				float64(float32(-2.25)),
				float64(float32(3.4028235e38)),
				float64(float32(0)),
				nil,
			},
			check: func(t *testing.T, bind driver.Value, got any) {
				require.Equal(t, bind, got) // both sides are float32-rounded
			},
		},
		{
			name:  "DOUBLE boundaries",
			col:   "DOUBLE PRECISION",
			binds: []driver.Value{1.25, -math.MaxFloat64, math.MaxFloat64, 0.0, nil},
			check: func(t *testing.T, bind driver.Value, got any) {
				require.Equal(t, bind, got)
			},
		},
		{
			name:  "NUMERIC(9,2)",
			col:   "NUMERIC(9,2)",
			binds: []driver.Value{"123.45", "-987.65", "0.01", nil},
			check: func(t *testing.T, bind driver.Value, got any) {
				require.Equal(t, bind, got)
			},
		},
		{
			name:  "NUMERIC(18,2) extremes",
			col:   "NUMERIC(18,2)",
			binds: []driver.Value{"999999999999999.99", "-999999999999999.99"},
			check: func(t *testing.T, bind driver.Value, got any) {
				require.Equal(t, bind, got)
			},
		},
		{
			name:  "VARCHAR values",
			col:   "VARCHAR(4000)",
			binds: []driver.Value{"hello", "", "héllo 🎉", strings.Repeat("x", 3000), nil},
			check: func(t *testing.T, bind driver.Value, got any) {
				require.Equal(t, bind, got)
			},
		},
		{
			name:  "CHAR trailing space trim",
			col:   "CHAR(10)",
			binds: []driver.Value{"ab", "firebird", nil},
			check: func(t *testing.T, bind driver.Value, got any) {
				// Jaybird parity (TrimmableField): CHAR values are returned
				// with trailing blanks trimmed.
				require.Equal(t, bind, got)
			},
		},
		{
			name:  "DATE boundaries",
			col:   "DATE",
			binds: []driver.Value{time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(9999, 12, 31, 0, 0, 0, 0, time.UTC), time.Date(2026, 8, 27, 0, 0, 0, 0, time.UTC), nil},
			check: func(t *testing.T, bind driver.Value, got any) {
				if bind == nil {
					require.Nil(t, got)
					return
				}
				gotTime, ok := got.(time.Time)
				require.True(t, ok, "got %T", got)
				dtypeWallClock(t, bind.(time.Time), gotTime)
			},
		},
		{
			name:  "TIME boundaries",
			col:   "TIME",
			binds: []driver.Value{time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(0, 1, 1, 23, 59, 59, 999900000, time.UTC), nil},
			check: func(t *testing.T, bind driver.Value, got any) {
				if bind == nil {
					require.Nil(t, got)
					return
				}
				gotTime, ok := got.(time.Time)
				require.True(t, ok, "got %T", got)
				dtypeWallClock(t, bind.(time.Time), gotTime)
			},
		},
		{
			name:  "TIMESTAMP boundaries",
			col:   "TIMESTAMP",
			binds: []driver.Value{time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(9999, 12, 31, 23, 59, 59, 999900000, time.UTC), time.Date(2024, 2, 29, 12, 34, 56, 789000000, time.UTC), nil},
			check: func(t *testing.T, bind driver.Value, got any) {
				if bind == nil {
					require.Nil(t, got)
					return
				}
				gotTime, ok := got.(time.Time)
				require.True(t, ok, "got %T", got)
				dtypeWallClock(t, bind.(time.Time), gotTime)
			},
		},
		{
			name:  "BLOB SUB_TYPE BINARY round trip",
			col:   "BLOB SUB_TYPE BINARY",
			binds: []driver.Value{[]byte{0x00, 0x01, 0xFF}, []byte{}, nil, randomBytes(1024 * 1024)},
			check: func(t *testing.T, bind driver.Value, got any) {
				if bind == nil {
					require.Nil(t, got)
					return
				}
				gotBytes, ok := got.([]byte)
				require.True(t, ok, "got %T", got)
				require.True(t, bytes.Equal(bind.([]byte), gotBytes))
			},
		},
		{
			name:  "BLOB SUB_TYPE TEXT round trip",
			col:   "BLOB SUB_TYPE TEXT",
			binds: []driver.Value{"text blob", "emoji 🎉 text", strings.Repeat("b", 100*1024), nil},
			check: func(t *testing.T, bind driver.Value, got any) {
				if bind == nil {
					require.Nil(t, got)
					return
				}
				require.Equal(t, bind, got)
			},
		},
		{
			name:  "INT128 extremes",
			gate:  requireInt128Support,
			col:   "INT128",
			binds: []driver.Value{"170141183460469231731687303715884105726", "-170141183460469231731687303715884105727", nil},
			check: func(t *testing.T, bind driver.Value, got any) {
				require.Equal(t, bind, got)
			},
		},
		{
			name:  "NUMERIC(38,2) extremes",
			gate:  requireDecimal38Support,
			col:   "NUMERIC(38,2)",
			binds: []driver.Value{"9999999999999999999999999999999999.99", "-9999999999999999999999999999999999.99"},
			check: func(t *testing.T, bind driver.Value, got any) {
				require.Equal(t, bind, got)
			},
		},
		{
			name:  "DECFLOAT(16)",
			gate:  requireDecFloatSupport,
			col:   "DECFLOAT(16)",
			binds: []driver.Value{"1.1", "-120.2", nil},
			check: func(t *testing.T, bind driver.Value, got any) {
				require.Equal(t, bind, got)
			},
		},
		{
			name:  "DECFLOAT(34)",
			gate:  requireDecFloatSupport,
			col:   "DECFLOAT(34)",
			binds: []driver.Value{"1.1", "-120.2", nil},
			check: func(t *testing.T, bind driver.Value, got any) {
				require.Equal(t, bind, got)
			},
		},
		{
			name:  "BOOLEAN all values",
			gate:  requireBooleanSupport,
			col:   "BOOLEAN",
			binds: []driver.Value{true, false, nil},
			check: func(t *testing.T, bind driver.Value, got any) {
				require.Equal(t, bind, got)
			},
		},
		{
			name:  "TIMESTAMP WITH TIME ZONE instant preserved",
			gate:  requireTimeZoneSupport,
			col:   "TIMESTAMP WITH TIME ZONE",
			binds: []driver.Value{time.Date(2024, 1, 15, 10, 30, 0, 0, time.FixedZone("UTC-5", -5*3600)), time.Date(1967, 8, 11, 23, 45, 1, 0, time.UTC), nil},
			check: func(t *testing.T, bind driver.Value, got any) {
				if bind == nil {
					require.Nil(t, got)
					return
				}
				gotTime, ok := got.(time.Time)
				require.True(t, ok, "got %T", got)
				// WITH TIME ZONE columns preserve the instant, not the wall clock.
				require.True(t, bind.(time.Time).Equal(gotTime), "want %v, got %v", bind, gotTime)
			},
		},
		{
			name:  "TIME WITH TIME ZONE instant preserved",
			gate:  requireTimeZoneSupport,
			col:   "TIME WITH TIME ZONE",
			binds: []driver.Value{time.Date(0, 1, 1, 9, 15, 0, 0, time.FixedZone("UTC+5:30", 5*3600+1800)), nil},
			check: func(t *testing.T, bind driver.Value, got any) {
				if bind == nil {
					require.Nil(t, got)
					return
				}
				gotTime, ok := got.(time.Time)
				require.True(t, ok, "got %T", got)
				// The server renders WITH TIME ZONE values in the session
				// zone, so compare the instant of the time-of-day (wall clock
				// minus zone offset), not the raw hour fields.
				timeOfDay := func(v time.Time) int {
					_, off := v.Zone()
					return v.Hour()*3600 + v.Minute()*60 + v.Second() - off
				}
				want := bind.(time.Time)
				require.Equal(t, timeOfDay(want)%86400, ((timeOfDay(gotTime)%86400)+86400)%86400)
			},
		},
	}

	for i, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			if c.gate != nil {
				c.gate(t)
			}
			// Short names: Firebird 2.5 limits identifiers to 31 bytes.
			table := fmt.Sprintf("TM_%02d", i)
			mustExec(t, dtypeCtx, db, "CREATE TABLE "+table+" (VAL "+c.col+")")
			t.Cleanup(func() { _, _ = db.Exec("DROP TABLE " + table) })

			for _, bind := range c.binds {
				mustExec(t, dtypeCtx, db, "INSERT INTO "+table+" (VAL) VALUES (?)", bind)
				var got any
				require.NoError(t, db.QueryRow("SELECT VAL FROM "+table).Scan(&got))
				c.check(t, bind, got)
				mustExec(t, dtypeCtx, db, "DELETE FROM "+table)
			}
		})
	}
}

func randomBytes(n int) []byte {
	b := make([]byte, n)
	rand.Read(b)
	return b
}

// TestDatatypeColumnMetadata mirrors Jaybird's FBResultSetMetaDataParametrizedTest
// (driver-level subset): every column of a representative result set must
// report its database type name, scan type, nullability and numeric properties.
func TestDatatypeColumnMetadata(t *testing.T) {
	requireBooleanSupport(t) // the fixture table includes a BOOLEAN column
	db, _, _ := createTestDatabaseWithDDL(t, "dtype_meta_", `
		CREATE TABLE DTYPE_META (
			C_SMALL  SMALLINT NOT NULL,
			C_INT    INTEGER,
			C_BIG    BIGINT,
			C_FLOAT  FLOAT,
			C_DOUBLE DOUBLE PRECISION,
			C_NUM    NUMERIC(9,2),
			C_VARCH  VARCHAR(123),
			C_CHAR   CHAR(10),
			C_DATE   DATE,
			C_TIME   TIME,
			C_TS     TIMESTAMP,
			C_BLOB   BLOB SUB_TYPE BINARY,
			C_BOOL   BOOLEAN
		)`)

	rows, err := db.Query("SELECT * FROM DTYPE_META WHERE 1=0")
	require.NoError(t, err)
	defer rows.Close()

	types, err := rows.ColumnTypes()
	require.NoError(t, err)
	require.Len(t, types, 13)

	names := make([]string, len(types))
	for i, ct := range types {
		names[i] = ct.Name()
	}
	require.Equal(t, []string{
		"C_SMALL", "C_INT", "C_BIG", "C_FLOAT", "C_DOUBLE", "C_NUM", "C_VARCH",
		"C_CHAR", "C_DATE", "C_TIME", "C_TS", "C_BLOB", "C_BOOL",
	}, names)

	wantType := map[string]string{
		"C_SMALL": "SHORT", "C_INT": "LONG", "C_BIG": "INT64",
		"C_FLOAT": "FLOAT", "C_DOUBLE": "DOUBLE", "C_VARCH": "VARYING",
		"C_CHAR": "TEXT", "C_DATE": "DATE", "C_TIME": "TIME",
		"C_TS": "TIMESTAMP", "C_BLOB": "BLOB", "C_BOOL": "BOOLEAN",
	}
	wantScanKind := map[string]reflect.Kind{
		"C_SMALL": reflect.Int64, "C_INT": reflect.Int64, "C_BIG": reflect.Int64,
		"C_FLOAT": reflect.Float64, "C_DOUBLE": reflect.Float64,
		"C_VARCH": reflect.String, "C_CHAR": reflect.String,
		"C_DATE": reflect.Struct, "C_TIME": reflect.Struct, "C_TS": reflect.Struct,
		"C_BLOB": reflect.Slice, "C_BOOL": reflect.Bool,
	}
	for i, ct := range types {
		name := names[i]
		if want, ok := wantType[name]; ok {
			require.Equal(t, want, ct.DatabaseTypeName(), "%s: DatabaseTypeName", name)
		}
		if want, ok := wantScanKind[name]; ok {
			require.Equal(t, want, ct.ScanType().Kind(), "%s: ScanType kind", name)
		}
	}
	// Nullable: the NOT NULL column reports not-nullable, everything else nullable.
	nullable, ok := types[0].Nullable()
	require.True(t, ok)
	require.False(t, nullable, "C_SMALL is NOT NULL")
	nullable, ok = types[2].Nullable()
	require.True(t, ok)
	require.True(t, nullable, "C_BIG is nullable")

	// DecimalSize on NUMERIC(9,2): reported with Firebird's negative scale.
	p, s, ok := types[5].DecimalSize()
	require.True(t, ok, "NUMERIC(9,2) should report precision/scale")
	require.True(t, p > 0)
	require.Equal(t, int64(-2), s)

	// Length on VARCHAR(123): at least the declared character length.
	l, ok := types[6].Length()
	require.True(t, ok)
	require.GreaterOrEqual(t, l, int64(123))
}

// TestSessionTimeZone mirrors Jaybird's SessionTimeZoneTest: the session zone
// applies to CURRENT_TIMESTAMP, naive columns keep their wall clock across
// connections with different zones, and an invalid zone is rejected.
func TestSessionTimeZone(t *testing.T) {
	requireTimeZoneSupport(t)

	dbPath, dsn, err := CreateTestDatabase("dtype_session_tz_")
	require.NoError(t, err)
	t.Cleanup(func() { _ = removeDatabaseFile(dbPath) })

	tokyoDB := openTestDatabase(t, dsn+"?timezone=Asia/Tokyo")
	utcDB := openTestDatabase(t, dsn+"?timezone=UTC")

	mustExec(t, dtypeCtx, tokyoDB, `CREATE TABLE TZ_SESSION (ID INTEGER PRIMARY KEY, NAIVE TIMESTAMP, AWARE TIMESTAMP WITH TIME ZONE)`)

	// CURRENT_TIMESTAMP honors the session zone.
	var now time.Time
	require.NoError(t, tokyoDB.QueryRow("SELECT CURRENT_TIMESTAMP FROM RDB$DATABASE").Scan(&now))
	_, offset := now.Zone()
	require.Equal(t, 9*3600, offset, "CURRENT_TIMESTAMP should carry the Asia/Tokyo offset, got %v", now)

	// A wall clock written from the Tokyo connection reads back unchanged
	// through both the Tokyo and the UTC connection.
	naive := time.Date(2024, 6, 1, 3, 4, 5, 0, time.FixedZone("JST", 9*3600))
	aware := time.Date(2024, 6, 1, 3, 4, 5, 0, time.FixedZone("JST", 9*3600))
	mustExec(t, dtypeCtx, tokyoDB, "INSERT INTO TZ_SESSION (ID, NAIVE, AWARE) VALUES (1, ?, ?)", naive, aware)

	var gotNaive time.Time
	require.NoError(t, tokyoDB.QueryRow("SELECT NAIVE FROM TZ_SESSION WHERE ID = 1").Scan(&gotNaive))
	dtypeWallClock(t, naive, gotNaive)

	require.NoError(t, utcDB.QueryRow("SELECT NAIVE FROM TZ_SESSION WHERE ID = 1").Scan(&gotNaive))
	dtypeWallClock(t, naive, gotNaive)

	// The aware column preserves the instant in both sessions.
	var gotAware time.Time
	require.NoError(t, tokyoDB.QueryRow("SELECT AWARE FROM TZ_SESSION WHERE ID = 1").Scan(&gotAware))
	require.True(t, aware.Equal(gotAware))
	require.NoError(t, utcDB.QueryRow("SELECT AWARE FROM TZ_SESSION WHERE ID = 1").Scan(&gotAware))
	require.True(t, aware.Equal(gotAware))
}

// TestCharsetRoundTrip mirrors Jaybird's FBEncodingsTest: text round-trips
// through non-UTF8 connection charsets.
func TestCharsetRoundTrip(t *testing.T) {
	cases := []struct {
		charset string
		column  string
		value   string
	}{
		{"WIN1251", "VARCHAR(100) CHARACTER SET WIN1251", "Привет, мир!"},
		{"WIN1252", "VARCHAR(100) CHARACTER SET WIN1252", "café ünïcode"},
		{"UTF8", "VARCHAR(100) CHARACTER SET UTF8", "héllo 🎉"},
	}
	for _, c := range cases {
		t.Run(c.charset, func(t *testing.T) {
			dbPath, dsn, err := CreateTestDatabase("dtype_charset_")
			require.NoError(t, err)
			t.Cleanup(func() { _ = removeDatabaseFile(dbPath) })
			db := openTestDatabase(t, dsn+"?charset="+c.charset)

			mustExec(t, dtypeCtx, db, "CREATE TABLE CS_TEST (VAL "+c.column+")")
			mustExec(t, dtypeCtx, db, "INSERT INTO CS_TEST (VAL) VALUES (?)", c.value)

			var got string
			require.NoError(t, db.QueryRow("SELECT VAL FROM CS_TEST").Scan(&got))
			require.Equal(t, c.value, got)
		})
	}
}

// TestSqlCounts mirrors Jaybird's SqlCountHolderTest integration side: the
// affected-row counts reported through isc_info_sql_records must be exact.
func TestSqlCounts(t *testing.T) {
	db, _, _ := createTestDatabaseWithDDL(t, "dtype_counts_",
		`CREATE TABLE COUNTS (ID INTEGER PRIMARY KEY, V VARCHAR(10))`)

	for i := 1; i <= 3; i++ {
		res := mustExec(t, dtypeCtx, db, "INSERT INTO COUNTS (ID, V) VALUES (?, ?)", i, "x")
		n, err := res.RowsAffected()
		require.NoError(t, err)
		require.Equal(t, int64(1), n)
	}

	res := mustExec(t, dtypeCtx, db, "UPDATE COUNTS SET V = 'y' WHERE ID <= 2")
	n, err := res.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(2), n)

	res = mustExec(t, dtypeCtx, db, "UPDATE COUNTS SET V = 'z' WHERE ID = 99")
	n, err = res.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(0), n)

	res = mustExec(t, dtypeCtx, db, "DELETE FROM COUNTS")
	n, err = res.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(3), n)
}
