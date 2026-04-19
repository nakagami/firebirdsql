package firebirdsql

import (
	"bytes"
	"database/sql"
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// eqCmp is a simple equality comparator for comparable types.
func eqCmp[T comparable](a, b T) bool { return a == b }

// runNullCase creates a two-row table (id INT PK, v <columnDDL>), inserts one
// Valid=true and one Valid=false row via sql.Null[T] bind params, then scans
// each back and asserts the Valid flag and round-tripped value.
// The test is skipped when fbMajor < minVersion (0 means no version requirement).
func runNullCase[T any](
	t *testing.T, db *sql.DB,
	name, tableSuffix, columnDDL string,
	sample T,
	equal func(a, b T) bool,
	minVersion, fbMajor int,
) {
	t.Helper()
	t.Run(name, func(t *testing.T) {
		if minVersion > 0 && fbMajor < minVersion {
			t.Skipf("requires Firebird %d+", minVersion)
		}
		table := "tnull_" + tableSuffix
		_, err := db.Exec(fmt.Sprintf(`CREATE TABLE %s (id INTEGER NOT NULL PRIMARY KEY, v %s)`, table, columnDDL))
		require.NoError(t, err)

		_, err = db.Exec(fmt.Sprintf(`INSERT INTO %s (id, v) VALUES (1, ?)`, table), sql.Null[T]{V: sample, Valid: true})
		require.NoError(t, err)
		_, err = db.Exec(fmt.Sprintf(`INSERT INTO %s (id, v) VALUES (2, ?)`, table), sql.Null[T]{})
		require.NoError(t, err)

		var got1 sql.Null[T]
		require.NoError(t, db.QueryRow(fmt.Sprintf(`SELECT v FROM %s WHERE id=1`, table)).Scan(&got1))
		assert.True(t, got1.Valid, "id=1: expected Valid=true")
		assert.True(t, equal(got1.V, sample), "id=1: got %v want %v", got1.V, sample)

		var got2 sql.Null[T]
		require.NoError(t, db.QueryRow(fmt.Sprintf(`SELECT v FROM %s WHERE id=2`, table)).Scan(&got2))
		assert.False(t, got2.Valid, "id=2: expected Valid=false")
	})
}

func TestNullGeneric(t *testing.T) {
	_, dsn, err := CreateTestDatabase("test_null_generic_")
	require.NoError(t, err)
	time.Sleep(1 * time.Second)

	db, err := sql.Open("firebirdsql", dsn)
	require.NoError(t, err)
	defer db.Close()

	fbMajor := get_firebird_major_version(t)

	// integer types
	runNullCase(t, db, "SMALLINT/int16", "int16", "SMALLINT", int16(1234), eqCmp[int16], 0, fbMajor)
	runNullCase(t, db, "INTEGER/int32", "int32", "INTEGER", int32(1_000_001), eqCmp[int32], 0, fbMajor)
	runNullCase(t, db, "BIGINT/int64", "int64", "BIGINT", int64(9_000_000_000), eqCmp[int64], 0, fbMajor)

	// fixed-point decimal — driver returns decimal.Decimal (xsqlvar.go:354,363,373)
	runNullCase(t, db, "DECIMAL_18_4/decimal.Decimal", "dec_dec", "DECIMAL(18,4)",
		decimal.New(12345, -2), // 123.45
		func(a, b decimal.Decimal) bool { return a.Equal(b) }, 0, fbMajor)
	runNullCase(t, db, "DECIMAL_18_4/float64", "dec_f64", "DECIMAL(18,4)",
		float64(123.45), eqCmp[float64], 0, fbMajor)
	runNullCase(t, db, "DECIMAL_18_4/string", "dec_str", "DECIMAL(18,4)",
		"123.45", eqCmp[string], 0, fbMajor)

	// floating point
	runNullCase(t, db, "FLOAT/float32", "float32", "FLOAT", float32(1.5), eqCmp[float32], 0, fbMajor)
	runNullCase(t, db, "DOUBLE_PRECISION/float64", "float64", "DOUBLE PRECISION", float64(3.5), eqCmp[float64], 0, fbMajor)

	// temporal — use component-wise equality for TIME (timezone offset is re-derived on read)
	runNullCase(t, db, "DATE/time.Time", "date", "DATE",
		time.Date(1967, 8, 11, 0, 0, 0, 0, time.Local),
		func(a, b time.Time) bool {
			ay, am, ad := a.Date()
			by, bm, bd := b.Date()
			return ay == by && am == bm && ad == bd
		}, 0, fbMajor)
	runNullCase(t, db, "TIME/time.Time", "time_t", "TIME",
		time.Date(0, 1, 1, 23, 45, 1, 0, time.Local),
		func(a, b time.Time) bool {
			return a.Hour() == b.Hour() && a.Minute() == b.Minute() &&
				a.Second() == b.Second() && a.Nanosecond() == b.Nanosecond()
		}, 0, fbMajor)
	runNullCase(t, db, "TIMESTAMP/time.Time", "ts", "TIMESTAMP",
		time.Date(1967, 8, 11, 23, 45, 1, 0, time.Local),
		func(a, b time.Time) bool { return a.Equal(b) }, 0, fbMajor)

	seoulLoc, err := time.LoadLocation("Asia/Seoul")
	require.NoError(t, err)
	runNullCase(t, db, "TIME_TZ/time.Time", "timetz", "TIME WITH TIME ZONE",
		time.Date(0, 1, 1, 23, 45, 1, 0, seoulLoc),
		func(a, b time.Time) bool {
			return a.Hour() == b.Hour() && a.Minute() == b.Minute() &&
				a.Second() == b.Second() && a.Nanosecond() == b.Nanosecond()
		}, 4, fbMajor)
	runNullCase(t, db, "TIMESTAMP_TZ/time.Time", "tstz", "TIMESTAMP WITH TIME ZONE",
		time.Date(1967, 8, 11, 23, 45, 1, 0, seoulLoc),
		func(a, b time.Time) bool { return a.Equal(b) }, 4, fbMajor)

	// string types
	runNullCase(t, db, "VARCHAR/string", "varchar", "VARCHAR(64)", "hello world", eqCmp[string], 0, fbMajor)
	// CHAR pads to fixed width; driver trims trailing spaces on read (xsqlvar.go:338)
	runNullCase(t, db, "CHAR/string", "char16", "CHAR(16)", "hi", eqCmp[string], 0, fbMajor)
	runNullCase(t, db, "VARCHAR_OCTETS/[]byte", "octets", "VARCHAR(64) CHARACTER SET OCTETS",
		[]byte{0x00, 0xff, 0x10, 0x42},
		func(a, b []byte) bool { return bytes.Equal(a, b) }, 0, fbMajor)

	// BLOBs — sub_type 1 (TEXT) decoded to string; sub_type 0 stays []byte (rows.go:120-128)
	runNullCase(t, db, "BLOB_TEXT/string", "blob_txt", "BLOB SUB_TYPE TEXT", "memo body", eqCmp[string], 0, fbMajor)
	runNullCase(t, db, "BLOB_BINARY/[]byte", "blob_bin", "BLOB SUB_TYPE BINARY",
		[]byte{0x01, 0x02, 0x03, 0xfe, 0xff},
		func(a, b []byte) bool { return bytes.Equal(a, b) }, 0, fbMajor)

	// BOOLEAN (Firebird 3+)
	runNullCase(t, db, "BOOLEAN/bool", "bool_col", "BOOLEAN", true, eqCmp[bool], 3, fbMajor)

	// INT128 (Firebird 4+) — write via string binding (server coerces); read into *big.Int.
	// Binding *big.Int directly is rejected by database/sql's DefaultParameterConverter.
	t.Run("INT128/big.Int", func(t *testing.T) {
		if fbMajor < 4 {
			t.Skip("requires Firebird 4+")
		}
		_, err := db.Exec(`CREATE TABLE tnull_int128 (id INTEGER NOT NULL PRIMARY KEY, v INT128)`)
		require.NoError(t, err)

		_, err = db.Exec(`INSERT INTO tnull_int128 (id, v) VALUES (1, ?)`, "170141183460469231731687303715884105727")
		require.NoError(t, err)
		_, err = db.Exec(`INSERT INTO tnull_int128 (id, v) VALUES (2, ?)`, nil)
		require.NoError(t, err)

		want := new(big.Int)
		want.SetString("170141183460469231731687303715884105727", 10)

		var got1 sql.Null[*big.Int]
		require.NoError(t, db.QueryRow(`SELECT v FROM tnull_int128 WHERE id=1`).Scan(&got1))
		assert.True(t, got1.Valid, "id=1: expected Valid=true")
		assert.Equal(t, 0, got1.V.Cmp(want), "id=1: value mismatch: got %v want %v", got1.V, want)

		var got2 sql.Null[*big.Int]
		require.NoError(t, db.QueryRow(`SELECT v FROM tnull_int128 WHERE id=2`).Scan(&got2))
		assert.False(t, got2.Valid, "id=2: expected Valid=false")
	})

	// DECFLOAT(34) (Firebird 4+) — driver returns decimal.Decimal (xsqlvar.go:453)
	runNullCase(t, db, "DECFLOAT34/decimal.Decimal", "dec34dec", "DECFLOAT(34)",
		decimal.RequireFromString("1.5"),
		func(a, b decimal.Decimal) bool { return a.Equal(b) }, 4, fbMajor)
	runNullCase(t, db, "DECFLOAT34/float64", "dec34f64", "DECFLOAT(34)", float64(1.5), eqCmp[float64], 4, fbMajor)
	runNullCase(t, db, "DECFLOAT34/string", "dec34str", "DECFLOAT(34)", "1.5", eqCmp[string], 4, fbMajor)

	// Asserted failure: a fractional DECFLOAT value cannot scan into sql.Null[int64]
	// because decimal.Decimal.Value() returns "1.1" and strconv.ParseInt("1.1") fails.
	// Use sql.Null[float64] or sql.Null[decimal.Decimal] instead.
	t.Run("DECFLOAT34/int64/rejects_fractional", func(t *testing.T) {
		if fbMajor < 4 {
			t.Skip("requires Firebird 4+")
		}
		_, err := db.Exec(`CREATE TABLE tnull_dec34neg (id INTEGER NOT NULL PRIMARY KEY, v DECFLOAT(34))`)
		require.NoError(t, err)
		_, err = db.Exec(`INSERT INTO tnull_dec34neg (id, v) VALUES (1, '1.1')`)
		require.NoError(t, err)
		var got sql.Null[int64]
		err = db.QueryRow(`SELECT v FROM tnull_dec34neg WHERE id=1`).Scan(&got)
		assert.Error(t, err, "fractional DECFLOAT should not scan into sql.Null[int64]")
	})
}
