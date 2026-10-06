package firebirdsql

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Live tests for Firebird array columns: op_put_slice / op_get_slice round
// trips through the database/sql surface.

const arrayTestDDLBase = `CREATE TABLE ARR_T (
	ID INTEGER NOT NULL PRIMARY KEY,
	INTS INTEGER[1:5],
	BIGS BIGINT[1:4],
	SMALLS SMALLINT[0:2],
	NUMS NUMERIC(15,2)[1:3],
	FLTS FLOAT[1:3],
	DBLS DOUBLE PRECISION[1:3],
	STRS VARCHAR(10)[1:4],
	CHARS CHAR(4)[1:3],
	STAMPS TIMESTAMP[1:2],
	GRID INTEGER[0:2,0:2]`

func openArrayTestDB(t *testing.T) *sql.DB {
	t.Helper()
	var ddl string
	if testServerVersion(t).EqualOrGreater(3, 0) {
		ddl = arrayTestDDLBase + ",\n\tFLAGS BOOLEAN[1:3]\n)"
	} else {
		// BOOLEAN (and thus BOOLEAN arrays) does not exist before FB3.
		ddl = arrayTestDDLBase + "\n)"
	}
	db, _, _ := createTestDatabaseWithDDL(t, "arr_", ddl)
	return db
}

func TestArrayRoundTripIntegers(t *testing.T) {
	db := openArrayTestDB(t)
	ctx := context.Background()

	_, err := db.ExecContext(ctx,
		"INSERT INTO ARR_T (ID, INTS, BIGS, SMALLS) VALUES (?, ?, ?, ?)",
		1, FlatArray[int32]{10, 20, 30}, FlatArray[int64]{900000000000, -2}, FlatArray[int64]{7, 8, 9})
	require.NoError(t, err)

	var ints FlatArray[int64]
	var bigs FlatArray[int64]
	var smalls FlatArray[int64]
	require.NoError(t, db.QueryRowContext(ctx,
		"SELECT INTS, BIGS, SMALLS FROM ARR_T WHERE ID = 1").Scan(&ints, &bigs, &smalls))
	// Partial writes zero-fill the tail; reads return the declared shape.
	require.Equal(t, FlatArray[int64]{10, 20, 30, 0, 0}, ints)
	require.Equal(t, FlatArray[int64]{900000000000, -2, 0, 0}, bigs)
	require.Equal(t, FlatArray[int64]{7, 8, 9}, smalls)
}

func TestArrayRoundTripPlainSliceParam(t *testing.T) {
	db := openArrayTestDB(t)
	ctx := context.Background()

	// Plain []int64 goes through CheckNamedValue → ArrayValue.
	_, err := db.ExecContext(ctx, "INSERT INTO ARR_T (ID, INTS) VALUES (?, ?)", 1, []int64{5, 6, 7, 8})
	require.NoError(t, err)

	var ints FlatArray[int64]
	require.NoError(t, db.QueryRowContext(ctx, "SELECT INTS FROM ARR_T WHERE ID = 1").Scan(&ints))
	require.Equal(t, FlatArray[int64]{5, 6, 7, 8, 0}, ints)
}

func TestArrayDimensionedRoundTrip(t *testing.T) {
	db := openArrayTestDB(t)
	ctx := context.Background()

	in := Array[int64]{
		Elements: []int64{1, 2, 3, 4, 5, 6, 7, 8, 9},
		Dims:     []ArrayDimension{{LowerBound: 0, Length: 3}, {LowerBound: 0, Length: 3}},
		Valid:    true,
	}
	_, err := db.ExecContext(ctx, "INSERT INTO ARR_T (ID, GRID) VALUES (?, ?)", 1, in)
	require.NoError(t, err)

	var out Array[int64]
	require.NoError(t, db.QueryRowContext(ctx, "SELECT GRID FROM ARR_T WHERE ID = 1").Scan(&out))
	require.True(t, out.Valid)
	require.Equal(t, in.Dims, out.Dims)
	require.Equal(t, in.Elements, out.Elements)
}

func TestArrayPartialWrite(t *testing.T) {
	db := openArrayTestDB(t)
	ctx := context.Background()

	// Write fewer elements than the declared shape: the driver fills the
	// prefix, the server zero-fills the tail. The read side sees the declared
	// element count with zero padding (documented driver semantics).
	_, err := db.ExecContext(ctx, "INSERT INTO ARR_T (ID, INTS) VALUES (?, ?)", 1, FlatArray[int64]{42, 43})
	require.NoError(t, err)

	var ints FlatArray[int64]
	require.NoError(t, db.QueryRowContext(ctx, "SELECT INTS FROM ARR_T WHERE ID = 1").Scan(&ints))
	require.Equal(t, FlatArray[int64]{42, 43, 0, 0, 0}, ints)
}

func TestArrayNull(t *testing.T) {
	db := openArrayTestDB(t)
	ctx := context.Background()

	_, err := db.ExecContext(ctx, "INSERT INTO ARR_T (ID, INTS, BIGS) VALUES (?, ?, ?)", 1, nil, FlatArray[int64]{1})
	require.NoError(t, err)

	var ints FlatArray[int64]
	var bigs FlatArray[int64]
	require.NoError(t, db.QueryRowContext(ctx, "SELECT INTS, BIGS FROM ARR_T WHERE ID = 1").Scan(&ints, &bigs))
	require.Empty(t, ints) // NULL array scans as nil/empty
	require.Equal(t, FlatArray[int64]{1, 0, 0, 0}, bigs)

	// NULL array read as invalid Array (BOOLEAN column: FB3+ only).
	if testServerVersion(t).EqualOrGreater(3, 0) {
		var dims Array[int64]
		require.NoError(t, db.QueryRowContext(ctx, "SELECT FLAGS FROM ARR_T WHERE ID = 1").Scan(&dims))
		require.False(t, dims.Valid)
	}
}

func TestArrayRoundTripText(t *testing.T) {
	db := openArrayTestDB(t)
	ctx := context.Background()

	_, err := db.ExecContext(ctx, "INSERT INTO ARR_T (ID, STRS, CHARS) VALUES (?, ?, ?)",
		1, FlatArray[string]{"alpha", "be", "c"}, FlatArray[string]{"abcd", "xy"})
	require.NoError(t, err)

	var strs FlatArray[string]
	var chars FlatArray[string]
	require.NoError(t, db.QueryRowContext(ctx, "SELECT STRS, CHARS FROM ARR_T WHERE ID = 1").Scan(&strs, &chars))
	require.Equal(t, FlatArray[string]{"alpha", "be", "c", ""}, strs)
	// CHAR(n) elements come back space-trimmed like scalar CHAR columns;
	// the missing tail element decodes as the empty string.
	require.Equal(t, FlatArray[string]{"abcd", "xy", ""}, chars)
}

func TestArrayRoundTripFloats(t *testing.T) {
	db := openArrayTestDB(t)
	ctx := context.Background()

	_, err := db.ExecContext(ctx, "INSERT INTO ARR_T (ID, FLTS, DBLS, NUMS) VALUES (?, ?, ?, ?)",
		1, FlatArray[float64]{1.5, 2.25, -3.75}, FlatArray[float64]{1.1, 2.2, 3.3}, FlatArray[string]{"10.50", "2.00"})
	require.NoError(t, err)

	var flts FlatArray[float64]
	var dbls FlatArray[float64]
	var nums FlatArray[string]
	require.NoError(t, db.QueryRowContext(ctx, "SELECT FLTS, DBLS, NUMS FROM ARR_T WHERE ID = 1").Scan(&flts, &dbls, &nums))
	require.InDeltaSlice(t, []float64{1.5, 2.25, -3.75}, flts, 0.0001)
	require.Equal(t, FlatArray[float64]{1.1, 2.2, 3.3}, dbls)
	require.Equal(t, FlatArray[string]{"10.50", "2.00", "0.00"}, nums)
}

func TestArrayRoundTripTimestamps(t *testing.T) {
	db := openArrayTestDB(t)
	ctx := context.Background()

	stamp := time.Date(2026, 9, 12, 13, 45, 10, 0, time.UTC)
	_, err := db.ExecContext(ctx, "INSERT INTO ARR_T (ID, STAMPS) VALUES (?, ?)",
		1, FlatArray[time.Time]{stamp})
	require.NoError(t, err)

	var stamps FlatArray[time.Time]
	require.NoError(t, db.QueryRowContext(ctx, "SELECT STAMPS FROM ARR_T WHERE ID = 1").Scan(&stamps))
	require.Len(t, stamps, 2)
	// TIMESTAMP elements are wall-clock values decoded in the connection
	// timezone (same as scalar TIMESTAMP columns); compare wall clock.
	require.Equal(t, stamp.Format("2006-01-02 15:04:05"), stamps[0].Format("2006-01-02 15:04:05"))
}

func TestArrayRoundTripBoolean(t *testing.T) {
	requireBooleanSupport(t)
	db := openArrayTestDB(t)
	ctx := context.Background()

	_, err := db.ExecContext(ctx, "INSERT INTO ARR_T (ID, FLAGS) VALUES (?, ?)", 1, FlatArray[bool]{true, false, true})
	require.NoError(t, err)

	var flags FlatArray[bool]
	require.NoError(t, db.QueryRowContext(ctx, "SELECT FLAGS FROM ARR_T WHERE ID = 1").Scan(&flags))
	require.Equal(t, FlatArray[bool]{true, false, true}, flags)
}

func TestArrayPreparedReuse(t *testing.T) {
	db := openArrayTestDB(t)
	ctx := context.Background()

	stmt, err := db.PrepareContext(ctx, "INSERT INTO ARR_T (ID, INTS) VALUES (?, ?)")
	require.NoError(t, err)
	defer stmt.Close()

	for i := int64(1); i <= 3; i++ {
		_, err = stmt.ExecContext(ctx, i, FlatArray[int64]{i, i * 2})
		require.NoError(t, err)
	}
	require.NoError(t, stmt.Close())

	var ints FlatArray[int64]
	require.NoError(t, db.QueryRowContext(ctx, "SELECT INTS FROM ARR_T WHERE ID = 2").Scan(&ints))
	require.Equal(t, FlatArray[int64]{2, 4, 0, 0, 0}, ints)
}

func TestArrayInTransaction(t *testing.T) {
	db := openArrayTestDB(t)
	ctx := context.Background()

	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, "INSERT INTO ARR_T (ID, INTS) VALUES (?, ?)", 1, FlatArray[int64]{7, 8})
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	var ints FlatArray[int64]
	require.NoError(t, db.QueryRowContext(ctx, "SELECT INTS FROM ARR_T WHERE ID = 1").Scan(&ints))
	require.Equal(t, FlatArray[int64]{7, 8, 0, 0, 0}, ints)
}

func TestArrayErrors(t *testing.T) {
	db := openArrayTestDB(t)
	ctx := context.Background()

	// Too many elements for the declared shape.
	_, err := db.ExecContext(ctx, "INSERT INTO ARR_T (ID, INTS) VALUES (?, ?)",
		1, FlatArray[int64]{1, 2, 3, 4, 5, 6})
	require.Error(t, err)
	require.Contains(t, err.Error(), "but the column holds")

	// NULL elements are not representable.
	_, err = db.ExecContext(ctx, "INSERT INTO ARR_T (ID, STRS) VALUES (?, ?)",
		1, FlatArray[string]{"a", "", "c"})
	require.NoError(t, err) // empty string is fine...

	_, err = db.ExecContext(ctx, "INSERT INTO ARR_T (ID, STAMPS) VALUES (?, ?)",
		1, FlatArray[time.Time]{{}, {}, {}, {}})
	require.Error(t, err)
	require.Contains(t, err.Error(), "but the column holds")

	// VARCHAR element overflow.
	_, err = db.ExecContext(ctx, "INSERT INTO ARR_T (ID, STRS) VALUES (?, ?)",
		1, FlatArray[string]{"this is way over ten characters"})
	require.Error(t, err)

	// Ragged multi-dimensional argument.
	_, err = db.ExecContext(ctx, "INSERT INTO ARR_T (ID, GRID) VALUES (?, ?)",
		1, [][]int64{{1, 2}, {1}})
	require.Error(t, err)
	require.Contains(t, err.Error(), "ragged")

	// Plain slices cannot be scan targets (database/sql conversion).
	var plain []int64
	_, err = db.ExecContext(ctx, "INSERT INTO ARR_T (ID, INTS) VALUES (?, ?)", 9, FlatArray[int64]{1})
	require.NoError(t, err)
	require.Error(t, db.QueryRowContext(ctx, "SELECT INTS FROM ARR_T WHERE ID = 9").Scan(&plain))
}

func TestArrayColumnMetadata(t *testing.T) {
	db := openArrayTestDB(t)
	ctx := context.Background()

	rows, err := db.QueryContext(ctx, "SELECT INTS, GRID FROM ARR_T")
	require.NoError(t, err)
	defer rows.Close()

	cts, err := rows.ColumnTypes()
	require.NoError(t, err)
	require.Equal(t, "ARRAY", cts[0].DatabaseTypeName())
	require.Equal(t, "ARRAY", cts[1].DatabaseTypeName())
	require.NoError(t, rows.Close())
}
