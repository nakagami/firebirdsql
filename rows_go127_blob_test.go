//go:build go1.27

package firebirdsql

import (
	"context"
	"database/sql/driver"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

// Supply the replies to opening, reading and closing a real (non-NULL) BLOB.
// Two segments in the final response exercise assembly into the scan value.
func columnScanBlobFrames(f *acceptFrame, data []byte) {
	f.opResponseFrame(1, nil)
	var segments []byte
	for _, part := range [][]byte{data[:len(data)/2], data[len(data)/2:]} {
		segments = append(segments, byte(len(part)), byte(len(part)>>8))
		segments = append(segments, part...)
	}
	f.opResponseFrame(2, segments)
	f.opResponseFrame(0, nil)
}

func cannedColumnScanBlobRows(frame []byte, source [][]driver.Value, subtype int, procedure bool) *firebirdsqlRows {
	wp := testProtocol(frame)
	wp.charset = "UTF8"
	rows := &firebirdsqlRows{
		ctx: context.Background(),
		stmt: &firebirdsqlStmt{
			stmtType:     isc_info_sql_stmt_select,
			fc:           &firebirdsqlConn{wp: wp, tx: &firebirdsqlTx{transHandle: 1}},
			resultXsqlda: []xSQLVAR{{sqltype: SQL_TYPE_BLOB, sqlsubtype: subtype}},
		},
		currentChunk:    source,
		currentChunkIdx: -1,
	}
	if procedure {
		rows.stmt.stmtType = isc_info_sql_stmt_exec_procedure
		rows.currentChunk = nil
		rows.result = source[0]
	}
	return rows
}

func TestColumnScanBlobMaterialization(t *testing.T) {
	for _, subtype := range []int{0, 1} {
		t.Run(fmt.Sprintf("subtype=%d", subtype), func(t *testing.T) {
			firstID := []byte{1, 0, 0, 0, 0, 0, 0, 0}
			secondID := []byte{2, 0, 0, 0, 0, 0, 0, 0}
			source := [][]driver.Value{{append([]byte(nil), firstID...)}, {append([]byte(nil), secondID...)}}
			first := []byte("first BLOB contents")
			second := []byte("a different second BLOB")
			var f acceptFrame
			columnScanBlobFrames(&f, first)
			columnScanBlobFrames(&f, second)
			f.opResponseFrame(99, nil) // unchanged by repeated ScanColumn calls
			rows := cannedColumnScanBlobRows(f.bytes(), source, subtype, false)

			require.NoError(t, rows.NextRow())
			require.Equal(t, firstID, source[0][0], "materialization must not replace a chunk's BLOB id")
			require.NotSame(t, &source[0][0], &rows.scanRow[0])
			buffer := &rows.scanRow[0]
			var retained, repeated []byte
			var retainedAny any
			require.NoError(t, rows.ScanColumn(driver.ScanContext{}, 0, &retained))
			require.NoError(t, rows.ScanColumn(driver.ScanContext{}, 0, &retainedAny))
			require.NoError(t, rows.ScanColumn(driver.ScanContext{}, 0, &repeated))
			require.Equal(t, first, retained)
			require.Equal(t, first, repeated)
			repeated[0] = '!'
			var again []byte
			require.NoError(t, rows.ScanColumn(driver.ScanContext{}, 0, &again))
			require.Equal(t, first, again, "byte destinations must not alias the materialized value")

			require.NoError(t, rows.NextRow())
			require.Same(t, buffer, &rows.scanRow[0], "reuse one destination row")
			require.Equal(t, firstID, source[0][0])
			require.Equal(t, secondID, source[1][0])
			var got []byte
			require.NoError(t, rows.ScanColumn(driver.ScanContext{}, 0, &got))
			require.Equal(t, second, got)
			require.Equal(t, first, retained, "advancing must not overwrite an earlier owned value")
			if subtype == 0 {
				require.Equal(t, first, retainedAny)
			} else {
				require.Equal(t, string(first), retainedAny)
			}
			require.ErrorIs(t, rows.NextRow(), io.EOF)
			handle, _, _, err := rows.stmt.fc.wp.opResponse()
			require.NoError(t, err)
			require.Equal(t, int32(99), handle, "repeated scans must not issue additional BLOB reads")
		})
	}
}

func TestColumnScanProcedureBlobMaterialization(t *testing.T) {
	id := []byte{3, 0, 0, 0, 0, 0, 0, 0}
	source := [][]driver.Value{{append([]byte(nil), id...)}}
	payload := []byte("procedure BLOB contents")
	var f acceptFrame
	columnScanBlobFrames(&f, payload)
	rows := cannedColumnScanBlobRows(f.bytes(), source, 0, true)
	require.NoError(t, rows.NextRow())
	require.Equal(t, id, source[0][0])
	var got, repeated []byte
	require.NoError(t, rows.ScanColumn(driver.ScanContext{}, 0, &got))
	require.NoError(t, rows.ScanColumn(driver.ScanContext{}, 0, &repeated))
	require.Equal(t, payload, got)
	require.Equal(t, payload, repeated)
	require.ErrorIs(t, rows.NextRow(), io.EOF)
	require.Equal(t, payload, got)
}

func TestColumnScanBlobMaterializationErrors(t *testing.T) {
	for _, procedure := range []bool{false, true} {
		for _, malformed := range []bool{false, true} {
			t.Run(fmt.Sprintf("procedure=%t/malformed=%t", procedure, malformed), func(t *testing.T) {
				id := []byte{4, 0, 0, 0, 0, 0, 0, 0}
				source := [][]driver.Value{{append([]byte(nil), id...)}}
				var f acceptFrame
				f.opResponseFrame(1, nil)
				if malformed {
					f.opResponseFrame(2, []byte{5, 0, 'a'})
				} else {
					f.opResponseFrame(0, nil, isc_arg_gds, 335544336)
				}
				rows := cannedColumnScanBlobRows(f.bytes(), source, 0, procedure)
				err := rows.NextRow()
				require.Error(t, err, "BLOB read errors must surface during NextRow")
				require.Equal(t, id, source[0][0])
				if malformed {
					require.ErrorIs(t, err, driver.ErrBadConn)
					require.ErrorContains(t, err, "exceeds response")
					require.True(t, rows.badConn)
				} else {
					var serverErr *FbError
					require.ErrorAs(t, err, &serverErr)
					require.NotErrorIs(t, err, driver.ErrBadConn)
					require.False(t, rows.badConn, "a complete server error frame keeps the connection synchronized")
				}
			})
		}
	}
}
