package firebirdsql

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"math"
	"net"
	"testing"
	"time"
)

func recordItem(tag byte, n int64, width int) []byte {
	b := make([]byte, 3+width)
	b[0] = tag
	binary.LittleEndian.PutUint16(b[1:], uint16(width))
	if width == 4 {
		binary.LittleEndian.PutUint32(b[3:], uint32(n))
	} else if width == 8 {
		binary.LittleEndian.PutUint64(b[3:], uint64(n))
	}
	return b
}
func recordBlock(items ...[]byte) []byte {
	var data []byte
	for _, item := range items {
		data = append(data, item...)
	}
	data = append(data, isc_info_end)
	b := []byte{isc_info_sql_records, byte(len(data)), byte(len(data) >> 8)}
	b = append(b, data...)
	return append(b, isc_info_end)
}
func typicalRecords(width int) []byte {
	return recordBlock(recordItem(15, 3, width), recordItem(16, 5, width), recordItem(13, 7, width), recordItem(14, 11, width))
}

func TestStatementRecordsOrderingAndWidths(t *testing.T) {
	for _, width := range []int{4, 8} {
		for _, order := range [][4]byte{{13, 14, 15, 16}, {16, 13, 15, 14}, {14, 16, 13, 15}} {
			items := [][]byte{{90, 1, 0, 99}} // unknown nested TLV is skipped
			for _, tag := range order {
				items = append(items, recordItem(tag, int64(tag), width))
			}
			b := append([]byte{91, 2, 0, 1, 2}, recordBlock(items...)...)
			r, err := decodeStatementRecords(b)
			if err != nil || !r.available || r.selected != 13 || r.inserted != 14 || r.updated != 15 || r.deleted != 16 {
				t.Fatalf("%+v %v", r, err)
			}
			if count, e := r.rowsAffected(isc_info_sql_stmt_insert); e != nil || count != 45 {
				t.Fatalf("count=%d err=%v", count, e)
			}
			if count, e := r.rowsAffected(isc_info_sql_stmt_select); e != nil || count != 13 {
				t.Fatalf("select=%d err=%v", count, e)
			}
		}
	}
	b := recordBlock(recordItem(13, 0, 4), recordItem(14, math.MaxInt32, 4), recordItem(15, math.MaxInt32, 4), recordItem(16, 1<<40, 8))
	r, err := decodeStatementRecords(b)
	if err != nil {
		t.Fatal(err)
	}
	n, err := r.rowsAffected(isc_info_sql_stmt_update)
	if err != nil || n != 2*int64(math.MaxInt32)+(1<<40) {
		t.Fatalf("wide count=%d %v", n, err)
	}
}

func TestStatementRecordsMalformed(t *testing.T) {
	valid := typicalRecords(4)
	for i := 0; i < len(valid); i++ {
		if _, err := decodeStatementRecords(valid[:i]); err == nil {
			t.Fatalf("accepted prefix %d", i)
		}
	}
	for _, b := range [][]byte{
		{isc_info_truncated}, {isc_info_error, 0, 0},
		recordBlock(recordItem(13, 0, 4)),                       // missing required items
		recordBlock(recordItem(13, 0, 4), recordItem(13, 0, 4)), // duplicate
		recordBlock(recordItem(13, -1, 4)), recordBlock(recordItem(13, -1, 8)),
		recordBlock(recordItem(13, 0, 3)),
		append(append([]byte{}, valid[:len(valid)-1]...), valid...), // duplicate records block
	} {
		if _, err := decodeStatementRecords(b); err == nil {
			t.Fatalf("accepted %v", b)
		}
	}
	r := statementRecords{inserted: math.MaxInt64, updated: 1}
	if _, err := r.rowsAffected(isc_info_sql_stmt_insert); !errors.Is(err, errStatementRecordsOverflow) {
		t.Fatalf("sum overflow=%v", err)
	}
	r = statementRecords{inserted: math.MaxInt64, deleted: 1}
	if _, err := r.rowsAffected(isc_info_sql_stmt_delete); !errors.Is(err, errStatementRecordsOverflow) {
		t.Fatalf("sum overflow=%v", err)
	}
}

func TestStatementRecordsAbsentVersusZero(t *testing.T) {
	r, err := decodeStatementRecords([]byte{isc_info_end})
	if err != nil || r.available {
		t.Fatalf("absent=%+v %v", r, err)
	}
	n, err := r.rowsAffected(isc_info_sql_stmt_ddl)
	if err != nil || n != 0 {
		t.Fatalf("DDL count=%d %v", n, err)
	}
	r, err = decodeStatementRecords(recordBlock(recordItem(13, 0, 4), recordItem(14, 0, 4), recordItem(15, 0, 4), recordItem(16, 0, 4)))
	if err != nil || !r.available {
		t.Fatalf("zero=%+v %v", r, err)
	}
}

type recordDeadlineConn struct{ net.Conn }

func (recordDeadlineConn) SetDeadline(time.Time) error { return nil }

func TestStatementRecordsInvalidMetadataKeepsExecSuccess(t *testing.T) {
	for _, autocommit := range []bool{false, true} {
		var frames acceptFrame
		frames.opResponseFrame(0, nil)                                    // successful DML
		frames.opResponseFrame(0, []byte{isc_info_sql_records, 255, 255}) // malformed counts
		if autocommit {
			frames.opResponseFrame(0, nil)
		} // commit-retaining ack
		wp := testProtocol(frames.bytes())
		wp.conn.conn = recordDeadlineConn{}
		var written bytes.Buffer
		wp.conn.writer = bufio.NewWriter(&written)
		fc := &firebirdsqlConn{wp: wp, isAutocommit: autocommit}
		fc.tx = &firebirdsqlTx{fc: fc, isAutocommit: autocommit, transHandle: 1}
		stmt := &firebirdsqlStmt{fc: fc, stmtHandle: 2, stmtType: isc_info_sql_stmt_insert}
		result, err := stmt.exec(context.Background(), nil)
		if err != nil || result == nil {
			t.Fatalf("successful DML turned into an error: %v", err)
		}
		if _, err = result.RowsAffected(); !errors.Is(err, errStatementRecords) {
			t.Fatalf("count error lost: %v", err)
		}
		// Exactly one execute and one info request; autocommit still occurs once.
		b := written.Bytes()
		wantLen := 48
		if autocommit {
			wantLen += 8
		}
		if len(b) != wantLen || binary.BigEndian.Uint32(b[:4]) != op_execute || binary.BigEndian.Uint32(b[24:28]) != op_info_sql {
			t.Fatalf("unexpected command sequence: %x", b)
		}
		if autocommit && binary.BigEndian.Uint32(b[48:52]) != op_commit_retaining {
			t.Fatalf("commit skipped: %x", b)
		}
	}
}

func TestStatementRecordsLive(t *testing.T) {
	db, err := sql.Open("firebirdsql_createdb", GetTestDSN("record_counts_"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for _, tc := range []struct {
		sql   string
		count int64
	}{
		{"CREATE TABLE RECORD_COUNTS (ID INTEGER)", 0},
		{"INSERT INTO RECORD_COUNTS VALUES (1)", 1},
		{"INSERT INTO RECORD_COUNTS SELECT 2 FROM RDB$DATABASE", 1},
		{"UPDATE RECORD_COUNTS SET ID=ID+1", 2},
		{"UPDATE RECORD_COUNTS SET ID=0 WHERE ID<0", 0},
		{"DELETE FROM RECORD_COUNTS", 2},
	} {
		result, e := conn.ExecContext(context.Background(), tc.sql)
		if e != nil {
			t.Fatal(e)
		}
		count, e := result.RowsAffected()
		if e != nil || count != tc.count {
			t.Fatalf("%s: count=%d want=%d err=%v", tc.sql, count, tc.count, e)
		}
	}
	tx, err := conn.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	stmt, err := tx.Prepare("INSERT INTO RECORD_COUNTS VALUES (?)")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		result, e := stmt.Exec(i)
		if e != nil {
			t.Fatal(e)
		}
		n, e := result.RowsAffected()
		if e != nil || n != 1 {
			t.Fatalf("prepared count=%d %v", n, e)
		}
	}
	_ = stmt.Close()
	if err = tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	var remaining int
	if err = conn.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM RECORD_COUNTS").Scan(&remaining); err != nil || remaining != 0 {
		t.Fatalf("rollback changed: %d %v", remaining, err)
	}
}

func FuzzStatementRecords(f *testing.F) {
	f.Add(typicalRecords(4))
	f.Add(typicalRecords(8))
	f.Add([]byte{isc_info_end})
	f.Fuzz(func(t *testing.T, b []byte) {
		r, err := decodeStatementRecords(b)
		if err == nil {
			n, e := r.rowsAffected(isc_info_sql_stmt_insert)
			if e == nil && n < 0 {
				t.Fatal("negative count")
			}
		}
	})
}

func BenchmarkStatementRecords(b *testing.B) {
	buf := typicalRecords(4)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := decodeStatementRecords(buf); err != nil {
			b.Fatal(err)
		}
	}
}
