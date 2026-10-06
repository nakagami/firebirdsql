/*******************************************************************************
The MIT License (MIT)

Copyright (c) 2013-2025 Hajime Nakagami

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
	"bufio"
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"
)

// Unit tests for the end-of-transaction reads (no server): a wireProtocol over canned
// replies, with the written bytes captured.

func txEndTestConn(replies []byte) (*firebirdsqlConn, *bytes.Buffer) {
	wp := testProtocol(replies)
	wp.conn.conn = recordDeadlineConn{}
	var written bytes.Buffer
	wp.conn.writer = bufio.NewWriter(&written)
	fc := &firebirdsqlConn{wp: wp, isAutocommit: true, transactionSet: map[*firebirdsqlTx]struct{}{}}
	fc.tx = &firebirdsqlTx{fc: fc, isAutocommit: true, transHandle: 1}
	fc.transactionSet[fc.tx] = struct{}{}
	return fc, &written
}

// refusedGDS is the GDS code the tests use for a request the server refuses; which
// error it is does not matter.
const refusedGDS = ISCUniqueKeyViolation

func firstOpcode(b []byte) int32 {
	if len(b) < 4 {
		return -1
	}
	return int32(binary.BigEndian.Uint32(b[:4]))
}

// writtenOpcodes lists the opcodes of the fixed-size packets these tests write:
// op_commit/op_commit_retaining/op_rollback (8 bytes) and op_free_statement (12).
func writtenOpcodes(t *testing.T, b []byte) []int32 {
	t.Helper()
	var ops []int32
	for len(b) >= 4 {
		op := firstOpcode(b)
		ops = append(ops, op)
		switch op {
		case op_commit, op_commit_retaining, op_rollback:
			b = b[8:]
		case op_free_statement:
			b = b[12:]
		default:
			return ops // a variable-size packet (op_execute, ...): stop here
		}
	}
	return ops
}

// pastDeadlineCtx reports a deadline already passed while Err is still nil and Done never
// closes: the window where the wall clock is ahead of the context's timer.
type pastDeadlineCtx struct{ context.Context }

func (pastDeadlineCtx) Deadline() (time.Time, bool) { return time.Now().Add(-time.Second), true }

// A commit reply the parser rejects (tagged driver.ErrBadConn, #279) arrives after the
// statement executed: database/sql must not retry it, the conn must still be discarded.
func TestCommitRetainingMalformedReplyIsNotRetried(t *testing.T) {
	var f acceptFrame
	f.opResponseFrame(0, nil, isc_arg_gds, 335544665, isc_arg_string, 1<<30) // string length out of range
	fc, _ := txEndTestConn(f.bytes())

	err := fc.tx.commitRetaining(context.Background())
	if err == nil {
		t.Fatal("commitRetaining succeeded on a malformed reply")
	}
	if errors.Is(err, driver.ErrBadConn) {
		t.Fatalf("err = %v carries driver.ErrBadConn: database/sql would run the statement again", err)
	}
	if fc.IsValid() {
		t.Fatal("IsValid() = true after an unread commit reply; the conn would be pooled")
	}
}

// With the context already done database/sql cannot retry, so the tag stays.
func TestCommitRetainingDoneContextKeepsBadConn(t *testing.T) {
	var f acceptFrame
	f.opResponseFrame(0, nil, isc_arg_gds, 335544665, isc_arg_string, 1<<30)
	fc, _ := txEndTestConn(f.bytes())

	err := fc.tx.commitRetaining(teardownCtx)
	if !errors.Is(err, driver.ErrBadConn) {
		t.Fatalf("err = %v, want driver.ErrBadConn", err)
	}
	if fc.IsValid() {
		t.Fatal("IsValid() = true after an unread commit reply")
	}
}

// A commit the server refuses leaves Firebird's transaction active with its work. It must
// be rolled back, or a later commit on the connection would commit work the caller was
// told had failed. The server error is a reply read in full: the wire stays usable.
func TestRefusedCommitRollsBack(t *testing.T) {
	for name, tc := range map[string]struct {
		commit func(*firebirdsqlTx) error
		op     int32
	}{
		"commit-retaining live":     {func(tx *firebirdsqlTx) error { return tx.commitRetaining(context.Background()) }, op_commit_retaining},
		"commit-retaining teardown": {func(tx *firebirdsqlTx) error { return tx.commitRetaining(teardownCtx) }, op_commit_retaining},
		"Tx.Commit":                 {(*firebirdsqlTx).Commit, op_commit},
	} {
		t.Run(name, func(t *testing.T) {
			var f acceptFrame
			f.opResponseFrame(0, nil, isc_arg_gds, refusedGDS) // commit refused
			f.opResponseFrame(0, nil)                          // rollback ack
			fc, written := txEndTestConn(f.bytes())

			err := tc.commit(fc.tx)
			if !isServerError(err) {
				t.Fatalf("err = %v, want the commit's *FbError", err)
			}
			if ops := writtenOpcodes(t, written.Bytes()); len(ops) != 2 || ops[0] != tc.op || ops[1] != op_rollback {
				t.Fatalf("written opcodes = %v, want [%d %d] (commit, then rollback)", ops, tc.op, op_rollback)
			}
			if !fc.tx.needBegin {
				t.Fatal("needBegin = false: the rolled-back transaction would be reused")
			}
			if !fc.IsValid() {
				t.Fatal("IsValid() = false; both replies were read")
			}
		})
	}
}

// When the server refuses the rollback too, it still holds the transaction; reusing fc.tx
// would overwrite the handle and leak it, so the connection must be dropped.
func TestRefusedRollbackDropsConn(t *testing.T) {
	var f acceptFrame
	f.opResponseFrame(0, nil, isc_arg_gds, refusedGDS)
	fc, _ := txEndTestConn(f.bytes())

	err := fc.tx.rollback(context.Background())
	if !isServerError(err) {
		t.Fatalf("err = %v, want *FbError", err)
	}
	if fc.IsValid() {
		t.Fatal("IsValid() = true; the transaction the server still holds would leak")
	}
	if _, ok := fc.transactionSet[fc.tx]; !ok {
		t.Fatal("transaction that failed to end left the set")
	}
}

// noRetryAfterExec takes driver.ErrBadConn out of the chain and keeps the rest.
func TestNoRetryAfterExecKeepsChain(t *testing.T) {
	err := noRetryAfterExec(context.Background(), fmt.Errorf("read: %w: %w", os.ErrDeadlineExceeded, fmt.Errorf("%w: %w", context.DeadlineExceeded, driver.ErrBadConn)))
	if errors.Is(err, driver.ErrBadConn) {
		t.Fatalf("err = %v still carries driver.ErrBadConn", err)
	}
	if !errors.Is(err, os.ErrDeadlineExceeded) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v lost the rest of its chain", err)
	}
	if !isServerError(noRetryAfterExec(context.Background(), errors.Join(&FbError{Message: "x"}, driver.ErrBadConn))) {
		t.Fatal("*FbError lost from the chain")
	}
	if err := noRetryAfterExec(teardownCtx, driver.ErrBadConn); err != driver.ErrBadConn {
		t.Fatalf("done ctx: err = %v, want driver.ErrBadConn kept", err)
	}
}

// The statement ran, but ctx ended around its reply: exec reports an error, so the work
// must not be committed. Before, fc.exec's deferred Stmt.Close committed it anyway, and
// with the wall clock ahead of the context's timer database/sql also ran it again.
func TestExecAbandonedAfterContextIsNotCommitted(t *testing.T) {
	var f acceptFrame
	f.opResponseFrame(0, nil) // the execute succeeded
	fc, written := txEndTestConn(f.bytes())
	fc.tx.needBegin = false
	stmt := &firebirdsqlStmt{fc: fc, stmtHandle: 2, stmtType: isc_info_sql_stmt_insert}

	start := time.Now()
	_, err := stmt.exec(pastDeadlineCtx{context.Background()}, nil)
	if !errors.Is(err, driver.ErrBadConn) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("exec err = %v, want DeadlineExceeded tagged driver.ErrBadConn", err)
	}
	if elapsed := time.Since(start); elapsed < 900*time.Millisecond {
		t.Fatalf("exec tagged ErrBadConn after %v, before giving the context's timer its second", elapsed)
	}
	if fc.IsValid() {
		t.Fatal("IsValid() = true after an abandoned statement")
	}
	written.Reset()
	if err := stmt.Close(); !errors.Is(err, driver.ErrBadConn) {
		t.Fatalf("Close err = %v, want the desynced-wire refusal", err)
	}
	fc.wp.conn.writer.Flush()
	if written.Len() != 0 {
		t.Fatalf("Stmt.Close wrote %x after the abandon; it must not free or commit", written.Bytes())
	}
}

// rows.Close after a fetch abandoned partway must not write to the wire (no free, no
// commit) and must report ErrBadConn so database/sql drops the connection.
func TestRowsCloseAfterAbandonedFetch(t *testing.T) {
	fc, written := txEndTestConn(nil)
	stmt := &firebirdsqlStmt{fc: fc, stmtHandle: 2, stmtType: isc_info_sql_stmt_select}
	rows := &firebirdsqlRows{stmt: stmt, ctx: context.Background(), closeStmtOnClose: true}
	rows.readFailed(os.ErrDeadlineExceeded) // the OS deadline cut the fetch reply short

	if err := rows.Close(); !errors.Is(err, driver.ErrBadConn) {
		t.Fatalf("Close err = %v, want driver.ErrBadConn", err)
	}
	fc.wp.conn.writer.Flush()
	if written.Len() != 0 {
		t.Fatalf("rows.Close wrote %x to an out-of-step wire", written.Bytes())
	}
	if fc.IsValid() {
		t.Fatal("IsValid() = true after an abandoned fetch")
	}
}

// opResponseTimeout flags the wire on any failure that is not a server reply, including
// a timeout inside opResponse's lazy drain, which surfaces as an unexpected opcode.
func TestOpResponseTimeoutFlagsDesynced(t *testing.T) {
	var lazy acceptFrame
	lazy.opResponseFrame(0, nil) // the pending lazy ack, then nothing: the next read fails
	for name, tc := range map[string]struct {
		replies []byte
		lazy    int
	}{
		"truncated":  {replies: []byte{0, 0}},
		"lazy drain": {replies: lazy.bytes(), lazy: 1},
	} {
		t.Run(name, func(t *testing.T) {
			fc, _ := txEndTestConn(tc.replies)
			fc.wp.lazyResponseCount = tc.lazy
			if _, _, _, err := fc.wp.opResponseTimeout(abandonReadTimeout); err == nil {
				t.Fatal("opResponseTimeout succeeded on a missing reply")
			}
			if !fc.wp.desynced {
				t.Fatal("desynced not set")
			}
		})
	}
}

// database/sql checks the BeginTx context before calling Commit, but it can end right
// after. Commit must then roll back (database/sql has marked the Tx done and will not)
// and report the context error, leaving the wire in sync.
func TestCommitAfterContextEndedRollsBack(t *testing.T) {
	var f acceptFrame
	f.opResponseFrame(0, nil) // rollback ack
	fc, written := txEndTestConn(f.bytes())
	fc.tx.isAutocommit = false
	fc.tx.ctx = teardownCtx

	err := fc.tx.Commit()
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Commit err = %v, want context.Canceled", err)
	}
	if op := firstOpcode(written.Bytes()); op != op_rollback {
		t.Fatalf("first opcode written = %d, want op_rollback (%d)", op, op_rollback)
	}
	if !fc.tx.needBegin {
		t.Fatal("needBegin = false: the rolled-back handle would be reused")
	}
	if _, ok := fc.transactionSet[fc.tx]; ok {
		t.Fatal("ended transaction left in transactionSet")
	}
	if !fc.IsValid() {
		t.Fatal("IsValid() = false; the rollback reply was read")
	}
}

// boundByContext must run fn even when ctx is already done: the request is on the wire
// and returning early would leave its reply unread on a conn that looks healthy.
func TestBoundByContextRunsOnDoneContext(t *testing.T) {
	fc, _ := txEndTestConn(nil)
	ran := false
	err := fc.wp.boundByContext(teardownCtx, func() error {
		ran = true
		return nil
	})
	if !ran || err != nil {
		t.Fatalf("ran = %v, err = %v; want fn run and its nil result", ran, err)
	}
}

// Every call that starts new work refuses a desynced wire before writing anything, with
// an error database/sql may retry on another connection.
func TestEntryPointsRefuseDesyncedWire(t *testing.T) {
	ctx := context.Background()
	for name, call := range map[string]func(fc *firebirdsqlConn) error{
		"prepare": func(fc *firebirdsqlConn) error { _, err := fc.prepare(ctx, "select 1 from rdb$database"); return err },
		"exec": func(fc *firebirdsqlConn) error {
			_, err := (&firebirdsqlStmt{fc: fc, stmtHandle: 2}).exec(ctx, nil)
			return err
		},
		"query": func(fc *firebirdsqlConn) error {
			_, err := (&firebirdsqlStmt{fc: fc, stmtHandle: 2}).query(ctx, nil)
			return err
		},
		"BeginTx":          func(fc *firebirdsqlConn) error { _, err := fc.BeginTx(ctx, driver.TxOptions{}); return err },
		"Ping":             func(fc *firebirdsqlConn) error { return fc.Ping(ctx) },
		"ExecImmediate":    func(fc *firebirdsqlConn) error { return fc.ExecImmediate(ctx, "select 1 from rdb$database") },
		"PrepareBatch":     func(fc *firebirdsqlConn) error { _, err := fc.PrepareBatch(ctx, "x", BatchOptions{}); return err },
		"QueryScrollable":  func(fc *firebirdsqlConn) error { _, err := fc.QueryScrollable(ctx, "x"); return err },
		"Commit":           func(fc *firebirdsqlConn) error { return fc.tx.Commit() },
		"Rollback":         func(fc *firebirdsqlConn) error { return fc.tx.Rollback() },
		"commit-retaining": func(fc *firebirdsqlConn) error { return fc.tx.commitRetaining(teardownCtx) },
		"batch Flush": func(fc *firebirdsqlConn) error {
			return (&PreparedBatch{fc: fc, encodedRows: [][]byte{{0}}}).Flush(ctx)
		},
		"batch Exec": func(fc *firebirdsqlConn) error {
			_, err := (&PreparedBatch{fc: fc, created: true, stmt: &firebirdsqlStmt{fc: fc, stmtHandle: 2}}).Exec(ctx)
			return err
		},
		"batch Cancel": func(fc *firebirdsqlConn) error {
			return (&PreparedBatch{fc: fc, created: true, stmt: &firebirdsqlStmt{fc: fc, stmtHandle: 2}}).Cancel(ctx)
		},
		"scroll Fetch": func(fc *firebirdsqlConn) error {
			_, _, err := (&ScrollableStmt{stmt: &firebirdsqlStmt{fc: fc, stmtHandle: 2}}).Fetch(ScrollNext, 0, 1)
			return err
		},
		"rows fetch": func(fc *firebirdsqlConn) error {
			stmt := &firebirdsqlStmt{fc: fc, stmtHandle: 2, stmtType: isc_info_sql_stmt_select}
			return (&firebirdsqlRows{stmt: stmt, ctx: ctx, moreData: true}).Next(nil)
		},
	} {
		t.Run(name, func(t *testing.T) {
			fc, written := txEndTestConn(nil)
			fc.wp.desynced = true
			err := call(fc)
			if !errors.Is(err, driver.ErrBadConn) {
				t.Fatalf("err = %v, want driver.ErrBadConn", err)
			}
			fc.wp.conn.writer.Flush()
			if written.Len() != 0 {
				t.Fatalf("%d bytes written to a desynced wire", written.Len())
			}
		})
	}
}

// After Tx.Commit database/sql closes the Tx's statements; with no transaction active
// Stmt.Close must not commit the ended handle.
func TestStmtCloseSkipsCommitWithoutTransaction(t *testing.T) {
	var f acceptFrame
	f.opResponseFrame(0, nil) // free ack (not lazy: acceptType 0)
	fc, written := txEndTestConn(f.bytes())
	fc.tx.needBegin = true
	stmt := &firebirdsqlStmt{fc: fc, stmtHandle: 2}

	if err := stmt.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	b := written.Bytes()
	if firstOpcode(b) != op_free_statement || len(b) != 12 {
		t.Fatalf("written %x, want a single op_free_statement", b)
	}
}

// A prepared statement that is not a SELECT, run with Query, has no cursor to free, but its
// work still needs the autocommit commit, and rows.Close is where it happens (under the
// query's context, its error returned to Rows.Close / Row.Scan). Before, it was left to
// Stmt.Close: fixed teardown bound, error dropped.
func TestRowsCloseCommitsPreparedNonSelect(t *testing.T) {
	t.Run("committed", func(t *testing.T) {
		var f acceptFrame
		f.opResponseFrame(0, nil) // commit ack
		fc, written := txEndTestConn(f.bytes())
		stmt := &firebirdsqlStmt{fc: fc, stmtHandle: 2, stmtType: isc_info_sql_stmt_insert}
		rows := newFirebirdsqlRows(context.Background(), stmt, nil)

		if err := rows.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if ops := writtenOpcodes(t, written.Bytes()); len(ops) != 1 || ops[0] != op_commit_retaining {
			t.Fatalf("written opcodes = %v, want a single op_commit_retaining (no free)", ops)
		}
	})
	t.Run("commit refused", func(t *testing.T) {
		var f acceptFrame
		f.opResponseFrame(0, nil, isc_arg_gds, refusedGDS) // commit refused
		f.opResponseFrame(0, nil)                          // rollback ack
		fc, written := txEndTestConn(f.bytes())
		stmt := &firebirdsqlStmt{fc: fc, stmtHandle: 2, stmtType: isc_info_sql_stmt_exec_procedure}
		rows := newFirebirdsqlRows(context.Background(), stmt, nil)

		if err := rows.Close(); !isServerError(err) {
			t.Fatalf("Close err = %v, want the commit's *FbError", err)
		}
		if ops := writtenOpcodes(t, written.Bytes()); len(ops) != 2 || ops[0] != op_commit_retaining || ops[1] != op_rollback {
			t.Fatalf("written opcodes = %v, want commit-retaining, rollback", ops)
		}
	})
}

// Stmt.Close reports a failed autocommit commit: for a statement prepared on a sql.Conn
// database/sql hands Close's error to the caller.
func TestStmtCloseReportsCommitError(t *testing.T) {
	t.Run("flagged wire: only the refusal, not a join", func(t *testing.T) {
		fc, written := txEndTestConn(nil)
		fc.wp.desynced = true
		err := (&firebirdsqlStmt{fc: fc, stmtHandle: 2}).Close()
		if err != errConnDesynced {
			t.Fatalf("err = %#v, want exactly errConnDesynced", err)
		}
		fc.wp.conn.writer.Flush()
		if written.Len() != 0 {
			t.Fatalf("%d bytes written to a desynced wire", written.Len())
		}
	})
	t.Run("commit reply lost", func(t *testing.T) {
		var f acceptFrame
		f.opResponseFrame(0, nil)          // free ack (not lazy: acceptType 0)
		replies := append(f.bytes(), 0, 0) // then a truncated commit reply
		fc, _ := txEndTestConn(replies)
		if err := (&firebirdsqlStmt{fc: fc, stmtHandle: 2}).Close(); err == nil {
			t.Fatal("Close succeeded although the commit reply was lost")
		}
		if fc.IsValid() {
			t.Fatal("IsValid() = true after an unread commit reply")
		}
	})
	t.Run("commit refused", func(t *testing.T) {
		var f acceptFrame
		f.opResponseFrame(0, nil)                          // free ack
		f.opResponseFrame(0, nil, isc_arg_gds, refusedGDS) // commit refused
		f.opResponseFrame(0, nil)                          // rollback ack
		fc, written := txEndTestConn(f.bytes())
		err := (&firebirdsqlStmt{fc: fc, stmtHandle: 2}).Close()
		if !isServerError(err) {
			t.Fatalf("err = %v, want the commit's *FbError", err)
		}
		if ops := writtenOpcodes(t, written.Bytes()); len(ops) != 3 ||
			ops[0] != op_free_statement || ops[1] != op_commit_retaining || ops[2] != op_rollback {
			t.Fatalf("written opcodes = %v, want free, commit-retaining, rollback", ops)
		}
		if !fc.IsValid() {
			t.Fatal("IsValid() = false; every reply was read")
		}
	})
}

// A server error read in full around a context expiry (a clean isc_cancelled, say) means
// the statement did not run and the wire is in step: the error is still tagged
// driver.ErrBadConn, but the connection must stay usable, so a Tx can go on after one
// timed-out statement.
func TestAbandonServerErrorKeepsWire(t *testing.T) {
	var f acceptFrame
	f.opResponseFrame(0, nil, isc_arg_gds, ISCCancelled)
	fc, _ := txEndTestConn(f.bytes())
	fc.tx.isAutocommit = false
	stmt := &firebirdsqlStmt{fc: fc, stmtHandle: 2, stmtType: isc_info_sql_stmt_insert}

	_, err := stmt.exec(pastDeadlineCtx{context.Background()}, nil)
	if !errors.Is(err, driver.ErrBadConn) || !isServerError(err) {
		t.Fatalf("exec err = %v, want the server error tagged driver.ErrBadConn", err)
	}
	if !fc.IsValid() {
		t.Fatal("IsValid() = false after a server error read in full; the Tx would be lost")
	}
}

// When cancelAndDrain reads the statement's own success reply, abandon gets a nil error:
// the result must carry the context error, not "%!w(<nil>)".
func TestAbandonNilErrorReportsContext(t *testing.T) {
	fc, _ := txEndTestConn(nil)
	err := (&firebirdsqlStmt{fc: fc}).abandon(pastDeadlineCtx{context.Background()}, nil)
	if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, driver.ErrBadConn) {
		t.Fatalf("err = %v, want DeadlineExceeded tagged driver.ErrBadConn", err)
	}
	if fc.IsValid() {
		t.Fatal("IsValid() = true; the statement ran and must not be committed")
	}
}

// A reply the parser rejects (tagged driver.ErrBadConn) after op_execute, with the context
// live: the statement may have run, so the error must not carry driver.ErrBadConn
// (database/sql would run it again, repeating what the server does not roll back), and
// this connection must not commit it: flagging the wire makes the deferred Stmt.Close
// write nothing, and dropping the connection rolls the work back.
func TestExecMalformedReplyIsNotCommitted(t *testing.T) {
	var f acceptFrame
	f.opResponseFrame(0, nil, isc_arg_gds, 335544665, isc_arg_string, 1<<30)
	fc, written := txEndTestConn(f.bytes())
	stmt := &firebirdsqlStmt{fc: fc, stmtHandle: 2, stmtType: isc_info_sql_stmt_insert}

	_, err := stmt.exec(context.Background(), nil)
	if err == nil || errors.Is(err, driver.ErrBadConn) {
		t.Fatalf("exec err = %v, want the parser error without driver.ErrBadConn", err)
	}
	if !strings.Contains(err.Error(), "out of range") {
		t.Fatalf("exec err = %v lost the parser's message", err)
	}
	if fc.IsValid() {
		t.Fatal("IsValid() = true after a malformed execute reply")
	}
	fc.wp.conn.writer.Flush()
	written.Reset()
	_ = stmt.Close()
	fc.wp.conn.writer.Flush()
	if written.Len() != 0 {
		t.Fatalf("Stmt.Close wrote %x; the statement would be committed and then run again", written.Bytes())
	}
}

// rows.Close reports the free's own error when the free failed and flagged the wire, not
// the commit's refusal; on a wire another statement flagged it keeps driver.ErrBadConn.
func TestRowsCloseErrorPrecedence(t *testing.T) {
	t.Run("free failed", func(t *testing.T) {
		fc, _ := txEndTestConn([]byte{0, 0}) // non-lazy free ack, truncated
		stmt := &firebirdsqlStmt{fc: fc, stmtHandle: 2, stmtType: isc_info_sql_stmt_select}
		err := newFirebirdsqlRows(context.Background(), stmt, nil).Close()
		if !errors.Is(err, io.EOF) || errors.Is(err, errConnDesynced) {
			t.Fatalf("err = %v, want only the free's own read error, not joined with the commit's refusal", err)
		}
	})
	t.Run("wire flagged by another statement", func(t *testing.T) {
		fc, written := txEndTestConn(nil)
		fc.wp.desynced = true
		stmt := &firebirdsqlStmt{fc: fc, stmtHandle: 2, stmtType: isc_info_sql_stmt_select}
		err := newFirebirdsqlRows(context.Background(), stmt, nil).Close()
		if !errors.Is(err, driver.ErrBadConn) {
			t.Fatalf("err = %v, want driver.ErrBadConn (nothing was sent)", err)
		}
		fc.wp.conn.writer.Flush()
		if written.Len() != 0 {
			t.Fatalf("%d bytes written to a desynced wire", written.Len())
		}
	})
}

// A second rows.Close must not commit again.
func TestRowsCloseTwice(t *testing.T) {
	var f acceptFrame
	f.opResponseFrame(0, nil) // commit ack
	fc, written := txEndTestConn(f.bytes())
	stmt := &firebirdsqlStmt{fc: fc, stmtHandle: 2, stmtType: isc_info_sql_stmt_insert}
	rows := newFirebirdsqlRows(context.Background(), stmt, nil)
	if err := rows.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	fc.wp.conn.writer.Flush()
	written.Reset()
	if err := rows.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	fc.wp.conn.writer.Flush()
	if written.Len() != 0 {
		t.Fatalf("second Close wrote %x", written.Bytes())
	}
}

// A blob read is new work: refused on a desynced wire before anything is written.
func TestGetBlobSegmentsRefusesDesyncedWire(t *testing.T) {
	fc, written := txEndTestConn(nil)
	fc.wp.desynced = true
	if _, err := fc.wp.getBlobSegments(make([]byte, 8), 1); !errors.Is(err, driver.ErrBadConn) {
		t.Fatalf("err = %v, want driver.ErrBadConn", err)
	}
	fc.wp.conn.writer.Flush()
	if written.Len() != 0 {
		t.Fatalf("%d bytes written to a desynced wire", written.Len())
	}
}

// The same rule on the query path: a reply the parser rejects after op_execute flags the
// wire and does not invite a retry.
func TestQueryMalformedReplyFlagsWire(t *testing.T) {
	var f acceptFrame
	f.opResponseFrame(0, nil, isc_arg_gds, 335544665, isc_arg_string, 1<<30)
	fc, _ := txEndTestConn(f.bytes())
	stmt := &firebirdsqlStmt{fc: fc, stmtHandle: 2, stmtType: isc_info_sql_stmt_select}

	_, err := stmt.query(context.Background(), nil)
	if err == nil || errors.Is(err, driver.ErrBadConn) {
		t.Fatalf("query err = %v, want the parser error without driver.ErrBadConn", err)
	}
	if fc.IsValid() {
		t.Fatal("IsValid() = true after a malformed execute reply")
	}
}

// The records request runs after the statement executed: a server error there is count
// metadata (RowsAffected's error), not Exec's, and the work is committed. Reporting it as
// Exec's error while fc.exec's deferred Stmt.Close committed the work would tell the
// caller the statement failed when it did not.
func TestExecInfoServerErrorBelongsToRowsAffected(t *testing.T) {
	var f acceptFrame
	f.opResponseFrame(0, nil)                          // execute ok
	f.opResponseFrame(0, nil, isc_arg_gds, refusedGDS) // records request refused
	f.opResponseFrame(0, nil)                          // commit ack
	fc, written := txEndTestConn(f.bytes())
	stmt := &firebirdsqlStmt{fc: fc, stmtHandle: 2, stmtType: isc_info_sql_stmt_insert}

	result, err := stmt.exec(context.Background(), nil)
	if err != nil {
		t.Fatalf("exec err = %v, want success (the statement ran)", err)
	}
	if _, rerr := result.RowsAffected(); !isServerError(rerr) {
		t.Fatalf("RowsAffected err = %v, want the records request's *FbError", rerr)
	}
	if b := written.Bytes(); len(b) < 8 || firstOpcode(b[len(b)-8:]) != op_commit_retaining {
		t.Fatalf("written %x, want it to end with op_commit_retaining", b)
	}
	if !fc.IsValid() {
		t.Fatal("IsValid() = false; every reply was read")
	}
}

// boundByContext (connect, BeginTx, prepare, bind describe) flags the wire on a parser
// driver.ErrBadConn even with a live context: database/sql may retry a Tx statement on
// this very connection, which would read the abandoned reply's leftovers.
func TestBoundByContextFlagsAbandonedReply(t *testing.T) {
	for name, tc := range map[string]struct {
		err  error
		flag bool
	}{
		"parser ErrBadConn": {fmt.Errorf("length out of range: %w", driver.ErrBadConn), true},
		"unexpected opcode": {NewErrOpResonse(op_dummy), true},
		"EOF":               {io.EOF, true},
		"unexpected EOF":    {fmt.Errorf("reading reply: %w", io.ErrUnexpectedEOF), true},
		"network error":     {os.ErrDeadlineExceeded, true},
		"plain error":       {errors.New("describe buffer truncated"), false},
		"server error":      {&FbError{Message: "x"}, false},
	} {
		t.Run(name, func(t *testing.T) {
			for _, ctx := range []context.Context{context.Background(), func() context.Context {
				c, cancel := context.WithCancel(context.Background())
				t.Cleanup(cancel)
				return c
			}()} {
				fc, _ := txEndTestConn(nil)
				_ = fc.wp.boundByContext(ctx, func() error { return tc.err })
				if fc.wp.desynced != tc.flag {
					t.Fatalf("desynced = %v, want %v", fc.wp.desynced, tc.flag)
				}
			}
		})
	}
}

// A query context that ends between fetches, with nothing in flight, leaves the wire in
// step: rows.Close reports driver.ErrBadConn as before, but the connection is not flagged,
// so a Tx the query ran in can still commit.
func TestRowsCancelBetweenFetchesKeepsTx(t *testing.T) {
	var f acceptFrame
	f.opResponseFrame(0, nil) // free ack (not lazy)
	fc, written := txEndTestConn(f.bytes())
	fc.tx.isAutocommit = false
	stmt := &firebirdsqlStmt{fc: fc, stmtHandle: 2, stmtType: isc_info_sql_stmt_select}
	rows := newFirebirdsqlRows(teardownCtx, stmt, nil)

	if err := rows.Next(make([]driver.Value, 0)); !errors.Is(err, context.Canceled) {
		t.Fatalf("Next err = %v, want context.Canceled", err)
	}
	if err := rows.Close(); !errors.Is(err, driver.ErrBadConn) {
		t.Fatalf("Close err = %v, want driver.ErrBadConn", err)
	}
	if !fc.IsValid() {
		t.Fatal("IsValid() = false; nothing was in flight, the Tx would be lost")
	}
	fc.wp.conn.writer.Flush()
	if b := written.Bytes(); firstOpcode(b) != op_cancel {
		t.Fatalf("written %x, want op_cancel first", b)
	}
}

// markUnlessReplyRead is the one rule for the wire after a read whose request is on it:
// success or a status vector (read in full) keeps it in step; anything else flags it.
func TestMarkUnlessReplyRead(t *testing.T) {
	fbErr := &FbError{GDSCodes: []int{ISCCancelled}, Message: "operation was cancelled"}
	cases := []struct {
		name     string
		err      error
		desynced bool
	}{
		{"success", nil, false},
		{"server error", fbErr, false},
		{"wrapped server error", fmt.Errorf("op: %w", fbErr), false},
		{"server error tagged ErrBadConn", fmt.Errorf("%w: %w", fbErr, driver.ErrBadConn), false},
		{"OS deadline", os.ErrDeadlineExceeded, true},
		{"EOF", io.EOF, true},
		{"parser ErrBadConn", fmt.Errorf("firebirdsql: length 9 out of range: %w", driver.ErrBadConn), true},
		{"unexpected opcode", NewErrOpResonse(op_dummy), true},
		{"context error", context.Canceled, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p := testProtocol(nil)
			if got := p.markUnlessReplyRead(c.err); got != c.err {
				t.Fatalf("returned %v, want the error unchanged", got)
			}
			if p.desynced != c.desynced {
				t.Fatalf("desynced = %v, want %v", p.desynced, c.desynced)
			}
		})
	}
}

// endsDuringReadCtx reports itself live on its first Err call and cancelled from then on:
// the context ends while a request is on the wire. Done is nil, so no watcher runs.
type endsDuringReadCtx struct {
	context.Context
	calls *int
}

func (c endsDuringReadCtx) Err() error {
	*c.calls++
	if *c.calls == 1 {
		return nil
	}
	return context.Canceled
}

// packetLog records each write that reaches it as one packet: sendPackets flushes once per
// request, so the first four bytes of each packet are its opcode.
type packetLog struct{ packets [][]byte }

func (l *packetLog) Write(b []byte) (int, error) {
	l.packets = append(l.packets, append([]byte(nil), b...))
	return len(b), nil
}

func (l *packetLog) sent(op int32) bool {
	for _, p := range l.packets {
		if firstOpcode(p) == op {
			return true
		}
	}
	return false
}

// Every read site whose request is on the wire follows markUnlessReplyRead: a status vector
// keeps the connection usable whatever the context, a reply cut short drops it. A statement
// site whose success reply arrives after the deadline drops it too, so the work is not
// committed. The table is the place to add a new read site.
func TestReadSiteDisposition(t *testing.T) {
	type site struct {
		name string
		// run performs the site's request and read against fc; ended makes the context
		// end while the read is on the wire (when the site takes a context).
		run func(fc *firebirdsqlConn, ended bool) error
		// prelude lays down the replies to the requests the site makes before its read.
		prelude func(f *acceptFrame)
		// success lays down a success reply, for a statement site's "success after the
		// deadline" case; nil when the site has no such case.
		success   func(f *acceptFrame)
		takesCtx  bool
		tagsAfter bool // a statement site: ErrBadConn only once the context ended
		ctxErr    bool // reports the context error once it ended (else the read's, tagged)
		// serverErrorGap, when set, skips the server-error case for a known gap.
		serverErrorGap string
	}
	stmtCtx := func(ended bool) context.Context {
		if ended {
			return pastDeadlineCtx{context.Background()}
		}
		return context.Background()
	}
	okReply := func(f *acceptFrame) { f.opResponseFrame(0, nil) }
	batchCompleted := func(f *acceptFrame) {
		for _, v := range []int32{op_batch_cs, 2, 1, 1, 0, 0, 1} { // stmt, total, counts, 1 row updated
			f.int32(v)
		}
	}
	sites := []site{
		{name: "exec", takesCtx: true, tagsAfter: true, success: okReply,
			run: func(fc *firebirdsqlConn, ended bool) error {
				_, err := (&firebirdsqlStmt{fc: fc, stmtHandle: 2, stmtType: isc_info_sql_stmt_insert}).exec(stmtCtx(ended), nil)
				return err
			}},
		{name: "query", takesCtx: true, tagsAfter: true, success: okReply,
			run: func(fc *firebirdsqlConn, ended bool) error {
				_, err := (&firebirdsqlStmt{fc: fc, stmtHandle: 2, stmtType: isc_info_sql_stmt_select}).query(stmtCtx(ended), nil)
				return err
			}},
		{name: "execute procedure", takesCtx: true, tagsAfter: true,
			serverErrorGap: "opSqlResponse does not parse the status vector of a failing execute yet",
			run: func(fc *firebirdsqlConn, ended bool) error {
				_, err := (&firebirdsqlStmt{fc: fc, stmtHandle: 2, stmtType: isc_info_sql_stmt_exec_procedure}).query(stmtCtx(ended), nil)
				return err
			}},
		{name: "ExecImmediate", takesCtx: true, tagsAfter: true, success: okReply,
			run: func(fc *firebirdsqlConn, ended bool) error {
				return fc.ExecImmediate(stmtCtx(ended), "update t set a = 1")
			}},
		{name: "batch completion", takesCtx: true, tagsAfter: true, success: batchCompleted,
			run: func(fc *firebirdsqlConn, ended bool) error {
				b := &PreparedBatch{fc: fc, stmt: &firebirdsqlStmt{fc: fc, stmtHandle: 2}, created: true}
				_, err := b.Exec(stmtCtx(ended))
				return err
			}},
		{name: "batch release",
			run: func(fc *firebirdsqlConn, ended bool) error {
				return fc.wp.opBatchRelease(2, op_batch_rls)
			}},
		{name: "fetch", takesCtx: true, ctxErr: true,
			run: func(fc *firebirdsqlConn, ended bool) error {
				var ctx context.Context = context.Background()
				if ended {
					ctx = endsDuringReadCtx{context.Background(), new(int)}
				}
				stmt := &firebirdsqlStmt{fc: fc, stmtHandle: 2, stmtType: isc_info_sql_stmt_select}
				return newFirebirdsqlRows(ctx, stmt, nil).Next(make([]driver.Value, 0))
			}},
		{name: "blob in a row",
			run: func(fc *firebirdsqlConn, ended bool) error {
				_, err := fc.wp.getBlobSegments(make([]byte, 8), 1)
				return err
			}},
		{name: "scrollable execute",
			prelude: func(f *acceptFrame) {
				f.opResponseFrame(2, nil)                                       // allocate: statement handle 2
				f.opResponseFrame(0, parseXsqldaFrame(0, []byte{isc_info_end})) // prepare: a select, no columns
			},
			run: func(fc *firebirdsqlConn, ended bool) error {
				fc.wp.protocolVersion = PROTOCOL_VERSION18
				_, err := fc.QueryScrollable(context.Background(), "select 1 from rdb$database")
				return err
			}},
		{name: "scroll fetch",
			run: func(fc *firebirdsqlConn, ended bool) error {
				s := &ScrollableStmt{stmt: &firebirdsqlStmt{fc: fc, stmtHandle: 2}}
				_, _, err := s.Fetch(ScrollNext, 0, 1)
				return err
			}},
		{name: "ping",
			run: func(fc *firebirdsqlConn, ended bool) error {
				return fc.Ping(context.Background())
			}},
	}
	for _, s := range sites {
		for _, ended := range []bool{false, true} {
			if ended && !s.takesCtx {
				continue
			}
			for _, reply := range []string{"server error", "cut short", "success"} {
				if reply == "success" && (!ended || s.success == nil) {
					continue
				}
				name := fmt.Sprintf("%s/%s/ctx ended=%v", s.name, reply, ended)
				t.Run(name, func(t *testing.T) {
					t.Parallel()
					if reply == "server error" && s.serverErrorGap != "" {
						t.Skip(s.serverErrorGap)
					}
					var f acceptFrame
					if s.prelude != nil {
						s.prelude(&f)
					}
					switch reply {
					case "server error":
						f.opResponseFrame(0, nil, isc_arg_gds, ISCCancelled)
					case "success":
						s.success(&f)
					}
					if reply != "cut short" {
						f.opResponseFrame(0, nil) // acks for whatever a kept
						f.opResponseFrame(0, nil) // connection sends next
					}
					fc, _ := txEndTestConn(f.bytes())
					var log packetLog
					fc.wp.conn.writer = bufio.NewWriter(&log)
					// A Tx, where keeping the wire matters; autocommit for "success", where a
					// commit would follow.
					fc.tx.isAutocommit = reply == "success"

					err := s.run(fc, ended)
					if err == nil {
						t.Fatal("succeeded; want the read's failure")
					}
					wantValid := reply == "server error"
					if fc.IsValid() != wantValid {
						t.Fatalf("IsValid() = %v, want %v (err = %v)", fc.IsValid(), wantValid, err)
					}
					if ended && s.ctxErr {
						if !errors.Is(err, context.Canceled) {
							t.Fatalf("err = %v, want the context error", err)
						}
					}
					if s.tagsAfter {
						if tagged := errors.Is(err, driver.ErrBadConn); tagged != ended {
							t.Fatalf("err = %v: ErrBadConn tag = %v, want %v (only once the context ended)", err, tagged, ended)
						}
					}
					if reply == "success" {
						fc.wp.conn.writer.Flush()
						for _, op := range []int32{op_commit_retaining, op_commit, op_batch_rls} {
							if log.sent(op) {
								t.Fatalf("sent opcode %d after a reply read past the deadline; the work must not be committed", op)
							}
						}
					}
				})
			}
		}
	}
}

// A fetch the server answered with isc_cancelled (the watcher's op_cancel reached the
// running fetch) was read in full: the Tx must go on and commit.
func TestFetchCancelledCleanlyKeepsTx(t *testing.T) {
	var f acceptFrame
	f.opResponseFrame(0, nil, isc_arg_gds, ISCCancelled) // fetch: isc_cancelled
	f.opResponseFrame(0, nil)                            // close cursor ack
	f.opResponseFrame(0, nil)                            // commit ack
	fc, written := txEndTestConn(f.bytes())
	fc.tx.isAutocommit = false
	stmt := &firebirdsqlStmt{fc: fc, stmtHandle: 2, stmtType: isc_info_sql_stmt_select}
	rows := newFirebirdsqlRows(endsDuringReadCtx{context.Background(), new(int)}, stmt, nil)

	if err := rows.Next(make([]driver.Value, 0)); !errors.Is(err, context.Canceled) {
		t.Fatalf("Next err = %v, want context.Canceled", err)
	}
	if !fc.IsValid() {
		t.Fatal("IsValid() = false after a fetch reply read in full; the Tx would be lost")
	}
	if err := rows.Close(); !errors.Is(err, driver.ErrBadConn) {
		t.Fatalf("Close err = %v, want driver.ErrBadConn (a pooled conn is still evicted)", err)
	}
	fc.wp.conn.writer.Flush()
	written.Reset()
	if err := fc.tx.Commit(); err != nil {
		t.Fatalf("Commit err = %v, want the Tx committed", err)
	}
	fc.wp.conn.writer.Flush()
	if ops := writtenOpcodes(t, written.Bytes()); len(ops) != 1 || ops[0] != op_commit {
		t.Fatalf("Commit wrote opcodes %v, want [op_commit]", ops)
	}
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, errors.New("connection reset by peer") }

// sendPackets writes into a buffered writer; the socket write happens at Flush, so a failed
// flush is the failed send and must flag the wire.
func TestSendPacketsFlushErrorFlags(t *testing.T) {
	p := testProtocol(nil)
	p.conn.writer = bufio.NewWriter(failingWriter{})
	p.packInt(op_ping)
	_, err := p.sendPackets()
	if !errors.Is(err, driver.ErrBadConn) {
		t.Fatalf("sendPackets err = %v, want driver.ErrBadConn", err)
	}
	if !strings.Contains(err.Error(), "connection reset by peer") {
		t.Fatalf("sendPackets err = %v lost the write error", err)
	}
	if !p.desynced {
		t.Fatal("desynced = false after a failed send")
	}
	// After an execute the tag is taken out; the wrapped send error must not keep it.
	if stripped := stripBadConn("commit", err); errors.Is(stripped, driver.ErrBadConn) {
		t.Fatalf("stripBadConn(%v) still carries driver.ErrBadConn", err)
	}
}

// A batch release the server refuses after a successful batch: its ping reply must still be
// read, or the commit takes that reply as its own, reports success, and leaves the real
// commit reply on a connection that looks healthy. Both replies read, the batch commits.
func TestBatchRefusedReleaseKeepsWire(t *testing.T) {
	var f acceptFrame
	for _, v := range []int32{op_batch_cs, 2, 1, 1, 0, 0, 1} { // completion: 1 row updated
		f.int32(v)
	}
	f.opResponseFrame(0, nil, isc_arg_gds, ISCCancelled) // op_batch_rls refused
	f.opResponseFrame(0, nil)                            // its op_ping
	f.opResponseFrame(0, nil)                            // op_commit_retaining
	fc, _ := txEndTestConn(f.bytes())
	var log packetLog
	fc.wp.conn.writer = bufio.NewWriter(&log)
	b := &PreparedBatch{fc: fc, stmt: &firebirdsqlStmt{fc: fc, stmtHandle: 2}, created: true}

	res, err := b.Exec(context.Background())
	if err != nil {
		t.Fatalf("Exec err = %v, want the batch committed", err)
	}
	if res.Affected != 1 {
		t.Fatalf("Affected = %d, want 1", res.Affected)
	}
	if !fc.IsValid() {
		t.Fatal("IsValid() = false after a refused release read in full")
	}
	fc.wp.conn.writer.Flush()
	if !log.sent(op_commit_retaining) {
		t.Fatal("no op_commit_retaining sent")
	}
	if n := fc.wp.conn.reader.Buffered(); n != 0 {
		t.Fatalf("%d reply bytes left unread; the commit read the ping reply as its own", n)
	}
}

// failAfterPackets accepts the first n packets, then fails every write.
type failAfterPackets struct {
	packetLog
	n int
}

func (w *failAfterPackets) Write(b []byte) (int, error) {
	if len(w.packets) >= w.n {
		return 0, errors.New("broken pipe")
	}
	return w.packetLog.Write(b)
}

// The records request follows a successful execute. When its reply is cut short or its send
// fails, the statement may have run: nothing is committed, the wire is flagged (dropping the
// connection rolls the work back), and the error does not invite database/sql to run the
// statement again.
func TestExecRecordsReadFailureIsNotCommitted(t *testing.T) {
	for _, tc := range []struct {
		name     string
		sendFail bool
	}{
		{"reply cut short", false},
		{"send fails", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var f acceptFrame
			f.opResponseFrame(0, nil) // the execute succeeded; no records reply follows
			fc, _ := txEndTestConn(f.bytes())
			w := &failAfterPackets{n: 1 << 30}
			if tc.sendFail {
				w.n = 1 // op_execute goes out, the records request does not
			}
			fc.wp.conn.writer = bufio.NewWriter(w)
			stmt := &firebirdsqlStmt{fc: fc, stmtHandle: 2, stmtType: isc_info_sql_stmt_insert}

			_, err := stmt.exec(context.Background(), nil)
			if err == nil {
				t.Fatal("exec succeeded without its records reply")
			}
			if errors.Is(err, driver.ErrBadConn) {
				t.Fatalf("err = %v carries driver.ErrBadConn: database/sql would run the statement again", err)
			}
			if fc.IsValid() {
				t.Fatal("IsValid() = true; the work would be committed by the next statement")
			}
			fc.wp.conn.writer.Flush()
			if w.sent(op_commit_retaining) {
				t.Fatal("op_commit_retaining sent after a failed records read")
			}
		})
	}
}
