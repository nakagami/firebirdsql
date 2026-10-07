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
	"bufio"
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"testing"
)

func TestBatchPBWriterLayout(t *testing.T) {
	pb := newBatchPBWriter()
	pb.putInt32(batchTagRecordCounts, 1)
	pb.putByte(batchTagBlobPolicy, batchBlobIDUser)
	got := pb.bytes()
	if got[0] != batchVersion1 {
		t.Fatalf("version tag: got %d", got[0])
	}
	wantRec := []byte{batchTagRecordCounts, 4, 0, 0, 0, 1, 0, 0, 0}
	if !bytes.Equal(got[1:1+len(wantRec)], wantRec) {
		t.Fatalf("record counts item:\n got %v\nwant %v", got[1:1+len(wantRec)], wantRec)
	}
	off := 1 + len(wantRec)
	wantBlob := []byte{batchTagBlobPolicy, 1, 0, 0, 0, batchBlobIDUser}
	if !bytes.Equal(got[off:], wantBlob) {
		t.Fatalf("blob policy item:\n got %v\nwant %v", got[off:], wantBlob)
	}
}

func TestOpExecuteImmediatePacket(t *testing.T) {
	p2 := &wireProtocol{buf: make([]byte, 0, 256)}
	p2.packInt(op_execute_immediate)
	p2.packInt(7)
	p2.packInt(0)
	p2.packInt(3)
	p2.packString("create table t(i int)")
	p2.packBytes(nil)
	p2.packInt(0)
	raw := p2.buf
	if binary.BigEndian.Uint32(raw[0:4]) != uint32(op_execute_immediate) {
		t.Fatalf("opcode: %d", binary.BigEndian.Uint32(raw[0:4]))
	}
	if binary.BigEndian.Uint32(raw[4:8]) != 7 {
		t.Fatalf("trans handle")
	}
	if binary.BigEndian.Uint32(raw[8:12]) != 0 {
		t.Fatalf("statement must be 0")
	}
	if binary.BigEndian.Uint32(raw[12:16]) != 3 {
		t.Fatalf("dialect must be 3")
	}
}

func TestEncodeBatchRowNullBitmapAndInt(t *testing.T) {
	p := &wireProtocol{charset: "UTF8"}
	xs := []xSQLVAR{
		{sqltype: SQL_TYPE_LONG, sqllen: 4},
		{sqltype: SQL_TYPE_VARYING, sqllen: 64},
	}
	row, err := p.encodeBatchRow(xs, []driver.Value{int64(42), "hi"})
	if err != nil {
		t.Fatal(err)
	}
	if len(row) < 4 {
		t.Fatalf("row too short: %d", len(row))
	}
	if row[0]|row[1]|row[2]|row[3] != 0 {
		t.Fatalf("null bitmap should be clear: %v", row[:4])
	}
	if binary.BigEndian.Uint32(row[4:8]) != 42 {
		t.Fatalf("int payload: %v", row[4:8])
	}
	vlen := int(binary.BigEndian.Uint32(row[8:12]))
	if vlen != 2 {
		t.Fatalf("varchar len %d", vlen)
	}
	if string(row[12:14]) != "hi" {
		t.Fatalf("varchar data %q", row[12:14])
	}
}

func TestEncodeBatchRowRejectsBlobType(t *testing.T) {
	p := &wireProtocol{charset: "UTF8"}
	xs := []xSQLVAR{{sqltype: SQL_TYPE_BLOB, sqllen: 8}}
	_, err := p.encodeBatchRow(xs, []driver.Value{[]byte("x")})
	if err == nil {
		t.Fatal("expected blob rejection")
	}
}

func TestCalculateBatchMessageLengthPositive(t *testing.T) {
	xs := []xSQLVAR{
		{sqltype: SQL_TYPE_LONG, sqllen: 4},
		{sqltype: SQL_TYPE_VARYING, sqllen: 64},
	}
	n := calculateBatchMessageLength(xs)
	if n <= 0 {
		t.Fatalf("message length %d", n)
	}
}

func TestOpBatchCompletionParse(t *testing.T) {
	var body bytes.Buffer
	writeBE := func(v int32) {
		var b [4]byte
		binary.BigEndian.PutUint32(b[:], uint32(v))
		body.Write(b[:])
	}
	writeBE(op_batch_cs)
	writeBE(11) // stmt
	writeBE(2)  // total
	writeBE(2)  // update counts
	writeBE(1)  // detailed
	writeBE(1)  // simplified
	writeBE(1)  // update[0]
	writeBE(1)  // update[1]
	writeBE(0)  // detailed row
	writeBE(isc_arg_end)
	writeBE(1) // simplified row

	c, err := testProtocol(body.Bytes()).opBatchCompletion()
	if err != nil {
		t.Fatal(err)
	}
	if c.TotalRecords != 2 {
		t.Fatalf("total %d", c.TotalRecords)
	}
	if len(c.UpdateCounts) != 2 || c.UpdateCounts[0] != 1 {
		t.Fatalf("updates %v", c.UpdateCounts)
	}
	if len(c.DetailedErrors) != 1 || c.DetailedErrors[0].Row != 0 {
		t.Fatalf("detailed %v", c.DetailedErrors)
	}
	if len(c.SimplifiedErrors) != 1 || c.SimplifiedErrors[0] != 1 {
		t.Fatalf("simplified %v", c.SimplifiedErrors)
	}
}

func TestNormalizeBatchOptions(t *testing.T) {
	o := normalizeBatchOptions(BatchOptions{})
	if !o.RecordCounts {
		t.Fatal("RecordCounts should default true")
	}
	if o.DetailedErrors != 1 {
		t.Fatalf("DetailedErrors default: %d", o.DetailedErrors)
	}
	o2 := normalizeBatchOptions(BatchOptions{ContinueOnError: true})
	if o2.DetailedErrors != -1 {
		t.Fatalf("ContinueOnError DetailedErrors: %d", o2.DetailedErrors)
	}
}

func TestOpBatchCreateMsgPacketShapes(t *testing.T) {
	p := &wireProtocol{buf: make([]byte, 0, 128), protocolVersion: PROTOCOL_VERSION16}
	p.packInt(op_batch_create)
	p.packInt(5)
	p.packBytes([]byte{1, 2})
	p.packInt(40)
	p.packBytes([]byte{batchVersion1})
	raw := p.buf
	if binary.BigEndian.Uint32(raw[0:4]) != uint32(op_batch_create) {
		t.Fatalf("create opcode")
	}

	p2 := &wireProtocol{buf: make([]byte, 0, 128), protocolVersion: PROTOCOL_VERSION17}
	p2.packInt(op_batch_msg)
	p2.packInt(5)
	p2.packInt(1)
	row := []byte{0, 0, 0, 0, 0, 0, 0, 1}
	p2.appendBytes(row)
	raw2 := p2.buf
	if binary.BigEndian.Uint32(raw2[0:4]) != uint32(op_batch_msg) {
		t.Fatalf("msg opcode")
	}
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

func firstOpcode(b []byte) int32 {
	if len(b) < 4 {
		return -1
	}
	return int32(binary.BigEndian.Uint32(b[:4]))
}

// batchTestConn builds a connection over canned replies, with the written packets logged.
func batchTestConn(replies []byte) (*firebirdsqlConn, *packetLog) {
	wp := testProtocol(replies)
	wp.conn.conn = recordDeadlineConn{}
	var log packetLog
	wp.conn.writer = bufio.NewWriter(&log)
	fc := &firebirdsqlConn{wp: wp, isAutocommit: true, transactionSet: map[*firebirdsqlTx]struct{}{}}
	fc.tx = &firebirdsqlTx{fc: fc, isAutocommit: true, transHandle: 1}
	fc.transactionSet[fc.tx] = struct{}{}
	return fc, &log
}

// Each batch request is followed by an op_ping / op_batch_sync, and the server answers both.
// Both replies are read whichever of them is a refusal, or the next request on the
// connection reads the one left behind. A reply cut short leaves the wire at an unknown
// position, so the connection is marked desynced.
func TestBatchTwoRepliesAlwaysRead(t *testing.T) {
	requests := []struct {
		name    string
		version int32
		send    func(p *wireProtocol) error
	}{
		{"create", PROTOCOL_VERSION16, func(p *wireProtocol) error {
			return p.opBatchCreate(2, []byte{1}, 8, []byte{batchVersion1})
		}},
		{"msg/op_ping", PROTOCOL_VERSION16, func(p *wireProtocol) error {
			return p.opBatchMsg(2, [][]byte{{0, 0, 0, 1}})
		}},
		{"msg/op_batch_sync", PROTOCOL_VERSION17, func(p *wireProtocol) error {
			return p.opBatchMsg(2, [][]byte{{0, 0, 0, 1}})
		}},
		{"release", PROTOCOL_VERSION16, func(p *wireProtocol) error {
			return p.opBatchRelease(2, op_batch_rls)
		}},
	}
	refused := func(f *acceptFrame) { f.opResponseFrame(0, nil, isc_arg_gds, ISCCancelled) }
	ok := func(f *acceptFrame) { f.opResponseFrame(0, nil) }

	inStep := []struct {
		name    string
		first   func(f *acceptFrame)
		second  func(f *acceptFrame)
		wantNil bool
	}{
		{"refused,ok", refused, ok, false},
		{"ok,refused", ok, refused, false},
		{"refused,refused", refused, refused, false},
		{"ok,ok", ok, ok, true},
	}
	cutShort := []struct {
		name  string
		build func(f *acceptFrame)
	}{
		{"refused,nothing", func(f *acceptFrame) { refused(f) }},
		{"refused,opcode only", func(f *acceptFrame) { refused(f); f.int32(op_response) }},
		{"nothing", func(f *acceptFrame) {}},
		{"opcode only", func(f *acceptFrame) { f.int32(op_response) }},
	}

	for _, rq := range requests {
		for _, c := range inStep {
			t.Run(rq.name+"/"+c.name, func(t *testing.T) {
				var f acceptFrame
				c.first(&f)
				c.second(&f)
				f.opResponseFrame(7, nil) // marker: the next reply on the wire
				p := testProtocol(f.bytes())
				p.protocolVersion = rq.version

				err := rq.send(p)
				if c.wantNil {
					if err != nil {
						t.Fatalf("err = %#v, want nil", err)
					}
				} else {
					var fbErr *FbError
					if !errors.As(err, &fbErr) {
						t.Fatalf("err = %v, want the server's refusal", err)
					}
				}
				if p.desynced {
					t.Fatal("desynced after two replies read in full")
				}
				handle, _, _, err := p.opResponse()
				if err != nil || handle != 7 {
					t.Fatalf("next reply = handle %d, err %v; want the marker (handle 7)", handle, err)
				}
				if n := p.conn.reader.Buffered(); n != 0 {
					t.Fatalf("%d bytes left on the wire", n)
				}
			})
		}
		for _, c := range cutShort {
			t.Run(rq.name+"/"+c.name, func(t *testing.T) {
				var f acceptFrame
				c.build(&f)
				p := testProtocol(f.bytes())
				p.protocolVersion = rq.version

				err := rq.send(p)
				var fbErr *FbError
				if err == nil || errors.As(err, &fbErr) {
					t.Fatalf("err = %v, want a read error", err)
				}
				if !p.desynced {
					t.Fatal("not desynced after a reply cut short")
				}
			})
		}
	}
}

// A batch release the server refuses after a successful batch: its ping reply must still be
// read, or the commit takes that reply as its own, reports success, and leaves the real
// commit reply on a connection that looks healthy. Both replies read, the batch commits.
func TestBatchRefusedReleaseKeepsWire(t *testing.T) {
	completion := func(f *acceptFrame) {
		for _, v := range []int32{op_batch_cs, 2, 1, 1, 0, 0, 1} { // completion: 1 row updated
			f.int32(v)
		}
	}

	t.Run("refused release", func(t *testing.T) {
		var f acceptFrame
		completion(&f)
		f.opResponseFrame(0, nil, isc_arg_gds, ISCCancelled) // op_batch_rls refused
		f.opResponseFrame(0, nil)                            // its op_ping
		f.opResponseFrame(0, nil)                            // op_commit_retaining
		fc, log := batchTestConn(f.bytes())
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
	})

	// The release succeeds but the reply to its ping never arrives in full: nothing may be
	// committed into a wire whose position is unknown.
	t.Run("ping reply cut short", func(t *testing.T) {
		var f acceptFrame
		completion(&f)
		f.opResponseFrame(0, nil) // op_batch_rls
		fc, log := batchTestConn(f.bytes())
		b := &PreparedBatch{fc: fc, stmt: &firebirdsqlStmt{fc: fc, stmtHandle: 2}, created: true}

		_, err := b.Exec(context.Background())
		if !errors.Is(err, driver.ErrBadConn) {
			t.Fatalf("Exec err = %v, want driver.ErrBadConn", err)
		}
		if fc.IsValid() {
			t.Fatal("IsValid() = true after a reply cut short")
		}
		fc.wp.conn.writer.Flush()
		if log.sent(op_commit_retaining) {
			t.Fatal("op_commit_retaining sent into a desynced wire")
		}
	})
}

// Once the wire is desynced nothing more is written for the server batch: it goes with the
// connection.
func TestBatchReleaseSkippedOnDesyncedWire(t *testing.T) {
	newBatch := func() (*PreparedBatch, *firebirdsqlConn, *packetLog) {
		fc, log := batchTestConn(nil)
		fc.wp.desynced = true
		return &PreparedBatch{fc: fc, stmt: &firebirdsqlStmt{fc: fc, stmtHandle: 2}, created: true}, fc, log
	}

	t.Run("Cancel", func(t *testing.T) {
		b, fc, log := newBatch()
		err := b.Cancel(context.Background())
		if !errors.Is(err, driver.ErrBadConn) {
			t.Fatalf("Cancel err = %v, want driver.ErrBadConn", err)
		}
		if b.created {
			t.Fatal("created still set")
		}
		fc.wp.conn.writer.Flush()
		if len(log.packets) != 0 {
			t.Fatalf("%d packets written to a desynced wire", len(log.packets))
		}
	})

	t.Run("releaseBeforeFree", func(t *testing.T) {
		b, fc, log := newBatch()
		b.releaseBeforeFree()
		if b.created {
			t.Fatal("created still set")
		}
		fc.wp.conn.writer.Flush()
		if len(log.packets) != 0 {
			t.Fatalf("%d packets written to a desynced wire", len(log.packets))
		}
	})
}
