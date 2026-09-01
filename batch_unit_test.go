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
	"database/sql/driver"
	"encoding/binary"
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
