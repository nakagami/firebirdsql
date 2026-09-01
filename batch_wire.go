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
	"database/sql/driver"
	"fmt"
)

// batchPBWriter builds a Firebird batch parameter buffer: tag + int32(len) + payload.
type batchPBWriter struct {
	buf []byte
}

func newBatchPBWriter() *batchPBWriter {
	return &batchPBWriter{buf: []byte{batchVersion1}}
}

func (pb *batchPBWriter) putByte(tag byte, val byte) {
	pb.buf = append(pb.buf, tag)
	pb.buf = append(pb.buf, int32_to_bytes(1)...)
	pb.buf = append(pb.buf, val)
}

func (pb *batchPBWriter) putInt32(tag byte, val int32) {
	pb.buf = append(pb.buf, tag)
	pb.buf = append(pb.buf, int32_to_bytes(4)...)
	pb.buf = append(pb.buf, int32_to_bytes(val)...)
}

func (pb *batchPBWriter) bytes() []byte {
	return pb.buf
}

func (p *wireProtocol) opPing() error {
	p.debugPrint("opPing")
	p.packInt(op_ping)
	_, err := p.sendPackets()
	return err
}

func (p *wireProtocol) receiveTwoResponses() error {
	for i := 0; i < 2; i++ {
		_, _, _, err := p.opResponse()
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *wireProtocol) opBatchCreate(stmtHandle int32, blr []byte, msgLen int32, pb []byte) error {
	p.debugPrint("opBatchCreate")
	p.packInt(op_batch_create)
	p.packInt(stmtHandle)
	p.packBytes(blr)
	p.packInt(msgLen)
	p.packBytes(pb)
	if _, err := p.sendPackets(); err != nil {
		return err
	}
	if err := p.opPing(); err != nil {
		return err
	}
	return p.receiveTwoResponses()
}

func (p *wireProtocol) opBatchMsg(stmtHandle int32, rows [][]byte) error {
	p.debugPrint("opBatchMsg:%d", len(rows))
	p.packInt(op_batch_msg)
	p.packInt(stmtHandle)
	p.packInt(int32(len(rows)))
	for _, row := range rows {
		p.appendBytes(row)
		pad := (4 - len(row)) & 3
		if pad > 0 {
			p.appendBytes(make([]byte, pad))
		}
	}
	if _, err := p.sendPackets(); err != nil {
		return err
	}
	if p.protocolVersion >= PROTOCOL_VERSION17 {
		p.packInt(op_batch_sync)
		if _, err := p.sendPackets(); err != nil {
			return err
		}
	} else if err := p.opPing(); err != nil {
		return err
	}
	return p.receiveTwoResponses()
}

func (p *wireProtocol) opBatchExec(stmtHandle, transHandle int32) error {
	p.debugPrint("opBatchExec")
	p.packInt(op_batch_exec)
	p.packInt(stmtHandle)
	p.packInt(transHandle)
	_, err := p.sendPackets()
	return err
}

func (p *wireProtocol) opBatchRelease(stmtHandle int32, kind int32) error {
	p.debugPrint("opBatchRelease:%d", kind)
	p.packInt(kind)
	p.packInt(stmtHandle)
	if _, err := p.sendPackets(); err != nil {
		return err
	}
	if err := p.opPing(); err != nil {
		return err
	}
	return p.receiveTwoResponses()
}

type batchCompletion struct {
	TotalRecords     int32
	UpdateCounts     []int32
	DetailedErrors   []batchRowError
	SimplifiedErrors []int32
}

func (c *batchCompletion) hasErrors() bool {
	return c != nil && (len(c.DetailedErrors) > 0 || len(c.SimplifiedErrors) > 0)
}

func (c *batchCompletion) affected() int64 {
	if c == nil {
		return 0
	}
	if len(c.UpdateCounts) == 0 {
		return int64(c.TotalRecords)
	}
	var n int64
	for _, u := range c.UpdateCounts {
		if u > 0 {
			n += int64(u)
		}
	}
	return n
}

type batchRowError struct {
	Row int32
	Err error
}

func (p *wireProtocol) opBatchCompletion() (*batchCompletion, error) {
	b, err := p.recvPackets(4)
	if err != nil {
		return nil, err
	}
	for bytes_to_bint32(b) == op_dummy {
		b, err = p.recvPackets(4)
		if err != nil {
			return nil, err
		}
	}
	for bytes_to_bint32(b) == op_response && p.lazyResponseCount > 0 {
		p.lazyResponseCount--
		_, _, _, _ = p._parse_op_response()
		b, err = p.recvPackets(4)
		if err != nil {
			return nil, err
		}
	}
	opcode := bytes_to_bint32(b)
	if opcode == op_response {
		_, _, _, err = p._parse_op_response()
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("firebirdsql: unexpected op_response instead of op_batch_cs")
	}
	if opcode != op_batch_cs {
		return nil, fmt.Errorf("firebirdsql: expected op_batch_cs, got %d", opcode)
	}

	if _, err = p.recvPackets(4); err != nil { // statement handle
		return nil, err
	}
	b, err = p.recvPackets(4)
	if err != nil {
		return nil, err
	}
	total := bytes_to_bint32(b)

	b, err = p.recvPackets(4)
	if err != nil {
		return nil, err
	}
	updateCount := int(bytes_to_bint32(b))
	b, err = p.recvPackets(4)
	if err != nil {
		return nil, err
	}
	detailedCount := int(bytes_to_bint32(b))
	b, err = p.recvPackets(4)
	if err != nil {
		return nil, err
	}
	simplifiedCount := int(bytes_to_bint32(b))

	if updateCount < 0 || detailedCount < 0 || simplifiedCount < 0 ||
		updateCount > maxWirePayload || detailedCount > maxWirePayload || simplifiedCount > maxWirePayload {
		return nil, fmt.Errorf("firebirdsql: invalid batch completion counts: %w", driver.ErrBadConn)
	}

	c := &batchCompletion{TotalRecords: total}
	c.UpdateCounts = make([]int32, updateCount)
	for i := 0; i < updateCount; i++ {
		b, err = p.recvPackets(4)
		if err != nil {
			return nil, err
		}
		c.UpdateCounts[i] = bytes_to_bint32(b)
	}

	c.DetailedErrors = make([]batchRowError, detailedCount)
	for i := 0; i < detailedCount; i++ {
		b, err = p.recvPackets(4)
		if err != nil {
			return nil, err
		}
		row := bytes_to_bint32(b)
		sv, err := p._parse_status_vector()
		if err != nil {
			return nil, err
		}
		var rowErr error
		if len(sv.gdsCodes) > 0 || sv.sqlCode != 0 {
			rowErr = &FbError{
				GDSCodes: sv.gdsCodes,
				SQLCode:  sv.sqlCode,
				SQLState: sv.sqlState,
				Params:   sv.params,
				Warnings: sv.warnings,
				Message:  sv.message,
			}
		} else {
			rowErr = ErrBatchRowFailed
		}
		c.DetailedErrors[i] = batchRowError{Row: row, Err: rowErr}
	}

	c.SimplifiedErrors = make([]int32, simplifiedCount)
	for i := 0; i < simplifiedCount; i++ {
		b, err = p.recvPackets(4)
		if err != nil {
			return nil, err
		}
		c.SimplifiedErrors[i] = bytes_to_bint32(b)
	}
	return c, nil
}
