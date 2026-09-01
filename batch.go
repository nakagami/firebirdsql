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
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
)

const defaultPreparedBatchFlushSize = 128 * 1024

// ErrBatchRowFailed marks a simplified per-row batch failure (no status vector).
var ErrBatchRowFailed = errors.New("batch row failed")

// BatchOptions configures batches created with PrepareBatch.
type BatchOptions struct {
	BufferBytes     int
	ContinueOnError bool
	DetailedErrors  int
	RecordCounts    bool // default true when using zero value via PrepareBatch defaults
}

// BatchResult describes server completion from PreparedBatch.Exec.
type BatchResult struct {
	Affected     int64
	UpdateCounts []int64
}

// BatchError is one row error from batch execution.
type BatchError struct {
	Row int
	Err error
}

// BatchExecutionError reports one or more row errors from batch execution.
type BatchExecutionError struct {
	Errors []BatchError
}

func (e *BatchExecutionError) Error() string {
	if e == nil || len(e.Errors) == 0 {
		return "batch failed"
	}
	return fmt.Sprintf("batch failed at row %d: %v", e.Errors[0].Row, e.Errors[0].Err)
}

func (e *BatchExecutionError) Unwrap() []error {
	if e == nil {
		return nil
	}
	out := make([]error, len(e.Errors))
	for i, be := range e.Errors {
		out[i] = be.Err
	}
	return out
}

// PreparedBatch is a Firebird protocol-16+ DML batch. Obtain via PrepareBatch on
// the driver connection (sql.Conn.Raw).
type PreparedBatch struct {
	fc            *firebirdsqlConn
	stmt          *firebirdsqlStmt
	opts          BatchOptions
	created       bool
	closed        bool
	encodedRows   [][]byte
	pendingBytes  int
	autoFlushSize int
	paramBlr      []byte
	msgLen        int32
}

// PrepareBatch prepares sql for batch DML (INSERT/UPDATE/DELETE with parameters).
// Requires negotiated wire protocol >= 16.
func (fc *firebirdsqlConn) PrepareBatch(ctx context.Context, sql string, opts BatchOptions) (*PreparedBatch, error) {
	if fc == nil || fc.wp == nil {
		return nil, driver.ErrBadConn
	}
	if fc.wp.protocolVersion < PROTOCOL_VERSION16 {
		return nil, fmt.Errorf("firebirdsql: batch requires wire protocol 16+, got %d", fc.wp.protocolVersion)
	}
	if fc.tx.needBegin {
		if err := fc.tx.begin(); err != nil {
			return nil, err
		}
	}
	stmt, err := newFirebirdsqlStmt(fc, sql)
	if err != nil {
		return nil, err
	}
	xs, err := fc.wp._fetchBindXsqlda(stmt.stmtHandle)
	if err != nil {
		_ = stmt.Close()
		return nil, err
	}
	if xs == nil {
		xs = []xSQLVAR{}
	}
	stmt.inputXsqlda = xs

	if err := validateBatchStatement(stmt); err != nil {
		_ = stmt.Close()
		return nil, err
	}

	opts = normalizeBatchOptions(opts)

	b := &PreparedBatch{
		fc:            fc,
		stmt:          stmt,
		opts:          opts,
		autoFlushSize: defaultPreparedBatchFlushSize,
		paramBlr:      calcBlr(stmt.inputXsqlda),
		msgLen:        int32(calculateBatchMessageLength(stmt.inputXsqlda)),
	}
	stmt.activeBatch = b
	_ = ctx
	return b, nil
}

// normalizeBatchOptions applies fbx-compatible defaults: RecordCounts on,
// DetailedErrors=1 (or server default when ContinueOnError).
func normalizeBatchOptions(opts BatchOptions) BatchOptions {
	opts.RecordCounts = true
	if opts.ContinueOnError {
		if opts.DetailedErrors == 0 {
			opts.DetailedErrors = -1 // omit tag; use server default
		}
	} else if opts.DetailedErrors == 0 {
		opts.DetailedErrors = 1
	}
	return opts
}

func validateBatchStatement(stmt *firebirdsqlStmt) error {
	if len(stmt.inputXsqlda) == 0 {
		return fmt.Errorf("statement used in batch must have parameters")
	}
	for i, x := range stmt.inputXsqlda {
		if x.sqltype == SQL_TYPE_BLOB {
			return fmt.Errorf("batch does not support blob parameters (arg %d)", i)
		}
		if x.sqltype == SQL_TYPE_ARRAY {
			return fmt.Errorf("batch does not support array parameters (arg %d)", i)
		}
	}
	if len(stmt.resultXsqlda) > 0 {
		return fmt.Errorf("batch supports only insert, update, and delete statements without result sets")
	}
	switch stmt.stmtType {
	case isc_info_sql_stmt_insert, isc_info_sql_stmt_update, isc_info_sql_stmt_delete:
		return nil
	default:
		return fmt.Errorf("batch supports only insert, update, and delete statements without result sets")
	}
}

func (b *PreparedBatch) parameterBuffer() []byte {
	pb := newBatchPBWriter()
	if b.opts.ContinueOnError {
		pb.putInt32(batchTagMultiError, 1)
	}
	if b.opts.RecordCounts {
		pb.putInt32(batchTagRecordCounts, 1)
	}
	if b.opts.DetailedErrors >= 0 {
		pb.putInt32(batchTagDetailedErrors, int32(b.opts.DetailedErrors))
	}
	if b.opts.BufferBytes > 0 {
		pb.putInt32(batchTagBufferBytes, int32(b.opts.BufferBytes))
	}
	pb.putByte(batchTagBlobPolicy, batchBlobIDUser)
	return pb.bytes()
}

func (b *PreparedBatch) ensureCreated() error {
	if b.closed {
		return fmt.Errorf("prepared batch is closed")
	}
	if b.created {
		return nil
	}
	if err := b.fc.wp.opBatchCreate(b.stmt.stmtHandle, b.paramBlr, b.msgLen, b.parameterBuffer()); err != nil {
		return err
	}
	b.created = true
	return nil
}

// PendingBytes returns the size of locally queued (unflushed) row data.
func (b *PreparedBatch) PendingBytes() int {
	if b == nil {
		return 0
	}
	return b.pendingBytes
}

// Add queues one parameter row.
func (b *PreparedBatch) Add(args ...driver.Value) error {
	if b == nil || b.closed {
		return fmt.Errorf("prepared batch is closed")
	}
	if len(args) != len(b.stmt.inputXsqlda) {
		return fmt.Errorf("expected %d arguments, got %d", len(b.stmt.inputXsqlda), len(args))
	}
	for i, x := range b.stmt.inputXsqlda {
		if x.sqltype == SQL_TYPE_BLOB || x.sqltype == SQL_TYPE_ARRAY {
			return fmt.Errorf("batch does not support blob/array parameters (arg %d)", i)
		}
		if args[i] != nil {
			if _, ok := args[i].([]byte); ok && x.sqltype == SQL_TYPE_BLOB {
				return fmt.Errorf("batch does not support blob parameters (arg %d)", i)
			}
		}
	}
	row, err := b.fc.wp.encodeBatchRow(b.stmt.inputXsqlda, args)
	if err != nil {
		return err
	}
	if b.autoFlushSize > 0 && b.pendingBytes > 0 && b.pendingBytes+len(row) > b.autoFlushSize {
		if err := b.Flush(context.Background()); err != nil {
			return err
		}
	}
	b.encodedRows = append(b.encodedRows, row)
	b.pendingBytes += len(row)
	return nil
}

// Flush sends queued rows to the server batch buffer without executing.
func (b *PreparedBatch) Flush(ctx context.Context) error {
	if b == nil || b.closed {
		return fmt.Errorf("prepared batch is closed")
	}
	if len(b.encodedRows) == 0 {
		return nil
	}
	if err := b.ensureCreated(); err != nil {
		return err
	}
	rows := b.encodedRows
	b.encodedRows = nil
	b.pendingBytes = 0
	_ = ctx
	return b.fc.wp.opBatchMsg(b.stmt.stmtHandle, rows)
}

// Exec flushes remaining rows, executes the batch, and releases the server batch.
func (b *PreparedBatch) Exec(ctx context.Context) (*BatchResult, error) {
	if b == nil || b.closed {
		return nil, fmt.Errorf("prepared batch is closed")
	}
	if err := b.Flush(ctx); err != nil {
		return nil, err
	}
	if !b.created {
		return &BatchResult{}, nil
	}

	if err := b.fc.wp.opBatchExec(b.stmt.stmtHandle, b.fc.tx.transHandle); err != nil {
		return nil, err
	}

	var completion *batchCompletion
	defer b.stmt.enforceDeadline(ctx)()
	err := b.stmt.withCancelWatcher(ctx, func() error {
		var e error
		completion, e = b.fc.wp.opBatchCompletion()
		return e
	})
	if err != nil {
		_ = b.fc.wp.opBatchRelease(b.stmt.stmtHandle, op_batch_rls)
		b.created = false
		return nil, err
	}

	res := &BatchResult{Affected: completion.affected()}
	if len(completion.UpdateCounts) > 0 {
		res.UpdateCounts = make([]int64, len(completion.UpdateCounts))
		for i, u := range completion.UpdateCounts {
			res.UpdateCounts[i] = int64(u)
		}
	}

	var batchErr error
	if completion.hasErrors() {
		be := &BatchExecutionError{}
		for _, de := range completion.DetailedErrors {
			be.Errors = append(be.Errors, BatchError{Row: int(de.Row), Err: de.Err})
		}
		for _, row := range completion.SimplifiedErrors {
			be.Errors = append(be.Errors, BatchError{Row: int(row), Err: ErrBatchRowFailed})
		}
		batchErr = be
	}

	// Release server batch after exec (fbx high-level always closes wire batch).
	_ = b.fc.wp.opBatchRelease(b.stmt.stmtHandle, op_batch_rls)
	b.created = false

	if b.fc.tx.isAutocommit {
		if batchErr != nil {
			_ = b.fc.tx.Rollback()
		} else if cerr := b.fc.tx.commitRetainging(); cerr != nil {
			return res, cerr
		}
	}

	if batchErr != nil {
		return res, batchErr
	}
	return res, nil
}

// Cancel clears local queue and releases the server batch (op_batch_rls).
func (b *PreparedBatch) Cancel(ctx context.Context) error {
	if b == nil || b.closed {
		return nil
	}
	b.encodedRows = nil
	b.pendingBytes = 0
	if !b.created {
		return nil
	}
	err := b.fc.wp.opBatchRelease(b.stmt.stmtHandle, op_batch_rls)
	b.created = false
	_ = ctx
	return err
}

// Close releases resources. Safe to call multiple times.
func (b *PreparedBatch) Close() error {
	if b == nil || b.closed {
		return nil
	}
	_ = b.Cancel(context.Background())
	b.closed = true
	if b.stmt != nil {
		b.stmt.activeBatch = nil
		err := b.stmt.Close()
		b.stmt = nil
		return err
	}
	return nil
}

func (b *PreparedBatch) releaseBeforeFree() {
	if b == nil || !b.created {
		return
	}
	_ = b.fc.wp.opBatchRelease(b.stmt.stmtHandle, op_batch_rls)
	b.created = false
}
