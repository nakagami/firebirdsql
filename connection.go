/*******************************************************************************
The MIT License (MIT)

Copyright (c) 2013-2019 Hajime Nakagami

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
	"io"
	"math/big"
	"net"
	"time"
)

type firebirdsqlConn struct {
	wp                *wireProtocol
	tx                *firebirdsqlTx
	dsn               *firebirdDsn
	columnNameToLower bool
	isAutocommit      bool
	clientPublic      *big.Int
	clientSecret      *big.Int
	transactionSet    map[*firebirdsqlTx]struct{}
}

// WireCipher returns the name of the wire-encryption cipher negotiated for this
// connection ("ChaCha64", "ChaCha", or "Arc4"), or an empty string when the
// connection is unencrypted (plaintext). Because the concrete connection type
// is unexported, applications reach it through the database/sql layer by
// asserting sql.Conn.Raw to an interface:
//
//	conn.Raw(func(dc any) error {
//	    cipher := dc.(interface{ WireCipher() string }).WireCipher()
//	    ...
//	})
func (fc *firebirdsqlConn) WireCipher() string {
	return fc.wp.conn.plugin
}

// ProtocolVersion returns the negotiated Firebird wire protocol version
// (for example 13, 18, or 19). Reach it through sql.Conn.Raw the same way as WireCipher.
func (fc *firebirdsqlConn) ProtocolVersion() int {
	return int(fc.wp.protocolVersion)
}

// IsValid implements driver.Validator. database/sql calls it before returning the
// connection to the pool: a connection whose wire was left mid-exchange (see
// wireProtocol.desynced) is discarded instead of serving the next query with the
// previous exchange's reply. Unlike driver.ErrBadConn this never makes database/sql
// run the statement again, which matters once the statement has executed.
func (fc *firebirdsqlConn) IsValid() bool {
	return !fc.wp.desynced
}

// errConnDesynced is returned, before anything is written, by every call that starts new
// work on a connection whose wire is desynced. It wraps driver.ErrBadConn: nothing was
// sent, so database/sql may safely retry on another connection, and a held sql.Conn is
// closed.
var errConnDesynced = fmt.Errorf("firebirdsql: connection is out of step with the server: %w", driver.ErrBadConn)

// checkWire refuses new work on a desynced wire. IsValid alone is not enough:
// database/sql validates a connection before it runs the statement closes queued for it
// (driverConn.onPut), and a held sql.Conn is never validated, so a connection can still
// reach a driver call after its wire was flagged.
func (fc *firebirdsqlConn) checkWire() error {
	if fc.wp.desynced {
		return errConnDesynced
	}
	return nil
}

// ============ driver.Conn implementation

func (fc *firebirdsqlConn) begin(isolationLevel int) (driver.Tx, error) {
	if err := fc.checkWire(); err != nil {
		return nil, err
	}
	tx, err := newFirebirdsqlTx(fc, isolationLevel, false, true)
	fc.tx = tx
	return driver.Tx(tx), err
}

// Begin starts and returns a new transaction.
//
// Deprecated: Drivers should implement ConnBeginTx instead (or additionally).
// -> is implemented in driver_go18.go with BeginTx()
func (fc *firebirdsqlConn) Begin() (driver.Tx, error) {
	return fc.begin(ISOLATION_LEVEL_READ_COMMITED)
}

// Close invalidates and potentially stops any current
// prepared statements and transactions, marking this
// connection as no longer in use.
//
// Because the sql package maintains a free pool of
// connections and only calls Close when there's a surplus of
// idle connections, it shouldn't be necessary for drivers to
// do their own connection caching.
func (fc *firebirdsqlConn) Close() (err error) {
	for tx := range fc.transactionSet {
		// Teardown: a fixed bound, never the transaction's own (possibly live) context.
		// On a desynced wire rollback sends nothing (checkWire).
		tx.rollback(teardownCtx)
	}
	fc.wp.clearAllInlineBlobCache()
	if fc.wp.desynced {
		// Detach would read the abandoned exchange's reply (or wait out
		// abandonReadTimeout on a silent wire); database/sql runs this Close in
		// the caller's goroutine. Dropping the socket makes the server roll back
		// and release the attachment.
		return fc.wp.conn.Close()
	}

	err = fc.wp.opDetach()
	if err != nil {
		fc.wp.conn.Close()
		return
	}
	// Teardown read: bounded so pool eviction (database/sql closing a conn flagged
	// ErrBadConn) cannot hang on a silent wire.
	_, _, _, err = fc.wp.opResponseTimeout(abandonReadTimeout)
	fc.wp.conn.Close()
	return
}

func (fc *firebirdsqlConn) prepare(ctx context.Context, query string) (driver.Stmt, error) {
	if fc.tx == nil {
		return nil, driver.ErrBadConn
	}
	if err := fc.checkWire(); err != nil {
		return nil, err
	}
	var stmt *firebirdsqlStmt
	err := fc.wp.withContextDeadline(ctx, func() error {
		if fc.tx.needBegin {
			if err := fc.tx.begin(); err != nil {
				return err
			}
		}
		var err error
		stmt, err = newFirebirdsqlStmt(fc, query)
		return err
	})
	if err != nil {
		return nil, err
	}
	return stmt, nil
}

// Prepare returns a prepared statement, bound to this connection.
func (fc *firebirdsqlConn) Prepare(query string) (driver.Stmt, error) {
	return fc.prepare(context.Background(), query)
}

// ============ driver.Tx implementation

func (fc *firebirdsqlConn) exec(ctx context.Context, query string, args []driver.Value) (result driver.Result, err error) {
	stmt, err := fc.prepare(ctx, query)
	if err != nil {
		return
	}
	defer stmt.Close()
	return stmt.(*firebirdsqlStmt).exec(ctx, args)
}

func (fc *firebirdsqlConn) Exec(query string, args []driver.Value) (result driver.Result, err error) {
	return fc.exec(context.Background(), query, args)
}

func (fc *firebirdsqlConn) query(ctx context.Context, query string, args []driver.Value) (rows driver.Rows, err error) {

	stmt, err := fc.prepare(ctx, query)
	if err != nil {
		return
	}
	rows, err = stmt.(*firebirdsqlStmt).query(ctx, args)
	if err == nil {
		rows.(*firebirdsqlRows).closeStmtOnClose = true
	}
	return
}

func (fc *firebirdsqlConn) Query(query string, args []driver.Value) (rows driver.Rows, err error) {
	return fc.query(context.Background(), query, args)
}

// openFirebirdsqlConn dials, authenticates and runs dbOp (attach or create)
// bounded by ctx (see withContextDeadline). On failure the socket is closed
// instead of being left to the garbage collector.
func openFirebirdsqlConn(ctx context.Context, dsn *firebirdDsn, dbOp func(*wireProtocol) error) (*firebirdsqlConn, error) {
	wp, err := newWireProtocolContext(ctx, dsn.addr, dsn.options["timezone"], dsn.options["charset"])
	if err != nil {
		return nil, err
	}
	var fc *firebirdsqlConn
	err = wp.withContextDeadline(ctx, func() error {
		var err error
		fc, err = handshakeFirebirdsqlConn(wp, dsn, dbOp)
		return err
	})
	if err != nil {
		wp.conn.Close()
		return nil, err
	}
	return fc, nil
}

func openFirebirdsqlConnWithWire(dsn *firebirdDsn, dbOp func(*wireProtocol) error, wire func(string, string, string) (*wireProtocol, error)) (*firebirdsqlConn, error) {
	wp, err := wire(dsn.addr, dsn.options["timezone"], dsn.options["charset"])
	if err != nil {
		return nil, err
	}
	return handshakeFirebirdsqlConn(wp, dsn, dbOp)
}

func handshakeFirebirdsqlConn(wp *wireProtocol, dsn *firebirdDsn, dbOp func(*wireProtocol) error) (*firebirdsqlConn, error) {
	columnNameToLower := convertToBool(dsn.options["column_name_to_lower"], false)
	clientPublic, clientSecret, err := getClientSeed()
	if err != nil {
		return nil, err
	}

	wp.clientVersion = dsn.options["client_version"]
	wp.osUser = dsn.options["os_user"]
	wp.hostName = dsn.options["host_name"]
	wp.maxInlineBlobSize = int32(parseOptionInt(dsn.options["max_inline_blob_size"], 65536))
	wp.maxBlobCacheSize = int32(parseOptionInt(dsn.options["max_blob_cache_size"], 10485760))
	wp.inlineBlobCache = newInlineBlobCache(int(wp.maxBlobCacheSize))

	if err = wp.opConnect(dsn.dbName, dsn.user, dsn.passwd, dsn.options, clientPublic); err != nil {
		return nil, err
	}
	if err = wp._parse_connect_response(dsn.user, dsn.passwd, dsn.options, clientPublic, clientSecret); err != nil {
		return nil, err
	}
	if err = dbOp(wp); err != nil {
		return nil, err
	}
	wp.dbHandle, _, _, err = wp.opResponse()
	if err != nil {
		return nil, err
	}

	fc := &firebirdsqlConn{
		transactionSet:    make(map[*firebirdsqlTx]struct{}),
		wp:                wp,
		dsn:               dsn,
		columnNameToLower: columnNameToLower,
		isAutocommit:      true,
		clientPublic:      clientPublic,
		clientSecret:      clientSecret,
	}
	fc.tx, err = newFirebirdsqlTx(fc, ISOLATION_LEVEL_READ_COMMITED, fc.isAutocommit, false)
	if err != nil {
		return nil, err
	}
	return fc, nil
}

func attachFirebirdsqlConn(ctx context.Context, dsn *firebirdDsn) (*firebirdsqlConn, error) {
	return openFirebirdsqlConn(ctx, dsn, func(wp *wireProtocol) error {
		return wp.opAttach(dsn.dbName, dsn.user, dsn.passwd, dsn.options["role"])
	})
}

func createFirebirdsqlConn(ctx context.Context, dsn *firebirdDsn) (*firebirdsqlConn, error) {
	return openFirebirdsqlConn(ctx, dsn, func(wp *wireProtocol) error {
		return wp.opCreate(dsn.dbName, dsn.user, dsn.passwd, dsn.options["role"])
	})
}

// withContextDeadline bounds fn, a run of wire round-trips that the server
// cannot cancel (connect/auth/attach, op_transaction, op_allocate/op_prepare),
// by ctx: ctx's deadline is mirrored onto the socket and cancelling ctx expires
// it at once. Without this those reads block until the OS gives up on the
// socket, which is never when the peer keeps the TCP connection open but stops
// answering.
//
// If ctx ends while fn is on the wire the exchange is left half done, so the
// error wraps driver.ErrBadConn and database/sql discards the connection; a
// server error is returned as is, since its reply was read in full (see
// boundByContext). The watcher is joined before the deadline is cleared, so a
// late cancellation cannot leave an expired deadline on a pooled connection.
func (p *wireProtocol) withContextDeadline(ctx context.Context, fn func() error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return p.boundByContext(ctx, fn)
}

// boundByContext is withContextDeadline without its entry check: fn always runs. It is for
// a read whose request is already on the wire (the reply to a commit or rollback), where
// returning early would leave that reply unread on a connection that still looks healthy.
// If ctx has already ended, fn's first blocking read fails as soon as the watcher expires
// the socket.
//
// A server error means the reply was read in full, so it is returned as is even when ctx
// ended meanwhile. Any other failure after ctx ended marks the wire desynced and wraps
// driver.ErrBadConn after awaitContextDone, which gives the context's timer up to a
// second to close Done: database/sql retries ErrBadConn unless it sees the context done.
// A driver.ErrBadConn from the wire parser marks the wire desynced whatever the context:
// it means the reply was abandoned partway, and database/sql may retry on this very
// connection (a statement prepared in a Tx).
func (p *wireProtocol) boundByContext(ctx context.Context, fn func() error) error {
	if ctx.Done() == nil {
		// Context can never be canceled; skip the watcher goroutine entirely.
		err := fn()
		p.markIfReplyAbandoned(err)
		return err
	}
	if dl, ok := ctx.Deadline(); ok {
		p.conn.SetDeadline(dl)
	}
	stop := make(chan struct{})
	watcherDone := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			p.conn.SetDeadline(time.Now())
		case <-stop:
		}
		close(watcherDone)
	}()
	err := fn()
	close(stop)
	<-watcherDone
	p.conn.SetDeadline(time.Time{})
	p.markIfReplyAbandoned(err)
	if err != nil && !isServerError(err) {
		if cerr := contextErrOrDeadlineExceeded(ctx); cerr != nil {
			p.markUnlessReplyRead(err)
			awaitContextDone(ctx)
			if ctx.Err() != nil {
				cerr = ctx.Err()
			}
			return fmt.Errorf("%w: %w", cerr, driver.ErrBadConn)
		}
	}
	return err
}

// markIfReplyAbandoned marks the wire desynced when err shows a reply abandoned partway:
// a driver.ErrBadConn from the wire parser (a mid-stream violation, see the wire-parse
// bounds convention), an unexpected opcode, EOF, or a network error. None of these can
// come from a fully consumed buffer. Errors from fully consumed buffers are plain errors
// and leave the wire alone.
//
// It is narrower than markUnlessReplyRead on purpose: boundByContext's fn (connect,
// begin, prepare) can fail with such a plain error (describe-vars, bind metadata) after
// every reply was read, and that must not cost the connection, or a Tx, while the context
// is live.
func (p *wireProtocol) markIfReplyAbandoned(err error) {
	if err == nil || isServerError(err) {
		return
	}
	var opErr *ErrOpResponse
	var netErr net.Error
	if errors.Is(err, driver.ErrBadConn) || errors.As(err, &opErr) ||
		errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.As(err, &netErr) {
		p.desynced = true
	}
}
