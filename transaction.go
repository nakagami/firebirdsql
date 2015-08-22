/*******************************************************************************
The MIT License (MIT)

Copyright (c) 2013-2014 Hajime Nakagami

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

type firebirdsqlTx struct {
	wp          *wireProtocol
	transHandle int32
}

func (tx *firebirdsqlTx) Commit() (err error) {
	tx.wp.opCommit(tx.transHandle)
	_, _, _, err = tx.wp.opResponse()
	tx.wp.opTransaction([]byte{
		byte(isc_tpb_version3),
		byte(isc_tpb_write),
		byte(isc_tpb_wait),
		byte(isc_tpb_read_committed),
		byte(isc_tpb_no_rec_version),
	})
	tx.transHandle, _, _, err = tx.wp.opResponse()
	return
}

func (tx *firebirdsqlTx) Rollback() (err error) {
	tx.wp.opRollback(tx.transHandle)
	_, _, _, err = tx.wp.opResponse()
	tx.wp.opTransaction([]byte{
		byte(isc_tpb_version3),
		byte(isc_tpb_write),
		byte(isc_tpb_wait),
		byte(isc_tpb_read_committed),
		byte(isc_tpb_no_rec_version),
	})
	tx.transHandle, _, _, err = tx.wp.opResponse()
	return
}

func newFirebirdsqlTx(wp *wireProtocol) (tx *firebirdsqlTx, err error) {
	tx = new(firebirdsqlTx)
	tx.wp = wp
	wp.opTransaction([]byte{
		byte(isc_tpb_version3),
		byte(isc_tpb_write),
		byte(isc_tpb_wait),
		byte(isc_tpb_read_committed),
		byte(isc_tpb_no_rec_version),
	})
	tx.transHandle, _, _, err = wp.opResponse()
	return
}
