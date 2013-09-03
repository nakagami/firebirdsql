/*******************************************************************************
The MIT License (MIT)

Copyright (c) 2013 Hajime Nakagami

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
    "fmt"
    "bytes"
    "encoding/binary"
)

func xdr_bytes(bs []byte) []byte {
    // XDR encoding bytes
    n := len(bs)
    padding := 0
    if n % 4 != 0 {
        padding = 4 - n % 4
    }
    buf := make([]byte, 4 + n + padding)
    buf[0] = byte(n >> 24 & 0xFF)
    buf[1] = byte(n >> 16 & 0xFF)
    buf[2] = byte(n >> 8 & 0xFF)
    buf[3] = byte(n & 0xFF)
    for i, b := range bs {
        buf[4+i]=b
    }
    return buf
}

func xdr_string(s string) []byte {
    // XDR encoding string
    bs := bytes.NewBufferString(s).Bytes()
    return xdr_bytes(bs)
}

type wirepPotocol struct {
    var buf[1024]byte
    var bufCount int

    var dbName string
    var user string
    var password string
}

func (p *wireProtocol) pack_int(i int32) {
    // pack big endian int32
    p.buf[p.bufCount+0] = byte(i >> 24 & 0xFF)
    p.buf[p.bufCount+1] = byte(i >> 16 & 0xFF)
    p.buf[p.bufCount+2] = byte(i >> 8 & 0xFF)
    p.buf[p.bufCount+3] = byte(i & 0xFF)
    p.bufCount += 4
}

func (p *wireProtocol) pack_bytes(b []byte) {
    for _, b := range xdr_bytes(b) {
        p.buf[p.bufCount]=b
        p.bufCount++
    }
}

func (p *wireProtocol) pack_string(s string) {
    for _, b := range xdr_string(s) {
        p.buf[p.bufCount]=b
        p.bufCount++
    }
}

func (p *wireProtocol) appendBytes(bs [] byte) {
    for _, b := range bs {
        p.buf[p.bufCount]=b
        p.bufCount++
    }
}

func (p *wireProtocol) uid() string {
    // TODO:
    return "Firebird Go Driver"
}

func (p *wireProtocol) sendPackets() {
    // TODO:
}

func (p *wireProtocol) recvPackets(n int) *bytes.Buffer {
    // TODO: recive n bytes 
    return nil
}

func (p *wireProtocol) _op_connect() {
    p.pack_int(op_connect)
    p.pack_int(op_attach)
    p.pack_int(2)   // CONNECT_VERSION2
    p.pack_int(1)   // Arch type (Generic = 1)
    if p.dbName is nil {
        p.pack_string("")
    }
    else {
        p.pack_string(bytes.NewBufferString(p.dbName))
    }
    p.pack_int(1)   // Protocol version understood count.
    p.pack_bytes(p.uid())
    p.pack_int(10)  // PROTOCOL_VERSION10
    p.pack_int(1)   // Arch type (Generic = 1)
    p.pack_int(2)   // Min type
    p.pack_int(3)   // Max type
    p.pack_int(2)   // Preference weight
    p.sendPackets()
}


func (p *wireProtocol)  _op_create() {
    page_size := 4096
    dpb = bytes([1])
    s = self.str_to_bytes("UTF8")       // always utf8
    dpb += bytes([68, len(s)]) + s
    dpb += bytes([48, len(s)]) + s
    s = self.str_to_bytes(self.user)
    dpb += bytes([28, len(s)]) + s
    s = self.str_to_bytes(self.password)
    dpb += bytes([29, len(s)]) + s
    dpb += bytes([63, 4]) + int_to_bytes(3, 4) # isc_dpb_sql_dialect = 3
    dpb += bytes([24, 4]) + bint_to_bytes(1, 4) # isc_dpb_force_write = 1
    dpb += bytes([54, 4]) + bint_to_bytes(1, 4) # isc_dpb_overwirte = 1
    dpb += bytes([4, 4]) + int_to_bytes(page_size, 4)
    p = xdrlib.Packer()
    p.pack_int(op_create)
    p.pack_int(0)                       # Database Object ID
    p.pack_string(p.dbName)
    p.pack_bytes(dpb)
    p.sendPackets()
}

func (p *wireProtocol) _op_accept() {
    b = recv_channel(self.sock, 4)
    while bytes_to_bint(b) == self.op_dummy:
        b = recv_channel(self.sock, 4)
    assert bytes_to_bint(b) == self.op_accept
    b = recv_channel(self.sock, 12)
    up = xdrlib.Unpacker(b)
    assert up.unpack_int() == 10
    assert  up.unpack_int() == 1
    assert up.unpack_int() == 3
    up.done()
}

func (p *wireProtocol) _op_attach() {
    dpb = bytes([1])
    s = self.str_to_bytes(self.charset)
    dpb += bytes([48, len(s)]) + s
    s = self.str_to_bytes(self.user)
    dpb += bytes([28, len(s)]) + s
    s = self.str_to_bytes(self.password)
    dpb += bytes([29, len(s)]) + s
    p = xdrlib.Packer()
    p.pack_int(self.op_attach)
    p.pack_int(0)                       # Database Object ID
    p.pack_string(self.str_to_bytes(self.filename))
    p.pack_bytes(dpb)
    p.sendPackets()
}

func (p *wireProtocol) _op_drop_database() {
    p = xdrlib.Packer()
    p.pack_int(self.op_drop_database)
    p.pack_int(self.db_handle)
    p.sendPackets()
}

func (p *wireProtocol) _op_service_attach() {
    dpb = bytes([2,2])
    s = self.str_to_bytes(self.user)
    dpb += bytes([isc_spb_user_name, len(s)]) + s
    s = self.str_to_bytes(self.password)
    dpb += bytes([isc_spb_password, len(s)]) + s
    dpb += bytes([isc_spb_dummy_packet_interval,0x04,0x78,0x0a,0x00,0x00])
    p = xdrlib.Packer()
    p.pack_int(self.op_service_attach)
    p.pack_int(0)
    p.pack_string(self.str_to_bytes('service_mgr'))
    p.pack_bytes(dpb)
    p.sendPackets()
}

func (p *wireProtocol) _op_service_info(param, item) {
    buffer_length := 512
    p = xdrlib.Packer()
    p.pack_int(self.op_service_info)
    p.pack_int(self.db_handle)
    p.pack_int(0)
    p.pack_bytes(param)
    p.pack_bytes(item)
    p.pack_int(buffer_length)
    p.sendPackets()
}

func (p *wireProtocol) _op_service_start(param [] byte) {
    p = xdrlib.Packer()
    p.pack_int(self.op_service_start)
    p.pack_int(self.db_handle)
    p.pack_int(0)
    p.pack_bytes(param)
    p.sendPackets()
}

func (p *wireProtocol) _op_service_detach() {
    p = xdrlib.Packer()
    p.pack_int(self.op_service_detach)
    p.pack_int(self.db_handle)
    p.sendPackets()
}

func (p *wireProtocol) _op_info_database(b []byte) {
    p = xdrlib.Packer()
    p.pack_int(self.op_info_database)
    p.pack_int(self.db_handle)
    p.pack_int(0)
    p.pack_bytes(b)
    p.pack_int(self.buffer_length)
    p.sendPackets()
}

func (p *wireProtocol) _op_transaction(tpb) {
    p = xdrlib.Packer()
    p.pack_int(self.op_transaction)
    p.pack_int(self.db_handle)
    p.pack_bytes(tpb)
    p.sendPackets()
}

func (p *wireProtcol) _op_commit(trans_handle int32) {
    p.pack_int(self.op_commit)
    p.pack_int(trans_handle)
    p.sendPackets()
}

func (p *wireProtcol) _op_commit_retaining(trans_handle int32) {
    p.pack_int(op_commit_retaining)
    p.pack_int(trans_handle)
    p.sendPackets()
}

func (p *wireProtocol) _op_rollback(trans_handle int32) {
    p.pack_int(self.op_rollback)
    p.pack_int(trans_handle)
    p.sendPackets()
}

func (p *wireProtocol) _op_rollback_retaining(trans_handle int32):
    p.pack_int(op_rollback_retaining)
    p.pack_int(trans_handle)
    p.sendPackets()

func (p *wireProtocol) _op_allocate_statement() {
    p.pack_int(self.op_allocate_statement)
    p.pack_int(self.db_handle)
    p.sendPackets()
}

func (p *wireProtocol) _op_info_transaction(trans_handle int32 , b []byte) {
    p = xdrlib.Packer()
    p.pack_int(self.op_info_transaction)
    p.pack_int(trans_handle)
    p.pack_int(0)
    p.pack_bytes(b)
    p.pack_int(self.buffer_length)
    p.sendPackets()
}

func (p *wireProtocol) _op_info_database(b [] byte) {
    p.pack_int(op_info_database)
    p.pack_int(self.db_handle)
    p.pack_int(0)
    p.pack_bytes(b)
    p.pack_int(self.buffer_length)
    p.sendPackets()
}

func (p *wireProtocol) _op_free_statement(stmt_handle int32, mode int32) {
    p.pack_int(self.op_free_statement)
    p.pack_int(stmt_handle)
    p.pack_int(mode)
    p.sendPackets()
}

func (p *wireProtocol) _op_prepare_statement(stmt_handle int 32, trans_handle int32, query string) {
    desc_items = bytes([isc_info_sql_stmt_type])+INFO_SQL_SELECT_DESCRIBE_VARS
    p = xdrlib.Packer()
    p.pack_int(self.op_prepare_statement)
    p.pack_int(trans_handle)
    p.pack_int(stmt_handle)
    p.pack_int(3)   # dialect = 3
    p.pack_string(self.str_to_bytes(query))
    p.pack_bytes(desc_items)
    p.pack_int(self.buffer_length)
    p.sendPackets()
}

func (p *wireProtocol) _op_info_sql(stmt_handle int32, vars) {
    p.pack_int(self.op_info_sql)
    p.pack_int(stmt_handle)
    p.pack_int(0)
    p.pack_bytes(vars)
    p.pack_int(self.buffer_length)
    p.sendPackets()
}

func (p *wireProtocol) _op_execute(stmt_handle, trans_handle, params) {
    p.pack_int(op_execute)
    p.pack_int(stmt_handle)
    p.pack_int(trans_handle)

    if len(params) == 0:
        p.pack_bytes(bytes([]))
        p.pack_int(0)
        p.pack_int(0)
        send_channel(self.sock, p.get_buffer())
    else:
        (blr, values) = params_to_blr(params)
        p.pack_bytes(blr)
        p.pack_int(0)
        p.pack_int(1)
        send_channel(self.sock, p.get_buffer() + values)
}

func (p *wireProtocol) _op_execute2(stmt_handle, trans_handle, params, output_blr) {
    p.pack_int(self.op_execute2)
    p.pack_int(stmt_handle)
    p.pack_int(trans_handle)

    if len(params) == 0:
        p.pack_bytes(bytes([]))
        p.pack_int(0)
        p.pack_int(0)
        send_channel(self.sock, p.get_buffer())
    else:
        (blr, values) = params_to_blr(params)
        p.pack_bytes(blr)
        p.pack_int(0)
        p.pack_int(1)
        send_channel(self.sock, p.get_buffer() + values)

    p.pack_bytes(output_blr)
    p.pack_int(0)
    p.sendPackets()
}

func (p *wireProtocol) _op_execute_immediate(self, trans_handle, db_handle, sql, params, in_msg=, out_msg=, possible_requests) {
    sql = self.str_to_bytes(sql)
    in_msg = self.str_to_bytes(in_msg)
    out_msg = self.str_to_bytes(out_msg)
    r = bint_to_bytes(self.op_execute_immediate, 4)
    r += bint_to_bytes(trans_handle, 4) + bint_to_bytes(db_handle, 4)
    r += bint_to_bytes(len(sql), 2) + sql
    r += bint_to_bytes(3, 2)    # dialect
    if len(params) == 0:
        r += bint_to_bytes(0, 2)    # in_blr len
        values = bytes([])
    else:
        (blr, values) = params_to_blr(params)
        r += bint_to_bytes(len(blr), 2) + blr
    r += bint_to_bytes(len(in_msg), 2) + in_msg
    r += bint_to_bytes(0, 2)    # unknown short int 0
    r += bint_to_bytes(len(out_msg), 2) + out_msg
    r += bint_to_bytes(possible_requests, 4)
    r += bytes([0]) * ((4-len(r+values)) & 3)    # padding
    send_channel(self.sock, r + values)
}

func (p *wireProtocol)  _op_fetch(stmt_handle int32, blr [] byte) {
    p.pack_int(self.op_fetch)
    p.pack_int(stmt_handle)
    p.pack_bytes(blr)
    p.pack_int(0)
    p.pack_int(400)
    p.sendPackets()
}

func (p *wireProtocol) _op_fetch_response(self, stmt_handle, xsqlda) {
    b = recv_channel(self.sock, 4)
    while bytes_to_bint(b) == self.op_dummy:
        b = recv_channel(self.sock, 4)
    if bytes_to_bint(b) == self.op_response:
        return self._parse_op_response()    # error occured
    if bytes_to_bint(b) != self.op_fetch_response:
        raise InternalError
    b = recv_channel(self.sock, 8)
    status = bytes_to_bint(b[:4])
    count = bytes_to_bint(b[4:8])
    rows = []
    while count:
        r = [None] * len(xsqlda)
        for i in range(len(xsqlda)):
            x = xsqlda[i]
            if x.io_length() < 0:
                b = recv_channel(self.sock, 4)
                ln = bytes_to_bint(b)
            else:
                ln = x.io_length()
            raw_value = recv_channel(self.sock, ln, word_alignment=True)
            if recv_channel(self.sock, 4) == bytes([0]) * 4: # Not NULL
                r[i] = x.value(raw_value)
        rows.append(r)
        b = recv_channel(self.sock, 12)
        op = bytes_to_bint(b[:4])
        status = bytes_to_bint(b[4:8])
        count = bytes_to_bint(b[8:])
    return rows, status != 100
}

func (p *wireProtocol) _op_detach() {
    p.pack_int(self.op_detach)
    p.pack_int(self.db_handle)
    p.sendPackets()
}

func (p *wireProtocol)  _op_open_blob(blob_id, trans_handle) {
    p = xdrlib.Packer()
    p.pack_int(self.op_open_blob)
    p.pack_int(trans_handle)
    p.appendPacket(blog_id)
    p.sendPackets()
}

func (p *wireProtocol)  _op_create_blob2(trans_handle int32) {
    p = xdrlib.Packer()
    p.pack_int(self.op_create_blob2)
    p.pack_int(0)
    p.pack_int(trans_handle)
    p.pack_int(0)
    p.pack_int(0)
    p.sendPackets()
}

func (p *wireProtocol) _op_get_segment(self, blob_handle) {
    p.pack_int(self.op_get_segment)
    p.pack_int(blob_handle)
    p.pack_int(self.buffer_length)
    p.pack_int(0)
    p.sendPackets()
}

func (p *wireProtocol) _op_batch_segments(blob_handle, seg_data) {
    ln = len(seg_data)
    p = xdrlib.Packer()
    p.pack_int(self.op_batch_segments)
    p.pack_int(blob_handle)
    p.pack_int(ln + 2)
    p.pack_int(ln + 2)
    pad_length = ((4-(ln+2)) & 3)
    send_channel(self.sock, p.get_buffer() 
            + int_to_bytes(ln, 2) + seg_data + bytes([0])*pad_length)
}

func (p *wireProtocol)  _op_close_blob(blob_handle) {
    p = xdrlib.Packer()
    p.pack_int(self.op_close_blob)
    p.pack_int(blob_handle)
    p.sendPackets()
}

func (p *wireProtocol)  _op_que_events(event_names, ast, args, event_id) {
    params = bytes([1])
    for name, n in event_names.items():
        params += bytes([len(name)])
        params += self.str_to_bytes(name)
        params += int_to_bytes(n, 4)
    p = xdrlib.Packer()
    p.pack_int(self.op_que_events)
    p.pack_int(self.db_handle)
    p.pack_bytes(params)
    p.pack_int(ast)
    p.pack_int(args)
    p.pack_int(event_id)
    p.sendPackets()
}

func (p *wireProtocol) _op_cancel_events(event_id int32) {
    p.pack_int(self.op_cancel_events)
    p.pack_int(self.db_handle)
    p.pack_int(event_id)
    send_channel(self.sock, p.get_buffer())
}

func (p *wireProtocol) _op_connect_request() {
    p = xdrlib.Packer()
    p.pack_int(self.op_connect_request)
    p.pack_int(1)    # async
    p.pack_int(self.db_handle)
    p.pack_int(0)
    send_channel(self.sock, p.get_buffer())

    b = recv_channel(self.sock, 4)
    while bytes_to_bint(b) == self.op_dummy:
        b = recv_channel(self.sock, 4)
    if bytes_to_bint(b) != self.op_response:
        raise InternalError

    h = bytes_to_bint(recv_channel(self.sock, 4))
    recv_channel(self.sock, 8)  # garbase
    ln = bytes_to_bint(recv_channel(self.sock, 4))
    ln += ln % 4    # padding
    family = bytes_to_bint(recv_channel(self.sock, 2))
    port = bytes_to_bint(recv_channel(self.sock, 2), u=True)
    b = recv_channel(self.sock, 4)
    ip_address = '.'.join([str(byte_to_int(c)) for c in b])
    ln -= 8
    recv_channel(self.sock, ln)

    (gds_codes, sql_code, message) = self._parse_status_vector()
    if sql_code or message:
        raise OperationalError(message, gds_codes, sql_code)

    return (h, port, family, ip_address)
}

func (p *wireProtocol) _op_response() {
    b = recv_channel(self.sock, 4)
    while bytes_to_bint(b) == self.op_dummy:
        b = recv_channel(self.sock, 4)
    if bytes_to_bint(b) != self.op_response:
        raise InternalError
    return self._parse_op_response()
}

func (p *wireProtocol) _op_event(self) {
    b = recv_channel(self.sock, 4)
    while bytes_to_bint(b) == self.op_dummy:
        b = recv_channel(self.sock, 4)
    if bytes_to_bint(b) == self.op_response:
        return self._parse_op_response()
    if bytes_to_bint(b) == self.op_exit or bytes_to_bint(b) == self.op_exit:
        raise DisconnectByPeer
    if bytes_to_bint(b) != self.op_event:
        raise InternalError
    return self._parse_op_event()
}

func (p *wireProtocol) _op_sql_response(xsqlda) {
    b = recv_channel(self.sock, 4)
    while bytes_to_bint(b) == self.op_dummy:
        b = recv_channel(self.sock, 4)
    if bytes_to_bint(b) != self.op_sql_response:
        raise InternalError

    b = recv_channel(self.sock, 4)
    count = bytes_to_bint(b[:4])

    r = []
    for i in range(len(xsqlda)):
        x = xsqlda[i]
        if x.io_length() < 0:
            b = recv_channel(self.sock, 4)
            ln = bytes_to_bint(b)
        else:
            ln = x.io_length()
        raw_value = recv_channel(self.sock, ln, word_alignment=True)
        if recv_channel(self.sock, 4) == bytes([0]) * 4: # Not NULL
            r.append(x.value(raw_value))
        else:
            r.append(None)

    b = recv_channel(self.sock, 32)     # ??? why 32 bytes skip

    return r
}
