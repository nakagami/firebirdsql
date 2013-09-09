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
    "net"
    "bytes"
    "encoding/binary"
    "regexp"
)

INFO_SQL_SELECT_DESCRIBE_VARS := []byte{
    isc_info_sql_select,
    isc_info_sql_describe_vars,
    isc_info_sql_sqlda_seq,
    isc_info_sql_type,
    isc_info_sql_sub_type,
    isc_info_sql_scale,
    isc_info_sql_length,
    isc_info_sql_null_ind,
    isc_info_sql_field,
    isc_info_sql_relation,
    isc_info_sql_owner,
    isc_info_sql_alias,
    isc_info_sql_describe_end
}

func int32_to_bytes(i32 int32) []byte {
    bs := []byte {
        byte(i32 & 0xFF),
        byte(i32 >> 8 & 0xFF),
        byte(i32 >> 16 & 0xFF),
        byte(i32 >> 24 & 0xFF),
    }
    return bs
}

func bint32_to_bytes(i32 int32) []byte {
    bs := []byte {
        byte(i32 >> 24 & 0xFF),
        byte(i32 >> 16 & 0xFF),
        byte(i32 >> 8 & 0xFF),
        byte(i32 & 0xFF),
    }
    return bs
}

func bytes_to_bint(b []byte) int32 {
    var i32 int32
    buffer = bytes.NewBuffer(b)
    binary.Read(buffer, binary.BigEndian, &i32)
    return i32
}

func bytes_to_int(b []byte) int32 {
    var i32 int32
    buffer = bytes.NewBuffer(b)
    binary.Read(buffer, binary.LittleEndian, &i32)
    return i32
}

type wirepPotocol struct {
    buf []byte
    buffer_len int
    bufCount int

    conn net.Conn
    dbHandle int32
    addr string
    dbname string
    user string
    password string
}

func NewWireProtocol (dsn string) *wireProtocol {
    p := new(wireProtocol)
    p.buffer_len = 1024
    p.buf, err = make([]byte, p.buffer_len)

    dsnPattern := regexp.MustCompile(
        `^(?:(?P<user>.*?)(?::(?P<passwd>.*))?@)?` + // [user[:password]@]
            `(?:\((?P<addr>[^\)]*)\)?` +            // [(addr)]
            `\/(?P<dbname>.*?)`)                    // /dbname

    p.addr = "127.0.0.1"
    for i, match := range matches {
        switch names[i] {
        case "user":
            p.user = match
        case "passwd":
            p.passwd = match
        case "addr":
            p.addr = match
        case "dbname":
            p.dbname = match
        }
    }
    if strings.ContainsRune(p.addr, ':') {
        p.addr += ":3050"
    }

    var err error
    p.conn, err = net.Dial("tcp", p.addr)

    return p, err
}

func (p *wireProtocol) packInt(i int32) {
    // pack big endian int32
    p.buf[p.bufCount+0] = byte(i >> 24 & 0xFF)
    p.buf[p.bufCount+1] = byte(i >> 16 & 0xFF)
    p.buf[p.bufCount+2] = byte(i >> 8 & 0xFF)
    p.buf[p.bufCount+3] = byte(i & 0xFF)
    p.bufCount += 4
}

func (p *wireProtocol) packBytes(b []byte) {
    for _, b := range xdrBytes(b) {
        p.buf[p.bufCount] = b
        p.bufCount++
    }
}

func (p *wireProtocol) packString(s string) {
    for _, b := range xdrString(s) {
        p.buf[p.bufCount] = b
        p.bufCount++
    }
}

func (p *wireProtocol) appendBytes(bs [] byte) {
    for _, b := range bs {
        p.buf[p.bufCount] = b
        p.bufCount++
    }
}

func (p *wireProtocol) uid() string {
    // TODO:
    return "Firebird Go Driver"
}

func (p *wireProtocol) sendPackets() (n int, err error) {
    n, err = p.conn.Write(p.buf)
    return
}

func (p *wireProtocol) recvPackets(n int) ([]byte, error) {
    buf, err := make([]byte, n)
    return p.conn.Read(buf)
}

func (p *wireProtocol) opConnect() {
    p.packInt(op_connect)
    p.packInt(op_attach)
    p.packInt(2)   // CONNECT_VERSION2
    p.packInt(1)   // Arch type (Generic = 1)
    p.packString(bytes.NewBufferString(p.dbname))
    p.packInt(1)   // Protocol version understood count.
    p.pack_bytes(p.uid())
    p.packInt(10)  // PROTOCOL_VERSION10
    p.packInt(1)   // Arch type (Generic = 1)
    p.packInt(2)   // Min type
    p.packInt(3)   // Max type
    p.packInt(2)   // Preference weight
    p.sendPackets()
}


func (p *wireProtocol) opCreate() {
    page_size := 4096

    encode := bytes.NewBufferString("UTF8").Bytes()
    user := bytes.NewBufferString(p.user).Bytes()
    password := bytes.NewBufferString(p.password).Bytes()
    dpb := bytes.Join([][]byte{
        []byte{1},
        []byte{68, len(encode)}, encode,
        []byte{48, len(encode)}, encode,
        []byte{28, len(user)}, user,
        []byte{29, len(password)}, password,
        []byte{63, 4}, int32_to_byte(3),
        []byte{24, 4}, bint32_to_byte(1),
        []byte{54, 4}, bint32_to_byte(1),
        []byte{4, 4}, int32_to_byte(page_size),
    }, nil)

    p = xdrlib.Packer()
    p.packInt(op_create)
    p.packInt(0)                       // Database Object ID
    p.packString(p.dbName)
    p.packBytes(dpb)
    p.sendPackets()
}

func (p *wireProtocol) opAccept() {
    b, err = p.recvPackets(4)
    for {
        if bytes_to_bint(b) == op_dummy {
            b, err = p.recvPackets(4)
        }
    }

    // assert bytes_to_bint(b) == op_accept
    b = p.recvPackets(12)
    // assert up.unpack_int() == 10
    // assert  up.unpack_int() == 1
    // assert up.unpack_int() == 3
}

func (p *wireProtocol) opAttach() {
    encode := bytes.NewBufferString("UTF8").Bytes()
    user := bytes.NewBufferString(p.user).Bytes()
    password := bytes.NewBufferString(p.password).Bytes()

    dbp := bytes.Join([][]byte{
        []byte{1},
        []byte{48, len(encode)}, encode,
        []byte{28, len(user)}, user,
        []byte{29, len(password)}, password,
    })
    p.packInt(op_attach)
    p.packInt(0)                       // Database Object ID
    p.packString(p.dbName)
    p.pack_bytes(dpb)
    p.sendPackets()
}

func (p *wireProtocol) opDropDatabase() {
    p.packInt(op_drop_database)
    p.packInt(p.dbHandle)
    p.sendPackets()
}


func (p *wireProtocol) opTransaction(tpb []byte) {
    p.packInt(op_transaction)
    p.packInt(p.dbHandle)
    p.packBytes(tpb)
    p.sendPackets()
}

func (p *wireProtcol) opCommit(transHandle int32) {
    p.pack_int(op_commit)
    p.pack_int(transHandle)
    p.sendPackets()
}

func (p *wireProtcol) opCommitRetaining(transHandle int32) {
    p.pack_int(op_commit_retaining)
    p.pack_int(transHandle)
    p.sendPackets()
}

func (p *wireProtocol) opRollback(transHandle int32) {
    p.pack_int(op_rollback)
    p.pack_int(transHandle)
    p.sendPackets()
}

func (p *wireProtocol) opRollbackRetaining(transHandle int32) {
    p.packInt(op_rollback_retaining)
    p.packInt(transHandle)
    p.sendPackets()
}

func (p *wireProtocol) opAallocateStatement() {
    p.packInt(op_allocate_statement)
    p.packInt(p.dbHandle)
    p.sendPackets()
}

func (p *wireProtocol) opInfoTransaction(transHandle int32 , b []byte) {
    p.packInt(op_info_transaction)
    p.packInt(transHandle)
    p.packInt(0)
    p.packBytes(b)
    p.packInt(p.buffer_length)
    p.sendPackets()
}

func (p *wireProtocol) opInfoDatabase(bs []byte) {
    p.packInt(op_info_database)
    p.packInt(p.dbHandle)
    p.packInt(0)
    p.packBytes(bs)
    p.packInt(p.buffer_length)
    p.sendPackets()
}

func (p *wireProtocol) opFreeStatement(stmtHandle int32, mode int32) {
    p.packInt(op_free_statement)
    p.packInt(stmtHandle)
    p.packInt(mode)
    p.sendPackets()
}

func (p *wireProtocol) opPrepareStatement(stmtHandle int 32, transHandle int32, query string) {
    descItems := bytes.Join([][]byte{
        []byte{ isc_info_sql_stmt_type },
        INFO_SQL_SELECT_DESCRIBE_VARS,
    }, nil)

    p.pack_int(self.op_prepare_statement)
    p.pack_int(transHandle)
    p.pack_int(stmtHandle)
    p.pack_int(3)   # dialect = 3
    p.pack_string(self.str_to_bytes(query))
    p.pack_bytes(desc_items)
    p.pack_int(self.buffer_length)
    p.sendPackets()
}

func (p *wireProtocol) _op_info_sql(stmtHandle int32, vars) {
    p.pack_int(self.op_info_sql)
    p.pack_int(stmtHandle)
    p.pack_int(0)
    p.pack_bytes(vars)
    p.pack_int(self.buffer_length)
    p.sendPackets()
}

func (p *wireProtocol) _op_execute(stmtHandle, transHandle, params) {
    p.pack_int(op_execute)
    p.pack_int(stmtHandle)
    p.pack_int(transHandle)

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

func (p *wireProtocol) _op_execute2(stmtHandle, transHandle, params, output_blr) {
    p.pack_int(self.op_execute2)
    p.pack_int(stmtHandle)
    p.pack_int(transHandle)

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

func (p *wireProtocol) _op_execute_immediate(self, transHandle, dbHandle, sql, params, in_msg=, out_msg=, possible_requests) {
    sql = self.str_to_bytes(sql)
    in_msg = self.str_to_bytes(in_msg)
    out_msg = self.str_to_bytes(out_msg)
    r = bint_to_bytes(self.op_execute_immediate, 4)
    r += bint_to_bytes(transHandle, 4) + bint_to_bytes(db_handle, 4)
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

func (p *wireProtocol)  _op_fetch(stmtHandle int32, blr [] byte) {
    p.pack_int(self.op_fetch)
    p.pack_int(stmtHandle)
    p.pack_bytes(blr)
    p.pack_int(0)
    p.pack_int(400)
    p.sendPackets()
}

func (p *wireProtocol) _op_fetch_response(self, stmtHandle, xsqlda) {
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

func (p *wireProtocol)  _op_open_blob(blob_id, transHandle) {
    p = xdrlib.Packer()
    p.pack_int(self.op_open_blob)
    p.pack_int(transHandle)
    p.appendPacket(blog_id)
    p.sendPackets()
}

func (p *wireProtocol)  _op_create_blob2(transHandle int32) {
    p = xdrlib.Packer()
    p.pack_int(self.op_create_blob2)
    p.pack_int(0)
    p.pack_int(transHandle)
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

//------------------------------------------------------------------------

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
