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
    "os"
    "errors"
    "net"
    "bytes"
    "strings"
    "container/list"
    "database/sql/driver"
)

func debugPrint(s string) {
//    fmt.Println(s)
}

func _INFO_SQL_SELECT_DESCRIBE_VARS() [] byte {
    return []byte{
        isc_info_sql_stmt_type,
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
        isc_info_sql_describe_end,
    }
}

type wireProtocol struct {
    buf []byte
    buffer_len int
    bufCount int

    conn net.Conn
    dbHandle int32
    addr string
}

func newWireProtocol (addr string) (*wireProtocol, error) {
    p := new(wireProtocol)
    p.buffer_len = 1024
    var err error
    p.buf = make([]byte, p.buffer_len)

    p.addr = addr
    p.conn, err = net.Dial("tcp", p.addr)
    if err != nil {
        return nil, err
    }

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

func (p *wireProtocol) uid() []byte {
    user := os.Getenv("USER")
    if user == "" {
        user = os.Getenv("USERNAME")
    }
    hostname, _ := os.Hostname()

    userBytes := bytes.NewBufferString(user).Bytes()
    hostnameBytes := bytes.NewBufferString(hostname).Bytes()
    return bytes.Join([][]byte{
        []byte{1, byte(len(userBytes))}, userBytes,
        []byte{4, byte(len(hostnameBytes))}, hostnameBytes,
        []byte{6, 0},
    }, nil)
}

func (p *wireProtocol) sendPackets() (n int, err error) {
    debugPrint(fmt.Sprintf("sendPackets():%v", p.buf[:p.bufCount]))
    n, err = p.conn.Write(p.buf[:p.bufCount])
    p.bufCount = 0
    return
}

func (p *wireProtocol) recvPackets(n int) ([]byte, error) {
    buf := make([]byte, n)
    _, err := p.conn.Read(buf)
    return buf, err
}

func (p *wireProtocol) recvPacketsAlignment(n int) ([]byte, error) {
    padding := n % 4
    if padding > 0 {
        padding = 4 - padding
    }
    buf, err := p.recvPackets(n + padding)
    return buf[0:n], err
}

func (p *wireProtocol) _parse_status_vector() (*list.List, int, string, error) {
    sql_code := 0
    gds_code := 0
    gds_codes := list.New()
    num_arg := 0
    message := ""

    b, err := p.recvPackets(4)
    n := bytes_to_bint32(b)
    for ;n != isc_arg_end; {
        switch {
        case n == isc_arg_gds:
            b, err = p.recvPackets(4)
            gds_code := int(bytes_to_bint32(b))
            if gds_code != 0 {
                gds_codes.PushBack(gds_code)
                message += errmsgs[gds_code]
                num_arg = 0
            }
        case n == isc_arg_number:
            b, err = p.recvPackets(4)
            num := int(bytes_to_bint32(b))
            if gds_code == 335544436 {
                sql_code = num
            }
            num_arg += 1
            message = strings.Replace(message, "@" + string(num_arg), string(num), 1)
        case n == isc_arg_string || n == isc_arg_interpreted:
            b, err = p.recvPackets(4)
            nbytes := int(bytes_to_bint32(b))
            b, err = p.recvPacketsAlignment(nbytes)
            s := bytes_to_str(b)
            num_arg += 1
            message = strings.Replace(message, "@" + string(num_arg), s, 1)
        case n == isc_arg_sql_state:
            b, err = p.recvPackets(4)
            nbytes := int(bytes_to_bint32(b))
            b, err = p.recvPacketsAlignment(nbytes)
            _ = bytes_to_str(b)    // skip status code
        }
        b, err = p.recvPackets(4)
        n = bytes_to_bint32(b)
    }

    return gds_codes, sql_code, message, err
}


func (p *wireProtocol) _parse_op_response() (int32, int32, []byte, error) {
    b, err := p.recvPackets(16)
    h := bytes_to_bint32(b[0:4])           // Object handle
    oid := bytes_to_bint32(b[4:12])                       // Object ID
    buf_len := int(bytes_to_bint32(b[12:]))     // buffer length
    buf, err := p.recvPacketsAlignment(buf_len)

    gds_code_list, sql_code, message, err := p._parse_status_vector()
    if gds_code_list.Len() > 0 || sql_code != 0 {
        err = errors.New(message)
    }

    return h, oid, buf, err
}


func (p *wireProtocol) _parse_select_items(buf []byte, xsqlda []xSQLVAR) (int, error) {
    var err error
    var ln int
    index := 0
    i := 0
    for item := int(buf[i]); item != isc_info_end; item=int(buf[i]) {
        i++
        switch item {
        case isc_info_sql_sqlda_seq:
            ln = int(bytes_to_int16(buf[i:i+2]))
            i += 2
            index = int(bytes_to_int32(buf[i:i+ln]))
            i += ln
        case isc_info_sql_type:
            ln = int(bytes_to_int16(buf[i:i+2]))
            i += 2
            sqltype := int(bytes_to_int32(buf[i:i+ln]))
            if (sqltype % 2 != 0) {
                sqltype--
            }
            xsqlda[index-1].sqltype = sqltype
            i += ln
        case isc_info_sql_sub_type:
            ln = int(bytes_to_int16(buf[i:i+2]))
            i += 2
            xsqlda[index-1].sqlsubtype = int(bytes_to_int32(buf[i:i+ln]))
            i += ln
        case isc_info_sql_scale:
            ln = int(bytes_to_int16(buf[i:i+2]))
            i += 2
            xsqlda[index-1].sqlscale = int(bytes_to_int32(buf[i:i+ln]))
            i += ln
        case isc_info_sql_length:
            ln = int(bytes_to_int16(buf[i:i+2]))
            i += 2
            xsqlda[index-1].sqllen = int(bytes_to_int32(buf[i:i+ln]))
            i += ln
        case isc_info_sql_null_ind:
            ln = int(bytes_to_int16(buf[i:i+2]))
            i += 2
            xsqlda[index-1].null_ok = bytes_to_int32(buf[i:i+ln]) != 0
            i += ln
        case isc_info_sql_field:
            ln = int(bytes_to_int16(buf[i:i+2]))
            i += 2
            xsqlda[index-1].fieldname = bytes_to_str(buf[i:i+ln])
            i += ln
        case isc_info_sql_relation:
            ln = int(bytes_to_int16(buf[i:i+2]))
            i += 2
            xsqlda[index-1].relname = bytes_to_str(buf[i:i+ln])
            i += ln
        case isc_info_sql_owner:
            ln = int(bytes_to_int16(buf[i:i+2]))
            i += 2
            xsqlda[index-1].ownname = bytes_to_str(buf[i:i+ln])
            i += ln
        case isc_info_sql_alias:
            ln = int(bytes_to_int16(buf[i:i+2]))
            i += 2
            xsqlda[index-1].aliasname = bytes_to_str(buf[i:i+ln])
            i += ln
        case isc_info_truncated:
            return index, err    // return next index
        case isc_info_sql_describe_end:
            /* NOTHING */
        default:
            err = errors.New(fmt.Sprintf("Invalid item [%02x] ! i=%d", buf[i], i))
            break
        }
    }
    return -1, err   // no more info
}

func (p *wireProtocol) parse_xsqlda(buf []byte, stmtHandle int32) (int32, []xSQLVAR, error) {
    var ln, col_len, next_index int
    var err error
    var stmt_type int32
    var rbuf[]byte
    var xsqlda []xSQLVAR
    i := 0

    for i < len(buf) {
        if buf[i] == byte(isc_info_sql_stmt_type) {
            i += 1
            ln = int(bytes_to_int16(buf[i:i+2]))
            i += 2
            stmt_type = int32(bytes_to_int32(buf[i:i+ln]))
            i += ln
        } else if buf[i] == byte(isc_info_sql_select) && buf[i+1] == byte(isc_info_sql_describe_vars) {
            i += 2
            ln = int(bytes_to_int16(buf[i:i+2]))
            i += 2
            col_len = int(bytes_to_int32(buf[i:i+ln]))
            xsqlda = make([]xSQLVAR, col_len)
            next_index, err = p._parse_select_items(buf[i+ln:], xsqlda)
            for next_index > 0 {   // more describe vars
                p.opInfoSql(stmtHandle,
                    bytes.Join([][]byte{
                        []byte{isc_info_sql_sqlda_start, 2},
                        int16_to_bytes(int16(next_index)),
                        _INFO_SQL_SELECT_DESCRIBE_VARS(),
                    }, nil))

                _, _, rbuf, err = p.opResponse()
                // buf[:2] == []byte{0x04,0x07}
                ln = int(bytes_to_int16(rbuf[2:4]))
                // bytes_to_int(rbuf[4:4+l]) == col_len
                next_index, err = p._parse_select_items(rbuf[4+ln:], xsqlda)
            }
        } else {
            break
        }
    }
    return stmt_type, xsqlda, err
}

func (p *wireProtocol) opConnect(dbName string) {
    debugPrint("opConnect")
    p.packInt(op_connect)
    p.packInt(op_attach)
    p.packInt(2)   // CONNECT_VERSION2
    p.packInt(1)   // Arch type (Generic = 1)
    p.packString(dbName)
    p.packInt(1)   // Protocol version understood count.
    p.packBytes(p.uid())
    p.packInt(10)  // PROTOCOL_VERSION10
    p.packInt(1)   // Arch type (Generic = 1)
    p.packInt(2)   // Min type
    p.packInt(3)   // Max type
    p.packInt(2)   // Preference weight
    p.sendPackets()
}


func (p *wireProtocol) opCreate(dbName string, user string, passwd string) {
    debugPrint("opCreate")
    var page_size int32
    page_size = 4096

    encode := bytes.NewBufferString("UTF8").Bytes()
    userBytes := bytes.NewBufferString(user).Bytes()
    passwdBytes := bytes.NewBufferString(passwd).Bytes()
    dpb := bytes.Join([][]byte{
        []byte{1},
        []byte{68, byte(len(encode))}, encode,
        []byte{48, byte(len(encode))}, encode,
        []byte{28, byte(len(userBytes))}, userBytes,
        []byte{29, byte(len(passwdBytes))}, passwdBytes,
        []byte{63, 4}, int32_to_bytes(3),
        []byte{24, 4}, bint32_to_bytes(1),
        []byte{54, 4}, bint32_to_bytes(1),
        []byte{4, 4}, int32_to_bytes(page_size),
    }, nil)

    p.packInt(op_create)
    p.packInt(0)                       // Database Object ID
    p.packString(dbName)
    p.packBytes(dpb)
    p.sendPackets()
}

func (p *wireProtocol) opAccept() (err error) {
    debugPrint("opAccept")
    b, _ := p.recvPackets(4)
    for bytes_to_bint32(b) == op_dummy {
        b, _ = p.recvPackets(4)
    }

    if bytes_to_bint32(b) != op_accept {
        err = errors.New("opAccept() protocol error")
    }
    b, _ = p.recvPackets(12)
    // assert up.unpack_int() == 10
    // assert  up.unpack_int() == 1
    // assert up.unpack_int() == 3
    return
}

func (p *wireProtocol) opAttach(dbName string, user string, passwd string) {
    debugPrint("opAttach")
    encode := bytes.NewBufferString("UTF8").Bytes()
    userBytes := bytes.NewBufferString(user).Bytes()
    passwdBytes := bytes.NewBufferString(passwd).Bytes()

    dbp := bytes.Join([][]byte{
        []byte{1},
        []byte{48, byte(len(encode))}, encode,
        []byte{28, byte(len(userBytes))}, userBytes,
        []byte{29, byte(len(passwdBytes))}, passwdBytes,
    }, nil)
    p.packInt(op_attach)
    p.packInt(0)                       // Database Object ID
    p.packString(dbName)
    p.packBytes(dbp)
    p.sendPackets()
}

func (p *wireProtocol) opDropDatabase() {
    debugPrint("opDropDatabase")
    p.packInt(op_drop_database)
    p.packInt(p.dbHandle)
    p.sendPackets()
}


func (p *wireProtocol) opTransaction(tpb []byte) {
    debugPrint("opTransaction")
    p.packInt(op_transaction)
    p.packInt(p.dbHandle)
    p.packBytes(tpb)
    p.sendPackets()
}

func (p *wireProtocol) opCommit(transHandle int32) {
    debugPrint("opCommit")
    p.packInt(op_commit)
    p.packInt(transHandle)
    p.sendPackets()
}

func (p *wireProtocol) opCommitRetaining(transHandle int32) {
    debugPrint("opCommitRetaining")
    p.packInt(op_commit_retaining)
    p.packInt(transHandle)
    p.sendPackets()
}

func (p *wireProtocol) opRollback(transHandle int32) {
    debugPrint("opRollback")
    p.packInt(op_rollback)
    p.packInt(transHandle)
    p.sendPackets()
}

func (p *wireProtocol) opRollbackRetaining(transHandle int32) {
    debugPrint("opRollbackRetaining")
    p.packInt(op_rollback_retaining)
    p.packInt(transHandle)
    p.sendPackets()
}

func (p *wireProtocol) opAllocateStatement() {
    debugPrint("opAllocateStatement")
    p.packInt(op_allocate_statement)
    p.packInt(p.dbHandle)
    p.sendPackets()
}

func (p *wireProtocol) opInfoTransaction(transHandle int32 , b []byte) {
    debugPrint("opInfoTransaction")
    p.packInt(op_info_transaction)
    p.packInt(transHandle)
    p.packInt(0)
    p.packBytes(b)
    p.packInt(int32(p.buffer_len))
    p.sendPackets()
}

func (p *wireProtocol) opInfoDatabase(bs []byte) {
    debugPrint("opInfoDatabase")
    p.packInt(op_info_database)
    p.packInt(p.dbHandle)
    p.packInt(0)
    p.packBytes(bs)
    p.packInt(int32(p.buffer_len))
    p.sendPackets()
}

func (p *wireProtocol) opFreeStatement(stmtHandle int32, mode int32) {
    debugPrint("opFreeStatement")
    p.packInt(op_free_statement)
    p.packInt(stmtHandle)
    p.packInt(mode)
    p.sendPackets()
}

func (p *wireProtocol) opPrepareStatement(stmtHandle int32, transHandle int32, query string) {
    debugPrint("opPrepareStatement")
    p.packInt(op_prepare_statement)
    p.packInt(transHandle)
    p.packInt(stmtHandle)
    p.packInt(3)                        // dialect = 3
    p.packString(query)
    p.packBytes(_INFO_SQL_SELECT_DESCRIBE_VARS())
    p.packInt(int32(p.buffer_len))
    p.sendPackets()
}

func (p *wireProtocol) opInfoSql(stmtHandle int32, vars []byte) {
    debugPrint("opInfoSql")
    p.packInt(op_info_sql)
    p.packInt(stmtHandle)
    p.packInt(0)
    p.packBytes(vars)
    p.packInt(int32(p.buffer_len))
    p.sendPackets()
}

func (p *wireProtocol) opExecute(stmtHandle int32, transHandle int32, params []driver.Value) {
    debugPrint("opExecute")
    p.packInt(op_execute)
    p.packInt(stmtHandle)
    p.packInt(transHandle)

    if len(params) == 0 {
        p.packInt(0)        // packBytes([])
        p.packInt(0)
        p.packInt(0)
        p.sendPackets()
    } else {
        blr, values := paramsToBlr(params)
        p.packBytes(blr)
        p.packInt(0)
        p.packInt(1)
        p.appendBytes(values)
        p.sendPackets()
    }
}

func (p *wireProtocol) opExecute2(stmtHandle int32, transHandle int32, params []driver.Value, outputBlr []byte) {
    debugPrint("opExecute2")
    p.packInt(op_execute2)
    p.packInt(stmtHandle)
    p.packInt(transHandle)

    if len(params) == 0 {
        p.packInt(0)        // packBytes([])
        p.packInt(0)
        p.packInt(0)
    } else {
        blr, values := paramsToBlr(params)
        p.packBytes(blr)
        p.packInt(0)
        p.packInt(1)
        p.appendBytes(values)
    }

    p.packBytes(outputBlr)
    p.packInt(0)
    p.sendPackets()
}

func (p *wireProtocol)  opFetch(stmtHandle int32, blr []byte) {
    debugPrint("opFetch")
    p.packInt(op_fetch)
    p.packInt(stmtHandle)
    p.packBytes(blr)
    p.packInt(0)
    p.packInt(400)
    p.sendPackets()
}

func (p *wireProtocol) opFetchResponse(stmtHandle int32, xsqlda []xSQLVAR) (*list.List, bool, error) {
    debugPrint("opFetchResponse")
    b, err := p.recvPackets(4)
    for bytes_to_bint32(b) == op_dummy {
        b, _ = p.recvPackets(4)
    }

    if bytes_to_bint32(b) == op_response {
        p._parse_op_response()      // error occured
        return nil, false, errors.New("opFetchResponse:Internal Error")
    }
    if bytes_to_bint32(b) != op_fetch_response {
        return nil, false, errors.New("opFetchResponse:Internal Error")
    }
    b, err = p.recvPackets(8)
    status := bytes_to_bint32(b[:4])
    count := int(bytes_to_bint32(b[4:8]))
    rows := list.New()
    for ; count > 0; {
        r := make([]driver.Value, len(xsqlda))
        for i, x := range xsqlda {
            var ln int
            if x.ioLength() < 0 {
                b, err = p.recvPackets(4)
                ln = int(bytes_to_bint32(b))
            } else {
                ln = x.ioLength()
            }
            raw_value, _ := p.recvPacketsAlignment(ln)
            b, err = p.recvPackets(4)
            if bytes_to_bint32(b) == 0 { // Not NULL
                r[i], err = x.value(raw_value)
            }
        }
        rows.PushBack(r)

        b, err = p.recvPackets(12)
        // op := int(bytes_to_bint32(b[:4]))
        status = bytes_to_bint32(b[4:8])
        count = int(bytes_to_bint32(b[8:]))
    }

    return rows, status != 100, err
}

func (p *wireProtocol) opDetach() {
    debugPrint("opDetatch")
    p.packInt(op_detach)
    p.packInt(p.dbHandle)
    p.sendPackets()
}

func (p *wireProtocol)  opOpenBlob(blobId int32, transHandle int32) {
    debugPrint("opOpenBlob")
    p.packInt(op_open_blob)
    p.packInt(transHandle)
    p.packInt(blobId)
    p.sendPackets()
}

func (p *wireProtocol)  opCreateBlob2(transHandle int32) {
    debugPrint("opCreateBlob2")
    p.packInt(op_create_blob2)
    p.packInt(0)
    p.packInt(transHandle)
    p.packInt(0)
    p.packInt(0)
    p.sendPackets()
}

func (p *wireProtocol) opGetSegment(blobHandle int32) {
    debugPrint("opGetSegment")
    p.packInt(op_get_segment)
    p.packInt(blobHandle)
    p.packInt(int32(p.buffer_len))
    p.packInt(0)
    p.sendPackets()
}

func (p *wireProtocol) opBatchSegments(blobHandle int32, seg_data []byte) {
    debugPrint("opBatchSegments")
    ln := len(seg_data)
    p.packInt(op_batch_segments)
    p.packInt(blobHandle)
    p.packInt(int32(ln + 2))
    p.packInt(int32(ln + 2))
    pad_length := ((4-(ln+2)) & 3)
    padding := make([]byte, pad_length)
    p.packBytes([]byte {byte(ln & 255), byte(ln >> 8)}) // little endian int16
    p.packBytes(seg_data)
    p.packBytes(padding)
    p.sendPackets()
}

func (p *wireProtocol)  opCloseBlob(blobHandle int32) {
    debugPrint("opCloseBlob")
    p.packInt(op_close_blob)
    p.packInt(blobHandle)
    p.sendPackets()
}

func (p *wireProtocol) opResponse() (int32, int32, []byte, error) {
    b, _ := p.recvPackets(4)
    for bytes_to_bint32(b) == op_dummy {
        b, _ = p.recvPackets(4)
    }

    if bytes_to_bint32(b) != op_response {
        return 0, 0, nil, errors.New("Error op_response")
    }
    return p._parse_op_response()
}

func (p *wireProtocol) opSqlResponse(xsqlda []xSQLVAR) ([]driver.Value, error){
    debugPrint("opSqlResponse")
    b, err := p.recvPackets(4)
    for bytes_to_bint32(b) == op_dummy {
        b, err = p.recvPackets(4)
    }

    if bytes_to_bint32(b) != op_sql_response {
        return nil, errors.New("Error op_sql_response")
    }

    b, err = p.recvPackets(4)
    // count := int(bytes_to_bint32(b))

    r := make([]driver.Value, len(xsqlda))
    var ln int
    for i, x := range xsqlda {
        if x.ioLength() < 0 {
            b, err = p.recvPackets(4)
            ln = int(bytes_to_bint32(b))
        } else {
            ln = x.ioLength()
        }
        raw_value, _ := p.recvPacketsAlignment(ln)
        b, err = p.recvPackets(4)
        if bytes_to_bint32(b) == 0 {    // Not NULL
            r[i], err = x.value(raw_value)
        }
    }

    b, err = p.recvPackets(32)   // ??? 32 bytes skip

    return r, err
}
