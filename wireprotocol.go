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

import (
	"bytes"
	"container/list"
	"crypto/rc4"
	"database/sql/driver"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"net"
	"os"
	"strconv"
	"strings"
	//"unsafe"
)

const (
	PLUGIN_NAME = "Srp"
	PLUGIN_LIST = "Srp,Legacy_Auth"
	BUFFER_LEN  = 1024
)

func debugPrint(p *wireProtocol, s string) {
	//fmt.Printf("[%x] %s\n", uintptr(unsafe.Pointer(p)), s)
}

func _INFO_SQL_SELECT_DESCRIBE_VARS() []byte {
	return []byte{
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

type wireChannel struct {
	conn      net.Conn
	rc4reader *rc4.Cipher
	rc4writer *rc4.Cipher
}

func newWireChannel(conn net.Conn) (wireChannel, error) {
	var err error
	c := new(wireChannel)
	c.conn = conn

	return *c, err
}

func (c *wireChannel) setAuthKey(key []byte) (err error) {
	c.rc4reader, err = rc4.NewCipher(key)
	c.rc4writer, err = rc4.NewCipher(key)
	return
}

func (c *wireChannel) Read(buf []byte) (n int, err error) {
	if c.rc4reader != nil {
		src := make([]byte, len(buf))
		n, err = c.conn.Read(src)
		c.rc4reader.XORKeyStream(buf, src)
		return
	}
	return c.conn.Read(buf)
}

func (c *wireChannel) Write(buf []byte) (n int, err error) {
	if c.rc4writer != nil {
		dst := make([]byte, len(buf))
		c.rc4writer.XORKeyStream(dst, buf)
		return c.conn.Write(dst)
	} else {
		return c.conn.Write(buf)
	}
}
func (c *wireChannel) Close() error {
	return c.conn.Close()
}

type wireProtocol struct {
	buf []byte

	conn     wireChannel
	dbHandle int32
	addr     string

	protocolVersion    int32
	acceptArchitecture int32
	acceptType         int32
	lazyResponseCount  int

	pluginName string
	user       string
	password   string
}

func newWireProtocol(addr string) (*wireProtocol, error) {
	p := new(wireProtocol)
	p.buf = make([]byte, 0, BUFFER_LEN)

	p.addr = addr
	conn, err := net.Dial("tcp", p.addr)
	if err != nil {
		return nil, err
	}

	p.conn, err = newWireChannel(conn)

	return p, err
}

func (p *wireProtocol) packInt(i int32) {
	// pack big endian int32
	p.buf = append(p.buf, byte(i>>24&0xFF))
	p.buf = append(p.buf, byte(i>>16&0xFF))
	p.buf = append(p.buf, byte(i>>8&0xFF))
	p.buf = append(p.buf, byte(i&0xFF))
}

func (p *wireProtocol) packBytes(b []byte) {
	for _, b := range xdrBytes(b) {
		p.buf = append(p.buf, b)
	}
}

func (p *wireProtocol) packString(s string) {
	for _, b := range xdrString(s) {
		p.buf = append(p.buf, b)
	}
}

func (p *wireProtocol) appendBytes(bs []byte) {
	for _, b := range bs {
		p.buf = append(p.buf, b)
	}
}

func getClientPublicBytes(clientPublic *big.Int) (bs []byte) {
	b := bytes.NewBufferString(hex.EncodeToString(bigToBytes(clientPublic))).Bytes()
	if len(b) > 254 {
		bs = bytes.Join([][]byte{
			[]byte{CNCT_specific_data, byte(255), 0}, b[:254],
			[]byte{CNCT_specific_data, byte(len(b)-254) + 1, 1}, b[254:],
		}, nil)
	} else {
		bs = bytes.Join([][]byte{
			[]byte{CNCT_specific_data, byte(len(b)) + 1, 0}, b,
		}, nil)
	}
	return bs
}

func (p *wireProtocol) uid(user string, password string, clientPublic *big.Int) []byte {
	sysUser := os.Getenv("USER")
	if sysUser == "" {
		sysUser = os.Getenv("USERNAME")
	}
	hostname, _ := os.Hostname()

	sysUserBytes := bytes.NewBufferString(sysUser).Bytes()
	hostnameBytes := bytes.NewBufferString(hostname).Bytes()
	pluginListNameBytes := bytes.NewBufferString(PLUGIN_LIST).Bytes()
	pluginNameBytes := bytes.NewBufferString(PLUGIN_NAME).Bytes()
	userBytes := bytes.NewBufferString(strings.ToUpper(user)).Bytes()

	return bytes.Join([][]byte{
		[]byte{CNCT_login, byte(len(userBytes))}, userBytes,
		[]byte{CNCT_plugin_name, byte(len(pluginNameBytes))}, pluginNameBytes,
		[]byte{CNCT_plugin_list, byte(len(pluginListNameBytes))}, pluginListNameBytes,
		getClientPublicBytes(clientPublic),
		[]byte{CNCT_client_crypt, 4, 1, 0, 0, 0},
		[]byte{CNCT_user, byte(len(sysUserBytes))}, sysUserBytes,
		[]byte{CNCT_host, byte(len(hostnameBytes))}, hostnameBytes,
		[]byte{CNCT_user_verification, 0},
	}, nil)
}

func (p *wireProtocol) sendPackets() (written int, err error) {
	debugPrint(p, fmt.Sprintf("\tsendPackets():%v", p.buf))
	n := 0
	for written < len(p.buf) {
		n, err = p.conn.Write(p.buf[written:])
		if err != nil {
			break
		}
		written += n
	}
	p.buf = make([]byte, 0, BUFFER_LEN)
	return
}

func (p *wireProtocol) recvPackets(n int) ([]byte, error) {
	buf := make([]byte, n)
	var err error
	read := 0
	totalRead := 0
	for totalRead < n {
		read, err = p.conn.Read(buf[totalRead:n])
		if err != nil {
			debugPrint(p, fmt.Sprintf("\trecvPackets():%v:%v", buf, err))
			return buf, err
		}
		totalRead += read
	}
	debugPrint(p, fmt.Sprintf("\trecvPackets():%v:%v", buf, err))
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
	for n != isc_arg_end {
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
			message = strings.Replace(message, "@"+strconv.Itoa(num_arg), strconv.Itoa(num), 1)
		case n == isc_arg_string || n == isc_arg_interpreted:
			b, err = p.recvPackets(4)
			nbytes := int(bytes_to_bint32(b))
			b, err = p.recvPacketsAlignment(nbytes)
			s := bytes_to_str(b)
			num_arg += 1
			message = strings.Replace(message, "@"+strconv.Itoa(num_arg), s, 1)
		case n == isc_arg_sql_state:
			b, err = p.recvPackets(4)
			nbytes := int(bytes_to_bint32(b))
			b, err = p.recvPacketsAlignment(nbytes)
			_ = bytes_to_str(b) // skip status code
		}
		b, err = p.recvPackets(4)
		n = bytes_to_bint32(b)
	}

	return gds_codes, sql_code, message, err
}

func (p *wireProtocol) _parse_op_response() (int32, int32, []byte, error) {
	b, err := p.recvPackets(16)
	h := bytes_to_bint32(b[0:4])            // Object handle
	oid := bytes_to_bint32(b[4:12])         // Object ID
	buf_len := int(bytes_to_bint32(b[12:])) // buffer length
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
	for item := int(buf[i]); item != isc_info_end; item = int(buf[i]) {
		i++
		switch item {
		case isc_info_sql_sqlda_seq:
			ln = int(bytes_to_int16(buf[i : i+2]))
			i += 2
			index = int(bytes_to_int32(buf[i : i+ln]))
			i += ln
		case isc_info_sql_type:
			ln = int(bytes_to_int16(buf[i : i+2]))
			i += 2
			sqltype := int(bytes_to_int32(buf[i : i+ln]))
			if sqltype%2 != 0 {
				sqltype--
			}
			xsqlda[index-1].sqltype = sqltype
			i += ln
		case isc_info_sql_sub_type:
			ln = int(bytes_to_int16(buf[i : i+2]))
			i += 2
			xsqlda[index-1].sqlsubtype = int(bytes_to_int32(buf[i : i+ln]))
			i += ln
		case isc_info_sql_scale:
			ln = int(bytes_to_int16(buf[i : i+2]))
			i += 2
			xsqlda[index-1].sqlscale = int(bytes_to_int32(buf[i : i+ln]))
			i += ln
		case isc_info_sql_length:
			ln = int(bytes_to_int16(buf[i : i+2]))
			i += 2
			xsqlda[index-1].sqllen = int(bytes_to_int32(buf[i : i+ln]))
			i += ln
		case isc_info_sql_null_ind:
			ln = int(bytes_to_int16(buf[i : i+2]))
			i += 2
			xsqlda[index-1].null_ok = bytes_to_int32(buf[i:i+ln]) != 0
			i += ln
		case isc_info_sql_field:
			ln = int(bytes_to_int16(buf[i : i+2]))
			i += 2
			xsqlda[index-1].fieldname = bytes_to_str(buf[i : i+ln])
			i += ln
		case isc_info_sql_relation:
			ln = int(bytes_to_int16(buf[i : i+2]))
			i += 2
			xsqlda[index-1].relname = bytes_to_str(buf[i : i+ln])
			i += ln
		case isc_info_sql_owner:
			ln = int(bytes_to_int16(buf[i : i+2]))
			i += 2
			xsqlda[index-1].ownname = bytes_to_str(buf[i : i+ln])
			i += ln
		case isc_info_sql_alias:
			ln = int(bytes_to_int16(buf[i : i+2]))
			i += 2
			xsqlda[index-1].aliasname = bytes_to_str(buf[i : i+ln])
			i += ln
		case isc_info_truncated:
			return index, err // return next index
		case isc_info_sql_describe_end:
			/* NOTHING */
		default:
			err = errors.New(fmt.Sprintf("Invalid item [%02x] ! i=%d", buf[i], i))
			break
		}
	}
	return -1, err // no more info
}

func (p *wireProtocol) parse_xsqlda(buf []byte, stmtHandle int32) (int32, []xSQLVAR, error) {
	var ln, col_len, next_index int
	var err error
	var stmt_type int32
	var rbuf []byte
	var xsqlda []xSQLVAR
	i := 0

	for i < len(buf) {
		if buf[i] == byte(isc_info_sql_stmt_type) {
			i += 1
			ln = int(bytes_to_int16(buf[i : i+2]))
			i += 2
			stmt_type = int32(bytes_to_int32(buf[i : i+ln]))
			i += ln
		} else if buf[i] == byte(isc_info_sql_select) && buf[i+1] == byte(isc_info_sql_describe_vars) {
			i += 2
			ln = int(bytes_to_int16(buf[i : i+2]))
			i += 2
			col_len = int(bytes_to_int32(buf[i : i+ln]))
			xsqlda = make([]xSQLVAR, col_len)
			next_index, err = p._parse_select_items(buf[i+ln:], xsqlda)
			for next_index > 0 { // more describe vars
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

func (p *wireProtocol) opConnect(dbName string, user string, password string, clientPublic *big.Int) {
	debugPrint(p, "opConnect")
	moreProtocol, _ := hex.DecodeString("ffff800b00000001000000000000000500000004ffff800c00000001000000000000000500000006ffff800d00000001000000000000000500000008")
	p.packInt(op_connect)
	p.packInt(op_attach)
	p.packInt(3)  // CONNECT_VERSION3
	p.packInt(36) // Arch type
	p.packString(dbName)
	p.packInt(4) // Protocol version understood count.
	p.packBytes(p.uid(strings.ToUpper(user), password, clientPublic))
	p.packInt(10) // PROTOCOL_VERSION10
	p.packInt(1)  // Arch type (Generic = 1)
	p.packInt(0)  // Min type
	p.packInt(5)  // Max type
	p.packInt(2)  // Preference weight
	p.appendBytes(moreProtocol)
	p.sendPackets()
}

func (p *wireProtocol) opCreate(dbName string, user string, password string) {
	debugPrint(p, "opCreate")
	var page_size int32
	page_size = 4096

	encode := bytes.NewBufferString("UTF8").Bytes()
	userBytes := bytes.NewBufferString(strings.ToUpper(user)).Bytes()
	passwordBytes := bytes.NewBufferString(password).Bytes()
	dpb := bytes.Join([][]byte{
		[]byte{1},
		[]byte{68, byte(len(encode))}, encode,
		[]byte{48, byte(len(encode))}, encode,
		[]byte{28, byte(len(userBytes))}, userBytes,
		[]byte{29, byte(len(passwordBytes))}, passwordBytes,
		[]byte{63, 4}, int32_to_bytes(3),
		[]byte{24, 4}, bint32_to_bytes(1),
		[]byte{54, 4}, bint32_to_bytes(1),
		[]byte{4, 4}, int32_to_bytes(page_size),
	}, nil)

	p.packInt(op_create)
	p.packInt(0) // Database Object ID
	p.packString(dbName)
	p.packBytes(dpb)
	p.sendPackets()
}

func (p *wireProtocol) opAccept(user string, password string, clientPublic *big.Int, clientSecret *big.Int) (err error) {
	debugPrint(p, "opAccept")

	b, err := p.recvPackets(4)
	opcode := bytes_to_bint32(b)

	for opcode == op_dummy {
		b, err = p.recvPackets(4)
	}

	if opcode == op_reject {
		return
	}
	if opcode == op_response {
		_, _, _, err = p._parse_op_response() // error occured
		return
	}

	b, _ = p.recvPackets(12)
	p.protocolVersion = int32(b[3])
	p.acceptArchitecture = bytes_to_bint32(b[4:8])
	p.acceptType = bytes_to_bint32(b[8:12])

	if opcode == op_cond_accept || opcode == op_accept_data {
		var readLength, ln int

		b, _ := p.recvPackets(4)
		ln = int(bytes_to_bint32(b))
		data, _ := p.recvPackets(ln)
		readLength = int(4 + ln)
		if readLength%4 != 0 {
			p.recvPackets(4 - readLength%4) // padding
			readLength += 4 - readLength%4
		}

		b, _ = p.recvPackets(4)
		ln = int(bytes_to_bint32(b))
		pluginName, _ := p.recvPackets(ln)
		readLength = int(4 + ln)
		if readLength%4 != 0 {
			p.recvPackets(4 - readLength%4) // padding
			readLength += 4 - readLength%4
		}
		p.pluginName = bytes_to_str(pluginName)

		b, _ = p.recvPackets(4)
		isAuthenticated := bytes_to_bint32(b)
		readLength += 4

		b, _ = p.recvPackets(4)
		ln = int(bytes_to_bint32(b))
		_, _ = p.recvPackets(ln) // keys
		readLength = int(4 + ln)
		if readLength%4 != 0 {
			p.recvPackets(4 - readLength%4) // padding
			readLength += 4 - readLength%4
		}

		if p.pluginName == "Legacy_Auth" && isAuthenticated == 0 {
			err = errors.New("opAccept() Unauthorized")
			return
		}

		if p.pluginName == "Srp" {
			ln = int(bytes_to_int16(data[:2]))
			serverSalt := data[2 : ln+2]
			serverPublic := bigFromHexString(bytes_to_str(data[4+ln:]))
			clientProof, authKey := getClientProof(strings.ToUpper(user), password, serverSalt, clientPublic, serverPublic, clientSecret)

			// Send op_cont_auth
			p.packInt(op_cont_auth)
			p.packString(hex.EncodeToString(clientProof))
			p.packString(PLUGIN_NAME)
			p.packString(PLUGIN_LIST)
			p.packString("")
			p.sendPackets()
			_, _, _, err = p.opResponse()
			if err != nil {
				return
			}

			// Send op_crypt
			p.packInt(op_crypt)
			p.packString("Arc4")
			p.packString("Symmetric")
			p.sendPackets()
			p.conn.setAuthKey(authKey)

			_, _, _, err = p.opResponse()
			if err != nil {
				return
			}

		}

	} else {
		if opcode != op_accept {
			err = errors.New("opAccept() protocol error")
			return
		}
	}

	return
}

func (p *wireProtocol) opAttach(dbName string, user string, password string) {
	debugPrint(p, "opAttach")
	encode := bytes.NewBufferString("UTF8").Bytes()
	userBytes := bytes.NewBufferString(strings.ToUpper(user)).Bytes()
	passwordBytes := bytes.NewBufferString(password).Bytes()

	dbp := bytes.Join([][]byte{
		[]byte{1},
		[]byte{48, byte(len(encode))}, encode,
		[]byte{28, byte(len(userBytes))}, userBytes,
		[]byte{29, byte(len(passwordBytes))}, passwordBytes,
	}, nil)
	p.packInt(op_attach)
	p.packInt(0) // Database Object ID
	p.packString(dbName)
	p.packBytes(dbp)
	p.sendPackets()
}

func (p *wireProtocol) opDropDatabase() {
	debugPrint(p, "opDropDatabase")
	p.packInt(op_drop_database)
	p.packInt(p.dbHandle)
	p.sendPackets()
}

func (p *wireProtocol) opTransaction(tpb []byte) {
	debugPrint(p, "opTransaction")
	p.packInt(op_transaction)
	p.packInt(p.dbHandle)
	p.packBytes(tpb)
	p.sendPackets()
}

func (p *wireProtocol) opCommit(transHandle int32) {
	debugPrint(p, "opCommit")
	p.packInt(op_commit)
	p.packInt(transHandle)
	p.sendPackets()
}

func (p *wireProtocol) opCommitRetaining(transHandle int32) {
	debugPrint(p, "opCommitRetaining")
	p.packInt(op_commit_retaining)
	p.packInt(transHandle)
	p.sendPackets()
}

func (p *wireProtocol) opRollback(transHandle int32) {
	debugPrint(p, "opRollback")
	p.packInt(op_rollback)
	p.packInt(transHandle)
	p.sendPackets()
}

func (p *wireProtocol) opRollbackRetaining(transHandle int32) {
	debugPrint(p, "opRollbackRetaining")
	p.packInt(op_rollback_retaining)
	p.packInt(transHandle)
	p.sendPackets()
}

func (p *wireProtocol) opAllocateStatement() {
	debugPrint(p, "opAllocateStatement")
	p.packInt(op_allocate_statement)
	p.packInt(p.dbHandle)
	p.sendPackets()
}

func (p *wireProtocol) opInfoTransaction(transHandle int32, b []byte) {
	debugPrint(p, "opInfoTransaction")
	p.packInt(op_info_transaction)
	p.packInt(transHandle)
	p.packInt(0)
	p.packBytes(b)
	p.packInt(int32(BUFFER_LEN))
	p.sendPackets()
}

func (p *wireProtocol) opInfoDatabase(bs []byte) {
	debugPrint(p, "opInfoDatabase")
	p.packInt(op_info_database)
	p.packInt(p.dbHandle)
	p.packInt(0)
	p.packBytes(bs)
	p.packInt(int32(BUFFER_LEN))
	p.sendPackets()
}

func (p *wireProtocol) opFreeStatement(stmtHandle int32, mode int32) {
	debugPrint(p, fmt.Sprintf("opFreeStatement:<%v>", stmtHandle))
	p.packInt(op_free_statement)
	p.packInt(stmtHandle)
	p.packInt(mode)
	p.sendPackets()
}

func (p *wireProtocol) opPrepareStatement(stmtHandle int32, transHandle int32, query string) {
	debugPrint(p, fmt.Sprintf("opPrepareStatement():%d,%d,%v", transHandle, stmtHandle, query))

	bs := bytes.Join([][]byte{
		[]byte{isc_info_sql_stmt_type},
		_INFO_SQL_SELECT_DESCRIBE_VARS(),
	}, nil)
	p.packInt(op_prepare_statement)
	p.packInt(transHandle)
	p.packInt(stmtHandle)
	p.packInt(3) // dialect = 3
	p.packString(query)
	p.packBytes(bs)
	p.packInt(int32(BUFFER_LEN))
	p.sendPackets()
}

func (p *wireProtocol) opInfoSql(stmtHandle int32, vars []byte) {
	debugPrint(p, "opInfoSql")
	p.packInt(op_info_sql)
	p.packInt(stmtHandle)
	p.packInt(0)
	p.packBytes(vars)
	p.packInt(int32(BUFFER_LEN))
	p.sendPackets()
}

func (p *wireProtocol) opExecute(stmtHandle int32, transHandle int32, params []driver.Value) {
	debugPrint(p, fmt.Sprintf("opExecute():%d,%d,%v", transHandle, stmtHandle, params))
	p.packInt(op_execute)
	p.packInt(stmtHandle)
	p.packInt(transHandle)

	if len(params) == 0 {
		p.packInt(0) // packBytes([])
		p.packInt(0)
		p.packInt(0)
		p.sendPackets()
	} else {
		blr, values := paramsToBlr(params, p.protocolVersion)
		p.packBytes(blr)
		p.packInt(0)
		p.packInt(1)
		p.appendBytes(values)
		p.sendPackets()
	}
}

func (p *wireProtocol) opExecute2(stmtHandle int32, transHandle int32, params []driver.Value, outputBlr []byte) {
	debugPrint(p, "opExecute2")
	p.packInt(op_execute2)
	p.packInt(stmtHandle)
	p.packInt(transHandle)

	if len(params) == 0 {
		p.packInt(0) // packBytes([])
		p.packInt(0)
		p.packInt(0)
	} else {
		blr, values := paramsToBlr(params, p.protocolVersion)
		p.packBytes(blr)
		p.packInt(0)
		p.packInt(1)
		p.appendBytes(values)
	}

	p.packBytes(outputBlr)
	p.packInt(0)
	p.sendPackets()
}

func (p *wireProtocol) opFetch(stmtHandle int32, blr []byte) {
	debugPrint(p, "opFetch")
	p.packInt(op_fetch)
	p.packInt(stmtHandle)
	p.packBytes(blr)
	p.packInt(0)
	p.packInt(400)
	p.sendPackets()
}

func (p *wireProtocol) opFetchResponse(stmtHandle int32, xsqlda []xSQLVAR) (*list.List, bool, error) {
	debugPrint(p, "opFetchResponse")
	b, err := p.recvPackets(4)
	for bytes_to_bint32(b) == op_dummy {
		b, _ = p.recvPackets(4)
	}

	if bytes_to_bint32(b) == op_response {
		p._parse_op_response() // error occured
		return nil, false, errors.New("opFetchResponse:Internal Error")
	}
	if bytes_to_bint32(b) != op_fetch_response {
		return nil, false, errors.New("opFetchResponse:Internal Error")
	}
	b, err = p.recvPackets(8)
	status := bytes_to_bint32(b[:4])
	count := int(bytes_to_bint32(b[4:8]))
	rows := list.New()

	for count > 0 {
		r := make([]driver.Value, len(xsqlda))
		if p.protocolVersion < PROTOCOL_VERSION13 {
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
		} else { // PROTOCOL_VERSION13
			bi256 := big.NewInt(256)
			n := len(xsqlda) / 8
			if len(xsqlda)%8 != 0 {
				n++
			}
			null_indicator := new(big.Int)
			b, _ := p.recvPacketsAlignment(n)
			for n = len(b) - 1; n > 0; n-- {
				null_indicator = null_indicator.Mul(null_indicator, bi256)
				bi := big.NewInt(int64(b[n]))
				null_indicator = null_indicator.Add(null_indicator, bi)
			}

			for i, x := range xsqlda {
				if null_indicator.Bit(1<<1) != 0 {
					continue
				}
				var ln int
				if x.ioLength() < 0 {
					b, err = p.recvPackets(4)
					ln = int(bytes_to_bint32(b))
				} else {
					ln = x.ioLength()
				}
				raw_value, _ := p.recvPacketsAlignment(ln)
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
	debugPrint(p, "opDetatch")
	p.packInt(op_detach)
	p.packInt(p.dbHandle)
	p.sendPackets()
}

func (p *wireProtocol) opOpenBlob(blobId int32, transHandle int32) {
	debugPrint(p, "opOpenBlob")
	p.packInt(op_open_blob)
	p.packInt(transHandle)
	p.packInt(blobId)
	p.sendPackets()
}

func (p *wireProtocol) opCreateBlob2(transHandle int32) {
	debugPrint(p, "opCreateBlob2")
	p.packInt(op_create_blob2)
	p.packInt(0)
	p.packInt(transHandle)
	p.packInt(0)
	p.packInt(0)
	p.sendPackets()
}

func (p *wireProtocol) opGetSegment(blobHandle int32) {
	debugPrint(p, "opGetSegment")
	p.packInt(op_get_segment)
	p.packInt(blobHandle)
	p.packInt(int32(BUFFER_LEN))
	p.packInt(0)
	p.sendPackets()
}

func (p *wireProtocol) opBatchSegments(blobHandle int32, seg_data []byte) {
	debugPrint(p, "opBatchSegments")
	ln := len(seg_data)
	p.packInt(op_batch_segments)
	p.packInt(blobHandle)
	p.packInt(int32(ln + 2))
	p.packInt(int32(ln + 2))
	pad_length := ((4 - (ln + 2)) & 3)
	padding := make([]byte, pad_length)
	p.packBytes([]byte{byte(ln & 255), byte(ln >> 8)}) // little endian int16
	p.packBytes(seg_data)
	p.packBytes(padding)
	p.sendPackets()
}

func (p *wireProtocol) opCloseBlob(blobHandle int32) {
	debugPrint(p, "opCloseBlob")
	p.packInt(op_close_blob)
	p.packInt(blobHandle)
	p.sendPackets()
}

func (p *wireProtocol) opResponse() (int32, int32, []byte, error) {
	debugPrint(p, "opResponse")
	for p.lazyResponseCount > 0 {
		p.lazyResponseCount--
		_, _, _, _ = p.opResponse()
	}
	b, _ := p.recvPackets(4)
	for bytes_to_bint32(b) == op_dummy {
		b, _ = p.recvPackets(4)
	}

	if bytes_to_bint32(b) != op_response {
		return 0, 0, nil, errors.New("Error op_response")
	}
	return p._parse_op_response()
}

func (p *wireProtocol) opSqlResponse(xsqlda []xSQLVAR) ([]driver.Value, error) {
	debugPrint(p, "opSqlResponse")
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

	if p.protocolVersion < PROTOCOL_VERSION13 {
		for i, x := range xsqlda {
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
	} else { // PROTOCOL_VERSION13
		n := len(xsqlda) / 8
		if len(xsqlda)%8 != 0 {
			n++
		}
		null_indicator := 0
		b, _ := p.recvPacketsAlignment(n)
		for n = len(b) - 1; n > 0; n-- {
			null_indicator <<= 8
			null_indicator += int(b[n])
		}

		for i, x := range xsqlda {
			if (null_indicator & (1 << 1)) != 0 {
				continue
			}
			if x.ioLength() < 0 {
				b, err = p.recvPackets(4)
				ln = int(bytes_to_bint32(b))
			} else {
				ln = x.ioLength()
			}
			raw_value, _ := p.recvPacketsAlignment(ln)
			r[i], err = x.value(raw_value)
		}
	}

	return r, err
}
