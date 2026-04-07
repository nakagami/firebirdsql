/*******************************************************************************
The MIT License (MIT)

Copyright (c) 2013-2024 Hajime Nakagami

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
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"net"
	"os"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/kardianos/osext"
	"gitlab.com/nyarla/go-crypt"
	// "unsafe"
)

const (
	PLUGIN_LIST       = "Srp256,Srp,Legacy_Auth"
	BUFFER_LEN        = 1024
	MAX_CHAR_LENGTH   = 32767
	BLOB_SEGMENT_SIZE = 32000
)

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

func _INFO_SQL_BIND_DESCRIBE_VARS() []byte {
	return []byte{
		isc_info_sql_bind,
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
	authData   []byte

	charset        string
	charsetByteLen int

	// Time Zone
	timezone string
}

func newWireProtocol(addr string, timezone string, charset string) (*wireProtocol, error) {
	p := new(wireProtocol)
	p.buf = make([]byte, 0, BUFFER_LEN)

	p.addr = addr
	conn, err := net.Dial("tcp", p.addr)
	if err != nil {
		return nil, err
	}

	p.conn, err = newWireChannel(conn)
	p.timezone = timezone
	p.charset = charset
	p.charsetLen()

	return p, err
}

// charsetLen sets the length of character depending the charset to get the correct size of column
func (p *wireProtocol) charsetLen() {
	// all ISO8859_X and WIN125X are 1 byte character length, so omit here
	// only add charset that character length is > 1
	switch p.charset {
	case "UNICODE_FSS", "UTF8":
		p.charsetByteLen = 4
	case "BIG_5", "SJIS_0208", "KSC_5601", "EUCJ_0208", "GB_2312", "KOI8R", "KOI8U":
		p.charsetByteLen = 2
	default:
		p.charsetByteLen = 1
	}
}

func (p *wireProtocol) packInt(i int32) {
	// pack big endian int32
	p.buf = append(p.buf, []byte{byte(i >> 24 & 0xFF), byte(i >> 16 & 0xFF), byte(i >> 8 & 0xFF), byte(i & 0xFF)}...)
}

func (p *wireProtocol) packBytes(b []byte) {
	p.buf = append(p.buf, xdrBytes(b)...)
}

func (p *wireProtocol) packString(s string) {
	p.buf = append(p.buf, xdrBytes([]byte(p.encodeString(s)))...)
}

func (p *wireProtocol) appendBytes(bs []byte) {
	p.buf = append(p.buf, bs...)
}

func getSrpClientPublicBytes(clientPublic *big.Int) (bs []byte) {
	b := []byte(hex.EncodeToString(bigIntToBytes(clientPublic)))
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

func (p *wireProtocol) uid(user string, password string, authPluginName string, wireCrypt bool, clientPublic *big.Int) []byte {
	sysUser := os.Getenv("USER")
	if sysUser == "" {
		sysUser = os.Getenv("USERNAME")
	}
	hostname, _ := os.Hostname()

	sysUserBytes := []byte(sysUser)
	hostnameBytes := []byte(hostname)
	pluginListNameBytes := []byte(PLUGIN_LIST)
	pluginNameBytes := []byte(authPluginName)
	userBytes := []byte(strings.ToUpper(user))
	var wireCryptByte byte
	if wireCrypt {
		wireCryptByte = 1
	} else {
		wireCryptByte = 0
	}

	var specific_data []byte
	if authPluginName == "Srp" || authPluginName == "Srp256" {
		specific_data = getSrpClientPublicBytes(clientPublic)
	} else if authPluginName == "Legacy_Auth" {
		b := []byte(crypt.Crypt(password, "9z")[2:])
		specific_data = bytes.Join([][]byte{
			[]byte{CNCT_specific_data, byte(len(b)) + 1, 0}, b,
		}, nil)
	} else {
		panic(fmt.Sprintf("Unknown plugin name:%s", authPluginName))
	}

	return bytes.Join([][]byte{
		[]byte{CNCT_login, byte(len(userBytes))}, userBytes,
		[]byte{CNCT_plugin_name, byte(len(pluginNameBytes))}, pluginNameBytes,
		[]byte{CNCT_plugin_list, byte(len(pluginListNameBytes))}, pluginListNameBytes,
		specific_data,
		[]byte{CNCT_client_crypt, 4, wireCryptByte, 0, 0, 0},
		[]byte{CNCT_user, byte(len(sysUserBytes))}, sysUserBytes,
		[]byte{CNCT_host, byte(len(hostnameBytes))}, hostnameBytes,
		[]byte{CNCT_user_verification, 0},
	}, nil)
}

func (p *wireProtocol) sendPackets() (written int, err error) {
	p.debugPrint("\tsendPackets():%v", p.buf)
	n := 0
	for written < len(p.buf) {
		n, err = p.conn.Write(p.buf[written:])
		if err != nil {
			// error while sending the package....
			err = driver.ErrBadConn
			break
		}
		written += n
	}
	p.conn.Flush()
	p.buf = p.buf[:0]
	return
}

func (p *wireProtocol) suspendBuffer() []byte {
	p.debugPrint("\tsuspendBuffer():%v", p.buf)
	buf := p.buf
	p.buf = make([]byte, 0, BUFFER_LEN)
	return buf
}

func (p *wireProtocol) resumeBuffer(buf []byte) {
	p.debugPrint("\tresumeBuffer():%v", buf)
	p.buf = buf
}

func (p *wireProtocol) recvPackets(n int) ([]byte, error) {
	buf := make([]byte, n)
	var err error
	read := 0
	totalRead := 0
	for totalRead < n {
		read, err = p.conn.Read(buf[totalRead:n])
		if err != nil {
			p.debugPrint("\trecvPackets():%v:%v", buf, err)
			return buf, err
		}
		totalRead += read
	}
	p.debugPrint("\trecvPackets():%v:%v", buf, err)
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

func (p *wireProtocol) _parse_status_vector() ([]int, int, string, error) {
	sql_code := 0
	gds_code := 0
	gds_codes := make([]int, 0)
	num_arg := 0
	message := ""

	b, err := p.recvPackets(4)
	if err != nil {
		return gds_codes, sql_code, message, err
	}
	n := bytes_to_bint32(b)
	for n != isc_arg_end {
		switch {
		case n == isc_arg_gds:
			b, err = p.recvPackets(4)
			if err != nil {
				return gds_codes, sql_code, message, err
			}
			gds_code = int(bytes_to_bint32(b))
			if gds_code != 0 {
				gds_codes = append(gds_codes, gds_code)
				if msg, ok := errmsgs[gds_code]; ok {
					message += msg
				} else {
					message += fmt.Sprintf("unknown gds_code: %d", gds_code)
				}
				num_arg = 0
			}
		case n == isc_arg_number:
			b, err = p.recvPackets(4)
			if err != nil {
				return gds_codes, sql_code, message, err
			}
			num := int(bytes_to_bint32(b))
			if gds_code == 335544436 {
				sql_code = num
			}
			num_arg++
			message = strings.Replace(message, "@"+strconv.Itoa(num_arg), strconv.Itoa(num), 1)
		case n == isc_arg_string:
			b, err = p.recvPackets(4)
			if err != nil {
				return gds_codes, sql_code, message, err
			}
			nbytes := int(bytes_to_bint32(b))
			b, err = p.recvPacketsAlignment(nbytes)
			if err != nil {
				return gds_codes, sql_code, message, err
			}
			s := bytes_to_str(b)
			num_arg++
			message = strings.Replace(message, "@"+strconv.Itoa(num_arg), s, 1)
		case n == isc_arg_interpreted:
			b, err = p.recvPackets(4)
			if err != nil {
				return gds_codes, sql_code, message, err
			}
			nbytes := int(bytes_to_bint32(b))
			b, err = p.recvPacketsAlignment(nbytes)
			if err != nil {
				return gds_codes, sql_code, message, err
			}
			s := bytes_to_str(b)
			message += s
		case n == isc_arg_sql_state:
			b, err = p.recvPackets(4)
			if err != nil {
				return gds_codes, sql_code, message, err
			}
			nbytes := int(bytes_to_bint32(b))
			b, err = p.recvPacketsAlignment(nbytes)
			if err != nil {
				return gds_codes, sql_code, message, err
			}
			_ = bytes_to_str(b) // skip status code
		}
		b, err = p.recvPackets(4)
		if err != nil {
			return gds_codes, sql_code, message, err
		}
		n = bytes_to_bint32(b)
	}

	return gds_codes, sql_code, message, err
}

func (p *wireProtocol) _parse_op_response() (int32, []byte, []byte, error) {
	// Receive first packet: Object handle, ID, and buffer length
	b, err := p.recvPackets(16)
	if err != nil {
		return 0, nil, nil, err
	}

	h := bytes_to_bint32(b[0:4])            // Object handle
	oid := b[4:12]                          // Object ID
	buf_len := int(bytes_to_bint32(b[12:])) // Buffer length

	// Receive data buffer if length is greater than zero
	var buf []byte
	if buf_len > 0 {
		buf, err = p.recvPacketsAlignment(buf_len)
		if err != nil {
			return h, oid, nil, err
		}
	}

	// Parse status vector for database-side errors
	gds_codes, sql_code, message, errV := p._parse_status_vector()
	if errV != nil {
		// Wrap protocol/network error
		return h, oid, buf, fmt.Errorf("protocol error during status vector parsing: %w", errV)
	}

	// Check if any Firebird errors were returned in the status vector
	if len(gds_codes) > 0 || sql_code != 0 {
		return h, oid, buf, &FbError{
			GDSCodes: gds_codes,
			Message:  message,
		}
	}

	return h, oid, buf, nil
}

func (p *wireProtocol) _guess_wire_crypt(buf []byte) (string, []byte) {
	var available_plugins []string
	plugin_nonce := make([][]byte, 0, 2)

	i := 0
	for i = 0; i < len(buf); {
		t := buf[i]
		i++
		ln := int(buf[i])
		i++
		v := buf[i : i+ln]
		i += ln
		if t == 1 {
			available_plugins = strings.Split(string(v), " ")
		} else if t == 3 {
			plugin_nonce = append(plugin_nonce, v)
		}
	}
	if slices.Contains(available_plugins, "ChaCha64") {
		for _, nonce := range plugin_nonce {
			if reflect.DeepEqual(nonce[:9], []byte{'C', 'h', 'a', 'C', 'h', 'a', '6', '4', 0}) {
				return "ChaCha64", nonce[9:]
			}
		}
	} else if slices.Contains(available_plugins, "ChaCha") {
		for _, nonce := range plugin_nonce {
			if reflect.DeepEqual(nonce[:7], []byte{'C', 'h', 'a', 'C', 'h', 'a', 0}) {
				return "ChaCha", nonce[7 : 7+12]
			}
		}
	} else if slices.Contains(available_plugins, "Arc4") {
		return "Arc4", nil
	}
	return "", nil
}

func (p *wireProtocol) _parse_connect_response(user string, password string, options map[string]string, clientPublic *big.Int, clientSecret *big.Int) (err error) {
	p.debugPrint("_parse_connect_response")

	b, err := p.recvPackets(4)
	opcode := bytes_to_bint32(b)

	for opcode == op_dummy {
		b, _ = p.recvPackets(4)
		opcode = bytes_to_bint32(b)
	}

	if opcode == op_reject {
		err = errors.New("_parse_connect_response() op_reject")
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
	p.user = user
	p.password = password

	// Check if server accepted compression
	if (p.acceptType & pflag_compress) != 0 {
		p.conn.enableCompression()
		p.acceptType = p.acceptType & ptype_MASK
	}

	if opcode == op_cond_accept || opcode == op_accept_data {
		var readLength, ln int

		b, _ := p.recvPackets(4)
		ln = int(bytes_to_bint32(b))
		data, _ := p.recvPacketsAlignment(ln)

		b, _ = p.recvPackets(4)
		ln = int(bytes_to_bint32(b))
		pluginName, _ := p.recvPacketsAlignment(ln)
		p.pluginName = bytes_to_str(pluginName)

		b, _ = p.recvPackets(4)
		isAuthenticated := bytes_to_bint32(b)
		readLength += 4

		b, _ = p.recvPackets(4)
		ln = int(bytes_to_bint32(b))
		_, _ = p.recvPacketsAlignment(ln) // keys

		var authData []byte
		var sessionKey []byte
		if isAuthenticated == 0 {
			if p.pluginName == "Srp" || p.pluginName == "Srp256" {

				// TODO: normalize user

				if len(data) == 0 {
					p.opContAuth(bigIntToBytes(clientPublic), p.pluginName, PLUGIN_LIST, "")
					b, _ := p.recvPackets(4)
					op := bytes_to_bint32(b)
					if op == op_response {
						_, _, _, err = p._parse_op_response() // error occurred
						return
					}

					if op != op_cont_auth {
						err = errors.New("Your user name and password are not defined. Ask your database administrator to set up a Firebird login.\n")
						return
					}

					b, _ = p.recvPackets(4)
					ln = int(bytes_to_bint32(b))
					data, _ = p.recvPacketsAlignment(ln)

					b, _ = p.recvPackets(4)
					ln = int(bytes_to_bint32(b))
					_, _ = p.recvPacketsAlignment(ln) // pluginName

					b, _ = p.recvPackets(4)
					ln = int(bytes_to_bint32(b))
					_, _ = p.recvPacketsAlignment(ln) // pluginList

					b, _ = p.recvPackets(4)
					ln = int(bytes_to_bint32(b))
					_, _ = p.recvPacketsAlignment(ln) // keys
				}
				if len(data) == 0 {
					err = errors.New("Your user name and password are not defined. Ask your database administrator to set up a Firebird login.\n")
					return
				}

				ln = int(bytes_to_int16(data[:2]))
				serverSalt := data[2 : ln+2]
				serverPublic := bigIntFromHexString(bytes_to_str(data[4+ln:]))
				authData, sessionKey = getClientProof(strings.ToUpper(user), password, serverSalt, clientPublic, serverPublic, clientSecret, p.pluginName)
				if DEBUG_SRP {
					fmt.Printf("pluginName=%s\nserverSalt=%s\nserverPublic(bin)=%s\nserverPublic=%s\nauthData=%v,sessionKey=%v\n",
						p.pluginName, serverSalt, data[4+ln:], serverPublic, authData, sessionKey)
				}
			} else if p.pluginName == "Legacy_Auth" {
				authData = []byte(crypt.Crypt(password, "9z")[2:])
			} else {
				err = errors.New("Your user name and password are not defined. Ask your database administrator to set up a Firebird login.\n")
				return
			}
		}

		var enc_plugin string
		var nonce []byte

		if opcode == op_cond_accept {
			p.opContAuth(authData, options["auth_plugin_name"], PLUGIN_LIST, "")
			var buf []byte
			_, _, buf, err = p.opResponse()
			if err != nil {
				return
			}
			enc_plugin, nonce = p._guess_wire_crypt(buf)
		}

		wire_crypt, _ := strconv.ParseBool(options["wire_crypt"])
		if enc_plugin != "" && wire_crypt && sessionKey != nil {
			// Send op_crypt
			p.opCrypt(enc_plugin)
			p.conn.setCryptKey(enc_plugin, sessionKey, nonce)
			_, _, _, err = p.opResponse()
			if err != nil {
				return
			}
		} else {
			p.authData = authData // use later opAttach and opCreate
		}

	} else {
		if opcode != op_accept {
			err = errors.New("_parse_connect_response() protocol error")
			return
		}
	}

	return
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
			// the length defined in buffer depends on character length of charset
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
			err = fmt.Errorf("Invalid item [%02x] ! i=%d", buf[i], i)
		}
	}
	return -1, err // no more info
}

func (p *wireProtocol) parse_xsqlda(buf []byte, stmtHandle int32) (int32, []xSQLVAR, []xSQLVAR, error) {
	var ln, col_len, next_index int
	var err error
	var stmt_type int32
	var xsqlda []xSQLVAR
	i := 0

	for i < len(buf) {
		if buf[i] == byte(isc_info_sql_stmt_type) && buf[i+1] == byte(0x04) && buf[i+2] == byte(0x00) {
			i++
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

				_, _, buf, err = p.opResponse()
				// buf[:2] == []byte{0x04,0x07}
				ln = int(bytes_to_int16(buf[2:4]))
				// bytes_to_int(buf[4:4+l]) == col_len
				next_index, err = p._parse_select_items(buf[4+ln:], xsqlda)
			}
		} else {
			break
		}
	}

	// Fetch input (bind) parameter metadata via a separate opInfoSql call.
	var inputXsqlda []xSQLVAR
	if err == nil {
		inputXsqlda, err = p._fetchBindXsqlda(stmtHandle)
	}

	return stmt_type, xsqlda, inputXsqlda, err
}

// _fetchBindXsqlda retrieves input (bind) parameter metadata for a prepared statement.
func (p *wireProtocol) _fetchBindXsqlda(stmtHandle int32) ([]xSQLVAR, error) {
	err := p.opInfoSql(stmtHandle, _INFO_SQL_BIND_DESCRIBE_VARS())
	if err != nil {
		return nil, err
	}
	_, _, buf, err := p.opResponse()
	if err != nil {
		return nil, err
	}
	if len(buf) < 6 || buf[0] != byte(isc_info_sql_bind) || buf[1] != byte(isc_info_sql_describe_vars) {
		return nil, nil
	}
	ln := int(bytes_to_int16(buf[2:4]))
	col_len := int(bytes_to_int32(buf[4 : 4+ln]))
	inputXsqlda := make([]xSQLVAR, col_len)
	next_index, err := p._parse_select_items(buf[4+ln:], inputXsqlda)
	for next_index > 0 {
		p.opInfoSql(stmtHandle,
			bytes.Join([][]byte{
				[]byte{isc_info_sql_sqlda_start, 2},
				int16_to_bytes(int16(next_index)),
				_INFO_SQL_BIND_DESCRIBE_VARS(),
			}, nil))
		_, _, buf, err = p.opResponse()
		ln = int(bytes_to_int16(buf[2:4]))
		next_index, err = p._parse_select_items(buf[4+ln:], inputXsqlda)
	}
	return inputXsqlda, err
}

func (p *wireProtocol) getBlobSegments(blobId []byte, transHandle int32) ([]byte, error) {
	suspendBuf := p.suspendBuffer()
	blob := []byte{}
	p.opOpenBlob2(blobId, transHandle)
	blobHandle, _, _, err := p.opResponse()
	if err != nil {
		p.resumeBuffer(suspendBuf)
		return nil, err
	}

	var rbuf []byte
	var more_data int32
	more_data = 1
	for more_data != 2 {
		p.opGetSegment(blobHandle)
		more_data, _, rbuf, err = p.opResponse()
		buf := rbuf
		for len(buf) > 0 {
			ln := int(bytes_to_int16(buf[0:2]))
			blob = append(blob, buf[2:ln+2]...)
			buf = buf[ln+2:]
		}
	}

	p.opCloseBlob(blobHandle)
	if (p.acceptType & ptype_MASK) == ptype_lazy_send {
		p.lazyResponseCount++
	} else {
		_, _, _, err = p.opResponse()
	}

	p.resumeBuffer(suspendBuf)
	return blob, err
}

func (p *wireProtocol) opConnect(dbName string, user string, password string, options map[string]string, clientPublic *big.Int) error {
	p.debugPrint("opConnect")
	wire_crypt := true
	wire_crypt, _ = strconv.ParseBool(options["wire_crypt"]) // errors default to false
	wire_compress := false
	wire_compress, _ = strconv.ParseBool(options["wire_compress"]) // errors default to false

	var protocols []string
	if wire_compress {
		// PROTOCOL_VERSION, Arch type (Generic=1), min, max|pflag_compress, weight
		protocols = []string{
			"0000000a00000001000000000000000500000002", // 10, 1, 0, 5, 2
			"ffff800b00000001000000000000000500000004", // 11, 1, 0, 5, 4
			"ffff800c00000001000000000000000500000006", // 12, 1, 0, 5, 6
			"ffff800d00000001000000000000010500000008", // 13, 1, 0, 0x105, 8
			"ffff800e0000000100000000000001050000000a", // 14, 1, 0, 0x105, 10
			"ffff800f0000000100000000000001050000000c", // 15, 1, 0, 0x105, 12
			"ffff80100000000100000000000001050000000e", // 16, 1, 0, 0x105, 14
			"ffff801100000001000000000000010500000010", // 17, 1, 0, 0x105, 16
		}
	} else {
		// PROTOCOL_VERSION, Arch type (Generic=1), min, max, weight
		protocols = []string{
			"0000000a00000001000000000000000500000002", // 10, 1, 0, 5, 2
			"ffff800b00000001000000000000000500000004", // 11, 1, 0, 5, 4
			"ffff800c00000001000000000000000500000006", // 12, 1, 0, 5, 6
			"ffff800d00000001000000000000000500000008", // 13, 1, 0, 5, 8
			"ffff800e0000000100000000000000050000000a", // 14, 1, 0, 5, 10
			"ffff800f0000000100000000000000050000000c", // 15, 1, 0, 5, 12
			"ffff80100000000100000000000000050000000e", // 16, 1, 0, 5, 14
			"ffff801100000001000000000000000500000010", // 17, 1, 0, 5, 16
		}
	}
	p.packInt(op_connect)
	p.packInt(op_attach)
	p.packInt(3) // CONNECT_VERSION3
	p.packInt(1) // Arch type(GENERIC)
	p.packString(dbName)
	p.packInt(int32(len(protocols)))
	p.packBytes(p.uid(strings.ToUpper(user), password, options["auth_plugin_name"], wire_crypt, clientPublic))
	buf, _ := hex.DecodeString(strings.Join(protocols, ""))
	p.appendBytes(buf)
	_, err := p.sendPackets()
	return err
}

// appendAuthAndTimezone appends auth data and session timezone to a DPB byte slice.
func (p *wireProtocol) appendAuthAndTimezone(dpb []byte) []byte {
	if p.authData != nil {
		specificAuthData := []byte(hex.EncodeToString(p.authData))
		dpb = bytes.Join([][]byte{
			dpb,
			{isc_dpb_specific_auth_data, byte(len(specificAuthData))}, specificAuthData}, nil)
	}
	if p.timezone != "" {
		tznameBytes := []byte(p.timezone)
		dpb = bytes.Join([][]byte{
			dpb,
			{isc_dpb_session_time_zone, byte(len(tznameBytes))}, tznameBytes}, nil)
	}
	return dpb
}

func (p *wireProtocol) opCreate(dbName string, user string, password string, role string) error {
	p.debugPrint("opCreate")
	var page_size int32
	page_size = 4096

	encode := []byte(p.charset)
	userBytes := []byte(strings.ToUpper(user))
	passwordBytes := []byte(password)
	roleBytes := []byte(role)
	dpb := bytes.Join([][]byte{
		[]byte{isc_dpb_version1},
		[]byte{isc_dpb_set_db_charset, byte(len(encode))}, encode,
		[]byte{isc_dpb_lc_ctype, byte(len(encode))}, encode,
		[]byte{isc_dpb_user_name, byte(len(userBytes))}, userBytes,
		[]byte{isc_dpb_password, byte(len(passwordBytes))}, passwordBytes,
		[]byte{isc_dpb_sql_role_name, byte(len(roleBytes))}, roleBytes,
		[]byte{isc_dpb_sql_dialect, 4}, int32_to_bytes(3),
		[]byte{isc_dpb_force_write, 4}, bint32_to_bytes(1),
		[]byte{isc_dpb_overwrite, 4}, bint32_to_bytes(1),
		[]byte{isc_dpb_page_size, 4}, int32_to_bytes(page_size),
		[]byte{isc_dpb_utf8_filename, 1, 1},
	}, nil)

	dpb = p.appendAuthAndTimezone(dpb)

	p.packInt(op_create)
	p.packInt(0) // Database Object ID
	p.packString(dbName)
	p.packBytes(dpb)
	_, err := p.sendPackets()
	return err
}

func (p *wireProtocol) opAttach(dbName string, user string, password string, role string) error {
	p.debugPrint("opAttach")
	encode := []byte(p.charset)
	userBytes := []byte(strings.ToUpper(user))
	passwordBytes := []byte(password)
	roleBytes := []byte(role)

	processName, err := osext.Executable()
	var processNameBytes []byte
	if err == nil {
		if len(processName) > 255 {
			//limit process name to last 255 symbols
			processName = processName[len(processName)-255:]
		}

		processNameBytes = []byte(processName)
	}
	pid := int32(os.Getpid())

	dpb := bytes.Join([][]byte{
		[]byte{isc_dpb_version1},
		[]byte{isc_dpb_sql_dialect, 4}, int32_to_bytes(3),
		[]byte{isc_dpb_lc_ctype, byte(len(encode))}, encode,
		[]byte{isc_dpb_user_name, byte(len(userBytes))}, userBytes,
		[]byte{isc_dpb_password, byte(len(passwordBytes))}, passwordBytes,
		[]byte{isc_dpb_sql_role_name, byte(len(roleBytes))}, roleBytes,
		[]byte{isc_dpb_process_id, 4}, int32_to_bytes(pid),
		[]byte{isc_dpb_process_name, byte(len(processNameBytes))}, processNameBytes,
		[]byte{isc_dpb_utf8_filename, 1, 1},
	}, nil)

	dpb = p.appendAuthAndTimezone(dpb)

	p.packInt(op_attach)
	p.packInt(0) // Database Object ID
	p.packString(dbName)
	p.packBytes(dpb)
	_, err = p.sendPackets()
	return err
}

func (p *wireProtocol) opContAuth(authData []byte, authPluginName string, authPluginList string, keys string) error {
	p.debugPrint("opContAuth")
	p.packInt(op_cont_auth)
	p.packString(hex.EncodeToString(authData))
	p.packString(authPluginName)
	p.packString(authPluginList)
	p.packString(keys)
	_, err := p.sendPackets()
	return err
}

func (p *wireProtocol) opCrypt(plugin string) error {
	p.packInt(op_crypt)
	p.packString(plugin)
	p.packString("Symmetric")
	_, err := p.sendPackets()
	return err
}

func (p *wireProtocol) opCryptCallback() error {
	p.debugPrint("opCryptCallback")
	p.packInt(op_crypt_key_callback)
	p.packInt(0)
	p.packInt(int32(BUFFER_LEN))
	_, err := p.sendPackets()
	return err
}

func (p *wireProtocol) opDropDatabase() error {
	p.debugPrint("opDropDatabase")
	p.packInt(op_drop_database)
	p.packInt(p.dbHandle)
	_, err := p.sendPackets()
	return err
}

func (p *wireProtocol) opTransaction(tpb []byte) error {
	p.debugPrint("opTransaction")
	p.packInt(op_transaction)
	p.packInt(p.dbHandle)
	p.packBytes(tpb)
	_, err := p.sendPackets()
	return err
}

func (p *wireProtocol) opCommit(transHandle int32) error {
	p.debugPrint("opCommit():%d", transHandle)
	p.packInt(op_commit)
	p.packInt(transHandle)
	_, err := p.sendPackets()
	return err
}

func (p *wireProtocol) opCommitRetaining(transHandle int32) error {
	p.debugPrint("opCommitRetaining():%d", transHandle)
	p.packInt(op_commit_retaining)
	p.packInt(transHandle)
	_, err := p.sendPackets()
	return err
}

func (p *wireProtocol) opRollback(transHandle int32) error {
	p.debugPrint("opRollback():%d", transHandle)
	p.packInt(op_rollback)
	p.packInt(transHandle)
	_, err := p.sendPackets()
	return err
}

func (p *wireProtocol) opRollbackRetaining(transHandle int32) error {
	p.debugPrint("opRollbackRetaining():%d", transHandle)
	p.packInt(op_rollback_retaining)
	p.packInt(transHandle)
	_, err := p.sendPackets()
	return err
}

func (p *wireProtocol) opAllocateStatement() error {
	p.debugPrint("opAllocateStatement")
	p.packInt(op_allocate_statement)
	p.packInt(p.dbHandle)
	_, err := p.sendPackets()
	return err
}

func (p *wireProtocol) opInfoTransaction(transHandle int32, b []byte) error {
	p.debugPrint("opInfoTransaction")
	p.packInt(op_info_transaction)
	p.packInt(transHandle)
	p.packInt(0)
	p.packBytes(b)
	p.packInt(int32(BUFFER_LEN))
	_, err := p.sendPackets()
	return err
}

func (p *wireProtocol) opInfoDatabase(bs []byte) error {
	p.debugPrint("opInfoDatabase")
	p.packInt(op_info_database)
	p.packInt(p.dbHandle)
	p.packInt(0)
	p.packBytes(bs)
	p.packInt(int32(BUFFER_LEN))
	_, err := p.sendPackets()
	return err
}

func (p *wireProtocol) opFreeStatement(stmtHandle int32, mode int32) error {
	p.debugPrint("opFreeStatement:<%v>", stmtHandle)
	p.packInt(op_free_statement)
	p.packInt(stmtHandle)
	p.packInt(mode)
	_, err := p.sendPackets()
	return err
}

func (p *wireProtocol) opPrepareStatement(stmtHandle int32, transHandle int32, query string) error {
	p.debugPrint("opPrepareStatement():%d,%d,%v", transHandle, stmtHandle, query)

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
	_, err := p.sendPackets()
	return err
}

func (p *wireProtocol) opInfoSql(stmtHandle int32, vars []byte) error {
	p.debugPrint("opInfoSql")
	p.packInt(op_info_sql)
	p.packInt(stmtHandle)
	p.packInt(0)
	p.packBytes(vars)
	p.packInt(int32(BUFFER_LEN))
	_, err := p.sendPackets()
	return err
}

func (p *wireProtocol) opExecute(stmt *firebirdsqlStmt, params []driver.Value, inputXsqlda []xSQLVAR) error {
	stmtHandle := stmt.stmtHandle
	transHandle := stmt.fc.tx.transHandle
	p.debugPrint("opExecute():%d,%d,%v", transHandle, stmtHandle, params)
	p.packInt(op_execute)
	p.packInt(stmtHandle)
	p.packInt(transHandle)

	if len(params) == 0 {
		p.packInt(0) // packBytes([])
		p.packInt(0)
		p.packInt(0)
	} else {
		blr, values := p.paramsToBlr(transHandle, params, p.protocolVersion, inputXsqlda)
		p.packBytes(blr)
		p.packInt(0)
		p.packInt(1)
		p.appendBytes(values)
	}
	if p.protocolVersion >= PROTOCOL_VERSION16 {
		// statement timeout
		p.appendBytes(bint32_to_bytes(0))
	}
	_, err := p.sendPackets()
	return err
}

func (p *wireProtocol) opExecute2(stmt *firebirdsqlStmt, params []driver.Value, outputBlr []byte, inputXsqlda []xSQLVAR) error {
	stmtHandle := stmt.stmtHandle
	transHandle := stmt.fc.tx.transHandle
	p.debugPrint("opExecute2")
	p.packInt(op_execute2)
	p.packInt(stmtHandle)
	p.packInt(transHandle)

	if len(params) == 0 {
		p.packInt(0) // packBytes([])
		p.packInt(0)
		p.packInt(0)
	} else {
		blr, values := p.paramsToBlr(transHandle, params, p.protocolVersion, inputXsqlda)
		p.packBytes(blr)
		p.packInt(0)
		p.packInt(1)
		p.appendBytes(values)
	}

	p.packBytes(outputBlr)
	p.packInt(0)

	if p.protocolVersion >= PROTOCOL_VERSION16 {
		// statement timeout
		p.appendBytes(bint32_to_bytes(0))
	}

	_, err := p.sendPackets()
	return err
}

func (p *wireProtocol) opFetch(stmtHandle int32, blr []byte) error {
	p.debugPrint("opFetch")
	p.packInt(op_fetch)
	p.packInt(stmtHandle)
	p.packBytes(blr)
	p.packInt(0)
	p.packInt(400)
	_, err := p.sendPackets()
	return err
}

// readRow decodes a single row from the wire. Pre-V13 protocols interleave
// per-column null flags; V13+ uses a leading null bitmap.
func (p *wireProtocol) readRow(xsqlda []xSQLVAR) ([]driver.Value, error) {
	r := make([]driver.Value, len(xsqlda))
	if p.protocolVersion < PROTOCOL_VERSION13 {
		for i, x := range xsqlda {
			var ln int
			if x.ioLength() < 0 {
				b, err := p.recvPackets(4)
				if err != nil {
					return nil, err
				}
				ln = int(bytes_to_bint32(b))
			} else {
				ln = x.ioLength()
			}
			rawValue, err := p.recvPacketsAlignment(ln)
			if err != nil {
				return nil, err
			}
			nullFlag, err := p.recvPackets(4)
			if err != nil {
				return nil, err
			}
			if bytes_to_bint32(nullFlag) == 0 { // Not NULL
				r[i], err = x.value(rawValue, p.timezone, p.charset)
				if err != nil {
					return nil, err
				}
			}
		}
	} else { // V13+ sends a null bitmap upfront instead of per-column null flags
		n := (len(xsqlda) + 7) / 8
		nullBytes, err := p.recvPacketsAlignment(n)
		if err != nil {
			return nil, err
		}
		for i, x := range xsqlda {
			if nullBytes[i/8]&(1<<(i%8)) != 0 {
				continue
			}
			var ln int
			if x.ioLength() < 0 {
				b, err := p.recvPackets(4)
				if err != nil {
					return nil, err
				}
				ln = int(bytes_to_bint32(b))
			} else {
				ln = x.ioLength()
			}
			rawValue, err := p.recvPacketsAlignment(ln)
			if err != nil {
				return nil, err
			}
			r[i], err = x.value(rawValue, p.timezone, p.charset)
			if err != nil {
				return nil, err
			}
		}
	}
	return r, nil
}

// opFetchResponse reads rows from a fetch response, returning them as a slice.
func (p *wireProtocol) opFetchResponse(stmtHandle int32, transHandle int32, xsqlda []xSQLVAR) ([][]driver.Value, bool, error) {
	p.debugPrint("opFetchResponse")
	b, err := p.recvPackets(4)
	for bytes_to_bint32(b) == op_dummy {
		b, _ = p.recvPackets(4)
	}

	for bytes_to_bint32(b) == op_response && p.lazyResponseCount > 0 {
		p.lazyResponseCount--
		p._parse_op_response()
		b, _ = p.recvPackets(4)
	}
	if bytes_to_bint32(b) != op_fetch_response {
		if bytes_to_bint32(b) == op_response {
			_, _, _, err := p._parse_op_response()
			if err != nil {
				return nil, false, err
			}
		}
		return nil, false, errors.New("opFetchResponse:Internal Error")
	}
	b, err = p.recvPackets(8)
	status := bytes_to_bint32(b[:4])
	count := int(bytes_to_bint32(b[4:8]))
	rows := make([][]driver.Value, 0, count) // pre-allocate to known chunk size

	for count > 0 {
		r, err2 := p.readRow(xsqlda)
		if err2 != nil {
			return nil, false, err2
		}
		rows = append(rows, r)

		b, err = p.recvPackets(12)
		// op := int(bytes_to_bint32(b[:4]))
		status = bytes_to_bint32(b[4:8])
		count = int(bytes_to_bint32(b[8:]))
	}

	return rows, status != 100, err
}

func (p *wireProtocol) opDetach() error {
	p.debugPrint("opDetach")
	p.packInt(op_detach)
	p.packInt(p.dbHandle)
	_, err := p.sendPackets()
	return err
}

func (p *wireProtocol) opOpenBlob(blobId []byte, transHandle int32) error {
	p.debugPrint("opOpenBlob")
	p.packInt(op_open_blob)
	p.packInt(transHandle)
	p.appendBytes(blobId)
	_, err := p.sendPackets()
	return err
}

func (p *wireProtocol) opOpenBlob2(blobId []byte, transHandle int32) error {
	p.debugPrint("opOpenBlob2")
	p.packInt(op_open_blob2)
	p.packInt(0)
	p.packInt(transHandle)
	p.appendBytes(blobId)
	_, err := p.sendPackets()
	return err
}

func (p *wireProtocol) opCreateBlob2(transHandle int32) error {
	p.debugPrint("opCreateBlob2")
	p.packInt(op_create_blob2)
	p.packInt(0)
	p.packInt(transHandle)
	p.packInt(0)
	p.packInt(0)
	_, err := p.sendPackets()
	return err
}

func (p *wireProtocol) opGetSegment(blobHandle int32) error {
	p.debugPrint("opGetSegment")
	p.packInt(op_get_segment)
	p.packInt(blobHandle)
	p.packInt(int32(BUFFER_LEN))
	p.packInt(0)
	_, err := p.sendPackets()
	return err
}

func (p *wireProtocol) opPutSegment(blobHandle int32, seg_data []byte) error {
	p.debugPrint("opPutSegment")
	ln := len(seg_data)
	p.packInt(op_put_segment)
	p.packInt(blobHandle)
	p.packInt(int32(ln))
	p.packInt(int32(ln))
	p.appendBytes(seg_data)
	padding := [3]byte{0x0, 0x0, 0x0}
	p.appendBytes(padding[:((4 - ln) & 3)])
	_, err := p.sendPackets()
	return err
}

func (p *wireProtocol) opBatchSegments(blobHandle int32, seg_data []byte) error {
	p.debugPrint("opBatchSegments")
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
	_, err := p.sendPackets()
	return err
}

func (p *wireProtocol) opCloseBlob(blobHandle int32) error {
	p.debugPrint("opCloseBlob")
	p.packInt(op_close_blob)
	p.packInt(blobHandle)
	_, err := p.sendPackets()
	return err
}

func (p *wireProtocol) opResponse() (int32, []byte, []byte, error) {
	p.debugPrint("opResponse")
	b, err := p.recvPackets(4)
	if err != nil {
		return 0, nil, nil, err
	}
	for bytes_to_bint32(b) == op_dummy {
		b, _ = p.recvPackets(4)
	}
	for bytes_to_bint32(b) == op_crypt_key_callback {

		err = p.opCryptCallback()
		if err != nil {
			return 0, nil, nil, err
		}

		b, _ = p.recvPackets(12)
		b, _ = p.recvPackets(4)

	}
	for bytes_to_bint32(b) == op_response && p.lazyResponseCount > 0 {
		p.lazyResponseCount--
		_, _, _, _ = p._parse_op_response()
		b, _ = p.recvPackets(4)
	}

	if bytes_to_bint32(b) != op_response {
		if bytes_to_bint32(b) == op_cont_auth {
			return 0, nil, nil, errors.New("Your user name and password are not defined. Ask your database administrator to set up a Firebird login.\n")
		}
		return 0, nil, nil, NewErrOpResonse(bytes_to_bint32(b))
	}
	return p._parse_op_response()
}

func (p *wireProtocol) opSqlResponse(xsqlda []xSQLVAR) ([]driver.Value, error) {
	p.debugPrint("opSqlResponse")
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
		b, _ = p.recvPackets(4)
	}

	if bytes_to_bint32(b) != op_sql_response {
		return nil, errors.New("Error op_sql_response")
	}

	b, err = p.recvPackets(4)
	if err != nil {
		return nil, err
	}
	count := int(bytes_to_bint32(b))
	if count == 0 {
		return nil, nil
	}

	return p.readRow(xsqlda)
}

func (p *wireProtocol) createBlob(value []byte, transHandle int32) ([]byte, error) {
	buf := p.suspendBuffer()
	p.opCreateBlob2(transHandle)
	blobHandle, blobId, _, err := p.opResponse()
	if err != nil {
		p.resumeBuffer(buf)
		return blobId, err
	}

	i := 0
	for i < len(value) {
		end := i + BLOB_SEGMENT_SIZE
		if end > len(value) {
			end = len(value)
		}
		p.opPutSegment(blobHandle, value[i:end])
		_, _, _, err := p.opResponse()
		if err != nil {
			break
		}
		i += BLOB_SEGMENT_SIZE
	}
	if err != nil {
		p.resumeBuffer(buf)
		return blobId, err
	}

	if err = p.opCloseBlob(blobHandle); err != nil {
		return nil, err
	}
	_, _, _, err = p.opResponse()

	p.resumeBuffer(buf)
	return blobId, err
}

// paramsToBlr converts parameters to BLR type descriptors and serialized values for the wire protocol.
// inputXsqlda contains the server-reported types for bind parameters (from isc_info_sql_bind).
// It is used to select the correct encoding for time.Time values: TIMESTAMP/TIME (without TZ)
// columns are encoded as local wall clock time to preserve round-trip correctness when time.Local != UTC.
func (p *wireProtocol) paramsToBlr(transHandle int32, params []driver.Value, protocolVersion int32, inputXsqlda []xSQLVAR) ([]byte, []byte) {
	var v, blr []byte

	ln := len(params) * 2
	// Each param contributes a type descriptor + null indicator pair, plus a header and terminator entry.
	blrList := make([][]byte, 0, len(params)*2+2)
	// Each param contributes a value entry; pre-v13 also adds a null flag per param.
	valuesList := make([][]byte, 0, len(params)*2)
	blrList = append(blrList, []byte{5, 2, 4, 0, byte(ln & 255), byte(ln >> 8)})

	if protocolVersion >= PROTOCOL_VERSION13 {
		n := (len(params) + 7) / 8
		if n%4 != 0 { // padding
			n += 4 - n%4
		}
		nullBytes := make([]byte, n)
		for i, param := range params {
			if param == nil {
				nullBytes[i/8] |= 1 << (i % 8)
			}
		}
		valuesList = append(valuesList, nullBytes)
	}

	for i, param := range params {
		switch f := param.(type) {
		case string:
			f = p.encodeString(f)
			b := str_to_bytes(f)
			if len(b) < MAX_CHAR_LENGTH {
				blr, v = _bytesToBlr(b)
			} else {
				v, _ = p.createBlob(b, transHandle)
				blr = []byte{9, 0}
			}
		case int:
			blr, v = _int32ToBlr(int32(f))
		case int16:
			blr, v = _int32ToBlr(int32(f))
		case int32:
			blr, v = _int32ToBlr(f)
		case int64:
			blr, v = _int64ToBlr(int64(f))
		case float64:
			blr, v = _float64ToBlr(float64(f))
		case time.Time:
			if f.Year() == 0 {
				if i < len(inputXsqlda) && inputXsqlda[i].sqltype == SQL_TYPE_TIME {
					// TIME (without TZ) column: encode local wall clock to preserve round-trip.
					blr, v = []byte{13}, _convert_time(f)
				} else {
					blr, v = _timeToBlr(f, protocolVersion, p.timezone)
				}
			} else {
				if i < len(inputXsqlda) {
					switch inputXsqlda[i].sqltype {
					case SQL_TYPE_DATE:
						// DATE column: encode local wall clock date to preserve round-trip.
						blr, v = _dateToBlr(f)
					case SQL_TYPE_TIMESTAMP:
						// TIMESTAMP (without TZ) column: encode local wall clock to preserve round-trip.
						blr, v = []byte{35}, _convert_timestamp(f)
					default:
						blr, v = _timestampToBlr(f, protocolVersion, p.timezone)
					}
				} else {
					blr, v = _timestampToBlr(f, protocolVersion, p.timezone)
				}
			}
		case bool:
			if f {
				v = []byte{1, 0, 0, 0}
			} else {
				v = []byte{0, 0, 0, 0}
			}
			blr = []byte{23}
		case nil:
			v = []byte{}
			blr = []byte{14, 0, 0}
		case []byte:
			if len(f) < MAX_CHAR_LENGTH {
				blr, v = _bytesToBlr(f)
			} else {
				v, _ = p.createBlob(f, transHandle)
				blr = []byte{9, 0}
			}
		default:
			// can't convert directory
			b := str_to_bytes(fmt.Sprintf("%v", f))
			if len(b) < MAX_CHAR_LENGTH {
				blr, v = _bytesToBlr(b)
			} else {
				v, _ = p.createBlob(b, transHandle)
				blr = []byte{9, 0}
			}
		}
		valuesList = append(valuesList, v)
		if protocolVersion < PROTOCOL_VERSION13 {
			if param == nil {
				valuesList = append(valuesList, []byte{0xff, 0xff, 0xff, 0xff})
			} else {
				valuesList = append(valuesList, []byte{0, 0, 0, 0})
			}
		}
		blrList = append(blrList, blr)
		blrList = append(blrList, []byte{7, 0})
	}
	blrList = append(blrList, []byte{255, 76}) // [blr_end, blr_eoc]

	blr = bytes.Join(blrList, nil)
	v = bytes.Join(valuesList, nil)

	return blr, v
}

func (p *wireProtocol) debugPrint(s string, a ...interface{}) {
	//if len(a) > 0 {
	//	s = fmt.Sprintf(s, a...)
	//}
	//fmt.Printf("[%x] %s\n", uintptr(unsafe.Pointer(p)), s)
}

func (p *wireProtocol) opConnectRequest() error {
	p.debugPrint("opConnectRequest()")
	p.packInt(op_connect_request)
	p.packInt(p_req_async)
	p.packInt(p.dbHandle)
	p.packInt(partner_identification)
	_, err := p.sendPackets()
	return err
}

func (p *wireProtocol) opQueEvents(auxHandle int32, epb []byte, eventId int32) error {
	p.debugPrint("opQueEvents():%d %d", auxHandle, eventId)
	p.packInt(op_que_events)
	p.packInt(auxHandle)
	p.packBytes(epb)
	p.packInt(address_of_ast_routine)
	p.packInt(argument_to_ast_routine)
	p.packInt(eventId)
	_, err := p.sendPackets()
	return err
}

func (p *wireProtocol) opCancelEvents(eventID int32) error {
	p.debugPrint("opCancelEvents():%d", eventID)
	p.packInt(op_cancel_events)
	p.packInt(p.dbHandle)
	p.packInt(eventID)
	_, err := p.sendPackets()
	return err
}

func (p *wireProtocol) opCancel(kind int) error {
	p.debugPrint("opCancel")
	p.packInt(op_cancel)
	p.packInt(int32(kind))
	_, err := p.sendPackets()
	return err
}

func (p *wireProtocol) encodeString(str string) string {
	if v, ok := encodeCharset(str, p.charset); ok {
		return v
	}
	return str
}

func (p *wireProtocol) opServiceAttach() error {
	p.debugPrint("opServiceAttach()")
	p.packInt(op_service_attach)
	p.packInt(0)
	p.packString("service_mgr")

	userBytes := []byte(p.user)
	passwordBytes := []byte(p.password)
	spb := bytes.Join([][]byte{
		{isc_spb_version, isc_spb_current_version},
		{isc_spb_user_name, byte(len(userBytes))}, userBytes,
		{isc_spb_password, byte(len(passwordBytes))}, passwordBytes,
		{isc_spb_utf8_filename, 1, 1},
	}, nil)
	if p.authData != nil {
		specificAuthData := []byte(hex.EncodeToString(p.authData))
		spb = bytes.Join([][]byte{
			spb,
			{isc_dpb_specific_auth_data, byte(len(specificAuthData))}, specificAuthData}, nil)
	}
	p.packBytes(spb)
	_, err := p.sendPackets()
	return err
}

func (p *wireProtocol) opServiceDetach() error {
	p.debugPrint("opServiceDetach()")
	p.packInt(op_service_detach)
	p.packInt(p.dbHandle)
	_, err := p.sendPackets()
	return err
}

func (p *wireProtocol) opServiceInfo(spb []byte, srb []byte, bufferLength int32) error {
	p.debugPrint("opServiceInfo(%v, %v, %v)", spb, srb, bufferLength)
	if bufferLength <= 0 {
		bufferLength = BUFFER_LEN
	}
	p.packInt(op_service_info)
	p.packInt(p.dbHandle)
	p.packInt(0)
	p.packBytes(spb)
	p.packBytes(srb)
	p.packInt(bufferLength)
	_, err := p.sendPackets()
	return err
}

func (p *wireProtocol) opServiceStart(spb []byte) error {
	p.debugPrint("opServiceStart(%v)", spb)
	p.packInt(op_service_start)
	p.packInt(p.dbHandle)
	p.packInt(0)
	p.packBytes(spb)
	_, err := p.sendPackets()
	return err
}
