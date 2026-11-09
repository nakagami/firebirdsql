package firebirdsql

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"os"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"gitlab.com/nyarla/go-crypt"
)

const (
	// wireTestPassword has characters outside the hex alphabet on purpose: the
	// SRP public key travels as hex, so a hex-looking password can show up in a
	// capture by chance.
	wireTestPassword = "Zq_nohex_Secret"
	// wireTestHash is crypt(3) of wireTestPassword with the salt "9z", without
	// the salt prefix. It's a fixed vector so that the tests don't share code
	// with the driver.
	wireTestHash = "1IZvPPQRN6w"
)

// acceptHeaderVersion is acceptHeader with a chosen protocol version.
func (f *acceptFrame) acceptHeaderVersion(opcode, version int32) {
	f.int32(opcode)
	f.int32(version)
	f.int32(1) // accept architecture
	f.int32(0) // accept type (no compression flag)
}

// authAccept writes an op_cond_accept or op_accept_data packet that asks the
// client to authenticate through plugin with the given server data.
func (f *acceptFrame) authAccept(opcode, version int32, data []byte, plugin string) {
	f.acceptHeaderVersion(opcode, version)
	f.blob(data)
	f.blob([]byte(plugin))
	f.int32(0) // not yet authenticated
	f.blob(nil)
}

// connectRun is the outcome of one _parse_connect_response run over a canned
// reply: the protocol, everything the client sent while handling the reply, the
// key pair it used and the error.
type connectRun struct {
	p                          *wireProtocol
	out                        *bytes.Buffer
	clientPublic, clientSecret *big.Int
	err                        error
}

// connectResponseWith runs _parse_connect_response as user, with options.
func connectResponseWith(t *testing.T, user string, reply []byte, options map[string]string) connectRun {
	t.Helper()
	clientPublic, clientSecret, err := getClientSeed()
	if err != nil {
		t.Fatalf("getClientSeed: %v", err)
	}
	r := connectRun{p: testProtocol(reply), out: new(bytes.Buffer), clientPublic: clientPublic, clientSecret: clientSecret}
	r.p.conn.writer = bufio.NewWriter(r.out)
	r.err = r.p._parse_connect_response(user, wireTestPassword, options, clientPublic, clientSecret)
	return r
}

// connectResponse runs _parse_connect_response over the canned reply and returns
// the protocol and everything the client sent while handling it.
func connectResponse(t *testing.T, reply []byte, wireCrypt string) (*wireProtocol, []byte, error) {
	t.Helper()
	options := testConnectOptions()
	options["wire_crypt"] = wireCrypt
	r := connectResponseWith(t, "SYSDBA", reply, options)
	return r.p, r.out.Bytes(), r.err
}

func TestParseConnectResponse_SuccessWithoutAccept(t *testing.T) {
	var f acceptFrame
	f.opResponseFrame(0, nil)
	for _, wireCrypt := range []string{"required", "true", "false"} {
		t.Run(wireCrypt, func(t *testing.T) {
			if _, _, err := connectResponse(t, f.bytes(), wireCrypt); err == nil {
				t.Fatal("a success reply to op_connect was accepted as a completed handshake")
			}
		})
	}
}

func TestParseConnectResponse_SuccessDuringContAuth(t *testing.T) {
	var f acceptFrame
	f.authAccept(op_cond_accept, PROTOCOL_VERSION13, nil, "Srp256")
	f.opResponseFrame(0, nil) // reply to the op_cont_auth carrying the public key

	t.Run("required", func(t *testing.T) {
		if _, _, err := connectResponse(t, f.bytes(), "required"); !errors.Is(err, errWireCryptRequired) {
			t.Fatalf("err = %v, want errWireCryptRequired: no cipher can exist without a proof", err)
		}
	})
	t.Run("enabled", func(t *testing.T) {
		p, _, err := connectResponse(t, f.bytes(), "true")
		if err != nil {
			t.Fatalf("err = %v, want the reply treated as authenticated", err)
		}
		if len(p.authData) != 0 {
			t.Fatalf("authData = %x, want none", p.authData)
		}
	})
}

func TestParseConnectResponse_RefusalKeepsServerError(t *testing.T) {
	var connect acceptFrame
	connect.opResponseFrame(0, nil, isc_arg_gds, ISCLogin)

	var contAuth acceptFrame
	contAuth.authAccept(op_cond_accept, PROTOCOL_VERSION13, nil, "Srp256")
	contAuth.opResponseFrame(0, nil, isc_arg_gds, ISCLogin)

	for name, reply := range map[string][]byte{"op_connect": connect.bytes(), "op_cont_auth": contAuth.bytes()} {
		for _, wireCrypt := range []string{"required", "true"} {
			t.Run(name+" "+wireCrypt, func(t *testing.T) {
				_, _, err := connectResponse(t, reply, wireCrypt)
				var fbErr *FbError
				if !errors.As(err, &fbErr) || !slices.Contains(fbErr.GDSCodes, ISCLogin) {
					t.Fatalf("err = %v, want the server's isc_login", err)
				}
			})
		}
	}
}

// TestParseConnectResponse_LegitimateAuthFlows pins the replies real servers
// send, so that refusing the rogue ones can't break them.
func TestParseConnectResponse_LegitimateAuthFlows(t *testing.T) {
	serverData := srpServerData(32, 256)

	var acceptData acceptFrame
	acceptData.authAccept(op_accept_data, PROTOCOL_VERSION13, serverData, "Srp256")

	var condAccept acceptFrame
	condAccept.authAccept(op_cond_accept, PROTOCOL_VERSION13, serverData, "Srp256")
	condAccept.opResponseFrame(0, nil) // reply to the proof

	var pluginSwitch acceptFrame
	pluginSwitch.authAccept(op_cond_accept, PROTOCOL_VERSION13, nil, "Srp")
	pluginSwitch.int32(op_cont_auth)
	pluginSwitch.blob(serverData)
	pluginSwitch.blob([]byte("Srp"))
	pluginSwitch.blob(nil)
	pluginSwitch.blob(nil)
	pluginSwitch.opResponseFrame(0, nil) // reply to the proof

	var authenticated acceptFrame
	authenticated.acceptHeaderVersion(op_accept_data, PROTOCOL_VERSION13)
	authenticated.blob(nil)
	authenticated.blob(nil)
	authenticated.int32(1) // authenticated from the op_connect data
	authenticated.blob(nil)

	var plain acceptFrame
	plain.acceptHeaderVersion(op_accept, 12)

	for _, tc := range []struct {
		name     string
		reply    []byte
		deferred bool // the proof goes in the attach block
	}{
		{"op_accept_data defers the proof", acceptData.bytes(), true},
		{"op_cond_accept sends the proof, no cipher", condAccept.bytes(), true},
		{"plugin switch through op_cont_auth", pluginSwitch.bytes(), true},
		{"already authenticated", authenticated.bytes(), false},
		{"plain op_accept below 13", plain.bytes(), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, _, err := connectResponse(t, tc.reply, "true")
			if err != nil {
				t.Fatalf("err = %v", err)
			}
			if deferred := len(p.authData) > 0; deferred != tc.deferred {
				t.Fatalf("authData = %x, want deferred = %v", p.authData, tc.deferred)
			}
		})
	}
}

func TestWireCryptRequiredLegacyPluginSendsNoHash(t *testing.T) {
	var f acceptFrame
	f.authAccept(op_cond_accept, PROTOCOL_VERSION13, nil, "Legacy_Auth")
	f.opResponseFrame(0, nil) // reply to the op_cont_auth a client would send

	_, sent, err := connectResponse(t, f.bytes(), "required")
	if !errors.Is(err, errWireCryptRequired) {
		t.Fatalf("err = %v, want errWireCryptRequired", err)
	}
	for _, needle := range []string{wireTestHash, hex.EncodeToString([]byte(wireTestHash))} {
		if bytes.Contains(sent, []byte(needle)) {
			t.Fatalf("the hash went out before the refusal: %x", sent)
		}
	}
}

// TestLegacyPluginSendsHashNotPassword covers a server selecting Legacy_Auth when
// required isn't set: the login must be the hash, never the password.
func TestLegacyPluginSendsHashNotPassword(t *testing.T) {
	assertHashOnly := func(t *testing.T, sent, authData []byte) {
		t.Helper()
		if string(authData) != wireTestHash {
			t.Errorf("authData = %q, want the hash %q", authData, wireTestHash)
		}
		for _, needle := range []string{wireTestPassword, hex.EncodeToString([]byte(wireTestPassword))} {
			if bytes.Contains(sent, []byte(needle)) {
				t.Errorf("the password went out: %x", sent)
			}
		}
	}

	for _, wireCrypt := range []string{"true", "false"} {
		t.Run("op_cond_accept "+wireCrypt, func(t *testing.T) {
			var f acceptFrame
			f.authAccept(op_cond_accept, PROTOCOL_VERSION13, nil, "Legacy_Auth")
			f.opResponseFrame(0, nil) // reply to op_cont_auth
			p, sent, err := connectResponse(t, f.bytes(), wireCrypt)
			if err != nil {
				t.Fatalf("err = %v", err)
			}
			assertHashOnly(t, sent, p.authData)
			if !bytes.Contains(sent, []byte(wireTestHash)) {
				t.Errorf("op_cont_auth doesn't carry the raw hash: %x", sent)
			}
		})
		t.Run("op_accept_data "+wireCrypt, func(t *testing.T) {
			var f acceptFrame
			f.authAccept(op_accept_data, PROTOCOL_VERSION13, nil, "Legacy_Auth")
			p, sent, err := connectResponse(t, f.bytes(), wireCrypt)
			if err != nil {
				t.Fatalf("err = %v", err)
			}
			assertHashOnly(t, sent, p.authData)
		})
	}
}

// attachRequest is a parsed op_attach, op_create or op_service_attach.
type attachRequest struct {
	op   int32
	name string
	pb   []byte
}

// readWireInt32 reads one big-endian XDR word.
func readWireInt32(r io.Reader) (int32, error) {
	var v int32
	err := binary.Read(r, binary.BigEndian, &v)
	return v, err
}

// readWireBlob reads a length-prefixed XDR field and its padding.
func readWireBlob(r io.Reader) ([]byte, error) {
	n, err := readWireInt32(r)
	if err != nil {
		return nil, err
	}
	if n < 0 || n > 1<<20 {
		return nil, fmt.Errorf("blob length %d", n)
	}
	b := make([]byte, int(n)+(4-int(n)%4)%4)
	if _, err := io.ReadFull(r, b); err != nil {
		return nil, err
	}
	return b[:n], nil
}

// readConnectRequest reads the rest of an op_connect after its opcode:
// operation, version, architecture, then the database name, the protocol count,
// the user identification and the protocols.
func readConnectRequest(r io.Reader) error {
	for i := 0; i < 3; i++ {
		if _, err := readWireInt32(r); err != nil {
			return err
		}
	}
	if _, err := readWireBlob(r); err != nil {
		return err
	}
	count, err := readWireInt32(r)
	if err != nil {
		return err
	}
	if _, err := readWireBlob(r); err != nil {
		return err
	}
	_, err = io.CopyN(io.Discard, r, int64(count)*20)
	return err
}

// readContAuthRequest reads the rest of a client op_cont_auth after its opcode:
// data, plugin name, plugin list and keys.
func readContAuthRequest(r io.Reader) error {
	for i := 0; i < 4; i++ {
		if _, err := readWireBlob(r); err != nil {
			return err
		}
	}
	return nil
}

// readAttachRequest parses the rest of an attach request after its opcode.
func readAttachRequest(r io.Reader, op int32) (attachRequest, error) {
	if _, err := readWireInt32(r); err != nil { // database object id
		return attachRequest{}, err
	}
	name, err := readWireBlob(r)
	if err != nil {
		return attachRequest{}, err
	}
	pb, err := readWireBlob(r)
	if err != nil {
		return attachRequest{}, err
	}
	return attachRequest{op: op, name: string(name), pb: pb}, nil
}

// fakeServerScript holds the replies of a scripted Firebird peer.
type fakeServerScript struct {
	connect  []byte // reply to op_connect
	contAuth []byte // reply to op_cont_auth
}

// fakeServer scripts one Firebird connection over loopback and records every
// byte the client sends. Read its fields only after wait returns.
type fakeServer struct {
	addr     string
	done     chan struct{}
	captured bytes.Buffer
	attaches []attachRequest
}

func startFakeServer(t *testing.T, script fakeServerScript) *fakeServer {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })
	s := &fakeServer{addr: listener.Addr().String(), done: make(chan struct{})}
	go func() {
		defer close(s.done)
		c, err := listener.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		s.serve(c, script)
	}()
	return s
}

// serve answers until the client closes the socket or sends an unknown opcode.
func (s *fakeServer) serve(c net.Conn, script fakeServerScript) {
	r := bufio.NewReader(io.TeeReader(c, &s.captured))
	var ok acceptFrame
	ok.opResponseFrame(1, nil)
	for {
		op, err := readWireInt32(r)
		if err != nil {
			return
		}
		var reply []byte
		switch op {
		case op_connect:
			if readConnectRequest(r) != nil {
				return
			}
			reply = script.connect
		case op_cont_auth:
			if readContAuthRequest(r) != nil {
				return
			}
			reply = script.contAuth
		case op_attach, op_create, op_service_attach:
			req, err := readAttachRequest(r, op)
			if err != nil {
				return
			}
			s.attaches = append(s.attaches, req)
			reply = ok.bytes()
		default:
			return
		}
		if _, err := c.Write(reply); err != nil {
			return
		}
	}
}

// wait blocks until the peer has seen the client's socket close, so that the
// capture is complete.
func (s *fakeServer) wait(t *testing.T) {
	t.Helper()
	select {
	case <-s.done:
	case <-time.After(5 * time.Second):
		t.Fatal("the fake server didn't see the client close its socket")
	}
}

// wireEntryPoint is one way to open a connection. On success, open returns a
// function that drops the socket without a detach.
type wireEntryPoint struct {
	name string
	op   int32
	open func(ctx context.Context, addr, user, password, wireCrypt string) (func(), error)
}

// wireEntryPoints covers the three builders of an attach block through their
// real callers. needleRole is sent as the role where the block has one.
func wireEntryPoints(needleRole string) []wireEntryPoint {
	dsn := func(addr, user, password, wireCrypt string) (*firebirdDsn, error) {
		return parseDSN(fmt.Sprintf("%s:%s@%s/probe.fdb?wire_crypt=%s&role=%s", user, password, addr, wireCrypt, needleRole))
	}
	return []wireEntryPoint{
		{"attach", op_attach, func(ctx context.Context, addr, user, password, wireCrypt string) (func(), error) {
			d, err := dsn(addr, user, password, wireCrypt)
			if err != nil {
				return nil, err
			}
			fc, err := attachFirebirdsqlConn(ctx, d)
			if err != nil {
				return nil, err
			}
			return func() { fc.wp.conn.Close() }, nil
		}},
		{"create", op_create, func(ctx context.Context, addr, user, password, wireCrypt string) (func(), error) {
			d, err := dsn(addr, user, password, wireCrypt)
			if err != nil {
				return nil, err
			}
			fc, err := createFirebirdsqlConn(ctx, d)
			if err != nil {
				return nil, err
			}
			return func() { fc.wp.conn.Close() }, nil
		}},
		{"service", op_service_attach, func(ctx context.Context, addr, user, password, wireCrypt string) (func(), error) {
			options := GetDefaultServiceManagerOptions()
			if wireCrypt == "false" {
				options = options.WithoutWireCrypt()
			}
			sm, err := NewServiceManagerContext(ctx, addr, user, password, options)
			if err != nil {
				return nil, err
			}
			return func() { sm.wp.conn.Close() }, nil
		}},
	}
}

// runAgainstFakeServer opens entry against a scripted peer and returns the
// peer, once it has seen the socket close, and the error from opening.
func runAgainstFakeServer(t *testing.T, entry wireEntryPoint, script fakeServerScript, user, wireCrypt string) (*fakeServer, error) {
	t.Helper()
	s := startFakeServer(t, script)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	closeConn, err := entry.open(ctx, s.addr, user, wireTestPassword, wireCrypt)
	if err == nil {
		closeConn()
	}
	s.wait(t)
	return s, err
}

func TestConnectSuccessResponseSendsNoAttach(t *testing.T) {
	var reply acceptFrame
	reply.opResponseFrame(0, nil)

	for _, entry := range wireEntryPoints("") {
		for _, wireCrypt := range []string{"required", "true", "false"} {
			if entry.op == op_service_attach && wireCrypt == "required" {
				continue // the service manager has no required mode
			}
			t.Run(entry.name+" "+wireCrypt, func(t *testing.T) {
				s, err := runAgainstFakeServer(t, entry, fakeServerScript{connect: reply.bytes()}, "SYSDBA", wireCrypt)
				if err == nil {
					t.Error("the connection was opened on a success reply to op_connect")
				}
				if len(s.attaches) != 0 {
					t.Errorf("%d attach requests sent after a success reply to op_connect", len(s.attaches))
				}
				if bytes.Contains(s.captured.Bytes(), []byte(wireTestPassword)) {
					t.Error("the password went on the wire")
				}
			})
		}
	}
}

// TestLegacyPluginHashInConnectPacket pins a documented limit: with
// auth_plugin_name=Legacy_Auth the hash is in the first packet, before any reply
// and whatever wire_crypt says.
func TestLegacyPluginHashInConnectPacket(t *testing.T) {
	var reject acceptFrame
	reject.int32(op_reject)

	s := startFakeServer(t, fakeServerScript{connect: reject.bytes()})
	d, err := parseDSN(fmt.Sprintf("SYSDBA:%s@%s/probe.fdb?auth_plugin_name=Legacy_Auth&wire_crypt=required", wireTestPassword, s.addr))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := attachFirebirdsqlConn(ctx, d); err == nil {
		t.Fatal("connected through op_reject")
	}
	s.wait(t)
	if !bytes.Contains(s.captured.Bytes(), []byte(wireTestHash)) {
		t.Fatal("the hash isn't in op_connect, so the documentation is out of date")
	}
	if bytes.Contains(s.captured.Bytes(), []byte(wireTestPassword)) {
		t.Fatal("the password went on the wire")
	}
}

// pbItem is one item of a parameter block: a tag, a one-byte length and the value.
type pbItem struct {
	tag   byte
	value []byte
}

// pbItems walks a DPB or SPB past its headerLen header bytes. Unlike dpbItems it
// keeps the order and duplicates, and it takes both block kinds.
func pbItems(t *testing.T, pb []byte, headerLen int) []pbItem {
	t.Helper()
	var items []pbItem
	for i := headerLen; i < len(pb); {
		if i+2 > len(pb) || i+2+int(pb[i+1]) > len(pb) {
			t.Fatalf("truncated item at offset %d: %x", i, pb)
		}
		n := int(pb[i+1])
		items = append(items, pbItem{pb[i], pb[i+2 : i+2+n]})
		i += 2 + n
	}
	return items
}

// itemValues returns the value of every item with the tag.
func itemValues(items []pbItem, tag byte) [][]byte {
	var values [][]byte
	for _, item := range items {
		if item.tag == tag {
			values = append(values, item.value)
		}
	}
	return values
}

// pbHeaderLen returns the header length of the block an attach request carries.
func pbHeaderLen(op int32) int {
	if op == op_service_attach {
		return 2 // isc_spb_version, isc_spb_current_version
	}
	return 1 // isc_dpb_version1
}

// attachBuilders are the three functions that build an attach block.
var attachBuilders = []struct {
	name  string
	op    int32
	build func(p *wireProtocol, user, password string) error
}{
	{"opAttach", op_attach, func(p *wireProtocol, user, password string) error {
		return p.opAttach("probe.fdb", user, password, "")
	}},
	{"opCreate", op_create, func(p *wireProtocol, user, password string) error {
		return p.opCreate("probe.fdb", user, password, "")
	}},
	{"opServiceAttach", op_service_attach, func(p *wireProtocol, user, password string) error {
		p.user, p.password = user, password
		return p.opServiceAttach()
	}},
}

// buildAttachBlock runs a builder at a protocol version and returns the items of
// the block it sent.
func buildAttachBlock(t *testing.T, op int32, build func(p *wireProtocol, user, password string) error, protocol int32, password string) []pbItem {
	t.Helper()
	var out bytes.Buffer
	p := testProtocol(nil)
	p.conn.writer = bufio.NewWriter(&out)
	p.charset = "UTF8"
	p.protocolVersion = protocol
	if err := build(p, "SYSDBA", password); err != nil {
		t.Fatalf("build: %v", err)
	}
	gotOp, err := readWireInt32(&out)
	if err != nil || gotOp != op {
		t.Fatalf("opcode = %d, %v; want %d", gotOp, err, op)
	}
	req, err := readAttachRequest(&out, op)
	if err != nil {
		t.Fatalf("readAttachRequest: %v", err)
	}
	return pbItems(t, req.pb, pbHeaderLen(op))
}

func TestAttachBuildersOmitPassword(t *testing.T) {
	long := strings.Repeat("Zq_", 100) // a one-byte length can't hold it
	for _, b := range attachBuilders {
		for _, protocol := range []int32{PROTOCOL_VERSION13, PROTOCOL_VERSION16, PROTOCOL_VERSION19} {
			for _, password := range []string{wireTestPassword, long} {
				t.Run(fmt.Sprintf("%s %d %d bytes", b.name, protocol, len(password)), func(t *testing.T) {
					items := buildAttachBlock(t, b.op, b.build, protocol, password)
					if v := itemValues(items, isc_dpb_password); len(v) != 0 {
						t.Errorf("password item sent: %q", v)
					}
					if v := itemValues(items, isc_dpb_password_enc); len(v) != 0 {
						t.Errorf("password_enc item sent: %q", v)
					}
					if v := itemValues(items, isc_dpb_user_name); len(v) != 1 || string(v[0]) != "SYSDBA" {
						t.Errorf("user name items = %q, want one SYSDBA", v)
					}
				})
			}
		}
	}
}

// TestLegacyPasswordHash pins the helper to vectors computed with the expression
// it replaced, crypt.Crypt(password, "9z")[2:], and to the limits of DES crypt.
func TestLegacyPasswordHash(t *testing.T) {
	for _, tc := range []struct{ password, want string }{
		{"masterkey", "QP3LMZ/MJh."},
		{"masterke", "QP3LMZ/MJh."}, // only the first 8 bytes count
		{"pässwörd", "9ZFEaqRcYmw"},
		{"\xe9abc", "UsrQcxO3w5k"},
		{"iabc", "UsrQcxO3w5k"}, // only the low 7 bits of a byte count
		{"", "retK2Kk/GLk"},
		{wireTestPassword, wireTestHash},
	} {
		if got := string(legacyPasswordHash(tc.password)); got != tc.want {
			t.Errorf("legacyPasswordHash(%q) = %q, want %q", tc.password, got, tc.want)
		}
	}
}

func TestAttachBuildersBelow13(t *testing.T) {
	for _, b := range attachBuilders {
		for _, protocol := range []int32{0, 10, 11, 12} {
			for _, tc := range []struct{ name, password, hash string }{
				{"nine bytes", "masterkey", "QP3LMZ/MJh."},
				{"non-ASCII", "pässwörd", "9ZFEaqRcYmw"},
				{"empty", "", "retK2Kk/GLk"},
				{"300 bytes", strings.Repeat("Zq_", 100), "vC0px0XA7DA"},
			} {
				t.Run(fmt.Sprintf("%s %d %s", b.name, protocol, tc.name), func(t *testing.T) {
					items := buildAttachBlock(t, b.op, b.build, protocol, tc.password)
					// Firebird 2.5 fails a login that carries both items.
					if v := itemValues(items, isc_dpb_password); len(v) != 0 {
						t.Errorf("password item sent: %q", v)
					}
					if v := itemValues(items, isc_dpb_password_enc); len(v) != 1 || string(v[0]) != tc.hash {
						t.Errorf("password_enc items = %q, want one %q", v, tc.hash)
					}
				})
			}
		}
	}
}

// assertAttachSeen checks the control: the capture really holds a readable
// attach block from this client, parsed from the attach frame itself, because
// op_connect also carries the database name and the user. It returns the items.
func assertAttachSeen(t *testing.T, s *fakeServer, entry wireEntryPoint, needleRole, needleUser string) []pbItem {
	t.Helper()
	if len(s.attaches) != 1 || s.attaches[0].op != entry.op {
		t.Fatalf("attach requests = %+v, want one op %d", s.attaches, entry.op)
	}
	items := pbItems(t, s.attaches[0].pb, pbHeaderLen(entry.op))
	tag, want := byte(isc_dpb_sql_role_name), needleRole
	if entry.op == op_service_attach {
		tag, want = isc_spb_user_name, needleUser
	}
	if v := itemValues(items, tag); len(v) != 1 || string(v[0]) != want {
		t.Fatalf("control item %d = %q, want %q: the attach block wasn't read correctly", tag, v, want)
	}
	return items
}

func TestPasswordNotOnWire(t *testing.T) {
	const needleRole, needleUser = "Needle_Role", "SysDba_Needle"
	serverData := srpServerData(32, 256)

	var acceptData acceptFrame
	acceptData.authAccept(op_accept_data, PROTOCOL_VERSION13, serverData, "Srp256")

	var condAccept acceptFrame
	condAccept.authAccept(op_cond_accept, PROTOCOL_VERSION13, serverData, "Srp256")

	var success acceptFrame
	success.opResponseFrame(0, nil)

	var plain acceptFrame
	plain.acceptHeaderVersion(op_accept, PROTOCOL_VERSION13)

	var plain12 acceptFrame
	plain12.acceptHeaderVersion(op_accept, 12)

	for _, entry := range wireEntryPoints(needleRole) {
		for _, tc := range []struct {
			name      string
			script    fakeServerScript
			wireCrypt string
		}{
			{"op_accept_data, wire crypt off", fakeServerScript{connect: acceptData.bytes()}, "false"},
			{"op_accept_data, wire crypt enabled", fakeServerScript{connect: acceptData.bytes()}, "true"},
			{"op_cond_accept, no cipher", fakeServerScript{connect: condAccept.bytes(), contAuth: success.bytes()}, "true"},
			{"plain op_accept", fakeServerScript{connect: plain.bytes()}, "true"},
		} {
			t.Run(entry.name+" "+tc.name, func(t *testing.T) {
				s, err := runAgainstFakeServer(t, entry, tc.script, needleUser, tc.wireCrypt)
				if err != nil {
					t.Fatalf("open: %v", err)
				}
				items := assertAttachSeen(t, s, entry, needleRole, needleUser)
				if v := itemValues(items, isc_dpb_password); len(v) != 0 {
					t.Errorf("password item sent: %q", v)
				}
				if v := itemValues(items, isc_dpb_password_enc); len(v) != 0 {
					t.Errorf("password_enc item sent: %q", v)
				}
				if bytes.Contains(s.captured.Bytes(), []byte(wireTestPassword)) {
					t.Error("the password went on the wire")
				}
				if bytes.Contains(s.captured.Bytes(), []byte(wireTestHash)) {
					t.Error("the hash went on the wire")
				}
			})
		}

		// A peer can always force the client below 13: on default settings it
		// gets the hash, which the same detector must see, but not the password.
		t.Run(entry.name+" plain op_accept at 12", func(t *testing.T) {
			s, err := runAgainstFakeServer(t, entry, fakeServerScript{connect: plain12.bytes()}, needleUser, "true")
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			items := assertAttachSeen(t, s, entry, needleRole, needleUser)
			if v := itemValues(items, isc_dpb_password); len(v) != 0 {
				t.Errorf("password item sent: %q", v)
			}
			if v := itemValues(items, isc_dpb_password_enc); len(v) != 1 || string(v[0]) != wireTestHash {
				t.Errorf("password_enc items = %q, want one %q", v, wireTestHash)
			}
			if bytes.Contains(s.captured.Bytes(), []byte(wireTestPassword)) {
				t.Error("the password went on the wire")
			}
			if !bytes.Contains(s.captured.Bytes(), []byte(wireTestHash)) {
				t.Error("the capture misses the password_enc item the block holds")
			}
		})

		// wire_crypt=required closes that. The service manager has no such mode.
		if entry.op != op_service_attach {
			t.Run(entry.name+" plain op_accept at 12, required", func(t *testing.T) {
				s, err := runAgainstFakeServer(t, entry, fakeServerScript{connect: plain12.bytes()}, needleUser, "required")
				if !errors.Is(err, errWireCryptRequired) {
					t.Errorf("err = %v, want errWireCryptRequired", err)
				}
				if len(s.attaches) != 0 {
					t.Errorf("%d attach requests sent", len(s.attaches))
				}
				if bytes.Contains(s.captured.Bytes(), []byte(wireTestHash)) {
					t.Error("the hash went on the wire")
				}
			})
		}
	}
}

// TestBelow13RequiresLegacyAuth checks that below protocol 13, where the hash is
// the only login, a list without Legacy_Auth refuses the connection and nothing
// follows op_connect.
func TestBelow13RequiresLegacyAuth(t *testing.T) {
	serverData := srpServerData(32, 256)

	var plain acceptFrame
	plain.acceptHeaderVersion(op_accept, 12)

	// Srp is in every list below, so the plugin check at the accept can't be what
	// refuses them: only the protocol can.
	var acceptData acceptFrame
	acceptData.authAccept(op_accept_data, 12, serverData, "Srp")

	var condAccept acceptFrame
	condAccept.authAccept(op_cond_accept, 12, serverData, "Srp")
	condAccept.opResponseFrame(0, nil) // reply to the proof

	for name, reply := range map[string][]byte{
		"op_accept":      plain.bytes(),
		"op_accept_data": acceptData.bytes(),
		"op_cond_accept": condAccept.bytes(),
	} {
		for _, tc := range []struct {
			list    string
			refused bool
		}{
			{"Srp256,Srp", true},
			{"Srp", true},
			{"Srp,Legacy_Auth", false},
			{defaultAuthPlugins, false},
		} {
			t.Run(name+" "+tc.list, func(t *testing.T) {
				clientPublic, clientSecret, err := getClientSeed()
				if err != nil {
					t.Fatalf("getClientSeed: %v", err)
				}
				options := testConnectOptions()
				options["auth_plugin_list"] = tc.list

				var out bytes.Buffer
				p := testProtocol(reply)
				p.conn.writer = bufio.NewWriter(&out)
				err = p._parse_connect_response("SYSDBA", wireTestPassword, options, clientPublic, clientSecret)
				if !tc.refused {
					if err != nil {
						t.Fatalf("refused a list that allows Legacy_Auth: %v", err)
					}
					return
				}
				if err == nil || !strings.Contains(err.Error(), "auth_plugin_list") {
					t.Fatalf("err = %v, want a refusal naming auth_plugin_list", err)
				}
				if out.Len() != 0 {
					t.Fatalf("sent %x before refusing", out.Bytes())
				}
			})
		}
	}
}

// TestListWithoutLegacyAuthConnectsAt13 is the other side of that boundary: from
// protocol 13 a list without Legacy_Auth is the documented hardening, not a refusal.
func TestListWithoutLegacyAuthConnectsAt13(t *testing.T) {
	serverData := srpServerData(32, 256)

	var plain acceptFrame
	plain.acceptHeaderVersion(op_accept, PROTOCOL_VERSION13)

	var acceptData acceptFrame
	acceptData.authAccept(op_accept_data, PROTOCOL_VERSION13, serverData, "Srp256")

	var condAccept acceptFrame
	condAccept.authAccept(op_cond_accept, PROTOCOL_VERSION13, serverData, "Srp256")
	condAccept.opResponseFrame(0, nil) // reply to the proof

	for name, reply := range map[string][]byte{
		"op_accept":      plain.bytes(),
		"op_accept_data": acceptData.bytes(),
		"op_cond_accept": condAccept.bytes(),
	} {
		t.Run(name, func(t *testing.T) {
			clientPublic, clientSecret, err := getClientSeed()
			if err != nil {
				t.Fatalf("getClientSeed: %v", err)
			}
			options := testConnectOptions()
			options["auth_plugin_list"] = "Srp256,Srp"
			p := testProtocol(reply)
			if err := p._parse_connect_response("SYSDBA", wireTestPassword, options, clientPublic, clientSecret); err != nil {
				t.Fatalf("err = %v", err)
			}
		})
	}
}

func TestBelow13RefusalSendsNoAttach(t *testing.T) {
	var plain acceptFrame
	plain.acceptHeaderVersion(op_accept, 12)

	for name, open := range map[string]func(context.Context, *firebirdDsn) (*firebirdsqlConn, error){
		"attach": attachFirebirdsqlConn,
		"create": createFirebirdsqlConn,
	} {
		t.Run(name, func(t *testing.T) {
			s := startFakeServer(t, fakeServerScript{connect: plain.bytes()})
			d, err := parseDSN(fmt.Sprintf("SYSDBA:%s@%s/probe.fdb?auth_plugin_list=Srp256,Srp", wireTestPassword, s.addr))
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			fc, err := open(ctx, d)
			if err == nil {
				fc.wp.conn.Close()
				t.Error("connected below 13 with a list that omits Legacy_Auth")
			}
			s.wait(t)
			if len(s.attaches) != 0 {
				t.Errorf("%d attach requests sent", len(s.attaches))
			}
			if bytes.Contains(s.captured.Bytes(), []byte(wireTestHash)) {
				t.Error("the hash went on the wire")
			}
			if bytes.Contains(s.captured.Bytes(), []byte(wireTestPassword)) {
				t.Error("the password went on the wire")
			}
		})
	}

	// A documented limit: the service manager always allows Legacy_Auth, so a peer
	// that picks protocol 12 gets the hash.
	t.Run("service manager", func(t *testing.T) {
		var entry wireEntryPoint
		for _, e := range wireEntryPoints("") {
			if e.op == op_service_attach {
				entry = e
			}
		}
		s, err := runAgainstFakeServer(t, entry, fakeServerScript{connect: plain.bytes()}, "SYSDBA", "true")
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		if !bytes.Contains(s.captured.Bytes(), []byte(wireTestHash)) {
			t.Fatal("the hash isn't in the attach, so the documentation is out of date")
		}
	})
}

// TestUserManagerPasswordIsRequestData pins what the documentation says about
// changing a user's password: it's request data, so it's readable unless the
// connection is encrypted, at every protocol.
func TestUserManagerPasswordIsRequestData(t *testing.T) {
	password := wireTestPassword
	user := User{Password: &password, UserId: -1, GroupId: -1}
	if !bytes.Contains(user.GetSpb(), []byte(password)) {
		t.Fatal("the password isn't in the request, so the documentation is out of date")
	}
}

// captureRelay forwards one server address and records what the client sends.
type captureRelay struct {
	addr string
	mu   sync.Mutex
	buf  bytes.Buffer
}

func (r *captureRelay) Write(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.buf.Write(p)
}

// contains reports whether the client sent needle. Bytes go in the capture before
// they reach the server, so everything that drew a reply is already there.
func (r *captureRelay) contains(needle []byte) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return bytes.Contains(r.buf.Bytes(), needle)
}

func startCaptureRelay(t *testing.T, target string) *captureRelay {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })
	r := &captureRelay{addr: listener.Addr().String()}
	go func() {
		for {
			client, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer client.Close()
				server, err := net.Dial("tcp", target)
				if err != nil {
					return
				}
				defer server.Close()
				done := make(chan struct{}, 2)
				go func() { _, _ = io.Copy(server, io.TeeReader(client, r)); done <- struct{}{} }()
				go func() { _, _ = io.Copy(client, server); done <- struct{}{} }()
				<-done
			}()
		}
	}()
	return r
}

// plaintextRefused handles the one error that may skip the live test: the server
// demands wire encryption. CI's configuration never does.
func plaintextRefused(t *testing.T, err error) {
	t.Helper()
	var fbErr *FbError
	if errors.As(err, &fbErr) && slices.Contains(fbErr.GDSCodes, ISCWirecryptIncompatible) {
		if os.Getenv("GITHUB_ACTIONS") != "" {
			t.Fatalf("CI's server refused a plaintext connection: %v", err)
		}
		t.Skipf("server requires wire encryption: %v", err)
	}
}

// isUnavailable reports whether err is the error stock FB 3 gives for a plugin
// switch it can't do without a cipher. The caller retries with the plugin the
// server prefers.
func isUnavailable(err error) bool {
	var fbErr *FbError
	return errors.As(err, &fbErr) && slices.Contains(fbErr.GDSCodes, ISCUnavailable)
}

// isLoginFailure reports whether err is a login failure: the server's isc_login,
// or the client's own refusal to go on with the exchange.
func isLoginFailure(err error) bool {
	var fbErr *FbError
	return errors.Is(err, errLoginRefused) || errors.As(err, &fbErr) && slices.Contains(fbErr.GDSCodes, ISCLogin)
}

// assertPasswordCapture checks a capture of a completed plaintext login. A login
// that was encrypted or never attached can't get here, so a password missing from
// the capture was really never sent.
func assertPasswordCapture(t *testing.T, r *captureRelay, protocol int, password string) {
	t.Helper()
	if r.contains([]byte(password)) {
		t.Errorf("protocol %d: the password went on the wire", protocol)
	}
	if protocol < PROTOCOL_VERSION13 {
		hash := crypt.Crypt(password, "9z")[2:]
		if !r.contains(append([]byte{isc_dpb_password_enc, byte(len(hash))}, hash...)) {
			t.Errorf("protocol %d: the capture misses the password_enc item", protocol)
		}
	}
}

// TestPasswordOnWireLive reads a real server's plaintext logins off the wire.
func TestPasswordOnWireLive(t *testing.T) {
	password := GetTestPassword()
	if len(password) < 4 {
		t.Skip("a password this short can match inside the hex SRP data")
	}
	dbPath := GetTestDatabase("test_password_wire_")
	dsn := func(addr, options string) string {
		path := dbPath
		if runtime.GOOS == "windows" {
			path = "/" + path
		}
		return fmt.Sprintf("%s:%s@%s%s?%s", GetTestUser(), password, addr, path, options)
	}

	// open logs in through a relay and returns the protocol of a plaintext login.
	open := func(t *testing.T, driver, options string) (*captureRelay, int, error) {
		t.Helper()
		relay := startCaptureRelay(t, testServerAddr())
		db, err := sql.Open(driver, dsn(relay.addr, options))
		if err != nil {
			return nil, 0, err
		}
		defer db.Close()
		db.SetMaxOpenConns(1)
		if err := db.Ping(); err != nil {
			return nil, 0, err
		}
		if cipher := getWireCipher(t, db); cipher != "" {
			t.Fatalf("connection is encrypted with %s, so its capture proves nothing", cipher)
		}
		return relay, negotiatedProtocol(t, db), nil
	}

	options := "wire_crypt=false"
	relay, protocol, err := open(t, "firebirdsql_createdb", options)
	if isUnavailable(err) {
		options = "wire_crypt=false&auth_plugin_name=Srp"
		relay, protocol, err = open(t, "firebirdsql_createdb", options)
	}
	if err != nil {
		plaintextRefused(t, err)
		t.Fatalf("create: %v", err)
	}

	t.Run("create", func(t *testing.T) {
		assertPasswordCapture(t, relay, protocol, password)
	})
	t.Run("attach", func(t *testing.T) {
		relay, protocol, err := open(t, "firebirdsql", options)
		if err != nil {
			t.Fatalf("attach: %v", err)
		}
		assertPasswordCapture(t, relay, protocol, password)
	})
	t.Run("service attach", func(t *testing.T) {
		if protocol >= PROTOCOL_VERSION13 {
			// Only here: below 13 this probe shares the code under test, and a
			// broken login there must fail rather than skip.
			requireServiceAvailable(t)
		}
		attach := func(o ServiceManagerOptions) (*captureRelay, error) {
			relay := startCaptureRelay(t, testServerAddr())
			// Bounded because this runs without requireServiceAvailable's guard
			// below 13, and a server's Services API can hang rather than refuse.
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			sm, err := NewServiceManagerContext(ctx, relay.addr, GetTestUser(), password, o)
			if err != nil {
				return nil, err
			}
			defer sm.Close()
			if cipher := sm.WireCipher(); cipher != "" {
				t.Fatalf("connection is encrypted with %s, so its capture proves nothing", cipher)
			}
			_, err = sm.GetServerVersion()
			return relay, err
		}
		o := GetDefaultServiceManagerOptions().WithoutWireCrypt()
		relay, err := attach(o)
		if isUnavailable(err) {
			relay, err = attach(o.WithAuthPlugin("Srp"))
		}
		if err != nil {
			t.Fatalf("service attach: %v", err)
		}
		assertPasswordCapture(t, relay, protocol, password)
	})
}
