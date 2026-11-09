package firebirdsql

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"runtime"
	"strings"
	"testing"
	"time"
)

// contAuthList writes an op_cont_auth with a plugin list.
func (f *acceptFrame) contAuthList(data []byte, plugin, list string) {
	f.int32(op_cont_auth)
	f.blob(data)
	f.blob([]byte(plugin))
	f.blob([]byte(list))
	f.blob(nil)
}

// contAuth writes a server op_cont_auth.
func (f *acceptFrame) contAuth(data []byte, plugin string) {
	f.contAuthList(data, plugin, "")
}

// wantContAuth is the bytes of a client op_cont_auth with the default plugin list.
func wantContAuth(data []byte, plugin string) []byte {
	var f acceptFrame
	f.contAuthList(data, plugin, defaultAuthPlugins)
	return f.bytes()
}

// srpSwitchAccept is the reply of a server that doesn't have the plugin the
// client named: op_accept_data, the plugin it selected, no data.
func srpSwitchAccept(plugin string) []byte {
	var f acceptFrame
	f.authAccept(op_accept_data, PROTOCOL_VERSION13, nil, plugin)
	return f.bytes()
}

// srpSwitchRun runs _parse_connect_response over the switch reply, followed by
// the rest of the script, and checks that nothing went out before the attach.
func srpSwitchRun(t *testing.T, plugin string, rest []byte) connectRun {
	t.Helper()
	options := testConnectOptions()
	options["wire_crypt"] = "false"
	r := connectResponseWith(t, "SYSDBA", append(srpSwitchAccept(plugin), rest...), options)
	if r.err != nil {
		t.Fatalf("_parse_connect_response: %v", r.err)
	}
	if r.out.Len() != 0 {
		t.Fatalf("sent %x before the attach, want nothing", r.out.Bytes())
	}
	r.p.charset = "UTF8"
	return r
}

// srpTestProof is the proof, in hex, that SYSDBA owes for srpServerData(32, …),
// computed independently of the driver's parsing.
func srpTestProof(serverData []byte, clientPublic, clientSecret *big.Int, plugin string) []byte {
	salt := serverData[2:34]
	serverPublic := bigIntFromHexString(string(serverData[36:]))
	proof, _ := getClientProof("SYSDBA", wireTestPassword, salt, clientPublic, serverPublic, clientSecret, plugin)
	return []byte(hex.EncodeToString(proof))
}

func publicHex(clientPublic *big.Int) []byte {
	return []byte(hex.EncodeToString(bigIntToBytes(clientPublic)))
}

func TestSrpAfterAttachBlockCarriesEmptyItem(t *testing.T) {
	for _, b := range attachBuilders {
		t.Run(b.name, func(t *testing.T) {
			r := srpSwitchRun(t, "Srp", nil)
			if err := b.build(r.p, "SYSDBA", wireTestPassword); err != nil {
				t.Fatal(err)
			}
			rd := bytes.NewReader(r.out.Bytes())
			op, err := readWireInt32(rd)
			if err != nil || op != b.op {
				t.Fatalf("op = %d (%v), want %d", op, err, b.op)
			}
			req, err := readAttachRequest(rd, op)
			if err != nil {
				t.Fatal(err)
			}
			tag := byte(isc_dpb_specific_auth_data)
			if op == op_service_attach {
				tag = isc_spb_specific_auth_data
			}
			values := itemValues(pbItems(t, req.pb, pbHeaderLen(op)), tag)
			if len(values) != 1 || len(values[0]) != 0 {
				t.Errorf("specific data items %x in %x, want one empty item", values, req.pb)
			}
		})
	}
}

// TestSrpAfterAttachFullSequence checks the exact bytes of the follow-up, including
// that the proof is hashed for the plugin the server selected. An op_cont_auth that
// names no plugin stays on the selected one, as in fbclient.
func TestSrpAfterAttachFullSequence(t *testing.T) {
	serverData := srpServerData(32, 256)
	for _, tc := range []struct{ selected, named string }{
		{"Srp", "Srp"},
		{"Srp256", "Srp256"},
		{"Srp", ""},
	} {
		t.Run(fmt.Sprintf("%s named %q", tc.selected, tc.named), func(t *testing.T) {
			var rest acceptFrame
			rest.contAuth(nil, tc.named)
			rest.contAuth(serverData, tc.named)
			rest.opResponseFrame(7, nil)
			r := srpSwitchRun(t, tc.selected, rest.bytes())

			handle, err := r.p.opAttachResponse()
			if err != nil {
				t.Fatalf("opAttachResponse: %v", err)
			}
			if handle != 7 {
				t.Errorf("handle = %d, want 7", handle)
			}
			if r.p.srpAfterAttach != nil {
				t.Error("the keys outlived the attach reply")
			}
			proof := srpTestProof(serverData, r.clientPublic, r.clientSecret, tc.selected)
			if want := map[string]int{"Srp": 40, "Srp256": 64}[tc.selected]; len(proof) != want {
				t.Errorf("proof is %d hex chars, want %d", len(proof), want)
			}
			want := append(wantContAuth(publicHex(r.clientPublic), tc.selected), wantContAuth(proof, tc.selected)...)
			if !bytes.Equal(r.out.Bytes(), want) {
				t.Fatalf("sent %x, want %x", r.out.Bytes(), want)
			}
		})
	}
}

// TestSrpAfterAttachServerSkipsPublicKeyRequest covers a server that answers the
// empty item with the salt and B straight away. The design doesn't depend on
// that, but it must not break it.
func TestSrpAfterAttachServerSkipsPublicKeyRequest(t *testing.T) {
	var rest acceptFrame
	rest.contAuth(srpServerData(32, 256), "Srp")
	rest.opResponseFrame(7, nil)
	r := srpSwitchRun(t, "Srp", rest.bytes())
	if _, err := r.p.opAttachResponse(); err != nil {
		t.Fatalf("opAttachResponse: %v", err)
	}
	if bytes.Contains(r.out.Bytes(), publicHex(r.clientPublic)) {
		t.Error("the public key was sent without being asked for")
	}
}

// TestSrpAfterAttachUsesUserAndPluginList checks that the exchange hashes the
// upper-cased user name and sends the client's own plugin list.
func TestSrpAfterAttachUsesUserAndPluginList(t *testing.T) {
	const list = "Srp"
	serverData := srpServerData(32, 256)
	var script acceptFrame
	script.authAccept(op_accept_data, PROTOCOL_VERSION13, nil, "Srp")
	script.contAuth(nil, "Srp")
	script.contAuth(serverData, "Srp")
	script.opResponseFrame(7, nil)
	options := testConnectOptions()
	options["wire_crypt"] = "false"
	options["auth_plugin_list"] = list
	r := connectResponseWith(t, "sysdba", script.bytes(), options)
	if r.err != nil {
		t.Fatalf("_parse_connect_response: %v", r.err)
	}
	if _, err := r.p.opAttachResponse(); err != nil {
		t.Fatalf("opAttachResponse: %v", err)
	}
	var want acceptFrame
	want.contAuthList(publicHex(r.clientPublic), "Srp", list)
	want.contAuthList(srpTestProof(serverData, r.clientPublic, r.clientSecret, "Srp"), "Srp", list)
	if !bytes.Equal(r.out.Bytes(), want.bytes()) {
		t.Fatalf("sent %x, want %x", r.out.Bytes(), want.bytes())
	}
}

// TestSrpAfterAttachRespectsAllowList checks that the allow-list still applies: a
// server selecting a plugin the client didn't list gets no attach and no exchange.
func TestSrpAfterAttachRespectsAllowList(t *testing.T) {
	options := testConnectOptions()
	options["wire_crypt"] = "false"
	options["auth_plugin_list"] = "Srp256"
	r := connectResponseWith(t, "SYSDBA", srpSwitchAccept("Srp"), options)
	if r.err == nil || !strings.Contains(r.err.Error(), "allow-list") {
		t.Fatalf("err = %v, want the allow-list refusal", r.err)
	}
	if r.out.Len() != 0 {
		t.Fatalf("sent %x, want nothing", r.out.Bytes())
	}
	if r.p.srpAfterAttach != nil {
		t.Error("exchange state set")
	}
}

// TestCondAcceptEmptyDataSendsPublicKeyInHex covers the other login that sends the
// public key: on op_cond_accept with no data it goes out before the attach, hex-encoded.
func TestCondAcceptEmptyDataSendsPublicKeyInHex(t *testing.T) {
	var script acceptFrame
	script.authAccept(op_cond_accept, PROTOCOL_VERSION13, nil, "Srp")
	script.contAuth(srpServerData(32, 256), "Srp")
	script.opResponseFrame(0, nil)
	options := testConnectOptions()
	options["wire_crypt"] = "false"
	r := connectResponseWith(t, "SYSDBA", script.bytes(), options)
	if r.err != nil {
		t.Fatalf("_parse_connect_response: %v", r.err)
	}
	if want := wantContAuth(publicHex(r.clientPublic), "Srp"); !bytes.HasPrefix(r.out.Bytes(), want) {
		t.Fatalf("sent %x, want it to start with %x", r.out.Bytes(), want)
	}
}

func TestSrpAfterAttachRequiredSendsNothing(t *testing.T) {
	p, sent, err := connectResponse(t, srpSwitchAccept("Srp"), "required")
	if !errors.Is(err, errWireCryptRequired) {
		t.Fatalf("err = %v, want errWireCryptRequired", err)
	}
	if len(sent) != 0 {
		t.Fatalf("sent %x, want nothing", sent)
	}
	if p.srpAfterAttach != nil {
		t.Error("exchange state set")
	}
}

func TestSrpAfterAttachServerErrorStaysFbError(t *testing.T) {
	serverData := srpServerData(32, 256)
	var afterAttach, afterPublic, afterProof acceptFrame
	afterAttach.opResponseFrame(0, nil, isc_arg_gds, ISCLogin)
	afterPublic.contAuth(nil, "Srp")
	afterPublic.opResponseFrame(0, nil, isc_arg_gds, ISCLogin)
	afterProof.contAuth(nil, "Srp")
	afterProof.contAuth(serverData, "Srp")
	afterProof.opResponseFrame(0, nil, isc_arg_gds, ISCLogin)
	for name, rest := range map[string][]byte{"attach": afterAttach.bytes(), "public key": afterPublic.bytes(), "proof": afterProof.bytes()} {
		t.Run(name, func(t *testing.T) {
			r := srpSwitchRun(t, "Srp", rest)
			_, err := r.p.opAttachResponse()
			requireGDSError(t, err, ISCLogin)
		})
	}
}

func TestSrpAfterAttachRogueServer(t *testing.T) {
	serverData := srpServerData(32, 256)
	frame := func(build func(f *acceptFrame)) []byte {
		var f acceptFrame
		build(&f)
		return f.bytes()
	}
	repeat := func(data []byte, plugin string, n int) []byte {
		return frame(func(f *acceptFrame) {
			for i := 0; i < n; i++ {
				f.contAuth(data, plugin)
			}
		})
	}
	saltTooLong := append([]byte{0xff, 0xff}, serverData[2:]...)
	for _, tc := range []struct {
		name    string
		rest    []byte
		maxSent int
	}{
		{"endless salt and B", repeat(serverData, "Srp", 1000), 1},
		{"endless empty", repeat(nil, "Srp", 1000), 1},
		{"endless alternating", frame(func(f *acceptFrame) {
			for i := 0; i < 500; i++ {
				f.contAuth(nil, "Srp")
				f.contAuth(serverData, "Srp")
			}
		}), 2},
		{"empty after the proof", frame(func(f *acceptFrame) {
			f.contAuth(nil, "Srp")
			f.contAuth(serverData, "Srp")
			f.contAuth(nil, "Srp")
		}), 2},
		{"bad salt length", frame(func(f *acceptFrame) { f.contAuth(saltTooLong, "Srp") }), 0},
		{"one byte of data", frame(func(f *acceptFrame) { f.contAuth([]byte{1}, "Srp") }), 0},
		{"salt without B", frame(func(f *acceptFrame) { f.contAuth(serverData[:34], "Srp") }), 0},
		{"names Legacy_Auth, no data", frame(func(f *acceptFrame) { f.contAuth(nil, "Legacy_Auth") }), 0},
		{"names Legacy_Auth, data", frame(func(f *acceptFrame) { f.contAuth([]byte("xx"), "Legacy_Auth") }), 0},
		{"names Legacy_Auth after the public key", frame(func(f *acceptFrame) {
			f.contAuth(nil, "Srp")
			f.contAuth(nil, "Legacy_Auth")
		}), 1},
		{"names Legacy_Auth after the proof", frame(func(f *acceptFrame) {
			f.contAuth(nil, "Srp")
			f.contAuth(serverData, "Srp")
			f.contAuth(nil, "Legacy_Auth")
		}), 2},
		{"names another SRP plugin", frame(func(f *acceptFrame) { f.contAuth(serverData, "Srp256") }), 0},
		{"truncated data length", frame(func(f *acceptFrame) { f.int32(op_cont_auth); f.buf.Write([]byte{0, 0}) }), 0},
		{"truncated data", frame(func(f *acceptFrame) { f.int32(op_cont_auth); f.int32(64); f.buf.Write([]byte{1, 2}) }), 0},
		{"negative data length", frame(func(f *acceptFrame) { f.int32(op_cont_auth); f.int32(-1) }), 0},
		{"huge data length", frame(func(f *acceptFrame) { f.int32(op_cont_auth); f.int32(1 << 30) }), 0},
		{"truncated name", frame(func(f *acceptFrame) { f.int32(op_cont_auth); f.blob(nil); f.int32(9) }), 0},
		{"truncated list", frame(func(f *acceptFrame) { f.int32(op_cont_auth); f.blob(nil); f.blob([]byte("Srp")) }), 0},
		{"truncated keys", frame(func(f *acceptFrame) { f.int32(op_cont_auth); f.blob(nil); f.blob([]byte("Srp")); f.blob(nil) }), 0},
		// Not refused: the case ends only because the script does.
		{"B not hex", frame(func(f *acceptFrame) {
			f.contAuth(nil, "Srp")
			f.contAuth(append(append([]byte{1, 0, 7}, 3, 0), "zzz"...), "Srp")
		}), 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := srpSwitchRun(t, "Srp", tc.rest)
			// The script is a finite reader, so a client that keeps answering
			// runs into its end rather than hanging.
			if _, err := r.p.opAttachResponse(); err == nil {
				t.Fatal("a rogue exchange succeeded")
			}
			out := r.out.Bytes()
			if sent := bytes.Count(out, []byte{0, 0, 0, op_cont_auth, 0}); sent > tc.maxSent {
				t.Errorf("sent %d op_cont_auth, want at most %d: %x", sent, tc.maxSent, out)
			}
			if bytes.Contains(out, []byte(wireTestHash)) || bytes.Contains(out, []byte(hex.EncodeToString([]byte(wireTestHash)))) {
				t.Errorf("the hash went out: %x", out)
			}
			if bytes.Contains(out, []byte(wireTestPassword)) {
				t.Errorf("the password went out: %x", out)
			}
		})
	}
}

// TestSrpAfterAttachLaterContAuthGetsNoProof checks that once the attach reply has
// come, an op_cont_auth during normal work gets the login error and no reply.
func TestSrpAfterAttachLaterContAuthGetsNoProof(t *testing.T) {
	serverData := srpServerData(32, 256)
	var rest acceptFrame
	rest.contAuth(nil, "Srp")
	rest.contAuth(serverData, "Srp")
	rest.opResponseFrame(7, nil)
	rest.contAuth(serverData, "Srp")
	r := srpSwitchRun(t, "Srp", rest.bytes())
	if _, err := r.p.opAttachResponse(); err != nil {
		t.Fatalf("opAttachResponse: %v", err)
	}
	sentBefore := r.out.Len()
	if _, _, _, err := r.p.opResponse(); !errors.Is(err, errLoginRefused) {
		t.Fatalf("err = %v, want the refusal", err)
	}
	if r.out.Len() != sentBefore {
		t.Fatalf("the late op_cont_auth got a reply: %x", r.out.Bytes()[sentBefore:])
	}
}

// TestSrpAfterAttachNotSetOnOtherPaths checks that at protocol 13 every other
// accept leaves no exchange behind, so the attach reply is read as before.
func TestSrpAfterAttachNotSetOnOtherPaths(t *testing.T) {
	serverData := srpServerData(32, 256)
	var withData, condAccept, condData, legacy, accept acceptFrame
	withData.authAccept(op_accept_data, PROTOCOL_VERSION13, serverData, "Srp")
	condAccept.authAccept(op_cond_accept, PROTOCOL_VERSION13, nil, "Srp")
	condAccept.contAuth(serverData, "Srp")
	condAccept.opResponseFrame(0, nil)
	condData.authAccept(op_cond_accept, PROTOCOL_VERSION13, serverData, "Srp")
	condData.opResponseFrame(0, nil)
	legacy.authAccept(op_accept_data, PROTOCOL_VERSION13, nil, "Legacy_Auth")
	accept.acceptHeader(op_accept)
	for name, reply := range map[string][]byte{
		"op_accept_data with data": withData.bytes(),
		"op_cond_accept":           condAccept.bytes(),
		"op_cond_accept with data": condData.bytes(),
		"Legacy_Auth":              legacy.bytes(),
		"op_accept":                accept.bytes(),
	} {
		t.Run(name, func(t *testing.T) {
			p, _, err := connectResponse(t, reply, "false")
			if err != nil {
				t.Fatal(err)
			}
			if p.srpAfterAttach != nil {
				t.Fatal("exchange state set")
			}
		})
	}
}

// TestLegacyHashIsRawAtEverySink checks that the hash goes out raw everywhere:
// Firebird compares it byte for byte, so the wire must carry it the way
// fbclient's LegacyClient produces it.
func TestLegacyHashIsRawAtEverySink(t *testing.T) {
	hexHash := []byte(hex.EncodeToString([]byte(wireTestHash)))
	assertRaw := func(t *testing.T, sent []byte) {
		t.Helper()
		if !bytes.Contains(sent, []byte(wireTestHash)) {
			t.Errorf("no raw hash in %x", sent)
		}
		if bytes.Contains(sent, hexHash) {
			t.Errorf("hex hash in %x", sent)
		}
	}
	for _, wireCrypt := range []string{"true", "false"} {
		for _, opcode := range []int32{op_cond_accept, op_accept_data} {
			var f acceptFrame
			f.authAccept(opcode, PROTOCOL_VERSION13, nil, "Legacy_Auth")
			if opcode == op_cond_accept {
				f.opResponseFrame(0, nil)
			}
			for _, b := range attachBuilders {
				t.Run(fmt.Sprintf("op %d wire_crypt=%s %s", opcode, wireCrypt, b.name), func(t *testing.T) {
					p, sent, err := connectResponse(t, f.bytes(), wireCrypt)
					if err != nil {
						t.Fatal(err)
					}
					if opcode == op_cond_accept {
						assertRaw(t, sent)
					} else if len(sent) != 0 {
						t.Errorf("op_accept_data sent %x before the attach", sent)
					}
					out := new(bytes.Buffer)
					p.conn.writer = bufio.NewWriter(out)
					p.charset = "UTF8"
					if err := b.build(p, "SYSDBA", wireTestPassword); err != nil {
						t.Fatal(err)
					}
					assertRaw(t, out.Bytes())
				})
			}
		}
	}
}

// srpStep says where srpStepServer stops answering.
type srpStep int

const (
	srpStepAttach    srpStep = iota // no reply to the attach
	srpStepPublicKey                // no reply to the client's public key
	srpStepProof                    // no reply to the proof
	srpStepNone                     // completes the login
)

func (s srpStep) String() string {
	return [...]string{"after attach", "after public key", "after proof", "none"}[s]
}

// srpStepServer scripts the switching exchange over loopback and stops at a step:
// holding the socket open, sending half of the next frame, or closing the socket.
type srpStepServer struct {
	addr    string
	reached chan srpStep
	done    chan struct{}
}

type srpStopMode int

const (
	srpStall srpStopMode = iota
	srpStallPartialFrame
	srpCloseSocket
)

func (m srpStopMode) String() string {
	return [...]string{"stall", "stall mid frame", "socket close"}[m]
}

func startSrpStepServer(t *testing.T, stop srpStep, mode srpStopMode) *srpStepServer {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &srpStepServer{addr: listener.Addr().String(), reached: make(chan srpStep, 1), done: make(chan struct{})}
	t.Cleanup(func() { listener.Close() })
	go func() {
		defer close(s.done)
		c, err := listener.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		s.serve(c, stop, mode)
	}()
	return s
}

func (s *srpStepServer) serve(c net.Conn, stop srpStep, mode srpStopMode) {
	r := bufio.NewReader(c)
	var empty, salted, final acceptFrame
	empty.contAuth(nil, "Srp")
	salted.contAuth(srpServerData(32, 256), "Srp")
	final.opResponseFrame(1, nil)
	script := map[srpStep][]byte{srpStepAttach: empty.bytes(), srpStepPublicKey: salted.bytes(), srpStepProof: final.bytes()}
	step := srpStepAttach
	for {
		op, err := readWireInt32(r)
		if err != nil {
			return
		}
		switch op {
		case op_connect:
			if readConnectRequest(r) != nil {
				return
			}
			if _, err := c.Write(srpSwitchAccept("Srp")); err != nil {
				return
			}
			continue
		case op_attach, op_create, op_service_attach:
			if _, err := readAttachRequest(r, op); err != nil {
				return
			}
		case op_cont_auth:
			if readContAuthRequest(r) != nil {
				return
			}
		default:
			return
		}
		if step == stop {
			s.reached <- step
			switch mode {
			case srpCloseSocket:
				return
			case srpStallPartialFrame:
				if _, err := c.Write(script[step][:6]); err != nil {
					return
				}
			}
			// Hold the socket until the client closes it.
			_, _ = io.Copy(io.Discard, r)
			return
		}
		if _, err := c.Write(script[step]); err != nil {
			return
		}
		step++
	}
}

// waitGoroutines waits for the goroutine count to fall back to baseline.
func waitGoroutines(t *testing.T, baseline int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for runtime.NumGoroutine() > baseline {
		if time.Now().After(deadline) {
			buf := make([]byte, 1<<16)
			buf = buf[:runtime.Stack(buf, true)]
			t.Fatalf("goroutines = %d, want at most %d\n%s", runtime.NumGoroutine(), baseline, buf)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

var srpStalledSteps = []srpStep{srpStepAttach, srpStepPublicKey, srpStepProof}

// TestSrpAfterAttachStallRespectsDeadline checks that a server going quiet at any
// step of the follow-up can't outlive the context, on any entry point.
func TestSrpAfterAttachStallRespectsDeadline(t *testing.T) {
	const budget = 300 * time.Millisecond
	for _, entry := range wireEntryPoints("") {
		for _, step := range srpStalledSteps {
			for _, mode := range []srpStopMode{srpStall, srpStallPartialFrame} {
				t.Run(fmt.Sprintf("%s %s %s", entry.name, step, mode), func(t *testing.T) {
					baseline := runtime.NumGoroutine()
					s := startSrpStepServer(t, step, mode)
					ctx, cancel := context.WithTimeout(context.Background(), budget)
					defer cancel()
					start := time.Now()
					closeConn, err := entry.open(ctx, s.addr, "SYSDBA", wireTestPassword, "false")
					elapsed := time.Since(start)
					if err == nil {
						closeConn()
						t.Fatal("connected through a stalled exchange")
					}
					if elapsed > budget+700*time.Millisecond {
						t.Errorf("returned after %v, budget %v", elapsed, budget)
					}
					if !errors.Is(err, context.DeadlineExceeded) {
						t.Errorf("err = %v, want the context's deadline", err)
					}
					select {
					case got := <-s.reached:
						if got != step {
							t.Errorf("stopped at %v, want %v", got, step)
						}
					default:
						t.Errorf("the server never reached %v", step)
					}
					await(t, s.done)
					waitGoroutines(t, baseline)
				})
			}
		}
	}
}

// TestSrpAfterAttachCancelInterrupts checks that an explicit cancel, with no
// deadline, ends the stall too.
func TestSrpAfterAttachCancelInterrupts(t *testing.T) {
	for _, entry := range wireEntryPoints("") {
		for _, step := range srpStalledSteps {
			t.Run(fmt.Sprintf("%s %s", entry.name, step), func(t *testing.T) {
				baseline := runtime.NumGoroutine()
				s := startSrpStepServer(t, step, srpStall)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				result := make(chan error, 1)
				go func() {
					closeConn, err := entry.open(ctx, s.addr, "SYSDBA", wireTestPassword, "false")
					if err == nil {
						closeConn()
					}
					result <- err
				}()
				select {
				case <-s.reached:
				case <-time.After(5 * time.Second):
					t.Fatal("the server never reached the step")
				}
				cancel()
				select {
				case err := <-result:
					if !errors.Is(err, context.Canceled) {
						t.Errorf("err = %v, want context.Canceled", err)
					}
				case <-time.After(2 * time.Second):
					t.Fatal("cancel didn't end the stall")
				}
				waitGoroutines(t, baseline)
			})
		}
	}
}

// TestSrpAfterAttachSocketClose checks that the peer closing the socket
// mid-exchange is a plain failed connect: an error, not a hang or a panic.
func TestSrpAfterAttachSocketClose(t *testing.T) {
	for _, entry := range wireEntryPoints("") {
		for _, step := range srpStalledSteps {
			t.Run(fmt.Sprintf("%s %s", entry.name, step), func(t *testing.T) {
				baseline := runtime.NumGoroutine()
				s := startSrpStepServer(t, step, srpCloseSocket)
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				closeConn, err := entry.open(ctx, s.addr, "SYSDBA", wireTestPassword, "false")
				if err == nil {
					closeConn()
					t.Fatal("connected through a closed socket")
				}
				if errors.Is(err, driver.ErrBadConn) {
					t.Errorf("err = %v, want no driver.ErrBadConn: a failed connect isn't a retryable pooled connection", err)
				}
				await(t, s.done)
				waitGoroutines(t, baseline)
			})
		}
	}
}

// TestSrpAfterAttachCompletesOverSocket runs the whole exchange through the real
// entry points.
func TestSrpAfterAttachCompletesOverSocket(t *testing.T) {
	for _, entry := range wireEntryPoints("") {
		t.Run(entry.name, func(t *testing.T) {
			s := startSrpStepServer(t, srpStepNone, srpStall)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			closeConn, err := entry.open(ctx, s.addr, "SYSDBA", wireTestPassword, "false")
			if err != nil {
				t.Fatal(err)
			}
			closeConn()
			await(t, s.done)
		})
	}
}

// TestSrpAfterAttachEventOwnerCloseEndsStall covers the event path, which has no
// context of its own: closing the owner must end the exchange at every step.
func TestSrpAfterAttachEventOwnerCloseEndsStall(t *testing.T) {
	for _, step := range srpStalledSteps {
		t.Run(step.String(), func(t *testing.T) {
			s := startSrpStepServer(t, step, srpStall)
			e, err := NewFBEvent("SYSDBA:" + wireTestPassword + "@" + s.addr + "/probe?wire_crypt=false")
			if err != nil {
				t.Fatal(err)
			}
			result := subscribeAsync(e)
			select {
			case <-s.reached:
			case <-time.After(5 * time.Second):
				t.Fatal("the server never reached the step")
			}
			closedOwner(t, e)
			canceledResult(t, result)
			await(t, s.done)
		})
	}
}

// TestSrpAfterAttachLive connects through a real plugin switch, if the test server
// makes one. On any other server its subtests skip.
func TestSrpAfterAttachLive(t *testing.T) {
	requireLiveSwitch := func(t *testing.T, options string) {
		t.Helper()
		switched, err := srpSwitchProbe(options)
		if err != nil {
			plaintextRefused(t, err)
			t.Fatalf("handshake: %v", err)
		}
		if !switched {
			t.Skipf("server didn't switch plugin with %q", options)
		}
	}
	for _, options := range []string{"wire_crypt=false", "wire_crypt=false&auth_plugin_name=Legacy_Auth"} {
		t.Run(options, func(t *testing.T) {
			requireLiveSwitch(t, options)
			file := GetTestDatabase("test_srp_switch_")
			dsn := GetTestDSNFromDatabase(file) + "?" + options
			created, err := sql.Open("firebirdsql_createdb", dsn)
			if err != nil {
				t.Fatal(err)
			}
			if err = created.Ping(); err != nil {
				created.Close()
				t.Fatalf("create: %v", err)
			}
			created.Close()

			db, err := sql.Open("firebirdsql", dsn)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			db.SetMaxOpenConns(1)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			conn, err := db.Conn(ctx)
			if err != nil {
				t.Fatalf("attach: %v", err)
			}
			defer conn.Close()
			var cipher string
			var plugin string
			if err = conn.Raw(func(dc any) error {
				fc := dc.(*firebirdsqlConn)
				cipher, plugin = fc.WireCipher(), fc.wp.pluginName
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if cipher != "" {
				t.Errorf("cipher = %q, want plaintext", cipher)
			}
			if plugin != "Srp" && plugin != "Srp256" {
				t.Errorf("plugin = %q, want an SRP one", plugin)
			}
			var n int
			if err = conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM rdb$relations").Scan(&n); err != nil {
				t.Fatalf("query: %v", err)
			}
			if n == 0 {
				t.Error("no relations")
			}

			// A wrong password is an ordinary login failure. A server with another
			// plugin after this one in AuthServer instead moves on to that plugin,
			// and the client refuses to follow.
			bad, err := sql.Open("firebirdsql", GetTestDSNFromDatabaseUserPassword(file, GetTestUser(), "wrong"+GetTestPassword())+"?"+options)
			if err != nil {
				t.Fatal(err)
			}
			defer bad.Close()
			if err = bad.PingContext(ctx); !isLoginFailure(err) {
				t.Errorf("wrong password: err = %v, want isc_login or the refusal", err)
			}
		})
	}

	t.Run("service", func(t *testing.T) {
		requireLiveSwitch(t, "wire_crypt=false")
		requireServiceAvailable(t)
		sm, err := NewServiceManager(testServerAddr(), GetTestUser(), GetTestPassword(), GetDefaultServiceManagerOptions().WithoutWireCrypt())
		if err != nil {
			t.Fatalf("service attach: %v", err)
		}
		defer sm.Close()
		if _, err = sm.GetServerVersion(); err != nil {
			t.Fatalf("GetServerVersion: %v", err)
		}
	})
}

// TestUnknownUserWithoutCipherLive needs no special server. Without wire encryption,
// a server at protocol 13+ answers an unknown user the way it answers a plugin
// switch: op_accept_data with no data. An op_cont_auth at that point would come back
// as isc_unavailable, which hides the login failure. A server with another plugin
// later in AuthServer asks to move on to that plugin after the attach, and the
// client refuses.
func TestUnknownUserWithoutCipherLive(t *testing.T) {
	const user, password = "NO_SUCH_USER", "no_such_password"
	check := func(t *testing.T, err error) {
		t.Helper()
		if err == nil {
			t.Fatal("an unknown user logged in")
		}
		plaintextRefused(t, err)
		if !isLoginFailure(err) {
			t.Errorf("err = %v, want isc_login or the refusal", err)
		}
	}
	t.Run("attach", func(t *testing.T) {
		db, err := sql.Open("firebirdsql", GetTestDSNFromDatabaseUserPassword(GetTestDatabase("test_unknown_user_"), user, password)+"?wire_crypt=false")
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		check(t, db.PingContext(ctx))
	})
	t.Run("service", func(t *testing.T) {
		requireServiceAvailable(t)
		sm, err := NewServiceManager(testServerAddr(), user, password, GetDefaultServiceManagerOptions().WithoutWireCrypt())
		if err == nil {
			defer sm.Close()
		}
		check(t, err)
	})
}

// srpSwitchProbe runs the handshake up to the attach and reports whether the server
// left the login to the attach.
func srpSwitchProbe(options string) (bool, error) {
	d, err := parseDSN(GetTestDSN("srp_switch_probe_") + "?" + options)
	if err != nil {
		return false, err
	}
	errProbeDone := errors.New("probe done")
	var switched bool
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_, err = openFirebirdsqlConn(ctx, d, func(wp *wireProtocol) error {
		switched = wp.srpAfterAttach != nil
		return errProbeDone
	})
	if errors.Is(err, errProbeDone) {
		return switched, nil
	}
	return false, err
}
