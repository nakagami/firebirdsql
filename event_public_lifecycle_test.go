//go:build !plan9
// +build !plan9

package firebirdsql

import (
	"fmt"
	"net"
	"testing"
	"time"
)

// This uses only public driver APIs and a local silent peer, so the same test
// can reproduce the pending-Subscribe bug on the unpatched driver. No Firebird
// server, credentials or external network route is required.
func TestLifecyclePublicClosePendingHandshake(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	owner, err := NewFBEvent(fmt.Sprintf("probe:probe@%s/probe", listener.Addr()))
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close()
	result := make(chan error, 1)
	go func() { _, err := owner.Subscribe([]string{"probe"}, func(Event) {}); result <- err }()
	peer, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	peer.SetReadDeadline(time.Now().Add(2 * time.Second))
	data := make([]byte, 4096)
	if _, err = peer.Read(data); err != nil {
		t.Fatal(err)
	} // handshake is in flight
	closed := make(chan error, 1)
	go func() { closed <- owner.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not return")
	}
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("Subscribe unexpectedly succeeded")
		}
	case <-time.After(2 * time.Second):
		// Release the peer even on the old driver; do not strand its Subscribe.
		peer.Close()
		select {
		case <-result:
		case <-time.After(2 * time.Second):
		}
		t.Fatal("Subscribe remained blocked after Close returned")
	}
}
