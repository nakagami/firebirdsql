//go:build !plan9

package firebirdsql

import (
	"strings"
	"testing"
)

// eventBuffer builds an op_event payload: a version byte followed by repeated
// <name-length><name><4-byte little-endian count> records.
func eventBuffer(version byte, entries ...struct {
	name  string
	count int32
}) []byte {
	buf := []byte{version}
	for _, e := range entries {
		buf = append(buf, byte(len(e.name)))
		buf = append(buf, e.name...)
		buf = append(buf, byte(e.count), byte(e.count>>8), byte(e.count>>16), byte(e.count>>24))
	}
	return buf
}

func TestEventManagerWait_OversizedBuffer(t *testing.T) {
	var f acceptFrame
	f.int32(op_event)
	f.int32(7)                // event handle
	f.int32(maxEpbLength + 1) // claimed event-buffer size: out of range

	em := &eventManager{wp: testProtocol(f.bytes()), done: make(chan struct{})}
	t.Cleanup(func() { <-em.done })
	chErr := em.wait(newRemoteEvent(), make(chan Event, 1))
	err := <-chErr
	if err == nil {
		t.Fatal("expected error for oversized event buffer, got nil")
	}
	if !strings.Contains(err.Error(), "out of range") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestEventManagerWait_TruncatedRead(t *testing.T) {
	var f acceptFrame
	f.int32(op_event)
	f.int32(7) // handle, then the stream ends before the size word

	em := &eventManager{wp: testProtocol(f.bytes()), done: make(chan struct{})}
	t.Cleanup(func() { <-em.done })
	chErr := em.wait(newRemoteEvent(), make(chan Event, 1))
	if err := <-chErr; err == nil {
		t.Fatal("expected error for truncated event frame, got nil")
	}
}

func TestGetEventCounts_Valid(t *testing.T) {
	e := newRemoteEvent()
	if err := e.queueEvents("evt"); err != nil {
		t.Fatal(err)
	}
	data := eventBuffer(byte(EPB_version1), struct {
		name  string
		count int32
	}{"evt", 5})

	events, err := e.getEventCounts(data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(events) != 1 || events[0].Name != "evt" || events[0].Count != 4 {
		t.Errorf("events: got %+v, want one evt with count 4", events)
	}
}

func TestGetEventCounts_Malformed(t *testing.T) {
	cases := []struct {
		name    string
		data    []byte
		wantSub string
	}{
		{"empty", nil, "version byte"},
		{"bad version", []byte{9, 3, 'e', 'v', 't'}, "version byte"},
		{"name past end", []byte{byte(EPB_version1), 50, 'e', 'v', 't'}, "name length"},
		{"count truncated", []byte{byte(EPB_version1), 3, 'e', 'v', 't', 0, 0}, "count truncated"},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			e := newRemoteEvent()
			_ = e.queueEvents("evt")
			_, err := e.getEventCounts(tt.data)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !strings.Contains(err.Error(), tt.wantSub) {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}
