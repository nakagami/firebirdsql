package firebirdsql

import (
	"bytes"
	"compress/zlib"
	"io"
	"testing"
)

func TestCompressionRoundtrip(t *testing.T) {
	// Test that data compressed by zlib.Writer can be decompressed by zlib.Reader
	original := []byte("Hello, Firebird wire protocol compression!")

	var compressed bytes.Buffer
	w := zlib.NewWriter(&compressed)
	_, err := w.Write(original)
	if err != nil {
		t.Fatalf("Failed to compress: %v", err)
	}
	err = w.Close()
	if err != nil {
		t.Fatalf("Failed to close writer: %v", err)
	}

	r, err := zlib.NewReader(&compressed)
	if err != nil {
		t.Fatalf("Failed to create reader: %v", err)
	}
	decompressed, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("Failed to decompress: %v", err)
	}
	r.Close()

	if !bytes.Equal(original, decompressed) {
		t.Errorf("Decompressed data doesn't match original.\nOriginal: %s\nDecompressed: %s", original, decompressed)
	}
}

func TestStreamingCompression(t *testing.T) {
	// Test that streaming compression works across multiple messages
	messages := [][]byte{
		[]byte("first message"),
		[]byte("second message"),
		[]byte("third message with more data"),
	}

	var compressed bytes.Buffer
	w := zlib.NewWriter(&compressed)

	for _, msg := range messages {
		w.Write(msg)
		w.Flush()
	}

	r, _ := zlib.NewReader(&compressed)
	var recovered [][]byte

	for _, msg := range messages {
		decompressed := make([]byte, len(msg))
		n, err := io.ReadFull(r, decompressed)
		if err != nil {
			t.Fatalf("Failed to decompress message: %v", err)
		}
		recovered = append(recovered, decompressed[:n])
	}
	r.Close()

	if len(recovered) != len(messages) {
		t.Errorf("Expected %d messages, got %d", len(messages), len(recovered))
	}

	for i, msg := range messages {
		if !bytes.Equal(msg, recovered[i]) {
			t.Errorf("Message %d doesn't match.\nExpected: %s\nGot: %s", i, msg, recovered[i])
		}
	}
}

func TestPflagCompressDetection(t *testing.T) {
	// Test that pflag_compress flag is correctly detected and stripped
	acceptType := ptype_lazy_send | pflag_compress // 5 | 0x100 = 0x105

	if (acceptType & pflag_compress) == 0 {
		t.Error("pflag_compress should be detected")
	}

	stripped := acceptType & ptype_MASK
	if stripped != ptype_lazy_send {
		t.Errorf("Expected %d after stripping, got %d", ptype_lazy_send, stripped)
	}
}

func TestPflagCompressNotSet(t *testing.T) {
	// Test that missing pflag_compress is correctly detected
	acceptType := ptype_lazy_send // 5

	if (acceptType & pflag_compress) != 0 {
		t.Error("pflag_compress should not be detected")
	}
}

func TestLargeDataCompression(t *testing.T) {
	// Test compression with larger data payloads
	// Simulate a large query result
	original := bytes.Repeat([]byte("A"), 100000)

	var compressed bytes.Buffer
	w := zlib.NewWriter(&compressed)
	w.Write(original)
	w.Close()

	// Compressed size should be significantly smaller for repetitive data
	if compressed.Len() >= len(original) {
		t.Errorf("Compression didn't reduce size for repetitive data. Original: %d, Compressed: %d",
			len(original), compressed.Len())
	}

	r, _ := zlib.NewReader(&compressed)
	decompressed, _ := io.ReadAll(r)
	r.Close()

	if !bytes.Equal(original, decompressed) {
		t.Error("Large data decompression failed")
	}
}

func TestDSNWireCompressParameter(t *testing.T) {
	// Test that wire_compress parameter is correctly parsed from DSN
	testDSNs := []struct {
		dsn          string
		wireCompress string
	}{
		{"user:password@localhost/dbname", "false"},
		{"user:password@localhost/dbname?wire_compress=true", "true"},
		{"user:password@localhost/dbname?wire_compress=false", "false"},
		{"user:password@localhost/dbname?wire_crypt=true&wire_compress=true", "true"},
	}

	for _, d := range testDSNs {
		dsn, err := parseDSN(d.dsn)
		if err != nil {
			t.Fatalf("Failed to parse DSN %s: %v", d.dsn, err)
		}
		if dsn.options["wire_compress"] != d.wireCompress {
			t.Errorf("DSN %s: expected wire_compress=%s, got %s",
				d.dsn, d.wireCompress, dsn.options["wire_compress"])
		}
	}
}
