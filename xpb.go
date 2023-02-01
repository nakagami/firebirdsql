package firebirdsql

import "bytes"

type XPBReader struct {
	buf []byte
	pos int
}

type XPBWriter struct {
	buf []byte
}

func NewXPBReader(buf []byte) *XPBReader {
	return &XPBReader{buf, 0}
}

func (pb *XPBReader) Next() (have bool, value byte) {
	if pb.End() {
		return false, 0
	}
	b := pb.buf[pb.pos]
	pb.pos++
	return true, b
}

func (pb *XPBReader) End() bool {
	return pb.pos >= len(pb.buf)
}

func (pb *XPBReader) Get() byte {
	b := pb.buf[pb.pos]
	return b
}

func (pb *XPBReader) GetString() string {
	l := int(pb.GetInt16())
	s := bytes_to_str(pb.buf[pb.pos : pb.pos+l])
	pb.pos += l
	return s
}

func (pb *XPBReader) GetInt16() int16 {
	r := bytes_to_int16(pb.buf[pb.pos : pb.pos+2])
	pb.pos += 2
	return r
}

func (pb *XPBReader) GetInt32() int32 {
	r := bytes_to_int32(pb.buf[pb.pos : pb.pos+4])
	pb.pos += 4
	return r
}

func NewXPBWriter() *XPBWriter {
	return &XPBWriter{}
}

func NewXPBWriterFromTag(tag byte) *XPBWriter {
	return &XPBWriter{buf: []byte{tag}}
}

func NewXPBWriterFromBytes(bytes []byte) *XPBWriter {
	return &XPBWriter{buf: bytes}
}

func (pb *XPBWriter) PutTag(tag byte) *XPBWriter {
	pb.buf = bytes.Join([][]byte{
		pb.buf,
		{tag},
	}, nil)
	return pb
}

func (pb *XPBWriter) PutByte(tag byte, val byte) *XPBWriter {
	pb.buf = bytes.Join([][]byte{
		pb.buf,
		{tag, val},
	}, nil)
	return pb
}

func (pb *XPBWriter) PutInt16(tag byte, val int16) *XPBWriter {
	pb.buf = bytes.Join([][]byte{
		pb.buf,
		{tag}, int16_to_bytes(val),
	}, nil)
	return pb
}

func (pb *XPBWriter) PutInt32(tag byte, val int32) *XPBWriter {
	pb.buf = bytes.Join([][]byte{
		pb.buf,
		{tag}, int32_to_bytes(val),
	}, nil)
	return pb
}

func (pb *XPBWriter) PutString(tag byte, val string) *XPBWriter {
	strBytes := str_to_bytes(val)
	pb.buf = bytes.Join([][]byte{
		pb.buf,
		{tag}, int16_to_bytes(int16(len(strBytes))), strBytes,
	}, nil)
	return pb
}

func (pb *XPBWriter) PutBytes(buf []byte) *XPBWriter {
	pb.buf = bytes.Join([][]byte{
		pb.buf,
		buf,
	}, nil)
	return pb
}

func (pb *XPBWriter) Bytes() []byte {
	return pb.buf
}
