/*******************************************************************************
MIT License

Copyright (c) 2024 YuyaOkumura

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*******************************************************************************/
// original https://github.com/convto/ChaCha20

package firebirdsql

import (
	"crypto/cipher"
	"encoding/binary"
	"math/bits"
)

type Cipher struct {
	constant [4]uint32
	key      [8]uint32
	counter  uint32
	nonce    [3]uint32
}

var _ cipher.Stream = (*Cipher)(nil)

func NewCipher(key [32]byte, count uint32, nonce [12]byte) *Cipher {
	c := new(Cipher)
	c.constant = [4]uint32{0x61707865, 0x3320646e, 0x79622d32, 0x6b206574}
	c.key = [8]uint32{
		binary.LittleEndian.Uint32(key[0:4]),
		binary.LittleEndian.Uint32(key[4:8]),
		binary.LittleEndian.Uint32(key[8:12]),
		binary.LittleEndian.Uint32(key[12:16]),
		binary.LittleEndian.Uint32(key[16:20]),
		binary.LittleEndian.Uint32(key[20:24]),
		binary.LittleEndian.Uint32(key[24:28]),
		binary.LittleEndian.Uint32(key[28:32]),
	}
	c.counter = count
	c.nonce = [3]uint32{
		binary.LittleEndian.Uint32(nonce[0:4]),
		binary.LittleEndian.Uint32(nonce[4:8]),
		binary.LittleEndian.Uint32(nonce[8:12]),
	}
	return c
}

func (c *Cipher) toState() [16]uint32 {
	return [16]uint32{
		c.constant[0], c.constant[1], c.constant[2], c.constant[3],
		c.key[0], c.key[1], c.key[2], c.key[3],
		c.key[4], c.key[5], c.key[6], c.key[7],
		c.counter, c.nonce[0], c.nonce[1], c.nonce[2],
	}
}

func (c *Cipher) XORKeyStream(dst, src []byte) {
	// NOTE: Skip error handling because this implementation is learning purpose.
	for len(src) > 0 {
		stream := c.keyStream()
		block := len(stream)
		if len(src) < block {
			block = len(src)
		}
		for i := range block {
			dst[i] = src[i] ^ stream[i]
		}
		c.counter++
		src, dst = src[block:], dst[block:]
	}
}

func (c *Cipher) keyStream() [64]byte {
	x := c.toState()
	for i := 0; i < 10; i++ {
		// column round
		x[0], x[4], x[8], x[12] = qr(x[0], x[4], x[8], x[12])
		x[1], x[5], x[9], x[13] = qr(x[1], x[5], x[9], x[13])
		x[2], x[6], x[10], x[14] = qr(x[2], x[6], x[10], x[14])
		x[3], x[7], x[11], x[15] = qr(x[3], x[7], x[11], x[15])
		// diagonal round
		x[0], x[5], x[10], x[15] = qr(x[0], x[5], x[10], x[15])
		x[1], x[6], x[11], x[12] = qr(x[1], x[6], x[11], x[12])
		x[2], x[7], x[8], x[13] = qr(x[2], x[7], x[8], x[13])
		x[3], x[4], x[9], x[14] = qr(x[3], x[4], x[9], x[14])
	}
	initial := c.toState()
	for i := range x {
		x[i] += initial[i]
	}
	var stream [64]byte
	for i, v := range x {
		stream[i*4] = byte(v)
		stream[i*4+1] = byte(v >> 8)
		stream[i*4+2] = byte(v >> 16)
		stream[i*4+3] = byte(v >> 24)
	}
	return stream
}

func qr(a, b, c, d uint32) (uint32, uint32, uint32, uint32) {
	a += b
	d ^= a
	d = bits.RotateLeft32(d, 16)
	c += d
	b ^= c
	b = bits.RotateLeft32(b, 12)
	a += b
	d ^= a
	d = bits.RotateLeft32(d, 8)
	c += d
	b ^= c
	b = bits.RotateLeft32(b, 7)
	return a, b, c, d
}
