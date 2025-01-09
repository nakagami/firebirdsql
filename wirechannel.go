/*******************************************************************************
The MIT License (MIT)

Copyright (c) 2013-2025 Hajime Nakagami

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
	"bufio"
	"crypto/rc4"
	"crypto/sha256"
	"errors"
	"fmt"
	"github.com/nakagami/chacha20"
	"net"
	//"unsafe"
)

type wireChannel struct {
	conn           net.Conn
	reader         *bufio.Reader
	writer         *bufio.Writer
	plugin         string
	rc4reader      *rc4.Cipher
	rc4writer      *rc4.Cipher
	chacha20reader *chacha20.Cipher
	chacha20writer *chacha20.Cipher
}

func newWireChannel(conn net.Conn) (wireChannel, error) {
	var err error
	c := new(wireChannel)
	c.conn = conn
	c.reader = bufio.NewReader(c.conn)
	c.writer = bufio.NewWriter(c.conn)

	return *c, err
}

func (c *wireChannel) setCryptKey(plugin string, sessionKey []byte, nonce []byte) (err error) {
	c.plugin = plugin
	if plugin == "ChaCha" {
		digest := sha256.New()
		digest.Write(sessionKey)
		key := digest.Sum(nil)
		c.chacha20reader, err = chacha20.NewUnauthenticatedCipher(key, nonce)
		c.chacha20writer, err = chacha20.NewUnauthenticatedCipher(key, nonce)
	} else if plugin == "Arc4" {
		c.rc4reader, err = rc4.NewCipher(sessionKey)
		c.rc4writer, err = rc4.NewCipher(sessionKey)
	} else {
		err = errors.New(fmt.Sprintf("Unknown wire encrypto plugin name:%s", plugin))
	}

	return
}

func (c *wireChannel) Read(buf []byte) (n int, err error) {
	if c.plugin != "" {
		src := make([]byte, len(buf))
		n, err = c.reader.Read(src)
		if c.plugin == "ChaCha" {
			c.chacha20reader.XORKeyStream(buf, src[0:n])
		} else if c.plugin == "Arc4" {
			c.rc4reader.XORKeyStream(buf, src[0:n])
		}
		return
	}
	return c.reader.Read(buf)
}

func (c *wireChannel) Write(buf []byte) (n int, err error) {
	if c.plugin != "" {
		dst := make([]byte, len(buf))
		if c.plugin == "ChaCha" {
			c.chacha20writer.XORKeyStream(dst, buf)
		} else if c.plugin == "Arc4" {
			c.rc4writer.XORKeyStream(dst, buf)
		}
		written := 0
		for written < len(buf) {
			n, err = c.writer.Write(dst[written:])
			if err != nil {
				return
			}
			written += n
		}
		n = written
	} else {
		n, err = c.writer.Write(buf)
	}
	return
}

func (c *wireChannel) Flush() error {
	return c.writer.Flush()
}

func (c *wireChannel) Close() error {
	return c.conn.Close()
}
