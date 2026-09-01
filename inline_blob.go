/*******************************************************************************
The MIT License (MIT)

Copyright (c) 2026 Alexey Kovyazin

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
	"database/sql/driver"
	"fmt"
	"sync"
)

type inlineBlobCache struct {
	mu      sync.Mutex
	maxSize int64
	size    int64
	byTx    map[int32]map[int64][]byte
}

func newInlineBlobCache(maxSize int) *inlineBlobCache {
	cacheSize := int64(maxSize)
	if cacheSize <= 0 {
		cacheSize = -1
	}
	return &inlineBlobCache{
		maxSize: cacheSize,
		byTx:    make(map[int32]map[int64][]byte, 4),
	}
}

func (c *inlineBlobCache) add(txHandle int32, blobId int64, data []byte) bool {
	if c == nil || c.maxSize <= 0 {
		return false
	}
	dataLen := int64(len(data))
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.size+dataLen > c.maxSize {
		return false
	}
	byBlob := c.byTx[txHandle]
	if byBlob == nil {
		byBlob = make(map[int64][]byte)
		c.byTx[txHandle] = byBlob
	}
	if _, exists := byBlob[blobId]; exists {
		return false
	}
	byBlob[blobId] = data
	c.size += dataLen
	return true
}

func (c *inlineBlobCache) getAndRemove(txHandle int32, blobId int64) ([]byte, bool) {
	if c == nil || c.maxSize <= 0 {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	byBlob := c.byTx[txHandle]
	if byBlob == nil {
		return nil, false
	}
	data, ok := byBlob[blobId]
	if !ok {
		return nil, false
	}
	delete(byBlob, blobId)
	c.size -= int64(len(data))
	if c.size < 0 {
		c.size = 0
	}
	if len(byBlob) == 0 {
		delete(c.byTx, txHandle)
	}
	return data, true
}

func (c *inlineBlobCache) clearTx(txHandle int32) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	byBlob := c.byTx[txHandle]
	if byBlob == nil {
		return
	}
	var removed int64
	for _, data := range byBlob {
		removed += int64(len(data))
	}
	delete(c.byTx, txHandle)
	c.size -= removed
	if c.size < 0 {
		c.size = 0
	}
}

func (c *inlineBlobCache) clearAll() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.byTx = make(map[int32]map[int64][]byte, 4)
	c.size = 0
}

func decodeInlineSegmentBuffer(buf []byte) ([]byte, error) {
	if len(buf) == 0 {
		return buf[:0], nil
	}
	outPos := 0
	for pos := 0; pos < len(buf); {
		if pos+2 > len(buf) {
			return nil, fmt.Errorf("invalid inline blob segment buffer length")
		}
		segLen := int(bytes_to_uint16(buf[pos : pos+2]))
		pos += 2
		if pos+segLen > len(buf) {
			return nil, fmt.Errorf("invalid inline blob segment length %d at %d", segLen, pos)
		}
		if segLen == 0 {
			continue
		}
		copy(buf[outPos:], buf[pos:pos+segLen])
		pos += segLen
		outPos += segLen
	}
	return buf[:outPos], nil
}

func blobIdToInt64(blobId []byte) (int64, error) {
	if len(blobId) != 8 {
		return 0, fmt.Errorf("firebirdsql: blob id length %d, want 8", len(blobId))
	}
	return bytes_to_bint64(blobId), nil
}

// parseInlineBlobPayload reads an op_inline_blob body after the opcode has been consumed.
func (p *wireProtocol) parseInlineBlobPayload() (txHandle int32, blobId int64, data []byte, err error) {
	b, err := p.recvPackets(4)
	if err != nil {
		return 0, 0, nil, err
	}
	txHandle = bytes_to_bint32(b)

	b, err = p.recvPackets(8)
	if err != nil {
		return 0, 0, nil, err
	}
	blobId = bytes_to_bint64(b)

	b, err = p.recvPackets(4)
	if err != nil {
		return 0, 0, nil, err
	}
	infoLen := int(bytes_to_bint32(b))
	if infoLen < 0 || infoLen > maxWirePayload {
		return 0, 0, nil, fmt.Errorf("firebirdsql: inline blob info length %d out of range: %w", infoLen, driver.ErrBadConn)
	}
	if infoLen > 0 {
		if _, err = p.recvPacketsAlignment(infoLen); err != nil {
			return 0, 0, nil, err
		}
	}

	b, err = p.recvPackets(4)
	if err != nil {
		return 0, 0, nil, err
	}
	dataLen := int(bytes_to_bint32(b))
	if dataLen < 0 || dataLen > maxWirePayload {
		return 0, 0, nil, fmt.Errorf("firebirdsql: inline blob data length %d out of range: %w", dataLen, driver.ErrBadConn)
	}
	if dataLen == 0 {
		return txHandle, blobId, nil, nil
	}
	raw, err := p.recvPacketsAlignment(dataLen)
	if err != nil {
		return 0, 0, nil, err
	}
	data, err = decodeInlineSegmentBuffer(raw)
	return txHandle, blobId, data, err
}

// consumeInlineBlobsStarting reads zero or more op_inline_blob packets starting from
// an already-read opcode in b. Returns the next non-inline opcode bytes.
func (p *wireProtocol) consumeInlineBlobsStarting(b []byte) ([]byte, error) {
	for bytes_to_bint32(b) == op_inline_blob {
		txHandle, blobId, data, err := p.parseInlineBlobPayload()
		if err != nil {
			return nil, err
		}
		if p.inlineBlobCache != nil {
			p.inlineBlobCache.add(txHandle, blobId, data)
		}
		b, err = p.recvPackets(4)
		if err != nil {
			return nil, err
		}
	}
	return b, nil
}

func (p *wireProtocol) clearInlineBlobCache(txHandle int32) {
	if p != nil && p.inlineBlobCache != nil {
		p.inlineBlobCache.clearTx(txHandle)
	}
}

func (p *wireProtocol) clearAllInlineBlobCache() {
	if p != nil && p.inlineBlobCache != nil {
		p.inlineBlobCache.clearAll()
	}
}
