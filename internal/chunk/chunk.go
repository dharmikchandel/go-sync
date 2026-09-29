// Package chunk splits file content into blocks.
package chunk

import (
	"crypto/sha256"
	"errors"
	"io"
)

// BlockSize is the maximum block size. 4 MiB (the size Dropbox uses) keeps
// per-block overhead small for big files while bounding memory: a client or
// server never holds more than a few blocks at once.
const BlockSize = 4 << 20

type Block struct {
	Hash [32]byte
	Data []byte
}

// Reader yields fixed-size blocks from an io.Reader. Every block is exactly
// BlockSize bytes except the last, which may be shorter. An empty input
// yields no blocks.
type Reader struct {
	r io.Reader
}

func NewReader(r io.Reader) *Reader {
	return &Reader{r: r}
}

// Next returns the next block, or io.EOF after the last one. Each block has
// its own buffer, so callers may keep blocks while reading further.
func (c *Reader) Next() (Block, error) {
	buf := make([]byte, BlockSize)
	// io.ReadFull loops over short reads. It returns io.EOF only when nothing
	// was read, and io.ErrUnexpectedEOF for a final partial block.
	n, err := io.ReadFull(c.r, buf)
	switch {
	case errors.Is(err, io.EOF):
		return Block{}, io.EOF
	case err != nil && !errors.Is(err, io.ErrUnexpectedEOF):
		return Block{}, err
	}
	data := buf[:n]
	return Block{Hash: sha256.Sum256(data), Data: data}, nil
}

// ContentID identifies content by its manifest: the SHA-256 of its block
// hashes, in order. Equal bytes (split the same way) give equal IDs, so the
// sync client can compare a local file with a server version without
// downloading it.
func ContentID(hashes [][]byte) [32]byte {
	h := sha256.New()
	for _, b := range hashes {
		h.Write(b)
	}
	return [32]byte(h.Sum(nil))
}

// Manifest reads r to the end and returns its block hashes in order.
func Manifest(r io.Reader) ([][]byte, error) {
	var hashes [][]byte
	chunks := NewReader(r)
	for {
		b, err := chunks.Next()
		if errors.Is(err, io.EOF) {
			return hashes, nil
		}
		if err != nil {
			return nil, err
		}
		hashes = append(hashes, b.Hash[:])
	}
}
