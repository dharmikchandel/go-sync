package chunk

import (
	"bytes"
	"crypto/rand"
	"errors"
	"io"
	"testing"
	"testing/iotest"
)

func TestReaderSplitsAtBlockBoundaries(t *testing.T) {
	cases := []struct {
		size  int
		sizes []int
	}{
		{0, nil},
		{1, []int{1}},
		{BlockSize - 1, []int{BlockSize - 1}},
		{BlockSize, []int{BlockSize}},
		{BlockSize + 1, []int{BlockSize, 1}},
		{2*BlockSize + 7, []int{BlockSize, BlockSize, 7}},
	}
	for _, tc := range cases {
		data := make([]byte, tc.size)
		rand.Read(data)

		// OneByteReader returns a single byte per Read: blocks must still be
		// full-size, which is exactly the short-read case the old code got wrong.
		r := NewReader(iotest.OneByteReader(bytes.NewReader(data)))
		var got []int
		var joined []byte
		for {
			b, err := r.Next()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			got = append(got, len(b.Data))
			joined = append(joined, b.Data...)
		}
		if len(got) != len(tc.sizes) {
			t.Fatalf("size %d: got block sizes %v, want %v", tc.size, got, tc.sizes)
		}
		for i := range got {
			if got[i] != tc.sizes[i] {
				t.Fatalf("size %d: got block sizes %v, want %v", tc.size, got, tc.sizes)
			}
		}
		if !bytes.Equal(joined, data) {
			t.Fatalf("size %d: blocks don't reassemble to the input", tc.size)
		}
	}
}
