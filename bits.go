package cascade

import (
	"encoding/binary"
)

// bitReader abstracts reading values from a byte array where the
// values are all some size in bits. The lowest order bits are
// used in the uint64s, and the number of bits per value must
// be no more than 64 - 8 == 56.
type bitReader struct {
	buf  []byte
	bits uint
	mask uint64
}

func newBitReader(buf []byte, bits uint) bitReader {
	return bitReader{
		buf:  buf,
		bits: bits,
		mask: 1<<bits - 1,
	}
}

func (br *bitReader) rawRead(n uint) uint64 {
	var tmp [8]byte
	copy(tmp[:], br.buf[n:])
	return binary.LittleEndian.Uint64(tmp[:])
}

func (br *bitReader) rawWrite(n uint, val uint64) {
	var tmp [8]byte
	binary.LittleEndian.PutUint64(tmp[:], val)
	copy(br.buf[n:], tmp[:])
}

func (br *bitReader) Get(idx uint) uint64 {
	b := idx * br.bits
	return br.rawRead(b/8) >> (b % 8) & br.mask
}

func (br *bitReader) Put(idx uint, val uint64) {
	b := idx * br.bits
	n, o := b/8, b%8
	v := br.rawRead(n)
	v &^= br.mask << o
	v |= val & br.mask << o
	br.rawWrite(n, v)
}
