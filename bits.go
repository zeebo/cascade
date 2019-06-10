package cascade

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
	var tmp u64
	copy(tmp[:], br.buf[n:])
	return tmp.toUint64()
}

func (br *bitReader) rawWrite(n uint, val uint64) {
	tmp := toU64(val)
	copy(br.buf[n:], tmp[:])
}

func (br *bitReader) Get(idx uint) uint64 {
	b := idx * br.bits
	return br.rawRead(b/8) >> (b % 8) & br.mask
}

func (br *bitReader) Put(idx uint, val uint64) {
	b := idx * br.bits
	n, o := b/8, b%8
	v := br.rawRead(n)      // read existing 8 bytes at n
	v &^= br.mask << o      // clear the bits we're going to be setting
	v |= val & br.mask << o // set the bits from the value
	br.rawWrite(n, v)       // write it back
}
