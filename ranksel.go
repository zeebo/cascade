package cascade

import (
	"math/bits"
	"unsafe"
)

//
// the layout of a block is
//
// | 8 bits offset    |
// | 64 bits occupied |
// | 64 bits runends  |
// | r bit remainders | * 64
//
// offsets store how far to go from the quotient to the end of the run for that
// quotient. it assumes that runs are typically short, and with high probability
// offsets are never more than O(q). since we allow offsets to go up to 256 before
// we error, there should be no problem unless an uneven distribution of hashes
// is added.
//

type rsqfData struct {
	buf   []byte // backing array
	rem   uint   // bits per remainder
	block uint   // size of a block. 17 + 8*rem
}

func newRSQFData(buf []byte, rem uint) *rsqfData {
	return &rsqfData{
		buf:   buf,
		rem:   rem,
		block: 17 + 8*rem,
	}
}

// Offset returns a pointer to the ith offset, which is the offset for the
// i*64th quotient.
func (r *rsqfData) Offset(i uint) *uint8 {
	return &r.buf[r.block*i]
}

// Occupied returns a pointer to the ith occupied vector, which contains the occupied
// bits for the quotients in [64 * i, 64 * i + 64).
func (r *rsqfData) Occupied(i uint) *u64 {
	off := r.block*i + 1
	return (*u64)(unsafe.Pointer(&r.buf[:off+8][off]))
}

// Runends returns a pointer to the ith runends vector, which contains the runends
// information for the bits in [64 * i, 64 * i + 64).
func (r *rsqfData) Runends(i uint) *u64 {
	off := r.block*i + 9
	return (*u64)(unsafe.Pointer(&r.buf[:off+8][off]))
}

// Remainders returns a bit reader for the ith remainders vector, which contains the
// remainders for the slots in [64 * i, 64 * i + 64).
func (r *rsqfData) Remainders(i uint) bitReader {
	off := r.block*i + 17
	return newBitReader(r.buf[off:off+r.rem*8], r.rem)
}

// Rank returns the number of set bits of the occupied bit vector starting at the
// sth bit and stopping at the (s+b)th bit. b is at most 64.
func (r *rsqfData) OccupiedRank(s, b uint) uint {
	idx, off := s/64, s%64

	// we remove off lower order bits and keep at most b higher order bits.
	occ := r.Occupied(idx).toUint64()
	occ >>= off
	occ <<= (64 - b) % 64
	rank := uint(bits.OnesCount64(occ))

	// if we overflow a single uint64, then grab the next one.
	if shift := 128 - off - b; shift < 64 {
		occ = r.Occupied(idx + 1).toUint64()
		occ <<= shift
		rank += uint(bits.OnesCount64(occ))
	}

	return rank
}

// SelectRunends returns the number of bits past s until the bth bit is set.
func (r *rsqfData) RunendsSelect(s, b uint) uint {
	idx, off, acc := s/64, s%64, uint(0)

check:
	run := r.Runends(idx).toUint64()
	run >>= off

	// use popcount to traverse a word at a time.
	if count := uint(bits.OnesCount64(run)); count <= b {
		acc += 64 - off
		b -= count
		off, idx = 0, idx+1
		goto check
	}

	// now that we're in a word, see if we can pop off as many bits as possible
	if count := uint(bits.OnesCount32(uint32(run))); count <= b {
		acc += 32
		run >>= 32
		b -= count
	}
	if count := uint(bits.OnesCount16(uint16(run))); count <= b {
		acc += 16
		run >>= 16
		b -= count
	}
	if count := uint(bits.OnesCount8(uint8(run))); count <= b {
		acc += 8
		run >>= 8
		b -= count
	}

	// clear off the b lowest order bits in run
	for ; b > 0; b-- {
		run &= run - 1
	}

	return acc + uint(bits.TrailingZeros64(run))
}

// QuotientSlot returns the slot that the run for the provided quotient ends at.
// It must be called on a quotient that has been inserted.
func (r *rsqfData) QuotientSlot(q uint) uint {
	rep := q / 64 * 64

	off := uint(*r.Offset(q / 64))
	if rep == q {
		return q + off
	}

	d := r.OccupiedRank(rep+1, q-rep-1)
	if d == 0 {
		return rep + off
	}

	t := r.RunendsSelect(rep+off+1, d)
	return rep + off + t + 1
}
