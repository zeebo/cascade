package cascade

import (
	"math/bits"
	"unsafe"
)

//
// the standard layout would be
//
// | 8 bits offset    |
// | 64 bits occupied |
// | 64 bits runends  |
// | r bit remainders | * 64
//
// but then the words are all misaligned by the first byte of offsets.
// putting that byte at the end doesn't help because subsequent blocks
// will also be misaligned.
//
// instead, combine 8 blocks into one superblock and move all the offsets to the front.
//
// | 8 bits offset | * 8
// (
//   | 64 bits occupied |
//   | 64 bits runends  |
//   | r bit remainders | * 64
// ) * 8
//
// this means each superblock has at least 512 elements, so q must be at least 9.
// if q is 9, then we can have any remainder (at most 64 - 9 = 55) and the block
// will still fit in 4096 bytes. the calculation for the number of bits contained
// in a superblock is 8 * 8 + (64 + 64 + 64 * r) * 8
//
// offsets store how far to go from the quotient to the end of the run for that
// quotient. it assumes that runs are typically short, and with high probability
// offsets are never more than O(q). since we allow offsets to go up to 256 before
// we error, there should be no problem unless an uneven distribution of hashes
// is added.
//

type rsqfData struct {
	buf    []byte // backing array
	rem    uint   // bits per remainder
	block  uint   // size of a block. (2 + r) * 8
	sblock uint   // size of a superblock. 8 + (2 + r) * 64
}

// Offset returns a pointer to the nth offset, which is the offset for the n*64th quotient.
func (r *rsqfData) Offset(n uint) *uint8 {
	idx := r.sblock * (n / 8) // superblock offset
	off := n % 8              // offset into superblock
	return &r.buf[idx+off]
}

// Occupied returns a pointer to the nth occupied vector, which contains the occupied
// bits for the quotients in [64 * n, 64 * n + 64).
func (r *rsqfData) Occupied(n uint) *u64 {
	idx := r.sblock * (n / 8) // superblock offset
	off := 8 + r.block*(n%8)  // offset into superblock
	return (*u64)(unsafe.Pointer(&r.buf[idx+off : idx+off+8][0]))
}

// Runends returns a pointer to the nth runends vector, which contains the runends
// information for the bits in [64 * n, 64 * n + 64).
func (r *rsqfData) Runends(n uint) *u64 {
	idx := r.sblock * (n / 8)    // superblock offset
	off := 8 + r.block*(n%8) + 8 // offset into superblock
	return (*u64)(unsafe.Pointer(&r.buf[idx+off : idx+off+8][0]))
}

// Remainders returns a bit reader for the nth remainders vector, which contains the
// remainders for the slots in [64 * n, 64 * n + 64).
func (r *rsqfData) Remainders(n uint) bitReader {
	idx := r.sblock * (n / 8)        // superblock offset
	off := 8 + r.block*(n%8) + 8 + 8 // offset into superblock
	return newBitReader(r.buf[idx+off:idx+off+r.rem*8], r.rem)
}

func rank(v u64, i uint) int {
	return bits.OnesCount64(v.toUint64() & (1<<(i&63) - 1))
}
