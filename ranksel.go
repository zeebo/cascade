package cascade

import (
	"fmt"
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
	buf     []byte // backing array
	quo     uint   // bits per quotient
	quoMask uint64 // mask for quotient
	rem     uint   // bits per remainder
	remMask uint64 // mask for remainder
	block   uint64 // size of a block. 17 + 8*rem
}

func newRSQFData(buf []byte, quo, rem uint) *rsqfData {
	return &rsqfData{
		buf:     buf,
		quo:     quo,
		quoMask: 1<<quo - 1,
		rem:     rem,
		remMask: 1<<rem - 1,
		block:   17 + 8*uint64(rem),
	}
}

// Offset returns a pointer to the ith offset, which is the offset for the
// i*64th quotient.
func (r *rsqfData) Offset(i uint64) *uint8 {
	return &r.buf[r.block*i]
}

// Occupied returns a pointer to the ith occupied vector, which contains the occupied
// bits for the quotients in [64 * i, 64 * i + 64).
func (r *rsqfData) Occupied(i uint64) *u64 {
	off := r.block*i + 1
	return (*u64)(unsafe.Pointer(&r.buf[:off+8][off]))
}

// Runends returns a pointer to the ith runends vector, which contains the runends
// information for the bits in [64 * i, 64 * i + 64).
func (r *rsqfData) Runends(i uint64) *u64 {
	off := r.block*i + 9
	return (*u64)(unsafe.Pointer(&r.buf[:off+8][off]))
}

// Remainders returns a bit reader for the ith remainders vector, which contains the
// remainders for the slots in [64 * i, 64 * i + 64).
func (r *rsqfData) Remainders(i uint64) bitReader {
	off := r.block*i + 17
	return newBitReader(r.buf[off:off+uint64(r.rem)*8], r.rem)
}

// Rank returns the number of set bits of the occupied bit vector starting at the
// sth bit and stopping at the (s+b)th bit. b is at most 64.
func (r *rsqfData) OccupiedRank(s, b uint64) uint {
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

	fmt.Printf("--- rank from bit %d for %d bits: %d\n", s, b, rank)
	return rank
}

// SelectRunends returns the number of bits past s until the bth bit is set.
func (r *rsqfData) RunendsSelect(s, b uint64) uint {
	idx, off, acc := s/64, uint(s%64), uint(0)
	fmt.Printf("--- selecting after bit %d for %d bits: ", s, b)

check:
	run := r.Runends(idx).toUint64()
	run >>= off

	// use popcount to traverse a word at a time.
	if count := uint64(bits.OnesCount64(run)); count <= b {
		acc += 64 - off
		b -= count
		off, idx = 0, idx+1
		goto check
	}

	// now that we're in a word, see if we can pop off as many bits as possible
	if count := uint64(bits.OnesCount32(uint32(run))); count <= b {
		acc += 32
		run >>= 32
		b -= count
	}
	if count := uint64(bits.OnesCount16(uint16(run))); count <= b {
		acc += 16
		run >>= 16
		b -= count
	}
	if count := uint64(bits.OnesCount8(uint8(run))); count <= b {
		acc += 8
		run >>= 8
		b -= count
	}

	// clear off the b lowest order bits in run
	for ; b > 0; b-- {
		run &= run - 1
	}

	fmt.Println(acc + uint(bits.TrailingZeros64(run)))
	return acc + uint(bits.TrailingZeros64(run))
}

// QuotientSlot returns the furthest slot that ends a run for some
// quotient in [q / 64 * 64, q].
func (r *rsqfData) QuotientSlot(q uint64) uint64 {
	rep := q / 64 * 64

	off := uint64(*r.Offset(q / 64))
	if rep == q {
		return q + off
	}

	d := uint64(r.OccupiedRank(rep+1, q-rep-1))
	if d == 0 {
		return rep + off
	}

	t := uint64(r.RunendsSelect(rep+off+1, d))
	return rep + off + t + 1
}

// Lookup reports true for any hash that has been inserted and possibly for some
// hashes that have not been inserted. If it ever reports false, then the hash
// has definitely not been inserted.
func (r *rsqfData) Lookup(hash uint64) bool {
	rem := hash & r.remMask
	hash >>= r.rem
	quo := hash & r.quoMask

	if r.Occupied(quo/64).toUint64()&(1<<quo%64-1) == 0 {
		return false
	}

	slot := r.QuotientSlot(quo)
	block := slot / 64
	rems := r.Remainders(block)
	runs := r.Runends(block).toUint64()
	idx := uint(slot % 64)
	sel := uint64(1) << idx

next:
	slotRem := rems.Get(idx)

	if slotRem == rem {
		return true
	} else if slotRem < rem || runs&sel != 0 {
		return false
	}

	if idx > 0 {
		idx--
		sel >>= 1
		goto next
	} else if block > 0 {
		block--
		rems = r.Remainders(block)
		runs = r.Runends(block).toUint64()
		idx = 63
		sel = 1 << 63
		goto next
	}

	return false
}

// Insert adds the hash to the filter so that Lookup will definitely report
// yes. If insert reports false, then the filter is in a broken state and no
// further operations should be performed on it. This should never happen if
// the hashes are randomly distributed.
func (r *rsqfData) Insert(hash uint64) bool {
	rem := hash & r.remMask
	hash >>= r.rem
	quo := hash & r.quoMask
	qblock, qidx := quo/64, uint(quo%64)

	slot := r.QuotientSlot(quo)
	fmt.Printf("quo:%d rem:%d slot:%d\n", quo, rem, slot)

	occs := r.Occupied(qblock)
	if quo > slot || (quo == slot && quo == qblock*64 && occs.toUint64()&(1<<qidx) == 0) {
		rems := r.Remainders(qblock)
		rems.Put(qidx, rem)
		occs := r.Occupied(qblock)
		*occs = toU64(occs.toUint64() | 1<<qidx)
		ends := r.Runends(qblock)
		*ends = toU64(ends.toUint64() | 1<<qidx)
		return true
	}

	return false
}

// findUnused finds the first unused slot after the quotient.
func (r *rsqfData) findUnused(quo uint64) uint64 {
next:
	end := r.QuotientSlot(quo)
	if quo > end {
		return quo
	}
	quo = end + 1
	goto next
}
