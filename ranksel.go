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
	base      unsafe.Pointer // backing array
	block     uint64         // size of a block. 17 + 8*rem
	blockMask uint64         // mask of number of blocks
	quoBits   uint           // bits per quotient
	quoMask   uint64         // mask for quotient
	remBits   uint           // bits per remainder
	remMask   uint64         // mask for remainder
}

// newRSQFData returns an abstraction around rsqf data. base must be a pointer to
// a buffer of bytes 2^quo * (rem + 2.125) + 7 bytes, and quo must be at least 6.
func newRSQFData(base *byte, quo, rem uint) *rsqfData {
	return &rsqfData{
		base:      unsafe.Pointer(base),
		quoBits:   quo,
		quoMask:   1<<quo - 1,
		remBits:   rem,
		remMask:   1<<rem - 1,
		block:     18 + 8*uint64(rem),
		blockMask: 1<<(quo-6) - 1,
	}
}

// rsqfBlock is a struct matching the layout of a block of data for easy code manipulation.
// it is important to use u64 so that padding is not inserted.
type rsqfBlock struct {
	offset   uint8
	occupied u64
	runends  u64
	// 512 is definitely too large by 8 bytes, so we're safe to
	// do a raw read of 8 bytes at any point up to 512.
	rems [512]byte
}

// enforce at compile time the offsets of the individual fields.
type _ struct {
	// offset is at offset 0
	_ [unsafe.Offsetof(rsqfBlock{}.offset) - 0]struct{}
	_ [0 - unsafe.Offsetof(rsqfBlock{}.offset)]struct{}

	// occupied is at offset 1
	_ [unsafe.Offsetof(rsqfBlock{}.occupied) - 1]struct{}
	_ [1 - unsafe.Offsetof(rsqfBlock{}.occupied)]struct{}

	// runends is at offset 9
	_ [unsafe.Offsetof(rsqfBlock{}.runends) - 9]struct{}
	_ [9 - unsafe.Offsetof(rsqfBlock{}.runends)]struct{}

	// rems is at offset 17
	_ [unsafe.Offsetof(rsqfBlock{}.rems) - 17]struct{}
	_ [17 - unsafe.Offsetof(rsqfBlock{}.rems)]struct{}
}

// getU64 reads the u64 at the provided byte offset in remainders.
func (bl *rsqfBlock) getU64(b uint) *u64 {
	return (*u64)(unsafe.Pointer(&bl.rems[b%512]))
}

// getBlock returns the ith block, wrapping around the number of blocks.
func (r *rsqfData) getBlock(i uint64) *rsqfBlock {
	offset := uintptr((i & r.blockMask) * r.block)
	return (*rsqfBlock)(unsafe.Pointer(uintptr(r.base) + offset))
}

// getRemainder reads the remainder starting at the ith bit.
func (r *rsqfData) getRemainder(bl *rsqfBlock, i uint) uint64 {
	return bl.getU64(i/8).toUint64() >> (i % 8)
}

// putRemainder stores rem in the ith bit using the mask to select bits.
func (r *rsqfData) putRemainder(bl *rsqfBlock, i uint, rem uint64) {
	o := i % 8                  // compute the unaligned amount
	v := bl.getU64(i / 8)       // get the pointer aligned to a byte
	u := v.toUint64()           // get the value out
	u &^= r.remMask << o        // clear the bits we're going to be setting
	u |= (rem & r.remMask) << o // set the bits from the value
	*v = toU64(u)               // write it back
}

// occupiedRank returns the number of set bits of the occupied bit vector starting at the
// sth bit and stopping at the (s+b)th bit. b is at most 64.
func (r *rsqfData) occupiedRank(s, b uint64) uint {
	idx, off := s/64, s%64

	// we remove off lower order bits and keep at most b higher order bits.
	occ := r.getBlock(idx).occupied.toUint64()
	occ >>= off
	occ <<= 64 - (b % 64)
	rank := uint(bits.OnesCount64(occ))

	// if we overflow a single uint64, then grab the next one.
	if shift := 128 - off - b; shift < 64 {
		occ = r.getBlock(idx + 1).occupied.toUint64()
		occ <<= shift
		rank += uint(bits.OnesCount64(occ))
	}

	return rank
}

// runendsSelect returns the number of bits past s until the bth bit is set.
func (r *rsqfData) runendsSelect(s, b uint64) uint {
	idx, off, acc := s/64, uint(s%64), uint(0)

check:
	run := r.getBlock(idx).runends.toUint64() >> off

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

	return acc + uint(bits.TrailingZeros64(run))
}

// Lookup reports true for any hash that has been inserted and possibly for some
// hashes that have not been inserted. If it ever reports false, then the hash
// has definitely not been inserted.
func (r *rsqfData) Lookup(hash uint64) bool {
	// break out the remainder and quotient.
	rem := hash & r.remMask
	quo := (hash >> (r.remBits % 64)) & r.quoMask

	fmt.Printf("L find hash:%d quo:%d rem:%d\n", hash, quo, rem)

	// grab the block for the provided hash.
	idx, off := quo/64, uint(quo%64)
	bl := r.getBlock(idx)

	// if the quotient isn't occupied, we're done.
	if bl.occupied.toUint64()>>off&1 == 0 {
		fmt.Println("L find unoccupied")
		return false
	}

	// compute where the end of the run containing the quotient would be.
	slot := r.rankSelect(quo)

	// grab the block for the computed index.
	idx, off = slot/64, uint(slot%64)
	bl = r.getBlock(idx)

	sel := uint64(1) << off
	runs := bl.runends.toUint64()

	for {
		// check for remainder matching, remainder never matching, or run ending.
		if slotRem := r.getRemainder(bl, r.remBits*off) & r.remMask; slotRem == rem {
			return true
		} else if slotRem < rem || runs&sel != 0 {
			return false
		}

		// walk back an offset if possible.
		if off > 0 {
			off--
			sel >>= 1
			continue
		}

		// walk back a block.
		idx, off = idx-1, 63
		bl = r.getBlock(idx)
		sel = 1 << 63
		runs = bl.runends.toUint64()
	}
}

// Insert adds the hash to the filter so that Lookup will definitely report
// yes. If insert reports false, then the filter is in a broken state and no
// further operations should be performed on it. This should never happen if
// the hashes are randomly distributed.
func (r *rsqfData) Insert(hash uint64) bool {
	defer func() {
		// runends should always have the same number of total bits as occupied
		occ, rem := 0, 0
		for j := uint64(0); j < 1<<r.quoBits; j++ {
			bl := r.getBlock(j)
			occ += bits.OnesCount64(bl.occupied.toUint64())
			rem += bits.OnesCount64(bl.runends.toUint64())
		}
		if occ != rem {
			panic("mismatch")
		}
	}()

	// break out the remainder and quotient.
	rem := hash & r.remMask
	quo := (hash >> (r.remBits % 64)) & r.quoMask

	fmt.Printf("I set  hash:%d quo:%d rem:%d\n", hash, quo, rem)

	// grab the block for the quotient to begin the search.
	idx, off := quo/64, uint(quo%64)
	bl := r.getBlock(idx)

	// compute where the end of the run for the quotient is.
	slot := r.rankSelect(quo)
	fmt.Printf("I init quo:%d rem:%d idx:%d off:%d slot:%d\n", quo, rem, idx, off, slot)

	// if the slot is before our quotient, or equal to it and it's the first
	// quotient inside of the block, then the quotient is missing and the
	// slot is empty.
	if slot < quo {
		r.putRemainder(bl, r.remBits*off, rem)
		bl.occupied = toU64(bl.occupied.toUint64() | 1<<off)
		bl.runends = toU64(bl.runends.toUint64() | 1<<off)
		return true
	}

	// save the location of the quotient for later.
	qbl, qoff := bl, off

	// the spot we're storing into is the next slot.
	slot++

	// grab the block containing the first unused value past the slot.
	last, numRuns := r.findFirstUnused(slot)
	idx, off = last/64, uint(last%64)
	roff := r.remBits * off
	bl = r.getBlock(idx)
	runs := bl.runends.toUint64()

	fmt.Printf("I find slot:%d last:%d quo:%d runs:%d\n", slot, last, quo, numRuns)
	quo += numRuns // keep track of what quotient the slot is in

	// copy things backwards until we get to slot.
	for last > slot {
		last--

		// figure out what our next state is.
		nbl, nidx, noff, nroff, nruns := bl, idx, off, roff, runs
		if off > 0 {
			noff--
			nroff -= r.remBits
		} else {
			nidx--
			noff = 63
			nroff = r.remBits * 63
			nbl = r.getBlock(nidx)
			nruns = nbl.runends.toUint64()
		}

		// keep track of if a run is ending.
		nends := nruns >> noff & 1

		// unconditionally set the remainder to the next one.
		r.putRemainder(bl, roff, r.getRemainder(nbl, nroff))

		// check if the next runends bit is different, and if so, flip it.
		fmt.Printf("I flip idx:%d off:%d quo:%d ends:%t\n", idx, off, quo, runs>>off&1 != nends)
		if runs>>off&1 != nends {
			runs ^= 1 << off
		}

		// update runends when we switch blocks
		if bl != nbl {
			bl.runends = toU64(runs)
		}

		// if we're on a representitive quotient, increment the offset
		if quo&63 == 0 {
			bl.offset++
		}
		quo -= nends

		// update to the next state.
		bl, idx, off, roff = nbl, nidx, noff, nroff
	}

	// always assign in case the loop didn't end on another block
	bl.runends = toU64(runs)

	// always bump the offset if we're on a represntitive quotient.

	fmt.Printf("I set  slot:%d last:%d quo:%d idx:%d off:%d roff:%d\n", slot, last, quo, idx, off, roff)

	// last == slot now, so we know that bl points into the right block, and
	// idx, off and roff are the appropriate values.
	r.putRemainder(bl, roff, rem)
	bl.runends = toU64(bl.runends.toUint64() | 1<<off)
	if off == 0 {
		bl.offset++
	}

	// if the quotient is unoccupied, flag it as occupied and we're done.
	if qbl.occupied.toUint64()>>qoff&1 == 0 {
		qbl.occupied = toU64(qbl.occupied.toUint64() | 1<<qoff)
		return true
	}

	// otherwise, clear the previous runends bit and bump the offset if
	// our quotient is a representitive quotient.
	if off > 0 {
		fmt.Printf("I del  idx:%d off:%d\n", idx, off-1)
		bl.runends = toU64(bl.runends.toUint64() &^ (1 << (off - 1)))
	} else {
		fmt.Printf("I del  idx:%d off:%d\n", idx-1, 63)
		nbl := r.getBlock(idx - 1)
		nbl.runends = toU64(nbl.runends.toUint64() &^ (1 << 63))
	}
	if quo&63 == 0 {
		bl.offset++
	}

	// if we're at an offset of 255, more adds are unsafe.
	return bl.offset < 255
}

func (r *rsqfData) rankSelect(slot uint64) (uint64, bool) {
	rep := slot &^ 63
	off := uint64(r.getBlock(slot / 64).offset)
	b := slot - rep + 1

	d := uint64(r.occupiedRank(rep+off, b))
	if d == 0 {
		fmt.Printf("R dead kind:rank quo:%d d:%d b:%d\n", slot, d, b)
		return 0, false
	}

	t := uint64(r.runendsSelect(rep+off, d))
	fmt.Printf("R rank kind:sel quo:%d slot:%d d:%d b:%d t:%d off:%d\n", slot, rep+off+t, d, b, t, off)
	return rep + off + t, true
}

// findFirstUnused finds the first unused slot after the quotient.
func (r *rsqfData) findFirstUnused(quo uint64) (uint64, uint64) {
	runs := uint64(0)
	for j := uint64(0); j < r.quoMask; j++ {
		slot, ok := r.rankSelect(quo)
		if quo > slot || !ok {
			return quo, runs
		}
		quo = slot + 1
		runs++
	}

	panic("too many iterations")
}
