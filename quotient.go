package cascade

type ( // helpers to keep things straight
	quotient  uint64
	remainder uint64
	index     uint
)

type quoFil struct {
	br   bitReader
	q, r uint  // quotient and remainder bits
	mask index // 1 << q - 1
	len  uint
}

func bufSize(q, r uint) uint { return ((1<<q)*(3+r) + 7) / 8 }

func newQuoFil(q, r uint, buf []byte) *quoFil {
	if buf == nil {
		buf = make([]byte, bufSize(q, r))
	}
	return &quoFil{
		br:   newBitReader(buf, 3+r),
		q:    q,
		r:    r,
		mask: 1<<q - 1,
	}
}

func (q *quoFil) Empty() bool         { return q.len == 0 }
func (q *quoFil) Len() uint           { return q.len }
func (q *quoFil) Cap() uint           { return 1 << q.q }
func (q *quoFil) Bits() uint          { return q.q + q.r }
func (q *quoFil) QuotientBits() uint  { return q.q }
func (q *quoFil) RemainderBits() uint { return q.r }

func (q *quoFil) Clear() {
	q.len = 0
	for i := range q.br.buf {
		q.br.buf[i] = 0
	}
}

func (q *quoFil) getSlot(idx index) slot     { return slot(q.br.Get(uint(idx))) }
func (q *quoFil) setSlot(idx index, sl slot) { q.br.Put(uint(idx), uint64(sl)) }

func (q *quoFil) quotient(hash uint64) quotient   { return quotient(hash >> q.r) }
func (q *quoFil) remainder(hash uint64) remainder { return remainder(hash & (1<<q.r - 1)) }

func (q *quoFil) index(quo quotient) index { return index(quo) & q.mask }
func (q *quoFil) next(idx index) index     { return (idx + 1) & q.mask }
func (q *quoFil) prev(idx index) index     { return (idx - 1) & q.mask }

func (q *quoFil) findRun(idx index) index {
	start := idx
	for q.getSlot(start).Shifted() {
		start = q.prev(start)
	}

	run := start
	for start != idx {
		run = q.next(run)
		for q.getSlot(run).Continuation() {
			run = q.next(run)
		}

		start = q.next(start)
		for !q.getSlot(start).Occupied() {
			start = q.next(start)
		}
	}

	return run
}

func (q *quoFil) insertSlot(idx index, s slot) {
	curr := s
	for {
		prev := q.getSlot(idx)

		empty := prev.Empty()
		if !empty {
			prev = prev.SetShifted()
			if prev.Occupied() {
				curr = curr.SetOccupied()
				prev = prev.ClearOccupied()
			}
		}

		q.setSlot(idx, curr)

		curr = prev
		idx = q.next(idx)

		if empty {
			return
		}
	}
}

func (q *quoFil) Lookup(hash uint64) bool {
	quo := q.quotient(hash)
	rem := q.remainder(hash)
	idx := q.index(quo)

	if !q.getSlot(idx).Occupied() {
		return false
	}

	run := q.findRun(idx)
	slot := q.getSlot(run)

	for {
		if srem := slot.Remainder(); srem == rem {
			return true
		} else if srem > rem {
			return false
		}

		run = q.next(run)
		slot = q.getSlot(run)

		if !slot.Continuation() {
			return false
		}
	}
}

func (q *quoFil) Add(hash uint64) {
	quo := q.quotient(hash)
	rem := q.remainder(hash)
	qidx := q.index(quo)

	qslot := q.getSlot(qidx)
	nslot := newSlot(rem)

	if qslot.Empty() {
		q.setSlot(qidx, nslot.SetOccupied())
		q.len++
		return
	}

	if !qslot.Occupied() {
		q.setSlot(qidx, qslot.SetOccupied())
	}

	run := q.findRun(qidx)
	ridx := run

	if qslot.Occupied() {
		rslot := q.getSlot(ridx)

		for {
			if srem := rslot.Remainder(); srem == rem {
				return
			} else if srem > rem {
				break
			}

			ridx = q.next(ridx)
			rslot = q.getSlot(ridx)

			if !rslot.Continuation() {
				break
			}
		}

		if ridx == run {
			old := q.getSlot(run)
			q.setSlot(run, old.SetContinuation())
		} else {
			nslot = nslot.SetContinuation()
		}
	}

	if ridx != qidx {
		nslot = nslot.SetShifted()
	}
	q.insertSlot(ridx, nslot)
	q.len++
}

//
// iterator
//

type quoFilIter struct {
	q    *quoFil
	idx  index
	quo  quotient
	vis  uint
	hash uint64
}

func (q *quoFil) Iter() (it quoFilIter) {
	it.q = q
	for q.len > 0 && !q.getSlot(it.idx).ClusterStart() {
		it.idx = q.next(it.idx)
	}
	return it
}

func (it *quoFilIter) Next() bool {
	if it.vis >= it.q.len {
		return false
	}
	for {
		s := it.q.getSlot(it.idx)
		if s.ClusterStart() {
			it.quo = quotient(it.idx)
		} else if s.RunStart() {
			quo := it.q.next(it.q.index(it.quo))
			for !it.q.getSlot(quo).Occupied() {
				quo = it.q.next(quo)
			}
			it.quo = quotient(quo)
		}
		it.idx = it.q.next(it.idx)
		if !s.Empty() {
			it.hash = uint64(it.quo)<<it.q.r | uint64(s.Remainder())
			it.vis++
			return true
		}
	}
}

func (it *quoFilIter) Hash() uint64 { return it.hash }
