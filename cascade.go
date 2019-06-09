package cascade

import (
	"os"

	"github.com/zeebo/errs"
	"github.com/zeebo/mon"
	"golang.org/x/sys/unix"
)

type casFilter struct {
	fh       *os.File
	q, r     uint
	levels   []*quoFil
	mappings [][]byte
}

var New = newCasFil

type Filter = casFilter

func newCasFil(fh *os.File, bits uint) *casFilter {
	// pages are assumed to be 4k. the hash is going to be
	// bits many long. our minimum false positive rate is
	// a remainder of 5 bits. each element has 3 bits of
	// overhead. we have 32768 bits in 4k. we want to find
	// the largest r such that (3+r)*2^q < 32768 with r+q = bits.

	r, vr := uint(0), uint(0)
	for cr := uint(1); cr < bits; cr++ {
		if cv := (3 + cr) * (1 << (bits - cr)); cv < 32768 && cv > vr {
			r, vr = cr, cv
		}
	}

	return &casFilter{
		fh: fh,
		q:  bits - r,
		r:  r,
	}
}

func (c *casFilter) QuotientBits() uint  { return c.q }
func (c *casFilter) RemainderBits() uint { return c.r }

func (c *casFilter) Len() uint {
	o := uint(0)
	for _, qf := range c.levels {
		o += qf.Len()
	}
	return o
}

// newLevel truncates the backing file to be large enough to hold a new level
// and maps the new section into a buffer.
func (c *casFilter) newLevel() (err error) {
	defer mon.Start().Stop(&err)

	// level 0 and level 1 are the same size
	if len(c.levels) > 1 {
		c.q++
		c.r--
	}

	// round the size up to the next page
	pageSize := int64(unix.Getpagesize())
	size := (int64(bufSize(c.q, c.r)) + pageSize - 1) / pageSize * pageSize

	currentSize := int64(0)
	for _, qf := range c.levels {
		currentSize += int64(len(qf.br.buf))
	}

	if err := c.fh.Truncate(currentSize + int64(size)); err != nil {
		return errs.Wrap(err)
	}

	buf, err := unix.Mmap(int(c.fh.Fd()), currentSize, int(size),
		unix.PROT_WRITE|unix.PROT_READ, unix.MAP_SHARED)
	if err != nil {
		return errs.Wrap(err)
	}

	c.mappings = append(c.mappings, buf)

	qf := newQuoFil(c.q, c.r, buf)
	qf.Clear()

	c.levels = append(c.levels, qf)

	return nil
}

// spill takes the non-empty prefix of the levels and inserts them into
// the first empty level, allocating one if necessary.
func (c *casFilter) spill() (err error) {
	defer mon.Start().Stop(&err)

	var prefix []*quoFil
	for i, qf := range c.levels {
		if qf.Empty() {
			break
		}
		prefix = c.levels[:i+1]
	}

	if len(prefix) == len(c.levels) {
		if err := c.newLevel(); err != nil {
			return errs.Wrap(err)
		}
	}

	// TODO(jeff): merge could be faster here by using the fact that
	// iterators return in sorted order. it would be contiguous writes.
	out := c.levels[len(prefix)]
	for _, qf := range prefix {
		for it := qf.Iter(); it.Next(); {
			out.Add(it.Hash())
		}
		qf.Clear()
	}

	if err := c.sync(); err != nil {
		return errs.Wrap(err)
	}

	return nil
}

func (c *casFilter) sync() (err error) {
	return nil

	// defer mon.Start().Stop(&err)

	// // really tank performance. the goal is to simulate a situation
	// // were we don't have any memory available.
	// for _, m := range c.mappings {
	// 	if err := unix.Msync(m, unix.MS_SYNC); err != nil {
	// 		return err
	// 	}
	// 	if err := unix.Madvise(m, unix.MADV_DONTNEED); err != nil {
	// 		return err
	// 	}
	// }

	// // REALLY tank performance. drop all the caches.
	// return exec.Command("sudo", "bash", "-c", "echo 3 > /proc/sys/vm/drop_caches").Run()
}

var addThunk mon.Thunk

func (c *casFilter) Add(hash uint64) (err error) {
	if len(c.levels) == 0 {
		if err := c.newLevel(); err != nil {
			return errs.Wrap(err)
		}
	}

	timer := addThunk.Start()
	c.levels[0].Add(hash)
	if c.levels[0].Len()*4 >= c.levels[0].Cap()*3 {
		err = c.spill()
	}
	timer.Stop(&err)
	return errs.Wrap(err)
}

func (c *casFilter) Lookup(hash uint64) bool {
	for _, qf := range c.levels {
		if !qf.Empty() && qf.Lookup(hash) {
			return true
		}
	}
	return false
}
