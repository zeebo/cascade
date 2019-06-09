package cascade

import (
	"testing"

	"github.com/zeebo/assert"
	"github.com/zeebo/pcg"
)

func TestQuotient(t *testing.T) {
	t.Run("Basic", func(t *testing.T) {
		q := newQuoFil(10, 5, nil)
		var e []uint64

		for i := 0; i < 500; i++ {
			x := pcg.Uint64()
			e = append(e, x)
			q.Add(x)
		}

		for _, v := range e {
			assert.That(t, q.Lookup(v))
		}
	})

	t.Run("False Positive", func(t *testing.T) {
		q := newQuoFil(10, 5, nil)

		for i := 0; i < 750; i++ {
			q.Add(pcg.Uint64())
		}

		got := 0
		for i := 0; i < 10000; i++ {
			if q.Lookup(pcg.Uint64()) {
				got++
			}
		}

		assert.That(t, got < 300)
	})

	t.Run("Bug 0", func(t *testing.T) {
		q := newQuoFil(5, 3, nil)
		q.Add(0x12)
		q.Add(0x14)
		q.Add(0x17)
		q.Add(0x26) // shows up as 46
		q.Add(0x40)

		for it := q.Iter(); it.Next(); {
			if it.Hash() == 0x46 {
				t.Fatal("got the wrong value")
			}
		}
	})

	t.Run("Iterator", func(t *testing.T) {
		q := newQuoFil(10, 5, nil)
		e := make(map[uint64]bool)

		for i := 0; i < 500; i++ {
			for {
				x := pcg.Uint64() & (1<<15 - 1)
				if e[x] {
					continue
				}

				e[x] = true
				q.Add(x)
				break
			}
		}

		for it := q.Iter(); it.Next(); {
			h := it.Hash()
			assert.That(t, e[h])
			delete(e, h)
		}

		assert.Equal(t, len(e), 0)
	})
}

func BenchmarkQuotient(b *testing.B) {
	b.Run("Add", func(b *testing.B) {
		q := newQuoFil(11, 5, nil)
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			q.Add(pcg.Uint64())

			// this causes a small amount of allocs, but whatever
			if (i+1)%1024 == 0 {
				q = newQuoFil(11, 5, nil)
			}
		}
	})

	b.Run("Lookup Full", func(b *testing.B) {
		q := newQuoFil(10, 5, nil)
		for i := 0; i < 750; i++ {
			q.Add(pcg.Uint64())
		}
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			q.Lookup(pcg.Uint64())
		}
	})
}
