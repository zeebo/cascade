package cascade

import (
	"testing"

	"github.com/zeebo/assert"
	"github.com/zeebo/pcg"
)

func TestBits(t *testing.T) {
	t.Run("Basic", func(t *testing.T) {
		br := newBitReader(make([]byte, 2), 5)

		br.Put(0, 1)
		br.Put(1, 2)
		br.Put(2, 3)

		assert.Equal(t, br.Get(0), uint32(1))
		assert.Equal(t, br.Get(1), uint32(2))
		assert.Equal(t, br.Get(2), uint32(3))
	})

	t.Run("Fuzz", func(t *testing.T) {
		for bits := uint(1); bits <= 64-8; bits++ {
			exp := make([]uint64, 10)
			br := newBitReader(make([]byte, (bits*10+7)/8), bits)
			check := func() {
				t.Helper()
				for i := uint(0); i < 10; i++ {
					assert.Equal(t, exp[i], br.Get(i))
				}
			}

			for j := 0; j < 100; j++ {
				i, v := uint(pcg.Uint32n(10)), pcg.Uint64()&(1<<bits-1)
				br.Put(i, v)
				exp[i] = v
				check()
			}
		}
	})
}

func BenchmarkBits(b *testing.B) {
	b.Run("Get", func(b *testing.B) {
		br := newBitReader(make([]byte, 4096), 11)
		for i := 0; i < b.N; i++ {
			br.Get(uint(pcg.Uint32n(4096 * 8 / 11)))
		}
	})

	b.Run("Put", func(b *testing.B) {
		br := newBitReader(make([]byte, 4096), 11)
		for i := 0; i < b.N; i++ {
			br.Put(uint(pcg.Uint32n(4096*8/11)), 0)
		}
	})
}
