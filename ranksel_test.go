package cascade

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/bits"
	"strings"
	"testing"
	"time"

	"github.com/zeebo/assert"
	"github.com/zeebo/pcg"
)

func hexBlock(b *rsqfBlock) {
	fmt.Printf("---\noff: %x\nocc: %x\nrun: %x\nrem: %x\n",
		b.offset, b.occupied.toUint64(), b.runends.toUint64(),
		binary.LittleEndian.Uint64(b.rems[:8]))
}

func binBlock(b *rsqfBlock) {
	fmt.Printf("---\noff: %08b\nocc: %064b\nrun: %064b\nrem: %064b\n",
		b.offset, b.occupied.toUint64(), b.runends.toUint64(),
		binary.LittleEndian.Uint64(b.rems[:8]))
}

func binBlock2(b, c *rsqfBlock) {
	fmt.Printf("occ: %064b %064b\nrun: %064b %064b\nrem: %064b %064b\noff: %-3d %-3d\n",
		bits.Reverse64(b.occupied.toUint64()), bits.Reverse64(c.occupied.toUint64()),
		bits.Reverse64(b.runends.toUint64()), bits.Reverse64(c.runends.toUint64()),
		bits.Reverse64(binary.LittleEndian.Uint64(b.rems[:8])),
		bits.Reverse64(binary.LittleEndian.Uint64(c.rems[:8])),
		b.offset, c.offset)
}

func TestRSQFData(t *testing.T) {
	t.Run("Rank", func(t *testing.T) {
		buf := make([]byte, 1024) // way too big
		data := newRSQFData(&buf[0], 7, 1)

		data.getBlock(0).occupied = toU64(math.MaxUint64)
		data.getBlock(1).occupied = toU64(math.MaxUint64)

		assert.Equal(t, data.occupiedRank(0, 1), uint(1))
		assert.Equal(t, data.occupiedRank(60, 20), uint(20))

		data.getBlock(2).occupied = toU64(0x5555555555555555)
		assert.Equal(t, data.occupiedRank(64*2+1, 15), uint(7))
	})

	t.Run("Select", func(t *testing.T) {
		buf := make([]byte, 1024) // way too big
		data := newRSQFData(&buf[0], 1, 1)

		data.getBlock(0).runends = toU64(math.MaxUint64)
		data.getBlock(1).runends = toU64(math.MaxUint64)
		data.getBlock(2).runends = toU64(math.MaxUint64)
		data.getBlock(3).runends = toU64(math.MaxUint64)
		data.getBlock(4).runends = toU64(math.MaxUint64)
		data.getBlock(5).runends = toU64(2)

		assert.Equal(t, data.runendsSelect(0, 0), uint(0))
		for i := uint64(0); i < 320; i++ {
			assert.Equal(t, data.runendsSelect(i, 320-i), 320-i-1)
		}
	})

	t.Run("Insert", func(t *testing.T) {
		buf := make([]byte, 2*(17+8)+7)
		data := newRSQFData(&buf[0], 7, 1)

		// insert rem 0 into every even quotient
		for i := uint64(0); i < 128; i += 2 {
			assert.That(t, data.Insert(i<<1))
		}

		// insert into random even slots to fill it up
		for i := 0; i < 64; i++ {
			quo := pcg.Uint64() % 64 * 2
			assert.That(t, data.Insert(quo<<1))
		}

		// look up every value. odd with a fingerprint of 1 and
		// even with a fingerprint of 0 so that we have no false
		// positives.
		for i := uint64(0); i < 128; i++ {
			assert.That(t, data.Lookup(i) == (i&3 == 0))
		}
	})

	t.Run("Dense", func(t *testing.T) {
		rng := pcg.New(uint64(time.Now().UnixNano()))
		buf := make([]byte, 2*(17+8)+7)
		data := newRSQFData(&buf[0], 7, 1)

		defer func() {
			fmt.Println("---")
			binBlock2(data.getBlock(0), data.getBlock(1))
		}()

		// fill almost all of it
		for i := 0; i < 120; i++ {
			quo := rng.Uint64() % 128
			fmt.Println("===")
			fmt.Printf("%sv\n", strings.Repeat(" ", int(5+quo+quo/64)))
			binBlock2(data.getBlock(0), data.getBlock(1))
			assert.That(t, data.Insert(quo<<1|1))
			assert.That(t, data.Lookup(quo<<1|1))
			fmt.Println("---")
			fmt.Printf("%sv\n", strings.Repeat(" ", int(5+quo+quo/64)))
			binBlock2(data.getBlock(0), data.getBlock(1))
		}
	})

	t.Run("Offsets", func(t *testing.T) {
		buf := make([]byte, 2*(17+8)+7)
		data := newRSQFData(&buf[0], 7, 1)

		data.Insert(0 << 1)
		binBlock2(data.getBlock(0), data.getBlock(1))

		data.Insert(0 << 1)
		binBlock2(data.getBlock(0), data.getBlock(1))

		data.Insert(0 << 1)
		binBlock2(data.getBlock(0), data.getBlock(1))

		data.Insert(127 << 1)
		binBlock2(data.getBlock(0), data.getBlock(1))

		data.Insert(127 << 1)
		binBlock2(data.getBlock(0), data.getBlock(1))

		data.Insert(127 << 1)
		binBlock2(data.getBlock(0), data.getBlock(1))

		data.Insert(63 << 1)
		binBlock2(data.getBlock(0), data.getBlock(1))

		data.Insert(63 << 1)
		binBlock2(data.getBlock(0), data.getBlock(1))

		data.Insert(63 << 1)
		binBlock2(data.getBlock(0), data.getBlock(1))

		data.Insert(64 << 1)
		binBlock2(data.getBlock(0), data.getBlock(1))

		assert.Equal(t, data.getBlock(0).offset, uint8(5))
		assert.Equal(t, data.getBlock(1).offset, uint8(3))
	})
}

func BenchmarkRSQFData(b *testing.B) {
	buf := make([]byte, 1024) // way too big
	data := newRSQFData(&buf[0], 1, 1)

	data.getBlock(0).occupied = toU64(math.MaxUint64)
	data.getBlock(1).occupied = toU64(math.MaxUint64)

	data.getBlock(0).runends = toU64(math.MaxUint64)
	data.getBlock(1).runends = toU64(math.MaxUint64)
	data.getBlock(2).runends = toU64(math.MaxUint64)
	data.getBlock(3).runends = toU64(math.MaxUint64)
	data.getBlock(4).runends = toU64(math.MaxUint64)
	data.getBlock(5).runends = toU64(2)

	b.Run("Rank", func(b *testing.B) {
		b.Run("Easy", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				data.occupiedRank(0, 1)
			}
		})

		b.Run("Hard", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				data.occupiedRank(60, 20)
			}
		})
	})

	b.Run("Select", func(b *testing.B) {
		b.Run("Easy", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				data.runendsSelect(0, 0)
			}
		})

		b.Run("Hard", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				data.runendsSelect(256, 63)
			}
		})
	})

	b.Run("Insert", func(b *testing.B) {
		b.Run("50", func(b *testing.B) {
			buf := make([]byte, 4096)
			data := newRSQFData(&buf[0], 10, 1)

			for i := 0; i < b.N; i++ {
				// fill it half full
				for j := 0; j < 512; j++ {
					data.Insert(pcg.Uint64())
				}

				b.StopTimer()
				for i := range buf {
					buf[i] = 0
				}
				b.StartTimer()
			}
		})

		b.Run("95", func(b *testing.B) {
			buf := make([]byte, 4096)
			data := newRSQFData(&buf[0], 10, 1)

			for i := 0; i < b.N; i++ {
				// fill it 95%
				for j := 0; j < 973; j++ {
					data.Insert(pcg.Uint64())
				}

				// reset the state
				for i := range buf {
					buf[i] = 0
				}
			}
		})
	})
}
