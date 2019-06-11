package cascade

import (
	"math"
	"testing"

	"github.com/zeebo/assert"
)

func TestRSQFData(t *testing.T) {
	t.Run("Rank", func(t *testing.T) {
		buf := make([]byte, 1024) // way too big
		data := newRSQFData(buf, 1)

		*data.Occupied(0) = toU64(math.MaxUint64)
		*data.Occupied(1) = toU64(math.MaxUint64)

		assert.Equal(t, data.OccupiedRank(0, 1), uint(1))
		assert.Equal(t, data.OccupiedRank(60, 20), uint(20))
	})

	t.Run("Select", func(t *testing.T) {
		buf := make([]byte, 1024) // way too big
		data := newRSQFData(buf, 1)

		*data.Runends(0) = toU64(math.MaxUint64)
		*data.Runends(1) = toU64(math.MaxUint64)
		*data.Runends(2) = toU64(math.MaxUint64)
		*data.Runends(3) = toU64(math.MaxUint64)
		*data.Runends(4) = toU64(math.MaxUint64)
		*data.Runends(5) = toU64(2)

		assert.Equal(t, data.RunendsSelect(0, 0), uint(0))
		for i := uint(0); i < 320; i++ {
			assert.Equal(t, data.RunendsSelect(i, 320-i-1), 320-i-1)
		}
	})
}

func BenchmarkRSQFData(b *testing.B) {
	buf := make([]byte, 1024) // way too big
	data := newRSQFData(buf, 1)

	*data.Occupied(0) = toU64(math.MaxUint64)
	*data.Occupied(1) = toU64(math.MaxUint64)

	*data.Runends(0) = toU64(math.MaxUint64)
	*data.Runends(1) = toU64(math.MaxUint64)
	*data.Runends(2) = toU64(math.MaxUint64)
	*data.Runends(3) = toU64(math.MaxUint64)
	*data.Runends(4) = toU64(math.MaxUint64)
	*data.Runends(5) = toU64(2)

	b.Run("Rank", func(b *testing.B) {
		b.Run("Easy", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				data.OccupiedRank(0, 1)
			}
		})

		b.Run("Hard", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				data.OccupiedRank(60, 20)
			}
		})
	})

	b.Run("Select", func(b *testing.B) {
		b.Run("Easy", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				data.RunendsSelect(0, 0)
			}
		})

		b.Run("Hard", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				data.RunendsSelect(256, 63)
			}
		})
	})
}
