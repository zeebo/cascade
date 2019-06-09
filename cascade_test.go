package cascade

import (
	"os"
	"testing"

	"github.com/zeebo/assert"
	"github.com/zeebo/pcg"
)

func TestCascade(t *testing.T) {
	t.Run("Basic", func(t *testing.T) {
		fh, err := os.Create("filter")
		assert.NoError(t, err)
		// defer os.Remove(fh.Name())
		defer fh.Close()

		cf := newCasFil(fh, 40)
		for i := 0; i < 1000000*3000; i++ {
			cf.Add(pcg.Uint64())
		}
	})
}
