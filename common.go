package cascade

// uint64 backed by an array that's always little endian

type u64 [8]byte

func toU64(v uint64) u64 {
	var f u64
	f[0] = byte(v)
	f[1] = byte(v >> 8)
	f[2] = byte(v >> 16)
	f[3] = byte(v >> 24)
	f[4] = byte(v >> 32)
	f[5] = byte(v >> 40)
	f[6] = byte(v >> 48)
	f[7] = byte(v >> 56)
	return f
}

func (v u64) toUint64() uint64 {
	return uint64(v[0]) |
		uint64(v[1])<<8 |
		uint64(v[2])<<16 |
		uint64(v[3])<<24 |
		uint64(v[4])<<32 |
		uint64(v[5])<<40 |
		uint64(v[6])<<48 |
		uint64(v[7])<<56
}
