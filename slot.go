package cascade

type slot uint64

func newSlot(rem remainder) slot { return slot(rem << 3) }
func (s slot) Empty() bool       { return s&7 == 0 }

func (s slot) Remainder() remainder            { return remainder(s >> 3) }
func (s slot) SetRemainder(rem remainder) slot { return newSlot(rem) | s&7 }

func (s slot) Occupied() bool      { return s&1 != 0 }
func (s slot) SetOccupied() slot   { return s | 1 }
func (s slot) ClearOccupied() slot { return s &^ 1 }

func (s slot) Continuation() bool      { return s&2 != 0 }
func (s slot) SetContinuation() slot   { return s | 2 }
func (s slot) ClearContinuation() slot { return s &^ 2 }

func (s slot) Shifted() bool      { return s&4 != 0 }
func (s slot) SetShifted() slot   { return s | 4 }
func (s slot) ClearShifted() slot { return s &^ 4 }

func (s slot) ClusterStart() bool { return s.Occupied() && !s.Continuation() && !s.Shifted() }
func (s slot) RunStart() bool     { return !s.Continuation() && (s.Occupied() || s.Shifted()) }
