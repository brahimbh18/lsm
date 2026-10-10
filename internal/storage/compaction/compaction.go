package compaction

import "errors"

const (
	DefaultL0TargetBytes   uint64 = 64 * 1024 * 1024
	DefaultL0FileTrigger          = 4
	DefaultLevelMultiplier uint64 = 10
)

var (
	ErrInvalidCompactionPolicy = errors.New("invalid compaction policy")
	ErrLevelOverflow           = errors.New("compaction level size overflows uint64")
)

// CompactionPolicy describes when a level should be compacted. L0 uses both
// byte and file thresholds because its files may overlap; higher levels use
// size pressure only.
type CompactionPolicy struct {
	L0TargetBytes   uint64
	L0FileTrigger   int
	LevelMultiplier uint64
}

func DefaultCompactionPolicy() CompactionPolicy {
	return CompactionPolicy{
		L0TargetBytes:   DefaultL0TargetBytes,
		L0FileTrigger:   DefaultL0FileTrigger,
		LevelMultiplier: DefaultLevelMultiplier,
	}
}

func (p CompactionPolicy) validate() error {
	if p.L0TargetBytes == 0 || p.L0FileTrigger <= 0 || p.LevelMultiplier < 2 {
		return ErrInvalidCompactionPolicy
	}
	return nil
}

// TargetBytes returns the byte budget for a level. Level zero is the L0
// trigger budget; each subsequent level is LevelMultiplier times larger.
func (p CompactionPolicy) TargetBytes(level int) (uint64, error) {
	if err := p.validate(); err != nil {
		return 0, err
	}
	if level < 0 {
		return 0, ErrInvalidCompactionPolicy
	}

	target := p.L0TargetBytes
	for current := 0; current < level; current++ {
		if target > ^uint64(0)/p.LevelMultiplier {
			return 0, ErrLevelOverflow
		}
		target *= p.LevelMultiplier
	}
	return target, nil
}

// ShouldCompact reports whether the supplied level is over its compaction
// trigger. L0 compacts on either accumulated bytes or file count; L1 and
// above compact only when their byte budget is exceeded.
func (p CompactionPolicy) ShouldCompact(level int, bytes uint64, files int) (bool, error) {
	target, err := p.TargetBytes(level)
	if err != nil {
		return false, err
	}
	if level == 0 {
		return bytes >= target || files >= p.L0FileTrigger, nil
	}
	return bytes >= target, nil
}
