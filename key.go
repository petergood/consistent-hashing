package consistenthashing

type Key interface {
	Hash() int64
}

type StringKey struct {
	keyValue string
}

func NewKey(value string) *StringKey {
	return &StringKey{
		keyValue: value,
	}
}

func (k *StringKey) Hash() int64 {
	return RollingHash(k.keyValue)
}
