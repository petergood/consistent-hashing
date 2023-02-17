package consistenthashing

// https://cp-algorithms.com/string/string-hashing.html
const (
	hashingPrime   = 51
	hashingModulus = 1000000000 + 9
)

type StringHasher = func(string) int64

func RollingHash(value string) int64 {
	// polynomial rolling hash (https://cp-algorithms.com/string/string-hashing.html)
	var hash, p int64
	hash = 0
	p = 1

	for _, s := range value {
		hash += (int64(s) * p) % hashingModulus
		p *= hashingPrime
	}

	return hash
}
