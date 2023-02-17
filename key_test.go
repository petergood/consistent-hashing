package consistenthashing

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReturnKeysHash(t *testing.T) {
	key := NewKey("key1")
	assert.Equal(t, int64(6819878), key.Hash())
}
