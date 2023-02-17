package consistenthashing

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func MakeHasher(hash int64) StringHasher {
	return func(str string) int64 {
		return hash
	}
}

func TestAddNode(t *testing.T) {
	node1 := NewNode("test1", RollingHash)
	node2 := NewNode("test2", RollingHash)

	for _, scenario := range []struct {
		ring         LocalHashRing
		hasher       StringHasher
		expectedRing LocalHashRing
	}{
		{
			LocalHashRing{
				keyspaceSize: 10,
				keyRanges: []NodeKeyRange{
					{KeyRange{lowerBound: 0, upperBound: 5}, node1},
					{KeyRange{lowerBound: 6, upperBound: 9}, node2},
				},
			},
			MakeHasher(7),
			LocalHashRing{
				keyspaceSize: 10,
				keyRanges: []NodeKeyRange{
					{KeyRange{lowerBound: 0, upperBound: 5}, node1},
					{KeyRange{lowerBound: 6, upperBound: 6}, node2},
					{KeyRange{lowerBound: 7, upperBound: 9}, NewNode("test", MakeHasher(7))},
				},
			},
		},
		{
			LocalHashRing{
				keyspaceSize: 10,
				keyRanges: []NodeKeyRange{
					{KeyRange{lowerBound: 0, upperBound: 5}, node1},
					{KeyRange{lowerBound: 6, upperBound: 9}, node2},
				},
			},
			MakeHasher(4),
			LocalHashRing{
				keyspaceSize: 10,
				keyRanges: []NodeKeyRange{
					{KeyRange{lowerBound: 0, upperBound: 3}, node1},
					{KeyRange{lowerBound: 4, upperBound: 5}, NewNode("test", MakeHasher(4))},
					{KeyRange{lowerBound: 6, upperBound: 9}, node2},
				},
			},
		},
		{
			LocalHashRing{
				keyspaceSize: 20,
				keyRanges: []NodeKeyRange{
					{KeyRange{lowerBound: 0, upperBound: 5}, node1},
					{KeyRange{lowerBound: 6, upperBound: 9}, node2},
					{KeyRange{lowerBound: 10, upperBound: 11}, node1},
					{KeyRange{lowerBound: 12, upperBound: 19}, node2},
				},
			},
			MakeHasher(11),
			LocalHashRing{
				keyspaceSize: 20,
				keyRanges: []NodeKeyRange{
					{KeyRange{lowerBound: 0, upperBound: 5}, node1},
					{KeyRange{lowerBound: 6, upperBound: 9}, node2},
					{KeyRange{lowerBound: 10, upperBound: 10}, node1},
					{KeyRange{lowerBound: 11, upperBound: 11}, NewNode("test", MakeHasher(11))},
					{KeyRange{lowerBound: 12, upperBound: 19}, node2},
				},
			},
		},
	} {
		t.Run(fmt.Sprintf("%d", scenario.hasher("")), func(t *testing.T) {
			ring := scenario.ring
			expectedRing := scenario.expectedRing

			node := NewNode("test", scenario.hasher)
			ring.AddNode(node)

			assert.Equal(t, expectedRing.keyspaceSize, ring.keyspaceSize)
			for i, nodeKeyRange := range expectedRing.keyRanges {
				assert.Equal(t, nodeKeyRange.keyRange.lowerBound, ring.keyRanges[i].keyRange.lowerBound)
				assert.Equal(t, nodeKeyRange.keyRange.upperBound, ring.keyRanges[i].keyRange.upperBound)
				assert.Equal(t, nodeKeyRange.node.GetName(), ring.keyRanges[i].node.GetName())
				assert.Equal(t, nodeKeyRange.node.GetHash(), ring.keyRanges[i].node.GetHash())
			}
		})
	}
}

func TestFindRangeForHash(t *testing.T) {
	for _, scenario := range []struct {
		ring          LocalHashRing
		hash          int64
		expectedIndex int
	}{
		{
			LocalHashRing{
				keyspaceSize: 10,
				keyRanges: []NodeKeyRange{
					{KeyRange{lowerBound: 0, upperBound: 5}, nil},
					{KeyRange{lowerBound: 6, upperBound: 9}, nil},
				},
			},
			int64(9),
			1,
		},
		{
			LocalHashRing{
				keyspaceSize: 10,
				keyRanges: []NodeKeyRange{
					{KeyRange{lowerBound: 0, upperBound: 5}, nil},
					{KeyRange{lowerBound: 6, upperBound: 9}, nil},
				},
			},
			int64(5),
			0,
		},
		{
			LocalHashRing{
				keyspaceSize: 20,
				keyRanges: []NodeKeyRange{
					{KeyRange{lowerBound: 0, upperBound: 9}, nil},
					{KeyRange{lowerBound: 10, upperBound: 12}, nil},
					{KeyRange{lowerBound: 13, upperBound: 19}, nil},
				},
			},
			int64(10),
			1,
		},
		{
			LocalHashRing{
				keyspaceSize: 20,
				keyRanges: []NodeKeyRange{
					{KeyRange{lowerBound: 0, upperBound: 9}, nil},
					{KeyRange{lowerBound: 10, upperBound: 12}, nil},
					{KeyRange{lowerBound: 13, upperBound: 19}, nil},
				},
			},
			int64(12),
			1,
		},
		{
			LocalHashRing{
				keyspaceSize: 20,
				keyRanges: []NodeKeyRange{
					{KeyRange{lowerBound: 0, upperBound: 9}, nil},
					{KeyRange{lowerBound: 10, upperBound: 12}, nil},
					{KeyRange{lowerBound: 13, upperBound: 19}, nil},
				},
			},
			int64(17),
			2,
		},
		{
			LocalHashRing{
				keyspaceSize: 20,
				keyRanges: []NodeKeyRange{
					{KeyRange{lowerBound: 0, upperBound: 9}, nil},
					{KeyRange{lowerBound: 10, upperBound: 12}, nil},
					{KeyRange{lowerBound: 13, upperBound: 19}, nil},
				},
			},
			int64(19),
			2,
		},
		{
			LocalHashRing{
				keyspaceSize: 20,
				keyRanges: []NodeKeyRange{
					{KeyRange{lowerBound: 0, upperBound: 9}, nil},
					{KeyRange{lowerBound: 10, upperBound: 12}, nil},
					{KeyRange{lowerBound: 13, upperBound: 19}, nil},
				},
			},
			int64(9),
			0,
		},
		{
			LocalHashRing{
				keyspaceSize: 10,
				keyRanges: []NodeKeyRange{
					{KeyRange{lowerBound: 0, upperBound: 5}, nil},
					{KeyRange{lowerBound: 6, upperBound: 9}, nil},
				},
			},
			int64(4),
			0,
		},
	} {
		t.Run(fmt.Sprintf("%d %d", scenario.hash, scenario.expectedIndex), func(t *testing.T) {
			index, _ := scenario.ring.FindRangeForHash(scenario.hash)
			assert.Equal(t, scenario.expectedIndex, index)
		})
	}
}
