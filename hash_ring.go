package consistenthashing

type HashRing interface {
	AddNode(node Node)
	RemoveNode(node Node)
	MarkAsFailed(node Node)
}

type HashRingKey = int64

type KeyRange struct {
	lowerBound HashRingKey
	upperBound HashRingKey
}

type NodeKeyRange struct {
	keyRange KeyRange
	node     Node
}

type LocalHashRing struct {
	keyspaceSize int64

	// nodeKeyRanges map[Node]KeyRange
	keyRanges []NodeKeyRange
}

func NewLocalHashRing(keyspaceSize int64) *LocalHashRing {
	return &LocalHashRing{
		keyspaceSize: keyspaceSize,
	}
}

func (ring *LocalHashRing) AddNode(node Node) {
	hash := node.GetHash()
	index, _ := ring.FindRangeForHash(hash)

	var upperBound int64
	if index < len(ring.keyRanges)-1 {
		upperBound = ring.keyRanges[index+1].keyRange.lowerBound - 1
	} else {
		upperBound = ring.keyspaceSize - 1
	}

	lowerBound := hash % ring.keyspaceSize

	newRange := NodeKeyRange{
		node: node,
		keyRange: KeyRange{
			lowerBound: lowerBound,
			upperBound: upperBound,
		},
	}

	ring.keyRanges[index].keyRange.upperBound = lowerBound - 1

	if index == len(ring.keyRanges)-1 {
		ring.keyRanges = append(ring.keyRanges, newRange)
	} else {
		ring.keyRanges = append(ring.keyRanges[:index+1], ring.keyRanges[index:]...)
		ring.keyRanges[index+1] = newRange
	}
}

func (*LocalHashRing) RemoveNode(node Node) {

}

func (*LocalHashRing) MarkAsFailed(node Node) {

}

func (ring *LocalHashRing) FindRangeForHash(hash int64) (int, NodeKeyRange) {
	h := hash % ring.keyspaceSize
	k := ring.keyRanges
	l := 0
	r := len(k)

	for l < r {
		mid := (l + r) / 2

		if h >= k[mid].keyRange.lowerBound && h <= k[mid].keyRange.upperBound {
			return mid, k[mid]
		}

		if h < k[mid].keyRange.lowerBound {
			r = mid
		} else {
			l = mid + 1
		}
	}

	return l, k[l]
}
