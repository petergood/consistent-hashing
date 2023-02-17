package consistenthashing

type Node interface {
	GetName() string
	GetHash() int64
}

type KeyNode struct {
	name   string
	hasher StringHasher
}

func NewNode(name string, hasher StringHasher) *KeyNode {
	return &KeyNode{
		name:   name,
		hasher: hasher,
	}
}

func (n *KeyNode) GetName() string {
	return n.name
}

func (n *KeyNode) GetHash() int64 {
	return n.hasher(n.name)
}
