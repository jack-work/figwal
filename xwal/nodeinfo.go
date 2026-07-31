package xwal

// NodeInfo is one node of the fork forest. Treat it as immutable: readers may
// hold it while a mutation runs.
//
// Frozen is derived (len(Children) > 0), never stored, so it cannot disagree
// with the forest.
type NodeInfo struct {
	Branch   []string `json:"branch"`          // key is strings.Join(Branch, "/")
	Trunk    string   `json:"trunk,omitempty"` // "" for the root and for stumps
	Parent   string   `json:"parent"`          // "" for the root
	Children []string `json:"children,omitempty"`
	IsRoot   bool     `json:"root,omitempty"`

	// Flat lineage. From is the node forked from; empty only for the root.
	From string `json:"from,omitempty"`
	Kind string `json:"kind,omitempty"` // null | loadout | conversation
}

func (n *NodeInfo) Frozen() bool { return len(n.Children) > 0 }

func (n *NodeInfo) stumpName() string {
	if n.Kind == "loadout" && len(n.Branch) == 1 {
		return n.Branch[0]
	}
	return ""
}
