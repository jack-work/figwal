package xwal

// NodeInfo is one node of the fork forest. Treat it as immutable: readers may
// hold it while a mutation runs.
type NodeInfo struct {
	Branch []string `json:"branch"`          // key is strings.Join(Branch, "/")
	Trunk  string   `json:"trunk,omitempty"` // "" for the root and for stumps
	IsRoot bool     `json:"root,omitempty"`

	// Flat lineage. From is the node forked from; empty only for the root.
	From string `json:"from,omitempty"`
	Kind string `json:"kind,omitempty"` // null | loadout | conversation
}

func (n *NodeInfo) stumpName() string {
	if n.Kind == "loadout" && len(n.Branch) == 1 {
		return n.Branch[0]
	}
	return ""
}
