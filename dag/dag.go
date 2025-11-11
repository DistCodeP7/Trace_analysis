package dag

import (
	"fmt"

	t "github.com/traces/types"
)

type Edge struct {
	From t.Event
	To   t.Event
}

type DAG struct {
	Nodes map[string][]t.Event // per process
	Edges []Edge               // all causal edges (according to happens-before)
}

func BuildDAG(trace t.Trace) *DAG {
	nodes := make(map[string][]t.Event)
	var edges []Edge

	for _, e := range trace {
		nodes[e.Process] = append(nodes[e.Process], e)
	}

	for i, a := range trace {
		for j := i + 1; j < len(trace); j++ {
			b := trace[j]
			if a.VClock.HappensBefore(b.VClock) {
				edges = append(edges, Edge{
					From: a,
					To:   b,
				})
			}
		}
	}

	return &DAG{
		Nodes: nodes,
		Edges: edges,
	}
}

// Export to Graphviz for visualization
func (d *DAG) ToGraphviz() string {
	out := "digraph G {\n"
	for _, e := range d.Edges {
		out += fmt.Sprintf("  \"%s:%d\" -> \"%s:%d\";\n",
			e.From.Process, e.From.MessageID,
			e.To.Process, e.To.MessageID)
	}
	out += "}\n"
	return out
}
