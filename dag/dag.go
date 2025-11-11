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
	Nodes map[string][]t.Event
	Edges []Edge
}

func BuildDAG(trace t.Trace) *DAG {
	nodes := make(map[string][]t.Event)
	var edges []Edge

	for _, e := range trace {
		nodes[e.Process] = append(nodes[e.Process], e)
	}

	for _, procEvents := range nodes {
		for k := 0; k < len(procEvents)-1; k++ {
			edges = append(edges, Edge{
				From: procEvents[k],
				To:   procEvents[k+1],
			})
		}
	}

	// ---
	// 3. Add all INTER-PROCESS edges (Causal Order)
	// This is the O(n^3) check for immediate causal dependencies
	// between different processes.
	// ---
	for i, a := range trace {
		for j, b := range trace {
			if i == j {
				continue
			}

			// We ONLY check for inter-process edges here.
			// The intra-process ones are already handled.
			if a.Process != b.Process {

				// Check if a -> b
				if a.VClock.HappensBefore(b.VClock) {

					// Now, check if this edge (a, b) is *immediate*.
					// It is NOT immediate if there exists any other event 'c'
					// such that a -> c -> b.
					isImmediate := true
					for k, c := range trace {
						if k == i || k == j {
							continue // Don't check 'a' or 'b' as 'c'
						}

						// Check for the transitive path a -> c -> b
						if a.VClock.HappensBefore(c.VClock) && c.VClock.HappensBefore(b.VClock) {
							isImmediate = false
							break // Found an intermediate event
						}
					}

					if isImmediate {
						edges = append(edges, Edge{From: a, To: b})
					}
				}
			}
		}
	}

	return &DAG{
		Nodes: nodes,
		Edges: edges,
	}
}

// Graphviz exporter (no change needed)
func (d *DAG) ToGraphviz() string {
	out := "digraph G {\n"
	for _, e := range d.Edges {
		out += fmt.Sprintf(" \"%s\" -> \"%s\";\n", e.From.VClock, e.To.VClock)
	}
	out += "}\n"
	return out
}
