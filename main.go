package main

import (
	"fmt"
	"math/rand"
	"os"
	"strings"
	"time"
)

// ---------- Event & Clock Structures ----------

type EventType int

const (
	EventSend EventType = iota
	EventReceive
)

type VectorClock map[string]int

func NewVectorClock(processes []string) VectorClock {
	vc := make(VectorClock)
	for _, p := range processes {
		vc[p] = 0
	}
	return vc
}

func (vc VectorClock) Copy() VectorClock {
	cp := make(VectorClock)
	for k, v := range vc {
		cp[k] = v
	}
	return cp
}

func (vc VectorClock) Merge(other VectorClock) {
	for k, v := range other {
		if v > vc[k] {
			vc[k] = v
		}
	}
}

func (vc VectorClock) LessOrEqual(other VectorClock) bool {
	for k, v := range vc {
		if v > other[k] {
			return false
		}
	}
	return true
}

type Event struct {
	ID        int
	Type      EventType
	Process   string
	MessageID int
	VClock    VectorClock
}

type Trace []*Event

// ---------- Causal Graph ----------

type CausalGraph struct {
	Events map[int]*Event
	Edges  map[int][]int // parent -> children
}

func NewCausalGraph() *CausalGraph {
	return &CausalGraph{
		Events: make(map[int]*Event),
		Edges:  make(map[int][]int),
	}
}

func BuildCausalGraph(trace Trace) *CausalGraph {
	g := NewCausalGraph()
	for _, e := range trace {
		g.Events[e.ID] = e
		g.Edges[e.ID] = []int{}
	}

	// Add edges according to vector clocks
	for i, e1 := range trace {
		for j := i + 1; j < len(trace); j++ {
			e2 := trace[j]
			if e1.VClock.LessOrEqual(e2.VClock) && !e2.VClock.LessOrEqual(e1.VClock) {
				g.Edges[e1.ID] = append(g.Edges[e1.ID], e2.ID)
			}
		}
	}

	return g
}

// ---------- Transitive Reduction ----------

func (g *CausalGraph) ReduceTransitive() {
	topo := g.topoSort()
	reachable := make(map[int]map[int]struct{})

	// Iterate in reverse topological order
	for i := len(topo) - 1; i >= 0; i-- {
		u := topo[i]
		if _, ok := reachable[u]; !ok {
			reachable[u] = make(map[int]struct{})
		}

		newEdges := []int{}
		for _, v := range g.Edges[u] {
			// If v already reachable via another child, skip edge
			if _, exists := reachable[u][v]; exists {
				continue
			}
			newEdges = append(newEdges, v)

			// Merge reachability of child into parent
			for r := range reachable[v] {
				reachable[u][r] = struct{}{}
			}
			reachable[u][v] = struct{}{}
		}
		g.Edges[u] = newEdges
	}
}

// ---------- Topological Sort ----------

func (g *CausalGraph) topoSort() []int {
	indeg := make(map[int]int)
	for _, es := range g.Edges {
		for _, v := range es {
			indeg[v]++
		}
	}

	var q []int
	for id := range g.Events {
		if indeg[id] == 0 {
			q = append(q, id)
		}
	}

	var order []int
	for len(q) > 0 {
		u := q[0]
		q = q[1:]
		order = append(order, u)
		for _, v := range g.Edges[u] {
			indeg[v]--
			if indeg[v] == 0 {
				q = append(q, v)
			}
		}
	}
	return order
}

// ---------- Trace Generation ----------

func GenerateAsyncTrace(processes []string, numEvents int) Trace {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	trace := make(Trace, 0, numEvents)
	processClocks := make(map[string]VectorClock)
	pendingMessages := make(map[string][]*Event)
	messageCounter := 0

	for _, p := range processes {
		processClocks[p] = NewVectorClock(processes)
		pendingMessages[p] = []*Event{}
	}

	for len(trace) < numEvents {
		process, action := getRandomProcessAction(processes, r, pendingMessages)

		switch action {
		case EventSend:
			receiver := getRandomOtherProcess(r, processes, process)
			senderClock := processClocks[process]
			senderClock[process]++

			sendEvent := &Event{
				ID:        len(trace),
				Type:      EventSend,
				Process:   process,
				MessageID: messageCounter,
				VClock:    senderClock.Copy(),
			}

			trace = append(trace, sendEvent)
			pendingMessages[receiver] = append(pendingMessages[receiver], sendEvent)
			messageCounter++

		case EventReceive:
			msgIdx := r.Intn(len(pendingMessages[process]))
			msg := pendingMessages[process][msgIdx]
			pendingMessages[process] = append(
				pendingMessages[process][:msgIdx],
				pendingMessages[process][msgIdx+1:]...,
			)

			receiverClock := processClocks[process]
			receiverClock[process]++
			receiverClock.Merge(msg.VClock)

			recvEvent := &Event{
				ID:        len(trace),
				Type:      EventReceive,
				Process:   process,
				MessageID: msg.MessageID,
				VClock:    receiverClock.Copy(),
			}

			trace = append(trace, recvEvent)
		}
	}
	return trace
}

func getRandomProcessAction(processes []string, r *rand.Rand, pending map[string][]*Event) (string, EventType) {
	p := processes[r.Intn(len(processes))]
	canReceive := len(pending[p]) > 0
	action := EventSend
	if canReceive && r.Intn(2) == 0 {
		action = EventReceive
	}
	return p, action
}

func getRandomOtherProcess(r *rand.Rand, processes []string, exclude string) string {
	for {
		p := processes[r.Intn(len(processes))]
		if p != exclude {
			return p
		}
	}
}

// ---------- Display ----------

func (trace Trace) Print() {
	fmt.Println("Trace:")
	for _, e := range trace {
		t := "SEND"
		if e.Type == EventReceive {
			t = "RECV"
		}
		fmt.Printf("Event %02d | %-5s | P:%s | Msg:%d | VC:%v\n",
			e.ID, t, e.Process, e.MessageID, e.VClock)
	}
}

func (g *CausalGraph) Print() {
	fmt.Println("\nReduced Causal DAG (direct happens-before edges only):")
	for src, dsts := range g.Edges {
		fmt.Printf("%02d -> %v\n", src, dsts)
	}
}
func (g *CausalGraph) WriteDOT(filename string) error {
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	fmt.Fprintln(f, "digraph G {")
	// Nodes with labels like id:type:process
	for id, e := range g.Events {
		typ := "S"
		if e.Type == EventReceive {
			typ = "R"
		}
		label := fmt.Sprintf("%d:%s:%s", id, typ, e.Process)
		fmt.Fprintf(f, "  %d [label=%q];\n", id, label)
	}
	// Edges
	for u, dsts := range g.Edges {
		for _, v := range dsts {
			fmt.Fprintf(f, "  %d -> %d;\n", u, v)
		}
	}
	fmt.Fprintln(f, "}")
	return nil
}

func main() {
	var (
		nEvents    = 200
		procsFlag  = "A,B,C,D"
		printTrace = false
	)

	processes := strings.Split(procsFlag, ",")
	for i := range processes {
		processes[i] = strings.TrimSpace(processes[i])
	}

	fmt.Printf("Generating trace: processes=%v events=%d\n", processes, nEvents)

	startGen := time.Now()
	trace := GenerateAsyncTrace(processes, nEvents)
	genDur := time.Since(startGen)

	if printTrace {
		trace.Print()
	} else {
		fmt.Printf("Trace generated: events=%d (generation time: %s)\n", len(trace), genDur)
	}

	startBuild := time.Now()
	graph := BuildCausalGraph(trace)
	graph.ReduceTransitive()
	buildDur := time.Since(startBuild)
	graph.WriteDOT("dot")

	// Compute edge statistics
	totalEdges := 0
	maxOut := 0
	var sumOut int64 = 0
	for _, dsts := range graph.Edges {
		c := len(dsts)
		totalEdges += c
		sumOut += int64(c)
		if c > maxOut {
			maxOut = c
		}
	}
	avgOut := float64(0)
	if len(graph.Events) > 0 {
		avgOut = float64(sumOut) / float64(len(graph.Events))
	}

	// Longest path (critical path) via DP on topological order
	topo := graph.topoSort()
	dp := make(map[int]int, len(graph.Events))
	longest := 0
	for _, u := range topo {
		// dp[u] is already the longest path length to u
		for _, v := range graph.Edges[u] {
			if dp[v] < dp[u]+1 {
				dp[v] = dp[u] + 1
				if dp[v] > longest {
					longest = dp[v]
				}
			}
		}
	}

	fmt.Println("Causal graph built (with transitive reduction).")
	fmt.Printf("  nodes: %d\n", len(graph.Events))
	fmt.Printf("  direct edges: %d\n", totalEdges)
	fmt.Printf("  avg out-degree: %.3f\n", avgOut)
	fmt.Printf("  max out-degree: %d\n", maxOut)
	fmt.Printf("  longest path length: %d\n", longest)
	fmt.Printf("  build time (incl reduction): %s\n", buildDur)
}
