package main

import (
	"fmt"
	"maps"
	"math/rand"
	"os"
	"slices"
	"sort"
	"strings"
	"time"
)

// ---------- types ----------

type EventType int

const (
	EventSend EventType = iota
	EventReceive
)

func (et EventType) String() string {
	switch et {
	case EventSend:
		return "SEND"
	case EventReceive:
		return "RECV"
	default:
		return "UNKNOWN"
	}
}

type VectorClock map[string]int

func NewVectorClock(processes []string) VectorClock {
	vc := make(VectorClock)
	for _, key := range processes {
		vc[key] = 0
	}
	return vc
}

func DeepCopy(vc VectorClock) VectorClock {
	newVC := make(VectorClock, len(vc))
	maps.Copy(newVC, vc)
	return newVC
}

func (vc VectorClock) String() string {
	keys := make([]string, 0, len(vc))
	for k := range vc {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s:%d", k, vc[k]))
	}
	return strings.Join(parts, ",")
}

func (vc VectorClock) Merge(other VectorClock) {
	for k, v := range other {
		if v > vc[k] {
			vc[k] = v
		}
	}
}

func (vc VectorClock) HappensBefore(other VectorClock) bool {
	lessOrEqual := true
	strictlyLess := false

	// union of all process keys
	keys := map[string]struct{}{}
	for k := range vc {
		keys[k] = struct{}{}
	}
	for k := range other {
		keys[k] = struct{}{}
	}

	for p := range keys {
		v1 := vc[p]
		v2 := other[p]

		if v1 > v2 {
			lessOrEqual = false
			break
		}
		if v1 < v2 {
			strictlyLess = true
		}
	}

	return lessOrEqual && strictlyLess
}

func sumVC(vc VectorClock) int {
	sum := 0
	for _, v := range vc {
		sum += v
	}
	return sum
}

type Event struct {
	Type    EventType
	Process string
	VClock  VectorClock
}

type Trace []Event

func (t Trace) String() string {
	var result string
	for i, e := range t {
		result += fmt.Sprintf("e-%02d: %-4s on %s, VClock: %s\n",
			i, e.Type.String(), e.Process, e.VClock.String())
	}
	return result
}

// ---------- causal graph ----------

type CausalGraph struct {
	Events []Event
	Edges  map[int][]int
	Root   []int
}

// Assumes it recives a trace that has been sorted by vector clock

func NewCausalGraph(trace Trace) *CausalGraph {
	numEvents := len(trace)
	g := &CausalGraph{
		Events: make([]Event, numEvents),
		Edges:  make(map[int][]int),
		Root:   []int{},
	}
	copy(g.Events, trace)

	inDegree := make([]int, numEvents)

	for i := range trace {
		g.Edges[i] = []int{}
	}

	for i := range trace {
		for j := i + 1; j < len(trace); j++ {
			if trace[i].VClock.HappensBefore(trace[j].VClock) {
				g.Edges[i] = append(g.Edges[i], j)
				inDegree[j]++
			} else if trace[j].VClock.HappensBefore(trace[i].VClock) {
				g.Edges[j] = append(g.Edges[j], i)
				inDegree[i]++
			}
		}
	}

	for i := range numEvents {
		if inDegree[i] == 0 {
			g.Root = append(g.Root, i)
		}
	}

	return g
}

func (g *CausalGraph) ReduceTransitive() {
	topo := g.topoSort()
	reachable := make(map[int]map[int]struct{})

	for i := len(topo) - 1; i >= 0; i-- {
		u := topo[i]
		if _, ok := reachable[u]; !ok {
			reachable[u] = make(map[int]struct{})
		}

		newEdges := []int{}
		for _, v := range g.Edges[u] {
			if _, exists := reachable[u][v]; exists {
				continue
			}
			newEdges = append(newEdges, v)
			for r := range reachable[v] {
				reachable[u][r] = struct{}{}
			}
			reachable[u][v] = struct{}{}
		}
		g.Edges[u] = newEdges
	}
}

func (g *CausalGraph) topoSort() []int {
	indeg := make(map[int]int)
	for _, es := range g.Edges {
		for _, v := range es {
			indeg[v]++
		}
	}

	var q []int
	for i := range g.Events {
		if indeg[i] == 0 {
			q = append(q, i)
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

// ---------- trace generation ----------

func GenerateAsyncTrace(processes []string, numEvents int) Trace {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	trace := make(Trace, 0, numEvents)
	processClocks := make(map[string]VectorClock)
	pendingMessages := make(map[string][]Event)

	for _, p := range processes {
		processClocks[p] = NewVectorClock(processes)
		pendingMessages[p] = []Event{}
	}

	for len(trace) < numEvents {
		process, action := getRandomProcessAction(processes, r, pendingMessages)

		switch action {
		case EventSend:
			receiver := getRandomOtherProcess(r, processes, process)
			senderClock := processClocks[process]
			senderClock[process]++

			sendEvent := Event{
				Type:    EventSend,
				Process: process,
				VClock:  DeepCopy(senderClock),
			}

			trace = append(trace, sendEvent)
			pendingMessages[receiver] = append(pendingMessages[receiver], sendEvent)

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

			recvEvent := Event{
				Type:    EventReceive,
				Process: process,
				VClock:  DeepCopy(receiverClock),
			}

			trace = append(trace, recvEvent)
		}
	}
	return trace
}

func getRandomProcessAction(processes []string, r *rand.Rand, pending map[string][]Event) (string, EventType) {
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

func sortTraceByVClock(trace Trace) Trace {
	sorted := make(Trace, len(trace))
	copy(sorted, trace)

	slices.SortStableFunc(sorted, func(e1, e2 Event) int {
		sum1 := sumVC(e1.VClock)
		sum2 := sumVC(e2.VClock)
		if sum1 != sum2 {
			return sum1 - sum2
		}

		if e1.Process < e2.Process {
			return -1
		} else if e1.Process > e2.Process {
			return 1
		}
		return 0
	})
	return sorted
}

// ---------- display ----------

func (trace Trace) Print() {
	fmt.Println("Trace:")
	fmt.Print(trace.String())
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
	for i, e := range g.Events {
		label := fmt.Sprintf("%s:%s\n%s", e.Type.String(), e.Process, e.VClock.String())
		fmt.Fprintf(f, "  %d [label=%q];\n", i, label)
	}
	for u, dsts := range g.Edges {
		for _, v := range dsts {
			fmt.Fprintf(f, "  %d -> %d;\n", u, v)
		}
	}
	fmt.Fprintln(f, "}")
	return nil
}

//-----------Traversal ----------


func (g *CausalGraph) CheckSafetyProperty(
	precondition func(Event) bool,
	// The postcondition now receives the trigger's ID and the future event's ID and data.
	postcondition func(triggerID int, triggerEvent Event, futureID int, futureEvent Event) bool,
) bool {
	for i, event := range g.Events {
		if precondition(event) {
			// Create a closure that captures the trigger's ID (i) and data (event).
			specificPostcondition := func(futureID int, futureEvent Event) bool {
				return postcondition(i, event, futureID, futureEvent)
			}

			visited := make(map[int]bool)
			for _, l := range g.Edges[i] {
			if !g.verifyFuture(l, visited, specificPostcondition) {
				fmt.Printf(
					"Safety property violated! Precondition met at event %d (%s on %s), but postcondition failed in its future.\n",
					i, event.Type, event.Process,
				)
				return false
			}
		}
		}
	}
	return true
}

func (g *CausalGraph) verifyFuture(u int, visited map[int]bool, postcondition func(futureID int, futureEvent Event) bool) bool {
	if visited[u] {
		return true
	}
	visited[u] = true

	// Pass the current node's ID 'u' and its data 'g.Events[u]' to the postcondition.
	if !postcondition(u, g.Events[u]) {
		fmt.Printf("--> Postcondition failed at event %d: %s on %s, VClock: %s\n", u, g.Events[u].Type, g.Events[u].Process, g.Events[u].VClock)
		return false
	}

	for _, v := range g.Edges[u] {
		if !g.verifyFuture(v, visited, postcondition) {
			return false
		}
	}
	return true
}

// ---------- main ----------

func main() {
	nEvents := 1000
	procsFlag := "A,B,C,D"
	printTrace := false

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
	sortedTrace := sortTraceByVClock(trace)
	graph := NewCausalGraph(sortedTrace)
	graph.ReduceTransitive()
	buildDur := time.Since(startBuild)
	graph.WriteDOT("dot")


	totalEdges := 0
	maxOut := 0
	var sumOut int64
	for _, dsts := range graph.Edges {
		c := len(dsts)
		totalEdges += c
		sumOut += int64(c)
		if c > maxOut {
			maxOut = c
		}
	}
	avgOut := 0.0
	if len(graph.Events) > 0 {
		avgOut = float64(sumOut) / float64(len(graph.Events))
	}

	topo := graph.topoSort()
	dp := make(map[int]int, len(graph.Events))
	longest := 0
	for _, u := range topo {
		for _, v := range graph.Edges[u] {
			if dp[v] < dp[u]+1 {
				dp[v] = dp[u] + 1
				if dp[v] > longest {
					longest = dp[v]
				}
			}
		}
	}

	fmt.Print(" Causal graph built (with transitive reduction).")
	fmt.Printf("  nodes: %d\n", len(graph.Events))
	fmt.Printf("  direct edges: %d\n", totalEdges)
	fmt.Printf("  avg out-degree: %.3f\n", avgOut)
	fmt.Printf("  max out-degree: %d\n", maxOut)
	fmt.Printf("  longest path length: %d\n", longest)
	fmt.Printf("  build time (incl reduction): %s\n", buildDur)



	
	precondition := func(e Event) bool {
		return e.Process == "A" && e.Type == EventSend && e.VClock["A"] == 5
	}

	// Use the new, robust signature for the postcondition.
	postcondition := func(triggerID int, triggerEvent Event, futureID int, futureEvent Event) bool {
		return triggerEvent.VClock.HappensBefore(futureEvent.VClock)
	}

	fmt.Println("\nChecking safety property: If A sends with clock A:5, all future events must happen after.")
	startTest := time.Now()
	// The call remains the same.
	propertyHolds := graph.CheckSafetyProperty(precondition, postcondition)
	testDur := time.Since(startTest)

	fmt.Printf("Safety property check completed (time: %s).\n", testDur)

	if propertyHolds {
		fmt.Println("Result: Safety property HOLDS.")
	} else {
		fmt.Println("Result: Safety property was VIOLATED.")
	}
}
