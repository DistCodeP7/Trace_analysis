package main

import (
	"reflect"
	"testing"
)

func TestBuildCausalGraph_TransitiveReduction(t *testing.T) {
	procs := []string{"A", "B"}

	vc0 := NewVectorClock(procs)
	vc0["A"] = 1

	vc1 := NewVectorClock(procs)
	vc1["A"] = 1
	vc1["B"] = 1

	vc2 := NewVectorClock(procs)
	vc2["A"] = 2
	vc2["B"] = 1

	trace := Trace{
		{Type: EventSend, Process: "A", VClock: vc0},
		{Type: EventReceive, Process: "B", VClock: vc1},
		{Type: EventSend, Process: "A", VClock: vc2},
	}

	g := NewCausalGraph(trace)
	g.ReduceTransitive()

	expected := map[int][]int{
		0: {1},
		1: {2},
		2: {},
	}

	if len(g.Edges) != len(expected) {
		t.Fatalf("unexpected number of nodes: got %d want %d", len(g.Edges), len(expected))
	}

	for k, want := range expected {
		got, ok := g.Edges[k]
		if !ok {
			t.Fatalf("missing edges entry for node %d", k)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("edges for node %d: got %v want %v", k, got, want)
		}
	}
}

func TestBuildCausalGraph_ConcurrentEventsNoEdges(t *testing.T) {
	procs := []string{"A", "B"}

	vc0 := NewVectorClock(procs)
	vc0["A"] = 1

	vc1 := NewVectorClock(procs)
	vc1["B"] = 1

	trace := Trace{
		{Type: EventSend, Process: "A", VClock: vc0},
		{Type: EventSend, Process: "B", VClock: vc1},
	}

	g := NewCausalGraph(trace)

	expected := map[int][]int{
		0: {},
		1: {},
	}

	if len(g.Edges) != len(expected) {
		t.Fatalf("unexpected number of nodes: got %d want %d", len(g.Edges), len(expected))
	}

	for k, want := range expected {
		got, ok := g.Edges[k]
		if !ok {
			t.Fatalf("missing edges entry for node %d", k)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("edges for node %d: got %v want %v", k, got, want)
		}
	}
}

func TestCausalGraph_TransitiveReductionAutomatic(t *testing.T) {
	trace := Trace{
		{Type: EventSend, Process: "A", VClock: VectorClock{"A": 1, "B": 0}},
		{Type: EventReceive, Process: "B", VClock: VectorClock{"A": 1, "B": 1}},
		{Type: EventSend, Process: "B", VClock: VectorClock{"A": 1, "B": 2}},
	}

	g := NewCausalGraph(trace)

	totalBefore := 0
	for _, es := range g.Edges {
		totalBefore += len(es)
	}

	g.ReduceTransitive()

	totalAfter := 0
	for _, es := range g.Edges {
		totalAfter += len(es)
	}

	if totalAfter >= totalBefore {
		t.Fatalf("expected some edges to be removed after reduction, got before=%d after=%d", totalBefore, totalAfter)
	}

	expected := map[int][]int{
		0: {1},
		1: {2},
		2: {},
	}

	for id, want := range expected {
		got := g.Edges[id]
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("node %d: got %v want %v", id, got, want)
		}
	}
}

func TestCausalGraph_TransitiveReductionLarger(t *testing.T) {
	trace := Trace{
		{Type: EventSend, Process: "A", VClock: VectorClock{"A": 1, "B": 0, "C": 0}},
		{Type: EventReceive, Process: "B", VClock: VectorClock{"A": 1, "B": 1, "C": 0}},
		{Type: EventSend, Process: "B", VClock: VectorClock{"A": 1, "B": 2, "C": 0}},
		{Type: EventSend, Process: "A", VClock: VectorClock{"A": 2, "B": 0, "C": 0}},
		{Type: EventReceive, Process: "C", VClock: VectorClock{"A": 2, "B": 0, "C": 1}},
		{Type: EventSend, Process: "C", VClock: VectorClock{"A": 2, "B": 0, "C": 2}},
		{Type: EventReceive, Process: "C", VClock: VectorClock{"A": 2, "B": 2, "C": 3}},
	}

	g := NewCausalGraph(trace)

	totalBefore := 0
	for _, es := range g.Edges {
		totalBefore += len(es)
	}

	g.ReduceTransitive()
	g.WriteDOT("dot")
	totalAfter := 0
	for _, es := range g.Edges {
		totalAfter += len(es)
	}

	if totalAfter >= totalBefore {
		t.Fatalf("expected some edges to be removed after reduction, got before=%d after=%d", totalBefore, totalAfter)
	}

	expected := map[int][]int{
		0: {1, 3},
		1: {2},
		2: {6},
		3: {4},
		4: {5},
		5: {6},
		6: {},
	}

	for id, want := range expected {
		got := g.Edges[id]
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("node %d: got %v want %v", id, got, want)
		}
	}
}

func Test_Pen(t *testing.T) {
	trace := Trace{
		{Type: EventSend, Process: "A", VClock: VectorClock{"A": 1, "B": 0, "C": 0}},
		{Type: EventSend, Process: "A", VClock: VectorClock{"A": 2, "B": 0, "C": 0}},
		{Type: EventReceive, Process: "A", VClock: VectorClock{"A": 3, "B": 2, "C": 4}},
		{Type: EventReceive, Process: "A", VClock: VectorClock{"A": 4, "B": 4, "C": 4}},

		{Type: EventReceive, Process: "B", VClock: VectorClock{"A": 0, "B": 1, "C": 1}},
		{Type: EventSend, Process: "B", VClock: VectorClock{"A": 0, "B": 2, "C": 1}},
		{Type: EventSend, Process: "B", VClock: VectorClock{"A": 0, "B": 3, "C": 1}},

		{Type: EventSend, Process: "C", VClock: VectorClock{"A": 0, "B": 0, "C": 1}},
		{Type: EventReceive, Process: "C", VClock: VectorClock{"A": 2, "B": 0, "C": 2}},
		{Type: EventReceive, Process: "C", VClock: VectorClock{"A": 2, "B": 2, "C": 3}},
		{Type: EventSend, Process: "C", VClock: VectorClock{"A": 2, "B": 2, "C": 4}},
	}

	trace = sortTraceByVClock(trace)

	g := NewCausalGraph(trace)

	totalBefore := 0
	for _, es := range g.Edges {
		totalBefore += len(es)
	}

	g.ReduceTransitive()
	g.WriteDOT("dot")

	totalAfter := 0
	for _, es := range g.Edges {
		totalAfter += len(es)
	}

	if totalAfter >= totalBefore {
		t.Fatalf("expected some edges to be removed after reduction, got before=%d after=%d", totalBefore, totalAfter)
	}

	expected := map[int][]int{
		0:  {2},
		1:  {3, 6},
		2:  {6},
		3:  {4},
		4:  {5, 7},
		5:  {10},
		6:  {7},
		7:  {8},
		8:  {9},
		9:  {10},
		10: {},
	}

	for id, want := range expected {
		got := g.Edges[id]
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("node %d: got %v want %v", id, got, want)
		}
	}
}
